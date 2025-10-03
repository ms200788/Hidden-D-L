# telegram_upload_bot.py

import os
import asyncio
import logging
import random
import string
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.utils.executor import start_webhook
from aiogram.types import ParseMode, InlineKeyboardMarkup, InlineKeyboardButton

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, BigInteger, Text, Boolean, TIMESTAMP, String, ForeignKey, func, select, delete

from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Attempt dotenv for local dev
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("telegram_upload_bot")

# --- Environment Variables ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)
UPLOAD_CHANNEL = int(os.getenv("UPLOAD_CHANNEL", "0") or 0)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
WEBAPP_HOST = os.getenv("WEBAPP_HOST", "0.0.0.0")
WEBAPP_PORT = int(os.getenv("WEBAPP_PORT", "8000"))

# Validate required envs
_required = [BOT_TOKEN, OWNER_ID, UPLOAD_CHANNEL, DATABASE_URL, WEBHOOK_URL]
if not all(_required):
    logger.error("One or more required ENV vars missing: BOT_TOKEN, OWNER_ID, UPLOAD_CHANNEL, DATABASE_URL, WEBHOOK_URL")
    # Optionally, exit early
    # raise RuntimeError("Missing environment variables")

# Ensure DATABASE_URL uses asyncpg
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
    logger.info("Adjusted DATABASE_URL to use asyncpg driver.")

# --- Constants & Helpers ---
DEEP_LINK_LENGTH = 64
LINK_PATTERN = re.compile(r"\{([^|{}]+)\|([^{}]+)\}")
FIRST_NAME_PLACEHOLDER = "{first_name}"


def gen_deep_link(n: int = DEEP_LINK_LENGTH) -> str:
    chars = string.ascii_letters + string.digits + "-_"
    return ''.join(random.choice(chars) for _ in range(n))


def format_message_template(text: Optional[str], user: types.User) -> str:
    if not text:
        return ""
    result = text
    if FIRST_NAME_PLACEHOLDER in result and getattr(user, "first_name", None):
        result = result.replace(FIRST_NAME_PLACEHOLDER, user.first_name or "")
    # Replace {Label|url} with Markdown links
    def repl(m: re.Match):
        label = m.group(1).strip()
        url = m.group(2).strip()
        return f"[{label}]({url})"
    result = LINK_PATTERN.sub(repl, result)
    return result


# --- Bot & Dispatcher ---
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.MARKDOWN_V2)
dp = Dispatcher(bot)

# --- Database models ---
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    user_id = Column(BigInteger, primary_key=True, index=True)
    last_active = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)


class Setting(Base):
    __tablename__ = "settings"
    key = Column(String, primary_key=True)
    value = Column(Text, nullable=True)


class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(BigInteger, nullable=False)
    deep_link = Column(Text, unique=True, nullable=False)
    protect_content = Column(Boolean, default=False)
    auto_delete_minutes = Column(Integer, default=0)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)


class FileModel(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(Integer, ForeignKey("sessions.id", ondelete="CASCADE"))
    file_message_id = Column(BigInteger, nullable=False)
    caption = Column(Text, nullable=True)


class Delivery(Base):
    __tablename__ = "deliveries"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(BigInteger, nullable=False)
    chat_id = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    delete_at = Column(TIMESTAMP(timezone=True), nullable=False)
    session_id = Column(Integer, ForeignKey("sessions.id", ondelete="SET NULL"))

# Setup engine & sessionmaker
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL must be set")
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# In-memory upload session state (transient)
upload_sessions: Dict[int, Dict[str, Any]] = {}

# Scheduler for deletion
scheduler = AsyncIOScheduler()


# --- DB helper functions ---
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables created / ensured.")


async def add_or_update_user(user_id: int):
    async with AsyncSessionLocal() as session:
        user = await session.get(User, user_id)
        if user:
            user.last_active = datetime.now(timezone.utc)
        else:
            session.add(User(user_id=user_id, last_active=datetime.now(timezone.utc)))
        await session.commit()


async def save_setting(key: str, value: str):
    async with AsyncSessionLocal() as session:
        obj = await session.get(Setting, key)
        if obj:
            obj.value = value
        else:
            session.add(Setting(key=key, value=value))
        await session.commit()


async def get_setting(key: str) -> Optional[str]:
    async with AsyncSessionLocal() as session:
        obj = await session.get(Setting, key)
        return obj.value if obj else None


async def create_session_record(owner_id: int, deep_link: str, protect: bool, timer: int) -> int:
    async with AsyncSessionLocal() as session:
        rec = SessionModel(owner_id=owner_id, deep_link=deep_link, protect_content=protect, auto_delete_minutes=timer)
        session.add(rec)
        await session.commit()
        await session.refresh(rec)
        return rec.id


async def add_file_record(session_id: int, file_message_id: int, caption: str):
    async with AsyncSessionLocal() as session:
        rec = FileModel(session_id=session_id, file_message_id=file_message_id, caption=caption)
        session.add(rec)
        await session.commit()


async def find_session_by_deeplink(deep_link: str) -> Optional[SessionModel]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(SessionModel).where(SessionModel.deep_link == deep_link))
        return result.scalars().first()


async def get_files_for_session(session_id: int) -> List[FileModel]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(FileModel).where(FileModel.session_id == session_id).order_by(FileModel.id))
        return result.scalars().all()


async def schedule_delivery_for_deletion(user_id: int, chat_id: int, message_id: int, delete_at: datetime, session_id: Optional[int]):
    async with AsyncSessionLocal() as session:
        rec = Delivery(user_id=user_id, chat_id=chat_id, message_id=message_id, delete_at=delete_at, session_id=session_id)
        session.add(rec)
        await session.commit()


async def get_due_deliveries(now_dt: datetime) -> List[Delivery]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Delivery).where(Delivery.delete_at <= now_dt))
        return result.scalars().all()


async def delete_delivery_record(delivery_id: int):
    async with AsyncSessionLocal() as session:
        await session.execute(delete(Delivery).where(Delivery.id == delivery_id))
        await session.commit()


async def count_table_rows(model) -> int:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(func.count()).select_from(model))
        return int(result.scalar_one())


async def count_active_users(days: int = 2) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(User))
        rows = result.scalars().all()
    cnt = sum(1 for r in rows if r.last_active and r.last_active.replace(tzinfo=timezone.utc) >= cutoff)
    return cnt


# --- Deletion Worker ---
async def deletion_worker():
    try:
        now_dt = datetime.now(timezone.utc)
        due = await get_due_deliveries(now_dt)
        if due:
            logger.info("Found %d messages to delete.", len(due))
        for d in due:
            try:
                await bot.delete_message(chat_id=int(d.chat_id), message_id=int(d.message_id))
                logger.debug(f"Deleted message {d.message_id} in chat {d.chat_id}")
            except Exception as exc:
                logger.warning("Error deleting message %s in chat %s: %s", d.message_id, d.chat_id, exc)
            try:
                await delete_delivery_record(d.id)
            except Exception as exc:
                logger.warning("Error deleting delivery record %s: %s", d.id, exc)
    except Exception as exc:
        logger.exception("Exception in deletion_worker: %s", exc)


# --- Delivery logic ---
async def deliver_files_to_user(user_chat_id: int, user_obj: types.User, session_row: SessionModel, files: List[FileModel]) -> int:
    delivered = 0
    for f in files:
        try:
            # Try copy_message first
            try:
                copied = await bot.copy_message(chat_id=user_chat_id, from_chat_id=UPLOAD_CHANNEL, message_id=f.file_message_id)
                delivered_msg_id = copied.message_id
            except Exception as exc:
                logger.debug("copy_message failed, fallback to forward: %s", exc)
                forwarded = await bot.forward_message(chat_id=user_chat_id, from_chat_id=UPLOAD_CHANNEL, message_id=f.file_message_id)
                delivered_msg_id = forwarded.message_id

            if session_row.auto_delete_minutes and session_row.auto_delete_minutes > 0:
                delete_at = datetime.now(timezone.utc) + timedelta(minutes=int(session_row.auto_delete_minutes))
                await schedule_delivery_for_deletion(user_id=user_obj.id, chat_id=user_chat_id, message_id=delivered_msg_id, delete_at=delete_at, session_id=session_row.id)

            delivered += 1
        except Exception as exc:
            logger.exception("Failed to deliver file %s to user %s: %s", f.file_message_id, user_chat_id, exc)

    if session_row.auto_delete_minutes and delivered > 0:
        try:
            await bot.send_message(chat_id=user_chat_id, text=f"⚠ These files will be auto-deleted in {session_row.auto_delete_minutes} minute(s).")
        except Exception as exc:
            logger.debug("Failed to send auto-delete notice: %s", exc)

    return delivered


# --- Handlers ---

@dp.message_handler(commands=["health"])
async def cmd_health(message: types.Message):
    await message.reply("OK")


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await add_or_update_user(message.from_user.id)
    args = message.get_args().strip()
    if args:
        deep_link = args
        session_row = await find_session_by_deeplink(deep_link)
        if not session_row:
            await message.reply("❌ Invalid or expired link.")
            return
        files = await get_files_for_session(session_row.id)
        if not files:
            await message.reply("⚠ No files available for this session.")
            return
        delivered = await deliver_files_to_user(user_chat_id=message.chat.id, user_obj=message.from_user, session_row=session_row, files=files)
        if delivered == 0:
            await message.reply("⚠ Failed to deliver files.")
        return

    # No args: welcome
    welcome_text = await get_setting("welcome_text") or "👋 Welcome, {first_name}!"
    formatted = format_message_template(welcome_text, message.from_user)
    welcome_image = await get_setting("welcome_image")
    try:
        if welcome_image:
            await bot.send_photo(chat_id=message.chat.id, photo=welcome_image, caption=formatted)
        else:
            await bot.send_message(chat_id=message.chat.id, text=formatted)
    except Exception as exc:
        logger.warning("Failed to send welcome: %s", exc)
        try:
            await message.reply("👋 Welcome!")
        except:
            pass


@dp.message_handler(commands=["setstart"])
async def cmd_setstart(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or image) with /setstart to set welcome message (and optional image).")
        return
    reply = message.reply_to_message
    text = reply.caption or reply.text or "👋 Welcome, {first_name}!"
    img_file_id = None
    if reply.photo:
        img_file_id = reply.photo[-1].file_id
    await save_setting("welcome_text", text)
    if img_file_id:
        await save_setting("welcome_image", img_file_id)
    await message.reply("✅ Welcome message and image (if provided) saved.")


@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    upload_sessions[OWNER_ID] = {"files": [], "protect": False, "timer": 0}
    await message.reply(
        "📤 Upload session started.\n"
        "Reply with `on` or `off` to set protect_content.\n"
        "Then reply with number of minutes for auto-delete (0 = off).\n"
        "Then send files (photos, documents, videos etc.).\n"
        "When done, send /d to finish, or /e to cancel."
    )


@dp.message_handler(lambda message: message.from_user.id == OWNER_ID and OWNER_ID in upload_sessions and message.text is not None)
async def owner_text_responses(message: types.Message):
    sess = upload_sessions.get(OWNER_ID)
    if sess is None:
        return
    txt = message.text.strip().lower()
    if txt in ("on", "off", "yes", "no"):
        sess["protect"] = txt in ("on", "yes")
        await message.reply(f"Protect content set to {sess['protect']}. Now send auto-delete minutes (0 = off).")
        return
    if txt.isdigit():
        minutes = int(txt)
        sess["timer"] = minutes
        await message.reply(f"Auto-delete timer set to {minutes}. Now send files, then /d to finish or /e to cancel.")
        return
    # not part of upload flow; ignore


@dp.message_handler(lambda message: message.from_user.id == OWNER_ID, content_types=types.ContentTypes.ANY)
async def owner_file_handler(message: types.Message):
    if OWNER_ID not in upload_sessions:
        return
    # Only accept media / document types
    if not (message.document or message.photo or message.video or message.audio or message.voice or message.sticker):
        return
    sess = upload_sessions[OWNER_ID]
    try:
        # copy or forward to upload channel
        try:
            sent = await bot.copy_message(chat_id=UPLOAD_CHANNEL, from_chat_id=message.chat.id, message_id=message.message_id)
            sent_id = sent.message_id
        except Exception as exc:
            logger.warning("copy_message failed: %s. Falling back to forward.", exc)
            forwarded = await bot.forward_message(chat_id=UPLOAD_CHANNEL, from_chat_id=message.chat.id, message_id=message.message_id)
            sent_id = forwarded.message_id

        sess["files"].append({"file_message_id": sent_id, "caption": message.caption or ""})
        await message.reply("📌 File added. Continue or send /d to finish, /e to cancel.")
    except Exception as exc:
        logger.exception("Failed to forward to upload channel: %s", exc)
        await message.reply("⚠ Error while forwarding to upload channel.")


@dp.message_handler(commands=["d"])
async def cmd_finish_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    if OWNER_ID not in upload_sessions:
        await message.reply("No active upload session.")
        return
    sess = upload_sessions.pop(OWNER_ID)
    files = sess.get("files", [])
    if not files:
        await message.reply("No files added. Session cancelled.")
        return
    deep_link = gen_deep_link()
    session_id = await create_session_record(OWNER_ID, deep_link, sess.get("protect", False), sess.get("timer", 0))
    for f in files:
        await add_file_record(session_id, f["file_message_id"], f.get("caption", ""))
    await message.reply(f"✅ Upload complete.\n🔗 Deep link (share with users):\n`/start {deep_link}`")


@dp.message_handler(commands=["e"])
async def cmd_cancel_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    if OWNER_ID in upload_sessions:
        upload_sessions.pop(OWNER_ID, None)
    await message.reply("❌ Upload cancelled.")


@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /broadcast <message>. Use {Label|url} syntax for links.")
        return
    text = LINK_PATTERN.sub(lambda m: f"[{m.group(1)}]({m.group(2)})", args)
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(User.user_id))
        user_ids = result.scalars().all()
    if not user_ids:
        await message.reply("No users to broadcast.")
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Open", callback_data="broadcast_open")]]
    )
    sent = 0
    for uid in user_ids:
        try:
            await bot.send_message(chat_id=uid, text=text, reply_markup=kb)
            sent += 1
        except Exception:
            pass
    await message.reply(f"✅ Broadcast done: {sent}/{len(user_ids)} sent.")


@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    total_users = await count_table_rows(User)
    active_users = await count_active_users(2)
    total_files = await count_table_rows(FileModel)
    total_sessions = await count_table_rows(SessionModel)
    await message.reply(
        f"📊 Stats:\n"
        f"👥 Total Users: {total_users}\n"
        f"🟢 Active (2d): {active_users}\n"
        f"📂 Files: {total_files}\n"
        f"📦 Sessions: {total_sessions}"
    )


@dp.message_handler()
async def fallback(message: types.Message):
    # Ignore unknown commands/messages
    return


# --- Webhook / Startup / Shutdown ---

async def on_startup(dp: Dispatcher):
    logger.info("Starting up...")
    await init_db()
    # Set webhook
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except Exception as exc:
        logger.exception("Error setting webhook: %s", exc)
    # Start deletion job
    scheduler.add_job(deletion_worker, "interval", seconds=15, id="deletion_worker", replace_existing=True)
    scheduler.start()
    logger.info("Scheduler started.")


async def on_shutdown(dp: Dispatcher):
    logger.info("Shutting down...")
    try:
        await bot.delete_webhook()
    except Exception:
        pass
    try:
        await bot.session.close()
    except Exception:
        pass
    try:
        await engine.dispose()
    except Exception:
        pass
    scheduler.shutdown(wait=False)
    logger.info("Shutdown complete.")


if __name__ == "__main__":
    # Setup a small health-check route via aiohttp
    from aiohttp import web

    async def health_handler(request):
        return web.Response(text="OK")

    web_app = web.Application()
    web_app.router.add_get("/health", health_handler)

    webhook_path = "/webhook"
    logger.info("Starting webhook server on %s:%s path %s", WEBAPP_HOST, WEBAPP_PORT, webhook_path)
    start_webhook(
        dispatcher=dp,
        webhook_path=webhook_path,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
        host=WEBAPP_HOST,
        port=WEBAPP_PORT,
        web_app=web_app,
    )
