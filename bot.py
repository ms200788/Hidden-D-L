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
from aiogram.types import ParseMode
from aiogram.dispatcher.webhook import SimpleRequestHandler
from aiohttp import web

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, BigInteger, Text, Boolean, TIMESTAMP, String, ForeignKey, func, select, delete

from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Attempt dotenv for local dev
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("telegram_upload_bot")

# --- Environment Variables ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)
UPLOAD_CHANNEL = int(os.getenv("UPLOAD_CHANNEL", "0") or 0)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
# This is the base URL of your Render web service, e.g., "https://your-app-name.onrender.com"
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
WEBAPP_HOST = os.getenv("WEBAPP_HOST", "0.0.0.0")
WEBAPP_PORT = int(os.getenv("WEBAPP_PORT", "10000")) # Render uses port 10000 by default

# Validate required envs
_required = [BOT_TOKEN, OWNER_ID, UPLOAD_CHANNEL, DATABASE_URL, WEBHOOK_URL]
if not all(_required):
    logger.error("One or more required ENV vars missing: BOT_TOKEN, OWNER_ID, UPLOAD_CHANNEL, DATABASE_URL, WEBHOOK_URL")
    raise RuntimeError("Missing environment variables")

# Ensure DATABASE_URL uses asyncpg for NeonDB
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
        # Basic escaping for MarkdownV2
        label = re.sub(r"([_*\[\]()~`>#+\-=|{}.!])", r"\\\1", label)
        url = re.sub(r"([)])", r"\\\1", url)
        return f"[{label}]({url})"
    result = LINK_PATTERN.sub(repl, result)
    return result


# --- Bot & Dispatcher ---
# Note: Using MarkdownV2, so special characters in user-generated text MUST be escaped.
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.MARKDOWN_V2)
dp = Dispatcher(bot)

# --- Database models ---
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    user_id = Column(BigInteger, primary_key=True, index=True)
    last_active = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


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
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# In-memory upload session state (transient)
upload_sessions: Dict[int, Dict[str, Any]] = {}

# Scheduler for deletion
scheduler = AsyncIOScheduler(timezone="UTC")


# --- DB helper functions ---
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables created / ensured.")


async def add_or_update_user(user_id: int):
    async with AsyncSessionLocal() as session:
        user = await session.get(User, user_id)
        if user:
            # last_active is updated automatically by onupdate=func.now()
            pass
        else:
            session.add(User(user_id=user_id))
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


# FIXED: This query is now efficient and runs directly on the database.
async def count_active_users(days: int = 2) -> int:
    """Counts users active in the last N days directly in the database."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    async with AsyncSessionLocal() as session:
        stmt = select(func.count(User.user_id)).where(User.last_active >= cutoff)
        result = await session.execute(stmt)
        return int(result.scalar_one())


# --- Deletion Worker ---
async def deletion_worker():
    try:
        now_dt = datetime.now(timezone.utc)
        due = await get_due_deliveries(now_dt)
        if not due:
            return
            
        logger.info("Found %d messages to delete.", len(due))
        for d in due:
            try:
                await bot.delete_message(chat_id=int(d.chat_id), message_id=int(d.message_id))
                logger.debug(f"Deleted message {d.message_id} in chat {d.chat_id}")
            except Exception as exc:
                logger.warning("Error deleting message %s in chat %s: %s", d.message_id, d.chat_id, exc)
            finally:
                # Always try to delete the DB record to prevent re-attempts
                await delete_delivery_record(d.id)
    except Exception as exc:
        logger.exception("Exception in deletion_worker: %s", exc)


# --- Delivery logic ---
async def deliver_files_to_user(user_chat_id: int, user_obj: types.User, session_row: SessionModel, files: List[FileModel]) -> int:
    delivered = 0
    for f in files:
        try:
            # Try copy_message first
            delivered_msg = await bot.copy_message(
                chat_id=user_chat_id,
                from_chat_id=UPLOAD_CHANNEL,
                message_id=f.file_message_id,
                protect_content=session_row.protect_content
            )

            if session_row.auto_delete_minutes and session_row.auto_delete_minutes > 0:
                delete_at = datetime.now(timezone.utc) + timedelta(minutes=int(session_row.auto_delete_minutes))
                await schedule_delivery_for_deletion(
                    user_id=user_obj.id,
                    chat_id=user_chat_id,
                    message_id=delivered_msg.message_id,
                    delete_at=delete_at,
                    session_id=session_row.id
                )
            delivered += 1
        except Exception as exc:
            logger.exception("Failed to deliver file %s to user %s: %s", f.file_message_id, user_chat_id, exc)

    if session_row.auto_delete_minutes and delivered > 0:
        try:
            # Note: Must escape the message for MarkdownV2
            await bot.send_message(
                chat_id=user_chat_id,
                text=f"⚠️ These files will be auto-deleted in *{session_row.auto_delete_minutes} minute(s)*\\."
            )
        except Exception as exc:
            logger.debug("Failed to send auto-delete notice: %s", exc)

    return delivered


# --- Handlers ---

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await add_or_update_user(message.from_user.id)
    args = message.get_args().strip()
    if args:
        deep_link = args
        session_row = await find_session_by_deeplink(deep_link)
        if not session_row:
            await message.reply("❌ Invalid or expired link\\.")
            return
        files = await get_files_for_session(session_row.id)
        if not files:
            await message.reply("⚠️ No files available for this session\\.")
            return
        
        await message.reply(f"⬇️ Preparing your files \\({len(files)}\\)\\.\\.\\.")
        delivered = await deliver_files_to_user(
            user_chat_id=message.chat.id,
            user_obj=message.from_user,
            session_row=session_row,
            files=files
        )
        if delivered == 0:
            await message.reply("⚠️ Failed to deliver files due to an internal error\\.")
        return

    # No args: welcome
    welcome_text = await get_setting("welcome_text") or "👋 Welcome, {first_name}\\!"
    formatted = format_message_template(welcome_text, message.from_user)
    welcome_image = await get_setting("welcome_image")
    try:
        if welcome_image:
            await bot.send_photo(chat_id=message.chat.id, photo=welcome_image, caption=formatted)
        else:
            await bot.send_message(chat_id=message.chat.id, text=formatted)
    except Exception as exc:
        logger.warning("Failed to send welcome message: %s", exc)
        try:
            await message.reply("👋 Welcome\\!")
        except Exception:
            pass


@dp.message_handler(commands=["setstart"], user_id=OWNER_ID)
async def cmd_setstart(message: types.Message):
    if not message.reply_to_message:
        await message.reply("Reply to a message with `/setstart` to set the welcome message\\.\nIt can be text or an image with a caption\\.")
        return
    
    reply = message.reply_to_message
    text = reply.caption or reply.text or "👋 Welcome, {first_name}\\!"
    img_file_id = reply.photo[-1].file_id if reply.photo else None
    
    await save_setting("welcome_text", text)
    await save_setting("welcome_image", img_file_id or "")
    await message.reply("✅ Welcome message and image (if provided) have been saved\\.")


@dp.message_handler(commands=["upload"], user_id=OWNER_ID)
async def cmd_upload(message: types.Message):
    upload_sessions[OWNER_ID] = {"files": [], "protect": False, "timer": 0, "step": "protect"}
    await message.reply(
        "📤 *New Upload Session Started*\n\n"
        "*Step 1: Content Protection*\n"
        "Should users be prevented from forwarding/saving these files? Send `yes` or `no`\\.",
    )


@dp.message_handler(lambda msg: msg.from_user.id == OWNER_ID and OWNER_ID in upload_sessions, content_types=types.ContentTypes.TEXT)
async def owner_text_responses(message: types.Message):
    sess = upload_sessions.get(OWNER_ID)
    if not sess: return

    step = sess.get("step")
    txt = message.text.strip().lower()

    if step == "protect":
        if txt in ("yes", "on"):
            sess["protect"] = True
            await message.reply("✅ Protection: *ON*\\.")
        elif txt in ("no", "off"):
            sess["protect"] = False
            await message.reply("✅ Protection: *OFF*\\.")
        else:
            await message.reply("Invalid input\\. Please send `yes` or `no`\\.")
            return
        
        sess["step"] = "timer"
        await message.answer(
            "*Step 2: Auto\\-Delete Timer*\n"
            "After how many minutes should the files be deleted from the user's chat? Send a number \\(e\\.g\\., `60` for 1 hour\\)\\. Send `0` to disable\\."
        )

    elif step == "timer":
        if not txt.isdigit():
            await message.reply("Invalid input\\. Please send a number of minutes \\(e\\.g\\., `15`\\), or `0` to disable\\.")
            return
        
        minutes = int(txt)
        sess["timer"] = minutes
        if minutes > 0:
            await message.reply(f"✅ Auto\\-Delete: *{minutes} minute(s)*\\.")
        else:
            await message.reply("✅ Auto\\-Delete: *Disabled*\\.")

        sess["step"] = "files"
        await message.answer(
            "*Step 3: Send Files*\n"
            "Now, send all the files you want to include in this bundle\\. When you are finished, send /d to generate the link\\.\n\nTo cancel, send /e\\."
        )


@dp.message_handler(lambda msg: msg.from_user.id == OWNER_ID and OWNER_ID in upload_sessions, content_types=types.ContentTypes.ANY)
async def owner_file_handler(message: types.Message):
    sess = upload_sessions.get(OWNER_ID)
    if not sess or sess.get("step") != "files":
        return

    # Only accept media / document types
    if not (message.document or message.photo or message.video or message.audio or message.voice or message.sticker):
        return

    try:
        sent = await bot.copy_message(chat_id=UPLOAD_CHANNEL, from_chat_id=message.chat.id, message_id=message.message_id)
        sess["files"].append({"file_message_id": sent.message_id, "caption": message.caption or ""})
        await message.reply(f"📌 File added \\({len(sess['files'])}\\)\\. Send more files, or /d to finish\\.")
    except Exception as exc:
        logger.exception("Failed to forward to upload channel: %s", exc)
        await message.reply("⚠️ Error: Could not save file to upload channel\\. Please try again\\.")


@dp.message_handler(commands=["d"], user_id=OWNER_ID)
async def cmd_finish_upload(message: types.Message):
    if OWNER_ID not in upload_sessions:
        await message.reply("No active upload session\\. Use /upload to start one\\.")
        return
    
    sess = upload_sessions.pop(OWNER_ID)
    files = sess.get("files", [])
    if not files:
        await message.reply("No files were added\\. Session cancelled\\.")
        return
        
    deep_link = gen_deep_link()
    session_id = await create_session_record(OWNER_ID, deep_link, sess.get("protect", False), sess.get("timer", 0))
    for f in files:
        await add_file_record(session_id, f["file_message_id"], f.get("caption", ""))
        
    bot_info = await bot.get_me()
    share_link = f"https://t.me/{bot_info.username}?start={deep_link}"
    
    await message.reply(
        f"✅ *Upload Complete*\n\n"
        f"Total Files: *{len(files)}*\n"
        f"Protection: *{'ON' if sess.get('protect') else 'OFF'}*\n"
        f"Auto\\-Delete: *{sess.get('timer', 0)} minutes*\n\n"
        f"Share this link with your users:\n"
        f"`{share_link}`"
    )


@dp.message_handler(commands=["e"], user_id=OWNER_ID)
async def cmd_cancel_upload(message: types.Message):
    if OWNER_ID in upload_sessions:
        upload_sessions.pop(OWNER_ID, None)
    await message.reply("❌ Upload session cancelled\\.")


@dp.message_handler(commands=["broadcast"], user_id=OWNER_ID)
async def cmd_broadcast(message: types.Message):
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: `/broadcast <message>`\\. Use `{Label|url}` for links\\.")
        return
    
    text_to_send = format_message_template(args, message.from_user)

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(User.user_id))
        user_ids = result.scalars().all()
    
    if not user_ids:
        await message.reply("No users to broadcast to.")
        return

    sent, failed = 0, 0
    await message.reply(f"📢 Starting broadcast to {len(user_ids)} users...")

    for uid in user_ids:
        try:
            await bot.send_message(chat_id=uid, text=text_to_send)
            sent += 1
        except Exception as e:
            logger.warning("Broadcast failed for user %d: %s", uid, e)
            failed += 1
        await asyncio.sleep(0.1) # Avoid hitting rate limits

    await message.reply(f"✅ Broadcast done\\.\n*Sent*: {sent}\n*Failed*: {failed}")


@dp.message_handler(commands=["stats"], user_id=OWNER_ID)
async def cmd_stats(message: types.Message):
    total_users = await count_table_rows(User)
    active_users = await count_active_users(2)
    total_files = await count_table_rows(FileModel)
    total_sessions = await count_table_rows(SessionModel)
    
    await message.reply(
        f"📊 *Bot Stats*\n\n"
        f"👥 *Total Users*: {total_users}\n"
        f"🟢 *Active Users* \\(48h\\): {active_users}\n"
        f"📂 *Total Files Shared*: {total_files}\n"
        f"📦 *Total Sessions Created*: {total_sessions}"
    )


# --- Webhook and Server Setup ---

# This function will be called when the application starts up
async def on_startup(dispatcher: Dispatcher):
    # 1. Initialize DB
    await init_db()

    # 2. Set webhook
    webhook_path = f"/webhook/{BOT_TOKEN}"
    full_webhook_url = WEBHOOK_URL.rstrip('/') + webhook_path
    try:
        await bot.set_webhook(full_webhook_url)
        logger.info(f"Webhook set to {full_webhook_url}")
    except Exception as exc:
        logger.exception("Failed to set webhook: %s", exc)

    # 3. Start APScheduler deletion job
    scheduler.add_job(deletion_worker, "interval", seconds=30, id="deletion_worker", replace_existing=True)
    scheduler.start()
    logger.info("APScheduler started (deletion worker every 30s).")

# This function will be called when the application shuts down
async def on_shutdown(dispatcher: Dispatcher):
    logger.info("Shutting down...")
    scheduler.shutdown(wait=False)
    await bot.delete_webhook()
    await dp.storage.close()
    await dp.storage.wait_closed()
    await (await bot.get_session()).close()
    await engine.dispose()
    logger.info("Shutdown complete.")

# Health check endpoint for Render/Uptime pinger
async def health_check(request: web.Request):
    return web.Response(text="OK")

if __name__ == "__main__":
    # The path for the webhook must be secret and known only to you and Telegram.
    # Using the bot token in the path is a simple way to achieve this.
    webhook_path = f"/webhook/{BOT_TOKEN}"

    # Create the main aiohttp application
    app = web.Application()

    # Create a handler for incoming Telegram updates.
    # This connects aiogram's Dispatcher to the aiohttp web server.
    webhook_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
    )
    
    # Register the webhook handler on the secret path
    app.router.add_post(webhook_path, webhook_handler)

    # Register the health check endpoint for Render's health checks
    app.router.add_get("/health", health_check)
    
    # Register the startup and shutdown callbacks
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    # Start the web server
    logger.info(f"Starting web server on {WEBAPP_HOST}:{WEBAPP_PORT}")
    web.run_app(
        app,
        host=WEBAPP_HOST,
        port=WEBAPP_PORT,
        access_log=logger,
    )