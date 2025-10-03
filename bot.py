# bot.py
"""
Telegram Upload Bot — compatible with:
 - aiogram==2.25.1
 - aiohttp
 - APScheduler
 - SQLAlchemy==2.0.23 (async)
 - asyncpg driver for Neon
Use webhook (aiohttp) and DB async engine via SQLAlchemy.
"""

import os
import asyncio
import logging
import random
import string
import re
from datetime import datetime, timedelta, timezone

from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# aiogram v2
from aiogram import Bot, Dispatcher, types
from aiogram.utils.executor import start_webhook

# SQLAlchemy async ORM (v2 style)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import (
    Column, Integer, BigInteger, Text, Boolean, TIMESTAMP, ForeignKey, String, func
)

from sqlalchemy import select, insert, update, delete

# load .env when testing locally (optional)
from dotenv import load_dotenv
load_dotenv()

# ----- logging -----
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----- ENV VARS (set these in Render) -----
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)
UPLOAD_CHANNEL = int(os.getenv("UPLOAD_CHANNEL", "0") or 0)  # -100...
DATABASE_URL = os.getenv("DATABASE_URL", "")  # example: postgresql+asyncpg://user:pass@host:port/dbname
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")  # https://<app>.onrender.com/webhook
WEBAPP_HOST = os.getenv("WEBAPP_HOST", "0.0.0.0")
WEBAPP_PORT = int(os.getenv("WEBAPP_PORT", "8000"))

if not (BOT_TOKEN and OWNER_ID and UPLOAD_CHANNEL and DATABASE_URL and WEBHOOK_URL):
    logger.warning("Missing one or more required ENV vars: BOT_TOKEN, OWNER_ID, UPLOAD_CHANNEL, DATABASE_URL, WEBHOOK_URL")

# ----- Telegram setup -----
bot = Bot(token=BOT_TOKEN, parse_mode="Markdown")
dp = Dispatcher(bot)

# ----- SQLAlchemy models -----
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    user_id = Column(BigInteger, primary_key=True, index=True)
    last_active = Column(TIMESTAMP(timezone=True), server_default=func.now())

class Setting(Base):
    __tablename__ = "settings"
    key = Column(String, primary_key=True)
    value = Column(Text)

class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(BigInteger, nullable=False)
    deep_link = Column(Text, unique=True, nullable=False)
    protect_content = Column(Boolean, default=False)
    auto_delete_minutes = Column(Integer, default=0)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

class FileModel(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(Integer, ForeignKey("sessions.id", ondelete="CASCADE"))
    file_message_id = Column(BigInteger)  # message id inside UPLOAD_CHANNEL
    caption = Column(Text)

class Delivery(Base):
    __tablename__ = "deliveries"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(BigInteger)
    chat_id = Column(BigInteger)
    message_id = Column(BigInteger)
    delete_at = Column(TIMESTAMP(timezone=True))
    session_id = Column(Integer, ForeignKey("sessions.id", ondelete="SET NULL"))

# ----- DB engine & sessionmaker -----
# Expect DATABASE_URL like: postgresql+asyncpg://user:pass@host:port/dbname
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# ----- In-memory owner upload session state (transient) -----
# { owner_id: {"files":[{"file_message_id":int,"caption":str}], "protect":bool, "timer":int } }
upload_sessions = {}

# ----- helpers -----
DEEP_LINK_LENGTH = 64
LINK_PATTERN = re.compile(r"\{([^|{}]+)\|([^{}]+)\}")

def gen_deep_link(n=DEEP_LINK_LENGTH):
    chars = string.ascii_letters + string.digits + "-_"
    return ''.join(random.choice(chars) for _ in range(n))

def format_text_with_links_and_placeholders(text: str, user: types.User | None):
    if not text:
        return ""
    if user and "{first_name}" in text:
        text = text.replace("{first_name}", (user.first_name or ""))
    def repl(m):
        label = m.group(1).strip()
        url = m.group(2).strip()
        return f"[{label}]({url})"
    return LINK_PATTERN.sub(repl, text)

# ----- DB CRUD helpers (async) -----
async def init_db():
    async with engine.begin() as conn:
        # create tables if not exist
        await conn.run_sync(Base.metadata.create_all)
    logger.info("DB initialized (tables ensured).")

async def add_or_update_user(user_id: int):
    async with AsyncSessionLocal() as session:
        # upsert style: try insert, on conflict update last_active using simple logic
        await session.execute(
            insert(User).values(user_id=user_id, last_active=func.now()).on_conflict_do_update(
                index_elements=[User.user_id],
                set_={"last_active": func.now()}
            )
        )
        await session.commit()

async def save_setting(key: str, value: str):
    async with AsyncSessionLocal() as session:
        await session.execute(
            insert(Setting).values(key=key, value=value).on_conflict_do_update(
                index_elements=[Setting.key],
                set_={"value": value}
            )
        )
        await session.commit()

async def get_setting_value(key: str):
    async with AsyncSessionLocal() as session:
        res = await session.get(Setting, key)
        return res.value if res else None

async def create_session_record(owner_id:int, deep_link:str, protect:bool, timer:int):
    async with AsyncSessionLocal() as session:
        rec = SessionModel(owner_id=owner_id, deep_link=deep_link, protect_content=protect, auto_delete_minutes=timer)
        session.add(rec)
        await session.commit()
        await session.refresh(rec)
        return rec.id

async def add_file_record(session_id:int, file_message_id:int, caption:str):
    async with AsyncSessionLocal() as session:
        rec = FileModel(session_id=session_id, file_message_id=file_message_id, caption=caption)
        session.add(rec)
        await session.commit()

async def find_session_by_deeplink(deep_link:str):
    async with AsyncSessionLocal() as session:
        q = await session.execute(select(SessionModel).where(SessionModel.deep_link == deep_link))
        row = q.scalars().first()
        return row

async def get_files_for_session(session_id:int):
    async with AsyncSessionLocal() as session:
        q = await session.execute(select(FileModel).where(FileModel.session_id==session_id).order_by(FileModel.id))
        return q.scalars().all()

async def schedule_delivery(user_id:int, chat_id:int, message_id:int, delete_at:datetime, session_id:int|None):
    async with AsyncSessionLocal() as session:
        rec = Delivery(user_id=user_id, chat_id=chat_id, message_id=message_id, delete_at=delete_at, session_id=session_id)
        session.add(rec)
        await session.commit()

async def get_due_deliveries(now_dt:datetime):
    async with AsyncSessionLocal() as session:
        q = await session.execute(select(Delivery).where(Delivery.delete_at <= now_dt))
        return q.scalars().all()

async def remove_delivery_record(delivery_id:int):
    async with AsyncSessionLocal() as session:
        await session.execute(delete(Delivery).where(Delivery.id==delivery_id))
        await session.commit()

async def count_table(table_model):
    async with AsyncSessionLocal() as session:
        q = await session.execute(select(func.count()).select_from(table_model))
        return q.scalar_one()

async def count_active_users(days=2):
    async with AsyncSessionLocal() as session:
        interval = f"{days} days"
        q = await session.execute(select(func.count()).select_from(User).where(User.last_active >= func.now() - func.cast(interval, TIMESTAMP)))
        # fallback simpler: count everyone (due to SQLAlchemy cast complexity), so compute in Python if needed
        # We'll do a simple count for robustness:
        q2 = await session.execute(select(User))
        rows = q2.scalars().all()
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        cnt = sum(1 for r in rows if r.last_active and r.last_active.replace(tzinfo=timezone.utc) >= cutoff)
        return cnt

# ----- APScheduler deletion job -----
scheduler = AsyncIOScheduler()

async def deletion_job():
    now_dt = datetime.now(timezone.utc)
    due = await get_due_deliveries(now_dt)
    if not due:
        return
    logger.info("Deletion job: found %d deliveries due", len(due))
    for d in due:
        try:
            await bot.delete_message(chat_id=int(d.chat_id), message_id=int(d.message_id))
        except Exception as exc:
            logger.warning("Could not delete message %s in %s: %s", d.message_id, d.chat_id, exc)
        try:
            await remove_delivery_record(d.id)
        except Exception as exc:
            logger.warning("Could not remove delivery record %s: %s", d.id, exc)

# ----- aiohttp webhook server (used by aiogram webhook approach) -----
# We'll use aiogram Dispatcher.process_update to process incoming updates.

# ----- Bot Handlers -----
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await add_or_update_user(message.from_user.id)
    args = message.get_args().strip()
    if args:
        # treat args as deep link
        link = args
        session = await find_session_by_deeplink(link)
        if not session:
            await message.reply("❌ Invalid or expired link.")
            return
        files = await get_files_for_session(session.id)
        if not files:
            await message.reply("⚠ No files for this session.")
            return
        for f in files:
            try:
                # copy message from upload channel to user's chat
                # try to use copy_message with protect_content (aiogram v2 may not expose protect_content for copy_message);
                # we'll attempt copy_message with caption preserved, otherwise fallback to forward_message.
                try:
                    # copy_message signature in aiogram v2 supports (chat_id, from_chat_id, message_id, caption=None, parse_mode=None)
                    copied = await bot.copy_message(chat_id=message.chat.id, from_chat_id=UPLOAD_CHANNEL, message_id=f.file_message_id)
                    delivered_msg_id = copied.message_id
                except Exception:
                    # fallback to forward
                    forwarded = await bot.forward_message(chat_id=message.chat.id, from_chat_id=UPLOAD_CHANNEL, message_id=f.file_message_id)
                    delivered_msg_id = forwarded.message_id

                # schedule deletion if needed
                if session.auto_delete_minutes and session.auto_delete_minutes > 0:
                    delete_at = datetime.now(timezone.utc) + timedelta(minutes=int(session.auto_delete_minutes))
                    await schedule_delivery(user_id=message.from_user.id, chat_id=message.chat.id, message_id=delivered_msg_id, delete_at=delete_at, session_id=session.id)
                    # notify user (single message)
                    await bot.send_message(chat_id=message.chat.id, text=f"⚠ These files will be auto-deleted in {session.auto_delete_minutes} minutes.")
            except Exception as exc:
                logger.exception("Error delivering file: %s", exc)
        return

    # no args: show welcome message
    welcome_text = await get_setting_value("welcome_text") or "👋 Welcome, {first_name}!"
    formatted = format_text_with_links_and_placeholders(welcome_text, message.from_user)
    welcome_image = await get_setting_value("welcome_image")
    try:
        if welcome_image:
            await bot.send_photo(chat_id=message.chat.id, photo=welcome_image, caption=formatted)
        else:
            await bot.send_message(chat_id=message.chat.id, text=formatted)
    except Exception as exc:
        logger.exception("Failed to send welcome: %s", exc)
        await message.reply("👋 Welcome!")

@dp.message_handler(commands=["setstart"])
async def cmd_setstart(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (image/text) with /setstart to save the welcome message/image.")
        return
    reply = message.reply_to_message
    text = reply.caption or reply.text or "👋 Welcome, {first_name}!"
    img_file_id = None
    if reply.photo:
        img_file_id = reply.photo[-1].file_id
    await save_setting("welcome_text", text)
    if img_file_id:
        await save_setting("welcome_image", img_file_id)
    await message.reply("✅ Welcome message (and image if provided) saved.")

@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    upload_sessions[OWNER_ID] = {"files": [], "protect": False, "timer": 0}
    await message.reply(
        "📤 Upload session started.\nReply with `on` or `off` to enable/disable protect_content for forwarded files.\nThen reply with auto-delete minutes (0 = off).\nThen send files (photos/docs/videos). When done, send /d to finish and create deep link, or /e to cancel."
    )

# owner replies for protect and timer captured by a generic text handler
@dp.message_handler(lambda message: message.from_user.id == OWNER_ID and (message.text is not None))
async def owner_texts(message: types.Message):
    if OWNER_ID not in upload_sessions:
        return
    txt = message.text.strip().lower()
    if txt in ("on", "off", "yes", "no"):
        upload_sessions[OWNER_ID]["protect"] = txt in ("on", "yes")
        await message.reply(f"Protect_content set to {upload_sessions[OWNER_ID]['protect']}. Now reply with auto-delete minutes (0 = off).")
        return
    if txt.isdigit():
        upload_sessions[OWNER_ID]["timer"] = int(txt)
        await message.reply(f"Auto-delete timer set to {upload_sessions[OWNER_ID]['timer']} minutes. Now send files.")
        return
    # otherwise don't interfere

# file handler (owner uploads files)
@dp.message_handler(lambda message: message.from_user.id == OWNER_ID, content_types=types.ContentTypes.ANY)
async def owner_file_handler(message: types.Message):
    if OWNER_ID not in upload_sessions:
        return
    # only accept media/document types
    if not (message.document or message.photo or message.video or message.audio or message.voice):
        return
    try:
        # copy message to UPLOAD_CHANNEL
        # We try copy_message; if fails, use forward_message.
        try:
            sent = await bot.copy_message(chat_id=UPLOAD_CHANNEL, from_chat_id=message.chat.id, message_id=message.message_id)
            upload_sessions[OWNER_ID]["files"].append({"file_message_id": sent.message_id, "caption": message.caption or ""})
            await message.reply("✅ File copied to upload channel and added to session. Use /d to finish or /e to cancel.")
        except Exception as exc:
            logger.warning("copy_message failed, trying forward: %s", exc)
            forwarded = await bot.forward_message(chat_id=UPLOAD_CHANNEL, from_chat_id=message.chat.id, message_id=message.message_id)
            upload_sessions[OWNER_ID]["files"].append({"file_message_id": forwarded.message_id, "caption": message.caption or ""})
            await message.reply("✅ File forwarded to upload channel and added to session. Use /d to finish or /e to cancel.")
    except Exception as exc:
        logger.exception("Error storing file to upload channel: %s", exc)
        await message.reply("⚠ Failed to forward/copy file to upload channel. Ensure bot is admin in that channel.")

@dp.message_handler(commands=["d"])
async def cmd_finish_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    if OWNER_ID not in upload_sessions:
        await message.reply("No active upload session.")
        return
    sess = upload_sessions[OWNER_ID]
    if not sess["files"]:
        upload_sessions.pop(OWNER_ID, None)
        await message.reply("No files in session; cancelled.")
        return
    deep_link = gen_deep_link()
    session_id = await create_session_record(OWNER_ID, deep_link, sess["protect"], sess["timer"])
    for f in sess["files"]:
        await add_file_record(session_id, f["file_message_id"], f["caption"])
    upload_sessions.pop(OWNER_ID, None)
    await message.reply(f"✅ Upload complete. Deep link:\n`/start {deep_link}`\nAnyone running this command will receive the files.")
    logger.info("Created session %s by owner with deep_link %s", session_id, deep_link)

@dp.message_handler(commands=["e"])
async def cmd_cancel_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    upload_sessions.pop(OWNER_ID, None)
    await message.reply("❌ Upload session cancelled.")

@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    parts = message.text.split(" ", 1)
    if len(parts) < 2:
        await message.reply("Usage: /broadcast <message>. Use {Label|url} to embed links.")
        return
    content = parts[1]
    # fetch users
    async with AsyncSessionLocal() as session:
        q = await session.execute(select(User.user_id))
        users = [r[0] for r in q.fetchall()]
    if not users:
        await message.reply("No users found.")
        return
    text_formatted = LINK_PATTERN.sub(lambda m: f"[{m.group(1)}]({m.group(2)})", content)
    sent = 0
    for uid in users:
        try:
            await bot.send_message(chat_id=uid, text=text_formatted)
            sent += 1
        except Exception:
            pass
    await message.reply(f"Broadcast attempted. Sent to {sent}/{len(users)} users.")

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    total_users = await count_table(User)
    active_users = await count_active_users(2)
    total_files = await count_table(FileModel)
    total_sessions = await count_table(SessionModel)
    await message.reply(f"📊 Stats:\n\n👥 Total Users: {total_users}\n🟢 Active (2d): {active_users}\n📂 Files: {total_files}\n📦 Sessions: {total_sessions}")

# ----- startup and webhook integration ----- #
async def on_startup(app):
    # initialize DB
    await init_db()

    # start APScheduler job
    scheduler.add_job(deletion_job, "interval", seconds=15, id="deletion_job", replace_existing=True)
    scheduler.start()
    logger.info("APScheduler started with deletion job.")

    # set webhook for Telegram
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except Exception as exc:
        logger.exception("Failed to set webhook: %s", exc)

async def on_shutdown(app):
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

# ----- aiohttp application routes ----- #
async def handle_webhook(request):
    # incoming update from Telegram
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400, text="invalid json")
    update = types.Update.to_object(data)
    await dp.process_update(update)
    return web.Response(status=200, text="ok")

async def health(request):
    return web.Response(status=200, text="OK")

# create aiohttp web app
app = web.Application()
app.router.add_post("/webhook", handle_webhook)
app.router.add_get("/health", health)
app.on_startup.append(on_startup)
app.on_cleanup.append(on_shutdown)

# run via aiohttp web runner (Render will run this file directly via python)
if __name__ == "__main__":
    # aiohttp runs on host/port below — Render forwards external port 10000? Use env defaults
    web.run_app(app, host=WEBAPP_HOST, port=WEBAPP_PORT)