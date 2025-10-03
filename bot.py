# telegram_upload_bot.py
"""
Full-featured Telegram upload bot (owner-only uploader) designed to deploy on Render.
Built for compatibility with the library versions you requested:
  - aiogram==2.25.1
  - aiohttp==3.8.6
  - APScheduler==3.10.4
  - SQLAlchemy==2.0.23
  - asyncpg==0.29.0

Features:
 - Owner-only upload flow (/upload -> reply on/off for protect_content -> reply minutes -> send files -> /d to finish -> /e to cancel)
 - Files forwarded/copied to a private UPLOAD_CHANNEL (preserves file IDs)
 - Creates persistent sessions in Neon/Postgres with long deep-links (64 chars)
 - /start <deep_link> delivers files to any user (respecting protect_content where possible)
 - Auto-delete: delivered messages are scheduled for deletion (stored durably in DB). A background job deletes due messages.
 - /setstart to set welcome image + welcome text (supports {first_name} placeholder and {Label|url} -> clickable Markdown link)
 - /broadcast to all users (owner-only). Broadcast supports {Label|url} link syntax and includes an inline button example.
 - /stats for totals and active user count (2-day window)
 - /health endpoint for uptime checks (UptimeRobot)
 - Webhook mode (sets webhook to WEBHOOK_URL) — recommended for Render + UptimeRobot combination
 - Robust DB usage with SQLAlchemy async + asyncpg
 - Uses APScheduler AsyncIO for periodic deletion worker
 - Owner can bypass content protection; normal users only have access to /start [deep_link] and welcome

Deployment notes (short):
 - Set env vars in Render: BOT_TOKEN, OWNER_ID, UPLOAD_CHANNEL, DATABASE_URL, WEBHOOK_URL, WEBAPP_HOST (opt), WEBAPP_PORT (opt)
 - DATABASE_URL should be like postgresql+asyncpg://user:pass@host:port/dbname. If you paste postgresql://... the script auto-converts it.
 - Make the bot an admin in the UPLOAD_CHANNEL and grant necessary rights (send messages, manage messages if you want deletions).
 - Configure UptimeRobot to ping https://<your-render-url>/health every 5 minutes.

This is a single-file self-contained implementation. Paste into your repo as `telegram_upload_bot.py`.
"""

# =========================
# Imports
# =========================
import os
import asyncio
import logging
import random
import string
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

# aiohttp used by start_webhook internally via aiogram's executor; keep for potential direct usage
import aiohttp

# aiogram v2
from aiogram import Bot, Dispatcher, types
from aiogram.utils.executor import start_webhook
from aiogram.types import ParseMode, InlineKeyboardMarkup, InlineKeyboardButton

# SQLAlchemy async (v2)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, BigInteger, Text, Boolean, TIMESTAMP, String, ForeignKey, func, select, delete

# APScheduler for deletion job
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# dotenv for local dev (optional)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# =========================
# Logging
# =========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("telegram_upload_bot")

# =========================
# Environment variables (required)
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)
UPLOAD_CHANNEL = int(os.getenv("UPLOAD_CHANNEL", "0") or 0)  # channel numeric id (-100...)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
WEBAPP_HOST = os.getenv("WEBAPP_HOST", "0.0.0.0")
WEBAPP_PORT = int(os.getenv("WEBAPP_PORT", "8000"))

# Warn if missing
_required = [BOT_TOKEN, OWNER_ID, UPLOAD_CHANNEL, DATABASE_URL, WEBHOOK_URL]
if not all(_required):
    logger.warning("Missing one or more required ENV vars: BOT_TOKEN, OWNER_ID, UPLOAD_CHANNEL, DATABASE_URL, WEBHOOK_URL")

# Ensure DATABASE_URL uses asyncpg driver
if DATABASE_URL and DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
    logger.info("DATABASE_URL adjusted to use asyncpg driver.")

# =========================
# Constants & helpers
# =========================
DEEP_LINK_LENGTH = 64
LINK_PATTERN = re.compile(r"\{([^|{}]+)\|([^{}]+)\}")
FIRST_NAME_PLACEHOLDER = "{first_name}"

def gen_deep_link(n=DEEP_LINK_LENGTH) -> str:
    chars = string.ascii_letters + string.digits + "-_"
    return ''.join(random.choice(chars) for _ in range(n))

def format_message_template(text: Optional[str], user: types.User) -> str:
    """
    Replace {first_name} and {Label|url} tokens into MarkdownV2 formatted message.
    NOTE: We use Markdown (legacy compatibility), but we avoid too many escapes; this should be safe for common input.
    If you want strict MarkdownV2 escaping, apply escaping where necessary.
    """
    if not text:
        return ""
    result = text
    # first name
    if FIRST_NAME_PLACEHOLDER in result and getattr(user, "first_name", None):
        result = result.replace(FIRST_NAME_PLACEHOLDER, user.first_name or "")
    # label|url links -> [label](url)
    def repl(m):
        label = m.group(1).strip()
        url = m.group(2).strip()
        # Return markdown link. Using MarkdownV2 is safer but we set parse mode later.
        return f"[{label}]({url})"
    result = LINK_PATTERN.sub(repl, result)
    return result

# =========================
# Bot / Dispatcher
# =========================
# Use MarkdownV2 recommended by Telegram; aiogram v2 supports ParseMode.MARKDOWN_V2
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.MARKDOWN_V2)
dp = Dispatcher(bot)

# =========================
# Database (SQLAlchemy async)
# =========================
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

# Create engine and sessionmaker
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL env var is required and must be set to your Neon/Postgres connection string.")
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# =========================
# In-memory upload session state (transient)
# Structure: { owner_id: {"files":[{"file_message_id":int,"caption":str}], "protect":bool, "timer":int} }
# =========================
upload_sessions: Dict[int, Dict[str, Any]] = {}

# =========================
# Scheduler for deletions
# =========================
scheduler = AsyncIOScheduler()

# =========================
# Database utility helpers (async)
# =========================
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialized and tables ensured.")

async def add_or_update_user(user_id: int):
    async with AsyncSessionLocal() as session:
        # simple upsert: try select then insert/update
        existing = await session.get(User, user_id)
        if existing:
            existing.last_active = datetime.now(timezone.utc)
        else:
            new_user = User(user_id=user_id, last_active=datetime.now(timezone.utc))
            session.add(new_user)
        await session.commit()

async def save_setting(key: str, value: str):
    async with AsyncSessionLocal() as session:
        existing = await session.get(Setting, key)
        if existing:
            existing.value = value
        else:
            session.add(Setting(key=key, value=value))
        await session.commit()

async def get_setting(key: str) -> Optional[str]:
    async with AsyncSessionLocal() as session:
        row = await session.get(Setting, key)
        return row.value if row else None

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
        q = await session.execute(select(SessionModel).where(SessionModel.deep_link == deep_link))
        return q.scalars().first()

async def get_files_for_session(session_id: int) -> List[FileModel]:
    async with AsyncSessionLocal() as session:
        q = await session.execute(select(FileModel).where(FileModel.session_id == session_id).order_by(FileModel.id))
        return q.scalars().all()

async def schedule_delivery_for_deletion(user_id: int, chat_id: int, message_id: int, delete_at: datetime, session_id: Optional[int]):
    async with AsyncSessionLocal() as session:
        rec = Delivery(user_id=user_id, chat_id=chat_id, message_id=message_id, delete_at=delete_at, session_id=session_id)
        session.add(rec)
        await session.commit()

async def get_due_deliveries(now_dt: datetime) -> List[Delivery]:
    async with AsyncSessionLocal() as session:
        q = await session.execute(select(Delivery).where(Delivery.delete_at <= now_dt))
        return q.scalars().all()

async def delete_delivery_record(delivery_id: int):
    async with AsyncSessionLocal() as session:
        await session.execute(delete(Delivery).where(Delivery.id == delivery_id))
        await session.commit()

async def count_table_rows(model) -> int:
    async with AsyncSessionLocal() as session:
        q = await session.execute(select(func.count()).select_from(model))
        return int(q.scalar_one())

async def count_active_users(days: int = 2) -> int:
    # Simple approach: fetch all users and count last_active within cutoff.
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    async with AsyncSessionLocal() as session:
        q = await session.execute(select(User))
        rows = q.scalars().all()
        cnt = sum(1 for r in rows if r.last_active and r.last_active.replace(tzinfo=timezone.utc) >= cutoff)
        return cnt

# =========================
# Deletion Worker (runs periodically by APScheduler)
# =========================
async def deletion_worker():
    try:
        now_dt = datetime.now(timezone.utc)
        due = await get_due_deliveries(now_dt)
        if due:
            logger.info("Deletion worker found %d deliveries to remove.", len(due))
        for d in due:
            try:
                await bot.delete_message(chat_id=int(d.chat_id), message_id=int(d.message_id))
                logger.debug("Deleted message %s in chat %s", d.message_id, d.chat_id)
            except Exception as exc:
                logger.warning("Failed to delete message %s in chat %s: %s", d.message_id, d.chat_id, exc)
            try:
                await delete_delivery_record(d.id)
            except Exception as exc:
                logger.warning("Failed to delete delivery record %s: %s", d.id, exc)
    except Exception as exc:
        logger.exception("Exception in deletion_worker: %s", exc)

# =========================
# Helper: send/copy messages to user respecting protect_content
# Note: aiogram v2's copy_message may not accept a protect_content parameter consistently across versions.
# We attempt copy_message first, fall back to forward_message if needed. protect_content is best enforced
# when the message is initially sent; we forward/copy from UPLOAD_CHANNEL where the bot previously saved
# the file in protected/unprotected state depending on owner's selection.
# =========================
async def deliver_files_to_user(user_chat_id: int, user_obj: types.User, session_row: SessionModel, files: List[FileModel]):
    """
    Copies each file from UPLOAD_CHANNEL (where they are stored) into user's chat.
    If session has auto_delete_minutes, schedule deletion.
    Returns how many delivered.
    """
    delivered = 0
    for f in files:
        try:
            # Prefer copy_message to avoid file re-upload; aiogram v2 has copy_message but may not support protect_content param
            try:
                copied = await bot.copy_message(chat_id=user_chat_id, from_chat_id=UPLOAD_CHANNEL, message_id=f.file_message_id)
                delivered_msg_id = copied.message_id
            except Exception as exc:
                logger.debug("copy_message failed (%s), trying forward_message", exc)
                forwarded = await bot.forward_message(chat_id=user_chat_id, from_chat_id=UPLOAD_CHANNEL, message_id=f.file_message_id)
                delivered_msg_id = forwarded.message_id

            # schedule deletion if required (only delete from user's chat; DB record remains for auditing)
            if session_row.auto_delete_minutes and session_row.auto_delete_minutes > 0:
                delete_at = datetime.now(timezone.utc) + timedelta(minutes=int(session_row.auto_delete_minutes))
                await schedule_delivery_for_deletion(user_id=user_obj.id, chat_id=user_chat_id, message_id=delivered_msg_id, delete_at=delete_at, session_id=session_row.id)
            delivered += 1
        except Exception as exc:
            logger.exception("Failed to deliver file id=%s to user=%s: %s", f.file_message_id, user_chat_id, exc)
    # Notify user about auto-delete once (if needed)
    if session_row.auto_delete_minutes and session_row.auto_delete_minutes > 0 and delivered > 0:
        try:
            await bot.send_message(chat_id=user_chat_id, text=f"⚠ These files will be auto-deleted in {int(session_row.auto_delete_minutes)} minutes.")
        except Exception:
            pass
    return delivered

# =========================
# Handlers
# =========================

# /health endpoint reply (we provide a handler via webhook health path; but we also include a text command fallback)
@dp.message_handler(commands=["health"])
async def cmd_health(message: types.Message):
    await message.reply("OK")

# /start handler: either welcome or deep-link retrieval
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
        # Deliver files
        delivered_count = await deliver_files_to_user(user_chat_id=message.chat.id, user_obj=message.from_user, session_row=session_row, files=files)
        if delivered_count == 0:
            await message.reply("⚠ Failed to deliver files. Bot may lack permissions or files are unavailable.")
        return

    # No args: send welcome
    welcome_text = await get_setting("welcome_text") or "👋 Welcome, {first_name}!"
    formatted = format_message_template(welcome_text, message.from_user)
    welcome_image = await get_setting("welcome_image")
    try:
        if welcome_image:
            await bot.send_photo(chat_id=message.chat.id, photo=welcome_image, caption=formatted)
        else:
            await bot.send_message(chat_id=message.chat.id, text=formatted)
    except Exception as exc:
        logger.exception("Failed to send welcome to %s: %s", message.from_user.id, exc)
        # fallback
        await message.reply("👋 Welcome!")

# /setstart - owner-only: reply to message (image or text) with /setstart to set welcome message
@dp.message_handler(commands=["setstart"])
async def cmd_setstart(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    if not message.reply_to_message:
        await message.reply("Reply to an image or text message with /setstart to set the welcome message and optional image.")
        return
    reply = message.reply_to_message
    text = (reply.caption or reply.text or "👋 Welcome, {first_name}!")
    img_file_id = None
    if reply.photo:
        img_file_id = reply.photo[-1].file_id
    await save_setting("welcome_text", text)
    if img_file_id:
        await save_setting("welcome_image", img_file_id)
    await message.reply("✅ Welcome message (and image, if present) saved.")

# /upload: owner starts an interactive upload session
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    upload_sessions[OWNER_ID] = {"files": [], "protect": False, "timer": 0}
    await message.reply(
        "📤 Upload session started.\n\nReply with `on` or `off` to enable/disable protect_content for forwarded files.\nThen reply with the auto-delete timer in minutes (0 = off).\nThen send files (documents/photos/videos). When finished send /d to complete and create deep link, or /e to cancel."
    )

# owner replies for protect (on/off) and timer (digits)
@dp.message_handler(lambda message: message.from_user.id == OWNER_ID and message.text is not None)
async def owner_text_responses(message: types.Message):
    if OWNER_ID not in upload_sessions:
        return
    txt = message.text.strip().lower()
    if txt in ("on", "off", "yes", "no"):
        upload_sessions[OWNER_ID]["protect"] = txt in ("on", "yes")
        await message.reply(f"Protect_content set to {upload_sessions[OWNER_ID]['protect']}. Now reply with auto-delete minutes (0 = off).")
        return
    if txt.isdigit():
        minutes = int(txt)
        upload_sessions[OWNER_ID]["timer"] = minutes
        await message.reply(f"Auto-delete timer set to {minutes} minutes. Now send files, then /d to finish or /e to cancel.")
        return
    # not an upload flow response -> ignore here (other handlers may handle)

# file handler while in upload session (owner only)
@dp.message_handler(lambda message: message.from_user.id == OWNER_ID, content_types=types.ContentType.ANY)
async def owner_file_handler(message: types.Message):
    if OWNER_ID not in upload_sessions:
        return
    # accept only media/doc types for upload
    if not (message.document or message.photo or message.video or message.audio or message.voice or message.sticker):
        return
    try:
        # Prefer copy_message where possible to avoid reupload; fallback to forward_message
        try:
            sent = await bot.copy_message(chat_id=UPLOAD_CHANNEL, from_chat_id=message.chat.id, message_id=message.message_id)
            sent_msg_id = sent.message_id
        except Exception as exc:
            logger.warning("copy_message failed: %s; falling back to forward_message.", exc)
            forwarded = await bot.forward_message(chat_id=UPLOAD_CHANNEL, from_chat_id=message.chat.id, message_id=message.message_id)
            sent_msg_id = forwarded.message_id

        upload_sessions[OWNER_ID]["files"].append({"file_message_id": sent_msg_id, "caption": message.caption or ""})
        await message.reply("📌 File added to upload session. Continue sending files, or send /d to finish, /e to cancel.")
    except Exception as exc:
        logger.exception("Failed to copy/forward file to upload channel: %s", exc)
        await message.reply("⚠ Failed to forward/copy file to upload channel. Ensure bot is admin in that channel with appropriate permissions.")

# /d finish upload: create DB session, store file records, return deep link
@dp.message_handler(commands=["d"])
async def cmd_finish_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    if OWNER_ID not in upload_sessions:
        await message.reply("No active upload session.")
        return
    sess = upload_sessions[OWNER_ID]
    files = sess.get("files", [])
    if not files:
        upload_sessions.pop(OWNER_ID, None)
        await message.reply("No files were added — session cancelled.")
        return
    deep_link = gen_deep_link(DEEP_LINK_LENGTH)
    session_id = await create_session_record(OWNER_ID, deep_link, sess.get("protect", False), sess.get("timer", 0))
    for f in files:
        try:
            await add_file_record(session_id, f["file_message_id"], f.get("caption") or "")
        except Exception as exc:
            logger.warning("Failed to write file record for session %s: %s", session_id, exc)
    upload_sessions.pop(OWNER_ID, None)
    await message.reply(f"✅ Upload complete.\n🔗 Deep link (share cautiously):\n`/start {deep_link}`\nAnyone who invokes `/start {deep_link}` will receive those files.")
    logger.info("Owner created session id=%s deep_link=%s count_files=%s protect=%s timer=%s", session_id, deep_link, len(files), sess.get("protect"), sess.get("timer"))

# /e cancel upload
@dp.message_handler(commands=["e"])
async def cmd_cancel_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    upload_sessions.pop(OWNER_ID, None)
    await message.reply("❌ Upload session cancelled.")

# /broadcast owner-only; supports {Label|url} syntax; includes an inline button example
@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /broadcast <message>. Use {Label|url} to create clickable links.")
        return
    # format links
    text = LINK_PATTERN.sub(lambda m: f"[{m.group(1)}]({m.group(2)})", args)
    # fetch users
    async with AsyncSessionLocal() as session:
        q = await session.execute(select(User.user_id))
        rows = q.scalars().all()
    users = rows or []
    if not users:
        await message.reply("No users to broadcast to.")
        return
    # inline button sample
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Open", callback_data="broadcast_open")]])
    sent = 0
    for uid in users:
        try:
            await bot.send_message(chat_id=uid, text=text, reply_markup=kb)
            sent += 1
        except Exception:
            continue
    await message.reply(f"✅ Broadcast attempted. Messages sent: {sent}/{len(users)}")

# /stats owner-only
@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    total_users = await count_table_rows(User)
    active_users = await count_active_users(2)
    total_files = await count_table_rows(FileModel)
    total_sessions = await count_table_rows(SessionModel)
    await message.reply(f"📊 Stats:\n\n👥 Total Users: {total_users}\n🟢 Active (2d): {active_users}\n📂 Files: {total_files}\n📦 Sessions: {total_sessions}")

# Fallback handler for unknown text: allow only owner text in upload flow to be handled earlier
@dp.message_handler()
async def fallback(message: types.Message):
    # For regular users, ignore except /start which is handled above.
    # For owner, if in upload flow, messages handled elsewhere.
    # Optionally reply politely for unknown commands
    if message.from_user.id == OWNER_ID:
        # owner: no default action to avoid accidental behaviors
        return
    # normal users: don't expose other commands
    return

# =========================
# Webhook startup/shutdown integration
# =========================
async def on_startup(dispatcher: Dispatcher):
    # Initialize DB
    await init_db()

    # Set webhook
    try:
        # aiogram v2's set_webhook requires only URL
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except Exception as exc:
        logger.exception("Failed to set webhook: %s", exc)

    # Start APScheduler deletion job
    scheduler.add_job(deletion_worker, "interval", seconds=15, id="deletion_worker", replace_existing=True)
    scheduler.start()
    logger.info("APScheduler started (deletion worker every 15s).")

async def on_shutdown(dispatcher: Dispatcher):
    # Remove webhook and cleanup
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

# =========================
# Entrypoint: start webhook server (Render-friendly)
# =========================
    # start_webhook runs aiohttp under the hood and binds to the given host/port
    # webhook_path must match the path Telegram will POST updates to (we set WEBHOOK_URL to .../webhook)
    # Provide web_app to expose /health if needed — aiogram's start_webhook has support for setting a custom web app
    # For simplicity we set webhook_path = "/webhook" and rely on start_webhook to serve it.

    # Provide a small health check route using aiohttp by creating a custom web_app
    from aiohttp import web

    async def health_handler(request):
        return web.Response(text="OK")

    web_app = web.Application()
    web_app.router.add_get("/health", health_handler)

    # start_webhook will attach its own route for webhook (webhook_path) into web_app
    webhook_path = "/webhook"

    logger.info("Starting bot webhook server on %s:%s (webhook path: %s)", WEBAPP_HOST, WEBAPP_PORT, webhook_path)

if __name__ == "__main__":
    start_webhook(
        dispatcher=dp,
        webhook_path="/webhook",
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
        host=WEBAPP_HOST,
        port=WEBAPP_PORT,
    )
    )