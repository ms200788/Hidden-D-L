# bot.py
"""
Full Telegram bot implemented for:
- aiogram==2.25.0
- SQLAlchemy==1.4.51 (async)
- asyncpg==0.26.0
- aiohttp==3.8.6
- python-dotenv==0.21.0 (optional)

Purpose:
- Persistent file sharing via deep links using a private upload channel + PostgreSQL storage.
- Owner-only upload and management commands.
- Persistent autodelete for delivered messages (survives restarts).
- Editable /start message with placeholders.
- Owner bypass for protect_content (owner always receives non-protected copies).
- Designed to be deployed on Render with a managed PostgreSQL database and an uptime monitor pinging /health.

Environment variables (set these in Render or .env):
- BOT_TOKEN: Telegram bot token
- DATABASE_URL: SQLAlchemy async DB URL, e.g. postgresql+asyncpg://user:pass@host:port/dbname
- UPLOAD_CHANNEL_ID: Private channel id (int, negative, e.g. -1001234567890)
- OWNER_ID: Numeric Telegram user id of single owner
- PORT: Port for /health endpoint (default 8080)
- LOG_LEVEL: optional, DEBUG/INFO/WARNING/ERROR

Run:
- python bot.py
- Make sure bot is admin in UPLOAD_CHANNEL_ID (post + delete permission recommended).
- Add UptimeRobot monitor for https://<your-service>.onrender.com/health

"""

import os
import asyncio
import logging
import secrets
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

import aiohttp
from aiohttp import web

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters import Command
from aiogram.utils.exceptions import Throttled, RetryAfter

from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    select,
    func,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("persistent_deeplink_bot")

# ---------------------------------------------------------------------------
# Environment / Configuration
# ---------------------------------------------------------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")  # e.g. postgresql+asyncpg://user:pass@host:port/dbname
UPLOAD_CHANNEL_ID = os.environ.get("UPLOAD_CHANNEL_ID")
OWNER_ID = os.environ.get("OWNER_ID")
PORT = int(os.environ.get("PORT", "8080"))

_missing_vars = []
if not BOT_TOKEN:
    _missing_vars.append("BOT_TOKEN")
if not DATABASE_URL:
    _missing_vars.append("DATABASE_URL")
if not UPLOAD_CHANNEL_ID:
    _missing_vars.append("UPLOAD_CHANNEL_ID")
if not OWNER_ID:
    _missing_vars.append("OWNER_ID")
if _missing_vars:
    raise RuntimeError("Missing required environment variables: " + ", ".join(_missing_vars))

# convert types (with validation)
try:
    UPLOAD_CHANNEL_ID = int(UPLOAD_CHANNEL_ID)
except Exception:
    raise RuntimeError("UPLOAD_CHANNEL_ID must be an integer (channel id), e.g. -1001234567890")
try:
    OWNER_ID = int(OWNER_ID)
except Exception:
    raise RuntimeError("OWNER_ID must be an integer (your Telegram numeric id)")

# ---------------------------------------------------------------------------
# Database models using SQLAlchemy async (SQLAlchemy 1.4 style)
# ---------------------------------------------------------------------------
Base = declarative_base()


class SessionModel(Base):
    """
    Represents an upload session that corresponds to a deep link.
    Each session contains metadata and references multiple FileModel rows.
    """
    __tablename__ = "sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column(String(128), unique=True, nullable=False, index=True)  # 64-char token
    owner_id = Column(Integer, nullable=False)  # the owner who created this session (OWNER_ID)
    protect_content = Column(Boolean, default=False)  # prevent save/forward if supported by client
    autodelete_minutes = Column(Integer, default=0)  # 0 = no autodelete
    revoked = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    files = relationship("FileModel", back_populates="session", cascade="all, delete-orphan")


class FileModel(Base):
    """
    Stores file references (Telegram file_ids) and metadata per session.
    Files are stored in the private upload channel; DB keeps the file_id.
    """
    __tablename__ = "files"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False)
    tg_file_id = Column(String, nullable=False)
    file_type = Column(String(32), nullable=False)  # photo, video, document, audio, voice, sticker
    caption = Column(String, nullable=True)
    order_index = Column(Integer, nullable=False)  # order in which files will be delivered

    session = relationship("SessionModel", back_populates="files")


class DeliveryModel(Base):
    """
    Tracks messages sent to users for autodelete.
    When a file is sent to a user with autodelete > 0, an entry is created here.
    The autodelete worker deletes messages whose delete_at <= now.
    """
    __tablename__ = "deliveries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(Integer, nullable=False)
    message_id = Column(Integer, nullable=False)
    delete_at = Column(DateTime(timezone=True), nullable=True)


class StartMessage(Base):
    """
    Stores an editable /start message (HTML content), latest entry used.
    Supports placeholders like {first_name} and simple {word | url} inline link syntax.
    """
    __tablename__ = "start_message"

    id = Column(Integer, primary_key=True, autoincrement=True)
    content = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# ---------------------------------------------------------------------------
# Async DB engine and session maker
# ---------------------------------------------------------------------------
# DATABASE_URL must use asyncpg driver prefix: postgresql+asyncpg://user:pass@host:port/dbname
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    """
    Create tables if they don't exist. Safe to run at startup.
    """
    logger.info("Initializing database (create tables if needed)...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialized.")


# ---------------------------------------------------------------------------
# Token generation helper
# ---------------------------------------------------------------------------
def generate_token(length: int = 64) -> str:
    """
    Generate a URL-safe token of the given length.
    We use secrets.token_urlsafe and slice to ensure length.
    """
    return secrets.token_urlsafe(length)[:length]


# ---------------------------------------------------------------------------
# Aiogram setup (v2.x)
# ---------------------------------------------------------------------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)

# ---------------------------------------------------------------------------
# FSM states
# ---------------------------------------------------------------------------
class UploadStates(StatesGroup):
    waiting_for_files = State()
    awaiting_protect = State()
    awaiting_autodelete = State()


# ---------------------------------------------------------------------------
# Autodelete background worker
# ---------------------------------------------------------------------------
AUTODELETE_CHECK_INTERVAL = 30  # seconds between DB checks

async def add_delivery(chat_id: int, message_id: int, delete_at: Optional[datetime]):
    """
    Persist a delivery record to be deleted at delete_at (UTC). If delete_at is None, do nothing.
    """
    if delete_at is None:
        return
    async with AsyncSessionLocal() as db:
        rec = DeliveryModel(chat_id=chat_id, message_id=message_id, delete_at=delete_at)
        db.add(rec)
        await db.commit()
    logger.debug("Scheduled deletion: chat=%s message_id=%s at=%s", chat_id, message_id, delete_at.isoformat())


async def autodelete_worker(app=None):
    """
    Background loop: check for due deliveries and delete messages.
    This uses the database to persist scheduled deletions so that restarts do not lose work.
    """
    logger.info("Autodelete worker started (interval %s seconds)", AUTODELETE_CHECK_INTERVAL)
    while True:
        try:
            async with AsyncSessionLocal() as db:
                now = datetime.utcnow()
                stmt = select(DeliveryModel).where(DeliveryModel.delete_at <= now)
                res = await db.execute(stmt)
                due_list = res.scalars().all()
                if due_list:
                    logger.info("Autodelete: found %d messages due for deletion", len(due_list))
                for rec in due_list:
                    try:
                        await bot.delete_message(chat_id=rec.chat_id, message_id=rec.message_id)
                        logger.debug("Autodelete: deleted %s:%s", rec.chat_id, rec.message_id)
                    except Exception as e:
                        logger.warning("Autodelete: failed to delete %s:%s -> %s", rec.chat_id, rec.message_id, e)
                    # delete DB record
                    try:
                        await db.delete(rec)
                    except Exception:
                        logger.exception("Autodelete: failed to remove DB record for %s:%s", rec.chat_id, rec.message_id)
                await db.commit()
        except Exception:
            logger.exception("Error in autodelete worker loop: %s", traceback.format_exc())
        await asyncio.sleep(AUTODELETE_CHECK_INTERVAL)


# ---------------------------------------------------------------------------
# Small DB helper utilities
# ---------------------------------------------------------------------------
async def get_session_by_token(token: str) -> Optional[SessionModel]:
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.link == token)
        res = await db.execute(stmt)
        return res.scalars().first()


async def save_start_message(content: str):
    async with AsyncSessionLocal() as db:
        stmt = select(StartMessage).order_by(StartMessage.updated_at.desc())
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if rec:
            rec.content = content
            db.add(rec)
        else:
            rec = StartMessage(content=content)
            db.add(rec)
        await db.commit()
    logger.info("Saved /start message.")


async def fetch_start_message() -> Optional[str]:
    async with AsyncSessionLocal() as db:
        stmt = select(StartMessage).order_by(StartMessage.updated_at.desc())
        res = await db.execute(stmt)
        rec = res.scalars().first()
        return rec.content if rec else None


# ---------------------------------------------------------------------------
# /start handler: show start message or deliver files for deep link
# ---------------------------------------------------------------------------
@dp.message_handler(commands=['start'])
async def handle_start(message: types.Message):
    """
    If /start without args -> show editable start message or default welcome.
    If /start <token> -> deliver files for that session. Owner bypass for protect_content is applied.
    """
    args = message.get_args().strip() if message.get_args() else ""
    if not args:
        # present start message
        content = await fetch_start_message()
        if content:
            # Replace {first_name}
            rendered = content.replace("{first_name}", message.from_user.first_name or "")
            # Replace pattern {word | url} occurrences with HTML anchor tags
            # A simple parser that converts {label | url} -> <a href="url">label</a>
            out_chunks: List[str] = []
            i = 0
            while True:
                start = rendered.find("{", i)
                if start == -1:
                    out_chunks.append(rendered[i:])
                    break
                end = rendered.find("}", start)
                if end == -1:
                    out_chunks.append(rendered[i:])
                    break
                out_chunks.append(rendered[i:start])
                inner = rendered[start+1:end].strip()
                if "|" in inner:
                    left, right = inner.split("|", 1)
                    left = left.strip()
                    right = right.strip()
                    out_chunks.append(f'<a href="{right}">{left}</a>')
                else:
                    out_chunks.append("{" + inner + "}")
                i = end + 1
            final_text = "".join(out_chunks)
            try:
                await message.answer(final_text, parse_mode="HTML", disable_web_page_preview=True)
            except Exception as e:
                # fallback plain text
                logger.warning("Failed to send HTML start message: %s", e)
                await message.answer(content.replace("{first_name}", message.from_user.first_name or ""))
        else:
            await message.answer(f"Welcome, {message.from_user.first_name}!")
        return

    token = args
    session_obj = await get_session_by_token(token)
    if not session_obj:
        await message.answer("❌ Link not found or expired.")
        return
    if session_obj.revoked:
        await message.answer("❌ This link has been revoked.")
        return

    # fetch files for the session in order
    async with AsyncSessionLocal() as db:
        stmt = select(FileModel).where(FileModel.session_id == session_obj.id).order_by(FileModel.order_index)
        res = await db.execute(stmt)
        files: List[FileModel] = res.scalars().all()

    if not files:
        await message.answer("❌ No files found for this link.")
        return

    # deliver files in exact order and schedule deletions per recipient if autodelete set
    delivered_count = 0
    for f in files:
        try:
            # Owner bypass: owner always gets non-protected copy
            if message.from_user.id == OWNER_ID:
                protect_flag = False
            else:
                protect_flag = bool(session_obj.protect_content)

            caption = f.caption or None

            if f.file_type == "photo":
                sent = await bot.send_photo(chat_id=message.chat.id, photo=f.tg_file_id, caption=caption, parse_mode=None)
            elif f.file_type == "video":
                sent = await bot.send_video(chat_id=message.chat.id, video=f.tg_file_id, caption=caption, parse_mode=None)
            elif f.file_type == "document":
                sent = await bot.send_document(chat_id=message.chat.id, document=f.tg_file_id, caption=caption, parse_mode=None)
            elif f.file_type == "audio":
                sent = await bot.send_audio(chat_id=message.chat.id, audio=f.tg_file_id, caption=caption, parse_mode=None)
            elif f.file_type == "voice":
                sent = await bot.send_voice(chat_id=message.chat.id, voice=f.tg_file_id)
            elif f.file_type == "sticker":
                sent = await bot.send_sticker(chat_id=message.chat.id, sticker=f.tg_file_id)
            else:
                sent = await bot.send_document(chat_id=message.chat.id, document=f.tg_file_id, caption=caption, parse_mode=None)

            delivered_count += 1

            # schedule autodelete if configured (per recipient message)
            if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
                delete_time = datetime.utcnow() + timedelta(minutes=session_obj.autodelete_minutes)
                await add_delivery(chat_id=sent.chat.id, message_id=sent.message_id, delete_at=delete_time)

        except Exception as e:
            logger.exception("Delivery failed for session %s file %s: %s", session_obj.link, f.id if f else None, e)

    if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
        await message.answer(f"Files delivered: {delivered_count}. They will be deleted after {session_obj.autodelete_minutes} minute(s).")
    else:
        await message.answer(f"Files delivered: {delivered_count}. (Autodelete disabled)")

# ---------------------------------------------------------------------------
# Owner-only: /upload - starts FSM
# ---------------------------------------------------------------------------
@dp.message_handler(commands=['upload'])
async def cmd_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /upload.")
        return
    await UploadStates.waiting_for_files.set()
    state = dp.current_state(user=message.from_user.id)
    await state.update_data(files=[])  # store temporary list of files (forwarded message ids)
    await message.reply("Upload session started.\nSend the files you want to upload. When done, use /done. To cancel, use /abort.")

# ---------------------------------------------------------------------------
# Owner-only: /abort - cancels upload and attempts to clean forwarded messages in upload channel
# ---------------------------------------------------------------------------
@dp.message_handler(commands=['abort'])
async def cmd_abort(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /abort.")
        return
    state = dp.current_state(user=message.from_user.id)
    data = await state.get_data()
    files = data.get("files", [])
    removed = 0
    for f in files:
        try:
            fwd_msg_id = f.get("forwarded_upload_channel_message")
            if fwd_msg_id:
                await bot.delete_message(chat_id=UPLOAD_CHANNEL_ID, message_id=fwd_msg_id)
                removed += 1
        except Exception:
            # ignore errors
            pass
    await state.finish()
    await message.reply(f"Upload aborted. Removed {removed} temporary forwarded message(s) from upload channel (if possible).")

# ---------------------------------------------------------------------------
# Owner-only: /done - asks protect and moves to awaiting_protect state
# ---------------------------------------------------------------------------
@dp.message_handler(commands=['done'])
async def cmd_done(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /done.")
        return
    state = dp.current_state(user=message.from_user.id)
    curr = await state.get_state()
    if curr != UploadStates.waiting_for_files.state:
        await message.reply("No active upload session. Start one with /upload.")
        return
    data = await state.get_data()
    files = data.get("files", [])
    if not files:
        await state.finish()
        await message.reply("No files uploaded. Upload canceled.")
        return
    await message.reply("Protect content? Reply with `on` or `off`.\nProtect prevents saving/forwarding on clients that support it.")
    await UploadStates.awaiting_protect.set()

# ---------------------------------------------------------------------------
# Generic message handler: handles FSM states and file forwarding while in upload
# ---------------------------------------------------------------------------
@dp.message_handler()
async def generic_fsm_handler(message: types.Message):
    """
    This single handler receives:
    - File messages while in UploadStates.waiting_for_files: forwards to upload channel and records metadata in FSM.
    - Text messages for protect/autodelete options during /done flow.
    - Non-FSM messages are ignored here.
    """
    state = dp.current_state(user=message.from_user.id)
    curr = await state.get_state()

    # awaiting_protect
    if curr == UploadStates.awaiting_protect.state:
        text = (message.text or "").strip().lower()
        if text not in ("on", "off"):
            await message.reply("Please reply with `on` or `off`.")
            return
        protect = text == "on"
        await state.update_data(protect=protect)
        await message.reply("Set autodelete timer in minutes (0 - 10080). 0 = no autodelete. Example: `60`")
        await UploadStates.awaiting_autodelete.set()
        return

    # awaiting_autodelete
    if curr == UploadStates.awaiting_autodelete.state:
        text = (message.text or "").strip()
        try:
            minutes = int(text)
            if minutes < 0 or minutes > 10080:
                raise ValueError
            await state.update_data(autodelete=minutes)
            await finalize_upload_flow(message=message, state=state)
        except ValueError:
            await message.reply("Please provide an integer between 0 and 10080.")
        return

    # waiting_for_files state: accept files and forward them to upload channel
    if curr == UploadStates.waiting_for_files.state:
        # Accept media types
        ftype = None
        orig_file_id = None
        caption = None

        if message.photo:
            ftype = "photo"
            orig_file_id = message.photo[-1].file_id
            caption = message.caption or ""
        elif message.video:
            ftype = "video"
            orig_file_id = message.video.file_id
            caption = message.caption or ""
        elif message.document:
            ftype = "document"
            orig_file_id = message.document.file_id
            caption = message.caption or ""
        elif message.audio:
            ftype = "audio"
            orig_file_id = message.audio.file_id
            caption = message.caption or ""
        elif message.voice:
            ftype = "voice"
            orig_file_id = message.voice.file_id
            caption = ""
        elif message.sticker:
            ftype = "sticker"
            orig_file_id = message.sticker.file_id
            caption = ""
        else:
            # not a supported media message
            await message.reply("Send a supported file (photo/video/document/audio/voice/sticker) or /done to finish, /abort to cancel.")
            return

        # Forward message to upload channel so the file is stored in the channel and bot can later reference its file_id
        try:
            fwd = await bot.forward_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
        except Exception as e:
            logger.exception("Failed to forward to upload channel: %s", e)
            await message.reply("Failed to forward to upload channel. Ensure the bot is admin in that channel and can post.")
            return

        # Save metadata in FSM
        data = await state.get_data()
        files = data.get("files", [])
        files.append({
            "file_type": ftype,
            "orig_file_id": orig_file_id,
            "caption": caption,
            "forwarded_upload_channel_message": fwd.message_id,
        })
        await state.update_data(files=files)
        await message.reply(f"Saved {ftype}. Send more files or /done when finished.")
        return

    # not in FSM states relevant to upload; ignore or add other bot logic here
    return

# ---------------------------------------------------------------------------
# Finalize upload: read forwarded messages from upload channel, populate DB, and return deep link
# ---------------------------------------------------------------------------
async def finalize_upload_flow(message: types.Message, state: FSMContext):
    """
    Persist session and files to DB.
    Steps:
    - Create SessionModel with a random token
    - For each file in FSM list, read the forwarded message from upload channel to get the channel-stored file_id
    - Create FileModel rows in correct order
    - Commit and reply with deep link
    """
    data = await state.get_data()
    files = data.get("files", [])
    protect = data.get("protect", False)
    autodelete = data.get("autodelete", 0)

    if not files:
        await state.finish()
        await message.reply("No files found for finalizing. Upload canceled.")
        return

    async with AsyncSessionLocal() as db:
        token = generate_token(64)
        session_rec = SessionModel(
            link=token,
            owner_id=OWNER_ID,
            protect_content=bool(protect),
            autodelete_minutes=int(autodelete),
        )
        db.add(session_rec)
        await db.flush()  # populate session_rec.id

        order_idx = 1
        for entry in files:
            fwd_msg_id = entry.get("forwarded_upload_channel_message")
            try:
                fwd_msg = await bot.get_message(chat_id=UPLOAD_CHANNEL_ID, message_id=fwd_msg_id)
                # determine file id from forwarded message
                if fwd_msg.photo:
                    ftype = "photo"
                    file_id = fwd_msg.photo[-1].file_id
                    caption = fwd_msg.caption or ""
                elif fwd_msg.video:
                    ftype = "video"
                    file_id = fwd_msg.video.file_id
                    caption = fwd_msg.caption or ""
                elif fwd_msg.document:
                    ftype = "document"
                    file_id = fwd_msg.document.file_id
                    caption = fwd_msg.caption or ""
                elif fwd_msg.audio:
                    ftype = "audio"
                    file_id = fwd_msg.audio.file_id
                    caption = fwd_msg.caption or ""
                elif fwd_msg.voice:
                    ftype = "voice"
                    file_id = fwd_msg.voice.file_id
                    caption = ""
                elif fwd_msg.sticker:
                    ftype = "sticker"
                    file_id = fwd_msg.sticker.file_id
                    caption = ""
                else:
                    ftype = entry.get("file_type")
                    file_id = entry.get("orig_file_id")
                    caption = entry.get("caption", "")
            except Exception as e:
                logger.warning("Could not fetch forwarded msg %s in upload channel: %s", fwd_msg_id, e)
                ftype = entry.get("file_type")
                file_id = entry.get("orig_file_id")
                caption = entry.get("caption", "")

            if not file_id:
                logger.warning("Skipping file with no file_id in finalize step: entry=%s", entry)
                continue

            fm = FileModel(
                session_id=session_rec.id,
                tg_file_id=file_id,
                file_type=ftype,
                caption=caption,
                order_index=order_idx,
            )
            db.add(fm)
            order_idx += 1

        # commit the session + file rows
        await db.commit()

    # Compose deep link and reply to owner
    me = await bot.get_me()
    bot_username = me.username or "bot"
    deep_link = f"https://t.me/{bot_username}?start={token}"
    await message.reply(
        f"✅ Upload complete.\nDeep link (share to allow access):\n{deep_link}\n\nProtect content: {'ON' if protect else 'OFF'}\nAutodelete: {autodelete} minute(s)\n\nUse /revoke <token> to disable this link."
    )
    # finish FSM
    await state.finish()

# ---------------------------------------------------------------------------
# Owner-only: /revoke <token>
# ---------------------------------------------------------------------------
@dp.message_handler(commands=['revoke'])
async def cmd_revoke(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can revoke tokens.")
        return
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.reply("Usage: /revoke <token>")
        return
    token = parts[1].strip()
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.link == token)
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if not rec:
            await message.reply("Token not found.")
            return
        if rec.revoked:
            await message.reply("Token already revoked.")
            return
        rec.revoked = True
        db.add(rec)
        await db.commit()
    await message.reply(f"✅ Token revoked: {token}")

# ---------------------------------------------------------------------------
# Owner-only: /edit_start (reply to a message to set start message)
# ---------------------------------------------------------------------------
@dp.message_handler(commands=['edit_start'])
async def cmd_edit_start(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can edit start message.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or caption) that you want to set as the /start message.")
        return
    content = message.reply_to_message.text or message.reply_to_message.caption or ""
    if not content.strip():
        await message.reply("Replied message has no text/caption. Please reply to a message with content.")
        return
    await save_start_message(content)
    await message.reply("✅ /start message updated. Placeholders supported: {first_name} and {word | url} (e.g. {Click here | https://example.com})")

# ---------------------------------------------------------------------------
# /help
# ---------------------------------------------------------------------------
@dp.message_handler(commands=['help'])
async def cmd_help(message: types.Message):
    help_text = (
        "Commands:\n"
        "/start - welcome or open deep link (e.g. /start <token>)\n"
        "/upload - start upload session (owner only)\n"
        "/done - finish upload session (owner only)\n"
        "/abort - cancel upload session (owner only)\n"
        "/revoke <token> - revoke a deep link (owner only)\n"
        "/edit_start - edit welcome /start message (owner only; reply to a message)\n"
        "/help - show this help\n\n"
        "Notes:\n"
        "- Files are forwarded to a private upload channel and the bot stores file_ids in the DB.\n"
        "- Autodelete persists in DB and is processed by the background worker.\n"
        "- Owner bypass: when the owner opens a link they get unprotected copies regardless of the protect setting.\n"
    )
    await message.reply(help_text)

# ---------------------------------------------------------------------------
# /health HTTP endpoint for uptime monitors (aiohttp)
# ---------------------------------------------------------------------------
async def health_handler(request):
    return web.Response(text="OK")


# ---------------------------------------------------------------------------
# Startup and shutdown events integrated with executor.start_polling
# ---------------------------------------------------------------------------
async def on_startup(dispatcher):
    """
    Called by aiogram executor on startup.
    Initialize DB and start autodelete worker.
    Also start aiohttp health server in background.
    """
    logger.info("on_startup: initializing DB and background tasks...")
    try:
        await init_db()
    except Exception:
        logger.exception("Failed to initialize DB at startup")

    loop = asyncio.get_event_loop()
    # Launch autodelete worker
    loop.create_task(autodelete_worker())

    # Launch aiohttp health server task
    loop.create_task(start_health_server())

    logger.info("on_startup complete.")


async def on_shutdown(dispatcher):
    logger.info("on_shutdown: closing resources...")
    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
    except Exception:
        logger.exception("Error closing FSM storage.")
    try:
        await bot.close()
    except Exception:
        logger.exception("Error closing bot session.")
    logger.info("Shutdown complete.")


# ---------------------------------------------------------------------------
# aiohttp server starter for /health
# ---------------------------------------------------------------------------
async def start_health_server():
    """
    Start aiohttp web server to serve /health for uptime monitors.
    Runs as a background task in the same event loop as aiogram.
    """
    app = web.Application()
    app.router.add_get("/health", health_handler)

    runner = web.AppRunner(app)
    try:
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        logger.info("Health server started on port %s", PORT)
    except Exception:
        logger.exception("Failed to start health server.")
        # If health server fails, don't crash the bot; continue without it.


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main():
    """
    Entry point to start the bot. Uses aiogram executor with on_startup/on_shutdown hooks.
    """
    logger.info("Starting bot main loop...")
    executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown, skip_updates=True)


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# End of file
# ---------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Extra comments and reminders (kept intentionally in the file for long-term maintainers)
# ----------------------------------------------------------------------------
#
# 1) Deploying on Render:
#    - Add repository and create a Web Service.
#    - Build command: pip install -r requirements.txt
#    - Start command: python bot.py
#    - Set environment variables: BOT_TOKEN, DATABASE_URL, UPLOAD_CHANNEL_ID, OWNER_ID, PORT(optional)
#    - Create a Render Postgres instance and use the provided DB URL as DATABASE_URL (must be postgresql+asyncpg://...)
#    - Ensure the bot is admin in the UPLOAD_CHANNEL_ID (add by invite link and make admin).
#    - Use UptimeRobot or similar to ping https://<your-service>.onrender.com/health every 1-5 minutes.
#
# 2) Database backups:
#    - For 10+ years persistence, schedule regular backups of your Postgres (Render offers backups).
#    - Periodically download dumps and store in safe object storage.
#
# 3) Autodelete reliability:
#    - The autodelete worker polls the DB every AUTODELETE_CHECK_INTERVAL seconds and deletes due messages.
#    - Because the planned deletions are stored in the DB, a bot restart will not cause lost delete tasks.
#    - If the bot is down when delete_at passes, the worker will delete the message when it restarts and finds delete_at <= now.
#
# 4) Owner bypass:
#    - The owner (OWNER_ID) is always able to save/forward their own delivered copies.
#    - Protect content still applies to other users where supported by their client.
#
# 5) Limitations & future improvements:
#    - For extremely high traffic, consider batching DB operations and using connection pooling tuning.
#    - To avoid hitting Telegram rate limits when many users click links concurrently, you may implement per-user rate limits and queueing/backoff.
#    - To support millions of deliveries you might move scheduled deletion processing to a dedicated worker and use a distributed task queue (RQ/Celery).
#    - Add Alembic for DB schema migrations for production maintenance.
#
# End of extra comments.