# bot.py
"""
Telegram session-based file-sharing bot (webhook mode)
-----------------------------------------------------
Features implemented (as you requested):
- Aiogram v2 webhook-based bot (no polling)
- Works with PostgreSQL (async) via SQLAlchemy async + asyncpg
- Stores only Telegram file_ids in a private upload channel (low storage)
- Session-based deep links: random 64-char tokens (not sequential)
- Owner-only management commands: /upload, /done, /abort, /revoke, /edit_start, /help
- Public command: /start (and deep-link payload to fetch session files)
- Protect-content option per session (owner bypassed)
- Autodelete timer per session (0 = disabled); delivered copies deleted after X minutes
- Delivery scheduling persisted in DB so restarts don't lose pending deletes
- /health endpoint for uptime monitoring (same aiohttp server)
- Webhook path: /webhook/<BOT_TOKEN> — set on startup
- Handles concurrent user deliveries with a semaphore
- Auto-creates database tables on startup (convenient for Render)
- Robust logging and error handling
- Uses Bot.set_current(bot) + Dispatcher.set_current(dp) so message.answer/reply work in webhook context

Deployment notes:
- Environment variables required:
    BOT_TOKEN (Bot token from BotFather)
    OWNER_ID (your Telegram numeric id; default provided below)
    DATABASE_URL (must be something like: postgresql+asyncpg://user:pass@host:port/dbname)
    UPLOAD_CHANNEL_ID (private channel id where bot is admin; e.g. -1001234567890)
    WEBHOOK_HOST (public https host, e.g. https://hidden-fnxx.onrender.com)
    PORT (optional, default 10000)
    LOG_LEVEL (optional: DEBUG/INFO)
- Make sure bot is admin in UPLOAD_CHANNEL_ID and allowed to post & delete messages
- requirements.txt should include compatible versions:
    aiogram==2.25.0
    SQLAlchemy==1.4.51
    asyncpg==0.26.0
    aiohttp==3.8.6
    python-dotenv==0.21.0
    psycopg2-binary==2.9.9
"""

# Standard library imports
import os
import sys
import asyncio
import logging
import secrets
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

# Third-party imports
from aiohttp import web
from aiohttp.web_request import Request

from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage

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

# ------------------------------------------------------------
# Logging configuration
# ------------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("session_file_bot")

# ------------------------------------------------------------
# Environment variables and basic validation
# ------------------------------------------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID_ENV = os.environ.get("OWNER_ID", "6169237879")  # default owner id you supplied
DATABASE_URL = os.environ.get("DATABASE_URL")
UPLOAD_CHANNEL_ID_ENV = os.environ.get("UPLOAD_CHANNEL_ID")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # e.g. https://hidden-fnxx.onrender.com
PORT = int(os.environ.get("PORT", "10000"))
MAX_CONCURRENT_DELIVERIES = int(os.environ.get("MAX_CONCURRENT_DELIVERIES", "50"))

_missing = []
if not BOT_TOKEN:
    _missing.append("BOT_TOKEN")
if not OWNER_ID_ENV:
    _missing.append("OWNER_ID")
if not DATABASE_URL:
    _missing.append("DATABASE_URL")
if not UPLOAD_CHANNEL_ID_ENV:
    _missing.append("UPLOAD_CHANNEL_ID")
if not WEBHOOK_HOST:
    _missing.append("WEBHOOK_HOST")
if _missing:
    raise RuntimeError("Missing required environment variables: " + ", ".join(_missing))

try:
    OWNER_ID = int(OWNER_ID_ENV)
except Exception:
    raise RuntimeError("OWNER_ID must be an integer")

try:
    UPLOAD_CHANNEL_ID = int(UPLOAD_CHANNEL_ID_ENV)
except Exception:
    raise RuntimeError("UPLOAD_CHANNEL_ID must be an integer (e.g. -1001234567890)")

# Build webhook path and URL
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST.rstrip('/')}{WEBHOOK_PATH}"

logger.info("Starting with config: OWNER_ID=%s, UPLOAD_CHANNEL_ID=%s, WEBHOOK_HOST=%s, PORT=%s",
            OWNER_ID, UPLOAD_CHANNEL_ID, WEBHOOK_HOST, PORT)

# ------------------------------------------------------------
# Database models using SQLAlchemy (async)
# ------------------------------------------------------------
Base = declarative_base()


class SessionModel(Base):
    """
    Represents a session (a group of files uploaded together).
    link: random token (64 chars) used in deep link
    protect_content: whether recipients should not be able to forward/save (owner bypassed)
    autodelete_minutes: 0 means disabled; else messages delivered to recipients deleted after this many minutes
    revoked: if True, session cannot be used
    """
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column(String(128), unique=True, nullable=False, index=True)
    owner_id = Column(Integer, nullable=False)
    protect_content = Column(Boolean, default=False)
    autodelete_minutes = Column(Integer, default=0)
    revoked = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    files = relationship("FileModel", back_populates="session", cascade="all, delete-orphan")


class FileModel(Base):
    """
    Represents a file belonging to a session.
    tg_file_id is the Telegram file_id stored from the upload channel (stable)
    order_index ensures files are delivered in original order
    """
    __tablename__ = "files"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False)
    tg_file_id = Column(String, nullable=False)
    file_type = Column(String(32), nullable=False)
    caption = Column(String, nullable=True)
    order_index = Column(Integer, nullable=False)

    session = relationship("SessionModel", back_populates="files")


class DeliveryModel(Base):
    """
    Persist scheduled deletions for delivered messages so they survive restarts.
    """
    __tablename__ = "deliveries"
    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(Integer, nullable=False)
    message_id = Column(Integer, nullable=False)
    delete_at = Column(DateTime(timezone=True), nullable=True)


class StartMessage(Base):
    """
    Single-row table to store the /start welcome content (text or html-compatible text).
    """
    __tablename__ = "start_message"
    id = Column(Integer, primary_key=True, autoincrement=True)
    content = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# ------------------------------------------------------------
# Async SQLAlchemy engine & sessionmaker
# ------------------------------------------------------------
# DATABASE_URL must include asyncpg driver, e.g. postgresql+asyncpg://user:pass@host:port/dbname
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    """
    Ensure database tables exist. This is convenient for deployment without migrations.
    """
    logger.info("Initializing DB (creating tables if required)...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("DB initialized.")


# ------------------------------------------------------------
# Aiogram Bot and Dispatcher (v2)
# ------------------------------------------------------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)

# Semaphore to limit concurrent send operations
delivery_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DELIVERIES)

# ------------------------------------------------------------
# FSM states for upload flow
# ------------------------------------------------------------
class UploadStates(StatesGroup):
    waiting_for_files = State()
    awaiting_protect = State()
    awaiting_autodelete = State()


# ------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------
def generate_token(length: int = 64) -> str:
    """
    Generate a URL-safe token sliced to exact length.
    """
    return secrets.token_urlsafe(length)[:length]


def get_state_for_message(message: types.Message) -> FSMContext:
    """
    Return an FSM context bound to both chat and user (reliable in webhook context).
    """
    return dp.current_state(chat=message.chat.id, user=message.from_user.id)


# ------------------------------------------------------------
# Autodelete worker (background)
# ------------------------------------------------------------
AUTODELETE_CHECK_INTERVAL = 30  # seconds


async def add_delivery(chat_id: int, message_id: int, delete_at: Optional[datetime]):
    """
    Persist a scheduled deletion to the DB.
    """
    if delete_at is None:
        return
    async with AsyncSessionLocal() as db:
        rec = DeliveryModel(chat_id=chat_id, message_id=message_id, delete_at=delete_at)
        db.add(rec)
        await db.commit()
    logger.debug("Scheduled deletion: %s:%s at %s", chat_id, message_id, delete_at.isoformat())


async def autodelete_worker():
    """
    Background loop that deletes due messages and removes records from DB.
    """
    logger.info("Autodelete worker started; checking every %s seconds", AUTODELETE_CHECK_INTERVAL)
    while True:
        try:
            async with AsyncSessionLocal() as db:
                now = datetime.utcnow()
                stmt = select(DeliveryModel).where(DeliveryModel.delete_at <= now)
                res = await db.execute(stmt)
                due = res.scalars().all()
                if due:
                    logger.info("Autodelete: found %d messages to delete", len(due))
                for rec in due:
                    try:
                        await bot.delete_message(chat_id=rec.chat_id, message_id=rec.message_id)
                        logger.debug("Autodelete: deleted %s:%s", rec.chat_id, rec.message_id)
                    except Exception as e:
                        logger.warning("Autodelete: failed to delete %s:%s -> %s", rec.chat_id, rec.message_id, e)
                    try:
                        await db.delete(rec)
                    except Exception:
                        logger.exception("Autodelete: failed to remove DB record for %s:%s", rec.chat_id, rec.message_id)
                await db.commit()
        except Exception:
            logger.exception("Error in autodelete worker: %s", traceback.format_exc())
        await asyncio.sleep(AUTODELETE_CHECK_INTERVAL)


# ------------------------------------------------------------
# DB helpers for sessions and start message
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# Send helper to attempt protect_content, fallback if signature mismatch
# ------------------------------------------------------------
async def send_media_with_protect(file_type: str, chat_id: int, tg_file_id: str, caption: Optional[str], protect: bool):
    """
    Send media while trying to pass protect_content argument. If aiogram method doesn't accept the argument,
    retry without it (TypeError fallback).
    """
    kwargs: Dict[str, Any] = {}
    if caption:
        kwargs["caption"] = caption
    # attempt to include protect_content
    kwargs["protect_content"] = protect
    try:
        if file_type == "photo":
            return await bot.send_photo(chat_id=chat_id, photo=tg_file_id, **kwargs)
        elif file_type == "video":
            return await bot.send_video(chat_id=chat_id, video=tg_file_id, **kwargs)
        elif file_type == "document":
            return await bot.send_document(chat_id=chat_id, document=tg_file_id, **kwargs)
        elif file_type == "audio":
            return await bot.send_audio(chat_id=chat_id, audio=tg_file_id, **kwargs)
        elif file_type == "voice":
            kwargs.pop("caption", None)
            return await bot.send_voice(chat_id=chat_id, voice=tg_file_id, **kwargs)
        elif file_type == "sticker":
            kwargs.pop("caption", None)
            return await bot.send_sticker(chat_id=chat_id, sticker=tg_file_id, **kwargs)
        else:
            return await bot.send_document(chat_id=chat_id, document=tg_file_id, **kwargs)
    except TypeError:
        # method signature doesn't accept protect_content
        kwargs.pop("protect_content", None)
        try:
            if file_type == "photo":
                return await bot.send_photo(chat_id=chat_id, photo=tg_file_id, **kwargs)
            elif file_type == "video":
                return await bot.send_video(chat_id=chat_id, video=tg_file_id, **kwargs)
            elif file_type == "document":
                return await bot.send_document(chat_id=chat_id, document=tg_file_id, **kwargs)
            elif file_type == "audio":
                return await bot.send_audio(chat_id=chat_id, audio=tg_file_id, **kwargs)
            elif file_type == "voice":
                kwargs.pop("caption", None)
                return await bot.send_voice(chat_id=chat_id, voice=tg_file_id, **kwargs)
            elif file_type == "sticker":
                kwargs.pop("caption", None)
                return await bot.send_sticker(chat_id=chat_id, sticker=tg_file_id, **kwargs)
            else:
                return await bot.send_document(chat_id=chat_id, document=tg_file_id, **kwargs)
        except Exception:
            logger.exception("Failed to send media after removing protect_content parameter")
            raise
    except Exception:
        logger.exception("Failed to send media")
        raise


# ------------------------------------------------------------
# Command handlers
# ------------------------------------------------------------
@dp.message_handler(commands=["start"])
async def handle_start(message: types.Message):
    """
    /start:
    - if no args: show configured welcome (supports {first_name} and {label | url})
    - if args: treat as session token; deliver files in order
    """
    args = message.get_args().strip() if message.get_args() else ""
    if not args:
        content = await fetch_start_message()
        if content:
            rendered = content.replace("{first_name}", message.from_user.first_name or "")
            # convert {label | url} to <a href="url">label</a>
            out: List[str] = []
            i = 0
            while True:
                start = rendered.find("{", i)
                if start == -1:
                    out.append(rendered[i:])
                    break
                end = rendered.find("}", start)
                if end == -1:
                    out.append(rendered[i:])
                    break
                out.append(rendered[i:start])
                inner = rendered[start + 1:end].strip()
                if "|" in inner:
                    left, right = inner.split("|", 1)
                    out.append(f'<a href="{right.strip()}">{left.strip()}</a>')
                else:
                    out.append("{" + inner + "}")
                i = end + 1
            final_text = "".join(out)
            try:
                await message.answer(final_text, parse_mode="HTML", disable_web_page_preview=True)
            except Exception:
                await message.answer(content.replace("{first_name}", message.from_user.first_name or ""))
        else:
            await message.answer(f"Welcome, {message.from_user.first_name or 'friend'}!")
        return

    # treat args as token
    token = args
    session_obj = await get_session_by_token(token)
    if not session_obj:
        await message.answer("❌ Link not found or expired.")
        return
    if session_obj.revoked:
        await message.answer("❌ This link has been revoked by the owner.")
        return

    # fetch files for this session ordered by order_index
    async with AsyncSessionLocal() as db:
        stmt = select(FileModel).where(FileModel.session_id == session_obj.id).order_by(FileModel.order_index)
        res = await db.execute(stmt)
        files = res.scalars().all()

    if not files:
        await message.answer("❌ No files for this link.")
        return

    delivered = 0
    for f in files:
        try:
            protect_flag = False if message.from_user.id == OWNER_ID else bool(session_obj.protect_content)
            # concurrency control for sends
            await delivery_semaphore.acquire()
            try:
                sent = await send_media_with_protect(f.file_type, chat_id=message.chat.id, tg_file_id=f.tg_file_id, caption=f.caption, protect=protect_flag)
            finally:
                delivery_semaphore.release()
            delivered += 1
            if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
                delete_time = datetime.utcnow() + timedelta(minutes=session_obj.autodelete_minutes)
                await add_delivery(chat_id=sent.chat.id, message_id=sent.message_id, delete_at=delete_time)
        except Exception:
            logger.exception("Failed to deliver file %s in session %s", f.tg_file_id, token)
    if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
        await message.answer(f"Files delivered: {delivered}. They will be deleted after {session_obj.autodelete_minutes} minute(s).")
    else:
        await message.answer(f"Files delivered: {delivered}. (Autodelete disabled)")


@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    """
    Start an upload session (owner only).
    After this, owner sends files and uses /done to finalize or /abort to cancel.
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /upload.")
        return
    state = get_state_for_message(message)
    await state.set_state(UploadStates.waiting_for_files.state)
    await state.update_data(files=[])
    await message.reply("Upload started. Send files now (photo/video/document/audio/voice/sticker). When finished send /done. To cancel send /abort.")


@dp.message_handler(commands=["abort"])
async def cmd_abort(message: types.Message):
    """
    Cancel active upload session and attempt to delete forwarded messages from upload channel.
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /abort.")
        return
    state = get_state_for_message(message)
    data = await state.get_data()
    files = data.get("files", []) or []
    removed = 0
    for f in files:
        ch_msg_id = f.get("channel_message_id")
        if ch_msg_id:
            try:
                await bot.delete_message(chat_id=UPLOAD_CHANNEL_ID, message_id=ch_msg_id)
                removed += 1
            except Exception:
                # ignore deletion errors
                pass
    await state.finish()
    await message.reply(f"Upload aborted. Removed {removed} temporary message(s) from upload channel (if possible).")


@dp.message_handler(commands=["done"])
async def cmd_done(message: types.Message):
    """
    Finalize upload: ask protect on/off then autodelete minutes (range 0-10080).
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /done.")
        return
    state = get_state_for_message(message)
    if await state.get_state() != UploadStates.waiting_for_files.state:
        await message.reply("No active upload session. Use /upload to start.")
        return
    data = await state.get_data()
    files = data.get("files", []) or []
    if not files:
        await state.finish()
        await message.reply("No files uploaded. Upload canceled.")
        return
    await message.reply("Protect content? Reply with `on` or `off`.\nProtect prevents saving/forwarding in many clients (owner always bypasses).")
    await state.set_state(UploadStates.awaiting_protect.state)


# Generic handler to collect files, and to handle protect/autodelete prompts
@dp.message_handler()
async def generic_handler(message: types.Message):
    state = get_state_for_message(message)
    curr = await state.get_state()

    # awaiting_protect
    if curr == UploadStates.awaiting_protect.state:
        text = (message.text or "").strip().lower()
        if text not in ("on", "off"):
            await message.reply("Reply with `on` or `off`.")
            return
        protect = (text == "on")
        await state.update_data(protect=protect)
        await message.reply("Set autodelete minutes (0 - 10080). 0 = no autodelete. Example: `60`")
        await state.set_state(UploadStates.awaiting_autodelete.state)
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
            await message.reply("Please send an integer between 0 and 10080.")
        return

    # waiting_for_files: accept the various media types and forward to upload channel
    if curr == UploadStates.waiting_for_files.state:
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
            await message.reply("Send supported files (photo/video/document/audio/voice/sticker) or /done to finish, /abort to cancel.")
            return

        # forward message to upload channel to get stable channel-side file_id
        try:
            fwd_msg = await bot.forward_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
        except Exception as e:
            logger.exception("Failed to forward to upload channel: %s", e)
            await message.reply("Failed to forward to upload channel. Ensure the bot is admin there and can post.")
            return

        # extract stable file_id from forwarded message (channel)
        channel_file_id = None
        channel_file_type = None
        channel_caption = None
        try:
            if fwd_msg.photo:
                channel_file_type = "photo"
                channel_file_id = fwd_msg.photo[-1].file_id
                channel_caption = fwd_msg.caption or ""
            elif fwd_msg.video:
                channel_file_type = "video"
                channel_file_id = fwd_msg.video.file_id
                channel_caption = fwd_msg.caption or ""
            elif fwd_msg.document:
                channel_file_type = "document"
                channel_file_id = fwd_msg.document.file_id
                channel_caption = fwd_msg.caption or ""
            elif fwd_msg.audio:
                channel_file_type = "audio"
                channel_file_id = fwd_msg.audio.file_id
                channel_caption = fwd_msg.caption or ""
            elif fwd_msg.voice:
                channel_file_type = "voice"
                channel_file_id = fwd_msg.voice.file_id
                channel_caption = ""
            elif fwd_msg.sticker:
                channel_file_type = "sticker"
                channel_file_id = fwd_msg.sticker.file_id
                channel_caption = ""
            else:
                # fallback to original captured ids
                channel_file_type = ftype
                channel_file_id = orig_file_id
                channel_caption = caption
        except Exception:
            logger.exception("Error extracting file id from forwarded message; falling back to captured ids.")
            channel_file_type = ftype
            channel_file_id = orig_file_id
            channel_caption = caption

        # store in FSM
        data = await state.get_data()
        files = data.get("files", []) or []
        files.append({
            "file_type": channel_file_type,
            "channel_file_id": channel_file_id,
            "caption": channel_caption,
            "channel_message_id": fwd_msg.message_id,
        })
        await state.update_data(files=files)
        await message.reply(f"Saved {channel_file_type}. Send more files or /done when finished.")
        return

    # not in FSM; ignore
    return


async def finalize_upload_flow(message: types.Message, state: FSMContext):
    """
    Persist session and files to DB, generate random deep link (64 char), and reply to owner.
    """
    data = await state.get_data()
    files = data.get("files", []) or []
    protect = data.get("protect", False)
    autodelete = data.get("autodelete", 0)

    if not files:
        await state.finish()
        await message.reply("No files to finalize.")
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
        await db.flush()

        order_idx = 1
        for entry in files:
            file_type = entry.get("file_type")
            file_id = entry.get("channel_file_id")
            caption = entry.get("caption", "")
            if not file_id:
                logger.warning("Skipping file without id: %s", entry)
                continue
            fm = FileModel(
                session_id=session_rec.id,
                tg_file_id=file_id,
                file_type=file_type,
                caption=caption,
                order_index=order_idx,
            )
            db.add(fm)
            order_idx += 1

        await db.commit()

    # Construct shareable link
    me = await bot.get_me()
    bot_username = me.username or "bot"
    tme_link = f"https://t.me/{bot_username}?start={session_rec.link}"
    await message.reply(
        f"✅ Upload complete.\nDeep link (share this):\n{tme_link}\n\nProtect content: {'ON' if protect else 'OFF'}\nAutodelete: {autodelete} minute(s)\n\nUse /revoke <token> to disable this link."
    )
    await state.finish()


@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    """
    Owner-only: revoke a session (prevent further deliveries).
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can revoke sessions.")
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


@dp.message_handler(commands=["edit_start"])
async def cmd_edit_start(message: types.Message):
    """
    Owner-only: set /start welcome message by replying to any message (text or caption).
    Placeholders supported: {first_name} and {label | url}
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can edit the /start message.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or caption) that you want to set as the /start message.")
        return
    content = message.reply_to_message.text or message.reply_to_message.caption or ""
    if not content.strip():
        await message.reply("Replied message has no text/caption.")
        return
    try:
        await save_start_message(content)
        await message.reply("✅ /start message updated. Placeholders supported: {first_name} and {word | url}")
    except Exception:
        logger.exception("Failed to save /start message.")
        await message.reply("Failed to save /start message; check logs.")


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    """
    Owner-only help showing management commands.
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use this /help for management. Use /start to see welcome.")
        return
    txt = (
        "Owner commands:\n"
        "/upload - start upload (owner only)\n"
        "/done - finalize upload (owner only)\n"
        "/abort - cancel upload (owner only)\n"
        "/revoke <token> - revoke a link (owner only)\n"
        "/edit_start - set /start welcome message (owner only; reply to a message)\n"
        "/help - this help\n\n"
        "Universal:\n"
        "/start - welcome or open deep link (/start <token>)\n"
    )
    await message.reply(txt)


# ------------------------------------------------------------
# Webhook handler (aiohttp)
# Important: we set Bot.set_current and Dispatcher.set_current before processing update
# ------------------------------------------------------------
async def webhook_handler(request: Request):
    """
    Accepts Telegram POSTs at /webhook/<BOT_TOKEN>.
    Parses JSON into Update and passes to aiogram Dispatcher.
    Ensures aiogram context is set so message.answer/reply work.
    """
    try:
        data = await request.json()
    except Exception as e:
        logger.warning("Received non-json webhook request: %s", e)
        return web.Response(status=400, text="Invalid JSON")

    # parse update to aiogram Update object
    try:
        update = types.Update.de_json(data)
    except Exception:
        try:
            update = types.Update(**data)
        except Exception:
            logger.exception("Failed to parse update JSON into types.Update")
            return web.Response(status=400, text="Bad update")

    # Crucial fix: set current bot and dispatcher in this execution context
    try:
        Bot.set_current(bot)
    except Exception:
        # ignore if not supported
        pass
    try:
        Dispatcher.set_current(dp)
    except Exception:
        # ignore if not supported by aiogram version
        pass

    # Now process update
    try:
        await dp.process_update(update)
    except Exception:
        logger.exception("Dispatcher failed to process update: %s", traceback.format_exc())
    return web.Response(text="OK")


# ------------------------------------------------------------
# Health endpoint
# ------------------------------------------------------------
async def health_handler(request: Request):
    return web.Response(text="OK")


# ------------------------------------------------------------
# Start-up & shutdown lifecycle
# ------------------------------------------------------------
async def start_background_services():
    """
    Initialize DB, start autodelete worker and any background tasks.
    """
    try:
        await init_db()
    except Exception:
        logger.exception("init_db failed at startup")

    # start autodelete worker
    asyncio.create_task(autodelete_worker())
    logger.info("Background autodelete task started.")


async def run_app():
    """
    Build aiohttp app, set routes, register webhook with Telegram, and run server.
    """
    await start_background_services()

    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, webhook_handler)
    # support GET for webhook path for easier debugging; Telegram uses POST
    app.router.add_get(WEBHOOK_PATH, lambda req: web.Response(text="Webhook endpoint (GET)"))
    app.router.add_get("/health", health_handler)
    app.router.add_get("/", lambda req: web.Response(text="Bot is running"))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("aiohttp server started on port %s (webhook path %s)", PORT, WEBHOOK_PATH)

    # register webhook on Telegram
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except Exception:
        logger.exception("Failed to set webhook to %s", WEBHOOK_URL)

    # keep running
    try:
        await asyncio.Event().wait()
    finally:
        logger.info("Shutting down: removing webhook & cleaning up")
        try:
            await bot.delete_webhook()
        except Exception:
            logger.exception("Failed to delete webhook during shutdown")
        await runner.cleanup()
        try:
            await bot.close()
        except Exception:
            logger.exception("Failed to close bot cleanly")


# ------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------
def main():
    logger.info("Launching bot: webhook_host=%s port=%s", WEBHOOK_HOST, PORT)
    try:
        asyncio.run(run_app())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, exiting.")
    except Exception:
        logger.exception("Fatal error in main loop")
        sys.exit(1)


if __name__ == "__main__":
    main()