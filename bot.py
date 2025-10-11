# bot.py
"""
Telegram file-sharing bot (webhook mode) - FULL working implementation
- Aiogram 2.25.0 compatible
- Uses a single aiohttp server to receive updates at /webhook/<BOT_TOKEN>
- Health endpoint at /health
- Persistent PostgreSQL via SQLAlchemy (async) + asyncpg
- Owner-only commands: /upload, /done, /abort, /revoke, /help, /edit_start
- Public command: /start (and deep-link payload)
- Owner bypass for protect_content
- Autodelete persisted in DB and processed by background worker
- Uploads forwarded to private upload channel; DB stores channel file IDs
- Uses Bot.set_current(bot) + Dispatcher.set_current(dp) before processing each Update
- Auto-creates DB tables on startup
- Concurrency control for deliveries
- Detailed logging for troubleshooting
- Long, commented file intended for easy maintenance and auditing
"""

# Standard library
import os
import sys
import asyncio
import logging
import secrets
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

# Third-party
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
# Configuration
# ------------------------------------------------------------
# Logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("telegram_file_share_bot")

# Environment variables (required)
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID_ENV = os.environ.get("OWNER_ID", "6169237879")  # default to provided owner ID
DATABASE_URL = os.environ.get("DATABASE_URL")
UPLOAD_CHANNEL_ID_ENV = os.environ.get("UPLOAD_CHANNEL_ID")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # e.g. https://hidden-fnxx.onrender.com
PORT = int(os.environ.get("PORT", "10000"))

# Validate environment variables early and fail fast with clear error
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

# Convert types
try:
    OWNER_ID = int(OWNER_ID_ENV)
except Exception:
    raise RuntimeError("OWNER_ID must be an integer (set env var OWNER_ID)")

try:
    UPLOAD_CHANNEL_ID = int(UPLOAD_CHANNEL_ID_ENV)
except Exception:
    raise RuntimeError("UPLOAD_CHANNEL_ID must be an integer channel id (e.g. -1001234567890)")

# Build webhook path and final webhook URL
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST.rstrip('/')}{WEBHOOK_PATH}"

# Safety info
logger.info("Configuration: BOT_TOKEN=%s..., OWNER_ID=%s, UPLOAD_CHANNEL_ID=%s, WEBHOOK_HOST=%s, PORT=%s",
            (BOT_TOKEN[:8] + "..."), OWNER_ID, UPLOAD_CHANNEL_ID, WEBHOOK_HOST, PORT)

# ------------------------------------------------------------
# Database setup (SQLAlchemy async)
# ------------------------------------------------------------
Base = declarative_base()


class SessionModel(Base):
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
    __tablename__ = "files"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False)
    tg_file_id = Column(String, nullable=False)  # stable channel-side file_id
    file_type = Column(String(32), nullable=False)  # photo, video, document, audio, voice, sticker
    caption = Column(String, nullable=True)
    order_index = Column(Integer, nullable=False)

    session = relationship("SessionModel", back_populates="files")


class DeliveryModel(Base):
    __tablename__ = "deliveries"
    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(Integer, nullable=False)
    message_id = Column(Integer, nullable=False)
    delete_at = Column(DateTime(timezone=True), nullable=True)


class StartMessage(Base):
    __tablename__ = "start_message"
    id = Column(Integer, primary_key=True, autoincrement=True)
    content = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# Create async engine and sessionmaker
# Note: DATABASE_URL must be like: postgresql+asyncpg://user:pass@host:port/dbname
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    """Create tables if they do not exist."""
    logger.info("Initializing database (create tables if needed)...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialized and tables ensured.")


# ------------------------------------------------------------
# Aiogram setup
# ------------------------------------------------------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)

# We'll use a semaphore to limit concurrent delivery operations and avoid hitting Telegram rate limits
MAX_CONCURRENT_DELIVERIES = int(os.environ.get("MAX_CONCURRENT_DELIVERIES", "50"))
delivery_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DELIVERIES)

# ------------------------------------------------------------
# FSM states
# ------------------------------------------------------------
class UploadStates(StatesGroup):
    waiting_for_files = State()
    awaiting_protect = State()
    awaiting_autodelete = State()


# ------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------
def generate_token(length: int = 64) -> str:
    """Return a URL-safe token (exact length slice)."""
    return secrets.token_urlsafe(length)[:length]


def get_state_for_message(message: types.Message) -> FSMContext:
    """
    Return FSMContext bound to the (chat, user) tuple.
    This ensures state is tied to the exact conversation where the owner issues commands.
    """
    return dp.current_state(chat=message.chat.id, user=message.from_user.id)


# ------------------------------------------------------------
# Autodelete background worker
# ------------------------------------------------------------
AUTODELETE_CHECK_INTERVAL = int(os.environ.get("AUTODELETE_CHECK_INTERVAL", "30"))  # seconds


async def add_delivery(chat_id: int, message_id: int, delete_at: Optional[datetime]):
    """Persist a message scheduled for deletion."""
    if delete_at is None:
        return
    async with AsyncSessionLocal() as db:
        rec = DeliveryModel(chat_id=chat_id, message_id=message_id, delete_at=delete_at)
        db.add(rec)
        await db.commit()
    logger.debug("Scheduled deletion for %s:%s at %s", chat_id, message_id, delete_at.isoformat())


async def autodelete_worker():
    """Continuously checks DB for due messages and deletes them."""
    logger.info("Autodelete worker starting (interval %s seconds)", AUTODELETE_CHECK_INTERVAL)
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
                        logger.debug("Autodelete: removed message %s:%s", rec.chat_id, rec.message_id)
                    except Exception as e:
                        logger.warning("Autodelete: failed to delete message %s:%s -> %s", rec.chat_id, rec.message_id, e)
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
    logger.info("Saved /start message content.")


async def fetch_start_message() -> Optional[str]:
    async with AsyncSessionLocal() as db:
        stmt = select(StartMessage).order_by(StartMessage.updated_at.desc())
        res = await db.execute(stmt)
        rec = res.scalars().first()
        return rec.content if rec else None


# ------------------------------------------------------------
# Send helper with fallback for protect_content param
# ------------------------------------------------------------
async def send_media_with_protect(ftype: str, chat_id: int, file_id: str, caption: Optional[str], protect: bool):
    """
    Send media while attempting to pass protect_content.
    If method signature doesn't accept protect_content, we retry without it.
    """
    kwargs: Dict[str, Any] = {}
    if caption:
        kwargs["caption"] = caption
    # try to set protect_content; if it fails due to signature mismatch, retry without it
    kwargs["protect_content"] = protect
    try:
        if ftype == "photo":
            return await bot.send_photo(chat_id=chat_id, photo=file_id, **kwargs)
        if ftype == "video":
            return await bot.send_video(chat_id=chat_id, video=file_id, **kwargs)
        if ftype == "document":
            return await bot.send_document(chat_id=chat_id, document=file_id, **kwargs)
        if ftype == "audio":
            return await bot.send_audio(chat_id=chat_id, audio=file_id, **kwargs)
        if ftype == "voice":
            # voice normally does not accept caption
            kwargs.pop("caption", None)
            return await bot.send_voice(chat_id=chat_id, voice=file_id, **kwargs)
        if ftype == "sticker":
            kwargs.pop("caption", None)
            return await bot.send_sticker(chat_id=chat_id, sticker=file_id, **kwargs)
        # fallback to document
        return await bot.send_document(chat_id=chat_id, document=file_id, **kwargs)
    except TypeError as e:
        # signature didn't accept protect_content — retry without it
        logger.debug("TypeError sending media (retry without protect): %s", e)
        kwargs.pop("protect_content", None)
        try:
            if ftype == "photo":
                return await bot.send_photo(chat_id=chat_id, photo=file_id, **kwargs)
            if ftype == "video":
                return await bot.send_video(chat_id=chat_id, video=file_id, **kwargs)
            if ftype == "document":
                return await bot.send_document(chat_id=chat_id, document=file_id, **kwargs)
            if ftype == "audio":
                return await bot.send_audio(chat_id=chat_id, audio=file_id, **kwargs)
            if ftype == "voice":
                kwargs.pop("caption", None)
                return await bot.send_voice(chat_id=chat_id, voice=file_id, **kwargs)
            if ftype == "sticker":
                kwargs.pop("caption", None)
                return await bot.send_sticker(chat_id=chat_id, sticker=file_id, **kwargs)
            return await bot.send_document(chat_id=chat_id, document=file_id, **kwargs)
        except Exception:
            logger.exception("Failed on retry sending media without protect_content.")
            raise
    except Exception:
        logger.exception("Failed sending media with protect_content.")
        raise


# ------------------------------------------------------------
# Message handlers
# ------------------------------------------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    """
    /start handler — if args present treat as token; else show welcome message.
    Note: this handler will be executed inside the aiogram Dispatcher context provided
    by setting Bot.set_current and Dispatcher.set_current before dp.process_update.
    """
    args = message.get_args().strip() if message.get_args() else ""
    if not args:
        # show custom start or default welcome
        content = await fetch_start_message()
        if content:
            # replace placeholder and convert {label | url}
            rendered = content.replace("{first_name}", (message.from_user.first_name or ""))
            # construct basic HTML anchor for {label | url}
            out = []
            i = 0
            while True:
                s = rendered.find("{", i)
                if s == -1:
                    out.append(rendered[i:])
                    break
                e = rendered.find("}", s)
                if e == -1:
                    out.append(rendered[i:])
                    break
                out.append(rendered[i:s])
                inner = rendered[s + 1:e].strip()
                if "|" in inner:
                    left, right = inner.split("|", 1)
                    out.append(f'<a href="{right.strip()}">{left.strip()}</a>')
                else:
                    out.append("{" + inner + "}")
                i = e + 1
            final_text = "".join(out)
            try:
                await message.answer(final_text, parse_mode="HTML", disable_web_page_preview=True)
            except Exception:
                logger.exception("Failed to send HTML start message; sending plain text fallback.")
                await message.answer(content.replace("{first_name}", (message.from_user.first_name or "")))
        else:
            await message.answer(f"Welcome, {message.from_user.first_name or 'friend'}!")
        return

    # treat args as token
    token = args
    session_row = await get_session_by_token(token)
    if not session_row:
        await message.answer("❌ Link not found or expired.")
        return
    if session_row.revoked:
        await message.answer("❌ This link has been revoked by the owner.")
        return

    # fetch files in session order
    async with AsyncSessionLocal() as db:
        stmt = select(FileModel).where(FileModel.session_id == session_row.id).order_by(FileModel.order_index)
        res = await db.execute(stmt)
        files = res.scalars().all()

    if not files:
        await message.answer("❌ No files found for this link.")
        return

    delivered_count = 0
    for f in files:
        try:
            protect_flag = False if message.from_user.id == OWNER_ID else bool(session_row.protect_content)
            # limit concurrency with semaphore per send
            await delivery_semaphore.acquire()
            try:
                sent_msg = await send_media_with_protect(f.file_type, chat_id=message.chat.id, file_id=f.tg_file_id, caption=f.caption, protect=protect_flag)
            finally:
                delivery_semaphore.release()

            delivered_count += 1
            # schedule autodelete for the recipient copy
            if session_row.autodelete_minutes and session_row.autodelete_minutes > 0:
                delete_time = datetime.utcnow() + timedelta(minutes=session_row.autodelete_minutes)
                await add_delivery(chat_id=sent_msg.chat.id, message_id=sent_msg.message_id, delete_at=delete_time)
        except Exception:
            logger.exception("Failed to deliver file for token %s", token)

    if session_row.autodelete_minutes and session_row.autodelete_minutes > 0:
        await message.answer(f"Files delivered: {delivered_count}. They will be deleted after {session_row.autodelete_minutes} minute(s).")
    else:
        await message.answer(f"Files delivered: {delivered_count}. (Autodelete disabled)")


@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    """Owner-only: start an upload session."""
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /upload.")
        return
    state = get_state_for_message(message)
    await state.set_state(UploadStates.waiting_for_files.state)
    await state.update_data(files=[])
    await message.reply("Upload session started. Send files (photo/video/document/audio/voice/sticker). When finished send /done. To cancel send /abort.")


@dp.message_handler(commands=["abort"])
async def cmd_abort(message: types.Message):
    """Owner-only: abort upload, attempt to remove forwarded messages from upload channel."""
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /abort.")
        return
    state = get_state_for_message(message)
    data = await state.get_data()
    files = data.get("files", []) or []
    removed = 0
    for item in files:
        ch_msg_id = item.get("channel_message_id")
        if ch_msg_id:
            try:
                await bot.delete_message(chat_id=UPLOAD_CHANNEL_ID, message_id=ch_msg_id)
                removed += 1
            except Exception:
                # ignore deletion errors
                pass
    await state.finish()
    await message.reply(f"Upload aborted. Removed {removed} forwarded messages from upload channel (if possible).")


@dp.message_handler(commands=["done"])
async def cmd_done(message: types.Message):
    """Owner-only: finalize upload; ask protect on/off then autodelete minutes."""
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
    await message.reply("Protect content? Reply with `on` or `off`.")
    await state.set_state(UploadStates.awaiting_protect.state)


@dp.message_handler()
async def generic_handler(message: types.Message):
    """
    Generic handler to:
    - Collect files during upload FSM (waiting_for_files)
    - Handle responses to protect prompt (awaiting_protect)
    - Handle autodelete minutes prompt (awaiting_autodelete)
    """
    state = get_state_for_message(message)
    curr = await state.get_state()

    # awaiting_protect
    if curr == UploadStates.awaiting_protect.state:
        text = (message.text or "").strip().lower()
        if text not in ("on", "off"):
            await message.reply("Please reply with `on` or `off`.")
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

    # waiting_for_files
    if curr == UploadStates.waiting_for_files.state:
        ftype = None
        orig_file_id = None
        caption = None

        # identify supported file types
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

        # forward to upload channel (bot must be admin)
        try:
            fwd_msg = await bot.forward_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
        except Exception as e:
            logger.exception("Failed to forward to upload channel: %s", e)
            await message.reply("Failed to forward to upload channel. Ensure bot is admin with permission to post.")
            return

        # immediately extract channel-specific stable file_id
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
                channel_file_type = ftype
                channel_file_id = orig_file_id
                channel_caption = caption
        except Exception:
            logger.exception("Error extracting file id from forwarded message; falling back to original captured ids.")
            channel_file_type = ftype
            channel_file_id = orig_file_id
            channel_caption = caption

        # store info in FSM
        data = await state.get_data()
        files = data.get("files", []) or []
        files.append({
            "file_type": channel_file_type,
            "channel_file_id": channel_file_id,
            "caption": channel_caption,
            "channel_message_id": fwd_msg.message_id
        })
        await state.update_data(files=files)
        await message.reply(f"Saved {channel_file_type}. Send more or /done when finished.")
        return

    # not in FSM: ignore
    return


async def finalize_upload_flow(message: types.Message, state: FSMContext):
    """Persist session and file rows into DB, generate deep link, finish FSM."""
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
        session_row = SessionModel(
            link=token,
            owner_id=OWNER_ID,
            protect_content=bool(protect),
            autodelete_minutes=int(autodelete),
        )
        db.add(session_row)
        await db.flush()  # ensure session_row.id exists

        order_idx = 1
        for entry in files:
            file_type = entry.get("file_type")
            file_id = entry.get("channel_file_id")
            caption = entry.get("caption", "")
            if not file_id:
                logger.warning("Skipping entry without file_id: %s", entry)
                continue
            fm = FileModel(
                session_id=session_row.id,
                tg_file_id=file_id,
                file_type=file_type,
                caption=caption,
                order_index=order_idx
            )
            db.add(fm)
            order_idx += 1

        await db.commit()

    # Build shareable deep link
    me = await bot.get_me()
    bot_username = me.username or "bot"
    tme_link = f"https://t.me/{bot_username}?start={session_row.link}"

    await message.reply(
        f"✅ Upload complete.\nDeep link:\n{tme_link}\n\nProtect content: {'ON' if protect else 'OFF'}\nAutodelete: {autodelete} minute(s)\n\nUse /revoke <token> to disable this link."
    )
    await state.finish()


@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    """Owner-only: revoke a token so it can no longer be used."""
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can revoke links.")
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
    """Owner-only: set the /start welcome message by replying to any message (text or photo caption)."""
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can edit /start message.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or caption) which you want to set as the /start message.")
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
    """Owner-only /help (management help)."""
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /help for management. Use /start for welcome.")
        return
    help_text = (
        "Owner commands:\n"
        "/upload - start upload session (owner only)\n"
        "/done - finalize upload (owner only)\n"
        "/abort - cancel upload session (owner only)\n"
        "/revoke <token> - revoke a link (owner only)\n"
        "/edit_start - edit the /start welcome message (owner only; reply to a message)\n"
        "/help - this help\n\n"
        "Universal:\n"
        "/start - show welcome or use deep link (/start <token>)\n"
    )
    await message.reply(help_text)


# ------------------------------------------------------------
# Webhook receiver (aiohttp) - key fix: set current bot/dispatcher context
# ------------------------------------------------------------
async def webhook_handler(request: Request):
    """
    Receive Telegram update POSTs and hand them to aiogram Dispatcher.
    Crucial fix: set Bot.set_current(bot) and Dispatcher.set_current(dp)
    so handlers can use message.answer(), message.reply(), etc. without context errors.
    """
    # ensure content type is JSON
    try:
        update_json = await request.json()
    except Exception as e:
        logger.warning("Received non-JSON request on webhook: %s", e)
        return web.Response(status=400, text="Invalid JSON")

    # parse update
    try:
        update = types.Update.de_json(update_json)
    except Exception:
        # fallback
        try:
            update = types.Update(**update_json)
        except Exception:
            logger.exception("Failed to construct Update object from JSON")
            return web.Response(status=400, text="Bad Update")

    # IMPORTANT: set current bot and dispatcher for this asyncio task / context
    try:
        Bot.set_current(bot)
        Dispatcher.set_current(dp)
    except Exception:
        # some aiogram versions may not have Dispatcher.set_current; ignore if not present
        try:
            dp.set_current(dp)
        except Exception:
            pass

    # process update with dispatcher
    try:
        await dp.process_update(update)
    except Exception:
        logger.exception("Dispatcher failed to process update: %s", traceback.format_exc())
        # Return 200 so Telegram does not immediately retry aggressively
    return web.Response(text="OK")


# ------------------------------------------------------------
# Health handler
# ------------------------------------------------------------
async def health_handler(request: Request):
    return web.Response(text="OK")


# ------------------------------------------------------------
# App startup routine to create aiohttp server, set webhook, start DB, workers
# ------------------------------------------------------------
async def start_background_services():
    """Start DB, autodelete worker, and any other background tasks."""
    # Initialize database (create tables)
    try:
        await init_db()
    except Exception:
        logger.exception("init_db failed during startup")

    # Start autodelete worker
    asyncio.create_task(autodelete_worker())
    logger.info("Started autodelete background worker")

    # you can add more background tasks here if needed (metrics, cleanup, etc.)


async def run_app():
    """Create aiohttp app, register webhook & health routes, set webhook with Telegram, and run server."""
    # Start DB and background workers
    await start_background_services()

    # aiohttp app
    app = web.Application()
    # webhook endpoint; Telegram will POST updates here
    app.router.add_post(WEBHOOK_PATH, webhook_handler)
    # optional GET to check
    app.router.add_get(WEBHOOK_PATH, lambda req: web.Response(text="Webhook endpoint (GET)"))
    app.router.add_get("/health", health_handler)
    # root to show something
    app.router.add_get("/", lambda req: web.Response(text="Bot running"))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("aiohttp server started on port %s (webhook path: %s)", PORT, WEBHOOK_PATH)

    # set webhook with Telegram
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook registered at Telegram: %s", WEBHOOK_URL)
    except Exception:
        logger.exception("Failed to register webhook at %s", WEBHOOK_URL)

    # keep running until cancelled
    try:
        await asyncio.Event().wait()
    finally:
        # cleanup on shutdown
        logger.info("Shutting down server; removing webhook and cleaning up")
        try:
            await bot.delete_webhook()
        except Exception:
            logger.exception("Failed to delete webhook during shutdown")
        await runner.cleanup()
        try:
            await bot.close()
        except Exception:
            logger.exception("Failed to close bot session")


# ------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------
def main():
    logger.info("Starting bot process, webhook host=%s, port=%s", WEBHOOK_HOST, PORT)
    # run the aiohttp+Dispatcher app inside asyncio.run
    try:
        asyncio.run(run_app())
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, exiting.")
    except Exception:
        logger.exception("Unhandled exception in main loop")
        sys.exit(1)


if __name__ == "__main__":
    main()

# End of file