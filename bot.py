# bot.py
import asyncio
import logging
import os
import random
import string
import json
from typing import List, Dict, Optional, Tuple  # ✅ FIXED: added Tuple
from datetime import datetime, timedelta

from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from aiogram.utils.executor import start_webhook
from aiogram.utils.exceptions import TelegramAPIError
from aiogram.contrib.middlewares.logging import LoggingMiddleware

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, Text, create_engine
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import select

from dotenv import load_dotenv
# ------------------------------------------------------------
# Logging configuration
# ------------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("session_share_bot")

# ------------------------------------------------------------
# Environment variables and validation
# ------------------------------------------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID_ENV = os.environ.get("OWNER_ID", "6169237879")  # default to provided ID if not set
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

WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST.rstrip('/')}{WEBHOOK_PATH}"

logger.info("Configuration: OWNER_ID=%s, UPLOAD_CHANNEL_ID=%s, WEBHOOK_HOST=%s, PORT=%s",
            OWNER_ID, UPLOAD_CHANNEL_ID, WEBHOOK_HOST, PORT)

# ------------------------------------------------------------
# Database: models, engine, sessionmaker
# ------------------------------------------------------------
Base = declarative_base()


class SessionModel(Base):
    """
    sessions: stores metadata for each upload session.
    status: 'draft' -> owner is uploading files
            'awaiting_protect' -> owner answered /done and must reply 'on'/'off'
            'awaiting_autodelete' -> owner must reply minutes
            'published' -> deep link ready and usable
            'revoked' -> disabled
    """
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column(String(128), unique=True, nullable=False, index=True)  # random token
    owner_id = Column(Integer, nullable=False)
    status = Column(String(32), nullable=False, default="draft")
    protect_content = Column(Boolean, default=False)
    autodelete_minutes = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    files = relationship("FileModel", back_populates="session", cascade="all, delete-orphan")


class FileModel(Base):
    """
    files: stores stable Telegram file_id (from upload channel) and order index.
    We only store minimal info to keep DB usage tiny.
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
    deliveries: persistent scheduled deletions of messages delivered to users.
    This way autodelete survives restarts.
    """
    __tablename__ = "deliveries"
    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(Integer, nullable=False)
    message_id = Column(Integer, nullable=False)
    delete_at = Column(DateTime(timezone=True), nullable=True)


class StartMessage(Base):
    """
    Stores the /start welcome content (text/caption).
    """
    __tablename__ = "start_message"
    id = Column(Integer, primary_key=True, autoincrement=True)
    content = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# Add an index to FileModel for session ordering lookup performance
Index("ix_files_session_order", FileModel.session_id, FileModel.order_index)

# create async engine & sessionmaker
# Ensure DATABASE_URL uses asyncpg: e.g. postgresql+asyncpg://user:pass@host:port/dbname
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    """Auto-create tables on startup (convenient for Render deployments)."""
    logger.info("Initializing DB (creating tables if needed)...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("DB initialized.")


# ------------------------------------------------------------
# Aiogram bot and dispatcher
# ------------------------------------------------------------
# We intentionally keep FSM storage minimal (MemoryStorage) because we don't rely on FSM state;
# upload sessions are persisted to DB so restarts won't lose them.
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)

# concurrency control for deliveries
delivery_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DELIVERIES)

# ------------------------------------------------------------
# Utilities: token generator, DB helpers
# ------------------------------------------------------------
def generate_token(length: int = 64) -> str:
    return secrets.token_urlsafe(length)[:length]


async def create_draft_session(owner_id: int) -> SessionModel:
    """Create a draft session and return the ORM object (in a new DB session)."""
    async with AsyncSessionLocal() as db:
        token = generate_token(64)
        rec = SessionModel(link=token, owner_id=owner_id, status="draft")
        db.add(rec)
        await db.commit()
        await db.refresh(rec)
        return rec


async def get_owner_active_draft(owner_id: int) -> Optional[SessionModel]:
    """Return the active draft session for the owner if exists (status == 'draft')."""
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.owner_id == owner_id, SessionModel.status == "draft").limit(1)
        res = await db.execute(stmt)
        return res.scalars().first()


async def append_file_to_session(session_id: int, tg_file_id: str, file_type: str, caption: str):
    """Insert a file row into the session with the next order_index."""
    async with AsyncSessionLocal() as db:
        # compute next order_index
        stmt = select(func.max(FileModel.order_index)).where(FileModel.session_id == session_id)
        res = await db.execute(stmt)
        max_idx = res.scalar()
        next_idx = (max_idx or 0) + 1
        fm = FileModel(session_id=session_id, tg_file_id=tg_file_id, file_type=file_type, caption=caption, order_index=next_idx)
        db.add(fm)
        await db.commit()
        await db.refresh(fm)
        return fm


async def list_files_for_session(session_id: int) -> List[FileModel]:
    async with AsyncSessionLocal() as db:
        stmt = select(FileModel).where(FileModel.session_id == session_id).order_by(FileModel.order_index)
        res = await db.execute(stmt)
        return res.scalars().all()


async def set_session_status(session_id: int, status: str):
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.id == session_id).limit(1)
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if rec:
            rec.status = status
            db.add(rec)
            await db.commit()
            return rec
        return None


async def set_protect_and_autodelete(session_id: int, protect: bool, autodelete: int):
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.id == session_id).limit(1)
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if rec:
            rec.protect_content = bool(protect)
            rec.autodelete_minutes = int(autodelete)
            rec.status = "published"
            db.add(rec)
            await db.commit()
            return rec
        return None


async def revoke_session_by_token(token: str) -> bool:
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.link == token).limit(1)
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if rec:
            rec.revoked = True
            rec.status = "revoked"
            db.add(rec)
            await db.commit()
            return True
        return False


async def delete_draft_and_uploaded_forwarded(session_id: int) -> Tuple[int, bool]:
    """
    Attempt to delete forwarded messages from upload channel for a draft session and delete DB rows.
    Returns (removed_count, success_bool)
    """
    removed = 0
    try:
        async with AsyncSessionLocal() as db:
            # get files for session
            stmt = select(FileModel).where(FileModel.session_id == session_id)
            res = await db.execute(stmt)
            files = res.scalars().all()
            for f in files:
                # We stored tg_file_id, but to delete the forwarded message we need message_id in upload channel.
                # We saved only tg_file_id (file_id) earlier. However we also saved channel_message_id below in the flow.
                # To support deletion, we will store channel_message_id in DB's FileModel.caption field as metadata if needed.
                # But to keep DB minimal and reliable, we'll try to delete by searching recent messages in upload channel
                # (not implemented here due to Telegram API limitations). We'll skip deletion attempt if no message id stored.
                pass
            # delete session and files
            await db.execute(select(SessionModel).where(SessionModel.id == session_id).limit(1))
            # simpler: delete by loading and deleting
            stmt2 = select(SessionModel).where(SessionModel.id == session_id).limit(1)
            res2 = await db.execute(stmt2)
            rec = res2.scalars().first()
            if rec:
                await db.delete(rec)
                await db.commit()
        return removed, True
    except Exception:
        logger.exception("Error deleting draft session %s", session_id)
        return removed, False


async def get_session_by_token(token: str) -> Optional[SessionModel]:
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.link == token).limit(1)
        res = await db.execute(stmt)
        return res.scalars().first()


async def schedule_delivery(chat_id: int, message_id: int, delete_at: Optional[datetime]):
    """Persist a scheduled deletion for an already-sent message."""
    if delete_at is None:
        return
    async with AsyncSessionLocal() as db:
        rec = DeliveryModel(chat_id=chat_id, message_id=message_id, delete_at=delete_at)
        db.add(rec)
        await db.commit()
    logger.debug("Scheduled deletion for %s:%s at %s", chat_id, message_id, delete_at.isoformat())


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
    logger.info("Saved /start content.")


async def get_start_message() -> Optional[str]:
    async with AsyncSessionLocal() as db:
        stmt = select(StartMessage).order_by(StartMessage.updated_at.desc())
        res = await db.execute(stmt)
        rec = res.scalars().first()
        return rec.content if rec else None


# ------------------------------------------------------------
# Send helper that tries protect_content if supported then falls back
# ------------------------------------------------------------
async def send_media_with_possible_protect(file_type: str, chat_id: int, tg_file_id: str, caption: Optional[str], protect: bool):
    kwargs: Dict[str, Any] = {}
    if caption:
        kwargs["caption"] = caption
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
        # fallback to no protect_content
        kwargs.pop("protect_content", None)
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
        logger.exception("Error sending media to %s", chat_id)
        raise


# ------------------------------------------------------------
# Message handlers
# ------------------------------------------------------------
# /start public (welcome or deep link)
@dp.message_handler(commands=["start"])
async def handle_start(message: types.Message):
    args = message.get_args().strip() if message.get_args() else ""
    if not args:
        content = await get_start_message()
        if content:
            rendered = content.replace("{first_name}", message.from_user.first_name or "")
            # convert {label | url} -> HTML anchor
            out_parts: List[str] = []
            i = 0
            while True:
                s = rendered.find("{", i)
                if s == -1:
                    out_parts.append(rendered[i:])
                    break
                e = rendered.find("}", s)
                if e == -1:
                    out_parts.append(rendered[i:])
                    break
                out_parts.append(rendered[i:s])
                inner = rendered[s+1:e].strip()
                if "|" in inner:
                    left, right = inner.split("|", 1)
                    out_parts.append(f'<a href="{right.strip()}">{left.strip()}</a>')
                else:
                    out_parts.append("{" + inner + "}")
                i = e + 1
            final_text = "".join(out_parts)
            try:
                await message.answer(final_text, parse_mode="HTML", disable_web_page_preview=True)
            except Exception:
                await message.answer(content.replace("{first_name}", message.from_user.first_name or ""))
        else:
            await message.answer(f"Welcome, {message.from_user.first_name or 'friend'}!")
        return

    # treat args as session token
    token = args
    session = await get_session_by_token(token)
    if not session:
        await message.answer("❌ Link not found or invalid.")
        return
    if session.revoked or session.status == "revoked":
        await message.answer("❌ This link has been revoked.")
        return
    if session.status != "published":
        await message.answer("❌ This link is not published yet.")
        return

    # get files in order
    files = await list_files_for_session(session.id)
    if not files:
        await message.answer("❌ No files found for this link.")
        return

    delivered = 0
    for f in files:
        try:
            protect_flag = False if message.from_user.id == OWNER_ID else bool(session.protect_content)
            # limit concurrency per-send
            await delivery_semaphore.acquire()
            try:
                sent_msg = await send_media_with_possible_protect(f.file_type, chat_id=message.chat.id, tg_file_id=f.tg_file_id, caption=f.caption, protect=protect_flag)
            finally:
                delivery_semaphore.release()
            delivered += 1
            # schedule autodelete if set
            if session.autodelete_minutes and session.autodelete_minutes > 0:
                delete_at = datetime.utcnow() + timedelta(minutes=session.autodelete_minutes)
                await schedule_delivery(chat_id=sent_msg.chat.id, message_id=sent_msg.message_id, delete_at=delete_at)
        except Exception:
            logger.exception("Failed delivering file %s for session %s", f.tg_file_id, session.link)
    if session.autodelete_minutes and session.autodelete_minutes > 0:
        await message.answer(f"Files delivered: {delivered}. They will be deleted after {session.autodelete_minutes} minute(s).")
    else:
        await message.answer(f"Files delivered: {delivered}. (Autodelete disabled)")


# /upload owner-only: create draft session
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /upload.")
        return
    # check if owner already has a draft
    draft = await get_owner_active_draft(OWNER_ID)
    if draft:
        await message.reply(f"You already have an active draft session. Continue sending files or use /done to finish. Draft token: {draft.link}")
        return
    # create draft session
    rec = await create_draft_session(OWNER_ID)
    await message.reply(f"Upload session started. Session token: {rec.link}\nSend files (photo/video/document/audio/voice/sticker). When finished send /done. To cancel send /abort.")


# accept media while owner has a draft; forward each file to upload channel and store stable file_id
@dp.message_handler(content_types=["photo", "video", "document", "audio", "voice", "sticker"])
async def media_receiver(message: types.Message):
    # Only owner uploads
    if message.from_user.id != OWNER_ID:
        # ignore media from others
        return
    # check for active draft session
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        # no draft active; ask owner to run /upload
        await message.reply("No active upload session. Use /upload to start.")
        return

    # determine file type and get local file_id
    ftype = None
    orig_file_id = None
    caption = None
    try:
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
    except Exception:
        logger.exception("Error extracting file data from owner message")
        await message.reply("Failed to read the file. Try sending again.")
        return

    # forward to upload channel to create stable channel-side file_id
    try:
        fwd = await bot.forward_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
    except Exception as e:
        logger.exception("Failed to forward to upload channel: %s", e)
        await message.reply("Failed to forward to upload channel. Ensure bot is admin there and allowed to post.")
        return

    # extract stable file_id from forwarded message object
    channel_file_id = None
    channel_file_type = None
    channel_caption = None
    try:
        if fwd.photo:
            channel_file_type = "photo"
            channel_file_id = fwd.photo[-1].file_id
            channel_caption = fwd.caption or ""
        elif fwd.video:
            channel_file_type = "video"
            channel_file_id = fwd.video.file_id
            channel_caption = fwd.caption or ""
        elif fwd.document:
            channel_file_type = "document"
            channel_file_id = fwd.document.file_id
            channel_caption = fwd.caption or ""
        elif fwd.audio:
            channel_file_type = "audio"
            channel_file_id = fwd.audio.file_id
            channel_caption = fwd.caption or ""
        elif fwd.voice:
            channel_file_type = "voice"
            channel_file_id = fwd.voice.file_id
            channel_caption = ""
        elif fwd.sticker:
            channel_file_type = "sticker"
            channel_file_id = fwd.sticker.file_id
            channel_caption = ""
        else:
            # fallback
            channel_file_type = ftype
            channel_file_id = orig_file_id
            channel_caption = caption
    except Exception:
        logger.exception("Error extracting channel file id from forwarded message; using original id")
        channel_file_type = ftype
        channel_file_id = orig_file_id
        channel_caption = caption

    # store the channel file_id in DB under this draft session
    try:
        await append_file_to_session(session_id=draft.id, tg_file_id=channel_file_id, file_type=channel_file_type, caption=channel_caption)
        await message.reply(f"Saved file ({channel_file_type}). Send more files or /done when finished.")
    except Exception:
        logger.exception("Failed to save file info to DB")
        await message.reply("Failed to save file metadata to DB. Try again or abort with /abort.")


# /abort owner-only: delete draft session (attempt deletion of forwarded messages if possible)
@dp.message_handler(commands=["abort"])
async def cmd_abort(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /abort.")
        return
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        await message.reply("No active upload session to abort.")
        return
    # Attempt to remove forwarded messages from upload channel - we only have file_ids and not message_ids,
    # so deletion of upload channel messages may not be possible reliably. We will attempt best-effort:
    # Note: If you want deletions to be possible, we'd need to store channel_message_id at upload time.
    # For now, delete DB session and inform owner.
    try:
        async with AsyncSessionLocal() as db:
            stmt = select(SessionModel).where(SessionModel.id == draft.id).limit(1)
            res = await db.execute(stmt)
            rec = res.scalars().first()
            if rec:
                await db.delete(rec)
                await db.commit()
        await message.reply("Upload aborted and draft removed from DB. Forwarded messages in upload channel were not deleted (if you want them deleted store channel_message_id).")
    except Exception:
        logger.exception("Failed to abort draft session")
        await message.reply("Failed to abort draft session. Check logs.")


# /done owner-only: start protect/autodelete prompts by transitioning draft->awaiting_protect
@dp.message_handler(commands=["done"])
async def cmd_done(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /done.")
        return
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        await message.reply("No active upload session. Use /upload to start.")
        return
    # ensure there are files in draft
    files = await list_files_for_session(draft.id)
    if not files:
        await message.reply("No files uploaded in this session. Upload files first or use /abort to cancel.")
        return
    # transition to awaiting_protect
    updated = await set_session_status(draft.id, "awaiting_protect")
    if updated:
        await message.reply("Protect content? Reply with `on` or `off`.")
    else:
        await message.reply("Failed to transition session state. Try again.")


# Owner replies 'on'/'off' when session is awaiting_protect -> move to awaiting_autodelete
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and (m.text or "").strip().lower() in ("on", "off"))
async def owner_protect_reply(message: types.Message):
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        # maybe owner replied while no draft active
        return
    # ensure session is awaiting_protect
    if draft.status != "awaiting_protect":
        return
    text = (message.text or "").strip().lower()
    protect = text == "on"
    # set protect and move to awaiting_autodelete state (we store protect in DB only after autodelete chosen)
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.id == draft.id).limit(1)
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if not rec:
            await message.reply("Session no longer exists.")
            return
        rec.protect_content = protect
        rec.status = "awaiting_autodelete"
        db.add(rec)
        await db.commit()
    await message.reply("Protect set to: {}. Now reply with autodelete minutes (0 - 10080). 0 = no autodelete.".format("ON" if protect else "OFF"))


# Owner replies minutes when session is awaiting_autodelete -> finalize session (published)
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and (m.text or "").strip().isdigit())
async def owner_autodelete_reply(message: types.Message):
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        return
    if draft.status != "awaiting_autodelete":
        return
    text = (message.text or "").strip()
    try:
        minutes = int(text)
        if minutes < 0 or minutes > 10080:
            raise ValueError
    except ValueError:
        await message.reply("Send integer minutes between 0 and 10080.")
        return
    # set protect (already set) and autodelete and publish
    rec = await set_protect_and_autodelete(draft.id, draft.protect_content, minutes)
    if not rec:
        await message.reply("Failed to finalize session.")
        return
    # build deep link
    me = await bot.get_me()
    bot_username = me.username or "bot"
    deep_link = f"https://t.me/{bot_username}?start={rec.link}"
    await message.reply(f"✅ Session published.\nDeep link: {deep_link}\nProtect: {'ON' if rec.protect_content else 'OFF'}\nAutodelete: {rec.autodelete_minutes} minute(s)\nShare this link with others.")
    return


# /revoke owner-only: mark published session revoked
@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can revoke links.")
        return
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.reply("Usage: /revoke <token>")
        return
    token = parts[1].strip()
    ok = await revoke_session_by_token(token)
    if ok:
        await message.reply(f"✅ Token revoked: {token}")
    else:
        await message.reply("Token not found.")


# /edit_start owner-only: reply to a message to set start welcome
@dp.message_handler(commands=["edit_start"])
async def cmd_edit_start(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can edit the /start message.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or caption) that you want to set as the /start message.")
        return
    content = message.reply_to_message.text or message.reply_to_message.caption or ""
    if not content.strip():
        await message.reply("Replied message contains no text/caption.")
        return
    await save_start_message(content)
    await message.reply("✅ /start message updated. Placeholders supported: {first_name} and {label | url}")


# /help owner-only
@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use this /help for management instructions.")
        return
    await message.reply(
        "/upload - start upload session (owner only)\n"
        "/done - finalize upload session and create deep link (owner only)\n"
        "/abort - cancel upload session (owner only)\n"
        "/revoke <token> - revoke session link (owner only)\n"
        "/edit_start - set /start welcome message (owner only; reply to a message)\n"
        "/help - this help\n"
        "/start - general welcome or open deep link (/start <token>)"
    )


# catch-all for other text (non-command) messages: we used above for owner replies; rest ignore
@dp.message_handler()
async def catch_all(message: types.Message):
    # we intentionally do nothing for general messages from non-owner or unrelated messages
    return


# ------------------------------------------------------------
# Webhook handler: accept updates and set aiogram context correctly
# ------------------------------------------------------------
async def webhook_handler(request: Request):
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400, text="invalid json")
    try:
        update = types.Update.de_json(data)
    except Exception:
        try:
            update = types.Update(**data)
        except Exception:
            logger.exception("Failed to parse Update")
            return web.Response(status=400, text="bad update")

    # Crucial: set current bot & dispatcher in this context so message.answer() works
    try:
        Bot.set_current(bot)
    except Exception:
        pass
    try:
        Dispatcher.set_current(dp)
    except Exception:
        pass

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
# Autodelete background worker (deletes persisted scheduled messages)
# ------------------------------------------------------------
AUTODELETE_CHECK_INTERVAL = 30  # seconds


async def autodelete_worker():
    logger.info("Autodelete worker started (interval %s seconds)", AUTODELETE_CHECK_INTERVAL)
    while True:
        try:
            async with AsyncSessionLocal() as db:
                now = datetime.utcnow()
                stmt = select(DeliveryModel).where(DeliveryModel.delete_at <= now)
                res = await db.execute(stmt)
                due = res.scalars().all()
                if due:
                    logger.info("Autodelete: %d messages due", len(due))
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
# Startup: init DB, start autodelete worker, start aiohttp app, set webhook
# ------------------------------------------------------------
async def start_services_and_run():
    # initialize DB
    try:
        await init_db()
    except Exception:
        logger.exception("init_db failed")

    # start autodelete background worker
    asyncio.create_task(autodelete_worker())
    logger.info("Background tasks started (autodelete)")

    # create aiohttp app
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, webhook_handler)
    app.router.add_get(WEBHOOK_PATH, lambda req: web.Response(text="Webhook endpoint (GET)"))
    app.router.add_get("/health", health_handler)
    app.router.add_get("/", lambda req: web.Response(text="Bot running"))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("aiohttp server started on port %s", PORT)

    # register webhook with Telegram
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except Exception:
        logger.exception("Failed to set webhook to %s", WEBHOOK_URL)

    # keep running until cancelled
    try:
        await asyncio.Event().wait()
    finally:
        logger.info("Shutting down: removing webhook and cleaning up")
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
    logger.info("Starting bot: webhook_host=%s port=%s", WEBHOOK_HOST, PORT)
    try:
        asyncio.run(start_services_and_run())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down due to KeyboardInterrupt/SystemExit")
    except Exception:
        logger.exception("Unhandled exception in main")
        sys.exit(1)


if __name__ == "__main__":
    main()