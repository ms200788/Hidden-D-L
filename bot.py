# bot.py
"""
Session-based file-sharing Telegram bot (aiogram 2.25.0)
- Webhook mode (aiohttp)
- PostgreSQL (async) using SQLAlchemy + asyncpg
- Stores only metadata and stable Telegram file_ids from a private upload channel (minimal DB size)
- Draft -> publish upload flow durable in DB (survives restarts)
- Random 64-char deep links per session: https://t.me/<bot>?start=<token>
- Supports up to 99 files per session (all Telegram media types)
- Owner-only management commands; /start is public
- /edit_start supports reply-to-photo (with caption) or reply-to-text
- Autodelete scheduling persisted in DB (optional per-session)
- Health endpoint /health for uptime monitoring (port 8080 when separated), webhook listens on PORT
"""

# Standard library
import os
import sys
import asyncio
import logging
import secrets
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

# aiohttp for webhook & health endpoints
from aiohttp import web
from aiohttp.web_request import Request

# aiogram v2 imports
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage

# SQLAlchemy async
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    select,
    func,
    Index,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# ---------------------------
# Logging
# ---------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("file_share_bot")

# ---------------------------
# Environment variables & basic validation
# ---------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID_ENV = os.environ.get("OWNER_ID", "6169237879")
DATABASE_URL = os.environ.get("DATABASE_URL")  # must include +asyncpg
UPLOAD_CHANNEL_ID_ENV = os.environ.get("UPLOAD_CHANNEL_ID")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # e.g. https://hidden-fnxx.onrender.com
PORT = int(os.environ.get("PORT", "10000"))
MAX_CONCURRENT_DELIVERIES = int(os.environ.get("MAX_CONCURRENT_DELIVERIES", "50"))
MAX_FILES_PER_SESSION = int(os.environ.get("MAX_FILES_PER_SESSION", "99"))

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

logger.info("Starting bot with OWNER_ID=%s, UPLOAD_CHANNEL_ID=%s, WEBHOOK_HOST=%s, PORT=%s",
            OWNER_ID, UPLOAD_CHANNEL_ID, WEBHOOK_HOST, PORT)

# ---------------------------
# Database models using SQLAlchemy (async)
# ---------------------------
Base = declarative_base()


class SessionModel(Base):
    """
    A session groups multiple files uploaded together.
    link: 64-char random token used in deep link
    status: draft -> awaiting_protect -> awaiting_autodelete -> published -> revoked
    protect_content: if True, recipients receive sent messages with protect_content flag (owner bypasses)
    autodelete_minutes: 0 means disabled; otherwise delivered messages are scheduled for deletion
    """
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column(String(128), unique=True, nullable=False, index=True)
    owner_id = Column(Integer, nullable=False)
    status = Column(String(32), nullable=False, default="draft")
    protect_content = Column(Boolean, default=False)
    autodelete_minutes = Column(Integer, default=0)
    revoked = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    files = relationship("FileModel", back_populates="session", cascade="all, delete-orphan")


class FileModel(Base):
    """
    A file record holds a stable Telegram file_id (from the upload channel)
    and the order index to preserve order.
    """
    __tablename__ = "files"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False)
    tg_file_id = Column(String, nullable=False)  # Telegram file_id (stable)
    file_type = Column(String(32), nullable=False)
    caption = Column(String, nullable=True)
    order_index = Column(Integer, nullable=False)

    session = relationship("SessionModel", back_populates="files")


class DeliveryModel(Base):
    """
    Persist scheduled deletions so they survive restarts.
    """
    __tablename__ = "deliveries"
    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(Integer, nullable=False)
    message_id = Column(Integer, nullable=False)
    delete_at = Column(DateTime(timezone=True), nullable=True)


class StartMessage(Base):
    """
    Stores the /start welcome message (text) and optional photo file_id for image + caption variant.
    """
    __tablename__ = "start_message"
    id = Column(Integer, primary_key=True, autoincrement=True)
    content = Column(String, nullable=True)
    photo_file_id = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


Index("ix_files_session_order", FileModel.session_id, FileModel.order_index)

# ---------------------------
# Async engine & sessionmaker
# ---------------------------
# DATABASE_URL must include asyncpg driver, e.g. postgresql+asyncpg://user:pass@host:port/dbname
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    """
    Create tables if missing. Convenient for Render deployments without migrations.
    """
    logger.info("Initializing database (creating tables if needed)...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialization complete.")


# ---------------------------
# Bot & Dispatcher (aiogram v2)
# ---------------------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)

# Semaphore to limit concurrent deliveries
delivery_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DELIVERIES)

# ---------------------------
# Utility helpers
# ---------------------------
def generate_token(length: int = 64) -> str:
    """
    Generate a URL-safe random token truncated to exact length.
    """
    return secrets.token_urlsafe(length)[:length]


# DB helpers
async def create_draft_session(owner_id: int) -> SessionModel:
    async with AsyncSessionLocal() as db:
        token = generate_token(64)
        rec = SessionModel(link=token, owner_id=owner_id, status="draft")
        db.add(rec)
        await db.commit()
        await db.refresh(rec)
        return rec


async def get_owner_active_draft(owner_id: int) -> Optional[SessionModel]:
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.owner_id == owner_id, SessionModel.status == "draft")
        res = await db.execute(stmt)
        return res.scalars().first()


async def append_file_to_session(session_id: int, tg_file_id: str, file_type: str, caption: str):
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(func.max(FileModel.order_index)).where(FileModel.session_id == session_id))
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


async def set_session_status(session_id: int, status: str) -> Optional[SessionModel]:
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.id == session_id).limit(1)
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if rec:
            rec.status = status
            db.add(rec)
            await db.commit()
            await db.refresh(rec)
            return rec
        return None


async def set_protect_and_autodelete(session_id: int, protect: bool, autodelete: int) -> Optional[SessionModel]:
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
            await db.refresh(rec)
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


async def delete_draft_session(session_id: int) -> bool:
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.id == session_id).limit(1)
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if rec:
            await db.delete(rec)
            await db.commit()
            return True
        return False


async def get_session_by_token(token: str) -> Optional[SessionModel]:
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.link == token).limit(1)
        res = await db.execute(stmt)
        return res.scalars().first()


async def schedule_delivery(chat_id: int, message_id: int, delete_at: Optional[datetime]):
    if delete_at is None:
        return
    async with AsyncSessionLocal() as db:
        rec = DeliveryModel(chat_id=chat_id, message_id=message_id, delete_at=delete_at)
        db.add(rec)
        await db.commit()
    logger.debug("Scheduled deletion: %s:%s at %s", chat_id, message_id, delete_at.isoformat())


async def save_start_message(content: Optional[str], photo_file_id: Optional[str] = None):
    async with AsyncSessionLocal() as db:
        stmt = select(StartMessage).order_by(StartMessage.updated_at.desc())
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if rec:
            rec.content = content
            rec.photo_file_id = photo_file_id
            db.add(rec)
        else:
            rec = StartMessage(content=content, photo_file_id=photo_file_id)
            db.add(rec)
        await db.commit()
    logger.info("Saved /start message (text: %s, photo: %s)", bool(content), bool(photo_file_id))


async def fetch_start_message() -> Tuple[Optional[str], Optional[str]]:
    async with AsyncSessionLocal() as db:
        stmt = select(StartMessage).order_by(StartMessage.updated_at.desc())
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if rec:
            return rec.content, rec.photo_file_id
        return None, None


# ---------------------------
# Sending helper that supports protect_content fallback
# ---------------------------
async def send_media_with_protect(file_type: str, chat_id: int, tg_file_id: str, caption: Optional[str], protect: bool):
    """
    Try to send with protect_content first; if the aiogram method doesn't accept it, retry without.
    """
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
        # fallback if protect_content not in method signature
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
        logger.exception("Failed to send media to %s", chat_id)
        raise


# ---------------------------
# Autodelete worker
# ---------------------------
AUTODELETE_CHECK_INTERVAL = 30  # seconds


async def autodelete_worker():
    logger.info("Autodelete worker started; checking every %s seconds", AUTODELETE_CHECK_INTERVAL)
    while True:
        try:
            async with AsyncSessionLocal() as db:
                now = datetime.utcnow()
                stmt = select(DeliveryModel).where(DeliveryModel.delete_at <= now)
                res = await db.execute(stmt)
                due = res.scalars().all()
                if due:
                    logger.info("Autodelete: found %d messages", len(due))
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


# ---------------------------
# Bot command handlers
# ---------------------------
@dp.message_handler(commands=["start"])
async def handle_start(message: types.Message):
    """
    /start - if no args: show welcome (supports photo+caption or text).
             if args: treat as deep link token and deliver files in order.
    """
    args = message.get_args().strip() if message.get_args() else ""
    if not args:
        content, photo = await fetch_start_message()
        if photo:
            # photo + caption path
            try:
                await message.answer_photo(photo=photo, caption=(content or "").replace("{first_name}", message.from_user.first_name or ""))
            except Exception:
                try:
                    await message.answer((content or "").replace("{first_name}", message.from_user.first_name or ""))
                except Exception:
                    logger.exception("Failed to send start message fallback")
        else:
            if content:
                try:
                    rendered = content.replace("{first_name}", message.from_user.first_name or "")
                    # support {label | url}
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
                    await message.answer(final_text, parse_mode="HTML", disable_web_page_preview=True)
                except Exception:
                    await message.answer(content.replace("{first_name}", message.from_user.first_name or ""))
            else:
                await message.answer(f"Welcome, {message.from_user.first_name or 'friend'}!")
        return

    # deep link path
    token = args
    session_obj = await get_session_by_token(token)
    if not session_obj:
        await message.answer("❌ Link not found or invalid.")
        return
    if session_obj.revoked or session_obj.status == "revoked":
        await message.answer("❌ This link has been revoked by the owner.")
        return
    if session_obj.status != "published":
        await message.answer("❌ This link is not published yet.")
        return

    files = await list_files_for_session(session_obj.id)
    if not files:
        await message.answer("❌ No files for this link.")
        return

    delivered = 0
    for f in files:
        try:
            protect_flag = False if message.from_user.id == OWNER_ID else bool(session_obj.protect_content)
            await delivery_semaphore.acquire()
            try:
                sent = await send_media_with_protect(f.file_type, chat_id=message.chat.id, tg_file_id=f.tg_file_id, caption=f.caption, protect=protect_flag)
            finally:
                delivery_semaphore.release()
            delivered += 1
            if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
                delete_time = datetime.utcnow() + timedelta(minutes=session_obj.autodelete_minutes)
                await schedule_delivery(chat_id=sent.chat.id, message_id=sent.message_id, delete_at=delete_time)
        except Exception:
            logger.exception("Failed to deliver file %s in session %s", f.tg_file_id, token)
    if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
        await message.answer(f"Files delivered: {delivered}. They will be deleted after {session_obj.autodelete_minutes} minute(s).")
    else:
        await message.answer(f"Files delivered: {delivered}. (Autodelete disabled)")


@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    """
    Owner-only command to start a draft upload session.
    Creates a draft in DB; owner sends media which are forwarded into UPLOAD_CHANNEL_ID
    and the stable channel file_id is stored.
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /upload.")
        return
    existing = await get_owner_active_draft(OWNER_ID)
    if existing:
        await message.reply(f"You already have an active draft session. Continue sending files or use /done to finish. Draft token: {existing.link}")
        return
    rec = await create_draft_session(OWNER_ID)
    await message.reply(f"Upload session started.\nDraft token: {rec.link}\nSend up to {MAX_FILES_PER_SESSION} files (photo/video/document/audio/voice/sticker). When finished send /done. To cancel send /abort.")


# Media handler: captures owner media while draft exists, forwards to upload channel and stores stable file_id
@dp.message_handler(content_types=["photo", "video", "document", "audio", "voice", "sticker", "animation"])
async def handle_owner_media(message: types.Message):
    # Only owner allowed to upload
    if message.from_user.id != OWNER_ID:
        return

    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        # Not in upload mode
        return

    # enforce max files limit
    files = await list_files_for_session(draft.id)
    if len(files) >= MAX_FILES_PER_SESSION:
        await message.reply(f"Upload limit reached ({MAX_FILES_PER_SESSION}). Use /done to finalize or /abort to cancel.")
        return

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
        elif message.animation:
            ftype = "animation"
            orig_file_id = message.animation.file_id
            caption = message.caption or ""
    except Exception:
        logger.exception("Error extracting file info from owner message")
        await message.reply("Failed to read the file. Try sending again.")
        return

    # Forward to upload channel to get the stable file_id
    try:
        fwd_msg = await bot.forward_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
    except Exception as e:
        logger.exception("Failed to forward to upload channel: %s", e)
        await message.reply("Failed to forward to upload channel. Ensure the bot is admin and allowed to post.")
        return

    # extract channel-side stable file_id
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
        elif fwd_msg.animation:
            channel_file_type = "animation"
            channel_file_id = fwd_msg.animation.file_id
            channel_caption = fwd_msg.caption or ""
        else:
            # fallback to the original captured ids
            channel_file_type = ftype
            channel_file_id = orig_file_id
            channel_caption = caption
    except Exception:
        logger.exception("Error extracting file id from forwarded message; falling back to original ids")
        channel_file_type = ftype
        channel_file_id = orig_file_id
        channel_caption = caption

    # store in DB
    try:
        await append_file_to_session(draft.id, channel_file_id, channel_file_type, channel_caption)
        current_count = len(await list_files_for_session(draft.id))
        await message.reply(f"Saved {channel_file_type}. ({current_count}/{MAX_FILES_PER_SESSION}) Send more or /done when finished.")
    except Exception:
        logger.exception("Failed to append file to DB")
        await message.reply("Failed to save file metadata to DB. Try again or abort with /abort.")


@dp.message_handler(commands=["done"])
async def cmd_done(message: types.Message):
    """
    Owner finishes upload and the bot asks protect on/off, then autodelete minutes.
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /done.")
        return
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        await message.reply("No active upload session. Use /upload to start.")
        return
    files = await list_files_for_session(draft.id)
    if not files:
        await message.reply("No files uploaded in this session. Upload files first or /abort to cancel.")
        return
    await message.reply("Protect content? Reply with `on` or `off`.")
    await set_session_status(draft.id, "awaiting_protect")


# owner replies "on"/"off" for protect while session awaiting_protect
@dp.message_handler(lambda message: message.from_user.id == OWNER_ID and (message.text or "").strip().lower() in ("on", "off"))
async def handle_protect_reply(message: types.Message):
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        return
    if draft.status != "awaiting_protect":
        return
    text = (message.text or "").strip().lower()
    protect = text == "on"
    # set protect flag and move to awaiting_autodelete
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


# owner replies minutes when session awaiting_autodelete
@dp.message_handler(lambda message: message.from_user.id == OWNER_ID and (message.text or "").strip().isdigit())
async def handle_autodelete_reply(message: types.Message):
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
        await message.reply("Please send an integer between 0 and 10080.")
        return
    rec = await set_protect_and_autodelete(draft.id, draft.protect_content, minutes)
    if not rec:
        await message.reply("Failed to finalize session.")
        return
    me = await bot.get_me()
    bot_username = me.username or "bot"
    deep_link = f"https://t.me/{bot_username}?start={rec.link}"
    await message.reply(
        f"✅ Session published.\nDeep link (share this):\n{deep_link}\n\nProtect content: {'ON' if rec.protect_content else 'OFF'}\nAutodelete: {rec.autodelete_minutes} minute(s)\n\nUse /revoke <token> to disable this link."
    )


@dp.message_handler(commands=["abort"])
async def cmd_abort(message: types.Message):
    """
    Cancel the current draft session and remove it from DB.
    (Forwarded messages in upload channel remain because we did not store channel_message_id to keep DB tiny.)
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /abort.")
        return
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        await message.reply("No active upload session to abort.")
        return
    ok = await delete_draft_session(draft.id)
    if ok:
        await message.reply("Upload aborted and draft removed from DB. Forwarded messages in upload channel remain (we didn't store channel message ids).")
    else:
        await message.reply("Failed to abort session.")


@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can revoke sessions.")
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


@dp.message_handler(commands=["edit_start"])
async def cmd_edit_start(message: types.Message):
    """
    Owner-only. Reply to a message:
      - if reply contains photo -> save photo file_id & caption (if any) as /start content
      - if reply contains text -> save text as /start content
      - if reply contains caption on other media -> save caption text
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /edit_start.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or photo). If you reply to a photo, the image and caption (if any) will become the /start content.")
        return
    reply = message.reply_to_message
    # photo path
    if reply.photo:
        photo_file_id = reply.photo[-1].file_id
        caption = reply.caption or ""
        await save_start_message(caption, photo_file_id)
        await message.reply("✅ /start updated: image + caption saved (if caption present).")
        return
    # text path
    if reply.text:
        await save_start_message(reply.text, None)
        await message.reply("✅ /start updated with text.")
        return
    # caption-only (e.g., document with caption)
    if reply.caption:
        await save_start_message(reply.caption, None)
        await message.reply("✅ /start updated with caption text.")
        return
    await message.reply("Unsupported message type. Reply to text or photo.")


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /help for management.")
        return
    txt = (
        "Owner commands:\n"
        "/upload - start upload session\n"
        "/done - finalize upload (reply 'on'/'off' then minutes)\n"
        "/abort - cancel upload\n"
        "/revoke <token> - revoke a session\n"
        "/edit_start (reply to a message) - set /start content (photo+caption or text)\n"
        "/help - this message\n\n"
        "Universal: /start - show welcome or open deep link (/start <token>)"
    )
    await message.reply(txt)


# fallback: ignore other messages
@dp.message_handler()
async def fallback_handler(message: types.Message):
    # Intentionally do nothing for unrelated messages
    return


# ---------------------------
# Webhook endpoint (aiohttp)
# must set current Bot & Dispatcher so message.answer() works
# ---------------------------
async def webhook_handler(request: Request):
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400, text="invalid json")

    # parse update
    try:
        update = types.Update.de_json(data)
    except Exception:
        try:
            update = types.Update(**data)
        except Exception:
            logger.exception("Failed to parse update JSON")
            return web.Response(status=400, text="bad update")

    # set current bot & dispatcher for aiogram context
    try:
        Bot.set_current(bot)
    except Exception:
        pass
    try:
        Dispatcher.set_current(dp)
    except Exception:
        # aiogram v2 sometimes doesn't implement set_current on Dispatcher; ignore
        pass

    # process update
    try:
        await dp.process_update(update)
    except Exception:
        logger.exception("Dispatcher failed to process update: %s", traceback.format_exc())
    return web.Response(text="OK")


# Health endpoint
async def health_handler(request: Request):
    return web.Response(text="OK")


# ---------------------------
# Startup and run: init DB, start autodelete worker, start aiohttp server, set webhook
# ---------------------------
async def start_services_and_run():
    # init db
    try:
        await init_db()
    except Exception:
        logger.exception("init_db failed")

    # start autodelete worker
    asyncio.create_task(autodelete_worker())
    logger.info("Autodelete worker started.")

    # build aiohttp app
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, webhook_handler)
    app.router.add_get(WEBHOOK_PATH, lambda req: web.Response(text="Webhook endpoint (GET)"))
    app.router.add_get("/health", health_handler)
    app.router.add_get("/", lambda req: web.Response(text="Bot running"))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("aiohttp server started on port %s (webhook path: %s)", PORT, WEBHOOK_PATH)

    # register webhook with Telegram
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


def main():
    logger.info("Launching bot main; webhook_host=%s port=%s", WEBHOOK_HOST, PORT)
    try:
        asyncio.run(start_services_and_run())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt - exiting")
    except Exception:
        logger.exception("Fatal error in main")
        sys.exit(1)


if __name__ == "__main__":
    main()