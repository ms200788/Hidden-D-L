# bot.py
"""
Final session file-sharing bot for Telegram
Framework: aiogram 2.25.0
DB: PostgreSQL via SQLAlchemy async + asyncpg
Features:
 - Webhook mode, /health endpoint
 - Draft upload flow: /upload -> send files -> /done -> protect on/off -> autodelete minutes -> published deep link
 - /abort to cancel a draft
 - /revoke <token> to revoke published links
 - /edit_start (owner only) to set start message (photo+caption or text)
 - /start shows welcome or serves deep-link (/start <token>)
 - Auto-create tables and safe auto-migration of missing columns
 - Uses upload channel to store files and keeps only stable Telegram file_ids in DB (low DB usage)
 - Supports all Telegram media types and up to 99 files/session
 - Owner bypasses protect_content
 - Autodelete scheduling persisted and worker cleans up
"""

import os
import sys
import asyncio
import logging
import secrets
import traceback
import html
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

from aiohttp import web
from aiohttp.web_request import Request

from aiogram import Bot, Dispatcher, types
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
    Index,
    text as sa_text,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# ---------------------------------------------------------------------
# Configuration & Logging
# ---------------------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("session_share_bot")

BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID_ENV = os.environ.get("OWNER_ID", "6169237879")
DATABASE_URL = os.environ.get("DATABASE_URL")  # Must include +asyncpg
UPLOAD_CHANNEL_ID_ENV = os.environ.get("UPLOAD_CHANNEL_ID")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # e.g. https://hidden-fnxx.onrender.com
PORT = int(os.environ.get("PORT", "10000"))
MAX_FILES_PER_SESSION = int(os.environ.get("MAX_FILES_PER_SESSION", "99"))
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

# ---------------------------------------------------------------------
# Database models
# ---------------------------------------------------------------------
Base = declarative_base()


class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column(String(128), unique=True, nullable=False, index=True)  # random token
    owner_id = Column(Integer, nullable=False)
    status = Column(String(32), nullable=False, default="draft")  # draft / awaiting_protect / awaiting_autodelete / published / revoked
    protect_content = Column(Boolean, default=False)
    autodelete_minutes = Column(Integer, default=0)
    revoked = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    files = relationship("FileModel", back_populates="session", cascade="all, delete-orphan")


class FileModel(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False)
    tg_file_id = Column(String, nullable=False)  # stable Telegram file_id from upload channel
    file_type = Column(String(32), nullable=False)
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
    photo_file_id = Column(String, nullable=True)  # may be added later by auto-migration
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


Index("ix_files_session_order", FileModel.session_id, FileModel.order_index)

# ---------------------------------------------------------------------
# Async engine & sessionmaker
# ---------------------------------------------------------------------
# DATABASE_URL must include +asyncpg, e.g. postgresql+asyncpg://user:pass@host:port/db
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


# ---------------------------------------------------------------------
# Auto-migration helper (safe additions)
# ---------------------------------------------------------------------
async def ensure_tables_and_columns():
    """
    Create missing tables and attempt safe additions of new columns.
    This avoids runtime errors like 'UndefinedColumnError' on upgrades.
    """
    logger.info("Applying DB create_all and safe auto-migrations...")
    async with engine.begin() as conn:
        # create tables if missing
        await conn.run_sync(Base.metadata.create_all)

        # For Postgres, ALTER TABLE ... ADD COLUMN IF NOT EXISTS is supported
        # Try to add known columns that might be missing.
        try:
            await conn.execute(sa_text("ALTER TABLE IF EXISTS start_message ADD COLUMN IF NOT EXISTS photo_file_id TEXT"))
        except Exception as e:
            logger.warning("Auto-migration: could not add start_message.photo_file_id: %s", e)

        try:
            await conn.execute(sa_text("ALTER TABLE IF EXISTS sessions ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'draft'"))
        except Exception as e:
            logger.warning("Auto-migration: could not add sessions.status: %s", e)

        try:
            await conn.execute(sa_text("ALTER TABLE IF EXISTS sessions ADD COLUMN IF NOT EXISTS protect_content BOOLEAN DEFAULT FALSE"))
        except Exception as e:
            logger.warning("Auto-migration: could not add sessions.protect_content: %s", e)

        try:
            await conn.execute(sa_text("ALTER TABLE IF EXISTS sessions ADD COLUMN IF NOT EXISTS autodelete_minutes INTEGER DEFAULT 0"))
        except Exception as e:
            logger.warning("Auto-migration: could not add sessions.autodelete_minutes: %s", e)

    logger.info("Auto-migration attempts finished.")


# ---------------------------------------------------------------------
# Aiogram bot & dispatcher (v2)
# ---------------------------------------------------------------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)

# Semaphore to throttle concurrent deliveries
delivery_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DELIVERIES)

# ---------------------------------------------------------------------
# Utility helpers: token, render text -> clickable links, DB helper functions
# ---------------------------------------------------------------------
def generate_token(length: int = 64) -> str:
    return secrets.token_urlsafe(length)[:length]


def render_text_with_links(text: Optional[str], first_name: Optional[str] = None) -> str:
    """
    Render text by:
     - replacing {first_name} with provided name
     - converting {word | url} into HTML <a href="url">word</a>
    Returns HTML-safe string ready to send as parse_mode="HTML".
    """
    if not text:
        return ""
    # Replace {first_name}
    rendered = text.replace("{first_name}", html.escape(first_name or ""))
    # Now convert {word | url} (possibly multiple)
    out_parts: List[str] = []
    i = 0
    L = len(rendered)
    while i < L:
        s = rendered.find("{", i)
        if s == -1:
            out_parts.append(html.escape(rendered[i:]))
            break
        e = rendered.find("}", s)
        if e == -1:
            out_parts.append(html.escape(rendered[i:]))
            break
        # append text before {
        out_parts.append(html.escape(rendered[i:s]))
        inner = rendered[s + 1:e].strip()
        if "|" in inner:
            left, right = inner.split("|", 1)
            left = left.strip()
            right = right.strip()
            # HTML escape left & right correctly (href must be raw)
            left_escaped = html.escape(left)
            href = html.escape(right, quote=True)
            out_parts.append(f'<a href="{href}">{left_escaped}</a>')
        else:
            out_parts.append(html.escape("{" + inner + "}"))
        i = e + 1
    return "".join(out_parts)


# -------------------- DB helpers --------------------
async def init_db():
    await ensure_tables_and_columns()


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


async def append_file_to_session(session_id: int, tg_file_id: str, file_type: str, caption: str) -> FileModel:
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
    logger.debug("Scheduled deletion for %s:%s at %s", chat_id, message_id, delete_at.isoformat())


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
    logger.info("Saved /start message (text present=%s, photo present=%s)", bool(content), bool(photo_file_id))


async def fetch_start_message() -> Tuple[Optional[str], Optional[str]]:
    async with AsyncSessionLocal() as db:
        stmt = select(StartMessage).order_by(StartMessage.updated_at.desc())
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if rec:
            return rec.content, rec.photo_file_id
        return None, None


# ---------------------------------------------------------------------
# Send helper to honor protect_content where supported
# ---------------------------------------------------------------------
async def send_media_with_protect(file_type: str, chat_id: int, tg_file_id: str, caption: Optional[str], protect: bool):
    kwargs: Dict[str, Any] = {}
    if caption:
        # captions may contain {word | url} and {first_name}, but captions come from original uploader.
        # We will assume captions should be rendered before sending.
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
        elif file_type == "animation":
            return await bot.send_animation(chat_id=chat_id, animation=tg_file_id, **kwargs)
        else:
            return await bot.send_document(chat_id=chat_id, document=tg_file_id, **kwargs)
    except TypeError:
        # Fallback if protect_content param isn't accepted
        kwargs.pop("protect_content", None)
        if "caption" in kwargs and kwargs["caption"] is None:
            kwargs.pop("caption", None)
        if file_type == "photo":
            return await bot.send_photo(chat_id=chat_id, photo=tg_file_id, **kwargs)
        elif file_type == "video":
            return await bot.send_video(chat_id=chat_id, video=tg_file_id, **kwargs)
        elif file_type == "document":
            return await bot.send_document(chat_id=chat_id, document=tg_file_id, **kwargs)
        elif file_type == "audio":
            return await bot.send_audio(chat_id=chat_id, audio=tg_file_id, **kwargs)
        elif file_type == "voice":
            return await bot.send_voice(chat_id=chat_id, voice=tg_file_id, **kwargs)
        elif file_type == "sticker":
            return await bot.send_sticker(chat_id=chat_id, sticker=tg_file_id, **kwargs)
        elif file_type == "animation":
            return await bot.send_animation(chat_id=chat_id, animation=tg_file_id, **kwargs)
        else:
            return await bot.send_document(chat_id=chat_id, document=tg_file_id, **kwargs)
    except Exception:
        logger.exception("Failed to send media to %s", chat_id)
        raise


# ---------------------------------------------------------------------
# Autodelete worker (persisted)
# ---------------------------------------------------------------------
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
                        logger.exception("Autodelete: failed to remove DB record")
                await db.commit()
        except Exception:
            logger.exception("Autodelete worker exception: %s", traceback.format_exc())
        await asyncio.sleep(AUTODELETE_CHECK_INTERVAL)


# ---------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------
@dp.message_handler(commands=["start"])
async def handle_start(message: types.Message):
    # parse arg token if present
    args = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            args = parts[1].strip()

    if not args:
        content, photo = await fetch_start_message()
        if photo:
            # photo welcome
            try:
                caption_html = render_text_with_links(content, message.from_user.first_name)
                # send as HTML
                await message.answer_photo(photo=photo, caption=caption_html, parse_mode="HTML")
            except Exception:
                try:
                    await message.answer(render_text_with_links(content, message.from_user.first_name), parse_mode="HTML")
                except Exception:
                    logger.exception("Failed to send /start fallback")
        else:
            if content:
                try:
                    await message.answer(render_text_with_links(content, message.from_user.first_name), parse_mode="HTML", disable_web_page_preview=True)
                except Exception:
                    await message.answer(content.replace("{first_name}", message.from_user.first_name or ""))
            else:
                await message.answer(f"Welcome, {message.from_user.first_name or 'friend'}!")
        return

    # treat arg as token for deep link
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
        await message.answer("❌ No files found for this link.")
        return

    delivered = 0
    for f in files:
        try:
            # render caption: replace {first_name} and {word | url}
            caption_text = f.caption or ""
            caption_html = render_text_with_links(caption_text, message.from_user.first_name)
            protect_flag = False if message.from_user.id == OWNER_ID else bool(session_obj.protect_content)
            await delivery_semaphore.acquire()
            try:
                sent = await send_media_with_protect(f.file_type, chat_id=message.chat.id, tg_file_id=f.tg_file_id, caption=caption_html or None, protect=protect_flag)
            finally:
                delivery_semaphore.release()
            delivered += 1
            if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
                delete_time = datetime.utcnow() + timedelta(minutes=session_obj.autodelete_minutes)
                await schedule_delivery(chat_id=sent.chat.id, message_id=sent.message_id, delete_at=delete_time)
        except Exception:
            logger.exception("Failed to deliver file %s for session %s", f.tg_file_id, token)
    if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
        await message.answer(f"Files delivered: {delivered}. They will be deleted after {session_obj.autodelete_minutes} minute(s).")
    else:
        await message.answer(f"Files delivered: {delivered}. (Autodelete disabled)")


@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /upload.")
        return
    existing = await get_owner_active_draft(OWNER_ID)
    if existing:
        await message.reply(f"You already have an active draft session. Continue sending files or use /done to finalize. Draft token: {existing.link}")
        return
    rec = await create_draft_session(OWNER_ID)
    await message.reply(f"Upload session started.\nDraft token: {rec.link}\nSend up to {MAX_FILES_PER_SESSION} files (any Telegram type). When finished send /done. To cancel send /abort.")


@dp.message_handler(content_types=["photo", "video", "document", "audio", "voice", "sticker", "animation"])
async def handle_owner_media(message: types.Message):
    # only owner can upload
    if message.from_user.id != OWNER_ID:
        return

    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        # not in upload mode
        return

    files = await list_files_for_session(draft.id)
    if len(files) >= MAX_FILES_PER_SESSION:
        await message.reply(f"Upload limit reached ({MAX_FILES_PER_SESSION}). Use /done to finalize or /abort to cancel.")
        return

    # determine file type and original file_id
    ftype = None
    orig_file_id = None
    caption = None
    try:
        if message.photo:
            ftype = "photo"; orig_file_id = message.photo[-1].file_id; caption = message.caption or ""
        elif message.video:
            ftype = "video"; orig_file_id = message.video.file_id; caption = message.caption or ""
        elif message.document:
            ftype = "document"; orig_file_id = message.document.file_id; caption = message.caption or ""
        elif message.audio:
            ftype = "audio"; orig_file_id = message.audio.file_id; caption = message.caption or ""
        elif message.voice:
            ftype = "voice"; orig_file_id = message.voice.file_id; caption = ""
        elif message.sticker:
            ftype = "sticker"; orig_file_id = message.sticker.file_id; caption = ""
        elif message.animation:
            ftype = "animation"; orig_file_id = message.animation.file_id; caption = message.caption or ""
    except Exception:
        logger.exception("Error extracting file info from owner message")
        await message.reply("Failed to read the file. Try sending again.")
        return

    # forward to upload channel to obtain stable file_id
    try:
        forwarded = await bot.forward_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
    except Exception as e:
        logger.exception("Failed to forward to upload channel: %s", e)
        await message.reply("Failed to forward to upload channel. Ensure bot is admin and can post there.")
        return

    # determine stable file_id and type from forwarded
    channel_file_id = None
    channel_file_type = None
    channel_caption = None
    try:
        if forwarded.photo:
            channel_file_type = "photo"; channel_file_id = forwarded.photo[-1].file_id; channel_caption = forwarded.caption or ""
        elif forwarded.video:
            channel_file_type = "video"; channel_file_id = forwarded.video.file_id; channel_caption = forwarded.caption or ""
        elif forwarded.document:
            channel_file_type = "document"; channel_file_id = forwarded.document.file_id; channel_caption = forwarded.caption or ""
        elif forwarded.audio:
            channel_file_type = "audio"; channel_file_id = forwarded.audio.file_id; channel_caption = forwarded.caption or ""
        elif forwarded.voice:
            channel_file_type = "voice"; channel_file_id = forwarded.voice.file_id; channel_caption = ""
        elif forwarded.sticker:
            channel_file_type = "sticker"; channel_file_id = forwarded.sticker.file_id; channel_caption = ""
        elif forwarded.animation:
            channel_file_type = "animation"; channel_file_id = forwarded.animation.file_id; channel_caption = forwarded.caption or ""
        else:
            channel_file_type = ftype; channel_file_id = orig_file_id; channel_caption = caption
    except Exception:
        logger.exception("Error extracting file id from forwarded; falling back to original")
        channel_file_type = ftype; channel_file_id = orig_file_id; channel_caption = caption

    # store stable file_id in DB (low storage)
    try:
        await append_file_to_session(draft.id, channel_file_id, channel_file_type, channel_caption or "")
        current_count = len(await list_files_for_session(draft.id))
        await message.reply(f"Saved {channel_file_type}. ({current_count}/{MAX_FILES_PER_SESSION}) Send more or /done when finished.")
    except Exception:
        logger.exception("Failed to append file to DB")
        await message.reply("Failed to save file metadata to DB. Try again or /abort.")


@dp.message_handler(commands=["done"])
async def cmd_done(message: types.Message):
    """
    When owner sends /done:
      - confirm all files for session are ready
      - move session to awaiting_protect state
      - prompt owner for protect on/off
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
        await message.reply("No files uploaded in this session. Upload files first or /abort.")
        return
    # Set awaiting_protect
    await set_session_status(draft.id, "awaiting_protect")
    await message.reply("All files for this session are ready. Protect content? Reply with `on` or `off`.")


# owner replies on/off for protect when in awaiting_protect state
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and (m.text or "").strip().lower() in ("on", "off"))
async def owner_protect_reply(message: types.Message):
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft or draft.status != "awaiting_protect":
        return
    text = (message.text or "").strip().lower()
    protect = text == "on"
    # set protect flag and move to awaiting_autodelete
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.id == draft.id).limit(1)
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if not rec:
            await message.reply("Session not found.")
            return
        rec.protect_content = protect
        rec.status = "awaiting_autodelete"
        db.add(rec)
        await db.commit()
    await message.reply(f"Protect set to {'ON' if protect else 'OFF'}. Now reply with autodelete minutes (0 - 10080). 0 = disabled.")


# owner replies autodelete minutes (awaiting_autodelete)
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and (m.text or "").strip().isdigit())
async def owner_autodelete_reply(message: types.Message):
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft or draft.status != "awaiting_autodelete":
        return
    text = (message.text or "").strip()
    try:
        minutes = int(text)
        if minutes < 0 or minutes > 10080:
            raise ValueError
    except ValueError:
        await message.reply("Please provide an integer between 0 and 10080.")
        return
    rec = await set_protect_and_autodelete(draft.id, draft.protect_content, minutes)
    if not rec:
        await message.reply("Failed to finalize session.")
        return
    me = await bot.get_me()
    bot_username = me.username or "bot"
    deep_link = f"https://t.me/{bot_username}?start={rec.link}"
    await message.reply(f"✅ Session published.\nDeep link:\n{deep_link}\nProtect: {'ON' if rec.protect_content else 'OFF'}\nAutodelete: {rec.autodelete_minutes} minute(s)\nShare this link. Use /revoke <token> to disable.")


@dp.message_handler(commands=["abort"])
async def cmd_abort(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /abort.")
        return
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        await message.reply("No active upload session.")
        return
    ok = await delete_draft_session(draft.id)
    if ok:
        await message.reply("Upload aborted and draft removed from DB. Forwarded messages in upload channel remain.")
    else:
        await message.reply("Failed to abort session.")


@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can /revoke.")
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
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /edit_start.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or photo) to set /start. Photo+caption will set image+caption.")
        return
    reply = message.reply_to_message
    if reply.photo:
        photo_file_id = reply.photo[-1].file_id
        caption = reply.caption or ""
        await save_start_message(caption, photo_file_id)
        await message.reply("✅ /start updated with image + caption (if caption present).")
        return
    if reply.text:
        await save_start_message(reply.text, None)
        await message.reply("✅ /start updated with text.")
        return
    if reply.caption:
        await save_start_message(reply.caption, None)
        await message.reply("✅ /start updated with caption.")
        return
    await message.reply("Unsupported message type. Reply to text or photo.")


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /help.")
        return
    await message.reply(
        "Owner commands:\n"
        "/upload - start upload\n"
        "/done - finalize upload (owner replies on/off then minutes)\n"
        "/abort - cancel upload\n"
        "/revoke <token> - revoke a session\n"
        "/edit_start - reply to message to set /start (photo or text)\n"
        "/help - this message\n\n"
        "Public: /start - show welcome or use /start <token> to fetch session files"
    )


@dp.message_handler()
async def fallback(message: types.Message):
    # intentionally ignore other messages
    return


# ---------------------------------------------------------------------
# Webhook handler (aiohttp) — must set current bot/dispatcher for aiogram v2 context
# ---------------------------------------------------------------------
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
            logger.exception("Failed to parse update")
            return web.Response(status=400, text="bad update")

    # set current context for aiogram
    try:
        Bot.set_current(bot)
    except Exception:
        pass
    try:
        Dispatcher.set_current(dp)
    except Exception:
        try:
            dp.set_current(dp)
        except Exception:
            pass

    try:
        await dp.process_update(update)
    except Exception:
        logger.exception("Dispatcher processing failed: %s", traceback.format_exc())
    return web.Response(text="OK")


async def health_handler(request: Request):
    return web.Response(text="OK")


# ---------------------------------------------------------------------
# Startup: init DB, start autodelete worker, start aiohttp server, set webhook
# ---------------------------------------------------------------------
async def start_services_and_run():
    try:
        await init_db()
    except Exception:
        logger.exception("init_db failed")

    # start autodelete worker
    asyncio.create_task(autodelete_worker())
    logger.info("Autodelete worker started.")

    # setup aiohttp app
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

    # set webhook with Telegram
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except Exception:
        logger.exception("Failed to set webhook to %s", WEBHOOK_URL)

    try:
        await asyncio.Event().wait()
    finally:
        logger.info("Shutting down gracefully")
        try:
            await bot.delete_webhook()
        except Exception:
            logger.exception("Failed to delete webhook")
        await runner.cleanup()
        try:
            await bot.close()
        except Exception:
            logger.exception("Failed to close bot cleanly")


def main():
    logger.info("Launching session_share_bot; webhook_host=%s port=%s", WEBHOOK_HOST, PORT)
    try:
        asyncio.run(start_services_and_run())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt - exiting")
    except Exception:
        logger.exception("Fatal error in main")
        sys.exit(1)


if __name__ == "__main__":
    main()