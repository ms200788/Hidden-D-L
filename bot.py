# bot.py
# Session-share Telegram bot (single file) - Final fixed version
# - Fixes: /edit_start photo saving & sending, deep-link delivery of files
# - Adds: internal self-ping (uses WEBHOOK_HOST from env) to keep Render service alive
# - Keeps all original behavior, DB models, and flows unchanged otherwise.
#
# Required env vars:
#   BOT_TOKEN, OWNER_ID, DATABASE_URL, UPLOAD_CHANNEL_ID, WEBHOOK_HOST
# Optional:
#   PORT (default 10000), BOT_USERNAME (for deep links; fallback to bot.get_me())
#
# Deploy the same way as before (webhook + /health). This file intentionally
# only applies minimal fixes you asked for and adds the self-ping keepalive.
# ------------------------------------------------------------------------------

import os
import sys
import asyncio
import logging
import secrets
import html
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

import aiohttp
from aiohttp import web
from aiohttp.web_request import Request

# aiogram v2 imports
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage

# SQLAlchemy async
from sqlalchemy import (
    Column,
    String,
    Integer,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    select,
    func,
    text as sa_text,
    Index,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# Logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("session_share_bot_final")

# ---------------------------
# Configuration from .env
# ---------------------------

BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID_ENV = os.environ.get("OWNER_ID")
DATABASE_URL = os.environ.get("DATABASE_URL")  # e.g. postgresql+asyncpg://user:pass@host:5432/dbname
UPLOAD_CHANNEL_ID_ENV = os.environ.get("UPLOAD_CHANNEL_ID")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # e.g. https://yourdomain.com
PORT = int(os.environ.get("PORT", "10000"))

MAX_FILES_PER_SESSION = int(os.environ.get("MAX_FILES_PER_SESSION", "99"))
MAX_DELIVERY_CONCURRENCY = int(os.environ.get("MAX_DELIVERY_CONCURRENCY", "50"))
AUTODELETE_CHECK_INTERVAL = int(os.environ.get("AUTODELETE_CHECK_INTERVAL", "30"))  # seconds
DRAFT_TOKEN_LENGTH = int(os.environ.get("DRAFT_TOKEN_LENGTH", "48"))
AUTODELETE_MAX_MIN = int(os.environ.get("AUTODELETE_MAX_MIN", "10080"))  # 7 days

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
    raise RuntimeError("Missing required env vars: " + ", ".join(_missing))

try:
    OWNER_ID = int(OWNER_ID_ENV)
except Exception as e:
    raise RuntimeError("OWNER_ID must be integer.") from e

try:
    UPLOAD_CHANNEL_ID = int(UPLOAD_CHANNEL_ID_ENV)
except Exception as e:
    raise RuntimeError("UPLOAD_CHANNEL_ID must be integer.") from e

WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST.rstrip('/')}{WEBHOOK_PATH}"

# BOT_USERNAME may be provided; otherwise we'll fetch via API at runtime
BOT_USERNAME = os.environ.get("BOT_USERNAME")  # optional override, e.g. "Code_66_bot"

logger.info(
    "Configuration: OWNER_ID=%s, UPLOAD_CHANNEL_ID=%s, WEBHOOK_HOST=%s, PORT=%s, BOT_USERNAME=%s",
    OWNER_ID,
    UPLOAD_CHANNEL_ID,
    WEBHOOK_HOST,
    PORT,
    BOT_USERNAME,
)

# ---------------------------
# Database models
# ---------------------------

Base = declarative_base()


class UserModel(Base):
    __tablename__ = "users"
    id = Column(BigInteger, primary_key=True)  # Telegram user id as BIGINT
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    username = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    link = Column(String(128), unique=True, nullable=False, index=True)
    owner_id = Column(BigInteger, nullable=False)
    status = Column(String(32), nullable=False, default="draft")  # draft/awaiting_protect/awaiting_autodelete/published/revoked
    protect_content = Column(Boolean, default=False)
    autodelete_minutes = Column(Integer, default=0)
    revoked = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    files = relationship("FileModel", back_populates="session", cascade="all, delete-orphan")


class FileModel(Base):
    __tablename__ = "files"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    session_id = Column(BigInteger, ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False)
    tg_file_id = Column(String, nullable=False)  # stable file_id stored after forwarding to upload channel
    file_type = Column(String(32), nullable=False)
    caption = Column(String, nullable=True)
    order_index = Column(Integer, nullable=False)

    session = relationship("SessionModel", back_populates="files")


class DeliveryModel(Base):
    __tablename__ = "deliveries"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    chat_id = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    delete_at = Column(DateTime(timezone=True), nullable=True)


class StartMessage(Base):
    __tablename__ = "start_message"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    content = Column(String, nullable=True)
    photo_file_id = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


try:
    Index("ix_files_session_order", FileModel.session_id, FileModel.order_index)
except Exception:
    pass

# ---------------------------
# Async DB engine and session
# ---------------------------

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

# ---------------------------
# DB initialization & migrations
# ---------------------------


async def ensure_tables_and_safe_migrations():
    """
    Create tables (if missing) and attempt safe migrations:
     - add missing columns if simple to add
     - attempt to alter integer -> bigint for known columns (best-effort)
    """
    logger.info("DB init: creating tables if needed and attempting safe migrations...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Best-effort: add columns if missing (no harm)
    try:
        async with engine.begin() as conn:
            await conn.execute(sa_text("ALTER TABLE IF EXISTS start_message ADD COLUMN IF NOT EXISTS photo_file_id TEXT"))
    except Exception:
        logger.debug("start_message.photo_file_id not added (might already exist)")

    try:
        async with engine.begin() as conn:
            await conn.execute(sa_text("ALTER TABLE IF EXISTS sessions ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'draft'"))
    except Exception:
        logger.debug("sessions.status may already exist")

    try:
        async with engine.begin() as conn:
            await conn.execute(sa_text("ALTER TABLE IF EXISTS sessions ADD COLUMN IF NOT EXISTS protect_content BOOLEAN DEFAULT FALSE"))
    except Exception:
        logger.debug("sessions.protect_content may already exist")

    try:
        async with engine.begin() as conn:
            await conn.execute(sa_text("ALTER TABLE IF EXISTS sessions ADD COLUMN IF NOT EXISTS autodelete_minutes INTEGER DEFAULT 0"))
    except Exception:
        logger.debug("sessions.autodelete_minutes may already exist")

    # Attempt to alter integer -> bigint columns to avoid numeric overflow issues (best-effort)
    await attempt_alter_integer_columns_to_bigint()


async def attempt_alter_integer_columns_to_bigint():
    """
    For PostgreSQL: check known columns; if data_type == 'integer', ALTER to BIGINT.
    This is best-effort and will be ignored if DB user lacks privilege.
    """
    logger.info("Attempting to alter integer columns to BIGINT (if necessary)")
    known = {
        "users": ["id"],
        "sessions": ["id", "owner_id"],
        "files": ["id", "session_id"],
        "deliveries": ["id", "chat_id", "message_id"],
        "start_message": ["id"],
    }
    async with engine.begin() as conn:
        for table, cols in known.items():
            for col in cols:
                try:
                    q = """ SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = :table AND column_name = :col; """
                    res = await conn.execute(sa_text(q).bindparams(table=table, col=col))
                    row = res.first()
                    if not row:
                        continue
                    dtype = row[0]
                    if dtype == "integer":
                        alter_sql = f'ALTER TABLE "{table}" ALTER COLUMN "{col}" TYPE BIGINT USING "{col}"::BIGINT;'
                        try:
                            logger.info("Altering %s.%s INTEGER -> BIGINT", table, col)
                            await conn.execute(sa_text(alter_sql))
                            logger.info("Altered %s.%s successfully", table, col)
                        except Exception:
                            logger.exception("Failed to ALTER %s.%s to BIGINT (insufficient privileges?)", table, col)
                except Exception:
                    logger.exception("Error while checking/altering %s.%s", table, col)


# ---------------------------
# Bot & dispatcher
# ---------------------------

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)

# a small semaphore to limit concurrent deliveries to Telegram (avoid floods)
delivery_semaphore = asyncio.Semaphore(MAX_DELIVERY_CONCURRENCY)

# ---------------------------
# Utilities
# ---------------------------


def generate_token(length: int = DRAFT_TOKEN_LENGTH) -> str:
    # generates URL-safe tokens of requested length
    tok = secrets.token_urlsafe(length * 2)[:length]
    return tok


def _escape_html(s: str) -> str:
    return html.escape(s or "")


def render_text_with_links(text: Optional[str], first_name: Optional[str] = None) -> str:
    """
    Replace {first_name} and convert {word | url} patterns into HTML anchor tags.

    Examples:
      "Visit {here | https://ex.com}" -> 'Visit <a href="https://ex.com">here</a>'
      "Hello {first_name}" -> 'Hello John'
    """
    if not text:
        return ""

    # First replace {first_name}
    out = text.replace("{first_name}", html.escape(first_name or ""))

    # Now parse {left | right} occurrences. We must preserve other braces if malformed.
    result_parts: List[str] = []
    i = 0
    L = len(out)
    while i < L:
        s = out.find("{", i)
        if s == -1:
            result_parts.append(_escape_html(out[i:]))
            break
        # append pre-brace text escaped
        result_parts.append(_escape_html(out[i:s]))
        e = out.find("}", s)
        if e == -1:
            # no closing brace; append rest escaped and break
            result_parts.append(_escape_html(out[s:]))
            break
        inner = out[s + 1:e].strip()
        if "|" in inner:
            left, right = inner.split("|", 1)
            left_t = left.strip()
            right_t = right.strip()
            left_escaped = html.escape(left_t)
            href = html.escape(right_t, quote=True)
            result_parts.append(f'<a href="{href}">{left_escaped}</a>')
        else:
            # not a link pattern - render literally escaped
            result_parts.append(_escape_html("{" + inner + "}"))
        i = e + 1
    return "".join(result_parts)


# ---------------------------
# Database helper functions
# ---------------------------


async def init_db():
    """ Initialize DB and run safe auto-migrations. """
    await ensure_tables_and_safe_migrations()
    logger.info("DB initialization completed.")


async def save_user_if_not_exists(user: types.User):
    """ Saves user's basic info for future broadcast. Non-blocking helper. """
    if not user:
        return
    async with AsyncSessionLocal() as db:
        try:
            stmt = select(UserModel).where(UserModel.id == int(user.id))
            res = await db.execute(stmt)
            if res.scalars().first():
                return
            rec = UserModel(id=int(user.id), first_name=user.first_name, last_name=user.last_name, username=user.username)
            db.add(rec)
            await db.commit()
            logger.debug("Saved user %s to DB", user.id)
        except Exception:
            logger.exception("save_user_if_not_exists failed")


async def create_draft_session(owner_id: int) -> SessionModel:
    """ Create a draft session. Retries on conflicts/unique collisions. """
    async with AsyncSessionLocal() as db:
        last_exc = None
        for attempt in range(6):
            token = generate_token(DRAFT_TOKEN_LENGTH)
            rec = SessionModel(link=token, owner_id=owner_id, status="draft")
            db.add(rec)
            try:
                await db.commit()
                await db.refresh(rec)
                logger.info("Created draft session %s for owner %s", rec.link, owner_id)
                return rec
            except Exception as e:
                last_exc = e
                await db.rollback()
                logger.warning("create_draft_session attempt %d failed: %s", attempt + 1, e)
        logger.error("create_draft_session failed after retries: %s", last_exc)
        raise RuntimeError("Failed to create draft session") from last_exc


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
        logger.debug("Appended file to session %s index %s", session_id, next_idx)
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
            logger.debug("Set session %s status -> %s", session_id, status)
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
            logger.info("Published session %s protect=%s autodelete=%s", session_id, protect, autodelete)
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
            logger.info("Revoked session %s", token)
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
            logger.info("Deleted draft session %s", session_id)
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
        logger.info("Saved start message (text present=%s photo present=%s)", bool(content), bool(photo_file_id))


async def fetch_start_message() -> Tuple[Optional[str], Optional[str]]:
    async with AsyncSessionLocal() as db:
        stmt = select(StartMessage).order_by(StartMessage.updated_at.desc())
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if rec:
            return rec.content, rec.photo_file_id
        return None, None


# ---------------------------
# Sending helpers
# ---------------------------


async def send_media_with_protect(chat_id: int, file_type: str, tg_file_id: str, caption: Optional[str], protect: bool):
    """
    Send media keeping caption exactly as provided (rendered HTML) and try protect_content if supported.
    Returns the sent message object.
    """
    kwargs: Dict[str, Any] = {}
    if caption:
        kwargs["caption"] = caption
        kwargs["parse_mode"] = "HTML"

    # Some versions of aiogram/Telegram don't support protect_content param in send_* methods.
    # We set it and catch TypeError to fallback.
    try:
        kwargs["protect_content"] = protect
        if file_type == "photo":
            return await bot.send_photo(chat_id=chat_id, photo=tg_file_id, **kwargs)
        elif file_type == "video":
            return await bot.send_video(chat_id=chat_id, video=tg_file_id, **kwargs)
        elif file_type == "document":
            return await bot.send_document(chat_id=chat_id, document=tg_file_id, **kwargs)
        elif file_type == "audio":
            return await bot.send_audio(chat_id=chat_id, audio=tg_file_id, **kwargs)
        elif file_type == "voice":
            # voice doesn't support caption
            kwargs.pop("caption", None)
            kwargs.pop("parse_mode", None)
            return await bot.send_voice(chat_id=chat_id, voice=tg_file_id, **kwargs)
        elif file_type == "sticker":
            kwargs.pop("caption", None)
            kwargs.pop("parse_mode", None)
            return await bot.send_sticker(chat_id=chat_id, sticker=tg_file_id, **kwargs)
        elif file_type == "animation":
            return await bot.send_animation(chat_id=chat_id, animation=tg_file_id, **kwargs)
        else:
            return await bot.send_document(chat_id=chat_id, document=tg_file_id, **kwargs)
    except TypeError:
        # fallback if protect_content param not supported
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
                return await bot.send_voice(chat_id=chat_id, voice=tg_file_id, **kwargs)
            elif file_type == "sticker":
                return await bot.send_sticker(chat_id=chat_id, sticker=tg_file_id, **kwargs)
            elif file_type == "animation":
                return await bot.send_animation(chat_id=chat_id, animation=tg_file_id, **kwargs)
            else:
                return await bot.send_document(chat_id=chat_id, document=tg_file_id, **kwargs)
        except Exception:
            logger.exception("Fallback send without protect failed")
            raise
    except Exception:
        logger.exception("send_media_with_protect failed")
        raise


# ---------------------------
# Autodelete worker
# ---------------------------


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
                            logger.debug("Autodelete deleted %s:%s", rec.chat_id, rec.message_id)
                        except Exception as e:
                            logger.warning("Autodelete failed to delete %s:%s -> %s", rec.chat_id, rec.message_id, e)
                        try:
                            await db.delete(rec)
                        except Exception:
                            logger.exception("Autodelete failed to remove DB entry")
                    await db.commit()
        except Exception:
            logger.exception("Autodelete worker exception: %s", traceback.format_exc())
        await asyncio.sleep(AUTODELETE_CHECK_INTERVAL)


# ---------------------------
# Self-ping (keepalive) worker
# ---------------------------

async def self_ping_worker():
    """
    Periodically GETs {WEBHOOK_HOST}/health to keep Render/hosting from idling the service.
    Uses WEBHOOK_HOST from environment so it can be changed for testing.
    """
    if not WEBHOOK_HOST:
        logger.warning("WEBHOOK_HOST not set; self-ping disabled.")
        return
    url = f"{WEBHOOK_HOST.rstrip('/')}/health"
    logger.info("Self-ping worker starting; will ping %s every 300s", url)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(url, timeout=15) as resp:
                        if resp.status == 200:
                            logger.debug("Self-ping OK: %s", url)
                        else:
                            logger.warning("Self-ping unexpected status %s for %s", resp.status, url)
                except asyncio.TimeoutError:
                    logger.warning("Self-ping timeout for %s", url)
                except Exception as e:
                    logger.warning("Self-ping failed for %s: %s", url, e)
        except Exception:
            logger.exception("Unexpected error in self-ping outer loop")
        # Sleep 5 minutes to be safe and keep service active
        await asyncio.sleep(300)


# ---------------------------
# Handlers
# ---------------------------

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    """
    /start -> greeting (owner-only config)
    /start <token> -> deep link delivery (sends files with ONLY original captions)
    """
    # Save the user quietly for broadcast list
    try:
        await save_user_if_not_exists(message.from_user)
    except Exception:
        logger.exception("save_user_if_not_exists in /start failed")

    args = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            args = parts[1].strip()

    if not args:
        # show start message content
        content, photo = await fetch_start_message()
        if photo:
            caption_html = render_text_with_links(content or "", message.from_user.first_name)
            try:
                # removed unsupported disable_web_page_preview kwarg for photo
                await message.answer_photo(photo=photo, caption=caption_html, parse_mode="HTML")
                return
            except Exception:
                logger.exception("Failed to send start photo fallback")
        if content:
            try:
                # send_message accepts disable_web_page_preview
                await message.answer(render_text_with_links(content, message.from_user.first_name), parse_mode="HTML", disable_web_page_preview=True)
            except Exception:
                await message.answer(content.replace("{first_name}", message.from_user.first_name or ""))
        else:
            await message.answer(f"Welcome, {message.from_user.first_name or 'friend'}!")
        return

    # token provided -> fetch session
    token = args
    session_obj = await get_session_by_token(token)
    if not session_obj:
        await message.answer("❌ Link not found or invalid.")
        return
    if session_obj.revoked or session_obj.status == "revoked":
        await message.answer("❌ This link has been revoked.")
        return
    if session_obj.status != "published":
        await message.answer("❌ This link is not published.")
        return

    files = await list_files_for_session(session_obj.id)
    if not files:
        await message.answer("❌ No files found for this link.")
        return

    delivered = 0
    for f in files:
        try:
            caption_html = render_text_with_links(f.caption or "", message.from_user.first_name)
            protect_flag = False if (message.from_user.id == OWNER_ID) else bool(session_obj.protect_content)
            await delivery_semaphore.acquire()
            try:
                sent = await send_media_with_protect(chat_id=message.chat.id, file_type=f.file_type, tg_file_id=f.tg_file_id, caption=caption_html or None, protect=protect_flag)
            finally:
                delivery_semaphore.release()
            delivered += 1
            if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
                delete_time = datetime.utcnow() + timedelta(minutes=session_obj.autodelete_minutes)
                await schedule_delivery(chat_id=sent.chat.id, message_id=sent.message_id, delete_at=delete_time)
        except Exception:
            logger.exception("Failed to deliver file %s for session %s", f.tg_file_id, session_obj.link)

    if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
        await message.answer(f"Files delivered: {delivered}.")
        await message.answer(f"Delivery complete. Above files will be deleted in {session_obj.autodelete_minutes} minute(s).\n\n")
    else:
        await message.answer(f"Files delivered: {delivered}")

    return


@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    """ Owner-only: start an upload session (draft). After starting, owner should send files; they will be forwarded to UPLOAD_CHANNEL. """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /upload.")
        return
    try:
        existing = await get_owner_active_draft(OWNER_ID)
    except Exception:
        logger.exception("get_owner_active_draft failed")
        existing = None
    if existing:
        await message.reply(f"You already have an active draft session. Continue sending files or use /done. Draft token: {existing.link}")
        return
    try:
        rec = await create_draft_session(OWNER_ID)
        await message.reply(
            f"✅ Upload session started.\nDraft token: {rec.link}\n"
            f"Send up to {MAX_FILES_PER_SESSION} files (photos/videos/documents/audio/voice/sticker/animation). "
            "When finished send /done. To cancel send /abort."
        )
    except Exception:
        logger.exception("create_draft_session failed")
        await message.reply("❌ Failed to start upload session. Check logs.")


@dp.message_handler(content_types=["photo", "video", "document", "audio", "voice", "sticker", "animation"])
async def handle_owner_media(message: types.Message):
    """ When owner sends media while a draft exists, forward to upload channel and save stable file id to DB as part of session. Non-owner messages: just quietly save user info for broadcast list. """
    # Save user info if it's not owner
    if message.from_user.id != OWNER_ID:
        try:
            await save_user_if_not_exists(message.from_user)
        except Exception:
            logger.exception("save_user_if_not_exists failed for non-owner media")
        return

    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        # No upload in progress; ignore
        return

    files = await list_files_for_session(draft.id)
    if len(files) >= MAX_FILES_PER_SESSION:
        await message.reply(f"Upload limit reached ({MAX_FILES_PER_SESSION}). Use /done or /abort.")
        return

    # Determine original file id and type
    ftype = None
    orig_file_id = None
    caption = ""
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
        logger.exception("Failed to parse incoming owner media")
        await message.reply("Failed to parse media. Try again.")
        return

    # Forward to upload channel
    try:
        fwd = await bot.forward_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
    except Exception:
        logger.exception("Forward to upload channel failed")
        await message.reply("Failed to forward to upload channel. Ensure bot is member/admin and channel id is correct.")
        return

    # Extract stable file id from the forwarded message (upload channel keeps file accessible)
    try:
        if getattr(fwd, "photo", None):
            channel_type = "photo"; channel_file_id = fwd.photo[-1].file_id; channel_caption = fwd.caption or ""
        elif getattr(fwd, "video", None):
            channel_type = "video"; channel_file_id = fwd.video.file_id; channel_caption = fwd.caption or ""
        elif getattr(fwd, "document", None):
            channel_type = "document"; channel_file_id = fwd.document.file_id; channel_caption = fwd.caption or ""
        elif getattr(fwd, "audio", None):
            channel_type = "audio"; channel_file_id = fwd.audio.file_id; channel_caption = fwd.caption or ""
        elif getattr(fwd, "voice", None):
            channel_type = "voice"; channel_file_id = fwd.voice.file_id; channel_caption = ""
        elif getattr(fwd, "sticker", None):
            channel_type = "sticker"; channel_file_id = fwd.sticker.file_id; channel_caption = ""
        elif getattr(fwd, "animation", None):
            channel_type = "animation"; channel_file_id = fwd.animation.file_id; channel_caption = fwd.caption or ""
        else:
            channel_type = ftype; channel_file_id = orig_file_id; channel_caption = caption
    except Exception:
        logger.exception("Failed to extract stable file id from forwarded message; falling back to original")
        channel_type = ftype; channel_file_id = orig_file_id; channel_caption = caption

    # Save metadata to DB
    try:
        await append_file_to_session(draft.id, channel_file_id, channel_type, channel_caption or "")
        current_count = len(await list_files_for_session(draft.id))
        await message.reply(f"Saved {channel_type}. ({current_count}/{MAX_FILES_PER_SESSION}) Send more or /done when finished.")
    except Exception:
        logger.exception("append_file_to_session failed")
        await message.reply("Failed to save file metadata; try again or /abort.")


@dp.message_handler(commands=["done"])
async def cmd_done(message: types.Message):
    """ Owner only: finalize the draft, ask for protect content and autodelete minutes. After finalization, session is published and summary message posted to upload channel only. """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /done.")
        return

    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        await message.reply("No active upload session.")
        return

    files = await list_files_for_session(draft.id)
    if not files:
        await message.reply("No files in current session. Upload files first.")
        return

    await set_session_status(draft.id, "awaiting_protect")
    await message.reply("All files for this session are ready. Protect content? Reply `on` or `off` (owner only).")


@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and (m.text or "").strip().lower() in ("on", "off"))
async def handle_protect_reply(message: types.Message):
    """ Owner replies 'on' or 'off' after /done; set protect_content and ask for autodelete minutes. """
    # find the latest draft that is awaiting_protect
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.owner_id == OWNER_ID, SessionModel.status == "awaiting_protect").order_by(SessionModel.created_at.desc()).limit(1)
        res = await db.execute(stmt)
        draft = res.scalars().first()
        if not draft:
            await message.reply("No session awaiting protect choice.")
            return
        text = (message.text or "").strip().lower()
        protect = text == "on"
        draft.protect_content = protect
        draft.status = "awaiting_autodelete"
        db.add(draft)
        await db.commit()
        await db.refresh(draft)
        await message.reply(f"Protect set to {'ON' if protect else 'OFF'}. Now reply with autodelete minutes (0 - {AUTODELETE_MAX_MIN}). 0 = disabled.")


@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and (m.text or "").strip().isdigit())
async def handle_autodelete_reply(message: types.Message):
    """ Owner replies with autodelete minutes after protect step. Finalize session, set to published and post summary to upload channel only. """
    # Get latest session awaiting_autodelete
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.owner_id == OWNER_ID, SessionModel.status == "awaiting_autodelete").order_by(SessionModel.created_at.desc()).limit(1)
        res = await db.execute(stmt)
        draft = res.scalars().first()
        if not draft:
            # This handler may catch unrelated digits; ignore
            return
        try:
            minutes = int((message.text or "").strip())
            if minutes < 0 or minutes > AUTODELETE_MAX_MIN:
                raise ValueError
        except ValueError:
            await message.reply(f"Please provide an integer 0..{AUTODELETE_MAX_MIN}.")
            return
        draft.autodelete_minutes = minutes
        draft.status = "published"
        db.add(draft)
        await db.commit()
        await db.refresh(draft)

    # Compose deep link, prefer BOT_USERNAME from env if provided, else use bot.get_me()
    try:
        bot_username = BOT_USERNAME
        if not bot_username:
            me = await bot.get_me()
            bot_username = me.username or me.first_name or "bot"
    except Exception:
        bot_username = BOT_USERNAME or "bot"

    deep_link = f"https://t.me/{bot_username}?start={draft.link}"

    # Fetch files count
    files = await list_files_for_session(draft.id)
    total = len(files)

    # Post summary message to upload channel ONLY (owner requested)
    try:
        protect_str = "ON" if draft.protect_content else "OFF"
        summary = (
            f"✅ Upload Complete!\n\n"
            f"Session - {draft.id}\n"
            f"Deep link - {deep_link}\n"
            f"No of files in this session - {total}\n"
            f"Autodelete - {draft.autodelete_minutes}\n"
            f"Protect content - {protect_str}\n"
        )
        await bot.send_message(chat_id=UPLOAD_CHANNEL_ID, text=summary)
    except Exception:
        logger.exception("Failed to post session summary to upload channel")

    # Also send the deep link + info privately to owner for confirmation
    try:
        owner_msg = (
            f"✅ Session published\n\n"
            f"Session ID: {draft.id}\n"
            f"Deep link: {deep_link}\n"
            f"Files: {total}\n"
            f"Protect: {protect_str}\n"
            f"Autodelete (minutes): {draft.autodelete_minutes}\n"
            f"Token: {draft.link}\n"
        )
        await bot.send_message(chat_id=OWNER_ID, text=owner_msg)
    except Exception:
        logger.exception("Failed to send publish confirmation to owner")

    # let owner know in the chat where /done was called
    try:
        await message.reply("✅ Session published and summary posted to upload channel. Deep link token saved. (Not broadcasted to users.)")
    except Exception:
        logger.exception("Failed to reply to owner after publishing")


@dp.message_handler(commands=["abort"])
async def cmd_abort(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can /abort.")
        return
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        await message.reply("No active upload session.")
        return
    ok = await delete_draft_session(draft.id)
    if ok:
        await message.reply("Upload aborted; draft removed.")
    else:
        await message.reply("Failed to abort session.")


@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can /revoke.")
        return
    parts = (message.text or "").strip().split()
    if len(parts) < 2:
        await message.reply("Usage: /revoke <token>")
        return
    token = parts[1].strip()
    ok = await revoke_session_by_token(token)
    if ok:
        await message.reply(f"✅ Token revoked: {token}")
    else:
        await message.reply("Token not found.")


@dp.message_handler(commands=["editstart", "edit_start"])
async def cmd_edit_start(message: types.Message):
    """
    Owner should reply to a message (text or photo+caption) with /editstart to set the /start welcome content.
    The stored start message will be used when users send /start (no args).
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can use /editstart.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or photo) to set /start content.")
        return
    reply = message.reply_to_message
    try:
        # If reply has photo -> save photo_file_id and caption (if present)
        if getattr(reply, "photo", None):
            photo_file_id = reply.photo[-1].file_id
            caption = reply.caption or ""
            # Save both caption and photo_file_id
            await save_start_message(caption, photo_file_id)
            await message.reply("✅ /start updated with image + caption (if present).")
            return
        # If reply has text -> save text
        if getattr(reply, "text", None) and reply.text.strip():
            await save_start_message(reply.text, None)
            await message.reply("✅ /start updated with text.")
            return
        # If reply has caption only (e.g., media without photo) -> save caption
        if getattr(reply, "caption", None):
            await save_start_message(reply.caption, None)
            await message.reply("✅ /start updated with caption.")
            return
        await message.reply("Unsupported message type. Reply to text or photo.")
    except Exception:
        logger.exception("cmd_edit_start failed")
        await message.reply("Failed to update /start message.")


@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    """
    Owner-only broadcast: reply to a message to broadcast its content to saved users.
    Supports {first_name} and {word | url} placeholders in text.
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can broadcast.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or photo) to broadcast.")
        return
    reply = message.reply_to_message

    # gather all users
    async with AsyncSessionLocal() as db:
        stmt = select(UserModel.id)
        res = await db.execute(stmt)
        rows = res.fetchall()
        user_ids = [r[0] for r in rows]
    if not user_ids:
        await message.reply("No users to broadcast to.")
        return

    # prepare content
    b_photo = None
    b_text = None
    try:
        if getattr(reply, "photo", None):
            b_photo = reply.photo[-1].file_id
            b_text = reply.caption or ""
        elif getattr(reply, "text", None) and reply.text.strip():
            b_text = reply.text
        else:
            b_text = reply.caption or ""
    except Exception:
        logger.exception("Failed to parse broadcast content")
        await message.reply("Failed to parse broadcast content.")
        return

    await message.reply(f"Broadcast starting to {len(user_ids)} users. This will run in background.")

    sem = asyncio.Semaphore(10)
    sent_count = 0
    fail_count = 0

    async def send_task(uid: int):
        nonlocal sent_count, fail_count
        try:
            await sem.acquire()
            fname = ""
            try:
                async with AsyncSessionLocal() as sdb:
                    q = select(UserModel).where(UserModel.id == int(uid))
                    r = await sdb.execute(q)
                    u = r.scalars().first()
                    fname = u.first_name if u else ""
            except Exception:
                fname = ""
            try:
                if b_photo:
                    caption_html = render_text_with_links(b_text or "", fname)
                    await bot.send_photo(chat_id=int(uid), photo=b_photo, caption=caption_html, parse_mode="HTML")
                    sent_count += 1
                else:
                    await bot.send_message(chat_id=int(uid), text=render_text_with_links(b_text or "", fname), parse_mode="HTML", disable_web_page_preview=True)
                    sent_count += 1
            except Exception:
                logger.exception("Broadcast send failed for %s", uid)
                fail_count += 1
        finally:
            try:
                sem.release()
            except Exception:
                pass

    tasks = [asyncio.create_task(send_task(uid)) for uid in user_ids]
    await asyncio.gather(*tasks)

    await message.reply(f"Broadcast complete. Sent: {sent_count}. Failed: {fail_count}.")


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can use /help.")
        return
    await message.reply(
        "/upload - start upload (owner only)\n"
        "/done - finalize upload (owner only)\n"
        "/abort - cancel upload\n"
        "/revoke <token> - revoke a published session\n"
        "/editstart - reply to a message to set /start\n"
        "/broadcast - reply to a message to broadcast all users\n"
        "/help - this message\n\n"
        "Public: /start and /start <token>"
    )


@dp.message_handler()
async def fallback_handler(message: types.Message):
    """ Catch-all: store user info for broadcast list and ignore. """
    try:
        await save_user_if_not_exists(message.from_user)
    except Exception:
        logger.exception("fallback: save_user_if_not_exists failed")


# ---------------------------
# Webhook handler + health
# ---------------------------

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
    try:
        # Set current bot context for aiogram internals if available
        try:
            Bot.set_current(bot)
        except Exception:
            pass
        await dp.process_update(update)
    except Exception:
        logger.exception("Dispatcher processing failed: %s", traceback.format_exc())
    return web.Response(text="OK")


async def health_handler(request: Request):
    return web.Response(text="OK")


# ---------------------------
# Startup sequence
# ---------------------------

async def start_services_and_run():
    logger.info("Launching session_share_bot; webhook_host=%s port=%s", WEBHOOK_HOST, PORT)
    try:
        await init_db()
    except Exception:
        logger.exception("init_db failed; proceeding anyway")

    # Start autodelete worker
    asyncio.create_task(autodelete_worker())
    logger.info("Autodelete worker scheduled")

    # Start self-ping worker to keep hosting awake (added as requested)
    asyncio.create_task(self_ping_worker())
    logger.info("Self-ping worker scheduled (keeps service alive)")

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

    # set webhook
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except Exception:
        logger.exception("Failed to set webhook to %s", WEBHOOK_URL)

    # keep running
    try:
        await asyncio.Event().wait()
    finally:
        logger.info("Shutdown initiated")
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
    try:
        asyncio.run(start_services_and_run())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt - exiting")
    except Exception:
        logger.exception("Fatal error in main")
        sys.exit(1)


if __name__ == "__main__":
    main()