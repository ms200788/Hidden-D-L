# bot.py
"""
Session-share Telegram bot
- aiogram==2.25.0
- aiohttp==3.8.6
- SQLAlchemy 1.4.x (async) + asyncpg
Single-file implementation that:
 - supports owner-only uploads that forward files to an upload channel and saves stable file_ids
 - supports /done -> protect on/off -> autodelete minutes -> publish session + deep link
 - deep link delivers files with original captions only; schedules deletions if autodelete > 0
 - /editstart works for reply-to-photo+caption, reply-to-photo, reply-to-text, reply-to-caption
 - /broadcast, /revoke, /help are owner-only
 - /start (no args) sends configured start message (supports {first_name} and {word | url})
 - /start <token> performs deep-link delivery
 - uses BigInteger for tg id columns to avoid numeric overflow
 - autodelete worker removes messages at scheduled times
"""

import os
import sys
import asyncio
import logging
import secrets
import html
import traceback
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Any, Dict

from aiohttp import web
from aiohttp.web_request import Request

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import ParseMode

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

# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("session_share_bot")

# ----------------------------
# Configuration (env)
# ----------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID_ENV = os.environ.get("OWNER_ID")
DATABASE_URL = os.environ.get("DATABASE_URL")  # e.g. postgresql+asyncpg://user:pass@host:5432/db
UPLOAD_CHANNEL_ID_ENV = os.environ.get("UPLOAD_CHANNEL_ID")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # e.g. https://hidden-fnxx.onrender.com
PORT = int(os.environ.get("PORT", "10000"))

# Limits & tunables
MAX_FILES_PER_SESSION = int(os.environ.get("MAX_FILES_PER_SESSION", "99"))
MAX_DELIVERY_CONCURRENCY = int(os.environ.get("MAX_DELIVERY_CONCURRENCY", "50"))
AUTODELETE_CHECK_INTERVAL = int(os.environ.get("AUTODELETE_CHECK_INTERVAL", "30"))
DRAFT_TOKEN_LENGTH = int(os.environ.get("DRAFT_TOKEN_LENGTH", "64"))
AUTODELETE_MAX_MIN = int(os.environ.get("AUTODELETE_MAX_MIN", "10080"))

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

logger.info(
    "Configuration: OWNER_ID=%s, UPLOAD_CHANNEL_ID=%s, WEBHOOK_HOST=%s, PORT=%s",
    OWNER_ID,
    UPLOAD_CHANNEL_ID,
    WEBHOOK_HOST,
    PORT,
)

# ----------------------------
# Database models
# ----------------------------
Base = declarative_base()


class UserModel(Base):
    __tablename__ = "users"
    id = Column(BigInteger, primary_key=True, autoincrement=True)  # internal id
    tg_id = Column(BigInteger, unique=True, nullable=False, index=True)  # telegram user id
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    username = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    link = Column(String(128), unique=True, nullable=False, index=True)
    owner_id = Column(BigInteger, nullable=False, index=True)
    status = Column(String(32), nullable=False, default="draft")  # draft/awaiting_protect/awaiting_autodelete/published/revoked
    protect_content = Column(Boolean, default=False)
    autodelete_minutes = Column(Integer, default=0)
    revoked = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    files = relationship("FileModel", back_populates="session", cascade="all, delete-orphan")


class FileModel(Base):
    __tablename__ = "files"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    session_id = Column(BigInteger, ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False, index=True)
    tg_file_id = Column(String, nullable=False)  # stable file_id from upload channel
    file_type = Column(String(32), nullable=False)
    caption = Column(String, nullable=True)
    order_index = Column(Integer, nullable=False, default=0)

    session = relationship("SessionModel", back_populates="files")


class DeliveryModel(Base):
    __tablename__ = "deliveries"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    chat_id = Column(BigInteger, nullable=False, index=True)
    message_id = Column(BigInteger, nullable=False)
    delete_at = Column(DateTime(timezone=True), nullable=True)


class StartMessage(Base):
    __tablename__ = "start_message"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    content = Column(String, nullable=True)
    photo_file_id = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


Index("ix_files_session_order", FileModel.session_id, FileModel.order_index)

# ----------------------------
# Async DB engine & sessionmaker
# ----------------------------
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


# ----------------------------
# DB init & safe migrations (best-effort)
# ----------------------------
async def ensure_tables_and_safe_migrations():
    logger.info("DB init: creating tables and attempting safe migrations...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # best-effort add columns that may be missing
        try:
            await conn.execute(sa_text("ALTER TABLE IF EXISTS start_message ADD COLUMN IF NOT EXISTS photo_file_id TEXT"))
        except Exception:
            logger.debug("start_message.photo_file_id may already exist or cannot be added.")

        for sql in (
            "ALTER TABLE IF EXISTS sessions ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'draft'",
            "ALTER TABLE IF EXISTS sessions ADD COLUMN IF NOT EXISTS protect_content BOOLEAN DEFAULT FALSE",
            "ALTER TABLE IF EXISTS sessions ADD COLUMN IF NOT EXISTS autodelete_minutes INTEGER DEFAULT 0",
        ):
            try:
                await conn.execute(sa_text(sql))
            except Exception:
                logger.debug("Column may already exist or cannot be added: %s", sql)

    # attempt to alter integers to bigint where needed (best-effort)
    await attempt_alter_integer_columns_to_bigint()


async def attempt_alter_integer_columns_to_bigint():
    logger.info("Attempting to convert integer columns to BIGINT where needed (best-effort).")
    known = {
        "users": ["id", "tg_id"],
        "sessions": ["id", "owner_id"],
        "files": ["id", "session_id"],
        "deliveries": ["id", "chat_id", "message_id"],
        "start_message": ["id"],
    }
    async with engine.begin() as conn:
        for table, cols in known.items():
            for col in cols:
                try:
                    q = """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = :table AND column_name = :col;
                    """
                    r = await conn.execute(sa_text(q).bindparams(table=table, col=col))
                    row = r.first()
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
                    logger.exception("Error checking/altering %s.%s", table, col)


async def init_db():
    try:
        await ensure_tables_and_safe_migrations()
        logger.info("Database initialization complete.")
    except Exception:
        logger.exception("init_db failed")
        raise


# ----------------------------
# Bot & Dispatcher
# ----------------------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)

delivery_semaphore = asyncio.Semaphore(MAX_DELIVERY_CONCURRENCY)


# ----------------------------
# Utils
# ----------------------------
def gen_token(n: int = DRAFT_TOKEN_LENGTH) -> str:
    return secrets.token_urlsafe(n)[:n]


def render_text_with_links(text: Optional[str], first_name: Optional[str] = None) -> str:
    """
    Replace {first_name} and convert {word | url} patterns into HTML anchor tags.
    """
    if not text:
        return ""
    t = text.replace("{first_name}", html.escape(first_name or ""))
    out = []
    i = 0
    L = len(t)
    while i < L:
        s = t.find("{", i)
        if s == -1:
            out.append(html.escape(t[i:]))
            break
        e = t.find("}", s)
        if e == -1:
            out.append(html.escape(t[i:]))
            break
        out.append(html.escape(t[i:s]))
        inner = t[s + 1 : e].strip()
        if "|" in inner:
            left, right = inner.split("|", 1)
            left = html.escape(left.strip())
            right = html.escape(right.strip(), quote=True)
            out.append(f'<a href="{right}">{left}</a>')
        else:
            out.append(html.escape("{" + inner + "}"))
        i = e + 1
    return "".join(out)


# ----------------------------
# DB helpers (CRUD)
# ----------------------------
async def save_user_if_not_exists(user: types.User):
    if not user:
        return
    async with AsyncSessionLocal() as db:
        try:
            stmt = select(UserModel).where(UserModel.tg_id == int(user.id))
            r = await db.execute(stmt)
            if r.scalars().first():
                return
            rec = UserModel(tg_id=int(user.id), first_name=user.first_name, last_name=user.last_name, username=user.username)
            db.add(rec)
            await db.commit()
            logger.debug("Saved user %s", user.id)
        except Exception:
            logger.exception("save_user_if_not_exists failed")


async def create_draft_session(owner_id: int) -> SessionModel:
    async with AsyncSessionLocal() as db:
        last_exc = None
        for attempt in range(5):
            token = gen_token(DRAFT_TOKEN_LENGTH)
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
                msg = str(e).lower()
                if "out of range" in msg or "numericvalueoutofrange" in msg or "invalid input for query argument" in msg:
                    logger.warning("Detected numeric overflow; attempting to ALTER integer columns to BIGINT and retry")
                    try:
                        await attempt_alter_integer_columns_to_bigint()
                    except Exception:
                        logger.exception("attempt_alter_integer_columns_to_bigint failed")
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
        fm = FileModel(session_id=session_id, tg_file_id=tg_file_id, file_type=file_type, caption=caption or "", order_index=next_idx)
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
        if not rec:
            return None
        rec.status = status
        db.add(rec)
        await db.commit()
        await db.refresh(rec)
        logger.debug("Set session %s status -> %s", session_id, status)
        return rec


async def publish_session(session_id: int, protect: bool, autodelete_minutes: int) -> Optional[SessionModel]:
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.id == session_id).limit(1)
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if not rec:
            return None
        rec.protect_content = bool(protect)
        rec.autodelete_minutes = int(autodelete_minutes)
        rec.status = "published"
        db.add(rec)
        await db.commit()
        await db.refresh(rec)
        logger.info("Published session %s protect=%s autodelete=%s", rec.link, rec.protect_content, rec.autodelete_minutes)
        return rec


async def revoke_session_by_token(token: str) -> bool:
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.link == token).limit(1)
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if not rec:
            return False
        rec.revoked = True
        rec.status = "revoked"
        db.add(rec)
        await db.commit()
        logger.info("Revoked session %s", token)
        return True


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
    logger.debug("Scheduled deletion %s:%s at %s", chat_id, message_id, delete_at.isoformat())


async def save_start_message(content: Optional[str], photo_file_id: Optional[str] = None):
    async with AsyncSessionLocal() as db:
        stmt = select(StartMessage).order_by(StartMessage.updated_at.desc()).limit(1)
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
        stmt = select(StartMessage).order_by(StartMessage.updated_at.desc()).limit(1)
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if rec:
            return rec.content, rec.photo_file_id
        return None, None


# ----------------------------
# Send helpers (handle protect_content compatibility)
# ----------------------------
async def send_media_with_protect(chat_id: int, file_type: str, tg_file_id: str, caption: Optional[str], protect: bool):
    kwargs: Dict[str, Any] = {}
    if caption:
        kwargs["caption"] = caption
        kwargs["parse_mode"] = ParseMode.HTML
    # add protect_content if supported; fall back gracefully if not
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
        # protect_content not supported -> remove and retry
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


# ----------------------------
# Autodelete worker
# ----------------------------
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
                    logger.info("Autodelete: %d messages due for deletion", len(due))
                for rec in due:
                    try:
                        await bot.delete_message(chat_id=rec.chat_id, message_id=rec.message_id)
                        logger.debug("Deleted message %s:%s", rec.chat_id, rec.message_id)
                    except Exception:
                        logger.warning("Autodelete failed to delete %s:%s", rec.chat_id, rec.message_id)
                    try:
                        await db.delete(rec)
                    except Exception:
                        logger.exception("Autodelete failed to remove DB record for %s:%s", rec.chat_id, rec.message_id)
                await db.commit()
        except Exception:
            logger.exception("Autodelete worker exception: %s", traceback.format_exc())
        await asyncio.sleep(AUTODELETE_CHECK_INTERVAL)


# ----------------------------
# Handlers
# ----------------------------
@dp.message_handler(commands=["health"])
async def cmd_health(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can use /health here.")
        return
    await message.reply("OK")


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    """
    /start -> greeting (with placeholders)
    /start <token> -> deep link delivery
    """
    # register user silently for broadcasts
    try:
        await save_user_if_not_exists(message.from_user)
    except Exception:
        logger.exception("save_user_if_not_exists failed in /start")

    args = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            args = parts[1].strip()

    if not args:
        # show start content
        content, photo = await fetch_start_message()
        if photo:
            caption_html = render_text_with_links(content or "", message.from_user.first_name)
            try:
                await bot.send_photo(chat_id=message.chat.id, photo=photo, caption=caption_html, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                return
            except Exception:
                logger.exception("Failed to send start photo")
        if content:
            try:
                await bot.send_message(chat_id=message.chat.id, text=render_text_with_links(content, message.from_user.first_name), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                return
            except Exception:
                pass
        await bot.send_message(chat_id=message.chat.id, text=f"Welcome, {message.from_user.first_name or 'friend'}!")
        return

    # args present -> deep link token
    token = args
    session = await get_session_by_token(token)
    if not session:
        await bot.send_message(chat_id=message.chat.id, text="❌ Link not found or invalid.")
        return
    if session.revoked or session.status == "revoked":
        await bot.send_message(chat_id=message.chat.id, text="❌ This link has been revoked.")
        return
    if session.status != "published":
        await bot.send_message(chat_id=message.chat.id, text="❌ This link is not published.")
        return

    files = await list_files_for_session(session.id)
    if not files:
        await bot.send_message(chat_id=message.chat.id, text="❌ No files for this link.")
        return

    delivered = 0
    last_sent_msg = None
    for f in files:
        try:
            caption_html = render_text_with_links(f.caption or "", message.from_user.first_name)
            protect_flag = False if message.from_user.id == OWNER_ID else bool(session.protect_content)
            await delivery_semaphore.acquire()
            try:
                sent_msg = await send_media_with_protect(chat_id=message.chat.id, file_type=f.file_type, tg_file_id=f.tg_file_id, caption=caption_html or None, protect=protect_flag)
            finally:
                delivery_semaphore.release()
            delivered += 1
            last_sent_msg = sent_msg
            if session.autodelete_minutes and session.autodelete_minutes > 0:
                delete_time = datetime.utcnow() + timedelta(minutes=session.autodelete_minutes)
                await schedule_delivery(chat_id=sent_msg.chat.id, message_id=sent_msg.message_id, delete_at=delete_time)
        except Exception:
            logger.exception("Failed to deliver file %s for session %s", f.tg_file_id, session.link)

    # Only if autodelete set - send Delivery complete message and schedule its deletion
    if session.autodelete_minutes and session.autodelete_minutes > 0:
        try:
            info = f"Delivery complete.\nAbove files will be deleted in {session.autodelete_minutes} minute(s)."
            sent_info = await bot.send_message(chat_id=message.chat.id, text=info)
            delete_time = datetime.utcnow() + timedelta(minutes=session.autodelete_minutes)
            await schedule_delivery(chat_id=sent_info.chat.id, message_id=sent_info.message_id, delete_at=delete_time)
        except Exception:
            logger.exception("Failed to send autodelete info message")
    # If autodelete is 0 -> specification says no deletion message should be sent; do nothing further.


@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /upload.")
        return
    try:
        existing = await get_owner_active_draft(OWNER_ID)
    except Exception:
        logger.exception("get_owner_active_draft failed")
        existing = None
    if existing:
        await message.reply(f"You already have an active draft session. Continue sending files or use /done. Draft token: `{existing.link}`", parse_mode=ParseMode.MARKDOWN)
        return
    try:
        rec = await create_draft_session(OWNER_ID)
        await message.reply(
            f"✅ Upload session started.\nDraft token: `{rec.link}`\n"
            f"Send up to {MAX_FILES_PER_SESSION} files (photo/video/document/audio/voice/sticker/animation).\n"
            "When finished send /done. To cancel send /abort.",
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        logger.exception("create_draft_session failed")
        await message.reply("❌ Failed to start upload session. Check logs.")


@dp.message_handler(content_types=["photo", "video", "document", "audio", "voice", "sticker", "animation"])
async def handle_owner_media(message: types.Message):
    # Save non-owner users for broadcast list and ignore their media
    if message.from_user.id != OWNER_ID:
        try:
            await save_user_if_not_exists(message.from_user)
        except Exception:
            logger.exception("save_user_if_not_exists failed for non-owner media")
        return

    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        # no active draft
        return

    files = await list_files_for_session(draft.id)
    if len(files) >= MAX_FILES_PER_SESSION:
        await message.reply(f"Upload limit reached ({MAX_FILES_PER_SESSION}). Use /done or /abort.")
        return

    # parse file
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

    # forward to upload channel
    try:
        fwd = await bot.forward_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
    except Exception:
        logger.exception("Forward to upload channel failed")
        await message.reply("Failed to forward to upload channel. Ensure bot is member/admin and channel id is correct.")
        return

    # extract stable file id from forwarded message
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

    # save metadata
    try:
        await append_file_to_session(draft.id, channel_file_id, channel_type, channel_caption or "")
        current_count = len(await list_files_for_session(draft.id))
        await message.reply(f"Saved {channel_type}. ({current_count}/{MAX_FILES_PER_SESSION}) Send more or /done when finished.")
    except Exception:
        logger.exception("append_file_to_session failed")
        await message.reply("Failed to save file metadata; try again or /abort.")


@dp.message_handler(commands=["done"])
async def cmd_done(message: types.Message):
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

    # ask protect
    await set_session_status(draft.id, "awaiting_protect")
    await message.reply("All files for this session are ready. Protect content? Reply `on` or `off` (owner only).")


@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and (m.text or "").strip().lower() in ("on", "off"))
async def handle_protect_input(message: types.Message):
    # find awaiting_protect session
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.owner_id == OWNER_ID, SessionModel.status == "awaiting_protect").order_by(SessionModel.created_at.desc()).limit(1)
        res = await db.execute(stmt)
        draft = res.scalars().first()
    if not draft:
        # ignore stray
        return
    choice = (message.text or "").strip().lower()
    protect = True if choice == "on" else False
    draft.protect_content = protect
    draft.status = "awaiting_autodelete"
    async with AsyncSessionLocal() as db:
        db.add(draft)
        await db.commit()
        await db.refresh(draft)
    await message.reply(f"Protect set to {'ON' if protect else 'OFF'}. Now reply with autodelete minutes (0 - {AUTODELETE_MAX_MIN}). 0 = disabled.")


@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and (m.text or "").strip().isdigit())
async def handle_autodelete_input(message: types.Message):
    # find awaiting_autodelete session
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.owner_id == OWNER_ID, SessionModel.status == "awaiting_autodelete").order_by(SessionModel.created_at.desc()).limit(1)
        res = await db.execute(stmt)
        draft = res.scalars().first()
    if not draft:
        # possibly unrelated numeric message - ignore
        return
    try:
        minutes = int((message.text or "").strip())
    except Exception:
        await message.reply(f"Invalid integer. Please provide 0..{AUTODELETE_MAX_MIN}.")
        return
    if minutes < 0 or minutes > AUTODELETE_MAX_MIN:
        await message.reply(f"Please provide number in range 0..{AUTODELETE_MAX_MIN}.")
        return

    # publish session
    published = await publish_session(draft.id, draft.protect_content, minutes)
    if not published:
        await message.reply("Failed to publish session; check logs.")
        return

    # deep link
    me = await bot.get_me()
    bot_username = me.username or ""
    deep_link = f"https://t.me/{bot_username}?start={published.link}"

    files = await list_files_for_session(published.id)
    total = len(files)

    # session summary - only to upload channel
    summary = (
        "✅ Upload Complete\n"
        f"Session ID: {published.id}\n"
        f"Deep Link: {deep_link}\n"
        f"Total Files: {total}\n"
        f"Protect: {'ON' if published.protect_content else 'OFF'}\n"
        f"Autodelete (minutes): {published.autodelete_minutes}\n"
        f"Token: `{published.link}`\n"
    )
    try:
        await bot.send_message(chat_id=UPLOAD_CHANNEL_ID, text=summary, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    except Exception:
        logger.exception("Failed to send session summary to upload channel")
    await message.reply("✅ Session published. Summary posted to upload channel (only).")


@dp.message_handler(commands=["abort"])
async def cmd_abort(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can /abort.")
        return
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        await message.reply("No active draft.")
        return
    ok = await delete_draft_session(draft.id)
    if ok:
        await message.reply("Upload aborted and draft removed.")
    else:
        await message.reply("Failed to abort draft.")


@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can /revoke.")
        return
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.reply("Usage: /revoke <token>")
        return
    token = parts[1].strip()
    ok = await revoke_session_by_token(token)
    if ok:
        await message.reply(f"✅ Revoked token {token}.")
    else:
        await message.reply("Token not found.")


@dp.message_handler(commands=["editstart", "edit_start"])
async def cmd_edit_start(message: types.Message):
    """
    Owner should reply to a message (text or photo+caption) with /editstart.
    Save photo_file_id and/or text accordingly.
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can use /editstart.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or photo) to set /start content.")
        return
    reply = message.reply_to_message
    try:
        if reply.photo:
            photo_file_id = reply.photo[-1].file_id
            caption = reply.caption or ""
            await save_start_message(caption, photo_file_id)
            await message.reply("✅ /start updated with image + caption (if present).")
            return
        if reply.text:
            await save_start_message(reply.text, None)
            await message.reply("✅ /start updated with text.")
            return
        if reply.caption:
            await save_start_message(reply.caption, None)
            await message.reply("✅ /start updated with caption.")
            return
        await message.reply("Unsupported reply type. Reply to text or photo.")
    except Exception:
        logger.exception("cmd_edit_start failed")
        await message.reply("Failed to update /start.")


@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    """
    Owner-only broadcast.
    Reply to a message to broadcast that message's content to all saved users.
    Supports placeholders in text: {first_name} and {word | url}
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can broadcast.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or photo) to broadcast.")
        return
    reply = message.reply_to_message

    # gather users
    async with AsyncSessionLocal() as db:
        stmt = select(UserModel.tg_id)
        res = await db.execute(stmt)
        rows = res.fetchall()
        user_ids = [r[0] for r in rows]
    if not user_ids:
        await message.reply("No users to broadcast to.")
        return

    # parse message content
    b_photo = None
    b_text = ""
    if reply.photo:
        b_photo = reply.photo[-1].file_id
        b_text = reply.caption or ""
    elif reply.text:
        b_text = reply.text
    else:
        b_text = reply.caption or ""

    await message.reply(f"Broadcasting to {len(user_ids)} users. This will run in background.")

    sem = asyncio.Semaphore(10)
    sent = 0
    failed = 0

    async def send_to(uid: int):
        nonlocal sent, failed
        try:
            await sem.acquire()
            # get first name
            fname = ""
            try:
                async with AsyncSessionLocal() as sdb:
                    q = select(UserModel).where(UserModel.tg_id == int(uid))
                    r = await sdb.execute(q)
                    usr = r.scalars().first()
                    fname = usr.first_name if usr else ""
            except Exception:
                fname = ""
            try:
                if b_photo:
                    caption_html = render_text_with_links(b_text or "", fname)
                    await bot.send_photo(chat_id=uid, photo=b_photo, caption=caption_html, parse_mode=ParseMode.HTML)
                else:
                    await bot.send_message(chat_id=uid, text=render_text_with_links(b_text or "", fname), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                sent += 1
            except Exception:
                logger.exception("Failed to send broadcast to %s", uid)
                failed += 1
        finally:
            sem.release()

    tasks = [asyncio.create_task(send_to(uid)) for uid in user_ids]
    await asyncio.gather(*tasks)
    await message.reply(f"Broadcast finished. Sent: {sent}. Failed: {failed}.")


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can use /help.")
        return
    await message.reply(
        "/upload - start upload (owner only)\n"
        "/done - finalize upload and publish (owner only)\n"
        "/abort - cancel current upload\n"
        "/editstart - reply to a message to set /start greeting (owner only)\n"
        "/revoke <token> - revoke a published session\n"
        "/broadcast - reply to a message to broadcast to all users\n"
        "/help - this message\n"
        "Public: /start and /start <token> to retrieve files"
    )


@dp.message_handler()
async def fallback_handler(message: types.Message):
    # register users for broadcast
    try:
        await save_user_if_not_exists(message.from_user)
    except Exception:
        logger.exception("fallback_handler: save_user_if_not_exists failed")


# ----------------------------
# Webhook handler + health
# ----------------------------
async def webhook_handler(request: Request):
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400, text="invalid json")
    try:
        update = types.Update.de_json(data)
    except Exception:
        logger.exception("Failed to parse update JSON")
        return web.Response(status=400, text="bad update")
    # ensure current bot context (some aiogram internals rely on it)
    try:
        Bot.set_current(bot)
    except Exception:
        pass
    try:
        await dp.process_update(update)
    except Exception:
        logger.exception("Dispatcher processing failed: %s", traceback.format_exc())
    return web.Response(text="OK")


async def health_handler(request: Request):
    return web.Response(text="OK")


# ----------------------------
# Startup / run
# ----------------------------
async def start_services_and_run():
    logger.info("Launching session_share_bot; webhook_host=%s port=%s", WEBHOOK_HOST, PORT)
    try:
        await init_db()
    except Exception:
        logger.exception("init_db failed; proceeding anyway")

    # start autodelete worker
    ad_task = asyncio.create_task(autodelete_worker())
    logger.info("Autodelete worker scheduled")

    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, webhook_handler)
    app.router.add_get(WEBHOOK_PATH, lambda req: web.Response(text="Webhook endpoint (GET)"))
    app.router.add_get("/health", health_handler)
    app.router.add_get("/", lambda req: web.Response(text="Bot running"))
    # store task for graceful cleanup
    app["autodelete_task"] = ad_task

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

    try:
        await asyncio.Event().wait()
    finally:
        logger.info("Shutdown initiated")
        with suppress(Exception):
            await bot.delete_webhook()
        try:
            ad_task.cancel()
            with suppress(Exception):
                await ad_task
        except Exception:
            logger.exception("Error canceling autodelete task")
        await runner.cleanup()
        with suppress(Exception):
            await bot.session.close()


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