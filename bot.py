# bot.py
"""
Session-share Telegram bot - final single-file.

Features:
- aiogram v2 (compatible with aiogram 2.25.0)
- PostgreSQL via SQLAlchemy async + asyncpg (recommended for concurrency)
- All Telegram ID columns use BigInteger to avoid overflow
- Upload flow: /upload (owner) -> send 1..99 files -> /done -> protect on/off -> autodelete minutes -> publish deep-link
- Files forwarded to UPLOAD_CHANNEL; DB stores stable file_id references (low DB storage)
- Deep link: https://t.me/<bot_username>?start=<token>
- /revoke <token> (owner only)
- /editstart (owner only) - reply to message (text or photo) to set /start content
- /broadcast (owner only) - reply to message and broadcast to saved users. Supports {first_name} and {word | url}
- /help (owner only), /health (public)
- Autodelete worker removes delivered messages when their timer expires
- Best-effort auto-migrations converting integer -> bigint for ID columns
- Designed to run on Render / any container host; use PostgreSQL for persistence and concurrency
"""

# -----------------------
# Standard library imports
# -----------------------
import os
import sys
import asyncio
import logging
import secrets
import html
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

# -----------------------
# aiohttp for webhook server
# -----------------------
from aiohttp import web
from aiohttp.web_request import Request

# -----------------------
# aiogram v2
# -----------------------
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage

# -----------------------
# SQLAlchemy + asyncpg
# -----------------------
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

# -----------------------
# Logging
# -----------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("session_share_bot")

# -----------------------
# Environment variables (required)
# -----------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID_ENV = os.environ.get("OWNER_ID")
DATABASE_URL = os.environ.get("DATABASE_URL")  # e.g. postgresql+asyncpg://user:pass@host:5432/dbname
UPLOAD_CHANNEL_ID_ENV = os.environ.get("UPLOAD_CHANNEL_ID")  # e.g. -1001234567890
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # e.g. https://service.onrender.com
PORT = int(os.environ.get("PORT", "10000"))

# Tunables
MAX_FILES_PER_SESSION = int(os.environ.get("MAX_FILES_PER_SESSION", "99"))
MAX_CONCURRENT_DELIVERIES = int(os.environ.get("MAX_CONCURRENT_DELIVERIES", "50"))
BROADCAST_CONCURRENCY = int(os.environ.get("BROADCAST_CONCURRENCY", "10"))
AUTODELETE_CHECK_INTERVAL = int(os.environ.get("AUTODELETE_CHECK_INTERVAL", "30"))  # seconds
DRAFT_TOKEN_LENGTH = int(os.environ.get("DRAFT_TOKEN_LENGTH", "64"))
AUTODELETE_MAX_MINUTES = int(os.environ.get("AUTODELETE_MAX_MINUTES", "10080"))  # 7 days

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
except Exception as e:
    raise RuntimeError("OWNER_ID must be an integer. Error: " + str(e))

try:
    UPLOAD_CHANNEL_ID = int(UPLOAD_CHANNEL_ID_ENV)
except Exception as e:
    raise RuntimeError("UPLOAD_CHANNEL_ID must be an integer. Error: " + str(e))

WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST.rstrip('/')}{WEBHOOK_PATH}"

logger.info("Configuration: OWNER_ID=%s, UPLOAD_CHANNEL_ID=%s, WEBHOOK_HOST=%s, PORT=%s", OWNER_ID, UPLOAD_CHANNEL_ID, WEBHOOK_HOST, PORT)

# -----------------------
# Database models
# -----------------------
Base = declarative_base()


class UserModel(Base):
    __tablename__ = "users"
    id = Column(BigInteger, primary_key=True)  # store Telegram user id as BIGINT
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    username = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    link = Column(String(128), unique=True, nullable=False, index=True)
    owner_id = Column(BigInteger, nullable=False)  # BIGINT to avoid overflow
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
    tg_file_id = Column(String, nullable=False)  # stable file_id from upload channel
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


Index("ix_files_session_order", FileModel.session_id, FileModel.order_index)

# -----------------------
# Async engine & sessionmaker
# -----------------------
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

# -----------------------
# Auto-migration helpers (safe)
# -----------------------
async def ensure_tables_and_safe_migrations():
    """
    Create tables if missing and perform safe, best-effort migrations:
    - Create missing tables/columns
    - Attempt to ALTER known integer ID columns to BIGINT if they are INTEGER (fix "integer out of range")
    """
    logger.info("DB init: creating tables and attempting safe migrations...")
    async with engine.begin() as conn:
        # Create tables
        await conn.run_sync(Base.metadata.create_all)

        # Add commonly-missing columns (safe)
        try:
            await conn.execute(sa_text("ALTER TABLE IF EXISTS start_message ADD COLUMN IF NOT EXISTS photo_file_id TEXT"))
        except Exception:
            logger.debug("Could not auto-add start_message.photo_file_id (ignorable)")

        try:
            await conn.execute(sa_text("ALTER TABLE IF EXISTS sessions ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'draft'"))
        except Exception:
            logger.debug("sessions.status already exists or could not be added")

        try:
            await conn.execute(sa_text("ALTER TABLE IF EXISTS sessions ADD COLUMN IF NOT EXISTS protect_content BOOLEAN DEFAULT FALSE"))
        except Exception:
            logger.debug("sessions.protect_content exists or could not be added")

        try:
            await conn.execute(sa_text("ALTER TABLE IF EXISTS sessions ADD COLUMN IF NOT EXISTS autodelete_minutes INTEGER DEFAULT 0"))
        except Exception:
            logger.debug("sessions.autodelete_minutes exists or could not be added")

    # Attempt to alter integer -> bigint for known columns
    await attempt_alter_integer_columns_to_bigint()


async def attempt_alter_integer_columns_to_bigint():
    """
    Check known columns, and if their data type is integer, attempt to ALTER them to BIGINT.
    This fixes numeric out of range problems due to large Telegram IDs.
    """
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
                    q = """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = :table AND column_name = :col;
                    """
                    res = await conn.execute(sa_text(q).bindparams(table=table, col=col))
                    row = res.first()
                    if not row:
                        continue
                    dtype = row[0]
                    if dtype == "integer":
                        alter_sql = f'ALTER TABLE "{table}" ALTER COLUMN "{col}" TYPE BIGINT USING "{col}"::BIGINT;'
                        try:
                            logger.info("Altering %s.%s from INTEGER -> BIGINT", table, col)
                            await conn.execute(sa_text(alter_sql))
                            logger.info("Altered %s.%s to BIGINT successfully", table, col)
                        except Exception:
                            logger.exception("Failed to ALTER %s.%s to BIGINT", table, col)
                except Exception:
                    logger.exception("Error checking/altering %s.%s", table, col)


# -----------------------
# Aiogram setup
# -----------------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)

delivery_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DELIVERIES)

# -----------------------
# Utilities
# -----------------------
def generate_token(length: int = DRAFT_TOKEN_LENGTH) -> str:
    return secrets.token_urlsafe(length)[:length]


def render_text_with_links(text: Optional[str], first_name: Optional[str] = None) -> str:
    """
    Replace {first_name} and convert {word | url} patterns into HTML-safe anchor tags.
    Example: "Click {here | https://example.com}" -> "Click <a href='https://example.com'>here</a>"
    """
    if not text:
        return ""
    rendered = text.replace("{first_name}", html.escape(first_name or ""))
    out: List[str] = []
    i = 0
    L = len(rendered)
    while i < L:
        s = rendered.find("{", i)
        if s == -1:
            out.append(html.escape(rendered[i:]))
            break
        e = rendered.find("}", s)
        if e == -1:
            out.append(html.escape(rendered[i:]))
            break
        out.append(html.escape(rendered[i:s]))
        inner = rendered[s + 1 : e].strip()
        if "|" in inner:
            left, right = inner.split("|", 1)
            left_escaped = html.escape(left.strip())
            href = html.escape(right.strip(), quote=True)
            out.append(f'<a href="{href}">{left_escaped}</a>')
        else:
            out.append(html.escape("{" + inner + "}"))
        i = e + 1
    return "".join(out)


# -----------------------
# Database helper functions
# -----------------------
async def init_db():
    await ensure_tables_and_safe_migrations()
    logger.info("Database initialization complete.")


async def save_user_if_not_exists(user: types.User):
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
            logger.debug("Saved user %s", user.id)
        except Exception:
            logger.exception("save_user_if_not_exists failed")


async def create_draft_session(owner_id: int) -> SessionModel:
    """
    Creates a draft session and retries with adjustments on certain DB errors (e.g. integer overflow).
    """
    async with AsyncSessionLocal() as db:
        last_exc = None
        for attempt in range(5):
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
                if "out of range" in str(e).lower() or "numericvalueoutofrange" in str(e).lower():
                    logger.warning("Detected numeric out of range; attempting to alter integer columns to BIGINT and retry.")
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


# -----------------------
# send helpers (protect content support)
# -----------------------
async def send_media_with_protect(file_type: str, chat_id: int, tg_file_id: str, caption: Optional[str], protect: bool):
    kwargs: Dict[str, Any] = {}
    if caption:
        kwargs["caption"] = caption
        kwargs["parse_mode"] = "HTML"
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
        # protect_content not supported in this aiogram version; retry without it
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


# -----------------------
# autodelete worker
# -----------------------
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
                        logger.debug("Deleted delivered message %s:%s", rec.chat_id, rec.message_id)
                    except Exception as e:
                        logger.warning("Autodelete: failed to delete %s:%s -> %s", rec.chat_id, rec.message_id, e)
                    try:
                        await db.delete(rec)
                    except Exception:
                        logger.exception("Autodelete: failed to delete DB record")
                await db.commit()
        except Exception:
            logger.exception("Autodelete worker exception: %s", traceback.format_exc())
        await asyncio.sleep(AUTODELETE_CHECK_INTERVAL)


# -----------------------
# handlers
# -----------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    """
    /start shows welcome (supports {first_name} and {word | url})
    /start <token> delivers files for token
    """
    # Save user for broadcast list
    try:
        await save_user_if_not_exists(message.from_user)
    except Exception:
        logger.exception("Failed to save user on /start")

    args = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            args = parts[1].strip()

    if not args:
        content, photo = await fetch_start_message()
        if photo:
            try:
                caption_html = render_text_with_links(content, message.from_user.first_name)
                await message.answer_photo(photo=photo, caption=caption_html, parse_mode="HTML", disable_notification=True)
            except Exception:
                try:
                    await message.answer(render_text_with_links(content, message.from_user.first_name), parse_mode="HTML")
                except Exception:
                    logger.exception("Failed to send start fallback")
        else:
            if content:
                try:
                    await message.answer(render_text_with_links(content, message.from_user.first_name), parse_mode="HTML", disable_web_page_preview=True)
                except Exception:
                    await message.answer(content.replace("{first_name}", message.from_user.first_name or ""))
        return

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
            logger.exception("Failed to deliver file %s for session %s", f.tg_file_id, session_obj.link)
    if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
        await message.answer(f"Files delivered: {delivered}. They will be deleted after {session_obj.autodelete_minutes} minute(s).")
    else:
        await message.answer(f"Files delivered: {delivered}. (Autodelete disabled)")


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
    # If not owner, just save user and ignore
    if message.from_user.id != OWNER_ID:
        try:
            await save_user_if_not_exists(message.from_user)
        except Exception:
            logger.exception("save_user_if_not_exists failed")
        return

    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        # no upload in progress
        return

    files = await list_files_for_session(draft.id)
    if len(files) >= MAX_FILES_PER_SESSION:
        await message.reply(f"Upload limit reached ({MAX_FILES_PER_SESSION}). Use /done or /abort.")
        return

    # detect type and file_id
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

    # forward to upload channel (so file is stored there and we can use it's stable file_id)
    try:
        fwd = await bot.forward_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
    except Exception:
        logger.exception("Forward to upload channel failed")
        await message.reply("Failed to forward to upload channel. Ensure the bot is member/admin and the channel id is correct.")
        return

    # extract file id from forwarded message
    try:
        if fwd.photo:
            channel_type = "photo"; channel_file_id = fwd.photo[-1].file_id; channel_caption = fwd.caption or ""
        elif fwd.video:
            channel_type = "video"; channel_file_id = fwd.video.file_id; channel_caption = fwd.caption or ""
        elif fwd.document:
            channel_type = "document"; channel_file_id = fwd.document.file_id; channel_caption = fwd.caption or ""
        elif fwd.audio:
            channel_type = "audio"; channel_file_id = fwd.audio.file_id; channel_caption = fwd.caption or ""
        elif fwd.voice:
            channel_type = "voice"; channel_file_id = fwd.voice.file_id; channel_caption = ""
        elif fwd.sticker:
            channel_type = "sticker"; channel_file_id = fwd.sticker.file_id; channel_caption = ""
        elif fwd.animation:
            channel_type = "animation"; channel_file_id = fwd.animation.file_id; channel_caption = fwd.caption or ""
        else:
            channel_type = ftype; channel_file_id = orig_file_id; channel_caption = caption
    except Exception:
        logger.exception("Failed to extract stable file id from forwarded message; falling back to original")
        channel_type = ftype; channel_file_id = orig_file_id; channel_caption = caption

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
        await message.reply("❌ Only owner can use /done.")
        return
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        await message.reply("No active upload session.")
        return
    files = await list_files_for_session(draft.id)
    if not files:
        await message.reply("No files uploaded in this session. Upload files first.")
        return
    await set_session_status(draft.id, "awaiting_protect")
    await message.reply("All files for this session are ready. Protect content? Reply with `on` or `off` (owner only).")


@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and (m.text or "").strip().lower() in ("on", "off"))
async def owner_protect_reply(message: types.Message):
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft or draft.status != "awaiting_protect":
        return
    text = (message.text or "").strip().lower()
    protect = text == "on"
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
    await message.reply(f"Protect set to {'ON' if protect else 'OFF'}. Now reply with autodelete minutes (0 - {AUTODELETE_MAX_MINUTES}). 0 = disabled.")


@dp.message_handler(commands=["setprotect"])
async def cmd_setprotect(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Only owner can use /setprotect")
        return
    parts = message.text.strip().split()
    if len(parts) < 2 or parts[1].lower() not in ("on", "off"):
        await message.reply("Usage: /setprotect on|off")
        return
    val = parts[1].lower() == "on"
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        await message.reply("No active draft.")
        return
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.id == draft.id).limit(1)
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if not rec:
            await message.reply("Session not found.")
            return
        rec.protect_content = val
        rec.status = "awaiting_autodelete"
        db.add(rec)
        await db.commit()
    await message.reply(f"Protect set to {'ON' if val else 'OFF'} for current draft. Now set autodelete with /setautodelete <minutes>.")


@dp.message_handler(commands=["setautodelete"])
async def cmd_setautodelete(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Only owner can use /setautodelete")
        return
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.reply(f"Usage: /setautodelete <minutes> (0 to disable, max {AUTODELETE_MAX_MINUTES})")
        return
    try:
        minutes = int(parts[1])
        if minutes < 0 or minutes > AUTODELETE_MAX_MINUTES:
            raise ValueError
    except Exception:
        await message.reply(f"Please provide integer minutes between 0 and {AUTODELETE_MAX_MINUTES}.")
        return
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft:
        await message.reply("No active draft.")
        return
    rec = await set_protect_and_autodelete(draft.id, draft.protect_content, minutes)
    if not rec:
        await message.reply("Failed to finalize session.")
        return
    me = await bot.get_me()
    bot_username = me.username or "bot"
    deep_link = f"https://t.me/{bot_username}?start={rec.link}"
    await message.reply(f"✅ Session published.\nDeep link:\n{deep_link}\nProtect: {'ON' if rec.protect_content else 'OFF'}\nAutodelete: {rec.autodelete_minutes} minute(s). Use /revoke <token> to disable.")


@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and (m.text or "").strip().isdigit())
async def owner_autodelete_reply(message: types.Message):
    draft = await get_owner_active_draft(OWNER_ID)
    if not draft or draft.status != "awaiting_autodelete":
        return
    try:
        minutes = int((message.text or "").strip())
        if minutes < 0 or minutes > AUTODELETE_MAX_MINUTES:
            raise ValueError
    except ValueError:
        await message.reply(f"Please send an integer between 0 and {AUTODELETE_MAX_MINUTES}.")
        return
    rec = await set_protect_and_autodelete(draft.id, draft.protect_content, minutes)
    if not rec:
        await message.reply("Failed to finalize session.")
        return
    me = await bot.get_me()
    bot_username = me.username or "bot"
    deep_link = f"https://t.me/{bot_username}?start={rec.link}"
    await message.reply(f"✅ Session published.\nDeep link:\n{deep_link}\nProtect: {'ON' if rec.protect_content else 'OFF'}\nAutodelete: {rec.autodelete_minutes} minute(s). Use /revoke <token> to disable.")


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
        await message.reply("Upload aborted; draft removed from DB.")
    else:
        await message.reply("Failed to abort session.")


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
        await message.reply(f"✅ Token revoked: {token}")
    else:
        await message.reply("Token not found.")


@dp.message_handler(commands=["editstart", "edit_start"])
async def cmd_edit_start(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can use /editstart.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or photo) to set /start.")
        return
    reply = message.reply_to_message
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
    await message.reply("Unsupported message type. Reply to text or photo.")


@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can use /broadcast.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or photo) to broadcast.")
        return
    reply = message.reply_to_message

    async with AsyncSessionLocal() as db:
        stmt = select(UserModel.id)
        res = await db.execute(stmt)
        rows = res.fetchall()
        user_ids = [r[0] for r in rows]
    if not user_ids:
        await message.reply("No users to broadcast to.")
        return

    b_text = None
    b_photo = None
    try:
        if reply.photo:
            b_photo = reply.photo[-1].file_id
            b_text = reply.caption or ""
        elif reply.text:
            b_text = reply.text
        else:
            b_text = reply.caption or ""
    except Exception:
        logger.exception("Failed to parse broadcast content")
        await message.reply("Failed to parse broadcast content.")
        return

    await message.reply(f"Broadcast starting to {len(user_ids)} users. This may take a while.")

    sem = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    sent_count = 0
    fail_count = 0

    async def send_to_user(uid: int):
        nonlocal sent_count, fail_count
        try:
            await sem.acquire()
            fname = ""
            try:
                async with AsyncSessionLocal() as sdb:
                    stmt = select(UserModel).where(UserModel.id == int(uid))
                    r = await sdb.execute(stmt)
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
                    await bot.send_message(chat_id=int(uid), text=render_text_with_links(b_text or "", fname), parse_mode="HTML")
                    sent_count += 1
            except Exception:
                logger.exception("Broadcast send failed for %s", uid)
                fail_count += 1
        finally:
            sem.release()

    tasks = [asyncio.create_task(send_to_user(uid)) for uid in user_ids]
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
        "/setprotect on|off - set protect for current draft\n"
        "/setautodelete <minutes> - set autodelete and publish\n"
        "/revoke <token> - revoke a published session\n"
        "/editstart - reply to a message to set /start\n"
        "/broadcast - reply to a message to broadcast all users\n"
        "/help - this message\n\nPublic: /start and /start <token>"
    )


@dp.message_handler()
async def fallback(message: types.Message):
    # Save user info silently for broadcast list
    try:
        await save_user_if_not_exists(message.from_user)
    except Exception:
        logger.exception("save_user_if_not_exists failed in fallback")
    # intentionally silent


# -----------------------
# Webhook handler for aiohttp
# -----------------------
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
    # process update
    try:
        # Set current bot context for aiogram internals if available
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
        await dp.process_update(update)
    except Exception:
        logger.exception("Dispatcher processing failed: %s", traceback.format_exc())
    return web.Response(text="OK")


async def health_handler(request: Request):
    return web.Response(text="OK")


# -----------------------
# Startup sequence & run server
# -----------------------
async def start_services_and_run():
    logger.info("Starting session_share_bot; webhook_host=%s port=%s", WEBHOOK_HOST, PORT)
    try:
        await init_db()
    except Exception:
        logger.exception("init_db failed; proceeding anyway (see logs)")

    # start autodelete worker
    asyncio.create_task(autodelete_worker())
    logger.info("Autodelete worker scheduled")

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

    # keep alive
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