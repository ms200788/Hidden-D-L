# bot.py
"""
Session-share Telegram bot (single file)
- aiogram==2.25.0
- aiohttp==3.8.6
- SQLAlchemy (1.4.x) + asyncpg
- python-dotenv optional for local testing

Features:
 - Owner-only upload flow: /upload -> send files -> /done -> protect on/off -> autodelete (0..10080)
 - Files forwarded to UPLOAD_CHANNEL (env) and file_id stored in DB per session
 - Session summary (session id, deep link, file count) posted ONLY to upload channel
 - Deep link access: /start <token> or clicking link will deliver files to user with original captions
 - Delivery supports protect_content (owner bypasses it)
 - If autodelete > 0: after sending files, bot sends a "Delivery complete" message
   that says "Above files will be deleted in <x> minutes." and schedules deletions
 - /revoke <token> owner-only to revoke session
 - /broadcast owner-only (supports {first_name} and {word | url})
 - /edit_start owner-only: reply to a message (text or photo+caption) to set /start greeting
 - /help owner-only
 - /health endpoint for uptime monitoring
 - DB uses BigInteger for Telegram IDs to avoid int32 overflow
 - Autodelete worker runs in background and deletes messages at scheduled times
 - Webhook via aiohttp for Render deployments
"""

import os
import sys
import asyncio
import logging
import secrets
import html
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

from aiohttp import web
from aiohttp.web_request import Request

# aiogram v2 imports
from aiogram import Bot, Dispatcher, types
from aiogram.utils.executor import start_webhook
from aiogram.types import ParseMode

# FSM not strictly necessary; we will use DB-driven simple state flags for flow
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
    Index,
    text as sa_text,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# Logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("session_share_bot")

# -------------------------
# Environment / Configuration
# -------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID_ENV = os.environ.get("OWNER_ID")
DATABASE_URL = os.environ.get("DATABASE_URL")  # e.g. postgresql+asyncpg://user:pass@host:5432/db
UPLOAD_CHANNEL_ID_ENV = os.environ.get("UPLOAD_CHANNEL_ID")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # e.g. https://your-render-url.onrender.com
PORT = int(os.environ.get("PORT", "10000"))

# Tunables
MAX_FILES_PER_SESSION = int(os.environ.get("MAX_FILES_PER_SESSION", "99"))
DRAFT_TOKEN_LENGTH = int(os.environ.get("DRAFT_TOKEN_LENGTH", "64"))
AUTODELETE_CHECK_SECONDS = int(os.environ.get("AUTODELETE_CHECK_SECONDS", "30"))
MAX_AUTODELETE_MINUTES = int(os.environ.get("MAX_AUTODELETE_MINUTES", "10080"))  # 7 days

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
except Exception:
    raise RuntimeError("OWNER_ID must be a numeric Telegram ID (can be large)")

try:
    UPLOAD_CHANNEL_ID = int(UPLOAD_CHANNEL_ID_ENV)
except Exception:
    raise RuntimeError("UPLOAD_CHANNEL_ID must be integer (channel ID), e.g. -1001234567890")

WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST.rstrip('/')}{WEBHOOK_PATH}"

logger.info("Configuration: OWNER_ID=%s, UPLOAD_CHANNEL_ID=%s, WEBHOOK_HOST=%s, PORT=%s", OWNER_ID, UPLOAD_CHANNEL_ID, WEBHOOK_HOST, PORT)

# -------------------------
# Database models
# -------------------------
Base = declarative_base()


class UserModel(Base):
    __tablename__ = "users"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    tg_id = Column(BigInteger, unique=True, nullable=False, index=True)  # Telegram numeric id
    first_name = Column(String, nullable=True)
    username = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    link = Column(String(128), unique=True, nullable=False, index=True)  # token used in deep link
    owner_id = Column(BigInteger, nullable=False, index=True)
    status = Column(String(32), nullable=False, default="draft")  # draft, awaiting_protect, awaiting_autodelete, published, revoked
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

# -------------------------
# Async DB engine
# -------------------------
engine = create_async_engine(DATABASE_URL, future=True, echo=False)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


# -------------------------
# DB init / migration helpers
# -------------------------
async def ensure_tables_and_migrate():
    """
    Create tables and attempt safe migrations (best-effort).
    - ensures tables exist
    - attempts to alter integer columns to BIGINT if needed (to avoid numeric overflow)
    """
    logger.info("DB init: creating tables and performing safe migrations (best-effort).")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        # try to add missing columns if necessary (safe-checked)
        try:
            await conn.execute(sa_text("ALTER TABLE IF EXISTS start_message ADD COLUMN IF NOT EXISTS photo_file_id TEXT"))
        except Exception:
            logger.debug("start_message.photo_file_id may already exist or alteration failed.")

    # attempt to alter integer columns to bigint to prevent numeric overflow problems
    await attempt_alter_integer_columns_to_bigint()


async def attempt_alter_integer_columns_to_bigint():
    """
    Inspect information_schema and alter integer -> bigint for known columns if necessary.
    This is best-effort; will log but continue on errors.
    """
    logger.info("Attempting to convert integer columns to BIGINT where needed.")
    info_sql = """
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name IN ('users','sessions','files','deliveries','start_message');
    """
    async with engine.begin() as conn:
        try:
            res = await conn.execute(sa_text(info_sql))
            rows = res.fetchall()
            # rows: list of tuples (table_name, column_name, data_type)
            # Only alter columns known to be integer
            for table_name, column_name, data_type in rows:
                if data_type == "integer":
                    try:
                        alter_sql = f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" TYPE BIGINT USING "{column_name}"::bigint;'
                        logger.info("Altering %s.%s -> BIGINT", table_name, column_name)
                        await conn.execute(sa_text(alter_sql))
                    except Exception:
                        logger.exception("Failed to alter %s.%s", table_name, column_name)
        except Exception:
            logger.exception("Failed reading information_schema for bigint migration")


# -------------------------
# Bot & Dispatcher
# -------------------------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# Semaphore to throttle concurrent deliveries to Telegram
DELIVERY_CONCURRENCY = int(os.environ.get("DELIVERY_CONCURRENCY", "50"))
delivery_sem = asyncio.Semaphore(DELIVERY_CONCURRENCY)


# -------------------------
# Utility helpers
# -------------------------
def gen_token(n: int = DRAFT_TOKEN_LENGTH) -> str:
    return secrets.token_urlsafe(n)[:n]


def render_text_with_links(text: Optional[str], first_name: Optional[str] = None) -> str:
    """
    Render placeholders:
    - {first_name}
    - {word | url}  -> becomes HTML anchor <a href="url">word</a>
    Returns HTML-escaped text with anchor tags included.
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


# -------------------------
# Database CRUD helpers
# -------------------------
async def init_db():
    await ensure_tables_and_migrate()
    logger.info("DB initialized.")


async def register_user_if_needed(user: types.User):
    if not user:
        return
    async with AsyncSessionLocal() as db:
        try:
            stmt = select(UserModel).where(UserModel.tg_id == int(user.id))
            r = await db.execute(stmt)
            existing = r.scalars().first()
            if existing:
                return
            rec = UserModel(tg_id=int(user.id), first_name=user.first_name, username=user.username)
            db.add(rec)
            await db.commit()
            logger.debug("Registered user %s", user.id)
        except Exception:
            logger.exception("register_user_if_needed failed")


async def create_draft(owner_id: int) -> SessionModel:
    async with AsyncSessionLocal() as db:
        last_exc = None
        for attempt in range(4):
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
                logger.warning("create_draft attempt %s failed: %s", attempt + 1, e)
                if "out of range" in str(e).lower():
                    logger.warning("Numeric overflow detected; attempting bigint migration and retry")
                    try:
                        await attempt_alter_integer_columns_to_bigint()
                    except Exception:
                        logger.exception("bigint migration attempt error")
        logger.error("create_draft failed after retries: %s", last_exc)
        raise RuntimeError("Failed to create draft session") from last_exc


async def get_owner_draft(owner_id: int) -> Optional[SessionModel]:
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.owner_id == owner_id, SessionModel.status == "draft")
        res = await db.execute(stmt)
        return res.scalars().first()


async def get_latest_owner_waiting(owner_id: int, waiting_status: str) -> Optional[SessionModel]:
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.owner_id == owner_id, SessionModel.status == waiting_status).order_by(SessionModel.created_at.desc()).limit(1)
        res = await db.execute(stmt)
        return res.scalars().first()


async def append_file(session_id: int, tg_file_id: str, file_type: str, caption: Optional[str]) -> FileModel:
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(func.max(FileModel.order_index)).where(FileModel.session_id == session_id))
        max_idx = res.scalar()
        idx = (max_idx or 0) + 1
        rec = FileModel(session_id=session_id, tg_file_id=tg_file_id, file_type=file_type, caption=caption or "", order_index=idx)
        db.add(rec)
        await db.commit()
        await db.refresh(rec)
        logger.debug("Appended file to session %s idx %s", session_id, idx)
        return rec


async def list_session_files(session_id: int) -> List[FileModel]:
    async with AsyncSessionLocal() as db:
        stmt = select(FileModel).where(FileModel.session_id == session_id).order_by(FileModel.order_index)
        res = await db.execute(stmt)
        return res.scalars().all()


async def set_session_status(session_id: int, status: str):
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


async def revoke_session(token: str) -> bool:
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


async def delete_draft(session_id: int) -> bool:
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.id == session_id).limit(1)
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if rec:
            await db.delete(rec)
            await db.commit()
            logger.info("Deleted draft %s", session_id)
            return True
        return False


async def get_session_by_token(token: str) -> Optional[SessionModel]:
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.link == token).limit(1)
        res = await db.execute(stmt)
        return res.scalars().first()


async def save_start(content: Optional[str], photo_file_id: Optional[str]):
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


async def fetch_start() -> Tuple[Optional[str], Optional[str]]:
    async with AsyncSessionLocal() as db:
        stmt = select(StartMessage).order_by(StartMessage.updated_at.desc()).limit(1)
        res = await db.execute(stmt)
        rec = res.scalars().first()
        if rec:
            return rec.content, rec.photo_file_id
        return None, None


async def schedule_deletion(chat_id: int, message_id: int, delete_at: Optional[datetime]):
    if not delete_at:
        return
    async with AsyncSessionLocal() as db:
        rec = DeliveryModel(chat_id=chat_id, message_id=message_id, delete_at=delete_at)
        db.add(rec)
        await db.commit()
    logger.debug("Scheduled deletion %s:%s at %s", chat_id, message_id, delete_at.isoformat())


# -------------------------
# Send helpers
# -------------------------
async def send_media(chat_id: int, file_type: str, tg_file_id: str, caption: Optional[str], protect: bool):
    """
    Send specific media type. If aiogram version doesn't accept protect_content arg, fallback.
    Returns the sent Message object.
    """
    kwargs = {}
    if caption:
        kwargs["caption"] = caption
        kwargs["parse_mode"] = ParseMode.HTML
    # Some aiogram versions accept protect_content parameter; wrap to catch TypeError
    try:
        kwargs["protect_content"] = protect
        if file_type == "photo":
            return await bot.send_photo(chat_id, tg_file_id, **kwargs)
        elif file_type == "video":
            return await bot.send_video(chat_id, tg_file_id, **kwargs)
        elif file_type == "document":
            return await bot.send_document(chat_id, tg_file_id, **kwargs)
        elif file_type == "audio":
            return await bot.send_audio(chat_id, tg_file_id, **kwargs)
        elif file_type == "voice":
            # voice does not support caption usually
            kwargs.pop("caption", None)
            kwargs.pop("parse_mode", None)
            return await bot.send_voice(chat_id, tg_file_id, **kwargs)
        elif file_type == "sticker":
            kwargs.pop("caption", None)
            return await bot.send_sticker(chat_id, tg_file_id, **kwargs)
        elif file_type == "animation":
            return await bot.send_animation(chat_id, tg_file_id, **kwargs)
        else:
            return await bot.send_document(chat_id, tg_file_id, **kwargs)
    except TypeError:
        # fallback without protect_content param
        kwargs.pop("protect_content", None)
        try:
            if file_type == "photo":
                return await bot.send_photo(chat_id, tg_file_id, **kwargs)
            elif file_type == "video":
                return await bot.send_video(chat_id, tg_file_id, **kwargs)
            elif file_type == "document":
                return await bot.send_document(chat_id, tg_file_id, **kwargs)
            elif file_type == "audio":
                return await bot.send_audio(chat_id, tg_file_id, **kwargs)
            elif file_type == "voice":
                kwargs.pop("caption", None); kwargs.pop("parse_mode", None)
                return await bot.send_voice(chat_id, tg_file_id, **kwargs)
            elif file_type == "sticker":
                kwargs.pop("caption", None)
                return await bot.send_sticker(chat_id, tg_file_id, **kwargs)
            elif file_type == "animation":
                return await bot.send_animation(chat_id, tg_file_id, **kwargs)
            else:
                return await bot.send_document(chat_id, tg_file_id, **kwargs)
        except Exception:
            logger.exception("Fallback send failed")
            raise
    except Exception:
        logger.exception("send_media failed")
        raise


# -------------------------
# Autodelete worker
# -------------------------
async def autodelete_loop():
    logger.info("Autodelete worker started (interval %s seconds)", AUTODELETE_CHECK_SECONDS)
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
        await asyncio.sleep(AUTODELETE_CHECK_SECONDS)


# -------------------------
# Handlers: commands & messages
# -------------------------
@dp.message_handler(commands=["health"])
async def cmd_health(message: types.Message):
    # owner-only health? public endpoint exists; but allow owner to use in chat
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can use /health here.")
        return
    await message.reply("OK")


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    """
    /start -> shows configured start message
    /start <token> -> deep link delivery
    """
    try:
        await register_user_if_needed(message.from_user)
    except Exception:
        logger.exception("Failed to register user in /start")

    args = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            args = parts[1].strip()

    if not args:
        # show start content (from DB)
        content, photo = await fetch_start()
        if photo:
            caption_html = render_text_with_links(content or "", message.from_user.first_name)
            try:
                await bot.send_photo(chat_id=message.chat.id, photo=photo, caption=caption_html, parse_mode=ParseMode.HTML, disable_notification=True)
                return
            except Exception:
                logger.exception("Failed to send start photo fallback")
        if content:
            try:
                await bot.send_message(chat_id=message.chat.id, text=render_text_with_links(content, message.from_user.first_name), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                return
            except Exception:
                pass
        # default fallback
        await bot.send_message(chat_id=message.chat.id, text=f"Welcome, {message.from_user.first_name or 'friend'}!")
        return

    # args present -> treat as deep link token
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

    files = await list_session_files(session.id)
    if not files:
        await bot.send_message(chat_id=message.chat.id, text="❌ No files for this link.")
        return

    delivered = 0
    last_sent = None
    for f in files:
        try:
            caption_html = render_text_with_links(f.caption or "", message.from_user.first_name)
            protect_flag = False if message.from_user.id == OWNER_ID else bool(session.protect_content)
            await delivery_sem.acquire()
            try:
                sent_msg = await send_media(chat_id=message.chat.id, file_type=f.file_type, tg_file_id=f.tg_file_id, caption=caption_html or None, protect=protect_flag)
            finally:
                delivery_sem.release()
            delivered += 1
            last_sent = sent_msg
            if session.autodelete_minutes and session.autodelete_minutes > 0:
                delete_time = datetime.utcnow() + timedelta(minutes=session.autodelete_minutes)
                await schedule_deletion(chat_id=sent_msg.chat.id, message_id=sent_msg.message_id, delete_at=delete_time)
        except Exception:
            logger.exception("Failed to deliver file %s for session %s", f.tg_file_id, session.link)

    # After sending all files, send the "Delivery complete" message only if autodelete set OR always?
    # Requirement: If autodelete set -> send last message "Delivery complete. Above files will be deleted in <x> minutes."
    if session.autodelete_minutes and session.autodelete_minutes > 0:
        try:
            info = f"Delivery complete.\nAbove files will be deleted in {session.autodelete_minutes} minute(s)."
            sent_info = await bot.send_message(chat_id=message.chat.id, text=info)
            # schedule deletion for this info message if autodelete? The spec expects the files deleted; not necessarily the info message.
            # We'll schedule deletion for the info message too (after same minutes) to keep chat clean.
            delete_time = datetime.utcnow() + timedelta(minutes=session.autodelete_minutes)
            await schedule_deletion(chat_id=sent_info.chat.id, message_id=sent_info.message_id, delete_at=delete_time)
        except Exception:
            logger.exception("Failed to send autodelete info message")
    else:
        # If no autodelete set, do not send the autodelete info (as per your requirement), but spec said:
        # "If autodelete set bot send a message ... If not set -> no deletion happen on the chat of user."
        pass

    # done


@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    """
    Owner-only: start upload session (draft).
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /upload.")
        return
    existing = await get_owner_draft(OWNER_ID)
    if existing:
        await message.reply(f"There's already an active draft session (token `{existing.link}`). Continue sending files or use /done to finish or /abort to cancel.", parse_mode=ParseMode.MARKDOWN)
        return
    try:
        draft = await create_draft(OWNER_ID)
        await message.reply(f"✅ Upload session started. Draft token: `{draft.link}`\nSend up to {MAX_FILES_PER_SESSION} files now (photo/video/document/audio/voice/sticker/animation). When finished send /done, to cancel send /abort.", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        logger.exception("Failed to create draft session")
        await message.reply("❌ Failed to start upload session. Check logs.")


@dp.message_handler(content_types=["photo", "video", "document", "audio", "voice", "sticker", "animation"])
async def owner_media_handler(message: types.Message):
    """
    When owner sends media during a draft, forward to upload channel, store file id metadata.
    Also send a copy to owner chat for confirmation (as requested earlier).
    """
    # If not owner: register user and ignore (we store users for broadcast)
    if message.from_user.id != OWNER_ID:
        try:
            await register_user_if_needed(message.from_user)
        except Exception:
            logger.exception("register user failed in media handler")
        return

    # find draft
    draft = await get_owner_draft(OWNER_ID)
    if not draft:
        # no draft active; ignore
        return

    files_now = await list_session_files(draft.id)
    if len(files_now) >= MAX_FILES_PER_SESSION:
        await message.reply(f"Upload limit reached ({MAX_FILES_PER_SESSION}). Use /done or /abort.")
        return

    # Determine original file id and type and caption
    file_type = None
    orig_file_id = None
    caption = None
    try:
        if message.photo:
            file_type = "photo"; orig_file_id = message.photo[-1].file_id; caption = message.caption or ""
        elif message.video:
            file_type = "video"; orig_file_id = message.video.file_id; caption = message.caption or ""
        elif message.document:
            file_type = "document"; orig_file_id = message.document.file_id; caption = message.caption or ""
        elif message.audio:
            file_type = "audio"; orig_file_id = message.audio.file_id; caption = message.caption or ""
        elif message.voice:
            file_type = "voice"; orig_file_id = message.voice.file_id; caption = ""
        elif message.sticker:
            file_type = "sticker"; orig_file_id = message.sticker.file_id; caption = ""
        elif message.animation:
            file_type = "animation"; orig_file_id = message.animation.file_id; caption = message.caption or ""
    except Exception:
        logger.exception("Failed to parse incoming media")
        await message.reply("Failed to parse media. Try again.")
        return

    # Forward to upload channel (so channel hosts the file and we get a stable file_id)
    try:
        forwarded = await bot.forward_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
    except Exception:
        logger.exception("Forward to upload channel failed")
        await message.reply("Failed to forward to upload channel. Ensure bot is added to the channel and has permissions.")
        return

    # Extract file id from forwarded message in upload channel (safe)
    try:
        if forwarded.photo:
            channel_file_id = forwarded.photo[-1].file_id
            channel_type = "photo"
            channel_caption = forwarded.caption or ""
        elif forwarded.video:
            channel_file_id = forwarded.video.file_id
            channel_type = "video"
            channel_caption = forwarded.caption or ""
        elif forwarded.document:
            channel_file_id = forwarded.document.file_id
            channel_type = "document"
            channel_caption = forwarded.caption or ""
        elif forwarded.audio:
            channel_file_id = forwarded.audio.file_id
            channel_type = "audio"
            channel_caption = forwarded.caption or ""
        elif forwarded.voice:
            channel_file_id = forwarded.voice.file_id
            channel_type = "voice"
            channel_caption = ""
        elif forwarded.sticker:
            channel_file_id = forwarded.sticker.file_id
            channel_type = "sticker"
            channel_caption = ""
        elif forwarded.animation:
            channel_file_id = forwarded.animation.file_id
            channel_type = "animation"
            channel_caption = forwarded.caption or ""
        else:
            channel_file_id = orig_file_id
            channel_type = file_type
            channel_caption = caption or ""
    except Exception:
        logger.exception("Failed to extract channel file id; using original")
        channel_file_id = orig_file_id
        channel_type = file_type
        channel_caption = caption or ""

    # Save metadata to DB
    try:
        await append_file(draft.id, channel_file_id, channel_type, channel_caption or "")
        count = len(await list_session_files(draft.id))
        await message.reply(f"Saved {channel_type}. ({count}/{MAX_FILES_PER_SESSION})")
    except Exception:
        logger.exception("append_file failed")
        await message.reply("Failed to save file metadata; try /abort and retry.")


@dp.message_handler(commands=["done"])
async def cmd_done(message: types.Message):
    """
    Owner-only finalize: set protect on/off via reply, then autodelete minutes via reply.
    We use DB flags to track flow:
     - set status -> awaiting_protect: wait for owner to reply 'on' or 'off'
     - then set status -> awaiting_autodelete: wait for owner to reply number
     - when autodelete set: publish and send summary to upload channel only (token, file count, session id)
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can use /done.")
        return

    draft = await get_owner_draft(OWNER_ID)
    if not draft:
        await message.reply("No active upload session.")
        return

    files = await list_session_files(draft.id)
    if not files:
        await message.reply("No files in current session. Upload files first.")
        return

    # set awaiting_protect
    await set_session_status(draft.id, "awaiting_protect")
    await message.reply("All files for this session are ready. Protect content? Reply `on` or `off` (owner only).")


@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and (m.text or "").strip().lower() in ("on", "off"))
async def handle_protect_input(message: types.Message):
    # find session awaiting_protect
    draft = await get_latest_owner_waiting(OWNER_ID, "awaiting_protect")
    if not draft:
        # ignore stray on/off
        return
    choice = (message.text or "").strip().lower()
    protect = True if choice == "on" else False
    draft.protect_content = protect
    draft.status = "awaiting_autodelete"
    # commit
    async with AsyncSessionLocal() as db:
        db.add(draft)
        await db.commit()
        await db.refresh(draft)
    await message.reply(f"Protect set to {'ON' if protect else 'OFF'}. Now reply with autodelete minutes (0 - {MAX_AUTODELETE_MINUTES}). 0 = disabled.")


@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and (m.text or "").strip().isdigit())
async def handle_autodelete_input(message: types.Message):
    # find session awaiting_autodelete
    draft = await get_latest_owner_waiting(OWNER_ID, "awaiting_autodelete")
    if not draft:
        return
    try:
        minutes = int((message.text or "").strip())
    except Exception:
        await message.reply(f"Invalid integer. Please provide 0..{MAX_AUTODELETE_MINUTES}.")
        return
    if minutes < 0 or minutes > MAX_AUTODELETE_MINUTES:
        await message.reply(f"Please provide number in range 0..{MAX_AUTODELETE_MINUTES}.")
        return

    # publish
    published = await publish_session(draft.id, draft.protect_content, minutes)
    if not published:
        await message.reply("Failed to publish session; check logs.")
        return

    # Create deep link
    me = await bot.get_me()
    bot_username = me.username or ""
    deep_link = f"https://t.me/{bot_username}?start={published.link}"

    # Send session summary ONLY to upload channel
    files = await list_session_files(published.id)
    total = len(files)
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
        await message.reply("❌ Only owner can abort.")
        return
    draft = await get_owner_draft(OWNER_ID)
    if not draft:
        await message.reply("No active draft.")
        return
    ok = await delete_draft(draft.id)
    if ok:
        await message.reply("Upload aborted and draft removed.")
    else:
        await message.reply("Failed to abort draft.")


@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can revoke.")
        return
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.reply("Usage: /revoke <token>")
        return
    token = parts[1].strip()
    ok = await revoke_session(token)
    if ok:
        await message.reply(f"✅ Revoked token {token}.")
    else:
        await message.reply("Token not found.")


@dp.message_handler(commands=["editstart", "edit_start"])
async def cmd_edit_start(message: types.Message):
    """
    Owner replies to a message and runs /editstart to set /start greeting.
    If reply contains photo + caption -> set both.
    If reply contains text only -> set text.
    """
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only owner can use /editstart.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or photo) to set /start.")
        return
    reply = message.reply_to_message
    try:
        if reply.photo:
            photo_file_id = reply.photo[-1].file_id
            caption = reply.caption or ""
            await save_start(caption, photo_file_id)
            await message.reply("✅ /start updated with image + caption (if present).")
            return
        if reply.text:
            await save_start(reply.text, None)
            await message.reply("✅ /start updated with text.")
            return
        if reply.caption:
            await save_start(reply.caption, None)
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
    Reply to a message to broadcast that message's text or photo to all saved users.
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

    # send concurrently with semaphore
    sem = asyncio.Semaphore(10)
    sent = 0
    failed = 0

    async def send_to(uid: int):
        nonlocal sent, failed
        try:
            await sem.acquire()
            # get first name for placeholder substitution
            async with AsyncSessionLocal() as sdb:
                q = select(UserModel).where(UserModel.tg_id == int(uid))
                r = await sdb.execute(q)
                usr = r.scalars().first()
                fname = usr.first_name if usr else ""
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
async def fallback(message: types.Message):
    # register user for broadcast and ignore
    try:
        await register_user_if_needed(message.from_user)
    except Exception:
        logger.exception("fallback: register_user_if_needed failed")


# -------------------------
# Webhook (aiohttp) handler
# -------------------------
async def handle_webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400, text="invalid json")
    try:
        update = types.Update.de_json(data)
    except Exception:
        logger.exception("Failed to parse update JSON")
        return web.Response(status=400, text="bad update")
    try:
        await dp.process_update(update)
    except Exception:
        logger.exception("Dispatcher processing failed: %s", traceback.format_exc())
    return web.Response(text="OK")


# -------------------------
# Startup & run
# -------------------------
async def on_startup(app):
    logger.info("Starting services: DB init and autodelete worker.")
    try:
        await init_db()
    except Exception:
        logger.exception("init_db failed")
    # start autodelete worker
    app["autodelete_task"] = asyncio.create_task(autodelete_loop())
    # set webhook (best-effort)
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except Exception:
        logger.exception("Failed to set webhook")


async def on_cleanup(app):
    logger.info("Shutting down: cleaning up webhook and tasks")
    try:
        task = app.get("autodelete_task")
        if task:
            task.cancel()
            with suppress_exceptions():
                await task
    except Exception:
        logger.exception("Error cancelling autodelete task")
    try:
        await bot.delete_webhook()
    except Exception:
        logger.exception("Failed to delete webhook")
    try:
        await bot.session.close()
    except Exception:
        logger.exception("Failed to close bot session")


class suppress_exceptions:
    """Context manager to ignore exceptions in awaited cancel join (for cleanup)"""
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return True


def make_app():
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get("/health", lambda req: web.Response(text="OK"))
    app.router.add_get("/", lambda req: web.Response(text="Bot running"))
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


# -------------------------
# Convenience: run aiohttp app directly
# -------------------------
def run():
    # create aiohttp web app and run
    app = make_app()
    logger.info("Launching webhook server on port %s and health server", PORT)
    web.run_app(app, host="0.0.0.0", port=PORT)


# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    run()