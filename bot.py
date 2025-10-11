# bot.py
"""
Aiogram 2.25.0 - Session file sharing bot
- Webhook mode (aiohttp)
- PostgreSQL async via SQLAlchemy + asyncpg
- Uploads forwarded to a private upload channel; DB stores only stable channel file_ids
- Draft/publish flow for upload (durable across restarts)
- /edit_start supports reply-to-image (saves image + caption) or reply-to-text
- Up to 99 files per session
- Owner-only management commands: /upload, /done, /abort, /revoke, /edit_start, /help
- Public: /start (welcome or deep link: /start <token>)
- /health endpoint for uptime monitoring
"""

import os
import sys
import asyncio
import logging
import secrets
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

from aiohttp import web
from aiohttp.web_request import Request

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey,
    select, func, Index
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# ----------------------------
# Configuration & Logging
# ----------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("session_share_bot")

BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID_ENV = os.environ.get("OWNER_ID", "6169237879")
DATABASE_URL = os.environ.get("DATABASE_URL")  # Must include +asyncpg
UPLOAD_CHANNEL_ID_ENV = os.environ.get("UPLOAD_CHANNEL_ID")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")
PORT = int(os.environ.get("PORT", "10000"))
MAX_CONCURRENT_DELIVERIES = int(os.environ.get("MAX_CONCURRENT_DELIVERIES", "50"))
MAX_FILES_PER_SESSION = int(os.environ.get("MAX_FILES_PER_SESSION", "99"))

_missing = []
if not BOT_TOKEN: _missing.append("BOT_TOKEN")
if not OWNER_ID_ENV: _missing.append("OWNER_ID")
if not DATABASE_URL: _missing.append("DATABASE_URL")
if not UPLOAD_CHANNEL_ID_ENV: _missing.append("UPLOAD_CHANNEL_ID")
if not WEBHOOK_HOST: _missing.append("WEBHOOK_HOST")
if _missing:
    raise RuntimeError("Missing required environment variables: " + ", ".join(_missing))

try:
    OWNER_ID = int(OWNER_ID_ENV)
except Exception:
    raise RuntimeError("OWNER_ID must be integer")

try:
    UPLOAD_CHANNEL_ID = int(UPLOAD_CHANNEL_ID_ENV)
except Exception:
    raise RuntimeError("UPLOAD_CHANNEL_ID must be an integer (e.g. -1001234567890)")

WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST.rstrip('/')}{WEBHOOK_PATH}"

logger.info("Configuration: OWNER_ID=%s, UPLOAD_CHANNEL_ID=%s, WEBHOOK_HOST=%s, PORT=%s",
            OWNER_ID, UPLOAD_CHANNEL_ID, WEBHOOK_HOST, PORT)

# ----------------------------
# Database (SQLAlchemy async)
# ----------------------------
Base = declarative_base()


class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column(String(128), unique=True, nullable=False, index=True)
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
    tg_file_id = Column(String, nullable=False)
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
    content = Column(String, nullable=True)     # text caption for start
    photo_file_id = Column(String, nullable=True)  # optional photo file_id for start
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


Index("ix_files_session_order", FileModel.session_id, FileModel.order_index)

# Create engine and sessionmaker
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    logger.info("Initializing database (create tables if needed)...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialized.")


# ----------------------------
# Aiogram (v2) setup
# ----------------------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)  # v2 Dispatcher

# concurrency limit
delivery_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DELIVERIES)

# ----------------------------
# Utilities
# ----------------------------
def generate_token(length: int = 64) -> str:
    return secrets.token_urlsafe(length)[:length]


# DB helper functions
async def create_draft(owner_id: int) -> SessionModel:
    async with AsyncSessionLocal() as db:
        token = generate_token(64)
        rec = SessionModel(link=token, owner_id=owner_id, status="draft")
        db.add(rec)
        await db.commit()
        await db.refresh(rec)
        return rec


async def get_owner_draft(owner_id: int) -> Optional[SessionModel]:
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.owner_id == owner_id, SessionModel.status == "draft")
        res = await db.execute(stmt)
        return res.scalars().first()


async def append_file(session_id: int, tg_file_id: str, file_type: str, caption: str) -> FileModel:
    async with AsyncSessionLocal() as db:
        # compute next index
        res = await db.execute(select(func.max(FileModel.order_index)).where(FileModel.session_id == session_id))
        max_idx = res.scalar()
        next_idx = (max_idx or 0) + 1
        fm = FileModel(session_id=session_id, tg_file_id=tg_file_id, file_type=file_type, caption=caption, order_index=next_idx)
        db.add(fm)
        await db.commit()
        await db.refresh(fm)
        return fm


async def list_files(session_id: int) -> List[FileModel]:
    async with AsyncSessionLocal() as db:
        stmt = select(FileModel).where(FileModel.session_id == session_id).order_by(FileModel.order_index)
        res = await db.execute(stmt)
        return res.scalars().all()


async def set_session_state(session_id: int, state: str) -> Optional[SessionModel]:
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(SessionModel).where(SessionModel.id == session_id).limit(1))
        rec = res.scalars().first()
        if rec:
            rec.status = state
            db.add(rec)
            await db.commit()
            await db.refresh(rec)
            return rec
        return None


async def finalize_session(session_id: int, protect: bool, autodelete: int) -> Optional[SessionModel]:
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(SessionModel).where(SessionModel.id == session_id).limit(1))
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


async def abort_session(session_id: int):
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(SessionModel).where(SessionModel.id == session_id).limit(1))
        rec = res.scalars().first()
        if rec:
            await db.delete(rec)
            await db.commit()
            return True
        return False


async def get_session_by_token(token: str) -> Optional[SessionModel]:
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(SessionModel).where(SessionModel.link == token).limit(1))
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
        res = await db.execute(select(StartMessage).order_by(StartMessage.updated_at.desc()))
        rec = res.scalars().first()
        if rec:
            rec.content = content
            rec.photo_file_id = photo_file_id
            db.add(rec)
        else:
            rec = StartMessage(content=content, photo_file_id=photo_file_id)
            db.add(rec)
        await db.commit()
    logger.info("Saved /start message (text:%s photo:%s)", bool(content), bool(photo_file_id))


async def fetch_start_message() -> Tuple[Optional[str], Optional[str]]:
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(StartMessage).order_by(StartMessage.updated_at.desc()))
        rec = res.scalars().first()
        if rec:
            return rec.content, rec.photo_file_id
        return None, None


# ----------------------------
# Helper: send with protect_content fallback
# ----------------------------
async def send_with_protect(ftype: str, chat_id: int, file_id: str, caption: Optional[str], protect: bool):
    kwargs: Dict[str, Any] = {}
    if caption:
        kwargs["caption"] = caption
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
            kwargs.pop("caption", None)
            return await bot.send_voice(chat_id=chat_id, voice=file_id, **kwargs)
        if ftype == "sticker":
            kwargs.pop("caption", None)
            return await bot.send_sticker(chat_id=chat_id, sticker=file_id, **kwargs)
        return await bot.send_document(chat_id=chat_id, document=file_id, **kwargs)
    except TypeError:
        # method signature doesn't accept protect_content -> retry without it
        kwargs.pop("protect_content", None)
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
        logger.exception("Failed sending media")
        raise


# ----------------------------
# Autodelete worker
# ----------------------------
AUTODELETE_INTERVAL = 30  # seconds


async def autodelete_worker():
    logger.info("Autodelete worker started; checking every %s seconds", AUTODELETE_INTERVAL)
    while True:
        try:
            async with AsyncSessionLocal() as db:
                now = datetime.utcnow()
                res = await db.execute(select(DeliveryModel).where(DeliveryModel.delete_at <= now))
                due = res.scalars().all()
                if due:
                    logger.info("Autodelete: found %d messages to delete", len(due))
                for rec in due:
                    try:
                        await bot.delete_message(chat_id=rec.chat_id, message_id=rec.message_id)
                    except Exception as e:
                        logger.warning("Autodelete failed for %s:%s -> %s", rec.chat_id, rec.message_id, e)
                    try:
                        await db.delete(rec)
                    except Exception:
                        logger.exception("Failed to delete delivery DB record %s", rec.id)
                await db.commit()
        except Exception:
            logger.exception("Autodelete worker exception: %s", traceback.format_exc())
        await asyncio.sleep(AUTODELETE_INTERVAL)


# ----------------------------
# Handlers
# ----------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    args = message.get_args().strip() if message.get_args() else ""
    if not args:
        content, photo = await fetch_start_message()
        if photo:
            # send image with caption if present
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
                    # support {word | url}
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
                        inner = rendered[s+1:e].strip()
                        if "|" in inner:
                            left, right = inner.split("|", 1)
                            out.append(f'<a href="{right.strip()}">{left.strip()}</a>')
                        else:
                            out.append("{" + inner + "}")
                        i = e + 1
                    final_text = "".join(out)
                    await message.answer(final_text, parse_mode="HTML", disable_web_page_preview=True)
                except Exception:
                    await message.answer(content.replace("{first_name}", message.from_user.first_name or ""))
            else:
                await message.answer(f"Welcome, {message.from_user.first_name or 'friend'}!")
        return

    # treat as deep link token
    token = args
    session_row = await get_session_by_token(token)
    if not session_row or session_row.revoked or session_row.status != "published":
        await message.answer("❌ Link not found or not available.")
        return

    files = await list_files(session_row.id)
    if not files:
        await message.answer("❌ No files found for this link.")
        return

    delivered = 0
    for f in files:
        try:
            protect_flag = False if message.from_user.id == OWNER_ID else bool(session_row.protect_content)
            await delivery_semaphore.acquire()
            try:
                sent = await send_with_protect(f.file_type, message.chat.id, f.tg_file_id, f.caption, protect_flag)
            finally:
                delivery_semaphore.release()
            delivered += 1
            if session_row.autodelete_minutes and session_row.autodelete_minutes > 0:
                delete_time = datetime.utcnow() + timedelta(minutes=session_row.autodelete_minutes)
                await schedule_delivery(chat_id=sent.chat.id, message_id=sent.message_id, delete_at=delete_time)
        except Exception:
            logger.exception("Failed to deliver file %s for session %s", f.tg_file_id, token)
    if session_row.autodelete_minutes and session_row.autodelete_minutes > 0:
        await message.answer(f"Files delivered: {delivered}. They will be deleted after {session_row.autodelete_minutes} minute(s).")
    else:
        await message.answer(f"Files delivered: {delivered}. (Autodelete disabled)")


@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /upload.")
        return
    existing = await get_owner_draft(OWNER_ID)
    if existing:
        await message.reply(f"You already have an active draft session. Continue sending files or use /done. Draft token: {existing.link}")
        return
    rec = await create_draft(OWNER_ID)
    await message.reply(f"Upload started. Draft token: {rec.link}\nSend up to {MAX_FILES_PER_SESSION} files (photo/video/document/audio/voice/sticker). When finished send /done. To cancel send /abort.")


# Media handler accepts owner files while draft exists
@dp.message_handler(content_types=["photo", "video", "document", "audio", "voice", "sticker"])
async def handle_owner_media(message: types.Message):
    # ignore non-owner
    if message.from_user.id != OWNER_ID:
        return
    draft = await get_owner_draft(OWNER_ID)
    if not draft:
        # not in an upload session
        return
    # check file count limit
    files = await list_files(draft.id)
    if len(files) >= MAX_FILES_PER_SESSION:
        await message.reply(f"Upload limit reached ({MAX_FILES_PER_SESSION}). Use /done to finalize or /abort to cancel.")
        return

    # determine type & original file id
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
        logger.exception("Error extracting file info from owner message")
        await message.reply("Failed to read the file. Try sending again.")
        return

    # forward message to upload channel (bot must be admin)
    try:
        forwarded = await bot.forward_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
    except Exception as e:
        logger.exception("Failed to forward to upload channel: %s", e)
        await message.reply("Failed to forward to upload channel. Ensure bot is admin there and allowed to post.")
        return

    # extract stable file_id from forwarded object
    channel_file_id = None
    channel_file_type = None
    channel_caption = None
    try:
        if forwarded.photo:
            channel_file_type = "photo"
            channel_file_id = forwarded.photo[-1].file_id
            channel_caption = forwarded.caption or ""
        elif forwarded.video:
            channel_file_type = "video"
            channel_file_id = forwarded.video.file_id
            channel_caption = forwarded.caption or ""
        elif forwarded.document:
            channel_file_type = "document"
            channel_file_id = forwarded.document.file_id
            channel_caption = forwarded.caption or ""
        elif forwarded.audio:
            channel_file_type = "audio"
            channel_file_id = forwarded.audio.file_id
            channel_caption = forwarded.caption or ""
        elif forwarded.voice:
            channel_file_type = "voice"
            channel_file_id = forwarded.voice.file_id
            channel_caption = ""
        elif forwarded.sticker:
            channel_file_type = "sticker"
            channel_file_id = forwarded.sticker.file_id
            channel_caption = ""
        else:
            channel_file_type = ftype
            channel_file_id = orig_file_id
            channel_caption = caption
    except Exception:
        logger.exception("Error extracting file id from forwarded message; falling back to original ids.")
        channel_file_type = ftype
        channel_file_id = orig_file_id
        channel_caption = caption

    # append to DB
    try:
        await append_file(draft.id, channel_file_id, channel_file_type, channel_caption)
        await message.reply(f"Saved {channel_file_type}. ({len(await list_files(draft.id))}/{MAX_FILES_PER_SESSION}) Send more or /done when finished.")
    except Exception:
        logger.exception("Failed to append file to session")
        await message.reply("Failed to save file metadata to DB. Try again or /abort.")


@dp.message_handler(commands=["done"])
async def cmd_done(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /done.")
        return
    draft = await get_owner_draft(OWNER_ID)
    if not draft:
        await message.reply("No active upload session. Use /upload to start.")
        return
    files = await list_files(draft.id)
    if not files:
        await message.reply("No files uploaded. Use /upload then send files.")
        return
    # move to awaiting_protect
    rec = await set_session_state(draft.id, "awaiting_protect")
    if rec:
        await message.reply("Protect content? Reply with `on` or `off`. (Owner bypasses protect.)")
    else:
        await message.reply("Failed to update session state. Try again.")


# handle owner reply "on"/"off" when awaiting_protect
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and (m.text or "").strip().lower() in ("on", "off"))
async def handle_protect_reply(message: types.Message):
    draft = await get_owner_draft(OWNER_ID)
    if not draft:
        return
    if draft.status != "awaiting_protect":
        return
    text = (message.text or "").strip().lower()
    protect = (text == "on")
    # update to awaiting_autodelete and store protect flag
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(SessionModel).where(SessionModel.id == draft.id).limit(1))
        rec = res.scalars().first()
        if not rec:
            await message.reply("Session not found.")
            return
        rec.protect_content = protect
        rec.status = "awaiting_autodelete"
        db.add(rec)
        await db.commit()
    await message.reply(f"Protect set to {'ON' if protect else 'OFF'}. Now reply with autodelete minutes (0 - 10080). 0 = disabled.")


# handle owner reply minutes when awaiting_autodelete
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and (m.text or "").strip().isdigit())
async def handle_autodelete_reply(message: types.Message):
    draft = await get_owner_draft(OWNER_ID)
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
        await message.reply("Please provide an integer between 0 and 10080.")
        return
    rec = await finalize_session(draft.id, draft.protect_content, minutes)
    if not rec:
        await message.reply("Failed to finalize session.")
        return
    me = await bot.get_me()
    bot_username = me.username or "bot"
    deep_link = f"https://t.me/{bot_username}?start={rec.link}"
    await message.reply(f"✅ Session published.\nDeep link: {deep_link}\nProtect: {'ON' if rec.protect_content else 'OFF'}\nAutodelete: {rec.autodelete_minutes} minute(s)\nShare the link.")
    return


@dp.message_handler(commands=["abort"])
async def cmd_abort(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /abort.")
        return
    draft = await get_owner_draft(OWNER_ID)
    if not draft:
        await message.reply("No active upload session.")
        return
    ok = await abort_session(draft.id)
    if ok:
        await message.reply("Upload aborted and draft removed. Forwarded messages in upload channel remain (we don't store message ids).")
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
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(SessionModel).where(SessionModel.link == token).limit(1))
        rec = res.scalars().first()
        if not rec:
            await message.reply("Token not found.")
            return
        rec.revoked = True
        rec.status = "revoked"
        db.add(rec)
        await db.commit()
    await message.reply(f"✅ Token revoked: {token}")


@dp.message_handler(commands=["edit_start"])
async def cmd_edit_start(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /edit_start.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or photo) to set as the /start message. If you reply to a photo, the image + caption will be used.")
        return
    reply = message.reply_to_message
    # If reply has photo: save photo and caption (if any)
    if reply.photo:
        photo_file_id = reply.photo[-1].file_id
        caption = reply.caption or ""
        await save_start_message(caption, photo_file_id)
        await message.reply("✅ /start updated with image and caption (if provided).")
        return
    # If reply has text
    if reply.text:
        await save_start_message(reply.text, None)
        await message.reply("✅ /start updated with text.")
        return
    # If reply has caption (e.g., document with caption)
    if reply.caption:
        await save_start_message(reply.caption, None)
        await message.reply("✅ /start updated with caption text.")
        return
    await message.reply("Unsupported message type. Reply to text or photo.")


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /help.")
        return
    await message.reply(
        "Owner commands:\n"
        "/upload - start upload session\n"
        "/done - finalize upload, then reply to prompts\n"
        "/abort - cancel upload\n"
        "/revoke <token> - revoke a session\n"
        "/edit_start - reply to a message to set /start welcome (image or text)\n"
        "/help - this message\n\n"
        "Universal:\n"
        "/start - show welcome or open deep link (/start <token>)"
    )


# fallback handler (do nothing)
@dp.message_handler()
async def fallback(message: types.Message):
    # intentionally ignore unrelated messages; this prevents handlers from swallowing things
    return


# ----------------------------
# Webhook handler (aiohttp) - bind aiogram context
# ----------------------------
async def webhook_handler(request: Request):
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400, text="Invalid JSON")
    try:
        update = types.Update.de_json(data)
    except Exception:
        try:
            update = types.Update(**data)
        except Exception:
            logger.exception("Failed to parse update JSON")
            return web.Response(status=400, text="Bad update")

    # IMPORTANT: set current bot and dispatcher in context for aiogram v2 webhook usage
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
        logger.exception("Dispatcher failed to process update: %s", traceback.format_exc())
    return web.Response(text="OK")


# Health endpoint
async def health(request: Request):
    return web.Response(text="OK")


# ----------------------------
# Startup: DB init, autodelete worker, aiohttp server, set webhook
# ----------------------------
async def start_services():
    try:
        await init_db()
    except Exception:
        logger.exception("init_db failed")

    # start autodelete worker
    asyncio.create_task(autodelete_worker())
    logger.info("Autodelete worker started.")

    # create aiohttp app
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, webhook_handler)
    app.router.add_get(WEBHOOK_PATH, lambda req: web.Response(text="Webhook endpoint (GET)"))
    app.router.add_get("/health", health)
    app.router.add_get("/", lambda req: web.Response(text="Bot running"))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("aiohttp server started on port %s (webhook path: %s)", PORT, WEBHOOK_PATH)

    # set webhook
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except Exception:
        logger.exception("Failed to set webhook to %s", WEBHOOK_URL)

    # run until cancelled
    try:
        await asyncio.Event().wait()
    finally:
        logger.info("Shutting down: remove webhook & cleanup")
        try:
            await bot.delete_webhook()
        except Exception:
            logger.exception("Deleting webhook failed")
        await runner.cleanup()
        try:
            await bot.close()
        except Exception:
            logger.exception("Failed to close bot session")


def main():
    logger.info("Starting bot main loop")
    try:
        asyncio.run(start_services())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt - exiting")
    except Exception:
        logger.exception("Fatal error in main")
        sys.exit(1)


if __name__ == "__main__":
    main()