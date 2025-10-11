# bot.py
"""
Fixed webhook-mode Telegram file-sharing bot (Aiogram v2.25.0)
- Uses a single aiohttp server to receive Telegram updates at /webhook/<BOT_TOKEN>
- Provides /health endpoint on same server
- Persistent PostgreSQL via SQLAlchemy async + asyncpg
- Owner-only: /upload, /done, /abort, /revoke, /help, /edit_start
- Public: /start (and deep-link tokens)
- Owner bypass for protect_content
- Autodelete persisted in DB and processed by background worker
- Captures channel file_ids at forward time (no get_message later)
- Delivery concurrency limit to reduce hitting Telegram limits
- Robust error handling and logging
"""

import os
import asyncio
import logging
import secrets
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

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

# -------------------------
# Basic configuration from ENV
# -------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("fixed_file_share_bot")

# Read required env vars
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = os.environ.get("OWNER_ID")
DATABASE_URL = os.environ.get("DATABASE_URL")
UPLOAD_CHANNEL_ID = os.environ.get("UPLOAD_CHANNEL_ID")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # e.g. https://hidden-fnxx.onrender.com
PORT = int(os.environ.get("PORT", "10000"))
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8080"))  # not used separately in this deployment but kept for compat
MAX_CONCURRENT_DELIVERIES = int(os.environ.get("MAX_CONCURRENT_DELIVERIES", "50"))

_missing = []
if not BOT_TOKEN:
    _missing.append("BOT_TOKEN")
if not OWNER_ID:
    _missing.append("OWNER_ID")
if not DATABASE_URL:
    _missing.append("DATABASE_URL")
if not UPLOAD_CHANNEL_ID:
    _missing.append("UPLOAD_CHANNEL_ID")
if not WEBHOOK_HOST:
    _missing.append("WEBHOOK_HOST")
if _missing:
    raise RuntimeError("Missing required env vars: " + ", ".join(_missing))

try:
    OWNER_ID = int(OWNER_ID)
except Exception:
    raise RuntimeError("OWNER_ID must be an integer")

try:
    UPLOAD_CHANNEL_ID = int(UPLOAD_CHANNEL_ID)
except Exception:
    raise RuntimeError("UPLOAD_CHANNEL_ID must be an integer (channel id)")

# Build webhook URL and path
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST.rstrip('/')}{WEBHOOK_PATH}"

# -------------------------
# Database models (SQLAlchemy Async)
# -------------------------
Base = declarative_base()


class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column(String(128), unique=True, nullable=False, index=True)  # token
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
    tg_file_id = Column(String, nullable=False)  # file_id that's safe to re-send
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


# -------------------------
# Async engine + sessionmaker
# -------------------------
# DATABASE_URL must use asyncpg driver: postgresql+asyncpg://user:pass@host:port/db
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    logger.info("Initializing DB (create tables if needed)...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("DB initialized.")


# -------------------------
# Utilities
# -------------------------
def generate_token(length: int = 64) -> str:
    return secrets.token_urlsafe(length)[:length]


# -------------------------
# Aiogram setup
# -------------------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)

# Semaphore for limiting concurrent send operations (avoid hammering Telegram)
delivery_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DELIVERIES)


# -------------------------
# FSM states
# -------------------------
class UploadStates(StatesGroup):
    waiting_for_files = State()
    awaiting_protect = State()
    awaiting_autodelete = State()


# -------------------------
# Autodelete worker (DB persisted)
# -------------------------
AUTODELETE_CHECK_INTERVAL = 30  # seconds


async def add_delivery(chat_id: int, message_id: int, delete_at: Optional[datetime]):
    if delete_at is None:
        return
    async with AsyncSessionLocal() as db:
        rec = DeliveryModel(chat_id=chat_id, message_id=message_id, delete_at=delete_at)
        db.add(rec)
        await db.commit()
    logger.debug("Added delivery task: %s:%s at %s", chat_id, message_id, delete_at.isoformat())


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
                        logger.exception("Autodelete: failed to remove DB record for %s:%s", rec.chat_id, rec.message_id)
                await db.commit()
        except Exception:
            logger.exception("Error in autodelete worker: %s", traceback.format_exc())
        await asyncio.sleep(AUTODELETE_CHECK_INTERVAL)


# -------------------------
# DB helpers (sessions/start message)
# -------------------------
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


# -------------------------
# Helper: reliably get FSM state for message (chat+user)
# -------------------------
def get_state_for_message(message: types.Message) -> FSMContext:
    return dp.current_state(chat=message.chat.id, user=message.from_user.id)


# -------------------------
# Helper: unified send that tries protect_content if supported
# -------------------------
async def _send_with_fallback(ftype: str, chat_id: int, tg_file_id: str, caption: Optional[str], protect: bool):
    """
    Send media while trying to pass protect_content. If the aiogram send_* method doesn't accept
    the parameter, fall back to calling without it.
    Returns the sent Message.
    """
    kwargs: Dict[str, Any] = {}
    if caption:
        kwargs["caption"] = caption

    # try with protect_content first (newer Telegram clients & aiogram versions support it)
    if protect is not None:
        kwargs["protect_content"] = protect

    try:
        if ftype == "photo":
            return await bot.send_photo(chat_id=chat_id, photo=tg_file_id, **kwargs)
        elif ftype == "video":
            return await bot.send_video(chat_id=chat_id, video=tg_file_id, **kwargs)
        elif ftype == "document":
            return await bot.send_document(chat_id=chat_id, document=tg_file_id, **kwargs)
        elif ftype == "audio":
            return await bot.send_audio(chat_id=chat_id, audio=tg_file_id, **kwargs)
        elif ftype == "voice":
            # voice rarely has caption
            return await bot.send_voice(chat_id=chat_id, voice=tg_file_id, **kwargs)
        elif ftype == "sticker":
            # sticker doesn't accept caption
            # remove caption and protect if not supported
            kwargs.pop("caption", None)
            return await bot.send_sticker(chat_id=chat_id, sticker=tg_file_id, **kwargs)
        else:
            return await bot.send_document(chat_id=chat_id, document=tg_file_id, **kwargs)
    except TypeError as e:
        # method signature may not accept protect_content; retry without it
        logger.debug("TypeError during send (retry without protect_content): %s", e)
        kwargs.pop("protect_content", None)
        try:
            if ftype == "photo":
                return await bot.send_photo(chat_id=chat_id, photo=tg_file_id, **kwargs)
            elif ftype == "video":
                return await bot.send_video(chat_id=chat_id, video=tg_file_id, **kwargs)
            elif ftype == "document":
                return await bot.send_document(chat_id=chat_id, document=tg_file_id, **kwargs)
            elif ftype == "audio":
                return await bot.send_audio(chat_id=chat_id, audio=tg_file_id, **kwargs)
            elif ftype == "voice":
                return await bot.send_voice(chat_id=chat_id, voice=tg_file_id, **kwargs)
            elif ftype == "sticker":
                kwargs.pop("caption", None)
                return await bot.send_sticker(chat_id=chat_id, sticker=tg_file_id, **kwargs)
            else:
                return await bot.send_document(chat_id=chat_id, document=tg_file_id, **kwargs)
        except Exception:
            logger.exception("Failed to resend media after removing protect_content fallback.")
            raise
    except Exception:
        logger.exception("Failed to send media.")
        raise


# -------------------------
# Handler: /start (welcome or deep link)
# -------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    args = message.get_args().strip() if message.get_args() else ""
    if not args:
        content = await fetch_start_message()
        if content:
            # Render placeholders and simple {label | url} -> anchor
            rendered = content.replace("{first_name}", message.from_user.first_name or "")
            out_chunks: List[str] = []
            i = 0
            while True:
                s = rendered.find("{", i)
                if s == -1:
                    out_chunks.append(rendered[i:])
                    break
                e = rendered.find("}", s)
                if e == -1:
                    out_chunks.append(rendered[i:])
                    break
                out_chunks.append(rendered[i:s])
                inner = rendered[s+1:e].strip()
                if "|" in inner:
                    left, right = inner.split("|", 1)
                    out_chunks.append(f'<a href="{right.strip()}">{left.strip()}</a>')
                else:
                    out_chunks.append("{" + inner + "}")
                i = e + 1
            final_text = "".join(out_chunks)
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
        await message.answer("❌ This link was revoked.")
        return

    # load files
    async with AsyncSessionLocal() as db:
        stmt = select(FileModel).where(FileModel.session_id == session_obj.id).order_by(FileModel.order_index)
        res = await db.execute(stmt)
        files: List[FileModel] = res.scalars().all()

    if not files:
        await message.answer("❌ No files found for this link.")
        return

    delivered = 0
    # deliver files in order; we acquire semaphore per individual send to limit concurrency overall
    for f in files:
        try:
            protect_flag = False if message.from_user.id == OWNER_ID else bool(session_obj.protect_content)
            # guard with semaphore
            await delivery_semaphore.acquire()
            try:
                sent = await _send_with_fallback(f.file_type, chat_id=message.chat.id, tg_file_id=f.tg_file_id, caption=f.caption, protect=protect_flag)
            finally:
                delivery_semaphore.release()

            delivered += 1
            if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
                delete_time = datetime.utcnow() + timedelta(minutes=session_obj.autodelete_minutes)
                await add_delivery(chat_id=sent.chat.id, message_id=sent.message_id, delete_at=delete_time)
        except Exception:
            logger.exception("Failed to deliver file id %s for token %s", f.tg_file_id if f else "<none>", token)

    if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
        await message.answer(f"Files delivered: {delivered}. They will be deleted after {session_obj.autodelete_minutes} minute(s).")
    else:
        await message.answer(f"Files delivered: {delivered}. (Autodelete disabled)")


# -------------------------
# Owner-only: /upload
# -------------------------
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /upload.")
        return
    state = get_state_for_message(message)
    await state.set_state(UploadStates.waiting_for_files.state)
    await state.update_data(files=[])
    await message.reply("Upload session started.\nSend files now (photos/videos/documents/audio/voice/sticker). When finished send /done. To cancel send /abort.")


# -------------------------
# Owner-only: /abort
# -------------------------
@dp.message_handler(commands=["abort"])
async def cmd_abort(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can cancel uploads.")
        return
    state = get_state_for_message(message)
    data = await state.get_data()
    files = data.get("files", []) or []
    removed = 0
    for f in files:
        ch_fwd_id = f.get("channel_message_id")
        if ch_fwd_id:
            try:
                await bot.delete_message(chat_id=UPLOAD_CHANNEL_ID, message_id=ch_fwd_id)
                removed += 1
            except Exception:
                # ignore deletion errors
                pass
    await state.finish()
    await message.reply(f"Upload aborted. Removed {removed} forwarded message(s) from upload channel (if possible).")


# -------------------------
# Owner-only: /done
# -------------------------
@dp.message_handler(commands=["done"])
async def cmd_done(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can finish uploads.")
        return
    state = get_state_for_message(message)
    if await state.get_state() != UploadStates.waiting_for_files.state:
        await message.reply("No active upload session. Start one with /upload.")
        return
    data = await state.get_data()
    files = data.get("files", []) or []
    if not files:
        await state.finish()
        await message.reply("No files uploaded. Upload canceled.")
        return
    await message.reply("Protect content? Reply with `on` or `off`.")
    await state.set_state(UploadStates.awaiting_protect.state)


# -------------------------
# Generic handler: accepts files during upload FSM and accepts protect/autodelete replies
# -------------------------
@dp.message_handler()
async def generic_handler(message: types.Message):
    state = get_state_for_message(message)
    curr = await state.get_state()

    # awaiting_protect
    if curr == UploadStates.awaiting_protect.state:
        text = (message.text or "").strip().lower()
        if text not in ("on", "off"):
            await message.reply("Please reply with `on` or `off`.")
            return
        protect = text == "on"
        await state.update_data(protect=protect)
        await message.reply("Set autodelete timer in minutes (0 - 10080). 0 = no autodelete. Example: `60`")
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

        # Forward to upload channel and capture channel-side file id immediately (reliable)
        try:
            fwd_msg = await bot.forward_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
        except Exception as e:
            logger.exception("Forward to upload channel failed: %s", e)
            await message.reply("Failed to forward to the upload channel. Ensure bot is admin and can post there.")
            return

        # Extract file_id from the forwarded message object (channel-side stable file id)
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
                # fallback to original metadata captured earlier
                channel_file_type = ftype
                channel_file_id = orig_file_id
                channel_caption = caption
        except Exception:
            logger.exception("Error extracting file id from forwarded message; falling back to captured ids.")
            channel_file_type = ftype
            channel_file_id = orig_file_id
            channel_caption = caption

        # Save in FSM state list
        data = await state.get_data()
        files = data.get("files", []) or []
        files.append({
            "file_type": channel_file_type,
            "channel_file_id": channel_file_id,
            "caption": channel_caption,
            "channel_message_id": fwd_msg.message_id,
        })
        await state.update_data(files=files)
        await message.reply(f"Saved {channel_file_type}. Send more or /done to finish.")
        return

    # not part of FSM, ignore other messages
    return


# -------------------------
# Finalize upload: persist session + files into DB and return deep link
# -------------------------
async def finalize_upload_flow(message: types.Message, state: FSMContext):
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
        await db.flush()  # ensure session_row.id is available

        order_idx = 1
        for entry in files:
            file_type = entry.get("file_type")
            file_id = entry.get("channel_file_id")
            caption = entry.get("caption", "")
            if not file_id:
                logger.warning("Skipping file without id during finalize: %s", entry)
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

    me = await bot.get_me()
    bot_username = me.username or "bot"
    tme_link = f"https://t.me/{bot_username}?start={session_row.link}"
    await message.reply(
        f"✅ Upload complete.\nDeep link:\n{tme_link}\n\nProtect content: {'ON' if protect else 'OFF'}\nAutodelete: {autodelete} minute(s)\n\nUse /revoke <token> to disable this link."
    )
    await state.finish()


# -------------------------
# Owner-only: /revoke <token>
# -------------------------
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


# -------------------------
# Owner-only: /edit_start (reply to message)
# -------------------------
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
    try:
        await save_start_message(content)
        await message.reply("✅ /start message updated. Placeholders: {first_name} and {word | url}")
    except Exception:
        logger.exception("Failed to save /start message.")
        await message.reply("Failed to save /start message; check logs.")


# -------------------------
# Owner-only: /help
# -------------------------
@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /help for management commands. Use /start for welcome.")
        return
    await message.reply(
        "/upload - start upload (owner only)\n"
        "/done - finish upload (owner only)\n"
        "/abort - cancel upload (owner only)\n"
        "/revoke <token> - revoke link (owner only)\n"
        "/edit_start - set /start welcome (owner only; reply to message)\n"
        "/help - this help\n\n"
        "Universal:\n"
        "/start - show welcome or open deep link (/start <token>)"
    )


# -------------------------
# Health endpoint
# -------------------------
async def health(request: Request):
    return web.Response(text="OK")


# -------------------------
# Webhook handler - receives Telegram updates as POSTs
# -------------------------
async def handle_webhook(request: Request):
    """
    Receive POSTs from Telegram webhook and pass to aiogram Dispatcher for processing.
    """
    try:
        data = await request.json()
    except Exception as e:
        logger.warning("Webhook got non-json request: %s", e)
        return web.Response(status=400, text="Invalid JSON")

    # quick debug log for the update (can be noisy)
    logger.debug("Received update via webhook: %s", data.get("update_id", "<no id>"))

    try:
        update = types.Update.de_json(data)
    except Exception:
        # fallback to building Update object manually
        try:
            update = types.Update(**data)
        except Exception:
            logger.exception("Failed to parse update JSON into types.Update")
            return web.Response(status=400, text="Bad update")

    try:
        await dp.process_update(update)
    except Exception:
        logger.exception("Dispatcher failed to process update: %s", traceback.format_exc())
        # still return 200 so Telegram doesn't retry too quickly
    return web.Response(text="OK")


# -------------------------
# Application startup/shutdown logic
# -------------------------
async def start_app():
    """
    Creates aiohttp application, sets webhook URL with Telegram, starts DB + background workers,
    and then waits forever (until cancellation).
    """
    # init DB
    try:
        await init_db()
    except Exception:
        logger.exception("DB initialization failed on startup")

    # start autodelete worker
    asyncio.create_task(autodelete_worker())

    # prepare aiohttp app with webhook and health routes
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    # some clients may send HEAD/GET to the path; handle them gracefully
    app.router.add_get(WEBHOOK_PATH, lambda request: web.Response(text="Webhook endpoint (GET)"))
    app.router.add_get("/health", health)
    app.router.add_head("/", lambda request: web.Response(status=200, text="OK"))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("aiohttp server started on port %s", PORT)

    # set webhook with Telegram
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except Exception:
        logger.exception("Failed to set webhook to %s", WEBHOOK_URL)

    # keep running until cancelled
    try:
        await asyncio.Event().wait()
    finally:
        logger.info("Shutdown initiated: deleting webhook and cleaning up server")
        try:
            await bot.delete_webhook()
        except Exception:
            logger.exception("Failed to delete webhook during shutdown")
        await runner.cleanup()
        try:
            await bot.close()
        except Exception:
            logger.exception("Failed to close bot session")


# -------------------------
# Entrypoint
# -------------------------
def main():
    logger.info("Starting bot - webhook host=%s port=%s", WEBHOOK_HOST, PORT)
    try:
        asyncio.run(start_app())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down (keyboard/system exit)")
    except Exception:
        logger.exception("Unhandled exception in main")


if __name__ == "__main__":
    main()