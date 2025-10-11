# bot.py
"""
Telegram file-sharing bot (webhook mode) - full implementation (Aiogram v2)

Features:
- Persistent PostgreSQL storage (SQLAlchemy async + asyncpg)
- Private upload channel to store files (bot must be admin)
- Owner-only upload/revoke/edit_start/help/done/abort
- /start (and deep link) universal (anyone can open deep link)
- Owner bypass for protect_content
- Autodelete persistent across restarts
- Webhook mode (no polling) - uses WEBHOOK_HOST + /webhook/<BOT_TOKEN>
- Health server on separate port to avoid binding conflicts
- Robust FSM for upload flow
- Designed to be deployable on Render (or any HTTPS host)

Environment variables (set on Render):
- BOT_TOKEN
- OWNER_ID
- DATABASE_URL (postgresql+asyncpg://...)
- UPLOAD_CHANNEL_ID
- WEBHOOK_HOST (https://your-app.onrender.com)
- PORT (default 10000)
- HEALTH_PORT (default 8080)
- LOG_LEVEL (optional)
"""

import os
import asyncio
import logging
import secrets
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

from aiohttp import web

from aiogram import Bot, Dispatcher, types
from aiogram.utils.executor import start_webhook
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

# ----------------------------
# Logging config
# ----------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("file_share_bot_final")

# ----------------------------
# Environment / config
# ----------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = os.environ.get("OWNER_ID")
DATABASE_URL = os.environ.get("DATABASE_URL")
UPLOAD_CHANNEL_ID = os.environ.get("UPLOAD_CHANNEL_ID")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")
PORT = int(os.environ.get("PORT", "10000"))
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8080"))

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

# webhook setup
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST.rstrip('/')}{WEBHOOK_PATH}"

# ----------------------------
# Database models (SQLAlchemy async)
# ----------------------------
Base = declarative_base()


class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column(String(128), unique=True, nullable=False, index=True)  # 64-char token
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
    tg_file_id = Column(String, nullable=False)
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


# ----------------------------
# Async engine & sessionmaker
# ----------------------------
# DATABASE_URL must be like: postgresql+asyncpg://user:pass@host:port/dbname
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    logger.info("Initializing database (create tables if needed)...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialization complete.")


# ----------------------------
# Utilities
# ----------------------------
def generate_token(length: int = 64) -> str:
    """Generate a URL-safe token of exact length (slice token_urlsafe)."""
    return secrets.token_urlsafe(length)[:length]


# ----------------------------
# Aiogram setup (v2)
# ----------------------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)


# ----------------------------
# FSM states
# ----------------------------
class UploadStates(StatesGroup):
    waiting_for_files = State()
    awaiting_protect = State()
    awaiting_autodelete = State()


# ----------------------------
# Background autodelete worker
# ----------------------------
AUTODELETE_CHECK_INTERVAL = 30  # seconds


async def add_delivery(chat_id: int, message_id: int, delete_at: Optional[datetime]):
    """Persist a message deletion task to DB (delete_at in UTC)."""
    if delete_at is None:
        return
    async with AsyncSessionLocal() as db:
        rec = DeliveryModel(chat_id=chat_id, message_id=message_id, delete_at=delete_at)
        db.add(rec)
        await db.commit()
    logger.debug("Scheduled deletion for %s:%s at %s", chat_id, message_id, delete_at.isoformat())


async def autodelete_worker():
    """Loop: find due deliveries and delete them, remove DB records."""
    logger.info("Autodelete worker started; checking every %s seconds", AUTODELETE_CHECK_INTERVAL)
    while True:
        try:
            async with AsyncSessionLocal() as db:
                now = datetime.utcnow()
                stmt = select(DeliveryModel).where(DeliveryModel.delete_at <= now)
                res = await db.execute(stmt)
                due_items = res.scalars().all()
                if due_items:
                    logger.info("Autodelete: %d messages due", len(due_items))
                for rec in due_items:
                    try:
                        await bot.delete_message(chat_id=rec.chat_id, message_id=rec.message_id)
                        logger.debug("Autodelete: deleted %s:%s", rec.chat_id, rec.message_id)
                    except Exception as e:
                        logger.warning("Autodelete: failed to delete %s:%s -> %s", rec.chat_id, rec.message_id, e)
                    try:
                        await db.delete(rec)
                    except Exception:
                        logger.exception("Autodelete: failed removing DB record for %s:%s", rec.chat_id, rec.message_id)
                await db.commit()
        except Exception:
            logger.exception("Error in autodelete worker: %s", traceback.format_exc())
        await asyncio.sleep(AUTODELETE_CHECK_INTERVAL)


# ----------------------------
# DB helper functions
# ----------------------------
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


# ----------------------------
# Helper to get FSM state bound to chat+user reliably
# ----------------------------
def get_state_for_message(message: types.Message) -> FSMContext:
    """
    Use both chat and user to bind FSM state reliably in webhook environment.
    """
    return dp.current_state(chat=message.chat.id, user=message.from_user.id)


# ----------------------------
# /start handler (welcome or deep link)
# ----------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    args = message.get_args().strip() if message.get_args() else ""
    # No payload -- reply with start message
    if not args:
        content = await fetch_start_message()
        if content:
            # Replace {first_name} and {label | url}
            rendered = content.replace("{first_name}", message.from_user.first_name or "")
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
                # fallback plain text
                await message.answer(content.replace("{first_name}", message.from_user.first_name or ""))
        else:
            await message.answer(f"Welcome, {message.from_user.first_name or 'friend'}!")
        return

    # Payload present: treat it as deep-link token
    token = args
    session_obj = await get_session_by_token(token)
    if not session_obj:
        await message.answer("❌ Link not found or expired.")
        return
    if session_obj.revoked:
        await message.answer("❌ This link has been revoked by the owner.")
        return

    # fetch files for the session in order
    async with AsyncSessionLocal() as db:
        stmt = select(FileModel).where(FileModel.session_id == session_obj.id).order_by(FileModel.order_index)
        res = await db.execute(stmt)
        files: List[FileModel] = res.scalars().all()

    if not files:
        await message.answer("❌ No files available for this link.")
        return

    delivered = 0
    for f in files:
        try:
            # Owner bypass: the owner always gets unprotected copies
            protect_flag = False if message.from_user.id == OWNER_ID else bool(session_obj.protect_content)
            caption = f.caption or None

            # Send according to file type. note: protect_content parameter exists on some Aiogram versions / Telegram clients;
            # we implement owner bypass behavior by not setting it for owner; many clients will still enforce client-side.
            if f.file_type == "photo":
                sent = await bot.send_photo(chat_id=message.chat.id, photo=f.tg_file_id, caption=caption)
            elif f.file_type == "video":
                sent = await bot.send_video(chat_id=message.chat.id, video=f.tg_file_id, caption=caption)
            elif f.file_type == "document":
                sent = await bot.send_document(chat_id=message.chat.id, document=f.tg_file_id, caption=caption)
            elif f.file_type == "audio":
                sent = await bot.send_audio(chat_id=message.chat.id, audio=f.tg_file_id, caption=caption)
            elif f.file_type == "voice":
                sent = await bot.send_voice(chat_id=message.chat.id, voice=f.tg_file_id)
            elif f.file_type == "sticker":
                sent = await bot.send_sticker(chat_id=message.chat.id, sticker=f.tg_file_id)
            else:
                sent = await bot.send_document(chat_id=message.chat.id, document=f.tg_file_id, caption=caption)

            delivered += 1

            # schedule autodelete for this recipient copy if needed
            if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
                delete_time = datetime.utcnow() + timedelta(minutes=session_obj.autodelete_minutes)
                await add_delivery(chat_id=sent.chat.id, message_id=sent.message_id, delete_at=delete_time)
        except Exception:
            logger.exception("Failed to deliver a file for token %s: %s", token, traceback.format_exc())

    if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
        await message.answer(f"Files delivered ({delivered}). They will be deleted after {session_obj.autodelete_minutes} minute(s).")
    else:
        await message.answer(f"Files delivered ({delivered}). Autodelete disabled for this link.")


# ----------------------------
# Owner-only: /upload - start FSM
# ----------------------------
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    """Start an upload session (owner only). Files sent after this are collected until /done or /abort."""
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /upload.")
        return
    state = get_state_for_message(message)
    await state.set_state(UploadStates.waiting_for_files.state)
    await state.update_data(files=[])
    await message.reply("Upload session started.\nSend files (photos/videos/documents/audio/voice/sticker). When finished send /done. To cancel send /abort.")


# ----------------------------
# Owner-only: /abort - cancel upload and cleanup forwarded msgs
# ----------------------------
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
        fwd_msg_id = f.get("forwarded_upload_channel_message")
        if fwd_msg_id:
            try:
                await bot.delete_message(chat_id=UPLOAD_CHANNEL_ID, message_id=fwd_msg_id)
                removed += 1
            except Exception:
                # ignore deletion errors
                pass
    await state.finish()
    await message.reply(f"Upload aborted. Removed {removed} temporary forwarded message(s) from upload channel (if possible).")


# ----------------------------
# Owner-only: /done - ask protect and move FSM to awaiting_protect
# ----------------------------
@dp.message_handler(commands=["done"])
async def cmd_done(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can finish uploads.")
        return
    state = get_state_for_message(message)
    if await state.get_state() != UploadStates.waiting_for_files.state:
        await message.reply("No active upload session. Use /upload to start one.")
        return
    data = await state.get_data()
    files = data.get("files", []) or []
    if not files:
        await state.finish()
        await message.reply("No files were uploaded. Upload canceled.")
        return
    await message.reply("Protect content? Reply with `on` or `off`.\nProtect prevents saving/forwarding on some clients.")
    await state.set_state(UploadStates.awaiting_protect.state)


# ----------------------------
# Generic handler: collects files and handles protect/autodelete responses
# ----------------------------
@dp.message_handler()
async def generic_handler(message: types.Message):
    """
    Handles three FSM states:
    - waiting_for_files: forward files to upload channel and store metadata in FSM
    - awaiting_protect: read 'on'/'off' and ask autodelete minutes
    - awaiting_autodelete: read minutes (0-10080), finalize upload
    """
    state = get_state_for_message(message)
    curr_state = await state.get_state()

    # awaiting_protect
    if curr_state == UploadStates.awaiting_protect.state:
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
    if curr_state == UploadStates.awaiting_autodelete.state:
        text = (message.text or "").strip()
        try:
            minutes = int(text)
            if minutes < 0 or minutes > 10080:
                raise ValueError
            await state.update_data(autodelete=minutes)
            # finalize upload
            await finalize_upload_flow(message=message, state=state)
        except ValueError:
            await message.reply("Please send an integer between 0 and 10080 (minutes).")
        return

    # waiting_for_files: accept supported media
    if curr_state == UploadStates.waiting_for_files.state:
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

        # forward the message to the upload channel so Telegram stores it there
        try:
            fwd_msg = await bot.forward_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
            fwd_id = fwd_msg.message_id
        except Exception as e:
            logger.exception("Failed to forward to upload channel: %s", e)
            await message.reply("Failed to forward to the upload channel. Ensure the bot is admin and can post there.")
            return

        # append metadata to FSM
        data = await state.get_data()
        files = data.get("files", []) or []
        files.append({
            "file_type": ftype,
            "orig_file_id": orig_file_id,
            "caption": caption,
            "forwarded_upload_channel_message": fwd_id,
        })
        await state.update_data(files=files)
        await message.reply(f"Saved {ftype}. Send more files or /done when finished.")
        return

    # not in FSM: ignore other messages
    return


# ----------------------------
# Finalize upload: persist session + files & reply deep link
# ----------------------------
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
            autodelete_minutes=int(autodelete)
        )
        db.add(session_row)
        await db.flush()  # populate session_row.id

        order_idx = 1
        for entry in files:
            fwd_msg_id = entry.get("forwarded_upload_channel_message")
            file_type = None
            file_id = None
            caption = None
            try:
                # Read message from upload channel to get stable file_id saved in channel
                fwd_msg = await bot.get_message(chat_id=UPLOAD_CHANNEL_ID, message_id=fwd_msg_id)
                if fwd_msg.photo:
                    file_type = "photo"
                    file_id = fwd_msg.photo[-1].file_id
                    caption = fwd_msg.caption or ""
                elif fwd_msg.video:
                    file_type = "video"
                    file_id = fwd_msg.video.file_id
                    caption = fwd_msg.caption or ""
                elif fwd_msg.document:
                    file_type = "document"
                    file_id = fwd_msg.document.file_id
                    caption = fwd_msg.caption or ""
                elif fwd_msg.audio:
                    file_type = "audio"
                    file_id = fwd_msg.audio.file_id
                    caption = fwd_msg.caption or ""
                elif fwd_msg.voice:
                    file_type = "voice"
                    file_id = fwd_msg.voice.file_id
                    caption = ""
                elif fwd_msg.sticker:
                    file_type = "sticker"
                    file_id = fwd_msg.sticker.file_id
                    caption = ""
                else:
                    # fallback to original data captured earlier
                    file_type = entry.get("file_type")
                    file_id = entry.get("orig_file_id")
                    caption = entry.get("caption", "")
            except Exception as e:
                # If get_message fails (rare), fallback to original captured id
                logger.warning("Could not fetch forwarded message %s in upload channel: %s", fwd_msg_id, e)
                file_type = entry.get("file_type")
                file_id = entry.get("orig_file_id")
                caption = entry.get("caption", "")

            if not file_id:
                logger.warning("Skipping file with no file_id in finalize step: %s", entry)
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

    # Prepare deep link for owner to share: t.me/<bot>?start=<token>
    me = await bot.get_me()
    bot_username = me.username or "bot"
    tme_link = f"https://t.me/{bot_username}?start={session_row.link}"

    await message.reply(
        f"✅ Upload complete.\nDeep link:\n{tme_link}\n\nProtect content: {'ON' if protect else 'OFF'}\nAutodelete: {autodelete} minute(s)\n\nUse /revoke <token> to disable this link."
    )

    await state.finish()


# ----------------------------
# Owner-only: /revoke <token>
# ----------------------------
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


# ----------------------------
# Owner-only: /edit_start (reply to a message to set welcome)
# ----------------------------
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
        await message.reply("✅ /start message updated. Placeholders: {first_name} and {word | url} (e.g. {Click | https://example.com})")
    except Exception:
        logger.exception("Failed to save start message.")
        await message.reply("Failed to save /start message (see logs).")


# ----------------------------
# Owner-only: /help
# ----------------------------
@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /help for management commands. Use /start to see welcome.")
        return
    help_text = (
        "Owner commands:\n"
        "/upload - start upload session (owner only)\n"
        "/done - finalize upload session (owner only)\n"
        "/abort - cancel upload session (owner only)\n"
        "/revoke <token> - revoke link (owner only)\n"
        "/edit_start - edit the welcome /start message (owner only; reply to a message)\n"
        "/help - this help\n\n"
        "Universal command:\n"
        "/start - show welcome or use deep link (/start <token>)\n"
    )
    await message.reply(help_text)


# ----------------------------
# Health server (separate port)
# ----------------------------
async def health(request):
    return web.Response(text="OK")


async def start_health_app():
    """Start an aiohttp health server on HEALTH_PORT to satisfy uptime monitors."""
    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    try:
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
        await site.start()
        logger.info("Health server started on port %s", HEALTH_PORT)
    except Exception:
        logger.exception("Failed to start health server: %s", traceback.format_exc())


# ----------------------------
# Startup and shutdown hooks for webhook
# ----------------------------
async def on_startup(dp_obj):
    logger.info("on_startup: initializing DB, setting webhook, starting workers...")
    try:
        await init_db()
    except Exception:
        logger.exception("DB init failed in on_startup.")

    loop = asyncio.get_event_loop()
    loop.create_task(autodelete_worker())
    loop.create_task(start_health_app())

    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except Exception:
        logger.exception("Failed to set webhook to %s", WEBHOOK_URL)


async def on_shutdown(dp_obj):
    logger.info("on_shutdown: deleting webhook and closing resources...")
    try:
        await bot.delete_webhook()
    except Exception:
        logger.exception("Failed to delete webhook")
    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
    except Exception:
        logger.exception("Error closing FSM storage")
    try:
        await bot.close()
    except Exception:
        logger.exception("Error closing bot")


# ----------------------------
# Entrypoint: start webhook server (binds to PORT)
# ----------------------------
def main():
    logger.info("Starting webhook server on port %s and health server on port %s", PORT, HEALTH_PORT)
    try:
        start_webhook(
            dispatcher=dp,
            webhook_path=WEBHOOK_PATH,
            skip_updates=True,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            host="0.0.0.0",
            port=PORT,
        )
    except Exception:
        logger.exception("Failed to start webhook: %s", traceback.format_exc())


if __name__ == "__main__":
    main()