# bot.py
"""
Webhook-mode Telegram file-sharing bot (Aiogram 2.25.0)
- Webhook server (aiohttp) to avoid polling conflicts
- Persistent PostgreSQL storage using SQLAlchemy async (1.4.x) + asyncpg
- Owner-only upload/revoke/edit_start
- Private upload channel to store files (bot must be admin)
- Persistent autodelete via DB (deliveries table)
- Owner bypass for protect_content
- Default PORT = 10000 (can be overridden with env var PORT)
"""

import os
import asyncio
import logging
import secrets
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict, Any

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

# -------------------------
# Logging
# -------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("file_share_bot")

# -------------------------
# Configuration (env vars)
# -------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = os.environ.get("OWNER_ID")
DATABASE_URL = os.environ.get("DATABASE_URL")  # must include +asyncpg: postgresql+asyncpg://user:pass@host:port/db
UPLOAD_CHANNEL_ID = os.environ.get("UPLOAD_CHANNEL_ID")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # e.g. https://hidden-fnxx.onrender.com (required)
port = int(os.getenv("PORT", 10000))
health_port = int(os.getenv("HEALTH_PORT", 8080))

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

# convert types
try:
    OWNER_ID = int(OWNER_ID)
except Exception:
    raise RuntimeError("OWNER_ID must be an integer")
try:
    UPLOAD_CHANNEL_ID = int(UPLOAD_CHANNEL_ID)
except Exception:
    raise RuntimeError("UPLOAD_CHANNEL_ID must be an integer (channel id), e.g. -1001234567890")

# webhook path
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"  # Telegram will call this URL

# -------------------------
# SQLAlchemy async models
# -------------------------
Base = declarative_base()


class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column(String(128), unique=True, nullable=False, index=True)
    owner_id = Column(Integer, nullable=False)
    protect_content = Column(Boolean, default=False)
    autodelete_minutes = Column(Integer, default=0)  # 0 = disabled
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
    content = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# -------------------------
# Async engine & sessionmaker
# -------------------------
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    logger.info("Initializing database (create tables if needed)...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialized.")


# -------------------------
# Token helper
# -------------------------
def generate_token(length: int = 64) -> str:
    return secrets.token_urlsafe(length)[:length]


# -------------------------
# Aiogram setup (v2)
# -------------------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)


# -------------------------
# FSM states
# -------------------------
class UploadStates(StatesGroup):
    waiting_for_files = State()
    awaiting_protect = State()
    awaiting_autodelete = State()


# -------------------------
# Autodelete worker
# -------------------------
AUTODELETE_CHECK_INTERVAL = 30  # seconds


async def add_delivery(chat_id: int, message_id: int, delete_at: Optional[datetime]):
    if delete_at is None:
        return
    async with AsyncSessionLocal() as db:
        rec = DeliveryModel(chat_id=chat_id, message_id=message_id, delete_at=delete_at)
        db.add(rec)
        await db.commit()
    logger.debug("Added delivery: %s:%s at %s", chat_id, message_id, delete_at.isoformat())


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
                    logger.info("Autodelete: found %d messages to delete", len(due))
                for rec in due:
                    try:
                        await bot.delete_message(chat_id=rec.chat_id, message_id=rec.message_id)
                        logger.debug("Autodelete: deleted message %s:%s", rec.chat_id, rec.message_id)
                    except Exception as e:
                        logger.warning("Autodelete: failed to delete message %s:%s -> %s", rec.chat_id, rec.message_id, e)
                    try:
                        await db.delete(rec)
                    except Exception:
                        logger.exception("Autodelete: failed to delete DB record for %s:%s", rec.chat_id, rec.message_id)
                await db.commit()
        except Exception:
            logger.exception("Error in autodelete worker: %s", traceback.format_exc())
        await asyncio.sleep(AUTODELETE_CHECK_INTERVAL)


# -------------------------
# DB helpers
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
    logger.info("Saved start message.")


async def fetch_start_message() -> Optional[str]:
    async with AsyncSessionLocal() as db:
        stmt = select(StartMessage).order_by(StartMessage.updated_at.desc())
        res = await db.execute(stmt)
        rec = res.scalars().first()
        return rec.content if rec else None


# -------------------------
# /start handler (deep link processing)
# -------------------------
@dp.message_handler(commands=["start"])
async def on_start(message: types.Message):
    args = message.get_args().strip() if message.get_args() else ""
    if not args:
        content = await fetch_start_message()
        if content:
            # Replace {first_name}
            rendered = content.replace("{first_name}", message.from_user.first_name or "")
            # Convert {label | url} to HTML anchor
            out = []
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
                    left = left.strip()
                    right = right.strip()
                    out.append(f'<a href="{right}">{left}</a>')
                else:
                    out.append("{" + inner + "}")
                i = end + 1
            final_text = "".join(out)
            try:
                await message.answer(final_text, parse_mode=None)
            except Exception:
                await message.answer(content.replace("{first_name}", message.from_user.first_name or ""))
        else:
            await message.answer(f"Welcome, {message.from_user.first_name}!")
        return

    # payload token - deliver files
    token = args
    session_obj = await get_session_by_token(token)
    if not session_obj:
        await message.answer("❌ Link not found or expired.")
        return
    if session_obj.revoked:
        await message.answer("❌ This link has been revoked by the owner.")
        return

    # fetch files
    async with AsyncSessionLocal() as db:
        stmt = select(FileModel).where(FileModel.session_id == session_obj.id).order_by(FileModel.order_index)
        res = await db.execute(stmt)
        files = res.scalars().all()

    if not files:
        await message.answer("❌ No files found for this link.")
        return

    delivered = 0
    for f in files:
        try:
            # owner bypass: owner always gets unprotected copies
            protect = False if message.from_user.id == OWNER_ID else bool(session_obj.protect_content)
            caption = f.caption or None

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

            # schedule autodelete per recipient
            if session_obj.autodelete_minutes and session_obj.autodelete_minutes > 0:
                delete_time = datetime.utcnow() + timedelta(minutes=session_obj.autodelete_minutes)
                await add_delivery(chat_id=sent.chat.id, message_id=sent.message_id, delete_at=delete_time)
        except Exception:
            logger.exception("Failed to deliver file for token %s: %s", token, traceback.format_exc())

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
    await UploadStates.waiting_for_files.set()
    state = dp.current_state(user=message.from_user.id)
    await state.update_data(files=[])
    await message.reply("Upload started. Send files now. When finished, send /done. To cancel, send /abort.")


# -------------------------
# Owner-only: /abort
# -------------------------
@dp.message_handler(commands=["abort"])
async def cmd_abort(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /abort.")
        return
    state = dp.current_state(user=message.from_user.id)
    data = await state.get_data()
    files = data.get("files", [])
    removed = 0
    for f in files:
        fwd_msg_id = f.get("forwarded_upload_channel_message")
        if fwd_msg_id:
            try:
                await bot.delete_message(chat_id=UPLOAD_CHANNEL_ID, message_id=fwd_msg_id)
                removed += 1
            except Exception:
                pass
    await state.finish()
    await message.reply(f"Upload aborted. Removed {removed} forwarded messages from upload channel (if possible).")


# -------------------------
# Owner-only: /done
# -------------------------
@dp.message_handler(commands=["done"])
async def cmd_done(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use /done.")
        return
    state = dp.current_state(user=message.from_user.id)
    if await state.get_state() != UploadStates.waiting_for_files.state:
        await message.reply("No active upload session. Use /upload first.")
        return
    data = await state.get_data()
    files = data.get("files", [])
    if not files:
        await state.finish()
        await message.reply("No files uploaded. Upload canceled.")
        return
    await message.reply("Protect content? Reply with `on` or `off`.")
    await UploadStates.awaiting_protect.set()


# -------------------------
# Generic FSM handler (files + protect + autodelete)
# -------------------------
@dp.message_handler()
async def generic_handler(message: types.Message):
    state = dp.current_state(user=message.from_user.id)
    curr = await state.get_state()

    # awaiting_protect
    if curr == UploadStates.awaiting_protect.state:
        text = (message.text or "").strip().lower()
        if text not in ("on", "off"):
            await message.reply("Reply with `on` or `off`.")
            return
        protect = text == "on"
        await state.update_data(protect=protect)
        await message.reply("Set autodelete minutes (0 - 10080). 0 = no autodelete.")
        await UploadStates.awaiting_autodelete.set()
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
            await message.reply("Send an integer between 0 and 10080.")
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

        try:
            fwd = await bot.forward_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
        except Exception as e:
            logger.exception("Failed to forward to upload channel: %s", e)
            await message.reply("Failed to forward to upload channel. Ensure bot is admin in that channel.")
            return

        data = await state.get_data()
        files = data.get("files", [])
        files.append({
            "file_type": ftype,
            "orig_file_id": orig_file_id,
            "caption": caption,
            "forwarded_upload_channel_message": fwd.message_id,
        })
        await state.update_data(files=files)
        await message.reply(f"Saved {ftype}. Send more or /done.")
        return

    # not in upload FSM: ignore other plain messages
    return


# -------------------------
# Finalize upload: persist session + files
# -------------------------
async def finalize_upload_flow(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data.get("files", [])
    protect = data.get("protect", False)
    autodelete = data.get("autodelete", 0)

    if not files:
        await state.finish()
        await message.reply("No files to finalize.")
        return

    async with AsyncSessionLocal() as db:
        token = generate_token(64)
        session_rec = SessionModel(
            link=token,
            owner_id=OWNER_ID,
            protect_content=bool(protect),
            autodelete_minutes=int(autodelete),
        )
        db.add(session_rec)
        await db.flush()

        order_idx = 1
        for entry in files:
            fwd_msg_id = entry.get("forwarded_upload_channel_message")
            try:
                fwd_msg = await bot.get_message(chat_id=UPLOAD_CHANNEL_ID, message_id=fwd_msg_id)
                if fwd_msg.photo:
                    ftype = "photo"
                    file_id = fwd_msg.photo[-1].file_id
                    caption = fwd_msg.caption or ""
                elif fwd_msg.video:
                    ftype = "video"
                    file_id = fwd_msg.video.file_id
                    caption = fwd_msg.caption or ""
                elif fwd_msg.document:
                    ftype = "document"
                    file_id = fwd_msg.document.file_id
                    caption = fwd_msg.caption or ""
                elif fwd_msg.audio:
                    ftype = "audio"
                    file_id = fwd_msg.audio.file_id
                    caption = fwd_msg.caption or ""
                elif fwd_msg.voice:
                    ftype = "voice"
                    file_id = fwd_msg.voice.file_id
                    caption = ""
                elif fwd_msg.sticker:
                    ftype = "sticker"
                    file_id = fwd_msg.sticker.file_id
                    caption = ""
                else:
                    ftype = entry.get("file_type")
                    file_id = entry.get("orig_file_id")
                    caption = entry.get("caption", "")
            except Exception as e:
                logger.warning("Could not fetch forwarded message %s: %s", fwd_msg_id, e)
                ftype = entry.get("file_type")
                file_id = entry.get("orig_file_id")
                caption = entry.get("caption", "")

            if not file_id:
                logger.warning("Skipping file without file_id: %s", entry)
                continue

            fm = FileModel(
                session_id=session_rec.id,
                tg_file_id=file_id,
                file_type=ftype,
                caption=caption,
                order_index=order_idx,
            )
            db.add(fm)
            order_idx += 1

        await db.commit()

    me = await bot.get_me()
    bot_username = me.username or "bot"
    deep_link = f"https://{WEBHOOK_HOST.lstrip('https://').lstrip('http://')}/start?token={session_rec.link}"
    # Note: Telegram deep link format is t.me/<username>?start=<token>, but for simplicity we give both:
    tme_link = f"https://t.me/{bot_username}?start={session_rec.link}"
    await message.reply(
        f"✅ Upload complete.\nDeep link (t.me):\n{tme_link}\n\nProtect: {'ON' if protect else 'OFF'}\nAutodelete: {autodelete} minute(s)\n\nUse /revoke <token> to disable."
    )
    await state.finish()


# -------------------------
# Owner-only: /revoke
# -------------------------
@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can revoke tokens.")
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
# Owner-only: /edit_start
# -------------------------
@dp.message_handler(commands=["edit_start"])
async def cmd_edit_start(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can edit start message.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message with the text or caption you want to use for /start.")
        return
    content = message.reply_to_message.text or message.reply_to_message.caption or ""
    if not content.strip():
        await message.reply("The replied message has no text/caption.")
        return
    await save_start_message(content)
    await message.reply("✅ /start message updated. Placeholders: {first_name} and {word | url}.")


# -------------------------
# /help
# -------------------------
@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    await message.reply(
        "/start - show welcome or use deep link (/start <token>)\n"
        "/upload - start upload (owner only)\n"
        "/done - finish upload (owner only)\n"
        "/abort - cancel upload (owner only)\n"
        "/revoke <token> - revoke link (owner only)\n"
        "/edit_start - edit welcome message (owner only; reply to a message)\n"
        "/help - this help"
    )


# -------------------------
# Health endpoint for readiness (aiohttp app will be started by start_webhook)
# -------------------------
async def health(request):
    return web.Response(text="OK")


# -------------------------
# Startup / Shutdown hooks for webhook
# -------------------------
async def on_startup(dp_obj):
    logger.info("on_startup: initializing DB and starting background tasks...")
    try:
        await init_db()
    except Exception:
        logger.exception("DB init failed at startup")

    # start autodelete worker
    loop = asyncio.get_event_loop()
    loop.create_task(autodelete_worker())

    # set webhook
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set to %s", WEBHOOK_URL)
    except Exception:
        logger.exception("Failed to set webhook to %s", WEBHOOK_URL)


async def on_shutdown(dp_obj):
    logger.info("on_shutdown: removing webhook and closing bot session...")
    try:
        await bot.delete_webhook()
    except Exception:
        logger.exception("Failed to delete webhook")
    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
    except Exception:
        logger.exception("Error closing storage")
    try:
        await bot.close()
    except Exception:
        logger.exception("Error closing bot")


# -------------------------
# Provide an aiohttp app for health check for Render (start_webhook will attach its own webserver)
# -------------------------
# aiogram's start_webhook will mount its own aiohttp server; we add a route by using on_startup task to create one if needed.
# But start_webhook doesn't expose the app easily, so we rely on webhook server path and the Render health check hitting /health.
# We'll create a simple aiohttp app separately as a background task so /health responds.
async def start_health_app():
    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Health server started on port %s", PORT)


# -------------------------
# Entrypoint: run webhook
# -------------------------
def main():
    # start auxiliary health app
    loop = asyncio.get_event_loop()
    loop.create_task(start_health_app())

    # start aiogram webhook
    # start_webhook parameters:
    #   dispatcher, webhook_path, skip_updates, on_startup, on_shutdown, host, port
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