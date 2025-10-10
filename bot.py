# bot.py
import os
import asyncio
import logging
import secrets
from datetime import datetime, timedelta
from typing import Optional, List, Dict

from aiohttp import web

from aiogram import Bot, Dispatcher, types
from aiogram.types import Message
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage

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

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Configuration - environment variables
# ---------------------------------------------------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")  # postgres://user:pass@host:port/db
UPLOAD_CHANNEL_ID = os.environ.get("UPLOAD_CHANNEL_ID")  # private channel id (negative)
OWNER_ID = os.environ.get("OWNER_ID")  # single owner Telegram ID
PORT = int(os.environ.get("PORT", "8080"))

if not BOT_TOKEN or not DATABASE_URL or not UPLOAD_CHANNEL_ID or not OWNER_ID:
    raise RuntimeError("Missing required env vars: BOT_TOKEN, DATABASE_URL, UPLOAD_CHANNEL_ID, OWNER_ID")

OWNER_ID = int(OWNER_ID)
UPLOAD_CHANNEL_ID = int(UPLOAD_CHANNEL_ID)

# ---------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------
Base = declarative_base()

class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True)
    link = Column(String(128), unique=True, nullable=False, index=True)
    owner_id = Column(Integer, nullable=False)
    protect_content = Column(Boolean, default=False)
    autodelete_minutes = Column(Integer, default=0)
    revoked = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    files = relationship("FileModel", back_populates="session", cascade="all, delete-orphan")

class FileModel(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False)
    tg_file_id = Column(String, nullable=False)
    file_type = Column(String(32), nullable=False)
    caption = Column(String, nullable=True)
    order_index = Column(Integer, nullable=False)
    session = relationship("SessionModel", back_populates="files")

class DeliveryModel(Base):
    __tablename__ = "deliveries"
    id = Column(Integer, primary_key=True)
    chat_id = Column(Integer, nullable=False)
    message_id = Column(Integer, nullable=False)
    delete_at = Column(DateTime(timezone=True), nullable=True)

class StartMessage(Base):
    __tablename__ = "start_message"
    id = Column(Integer, primary_key=True)
    content = Column(String, nullable=True)  # HTML content
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# ---------------------------------------------------------------------
# FSM States
# ---------------------------------------------------------------------
class UploadStates(StatesGroup):
    waiting_for_files = State()
    awaiting_protect = State()
    awaiting_autodelete = State()

# ---------------------------------------------------------------------
# Bot & Dispatcher
# ---------------------------------------------------------------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(storage=storage)

# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------
def generate_token(length: int = 64) -> str:
    return secrets.token_urlsafe(length)[:length]

async def add_delivery(chat_id: int, message_id: int, delete_at: Optional[datetime]):
    async with AsyncSessionLocal() as db:
        rec = DeliveryModel(chat_id=chat_id, message_id=message_id, delete_at=delete_at)
        db.add(rec)
        await db.commit()

async def schedule_autodelete():
    while True:
        try:
            async with AsyncSessionLocal() as db:
                now = datetime.utcnow()
                stmt = select(DeliveryModel).where(DeliveryModel.delete_at <= now)
                res = await db.execute(stmt)
                to_delete = res.scalars().all()
                for d in to_delete:
                    try:
                        await bot.delete_message(d.chat_id, d.message_id)
                    except Exception:
                        pass
                    await db.delete(d)
                await db.commit()
        except Exception:
            logger.exception("Error in autodelete scheduler")
        await asyncio.sleep(60)

# ---------------------------------------------------------------------
# Start message with optional payload
# ---------------------------------------------------------------------
@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandStart):
    payload = command.args
    if not payload:
        # Normal /start
        async with AsyncSessionLocal() as db:
            stmt = select(StartMessage).order_by(StartMessage.updated_at.desc())
            res = await db.execute(stmt)
            start_msg = res.scalars().first()
            if start_msg and start_msg.content:
                text = start_msg.content.replace("{first_name}", message.from_user.first_name)
                await message.answer(text, parse_mode="HTML")
            else:
                await message.answer(f"Welcome, {message.from_user.first_name}!")
        return

    # Start payload is token for deep link
    token = payload.strip()
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.link == token)
        res = await db.execute(stmt)
        session_obj = res.scalars().first()
        if not session_obj:
            await message.answer("❌ Link not found or revoked.")
            return
        if session_obj.revoked:
            await message.answer("❌ This link has been revoked.")
            return
        stmt2 = select(FileModel).where(FileModel.session_id == session_obj.id).order_by(FileModel.order_index)
        res2 = await db.execute(stmt2)
        files = res2.scalars().all()
        if not files:
            await message.answer("❌ No files available.")
            return

        sent_msgs = []
        for f in files:
            try:
                protect = session_obj.protect_content
                caption = f.caption or ""
                if f.file_type == "photo":
                    m = await bot.send_photo(message.chat.id, f.tg_file_id, caption=caption or None, protect_content=protect)
                elif f.file_type == "video":
                    m = await bot.send_video(message.chat.id, f.tg_file_id, caption=caption or None, protect_content=protect)
                elif f.file_type == "document":
                    m = await bot.send_document(message.chat.id, f.tg_file_id, caption=caption or None, protect_content=protect)
                elif f.file_type == "audio":
                    m = await bot.send_audio(message.chat.id, f.tg_file_id, caption=caption or None, protect_content=protect)
                elif f.file_type == "voice":
                    m = await bot.send_voice(message.chat.id, f.tg_file_id, protect_content=protect)
                elif f.file_type == "sticker":
                    m = await bot.send_sticker(message.chat.id, f.tg_file_id, protect_content=protect)
                else:
                    m = await bot.send_document(message.chat.id, f.tg_file_id, caption=caption or None, protect_content=protect)
                sent_msgs.append((m.chat.id, m.message_id))
                if session_obj.autodelete_minutes > 0:
                    delete_at = datetime.utcnow() + timedelta(minutes=session_obj.autodelete_minutes)
                    await add_delivery(m.chat.id, m.message_id, delete_at)
            except Exception as exc:
                logger.exception("Failed to send file: %s", exc)
        if session_obj.autodelete_minutes > 0:
            await message.answer(f"Files will auto-delete in {session_obj.autodelete_minutes} minutes.")
        else:
            await message.answer("Files delivered. (No auto-delete set)")

# ---------------------------------------------------------------------
# /upload - owner only
# ---------------------------------------------------------------------
@dp.message(Command(commands=["upload"]))
async def cmd_upload(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use this command.")
        return
    await state.clear()
    await state.set_state(UploadStates.waiting_for_files)
    await state.update_data(files=[])
    await message.reply("Send files to upload. When done, send /done. To cancel, send /abort.")

# ---------------------------------------------------------------------
# /abort - owner only
# ---------------------------------------------------------------------
@dp.message(Command(commands=["abort"]))
async def cmd_abort(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use this command.")
        return
    data = await state.get_data()
    files = data.get("files", [])
    for f in files:
        msg_id = f.get("forwarded_upload_channel_message")
        if msg_id:
            try:
                await bot.delete_message(UPLOAD_CHANNEL_ID, msg_id)
            except Exception:
                pass
    await state.clear()
    await message.reply("Upload aborted and temporary messages cleaned.")

# ---------------------------------------------------------------------
# /done - finish upload (owner only)
# ---------------------------------------------------------------------
@dp.message(Command(commands=["done"]))
async def cmd_done(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can use this command.")
        return
    current_state = await state.get_state()
    if current_state != UploadStates.waiting_for_files.state:
        await message.reply("No active upload session. Start with /upload.")
        return
    data = await state.get_data()
    files = data.get("files", [])
    if not files:
        await message.reply("No files uploaded. Use /abort to cancel or send files first.")
        await state.clear()
        return
    await message.reply("Protect content? Reply with `on` or `off`.")
    await state.set_state(UploadStates.awaiting_protect)

# ---------------------------------------------------------------------
# Generic handler for files & FSM
# ---------------------------------------------------------------------
@dp.message()
async def generic_handler(message: Message, state: FSMContext):
    st = await state.get_state()
    data = await state.get_data()
    if st == UploadStates.awaiting_protect.state:
        text = (message.text or "").strip().lower()
        if text in ("on", "off"):
            protect = text == "on"
            await state.update_data(protect=protect)
            await message.reply("Set autodelete timer in minutes (0-10080, 0=no deletion).")
            await state.set_state(UploadStates.awaiting_autodelete)
        else:
            await message.reply("Reply with `on` or `off`.")
        return
    if st == UploadStates.awaiting_autodelete.state:
        try:
            minutes = int(message.text.strip())
            if minutes < 0 or minutes > 10080:
                raise ValueError
            await state.update_data(autodelete=minutes)
            await finalize_upload(message, state)
        except ValueError:
            await message.reply("Send integer between 0 and 10080.")
        return
    if st == UploadStates.waiting_for_files.state:
        ftype, file_id, caption = None, None, ""
        if message.photo:
            ftype, file_id, caption = "photo", message.photo[-1].file_id, message.caption or ""
        elif message.video:
            ftype, file_id, caption = "video", message.video.file_id, message.caption or ""
        elif message.document:
            ftype, file_id, caption = "document", message.document.file_id, message.caption or ""
        elif message.audio:
            ftype, file_id, caption = "audio", message.audio.file_id, message.caption or ""
        elif message.voice:
            ftype, file_id, caption = "voice", message.voice.file_id, ""
        elif message.sticker:
            ftype, file_id, caption = "sticker", message.sticker.file_id, ""
        else:
            await message.reply("Send valid file type or /done to finish.")
            return
        try:
            fwd = await bot.forward_message(UPLOAD_CHANNEL_ID, message.chat.id, message.message_id)
            files = data.get("files", [])
            files.append({
                "file_type": ftype,
                "caption": caption,
                "orig_file_id": file_id,
                "forwarded_upload_channel_message": fwd.message_id
            })
            await state.update_data(files=files)
            await message.reply(f"Saved {ftype}. Send more or /done.")
        except Exception as e:
            logger.exception("Forward failed: %s", e)
            await message.reply("Failed to forward. Ensure bot is admin in upload channel.")

# ---------------------------------------------------------------------
# Finalize upload
# ---------------------------------------------------------------------
async def finalize_upload(message: Message, state: FSMContext):
    data = await state.get_data()
    files = data.get("files", [])
    protect = data.get("protect", False)
    autodelete = data.get("autodelete", 0)
    async with AsyncSessionLocal() as db:
        session_rec = SessionModel(
            link=generate_token(64),
            owner_id=OWNER_ID,
            protect_content=protect,
            autodelete_minutes=autodelete
        )
        db.add(session_rec)
        await db.flush()
        order_index = 1
        for f in files:
            fwd_msg_id = f.get("forwarded_upload_channel_message")
            try:
                fwd_msg = await bot.get_message(UPLOAD_CHANNEL_ID, fwd_msg_id)
                if fwd_msg.photo:
                    ftype, file_id, caption = "photo", fwd_msg.photo[-1].file_id, fwd_msg.caption or ""
                elif fwd_msg.video:
                    ftype, file_id, caption = "video", fwd_msg.video.file_id, fwd_msg.caption or ""
                elif fwd_msg.document:
                    ftype, file_id, caption = "document", fwd_msg.document.file_id, fwd_msg.caption or ""
                elif fwd_msg.audio:
                    ftype, file_id, caption = "audio", fwd_msg.audio.file_id, fwd_msg.caption or ""
                elif fwd_msg.voice:
                    ftype, file_id, caption = "voice", fwd_msg.voice.file_id, ""
                elif fwd_msg.sticker:
                    ftype, file_id, caption = "sticker", fwd_msg.sticker.file_id, ""
                else:
                    ftype, file_id, caption = f["file_type"], f["orig_file_id"], f["caption"]
            except Exception:
                ftype, file_id, caption = f["file_type"], f["orig_file_id"], f["caption"]
            fm = FileModel(session_id=session_rec.id, tg_file_id=file_id, file_type=ftype, caption=caption, order_index=order_index)
            db.add(fm)
            order_index += 1
        await db.commit()
        bot_username = (await bot.get_me()).username
        deep_link = f"https://t.me/{bot_username}?start={session_rec.link}"
        await message.reply(f"✅ Upload complete.\nDeep link:\n{deep_link}\nProtect: {'ON' if protect else 'OFF'}\nAutodelete: {autodelete} min")
        await state.clear()

# ---------------------------------------------------------------------
# /revoke - owner only
# ---------------------------------------------------------------------
@dp.message(Command(commands=["revoke"]))
async def cmd_revoke(message: Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can revoke links.")
        return
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Usage: /revoke <token>")
        return
    token = args[1].strip()
    async with AsyncSessionLocal() as db:
        stmt = select(SessionModel).where(SessionModel.link == token)
        res = await db.execute(stmt)
        session_obj = res.scalars().first()
        if not session_obj:
            await message.reply("Link not found.")
            return
        if session_obj.revoked:
            await message.reply("Link already revoked.")
            return
        session_obj.revoked = True
        db.add(session_obj)
        await db.commit()
        await message.reply("✅ Link revoked.")

# ---------------------------------------------------------------------
# /edit_start - owner only
# ---------------------------------------------------------------------
@dp.message(Command(commands=["edit_start"]))
async def cmd_edit_start(message: Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("❌ Only the owner can edit start message.")
        return
    if not message.reply_to_message or not (message.reply_to_message.text or message.reply_to_message.caption):
        await message.reply("Reply to a message with text/image to set as start message.")
        return
    content = message.reply_to_message.text or message.reply_to_message.caption
    async with AsyncSessionLocal() as db:
        stmt = select(StartMessage).order_by(StartMessage.updated_at.desc())
        res = await db.execute(stmt)
        start_msg = res.scalars().first()
        if start_msg:
            start_msg.content = content
            db.add(start_msg)
        else:
            start_msg = StartMessage(content=content)
            db.add(start_msg)
        await db.commit()
        await message.reply("✅ /start message updated successfully.")

# ---------------------------------------------------------------------
# /help
# ---------------------------------------------------------------------
@dp.message(Command(commands=["help"]))
async def cmd_help(message: Message):
    text = (
        "Commands:\n"
        "/start - welcome message or open deep link\n"
        "/upload - upload files (owner only)\n"
        "/done - finish upload (owner only)\n"
        "/abort - cancel upload (owner only)\n"
        "/revoke <token> - revoke deep link (owner only)\n"
        "/edit_start - set custom /start message (owner only)\n"
        "/help - this message"
    )
    await message.reply(text)

# ---------------------------------------------------------------------
# /health endpoint for uptime monitors
# ---------------------------------------------------------------------
async def health_handler(request):
    return web.Response(text="OK")

# ---------------------------------------------------------------------
# Startup & main
# ---------------------------------------------------------------------
async def on_startup():
    logger.info("Initializing DB...")