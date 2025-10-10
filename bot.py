# bot.py
import os
import asyncio
import logging
import secrets
from datetime import datetime, timedelta
from typing import Optional

from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage

from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# ------------------ Logging ------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------ Config ------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")  # postgres://user:pass@host:port/db
UPLOAD_CHANNEL_ID = int(os.environ.get("UPLOAD_CHANNEL_ID"))  # private channel id (negative)
OWNER_ID = int(os.environ.get("OWNER_ID"))  # single owner Telegram ID
PORT = int(os.environ.get("PORT", 8080))

if not BOT_TOKEN or not DATABASE_URL or not UPLOAD_CHANNEL_ID or not OWNER_ID:
    raise RuntimeError("Missing required env vars")

# ------------------ Database ------------------
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
    content = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# ------------------ FSM ------------------
class UploadStates(StatesGroup):
    waiting_for_files = State()
    awaiting_protect = State()
    awaiting_autodelete = State()

# ------------------ Bot ------------------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(storage=storage)

# ------------------ Helpers ------------------
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
            logger.exception("Autodelete scheduler error")
        await asyncio.sleep(60)

# ------------------ /start ------------------
@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandStart):
    payload = command.args
    async with AsyncSessionLocal() as db:
        if not payload:
            stmt = select(StartMessage).order_by(StartMessage.updated_at.desc())
            res = await db.execute(stmt)
            start_msg = res.scalars().first()
            if start_msg and start_msg.content:
                text = start_msg.content.replace("{first_name}", message.from_user.first_name)
                await message.answer(text, parse_mode="HTML")
            else:
                await message.answer(f"Welcome, {message.from_user.first_name}!")
            return

        # Payload = deep link token
        token = payload.strip()
        stmt = select(SessionModel).where(SessionModel.link == token)
        res = await db.execute(stmt)
        session_obj = res.scalars().first()
        if not session_obj:
            await message.answer("❌ Link not found or revoked.")
            return
        if session_obj.revoked:
            await message.answer("❌ Link has been revoked.")
            return

        stmt2 = select(FileModel).where(FileModel.session_id == session_obj.id).order_by(FileModel.order_index)
        res2 = await db.execute(stmt2)
        files = res2.scalars().all()
        if not files:
            await message.answer("❌ No files available.")
            return

        for f in files:
            protect = session_obj.protect_content
            caption = f.caption or ""
            try:
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
                if session_obj.autodelete_minutes > 0:
                    delete_at = datetime.utcnow() + timedelta(minutes=session_obj.autodelete_minutes)
                    await add_delivery(m.chat.id, m.message_id, delete_at)
            except Exception as e:
                logger.exception("Failed sending file: %s", e)
        if session_obj.autodelete_minutes > 0:
            await message.answer(f"Files will auto-delete in {session_obj.autodelete_minutes} minutes.")
        else:
            await message.answer("Files delivered. (No auto-delete set)")

# ------------------ /upload, /done, /abort, /revoke, /edit_start, file handler ------------------
# ... (same logic as previous adjusted code)
# Ensure all functions like generic_handler(), finalize_upload(), cmd_upload(), cmd_done(), cmd_abort(), cmd_revoke(), cmd_edit_start() are implemented with Aiogram 3.0.3 FSM syntax.

# ------------------ /health ------------------
async def health_handler(request):
    return web.Response(text="OK")

# ------------------ Startup ------------------
async def on_startup():
    await init_db()
    asyncio.create_task(schedule_autodelete())
    logger.info("Bot started and DB initialized")

async def main():
    await on_startup()
    web_app = web.Application()
    web_app.router.add_get("/health", health_handler)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"Web server running on port {PORT}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")