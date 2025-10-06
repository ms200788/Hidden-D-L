# bot.py
import os
import asyncio
import logging
import secrets
import string
from datetime import datetime, timedelta, timezone

from typing import Optional, List

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, Text
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiohttp import web

from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    DateTime,
    Boolean,
    ForeignKey,
    select,
    func,
    Text as SQLText,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ---------- Environment (no hard-coded sensitive data) ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))  # e.g. 6169237879
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))  # e.g. -1002985126019
DATABASE_URL = os.getenv("DATABASE_URL")  # async PG string, e.g. postgresql+asyncpg://...
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # if set, webhook will be used
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8080"))

if not BOT_TOKEN or OWNER_ID == 0 or UPLOAD_CHANNEL_ID == 0 or not DATABASE_URL:
    log.error("Missing required environment variables. BOT_TOKEN, OWNER_ID, UPLOAD_CHANNEL_ID, DATABASE_URL required.")
    raise SystemExit("Please set required environment variables.")

# ---------- Database setup ----------
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(BigInteger, primary_key=True)  # telegram user id
    first_name = Column(String, nullable=True)
    username = Column(String, nullable=True)
    last_seen = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class Session(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(BigInteger, nullable=False)
    started_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    finished = Column(Boolean, default=False)

class FileItem(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey("sessions.id"), nullable=True)
    upload_channel_message_id = Column(BigInteger, nullable=False)
    upload_channel_id = Column(BigInteger, nullable=False)
    origin_caption = Column(SQLText, nullable=True)
    origin_file_type = Column(String, nullable=True)
    deep_link = Column(String(256), unique=True, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    autodelete_minutes = Column(Integer, nullable=True)
    # relation
    session = relationship("Session", backref="files")

class PendingDelete(Base):
    __tablename__ = "pending_deletes"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False)
    chat_id = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    expire_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class Welcome(Base):
    __tablename__ = "welcome"
    id = Column(Integer, primary_key=True, autoincrement=True)
    image_file_id = Column(String, nullable=True)
    text = Column(SQLText, nullable=True)

# Create engine & session factory
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# ---------- Bot & dispatcher ----------
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

# ---------- Utils ----------
ALPHABET = string.ascii_letters + string.digits + "-_"

def gen_deep_link(length: int = 128) -> str:
    return ''.join(secrets.choice(ALPHABET) for _ in range(length))

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def add_or_update_user(tg_user: types.User):
    async with AsyncSessionLocal() as session:
        q = await session.get(User, tg_user.id)
        if q is None:
            q = User(id=tg_user.id, first_name=tg_user.first_name, username=tg_user.username, last_seen=datetime.now(timezone.utc))
            session.add(q)
        else:
            q.first_name = tg_user.first_name
            q.username = tg_user.username
            q.last_seen = datetime.now(timezone.utc)
        await session.commit()

# ---------- Background tasks ----------
DELETE_CHECK_INTERVAL = 15  # seconds

async def pending_delete_worker():
    log.info("Pending delete worker started.")
    while True:
        try:
            now = datetime.now(timezone.utc)
            async with AsyncSessionLocal() as s:
                q = await s.execute(select(PendingDelete).where(PendingDelete.expire_at <= now))
                due = q.scalars().all()
                for pd in due:
                    try:
                        await bot.delete_message(chat_id=pd.chat_id, message_id=pd.message_id)
                    except TelegramBadRequest as e:
                        log.debug(f"delete_message failed (probably already deleted): {e}")
                    except Exception as e:
                        log.exception("Error deleting message:", e)
                    # remove record
                    await s.delete(pd)
                await s.commit()
        except Exception:
            log.exception("pending_delete_worker loop error")
        await asyncio.sleep(DELETE_CHECK_INTERVAL)

# ---------- Handlers ----------
# start: show welcome image and text (supports {first_name})
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await add_or_update_user(message.from_user)
    async with AsyncSessionLocal() as s:
        q = await s.execute(select(Welcome).order_by(Welcome.id.desc()).limit(1))
        w = q.scalars().first()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Get Deep Link", callback_data="dummy")]])
    if w:
        text = w.text or ""
        text = text.replace("{first_name}", (message.from_user.first_name or ""))
        if w.image_file_id:
            try:
                await bot.send_photo(chat_id=message.chat.id, photo=w.image_file_id, caption=text, reply_markup=None)
            except Exception:
                # fallback to text
                await message.answer(text or "Welcome!")
        else:
            await message.answer(text or "Welcome!")
    else:
        await message.answer("Welcome! Use /start to see this message.")

# setstart owner-only
@dp.message(Command("setstart"))
async def cmd_setstart(message: Message):
    if message.from_user.id != OWNER_ID:
        return await message.reply("Only owner can set start message.")
    # Owner replies to a message or sends new text or photo to set welcome
    if message.reply_to_message:
        r = message.reply_to_message
        img_id = None
        if r.photo:
            img_id = r.photo[-1].file_id
        text = r.text or r.caption or ""
        async with AsyncSessionLocal() as s:
            w = Welcome(image_file_id=img_id, text=text)
            s.add(w)
            await s.commit()
        await message.reply("Start/welcome message set.")
    else:
        # set text-only: /setstart Your text here...
        args = message.text.partition(" ")[2]
        if not args:
            return await message.reply("Reply to a message (photo/text) or send `/setstart <text>`.")
        async with AsyncSessionLocal() as s:
            w = Welcome(image_file_id=None, text=args)
            s.add(w)
            await s.commit()
        await message.reply("Start/welcome text set.")

# start upload session (owner-only)
@dp.message(Command("startupload"))
async def cmd_startupload(message: Message):
    if message.from_user.id != OWNER_ID:
        return await message.reply("Only owner can start upload session.")
    async with AsyncSessionLocal() as s:
        sess = Session(owner_id=OWNER_ID)
        s.add(sess)
        await s.commit()
        await s.refresh(sess)
        await message.reply(f"Upload session started. Session id: {sess.id}\nSend files (documents/photos/audio/video). When done, send /d. To terminate and cancel use /e.")

# /d -> done session (mark finished)
@dp.message(Command("d"))
async def cmd_done(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    # find last unfinished session
    async with AsyncSessionLocal() as s:
        q = await s.execute(select(Session).where(Session.owner_id == OWNER_ID, Session.finished == False).order_by(Session.started_at.desc()).limit(1))
        sess = q.scalars().first()
        if not sess:
            return await message.reply("No active session found.")
        sess.finished = True
        await s.commit()
        await message.reply(f"Session {sess.id} marked finished. Files in session: {len(sess.files)}")

# /e -> terminate current session (delete it and files metadata?) We'll just mark finished and inform owner.
@dp.message(Command("e"))
async def cmd_terminate(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    async with AsyncSessionLocal() as s:
        q = await s.execute(select(Session).where(Session.owner_id == OWNER_ID, Session.finished == False).order_by(Session.started_at.desc()).limit(1))
        sess = q.scalars().first()
        if not sess:
            return await message.reply("No active session found.")
        # mark finished (you may also implement deletion of metadata if desired)
        sess.finished = True
        await s.commit()
        await message.reply(f"Session {sess.id} terminated.")

# owner can set autodelete default for upcoming uploads via /setautodel <minutes>
@dp.message(Command("setautodel"))
async def cmd_setautodel(message: Message):
    if message.from_user.id != OWNER_ID:
        return await message.reply("Owner only.")
    args = message.text.partition(" ")[2]
    if not args or not args.strip().isdigit():
        return await message.reply("Usage: /setautodel <minutes> (integer minutes). Use 0 to disable")
    minutes = int(args.strip())
    # Store in a simple in-memory place per owner for next uploads (since not asked to persist)
    # But we want persistence; simple approach: set an env var? Instead store in DB as a small table or in Session metadata.
    # For simplicity we'll set a short-lived flag in memory for now:
    global OWNER_NEXT_AUTODEL
    OWNER_NEXT_AUTODEL = minutes if minutes > 0 else None
    await message.reply(f"Auto-delete for next uploads will be: {minutes} minute(s).")

OWNER_NEXT_AUTODEL: Optional[int] = None

# Handler for messages that contain files (owner only allowed to upload)
@dp.message((Text(startswith="/") == False))
async def all_messages_handler(message: Message):
    # Save/update user record
    await add_or_update_user(message.from_user)

    # Only owner may upload files (except /start etc.)
    if message.from_user.id != OWNER_ID:
        # Allow users only to use /start and deep links; any other attempt -> reply
        txt = message.text or ""
        # If message looks like a deep link: match 128-char token
        candidate = txt.strip().split()[-1] if txt.strip() else ""
        if len(candidate) >= 64:  # user may paste deep link or anything; owners deep links typically 128
            # try to resolve deep link
            async with AsyncSessionLocal() as s:
                q = await s.execute(select(FileItem).where(FileItem.deep_link == candidate))
                fileobj = q.scalars().first()
                if fileobj:
                    await deliver_file_to_user(fileobj, message.from_user.id)
                    return
        # Otherwise show allowed commands
        return await message.reply("You can use /start or send a deep link to fetch a file.")

    # Owner messages: treat as upload if contains media
    if not (message.document or message.photo or message.video or message.audio or message.voice or message.sticker):
        # owner sent some text - allow commands to be processed above; ignore others
        return

    # Find active session
    async with AsyncSessionLocal() as s:
        q = await s.execute(select(Session).where(Session.owner_id==OWNER_ID, Session.finished==False).order_by(Session.started_at.desc()).limit(1))
        sess = q.scalars().first()
        if not sess:
            # auto-create session
            sess = Session(owner_id=OWNER_ID)
            s.add(sess)
            await s.commit()
            await s.refresh(sess)

    # Forward/copy file to upload channel (use copy_message to preserve file)
    try:
        origin_caption = message.caption or message.text or ""
        # copy message to upload channel
        copied = await bot.copy_message(
            chat_id=UPLOAD_CHANNEL_ID,
            from_chat_id=message.chat.id,
            message_id=message.message_id,
            caption=origin_caption,
        )
        upload_msg_id = copied.message_id
        # determine file type
        ftype = "document"
        if message.photo:
            ftype = "photo"
        elif message.video:
            ftype = "video"
        elif message.audio:
            ftype = "audio"
        elif message.voice:
            ftype = "voice"
        elif message.sticker:
            ftype = "sticker"

        # create deep link
        dl = gen_deep_link(128)
        # save file metadata
        async with AsyncSessionLocal() as s:
            fin = FileItem(
                session_id=sess.id,
                upload_channel_message_id=upload_msg_id,
                upload_channel_id=UPLOAD_CHANNEL_ID,
                origin_caption=origin_caption,
                origin_file_type=ftype,
                deep_link=dl,
                autodelete_minutes=OWNER_NEXT_AUTODEL,
            )
            s.add(fin)
            await s.commit()
            await s.refresh(fin)

        # reply to owner with deep link
        await message.reply(f"File uploaded ✅\nDeep link (shareable):\n`{dl}`\n\nNote: anyone with this link can fetch the file via the bot.\nAuto-delete (minutes): {OWNER_NEXT_AUTODEL or 0}")

        # reset OWNER_NEXT_AUTODEL after use
        OWNER_NEXT_AUTODEL = None
    except TelegramRetryAfter as e:
        await message.reply(f"Rate limited by Telegram. Retry after {e.timeout} seconds.")
    except Exception as e:
        log.exception("Error copying/uploading file")
        await message.reply(f"Upload failed: {e}")

# deliver file to user by copying from upload channel to user's chat_id
async def deliver_file_to_user(fileobj: FileItem, user_id: int):
    # send copy from upload channel message to user, with caption preserved.
    protect = True
    if user_id == OWNER_ID:
        protect = False  # owner bypass protect_content
    try:
        copied = await bot.copy_message(
            chat_id=user_id,
            from_chat_id=fileobj.upload_channel_id,
            message_id=fileobj.upload_channel_message_id,
            caption=fileobj.origin_caption,
            protect_content=protect,
        )
    except TelegramBadRequest as e:
        log.exception("Failed to copy message to user")
        try:
            await bot.send_message(user_id, "Sorry, failed to deliver file.")
        except Exception:
            pass
        return

    # If an autodelete was set for this file, add to pending_deletes for this user's message
    if fileobj.autodelete_minutes and fileobj.autodelete_minutes > 0:
        expire = datetime.now(timezone.utc) + timedelta(minutes=fileobj.autodelete_minutes)
        async with AsyncSessionLocal() as s:
            pd = PendingDelete(user_id=user_id, chat_id=user_id, message_id=copied.message_id, expire_at=expire)
            s.add(pd)
            await s.commit()
        # notify user about scheduled deletion
        try:
            await bot.send_message(user_id, f"This file will be deleted in {fileobj.autodelete_minutes} minute(s).")
        except Exception:
            pass

# callback for inline dummy (placeholder)
@dp.callback_query(Text(startswith="dummy"))
async def cb_dummy(callback: types.CallbackQuery):
    await callback.answer("This is a demo button.", show_alert=False)

# /deep <token> fallback for users who want command-based fetch
@dp.message(Command("deep"))
async def cmd_deep(message: Message):
    args = message.text.partition(" ")[2]
    if not args:
        return await message.reply("Usage: /deep <deep_link_token>")
    token = args.strip()
    async with AsyncSessionLocal() as s:
        q = await s.execute(select(FileItem).where(FileItem.deep_link == token))
        fileobj = q.scalars().first()
    if not fileobj:
        return await message.reply("Deep link not found.")
    await deliver_file_to_user(fileobj, message.from_user.id)

# /stats (owner only)
@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    two_days = datetime.now(timezone.utc) - timedelta(days=2)
    async with AsyncSessionLocal() as s:
        total_users = await s.scalar(select(func.count(User.id)))
        active_users = await s.scalar(select(func.count(User.id)).where(User.last_seen >= two_days))
        total_files = await s.scalar(select(func.count(FileItem.id)))
        total_sessions = await s.scalar(select(func.count(Session.id)))
    await message.reply(
        f"Stats:\n• Total users: {total_users}\n• Active users (2 days): {active_users}\n• Total files: {total_files}\n• Total sessions: {total_sessions}"
    )

# /broadcast - owner only with inline buttons support
@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    # Owner should reply to a message (text/photo) containing the broadcast payload.
    if not message.reply_to_message:
        return await message.reply("Reply to a message you want to broadcast (text or media).")
    payload = message.reply_to_message
    # parse optional inline buttons from owner message text line after /broadcast e.g.
    # /broadcast [Button1|https://...] [Button2|https://...]
    # A simple parser:
    parts = message.text.partition(" ")[2].strip()
    buttons = []
    if parts:
        for token in parts.split():
            if "|" in token:
                t, url = token.split("|", 1)
                buttons.append([InlineKeyboardButton(t, url=url)])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None

    # fetch all users
    async with AsyncSessionLocal() as s:
        rows = await s.execute(select(User.id))
        users = [r[0] for r in rows.all()]

    sent = 0
    for uid in users:
        try:
            if payload.photo:
                await bot.send_photo(chat_id=uid, photo=payload.photo[-1].file_id, caption=payload.caption or "", reply_markup=kb)
            elif payload.text:
                await bot.send_message(chat_id=uid, text=payload.text, reply_markup=kb)
            elif payload.document:
                await bot.send_document(chat_id=uid, document=payload.document.file_id, caption=payload.caption or "", reply_markup=kb)
            else:
                # fallback forward
                await bot.forward_message(chat_id=uid, from_chat_id=payload.chat.id, message_id=payload.message_id)
            sent += 1
        except Exception:
            continue

    await message.reply(f"Broadcast sent to {sent}/{len(users)} users.")

# simple health endpoint and start logic
async def on_startup(app):
    await init_db()
    # start background worker
    app["delete_worker"] = asyncio.create_task(pending_delete_worker())
    # if webhook configured, set webhook
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL + WEBHOOK_PATH)

async def on_shutdown(app):
    # cancel delete worker
    worker = app.get("delete_worker")
    if worker:
        worker.cancel()
    try:
        await bot.delete_webhook()
    except Exception:
        pass
    await bot.session.close()

# A quick endpoint to accept webhook requests (if using webhook)
async def handle_webhook(request: web.Request):
    body = await request.read()
    update = types.Update.to_object(types.BaseObject.parse_raw(body))
    await dp.feed_update(update)  # feed update into dispatcher
    return web.Response(text="ok")

# Health
async def health(request: web.Request):
    return web.Response(text="ok")

# Build aiohttp app for webhook mode; if polling mode, run polling instead.
def create_app():
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get("/health", health)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_shutdown)
    return app

# ---------- Entrypoint ----------
if __name__ == "__main__":
    # choose webhook if WEBHOOK_URL set, else polling
    if WEBHOOK_URL:
        app = create_app()
        web.run_app(app, host=HOST, port=PORT)
    else:
        # Polling mode (useful if webhook not configured)
        async def _polling_main():
            await init_db()
            # start delete worker
            worker = asyncio.create_task(pending_delete_worker())
            try:
                # Start long polling
                await dp.start_polling(bot)
            finally:
                worker.cancel()
        asyncio.run(_polling_main())