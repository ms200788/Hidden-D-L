# bot.py
"""
Final deployable Telegram Upload + Deep-link Bot (aiogram 2.25.0)
- Owner-only upload sessions, 1..95 files per session -> 1 deep link per session
- /d flow: owner replies 'on'/'off' then supplies autodelete minutes -> deep link returned
- /e aborts session
- Files copied to UPLOAD_CHANNEL_ID; DB stores metadata only
- /start (public) and /start <token> deep-link delivery
- /setstart, /broadcast, /stats, /restart, /help (owner-only)
- Auto-delete scheduling persisted in DB and resumed after restarts
- /health endpoint for UptimeRobot
"""

import os
import asyncio
import logging
import secrets
import html
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import ParseMode, InlineKeyboardMarkup
from aiogram.utils.exceptions import BadRequest

# SQLAlchemy async
from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, Boolean, DateTime, JSON, func, select
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL")  # must be postgresql+asyncpg://...
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))  # -100...
PING_SECRET = os.getenv("PING_SECRET", "")
PORT = int(os.getenv("PORT", "8000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

if not TELEGRAM_TOKEN or OWNER_ID == 0 or not DATABASE_URL or UPLOAD_CHANNEL_ID == 0:
    raise SystemExit("Set TELEGRAM_TOKEN, OWNER_ID, DATABASE_URL and UPLOAD_CHANNEL_ID env vars.")

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("upload_bot")

# ---------------- DB models ----------------
Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    tg_id = Column(BigInteger, unique=True, nullable=False, index=True)
    username = Column(String(255))
    first_name = Column(String(255))
    last_name = Column(String(255))
    last_active = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True)
    owner_id = Column(BigInteger, nullable=False)
    token = Column(String(255), unique=True, nullable=True)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class FileEntry(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, nullable=False, index=True)
    upload_channel_id = Column(BigInteger, nullable=False)
    upload_message_id = Column(BigInteger, nullable=False)
    caption = Column(Text)
    buttons = Column(JSON, nullable=True)
    protected = Column(Boolean, default=True)
    auto_delete_minutes = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Delivery(Base):
    __tablename__ = "deliveries"
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, nullable=False)
    chat_id = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    delete_at = Column(DateTime(timezone=True), nullable=True)
    deleted = Column(Boolean, default=False)
    delivered_at = Column(DateTime(timezone=True), server_default=func.now())


class Config(Base):
    __tablename__ = "config"
    key = Column(String(128), primary_key=True)
    value = Column(Text)


# ---------------- DB setup ----------------
engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True, future=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("DB initialized.")

# ---------------- Bot setup ----------------
bot = Bot(token=TELEGRAM_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)

# runtime state for /d interactive flow
active_sessions_cache: Dict[int, int] = {}
awaiting_protect_choice: Dict[int, int] = {}
awaiting_autodel_choice: Dict[int, int] = {}
awaiting_context: Dict[int, dict] = {}

# ---------------- Utilities ----------------
def parse_braces_links(text: str) -> str:
    if not text:
        return text
    out = ""
    i = 0
    while i < len(text):
        if text[i] == "{":
            j = text.find("}", i)
            if j == -1:
                out += html.escape(text[i:])
                break
            inside = text[i+1:j]
            if "|" in inside:
                word, url = inside.split("|", 1)
                out += f'<a href="{html.escape(url.strip(), quote=True)}">{html.escape(word.strip())}</a>'
            else:
                out += html.escape("{" + inside + "}")
            i = j + 1
        else:
            out += html.escape(text[i])
            i += 1
    return out


def substitute_first_name(text: str, first_name: Optional[str]) -> str:
    if not text:
        return text
    return text.replace("{first_name}", html.escape(first_name or ""))


def keyboard_to_json(kb: Optional[InlineKeyboardMarkup]) -> Optional[List[List[Dict[str, Any]]]]:
    if not kb:
        return None
    rows = []
    for r in kb.inline_keyboard:
        row = []
        for btn in r:
            row.append({"text": btn.text, "url": getattr(btn, "url", None), "callback_data": getattr(btn, "callback_data", None)})
        rows.append(row)
    return rows


def json_to_keyboard(j: Optional[List[List[Dict[str, Any]]]]) -> Optional[InlineKeyboardMarkup]:
    if not j:
        return None
    kb = InlineKeyboardMarkup()
    for row in j:
        buttons = []
        for b in row:
            if b.get("url"):
                buttons.append(types.InlineKeyboardButton(text=b.get("text", "link"), url=b.get("url")))
            elif b.get("callback_data"):
                buttons.append(types.InlineKeyboardButton(text=b.get("text", "btn"), callback_data=b.get("callback_data")))
            else:
                buttons.append(types.InlineKeyboardButton(text=b.get("text", "btn"), callback_data="noop"))
        kb.row(*buttons)
    return kb

# ---------------- DB helper funcs ----------------
async def create_session(owner_id: int) -> int:
    async with AsyncSessionLocal() as s:
        new = SessionModel(owner_id=owner_id, active=True)
        s.add(new)
        await s.commit()
        await s.refresh(new)
        active_sessions_cache[owner_id] = new.id
        return new.id


async def get_active_session(owner_id: int) -> Optional[int]:
    sid = active_sessions_cache.get(owner_id)
    if sid:
        async with AsyncSessionLocal() as s:
            row = await s.get(SessionModel, sid)
            if row and row.active:
                return sid
            else:
                active_sessions_cache.pop(owner_id, None)
                return None
    async with AsyncSessionLocal() as s:
        q = select(SessionModel).where(SessionModel.owner_id == owner_id, SessionModel.active == True).order_by(SessionModel.created_at.desc())
        res = await s.execute(q)
        row = res.scalar_one_or_none()
        if row:
            active_sessions_cache[owner_id] = row.id
            return row.id
    return None


async def add_file_entry(session_id: int, upload_channel_id: int, upload_message_id: int,
                         caption: Optional[str], buttons: Optional[List[List[Dict[str, Any]]]],
                         protected: bool, auto_delete_minutes: int) -> int:
    async with AsyncSessionLocal() as s:
        fe = FileEntry(session_id=session_id,
                       upload_channel_id=upload_channel_id,
                       upload_message_id=upload_message_id,
                       caption=caption,
                       buttons=buttons,
                       protected=protected,
                       auto_delete_minutes=auto_delete_minutes)
        s.add(fe)
        await s.commit()
        await s.refresh(fe)
        return fe.id


async def finalize_session(owner_id: int, session_id: int, protect: bool, autodel_minutes: int) -> str:
    token = secrets.token_urlsafe(64)
    while len(token) < 44:
        token = secrets.token_urlsafe(64)
    async with AsyncSessionLocal() as s:
        row = await s.get(SessionModel, session_id)
        if not row:
            raise RuntimeError("session not found")
        row.token = token
        row.active = False
        s.add(row)
        q = select(FileEntry).where(FileEntry.session_id == session_id)
        res = await s.execute(q)
        files = res.scalars().all()
        for f in files:
            f.protected = protect
            f.auto_delete_minutes = autodel_minutes
            s.add(f)
        await s.commit()
    active_sessions_cache.pop(owner_id, None)
    awaiting_protect_choice.pop(owner_id, None)
    awaiting_autodel_choice.pop(owner_id, None)
    awaiting_context.pop(owner_id, None)
    return token


async def set_config(key: str, value: str):
    async with AsyncSessionLocal() as s:
        cfg = await s.get(Config, key)
        if cfg:
            cfg.value = value
        else:
            cfg = Config(key=key, value=value)
            s.add(cfg)
        await s.commit()


async def get_config(key: str) -> Optional[str]:
    async with AsyncSessionLocal() as s:
        cfg = await s.get(Config, key)
        return cfg.value if cfg else None


async def upsert_user(tg_user: types.User):
    async with AsyncSessionLocal() as s:
        q = select(User).where(User.tg_id == tg_user.id)
        res = await s.execute(q)
        row = res.scalar_one_or_none()
        now = datetime.now(timezone.utc)
        if row:
            row.username = tg_user.username
            row.first_name = tg_user.first_name
            row.last_name = tg_user.last_name
            row.last_active = now
            s.add(row)
        else:
            u = User(tg_id=tg_user.id, username=tg_user.username,
                     first_name=tg_user.first_name, last_name=tg_user.last_name,
                     last_active=now)
            s.add(u)
        await s.commit()

# ---------------- Deletion worker ----------------
async def deletion_worker():
    logger.info("Starting deletion worker (poll every 30s)")
    while True:
        try:
            now = datetime.now(timezone.utc)
            async with AsyncSessionLocal() as s:
                q = select(Delivery).where(Delivery.delete_at != None, Delivery.delete_at <= now, Delivery.deleted == False)
                res = await s.execute(q)
                rows = res.scalars().all()
                for d in rows:
                    try:
                        await bot.delete_message(chat_id=d.chat_id, message_id=d.message_id)
                        d.deleted = True
                        s.add(d)
                        logger.info("Deleted message %s in chat %s", d.message_id, d.chat_id)
                    except BadRequest as e:
                        d.deleted = True
                        s.add(d)
                        logger.warning("Delete failed for message %s in chat %s: %s", d.message_id, d.chat_id, e)
                await s.commit()
        except Exception as e:
            logger.exception("Deletion worker error: %s", e)
        await asyncio.sleep(30)

# ---------------- HTTP health ----------------
async def handle_health(request):
    if PING_SECRET:
        token = request.query.get("token", "")
        if token != PING_SECRET:
            return web.Response(text="unauthorized", status=401)
    return web.Response(text="ok")


async def start_webapp(host="0.0.0.0", port=PORT):
    app = web.Application()
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info("Health endpoint started on port %s", port)

# ---------------- Handlers ----------------

# /start (public) — if args present treat as deep link token
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await upsert_user(message.from_user)

    args = message.get_args().strip()
    if args:
        token = args
        async with AsyncSessionLocal() as s:
            q = select(SessionModel).where(SessionModel.token == token)
            res = await s.execute(q)
            sess = res.scalar_one_or_none()
            if not sess:
                await message.reply("Invalid or expired link.")
                return
            q2 = select(FileEntry).where(FileEntry.session_id == sess.id)
            res2 = await s.execute(q2)
            files = res2.scalars().all()
            if not files:
                await message.reply("No files found for this link.")
                return
            delivered_pairs = []
            max_autodel = 0
            for f in files:
                try:
                    try:
                        msg = await bot.copy_message(chat_id=message.chat.id,
                                                     from_chat_id=f.upload_channel_id,
                                                     message_id=f.upload_message_id,
                                                     protect_content=bool(f.protected))
                    except TypeError:
                        msg = await bot.copy_message(chat_id=message.chat.id,
                                                     from_chat_id=f.upload_channel_id,
                                                     message_id=f.upload_message_id)
                    delivered_pairs.append((f, msg))
                    await asyncio.sleep(0.05)
                except Exception as e:
                    logger.exception("Delivery fail file %s to %s: %s", getattr(f, "id", "?"), message.chat.id, e)
            async with AsyncSessionLocal() as s2:
                for f, msg in delivered_pairs:
                    if f.auto_delete_minutes and f.auto_delete_minutes > 0:
                        delete_at = datetime.now(timezone.utc) + timedelta(minutes=int(f.auto_delete_minutes))
                        d = Delivery(file_id=f.id, chat_id=message.chat.id, message_id=msg.message_id, delete_at=delete_at)
                        s2.add(d)
                        max_autodel = max(max_autodel, f.auto_delete_minutes)
                await s2.commit()
            if max_autodel > 0:
                await message.reply(f"These files will be deleted in {max_autodel} minutes.")
            return

    # no args -> show saved start message (if any)
    cfg = await get_config("start_pointer")
    if cfg:
        try:
            channel_id_str, msg_id_str = cfg.split(":")
            c_id = int(channel_id_str)
            m_id = int(msg_id_str)
            start_template = await get_config("start_template") or ""
            txt = substitute_first_name(start_template, message.from_user.first_name)
            txt = parse_braces_links(txt)
            try:
                await bot.copy_message(chat_id=message.chat.id, from_chat_id=c_id, message_id=m_id, protect_content=False)
            except TypeError:
                await bot.copy_message(chat_id=message.chat.id, from_chat_id=c_id, message_id=m_id)
            if txt.strip():
                await bot.send_message(chat_id=message.chat.id, text=txt, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
            return
        except Exception as e:
            logger.exception("Error sending start pointer: %s", e)
            await message.reply("Welcome!")
            return
    await message.reply("Welcome! No start message configured.")

# /help (owner only)
@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Owner-only command.")
        return
    help_text = (
        "/upload - start upload session (owner only)\n"
        "/d - finalize session (owner only) -> then reply 'on'/'off' for protect, then enter minutes\n"
        "/e - abort session\n"
        "/setstart - reply to message to set /start content\n"
        "/broadcast - reply to message to broadcast to all users\n"
        "/stats - show totals\n"
        "/restart - restart bot\n"
        "/health - health endpoint\n"
        "Formatting: {first_name} and {word|url} supported."
    )
    await message.reply(help_text)

# /upload (owner only)
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Owner-only.")
        return
    existing = await get_active_session(OWNER_ID)
    if existing:
        await message.reply(f"Active session exists (id={existing}). Use /d to finalize or /e to abort.")
        return
    sid = await create_session(OWNER_ID)
    await message.reply(f"Upload session started (id={sid}). Send 1..95 files. When done send /d. To abort send /e.")

# owner media collector (only media content types — will NOT intercept text commands)
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID, content_types=[
    "photo", "document", "video", "animation", "sticker", "audio", "voice"
])
async def owner_file_collector(message: types.Message):
    sid = await get_active_session(OWNER_ID)
    if not sid:
        return
    # check limit
    async with AsyncSessionLocal() as s:
        q = select(FileEntry).where(FileEntry.session_id == sid)
        res = await s.execute(q)
        existing = res.scalars().all()
        if len(existing) >= 95:
            await message.reply("Session reached 95 files (max). Send /d to finalize or /e to abort.")
            return
    caption_text = message.caption or message.text or None
    buttons_json = None
    if message.reply_markup:
        try:
            buttons_json = keyboard_to_json(message.reply_markup)
        except Exception:
            buttons_json = None
    try:
        copied = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
        fid = await add_file_entry(session_id=sid,
                                   upload_channel_id=UPLOAD_CHANNEL_ID,
                                   upload_message_id=copied.message_id,
                                   caption=caption_text,
                                   buttons=buttons_json,
                                   protected=True,
                                   auto_delete_minutes=0)
        await message.reply(f"Saved file to upload channel (entry id={fid}).")
    except Exception as e:
        logger.exception("Forward to upload channel failed: %s", e)
        await message.reply("Failed to save file to upload channel. Ensure bot is admin there.")

# /d finalize: ask protect content (owner replies 'on'/'off', then minutes)
@dp.message_handler(commands=["d"])
async def cmd_d(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    sid = await get_active_session(OWNER_ID)
    if not sid:
        await message.reply("No active session.")
        return
    async with AsyncSessionLocal() as s:
        q = select(FileEntry).where(FileEntry.session_id == sid)
        res = await s.execute(q)
        files = res.scalars().all()
        if not files:
            await message.reply("Session contains no files. Upload at least one file before finalizing.")
            return
    awaiting_protect_choice[OWNER_ID] = sid
    awaiting_context[OWNER_ID] = {"session_id": sid}
    await message.reply("Protect content? Reply 'on' or 'off'.")


# owner finalize flow: handles only owner text replies for protect/autodel; ignores commands
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID, content_types=["text"])
async def owner_finalize_flow(message: types.Message):
    txt_raw = (message.text or "").strip()
    if txt_raw.startswith("/"):
        # command — let command handlers process it
        return

    # protect choice stage
    if OWNER_ID in awaiting_protect_choice and OWNER_ID not in awaiting_autodel_choice:
        txt = txt_raw.lower()
        if txt not in ("on", "off"):
            await message.reply("Please reply 'on' or 'off' for protect content.")
            return
        sid = awaiting_protect_choice.pop(OWNER_ID)
        protect = (txt == "on")
        awaiting_context[OWNER_ID]["protect"] = protect
        awaiting_autodel_choice[OWNER_ID] = sid
        await message.reply("Enter auto-delete time in minutes (0..10080). Send 0 to disable.")
        return

    # autodel stage
    if OWNER_ID in awaiting_autodel_choice:
        if not re.fullmatch(r"\d{1,5}", txt_raw):
            await message.reply("Please send a number between 0 and 10080.")
            return
        minutes = int(txt_raw)
        if minutes < 0 or minutes > 10080:
            await message.reply("Minutes must be between 0 and 10080.")
            return
        sid = awaiting_autodel_choice.pop(OWNER_ID)
        context = awaiting_context.pop(OWNER_ID, {})
        protect = context.get("protect", True)
        try:
            token = await finalize_session(OWNER_ID, sid, protect=protect, autodel_minutes=minutes)
        except Exception as e:
            logger.exception("Finalize error: %s", e)
            await message.reply("Failed to finalize session.")
            return
        bot_info = await bot.get_me()
        full_link = f"https://t.me/{bot_info.username}?start={token}"
        if minutes > 0:
            await bot.send_message(chat_id=OWNER_ID, text=f"Session finalized. Deep link:\n{full_link}\nThese files will be deleted in {minutes} minutes after delivery.")
        else:
            await bot.send_message(chat_id=OWNER_ID, text=f"Session finalized. Deep link:\n{full_link}")
        return

    # otherwise ignore
    return

# /e abort session
@dp.message_handler(commands=["e"])
async def cmd_e(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    sid = await get_active_session(OWNER_ID)
    if not sid:
        await message.reply("No active session.")
        return
    async with AsyncSessionLocal() as s:
        row = await s.get(SessionModel, sid)
        if row:
            row.active = False
            row.token = None
            s.add(row)
            await s.commit()
    active_sessions_cache.pop(OWNER_ID, None)
    awaiting_protect_choice.pop(OWNER_ID, None)
    awaiting_autodel_choice.pop(OWNER_ID, None)
    awaiting_context.pop(OWNER_ID, None)
    await message.reply("Session aborted. Uploaded copies remain in upload channel but are not accessible via deep link.")

# /setstart owner-only: reply to a message to set start content
@dp.message_handler(commands=["setstart"])
async def cmd_setstart(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or media) to set /start content.")
        return
    try:
        copied = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.reply_to_message.chat.id, message_id=message.reply_to_message.message_id)
        pointer = f"{UPLOAD_CHANNEL_ID}:{copied.message_id}"
        await set_config("start_pointer", pointer)
        template = message.reply_to_message.caption or message.reply_to_message.text or ""
        await set_config("start_template", template)
        await message.reply("Start content saved.")
    except Exception as e:
        logger.exception("Setstart failed: %s", e)
        await message.reply("Failed to set start content. Ensure bot is admin in upload channel.")

# /broadcast owner-only: reply to message to broadcast
@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    target = message.reply_to_message
    if not target:
        await message.reply("Reply to a message to broadcast.")
        return
    try:
        copied = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=target.chat.id, message_id=target.message_id)
        src_chat_id = UPLOAD_CHANNEL_ID
        src_msg_id = copied.message_id
    except Exception as e:
        logger.exception("Broadcast prepare failed: %s", e)
        await message.reply("Failed to prepare broadcast.")
        return
    async with AsyncSessionLocal() as s:
        q = select(User.tg_id)
        res = await s.execute(q)
        rows = [r[0] for r in res.fetchall()]
    sent = 0
    failed = 0
    for uid in rows:
        try:
            try:
                await bot.copy_message(chat_id=uid, from_chat_id=src_chat_id, message_id=src_msg_id, protect_content=True)
            except TypeError:
                await bot.copy_message(chat_id=uid, from_chat_id=src_chat_id, message_id=src_msg_id)
            sent += 1
            await asyncio.sleep(0.08)
        except Exception as e:
            logger.warning("Broadcast failed %s: %s", uid, e)
            failed += 1
    await message.reply(f"Broadcast complete. Sent: {sent}, Failed: {failed}")

# /stats
@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    two_days_ago = datetime.now(timezone.utc) - timedelta(days=2)
    async with AsyncSessionLocal() as s:
        total_users = (await s.execute(select(func.count()).select_from(User))).scalar()
        active_users = (await s.execute(select(func.count()).select_from(User).where(User.last_active >= two_days_ago))).scalar()
        total_files = (await s.execute(select(func.count()).select_from(FileEntry))).scalar()
        total_sessions = (await s.execute(select(func.count()).select_from(SessionModel))).scalar()
    await message.reply(f"Total users: {total_users}\nActive (2 days): {active_users}\nTotal files: {total_files}\nTotal sessions: {total_sessions}")

# /restart owner-only
@dp.message_handler(commands=["restart"])
async def cmd_restart(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    await message.reply("Restarting now...")
    await bot.close()
    os._exit(0)

# fallback - update user last_active for any non-command messages
@dp.message_handler(content_types=types.ContentType.ANY)
async def fallback(message: types.Message):
    # do not intercept commands (text starting with '/')
    if message.text and message.text.startswith("/"):
        return
    await upsert_user(message.from_user)
    # no reply for general users

# ---------------- Startup / Restore ----------------
async def restore_on_startup():
    # DB is source-of-truth; nothing heavy to load here
    logger.info("Restoring minimal runtime state (DB is source).")


async def on_startup(dp_obj):
    logger.info("Bot startup: init DB, start workers")
    await init_db()
    await restore_on_startup()
    loop = asyncio.get_event_loop()
    loop.create_task(deletion_worker())
    loop.create_task(start_webapp())
    logger.info("Startup complete.")

# ---------------- Run ----------------
if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)