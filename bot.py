# bot.py
"""
Telegram Upload + Deep-link Bot (aiogram 2.25.0)
- Uses async SQLAlchemy + asyncpg for PostgreSQL
- Owner-only upload sessions (1..95) -> single deep link per session
- /d prompts owner for protect_content then auto-delete minutes then returns deep link
- Files copied to UPLOAD_CHANNEL_ID; DB stores metadata only
- Auto-delete scheduler persists in DB and survives restarts
- /setstart copies replied message into upload channel and saves template for {first_name} and {word|url}
- /broadcast copies message to upload channel then copies to all users
- /stats shows totals
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
from aiogram.utils.exceptions import BadRequest  # aiogram 2.x exception

# SQLAlchemy async
from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, Boolean, DateTime, JSON, func, select
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# ----------------- CONFIG -----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL")
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))
PING_SECRET = os.getenv("PING_SECRET", "")
PORT = int(os.getenv("PORT", "8000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

if not TELEGRAM_TOKEN or OWNER_ID == 0 or not DATABASE_URL or UPLOAD_CHANNEL_ID == 0:
    raise SystemExit("Please set TELEGRAM_TOKEN, OWNER_ID, DATABASE_URL, UPLOAD_CHANNEL_ID env vars.")

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# ----------------- DATABASE MODELS -----------------
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
    session_id = Column(Integer, nullable=False)
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


# ----------------- ASYNC DB SETUP -----------------
engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True, future=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("DB initialized / tables ensured.")


# ----------------- BOT SETUP -----------------
bot = Bot(token=TELEGRAM_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)

# runtime caches / awaiting states for the /d flow
active_sessions_cache: Dict[int, int] = {}  # owner_id -> session_id
awaiting_protect_choice: Dict[int, int] = {}  # owner_id -> session_id
awaiting_autodel_choice: Dict[int, int] = {}
awaiting_confirmation: Dict[int, dict] = {}

# ----------------- UTILITIES -----------------
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
            inside = text[i + 1:j]
            if "|" in inside:
                word, url = inside.split("|", 1)
                word_esc = html.escape(word.strip())
                url_esc = html.escape(url.strip(), quote=True)
                out += f'<a href="{url_esc}">{word_esc}</a>'
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


# ----------------- DB HELPERS -----------------
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


async def finalize_session(owner_id: int, session_id: int, protect: bool, autodel_minutes: int) -> str:
    token = secrets.token_urlsafe(64)
    # ensure a reasonable minimum length
    while len(token) < 40:
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
    awaiting_confirmation.pop(owner_id, None)
    return token


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


# ----------------- DELETION WORKER -----------------
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
                        # maybe already deleted or permission error - mark as deleted
                        d.deleted = True
                        s.add(d)
                        logger.warning("Could not delete message %s in chat %s: %s", d.message_id, d.chat_id, e)
                await s.commit()
        except Exception as e:
            logger.exception("Deletion worker error: %s", e)
        await asyncio.sleep(30)


# ----------------- HTTP HEALTH -----------------
async def handle_health(request):
    if PING_SECRET:
        token = request.query.get("token", "")
        if token != PING_SECRET:
            return web.Response(text="unauthorized", status=401)
    return web.Response(text="ok")


async def start_webapp(app_host="0.0.0.0", app_port=PORT):
    app = web.Application()
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, app_host, app_port)
    await site.start()
    logger.info("HTTP health endpoint running on port %s", app_port)


# ----------------- COMMANDS & HANDLERS -----------------

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
            delivered = []
            max_autodel = 0
            for f in files:
                try:
                    # copy_message - protect_content supported in newer Telegram API; aiogram 2 passes kwargs to API
                    kwargs = {}
                    # only include protect_content param if attribute exists in method signature; we'll pass it and handle errors if not supported
                    kwargs["protect_content"] = bool(f.protected)
                    msg = await bot.copy_message(chat_id=message.chat.id,
                                                 from_chat_id=f.upload_channel_id,
                                                 message_id=f.upload_message_id,
                                                 **kwargs)
                    delivered.append((f, msg))
                    await asyncio.sleep(0.05)
                except Exception as e:
                    logger.exception("Failed to deliver file %s to %s: %s", f.id, message.chat.id, e)
            # record deliveries for auto-delete
            async with AsyncSessionLocal() as s2:
                for f, msg in delivered:
                    if f.auto_delete_minutes and f.auto_delete_minutes > 0:
                        delete_at = datetime.now(timezone.utc) + timedelta(minutes=int(f.auto_delete_minutes))
                        d = Delivery(file_id=f.id, chat_id=message.chat.id, message_id=msg.message_id, delete_at=delete_at)
                        s2.add(d)
                        max_autodel = max(max_autodel, f.auto_delete_minutes)
                await s2.commit()
            if max_autodel > 0:
                await message.reply(f"These files will be deleted in {max_autodel} minutes.")
            return

    # no args: show saved start
    cfg = await get_config("start_pointer")
    if cfg:
        try:
            channel_id_str, msg_id_str = cfg.split(":")
            c_id = int(channel_id_str)
            m_id = int(msg_id_str)
            start_template = await get_config("start_template") or ""
            txt = substitute_first_name(start_template, message.from_user.first_name)
            txt = parse_braces_links(txt)
            await bot.copy_message(chat_id=message.chat.id, from_chat_id=c_id, message_id=m_id, protect_content=False)
            if txt.strip():
                await bot.send_message(chat_id=message.chat.id, text=txt, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
            return
        except Exception as e:
            logger.exception("Error sending start pointer: %s", e)
            await message.reply("Welcome!")
            return
    await message.reply("Welcome! No start message configured.")


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Owner-only command.")
        return
    help_text = (
        "/upload - start upload session (owner only)\n"
        "/d - finalize session (owner only) -> prompts for protect + autodelete then returns deep link\n"
        "/e - abort session\n"
        "/setstart - reply to a message (text or media) to set start content\n"
        "/broadcast - reply to a message to broadcast to all users (owner only)\n"
        "/stats - show totals (owner only)\n"
        "/restart - restart bot (owner only)\n"
        "/health - health endpoint for uptime pings\n"
        "Use {first_name} in start template; use {word|url} to create clickable links."
    )
    await message.reply(help_text)


@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Owner-only.")
        return
    existing = await get_active_session(OWNER_ID)
    if existing:
        await message.reply(f"You already have an active session (id={existing}). Use /d to finalize or /e to abort.")
        return
    sid = await create_session(OWNER_ID)
    await message.reply(f"Upload session started (id={sid}). Send 1..95 files now. When done send /d. To abort send /e.")


@dp.message_handler(lambda m: m.from_user.id == OWNER_ID, content_types=types.ContentType.ANY)
async def owner_file_collector(message: types.Message):
    # skip commands (handled separately)
    if message.text and message.text.startswith("/"):
        return
    sid = await get_active_session(OWNER_ID)
    if not sid:
        return
    allowed = any([message.photo, message.document, message.video, message.animation, message.sticker, message.audio, message.voice])
    if not allowed:
        await message.reply("Send a supported file (photo, video, document, sticker, audio). To finish, send /d.")
        return
    # limit 95
    async with AsyncSessionLocal() as s:
        q = select(FileEntry).where(FileEntry.session_id == sid)
        res = await s.execute(q)
        existing = res.scalars().all()
        if len(existing) >= 95:
            await message.reply("Session reached maximum of 95 files. Send /d to finalize or /e to abort.")
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
        logger.exception("Failed to forward to upload channel: %s", e)
        await message.reply("Failed to save file to upload channel. Make sure the bot is admin in the upload channel.")


@dp.message_handler(commands=["d"])
async def cmd_finalize(message: types.Message):
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
            await message.reply("Session has no files. Send files first or /e to abort.")
            return
    awaiting_protect_choice[OWNER_ID] = sid
    awaiting_confirmation[OWNER_ID] = {"session_id": sid}
    await message.reply("Protect content? Reply with 'yes' or 'no' (this will set protect_content on delivering to users).")


@dp.message_handler(lambda m: m.from_user.id == OWNER_ID)
async def owner_finalize_flow(message: types.Message):
    # handle protect reply
    if OWNER_ID in awaiting_protect_choice and OWNER_ID not in awaiting_autodel_choice:
        txt = (message.text or "").strip().lower()
        if txt not in ("yes", "no"):
            await message.reply("Please reply 'yes' or 'no' for protect content.")
            return
        sid = awaiting_protect_choice[OWNER_ID]
        protect = txt == "yes"
        awaiting_confirmation[OWNER_ID]["protect"] = protect
        awaiting_autodel_choice[OWNER_ID] = sid
        awaiting_protect_choice.pop(OWNER_ID, None)
        await message.reply("Enter auto-delete time in minutes (0..10080). Enter 0 to disable auto-delete.")
        return
    # handle autodel reply
    if OWNER_ID in awaiting_autodel_choice:
        txt = (message.text or "").strip()
        if not re.fullmatch(r"\d{1,5}", txt):
            await message.reply("Please enter an integer number of minutes between 0 and 10080.")
            return
        minutes = int(txt)
        if minutes < 0 or minutes > 10080:
            await message.reply("Minutes must be between 0 and 10080.")
            return
        sid = awaiting_autodel_choice.pop(OWNER_ID, None)
        context = awaiting_confirmation.pop(OWNER_ID, {})
        protect = context.get("protect", True)
        token = await finalize_session(OWNER_ID, sid, protect=protect, autodel_minutes=minutes)
        bot_info = await bot.get_me()
        full_link = f"https://t.me/{bot_info.username}?start={token}"
        if minutes > 0:
            await bot.send_message(chat_id=OWNER_ID, text=f"Session finalized. Deep link:\n{full_link}\nThese files will be deleted in {minutes} minutes after delivery.")
        else:
            await bot.send_message(chat_id=OWNER_ID, text=f"Session finalized. Deep link:\n{full_link}")
        return
    # other owner messages ignored here


@dp.message_handler(commands=["e"])
async def cmd_abort(message: types.Message):
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
    awaiting_confirmation.pop(OWNER_ID, None)
    await message.reply("Session aborted. Uploaded copies remain in upload channel but are not linked.")


@dp.message_handler(commands=["setstart"])
async def cmd_setstart(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or media+caption) to set it as /start content.")
        return
    try:
        copied = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.reply_to_message.chat.id, message_id=message.reply_to_message.message_id)
        pointer = f"{UPLOAD_CHANNEL_ID}:{copied.message_id}"
        await set_config("start_pointer", pointer)
        template = message.reply_to_message.caption or message.reply_to_message.text or ""
        await set_config("start_template", template)
        await message.reply("Start content saved. /start will now send this content and template text with {first_name} substitution.")
    except Exception as e:
        logger.exception("Failed to set start: %s", e)
        await message.reply("Failed to set start content. Ensure bot is admin in upload channel.")


@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    target = message.reply_to_message
    if not target:
        await message.reply("Reply to a message (text or media) to broadcast.")
        return
    try:
        copied = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=target.chat.id, message_id=target.message_id)
        src_chat_id = UPLOAD_CHANNEL_ID
        src_msg_id = copied.message_id
    except Exception as e:
        logger.exception("Failed to copy broadcast message: %s", e)
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
            await bot.copy_message(chat_id=uid, from_chat_id=src_chat_id, message_id=src_msg_id, protect_content=True)
            sent += 1
            await asyncio.sleep(0.08)
        except Exception as e:
            logger.warning("Broadcast failed to %s: %s", uid, e)
            failed += 1
    await message.reply(f"Broadcast complete. Sent: {sent}, Failed: {failed}")


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


@dp.message_handler(commands=["restart"])
async def cmd_restart(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    await message.reply("Restarting now...")
    await bot.close()
    os._exit(0)


@dp.message_handler()
async def fallback_handler(message: types.Message):
    await upsert_user(message.from_user)
    # do nothing else for general users


# ----------------- STARTUP / RESTORE -----------------
async def restore_state_on_startup():
    # No heavy in-memory reconstruction needed; DB is source of truth.
    logger.info("Restoring runtime state (no heavy caches).")


async def on_startup(dp_obj):
    logger.info("Bot starting up...")
    await init_db()
    await restore_state_on_startup()
    loop = asyncio.get_event_loop()
    loop.create_task(deletion_worker())
    loop.create_task(start_webapp())
    logger.info("Startup complete.")


# ----------------- RUN -----------------
if __name__ == "__main__":
    # start polling with on_startup
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)