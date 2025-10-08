# bot.py
"""
Full deploy-ready Telegram bot implementing:
- Owner-only upload sessions -> deep links
- Alembic-aware startup (alembic upgrade head) + safe ALTER fallback
- /upload, /d, /e, /setstart, /broadcast, /revoke, /stats, /restart, /help, /health
- /start (public) and /start <token> to fetch session files
- Auto-delete delivered messages
- Robust DB commits and logging
- Designed for aiogram==2.25.1, SQLAlchemy async, asyncpg, Alembic
"""

import os
import re
import html
import secrets
import subprocess
import asyncio
import logging
import traceback
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote_plus

from aiohttp import web

from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor
from aiogram.utils.exceptions import BadRequest

from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, Boolean, DateTime, JSON, func, select, text
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# -----------------------
# Configuration via env vars
# -----------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL")  # e.g. postgresql+asyncpg://user:pass@host:5432/dbname
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))  # -100...
PING_SECRET = os.getenv("PING_SECRET", "")
PORT = int(os.getenv("PORT", "8000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DEEP_LINK_LENGTH = int(os.getenv("DEEP_LINK_LENGTH", "64"))  # default 64 chars

if not TELEGRAM_TOKEN or OWNER_ID == 0 or not DATABASE_URL or UPLOAD_CHANNEL_ID == 0:
    raise SystemExit("Missing required env vars: TELEGRAM_TOKEN, OWNER_ID, DATABASE_URL, UPLOAD_CHANNEL_ID")

# -----------------------
# Logging
# -----------------------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("deep_link_bot")

# -----------------------
# Database setup: models
# -----------------------
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
    token = Column(String(255), unique=True, nullable=True, index=True)
    header_message_id = Column(BigInteger, nullable=True)
    protect_content = Column(Boolean, default=True)
    auto_delete_minutes = Column(Integer, default=0)
    active = Column(Boolean, default=True)
    revoked = Column(Boolean, default=False)
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

# -----------------------
# Async engine & session factory
# -----------------------
engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True, future=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# -----------------------
# Bot & Dispatcher
# -----------------------
bot = Bot(token=TELEGRAM_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)

# -----------------------
# In-memory ephemeral state for owner flows
# -----------------------
active_sessions_cache: Dict[int, int] = {}      # owner_id -> session_id
awaiting_protect_choice: Dict[int, int] = {}    # owner_id -> session_id
awaiting_autodel_choice: Dict[int, int] = {}    # owner_id -> session_id
awaiting_context: Dict[int, dict] = {}          # owner_id -> context

# -----------------------
# Utilities: text parsing, keyboards
# -----------------------
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def gen_token(length: int = DEEP_LINK_LENGTH) -> str:
    # Ensure token_urlsafe produces sufficient length; clip to exact length
    nbytes = max(24, int(length * 3 / 4))
    token = secrets.token_urlsafe(nbytes)
    if len(token) < length:
        token = secrets.token_urlsafe(nbytes + 6)
    return token[:length]

def escape_html(text: Optional[str]) -> str:
    return "" if text is None else html.escape(text)

def substitute_first_name(template: Optional[str], first_name: Optional[str]) -> str:
    if not template:
        return ""
    return template.replace("{first_name}", html.escape(first_name or ""))

def parse_braces_links(text: Optional[str]) -> str:
    # Convert {word|url} -> <a href="url">word</a> safely
    if not text:
        return ""
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
                left, right = inside.split("|", 1)
                out += f'<a href="{html.escape(right.strip(), quote=True)}">{html.escape(left.strip())}</a>'
            else:
                out += html.escape("{" + inside + "}")
            i = j + 1
        else:
            out += html.escape(text[i])
            i += 1
    return out

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
    for r in j:
        buttons = []
        for b in r:
            if b.get("url"):
                buttons.append(InlineKeyboardButton(text=b.get("text", "link"), url=b.get("url")))
            elif b.get("callback_data"):
                buttons.append(InlineKeyboardButton(text=b.get("text", "btn"), callback_data=b.get("callback_data")))
            else:
                buttons.append(InlineKeyboardButton(text=b.get("text", "btn"), callback_data="noop"))
        kb.row(*buttons)
    return kb

# -----------------------
# Alembic helpers and fallback schema ensure
# -----------------------
def run_alembic_upgrade_head(cwd: Optional[str] = None) -> Tuple[bool, str]:
    """
    Run 'alembic upgrade head' via subprocess if Alembic and scripts present.
    Returns (success, output).
    """
    try:
        cmd = ["alembic", "upgrade", "head"]
        logger.info("Running Alembic: %s", " ".join(cmd))
        completed = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=120)
        output = completed.stdout + "\n" + completed.stderr
        if completed.returncode == 0:
            logger.info("Alembic upgrade succeeded.")
            return True, output
        else:
            logger.warning("Alembic upgrade returned non-zero code %s", completed.returncode)
            return False, output
    except FileNotFoundError as e:
        logger.warning("Alembic not installed (or not in PATH): %s", e)
        return False, str(e)
    except Exception as e:
        logger.exception("Error running alembic: %s", e)
        return False, str(e)

async def ensure_schema_fallback():
    # Apply safe ALTER TABLE IF NOT EXISTS statements for columns expected by code
    async with engine.begin() as conn:
        try:
            await conn.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS header_message_id BIGINT"))
            await conn.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS protect_content BOOLEAN DEFAULT TRUE"))
            await conn.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS auto_delete_minutes INTEGER DEFAULT 0"))
            await conn.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS revoked BOOLEAN DEFAULT FALSE"))
            await conn.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT TRUE"))
            logger.info("Applied fallback ALTER TABLE ensures.")
        except Exception:
            logger.exception("Fallback schema ensure failed.")

# -----------------------
# DB CRUD helpers
# -----------------------
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Created/ensured base tables (create_all).")

async def create_session(owner_id: int) -> int:
    async with AsyncSessionLocal() as session:
        new = SessionModel(owner_id=owner_id, active=True, revoked=False)
        session.add(new)
        await session.commit()
        await session.refresh(new)
        # post a header message in upload channel for grouping
        header_text = f"📦 New session #{new.id} started by owner."
        try:
            sent = await bot.send_message(chat_id=UPLOAD_CHANNEL_ID, text=header_text)
            new.header_message_id = sent.message_id
            session.add(new)
            await session.commit()
        except Exception:
            logger.exception("Failed to post header message in upload channel for session %s", new.id)
        active_sessions_cache[owner_id] = new.id
        logger.info("Created session id=%s owner=%s", new.id, owner_id)
        return new.id

async def get_active_session(owner_id: int) -> Optional[int]:
    sid = active_sessions_cache.get(owner_id)
    if sid:
        async with AsyncSessionLocal() as s:
            row = await s.get(SessionModel, sid)
            if row and row.active and not row.revoked:
                return sid
            else:
                active_sessions_cache.pop(owner_id, None)
                return None
    async with AsyncSessionLocal() as s:
        q = select(SessionModel).where(SessionModel.owner_id == owner_id, SessionModel.active == True, SessionModel.revoked == False).order_by(SessionModel.created_at.desc())
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
        logger.debug("Added file entry id=%s session=%s", fe.id, session_id)
        return fe.id

async def finalize_session(owner_id: int, session_id: int, protect: bool, autodel_minutes: int) -> str:
    token = gen_token(DEEP_LINK_LENGTH)
    async with AsyncSessionLocal() as s:
        row = await s.get(SessionModel, session_id)
        if not row:
            raise RuntimeError("session not found")
        row.token = token
        row.protect_content = protect
        row.auto_delete_minutes = autodel_minutes
        row.active = False
        s.add(row)
        q = select(FileEntry).where(FileEntry.session_id == session_id)
        res = await s.execute(q)
        files = res.scalars().all()
        for f in files:
            f.protected = protect
            f.auto_delete_minutes = autodel_minutes
            s.add(f)
        # update header message in upload channel to include deep link and file count
        file_count = len(files)
        try:
            bot_info = await bot.get_me()
            deep_link = f"https://t.me/{bot_info.username}?start={token}"
            header_text = f"📦 Session #{row.id}\nDeep link: {deep_link}\nFiles: {file_count}"
            if row.header_message_id:
                try:
                    await bot.edit_message_text(chat_id=UPLOAD_CHANNEL_ID, message_id=row.header_message_id, text=header_text)
                except Exception:
                    await bot.send_message(chat_id=UPLOAD_CHANNEL_ID, text=header_text)
            else:
                await bot.send_message(chat_id=UPLOAD_CHANNEL_ID, text=header_text)
        except Exception:
            logger.exception("Failed to update header for session %s", session_id)
        await s.commit()
    # clear ephemeral owner flow state
    active_sessions_cache.pop(owner_id, None)
    awaiting_protect_choice.pop(owner_id, None)
    awaiting_autodel_choice.pop(owner_id, None)
    awaiting_context.pop(owner_id, None)
    logger.info("Finalized session %s token=%s", session_id, token)
    return token

async def set_config(key: str, value: str):
    async with AsyncSessionLocal() as s:
        cfg = await s.get(Config, key)
        if cfg:
            cfg.value = value
            s.add(cfg)
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

# -----------------------
# Background deletion worker
# -----------------------
async def deletion_worker():
    logger.info("Deletion worker started (poll every 30s).")
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
                        logger.info("Deleted delivered message %s in chat %s", d.message_id, d.chat_id)
                    except BadRequest as e:
                        # mark as deleted to avoid endless retry
                        d.deleted = True
                        s.add(d)
                        logger.warning("Could not delete delivered message %s in chat %s: %s", d.message_id, d.chat_id, e)
                await s.commit()
        except Exception:
            logger.exception("Deletion worker error")
        await asyncio.sleep(30)

# -----------------------
# Health web endpoint
# -----------------------
async def handle_health(request):
    if PING_SECRET:
        token = request.query.get("token", "")
        if token != PING_SECRET:
            return web.Response(text="unauthorized", status=401)
    return web.Response(text="ok")

async def start_webapp(host: str = "0.0.0.0", port: int = PORT):
    app = web.Application()
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info("Health endpoint started on %s:%s", host, port)

# -----------------------
# Command handlers (all registered; owner-only checks inside)
# -----------------------

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    # store user record
    try:
        await upsert_user(message.from_user)
    except Exception:
        logger.exception("upsert_user failed")

    args = message.get_args().strip()
    if args:
        # decode in case URL encoded by Telegram client
        token = unquote_plus(args)
        async with AsyncSessionLocal() as s:
            q = select(SessionModel).where(SessionModel.token == token)
            res = await s.execute(q)
            sess = res.scalar_one_or_none()
            if not sess:
                await message.reply("Invalid or expired link.")
                return
            if sess.revoked or not sess.active:
                await message.reply("This link has been revoked or is inactive.")
                return
            # fetch files
            q2 = select(FileEntry).where(FileEntry.session_id == sess.id)
            res2 = await s.execute(q2)
            files = res2.scalars().all()
            if not files:
                await message.reply("No files found for this link.")
                return
            delivered_pairs: List[Tuple[FileEntry, types.Message]] = []
            max_autodel = 0
            for f in files:
                try:
                    try:
                        msg = await bot.copy_message(chat_id=message.chat.id,
                                                     from_chat_id=f.upload_channel_id,
                                                     message_id=f.upload_message_id,
                                                     protect_content=bool(f.protected))
                    except TypeError:
                        # older aiogram may not accept protect_content param
                        msg = await bot.copy_message(chat_id=message.chat.id,
                                                     from_chat_id=f.upload_channel_id,
                                                     message_id=f.upload_message_id)
                    delivered_pairs.append((f, msg))
                    await asyncio.sleep(0.03)
                except Exception:
                    logger.exception("Failed to copy file to user")
            # schedule deletes
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

    # no args -> show configured start pointer if set
    cfg = await get_config("start_pointer")
    if cfg:
        try:
            channel_id_str, msg_id_str = cfg.split(":")
            c_id = int(channel_id_str)
            m_id = int(msg_id_str)
            start_template = await get_config("start_template") or ""
            txt_with_name = substitute_first_name(start_template, message.from_user.first_name)
            html_text = parse_braces_links(txt_with_name)
            try:
                await bot.copy_message(chat_id=message.chat.id, from_chat_id=c_id, message_id=m_id, protect_content=False)
            except TypeError:
                await bot.copy_message(chat_id=message.chat.id, from_chat_id=c_id, message_id=m_id)
            if html_text.strip():
                await bot.send_message(chat_id=message.chat.id, text=html_text, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
            return
        except Exception:
            logger.exception("Error sending configured start")
            await message.reply("Welcome!")
            return
    await message.reply("Welcome! No start message configured.")

@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Owner-only command.")
        return
    help_msg = (
        "Owner commands:\n"
        "/upload - start upload session\n"
        "/d - finalize session (reply 'on'/'off' then minutes)\n"
        "/e - abort session\n"
        "/setstart - reply to a message to set /start content\n"
        "/broadcast - reply to a message to broadcast to all users\n"
        "/stats - show totals\n"
        "/revoke <token|link> - revoke a deep link\n"
        "/restart - restart the bot\n"
        "/health - HTTP endpoint\n\n"
        "Template usage: {first_name} and {word|https://example.com}"
    )
    await message.reply(help_msg)

@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Owner-only.")
        return
    existing = await get_active_session(OWNER_ID)
    if existing:
        await message.reply(f"You already have an active session (id={existing}). Use /d to finalize or /e to abort.")
        return
    sid = await create_session(OWNER_ID)
    await message.reply(f"Upload session started (id={sid}). Send 1..95 files. When done send /d. To abort send /e.")

# Collect owner files (explicit content types)
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID, content_types=[
    "photo", "document", "video", "animation", "sticker", "audio", "voice"
])
async def owner_file_collector(message: types.Message):
    sid = await get_active_session(OWNER_ID)
    if not sid:
        # ignore if no active session
        return
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
    except Exception:
        logger.exception("Failed to forward owner file to upload channel.")
        await message.reply("Failed to save file to upload channel. Ensure bot is admin there with post/edit rights.")

@dp.message_handler(commands=["d"])
async def cmd_d(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    sid = await get_active_session(OWNER_ID)
    if not sid:
        await message.reply("No active session.")
        return
    # ensure session has files
    async with AsyncSessionLocal() as s:
        q = select(FileEntry).where(FileEntry.session_id == sid)
        res = await s.execute(q)
        files = res.scalars().all()
        if not files:
            await message.reply("Session contains no files. Upload at least one before finalizing.")
            return
    awaiting_protect_choice[OWNER_ID] = sid
    awaiting_context[OWNER_ID] = {"session_id": sid}
    await message.reply("Protect content? Reply 'on' or 'off' (owner).")

# Owner finalize flow (protect -> autodel sequence)
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID, content_types=["text"])
async def owner_finalize_flow(message: types.Message):
    txt = (message.text or "").strip()
    if txt.startswith("/"):
        return
    # protect stage
    if OWNER_ID in awaiting_protect_choice and OWNER_ID not in awaiting_autodel_choice:
        low = txt.lower()
        if low not in ("on", "off"):
            await message.reply("Please reply 'on' or 'off'.")
            return
        sid = awaiting_protect_choice.pop(OWNER_ID)
        protect = (low == "on")
        awaiting_context[OWNER_ID]["protect"] = protect
        awaiting_autodel_choice[OWNER_ID] = sid
        await message.reply("Enter auto-delete time in minutes (0..10080). Send 0 to disable.")
        return
    # autodel stage
    if OWNER_ID in awaiting_autodel_choice:
        if not re.fullmatch(r"\d{1,5}", txt):
            await message.reply("Please send an integer minutes value between 0 and 10080.")
            return
        minutes = int(txt)
        if minutes < 0 or minutes > 10080:
            await message.reply("Minutes must be between 0 and 10080.")
            return
        sid = awaiting_autodel_choice.pop(OWNER_ID)
        context = awaiting_context.pop(OWNER_ID, {})
        protect = context.get("protect", True)
        try:
            token = await finalize_session(OWNER_ID, sid, protect=protect, autodel_minutes=minutes)
        except Exception:
            logger.exception("Failed finalizing session")
            await message.reply("Failed to finalize session due to internal error.")
            return
        bot_info = await bot.get_me()
        deep_link = f"https://t.me/{bot_info.username}?start={token}"
        if minutes > 0:
            await bot.send_message(chat_id=OWNER_ID, text=f"Session finalized. Deep link:\n{deep_link}\nThese files will be deleted in {minutes} minutes after delivery.")
        else:
            await bot.send_message(chat_id=OWNER_ID, text=f"Session finalized. Deep link:\n{deep_link}")
        return

@dp.message_handler(commands=["e"])
async def cmd_e(message: types.Message):
    if not is_owner(message.from_user.id):
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
    await message.reply("Session aborted. Uploaded copies remain in upload channel but are not linked.")

@dp.message_handler(commands=["setstart"])
async def cmd_setstart(message: types.Message):
    # Fixed: owner must reply to a message (media or text)
    if not is_owner(message.from_user.id):
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or media) to set /start content.")
        return
    try:
        # copy the replied message into UPLOAD_CHANNEL and store pointer and template
        copied = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.reply_to_message.chat.id, message_id=message.reply_to_message.message_id)
        pointer = f"{UPLOAD_CHANNEL_ID}:{copied.message_id}"
        await set_config("start_pointer", pointer)
        template = message.reply_to_message.caption or message.reply_to_message.text or ""
        await set_config("start_template", template)
        await message.reply("Start content saved. /start will now show this content and template text (supports {first_name} and {word|url}).")
    except Exception:
        logger.exception("Failed setstart")
        await message.reply("Failed to set start content. Make sure bot is admin in the upload channel (can post).")

@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    target = message.reply_to_message
    if not target:
        await message.reply("Reply to a message to broadcast it to all users.")
        return
    try:
        copied = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=target.chat.id, message_id=target.message_id)
        src_chat_id = UPLOAD_CHANNEL_ID
        src_msg_id = copied.message_id
    except Exception:
        logger.exception("Broadcast prepare failed")
        await message.reply("Failed to prepare broadcast (copy to upload channel).")
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
        except Exception:
            logger.warning("Broadcast failed to user %s", uid)
            failed += 1
    await message.reply(f"Broadcast complete. Sent: {sent}, Failed: {failed}")

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    two_days_ago = datetime.now(timezone.utc) - timedelta(days=2)
    async with AsyncSessionLocal() as s:
        total_users = (await s.execute(select(func.count()).select_from(User))).scalar()
        active_users = (await s.execute(select(func.count()).select_from(User).where(User.last_active >= two_days_ago))).scalar()
        total_files = (await s.execute(select(func.count()).select_from(FileEntry))).scalar()
        total_sessions = (await s.execute(select(func.count()).select_from(SessionModel))).scalar()
    await message.reply(f"Total users: {total_users}\nActive (2 days): {active_users}\nTotal files: {total_files}\nTotal sessions: {total_sessions}")

@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    arg = message.get_args().strip()
    if not arg:
        await message.reply("Usage: /revoke <token or full link>")
        return
    m = re.search(r"start=([A-Za-z0-9_\-]+)", arg)
    token = m.group(1) if m else arg
    async with AsyncSessionLocal() as s:
        q = select(SessionModel).where(SessionModel.token == token)
        res = await s.execute(q)
        sess = res.scalar_one_or_none()
        if not sess:
            await message.reply("Session not found for that token.")
            return
        sess.revoked = True
        sess.active = False
        s.add(sess)
        await s.commit()
        try:
            if sess.header_message_id:
                txt = f"📦 Session #{sess.id} — REVOKED by owner."
                await bot.edit_message_text(chat_id=UPLOAD_CHANNEL_ID, message_id=sess.header_message_id, text=txt)
        except Exception:
            pass
    await message.reply(f"Session #{sess.id} revoked. Deep link will no longer work.")

@dp.message_handler(commands=["restart"])
async def cmd_restart(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    await message.reply("Restarting now...")
    await bot.close()
    logger.info("Owner requested restart. Exiting.")
    os._exit(0)

@dp.message_handler(content_types=types.ContentType.ANY)
async def fallback_handler(message: types.Message):
    # avoid swallowing commands
    if message.text and message.text.startswith("/"):
        return
    try:
        await upsert_user(message.from_user)
    except Exception:
        logger.exception("upsert_user in fallback failed")

# -----------------------
# Startup routine: Alembic then fallback then workers
# -----------------------
async def startup_sequence():
    # create missing tables if not present
    logger.info("Running Base.metadata.create_all()")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # attempt alembic if present
    cwd = os.getcwd()
    if os.path.exists(os.path.join(cwd, "alembic.ini")) and os.path.isdir(os.path.join(cwd, "alembic")):
        success, output = run_alembic_upgrade_head(cwd=cwd)
        if not success:
            logger.warning("Alembic upgrade failed or not present; output:\n%s", output)
            logger.info("Falling back to safe schema ensures.")
            await ensure_schema_fallback()
    else:
        logger.info("No alembic found; applying fallback schema ensures.")
        await ensure_schema_fallback()

async def on_startup(dp_obj):
    logger.info("Bot startup - running migrations/schema ensures and background workers.")
    try:
        await startup_sequence()
    except Exception:
        logger.exception("Startup sequence failed.")
        # still proceed to start workers to keep bot responsive
    loop = asyncio.get_event_loop()
    loop.create_task(deletion_worker())
    loop.create_task(start_webapp())
    logger.info("Background workers started.")

if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)