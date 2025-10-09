# bot.py
"""
Deploy-ready Telegram Upload + Deep-link bot
- Aiogram v2.25.1
- PostgreSQL only (asyncpg)
- Alembic-aware (auto-run if present), fallback safe ALTERs
- Permanent deep links (revoked only via /revoke)
- Upload session isolation and ordering (seq)
- Flood-safe copying with RetryAfter handling
- All config via environment variables
"""

import os
import re
import html
import secrets
import subprocess
import asyncio
import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta
from urllib.parse import unquote_plus

from aiohttp import web

from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor
from aiogram.utils.exceptions import BadRequest, RetryAfter
import aiogram.utils.exceptions as ai_exc

from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, Boolean, DateTime, JSON, func, select, text
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# ----------------------------
# Environment / config
# ----------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL")  # must be postgresql+asyncpg://...
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))
PING_SECRET = os.getenv("PING_SECRET", "")
PORT = int(os.getenv("PORT", "8000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DEEP_LINK_LENGTH = int(os.getenv("DEEP_LINK_LENGTH", "48"))  # 32..64 recommended
AUTO_RUN_ALEMBIC = os.getenv("AUTO_RUN_ALEMBIC", "1")
MAX_FILES_PER_SESSION = int(os.getenv("MAX_FILES_PER_SESSION", "99"))
ALLOWED_UPLOAD_TYPES = os.getenv("ALLOWED_UPLOAD_TYPES", "photo,document,video,animation,sticker,audio,voice")

if not TELEGRAM_TOKEN:
    raise SystemExit("Missing TELEGRAM_TOKEN env var")
if OWNER_ID == 0:
    raise SystemExit("Missing OWNER_ID env var (numeric)")
if not DATABASE_URL:
    raise SystemExit("Missing DATABASE_URL env var")
if UPLOAD_CHANNEL_ID == 0:
    raise SystemExit("Missing UPLOAD_CHANNEL_ID env var")

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("upload_bot")

# ----------------------------
# Database models
# ----------------------------
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
    owner_id = Column(BigInteger, nullable=False, index=True)
    token = Column(String(255), unique=True, nullable=True, index=True)  # permanent token
    header_message_id = Column(BigInteger, nullable=True)
    protect_content = Column(Boolean, default=True)
    auto_delete_minutes = Column(Integer, default=0)
    active = Column(Boolean, default=True)   # True: usable; set False on abort/revoke
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
    seq = Column(Integer, nullable=False, default=0)  # ordering by upload
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

# ----------------------------
# Create async engine & sessionmaker
# ----------------------------
engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True, future=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# ----------------------------
# Aiogram init
# ----------------------------
bot = Bot(token=TELEGRAM_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)

# ----------------------------
# In-memory ephemeral state for owner interactive flows
# ----------------------------
active_sessions_cache: Dict[int, int] = {}  # owner_id -> session_id
awaiting_protect_choice: Dict[int, int] = {}
awaiting_autodel_choice: Dict[int, int] = {}
awaiting_context: Dict[int, dict] = {}

# ----------------------------
# Utility helpers
# ----------------------------
def is_owner(uid: int) -> bool:
    return uid == OWNER_ID

def gen_token(length: int = DEEP_LINK_LENGTH) -> str:
    n = max(24, int(length * 3 / 4))
    t = secrets.token_urlsafe(n)
    if len(t) < length:
        t = secrets.token_urlsafe(n + 6)
    return t[:length]

def escape_html(text: Optional[str]) -> str:
    return "" if text is None else html.escape(text)

def parse_braces_links(text: Optional[str]) -> str:
    """
    Convert {word|url} into safe <a href="url">word</a> with HTML escaping around other parts.
    """
    if not text:
        return ""
    out = []
    i = 0
    L = len(text)
    while i < L:
        if text[i] == "{":
            j = text.find("}", i)
            if j == -1:
                out.append(escape_html(text[i:]))
                break
            inside = text[i+1:j]
            if "|" in inside:
                left, right = inside.split("|", 1)
                out.append(f'<a href="{escape_html(right.strip())}">{escape_html(left.strip())}</a>')
            else:
                out.append(escape_html("{" + inside + "}"))
            i = j + 1
        else:
            out.append(escape_html(text[i]))
            i += 1
    return "".join(out)

def keyboard_to_json(kb: Optional[InlineKeyboardMarkup]) -> Optional[List[List[Dict[str, Any]]]]:
    if not kb:
        return None
    rows = []
    for r in kb.inline_keyboard:
        row = []
        for b in r:
            row.append({"text": b.text, "url": getattr(b, "url", None), "callback_data": getattr(b, "callback_data", None)})
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

# ----------------------------
# Alembic helper and fallback for first-run schema issues
# ----------------------------
def run_alembic_upgrade_head(cwd: Optional[str] = None) -> Tuple[bool, str]:
    try:
        cmd = ["alembic", "upgrade", "head"]
        logger.info("Running Alembic: %s", " ".join(cmd))
        completed = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=120)
        out = completed.stdout + "\n" + completed.stderr
        if completed.returncode == 0:
            logger.info("Alembic upgrade succeeded.")
            return True, out
        logger.warning("Alembic exited code %s", completed.returncode)
        return False, out
    except FileNotFoundError as e:
        logger.warning("Alembic not found: %s", e)
        return False, str(e)
    except Exception as e:
        logger.exception("Alembic run error: %s", e)
        return False, str(e)

async def ensure_schema_fallback():
    async with engine.begin() as conn:
        try:
            await conn.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS header_message_id BIGINT"))
            await conn.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS protect_content BOOLEAN DEFAULT TRUE"))
            await conn.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS auto_delete_minutes INTEGER DEFAULT 0"))
            await conn.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS revoked BOOLEAN DEFAULT FALSE"))
            await conn.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT TRUE"))
            await conn.execute(text("ALTER TABLE files ADD COLUMN IF NOT EXISTS seq INTEGER DEFAULT 0"))
            logger.info("Applied fallback ALTERs")
        except Exception:
            logger.exception("Fallback schema ensure failed")

# ----------------------------
# DB helpers
# ----------------------------
async def init_db_create_all():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("create_all executed")

async def create_session(owner_id: int) -> int:
    async with AsyncSessionLocal() as s:
        new = SessionModel(owner_id=owner_id, active=True, revoked=False)
        s.add(new)
        await s.commit()
        await s.refresh(new)
        header_text = f"📦 Upload session #{new.id} started by owner."
        try:
            sent = await bot.send_message(chat_id=UPLOAD_CHANNEL_ID, text=header_text)
            new.header_message_id = sent.message_id
            s.add(new)
            await s.commit()
        except Exception:
            logger.exception("Failed to post header in upload channel")
        active_sessions_cache[owner_id] = new.id
        logger.info("Created session %s for owner %s", new.id, owner_id)
        return new.id

async def get_active_session(owner_id: int) -> Optional[int]:
    sid = active_sessions_cache.get(owner_id)
    if sid:
        async with AsyncSessionLocal() as s:
            row = await s.get(SessionModel, sid)
            if row and row.active and not row.revoked:
                return sid
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
                         protected: bool, auto_delete_minutes: int, seq: int) -> int:
    async with AsyncSessionLocal() as s:
        fe = FileEntry(session_id=session_id,
                       upload_channel_id=upload_channel_id,
                       upload_message_id=upload_message_id,
                       caption=caption,
                       buttons=buttons,
                       protected=protected,
                       auto_delete_minutes=auto_delete_minutes,
                       seq=seq)
        s.add(fe)
        await s.commit()
        await s.refresh(fe)
        logger.debug("Added file entry id=%s seq=%s session=%s", fe.id, seq, session_id)
        return fe.id

async def finalize_session(owner_id: int, session_id: int, protect: bool, autodel_minutes: int) -> str:
    """
    Finalize: generate token, set protect/autodel on files, commit token before returning.
    IMPORTANT: do NOT set active = False here — keep active True so deep link remains usable.
    """
    token = gen_token(DEEP_LINK_LENGTH)
    async with AsyncSessionLocal() as s:
        row = await s.get(SessionModel, session_id)
        if not row:
            raise RuntimeError("session not found")
        row.token = token
        row.protect_content = protect
        row.auto_delete_minutes = autodel_minutes
        row.revoked = False
        s.add(row)
        q = select(FileEntry).where(FileEntry.session_id == session_id).order_by(FileEntry.seq.asc())
        res = await s.execute(q)
        files = res.scalars().all()
        for f in files:
            f.protected = protect
            f.auto_delete_minutes = autodel_minutes
            s.add(f)
        await s.commit()  # commit token and file updates before sending link
        # update header message with deep link and file count
        try:
            me = await bot.get_me()
            deep = f"https://t.me/{me.username}?start={token}"
            header_text = f"📦 Session #{row.id}\nDeep link:\n{deep}\nFiles: {len(files)}"
            if row.header_message_id:
                try:
                    await bot.edit_message_text(chat_id=UPLOAD_CHANNEL_ID, message_id=row.header_message_id, text=header_text)
                except Exception:
                    await bot.send_message(chat_id=UPLOAD_CHANNEL_ID, text=header_text)
            else:
                await bot.send_message(chat_id=UPLOAD_CHANNEL_ID, text=header_text)
        except Exception:
            logger.exception("Failed to update header after finalize")
    # clear ephemeral owner state
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

# ----------------------------
# Safe copy wrapper (handles RetryAfter & retries)
# ----------------------------
async def safe_copy_message(from_chat_id: int, message_id: int, to_chat_id: int, protect_content: Optional[bool] = None) -> types.Message:
    attempt = 0
    while True:
        try:
            if protect_content is not None:
                try:
                    return await bot.copy_message(chat_id=to_chat_id, from_chat_id=from_chat_id, message_id=message_id, protect_content=bool(protect_content))
                except TypeError:
                    return await bot.copy_message(chat_id=to_chat_id, from_chat_id=from_chat_id, message_id=message_id)
            else:
                return await bot.copy_message(chat_id=to_chat_id, from_chat_id=from_chat_id, message_id=message_id)
        except RetryAfter as e:
            attempt += 1
            wait = e.timeout + 1
            logger.warning("RetryAfter while copying message %s from %s to %s. Sleeping %s seconds (attempt %s)", message_id, from_chat_id, to_chat_id, wait, attempt)
            await asyncio.sleep(wait)
            continue
        except BadRequest as e:
            logger.warning("BadRequest copying message %s from %s to %s: %s", message_id, from_chat_id, to_chat_id, e)
            # bubble up BadRequest so caller can decide (message deleted / cannot be copied)
            raise
        except Exception as e:
            attempt += 1
            if attempt > 4:
                logger.exception("Failed copying message after attempts: %s", e)
                raise
            backoff = min(5 * attempt, 10)
            logger.warning("Unexpected error copying message, retry in %s seconds (attempt %s): %s", backoff, attempt, e)
            await asyncio.sleep(backoff)
            continue

# ----------------------------
# Deletion worker: removes delivered messages when their delete_at is reached
# ----------------------------
async def deletion_worker():
    logger.info("Deletion worker started.")
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
                        d.deleted = True
                        s.add(d)
                        logger.warning("Could not delete delivered message %s: %s", d.message_id, e)
                await s.commit()
        except Exception:
            logger.exception("Deletion worker error")
        await asyncio.sleep(30)

# ----------------------------
# Health endpoint for uptime monitors
# ----------------------------
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

# ----------------------------
# Handlers: /start, /upload, /d, /e, /setstart, /broadcast, /revoke, /stats, /help, /restart
# ----------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    try:
        await upsert_user(message.from_user)
    except Exception:
        logger.exception("upsert_user failed in /start")

    args = message.get_args().strip()
    if args:
        token = unquote_plus(args).strip()
        async with AsyncSessionLocal() as s:
            q = select(SessionModel).where(SessionModel.token == token)
            res = await s.execute(q)
            sess = res.scalar_one_or_none()
            if not sess:
                await message.reply("This link is invalid or expired.")
                return
            if sess.revoked or not sess.active:
                await message.reply("This link is invalid or expired.")
                return
            # fetch files for that session ordered by seq
            q2 = select(FileEntry).where(FileEntry.session_id == sess.id).order_by(FileEntry.seq.asc(), FileEntry.id.asc())
            res2 = await s.execute(q2)
            files = res2.scalars().all()
            if not files:
                await message.reply("No files found for this link.")
                return
            delivered_pairs: List[Tuple[FileEntry, types.Message]] = []
            max_autodel = 0
            for f in files:
                try:
                    # owner bypass: if requester is owner, protect_content=False to allow full control
                    protect = False if message.from_user and message.from_user.id == OWNER_ID else f.protected
                    msg = await safe_copy_message(from_chat_id=f.upload_channel_id, message_id=f.upload_message_id, to_chat_id=message.chat.id, protect_content=protect)
                    delivered_pairs.append((f, msg))
                    # small delay to avoid flood
                    await asyncio.sleep(0.05)
                except BadRequest as e:
                    logger.warning("BadRequest copying file entry %s: %s", f.id, e)
                except Exception:
                    logger.exception("Failed to deliver file entry %s", f.id)
            # schedule deletions
            async with AsyncSessionLocal() as s2:
                for f, msg in delivered_pairs:
                    if f.auto_delete_minutes and f.auto_delete_minutes > 0:
                        delete_at = datetime.now(timezone.utc) + timedelta(minutes=int(f.auto_delete_minutes))
                        d = Delivery(file_id=f.id, chat_id=message.chat.id, message_id=msg.message_id, delete_at=delete_at)
                        s2.add(d)
                        max_autodel = max(max_autodel, f.auto_delete_minutes)
                await s2.commit()
            # send final notification if autodelete set
            if max_autodel > 0:
                # safe text, avoid parse errors
                await message.reply(f"These files will be deleted in {max_autodel} minutes.")
            return

    # no args => show configured start message if any
    cfg = await get_config("start_pointer")
    if cfg:
        try:
            channel_id_str, msg_id_str = cfg.split(":")
            c_id = int(channel_id_str)
            m_id = int(msg_id_str)
            start_template = await get_config("start_template") or ""
            txt_with_name = start_template.replace("{first_name}", escape_html(message.from_user.first_name or ""))
            html_text = parse_braces_links(txt_with_name)
            try:
                await safe_copy_message(from_chat_id=c_id, message_id=m_id, to_chat_id=message.chat.id, protect_content=False)
            except Exception:
                logger.exception("Failed to copy start pointer")
            if html_text.strip():
                await bot.send_message(chat_id=message.chat.id, text=html_text, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
            return
        except Exception:
            logger.exception("sending start pointer failed")
            await message.reply("Welcome!")
            return
    await message.reply("Welcome! No start message configured.")

@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Owner-only command.")
        return
    help_text = (
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
        "Templates: use {first_name} and {word|https://example.com} inside captions/start."
    )
    await message.reply(escape_html(help_text))

@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Owner-only.")
        return
    existing = await get_active_session(OWNER_ID)
    if existing:
        await message.reply(f"Active session exists (id={existing}). Use /d to finalize or /e to abort.")
        return
    sid = await create_session(OWNER_ID)
    await message.reply(f"Upload session started (id={sid}). Send 1..{MAX_FILES_PER_SESSION} files. When done send /d. To abort send /e.")

# Owner media collector
allowed_types = [t.strip() for t in ALLOWED_UPLOAD_TYPES.split(",") if t.strip()]
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID, content_types=allowed_types)
async def owner_file_collector(message: types.Message):
    sid = await get_active_session(OWNER_ID)
    if not sid:
        return
    # count existing
    async with AsyncSessionLocal() as s:
        q = select(FileEntry).where(FileEntry.session_id == sid)
        res = await s.execute(q)
        existing = res.scalars().all()
        if len(existing) >= MAX_FILES_PER_SESSION:
            await message.reply(f"Session reached {MAX_FILES_PER_SESSION} files (max). Use /d to finalize or /e to abort.")
            return
        seq = len(existing) + 1
    caption_text = message.caption or message.text or None
    buttons_json = None
    if message.reply_markup:
        try:
            buttons_json = keyboard_to_json(message.reply_markup)
        except Exception:
            buttons_json = None
    # copy to upload channel safely
    try:
        copied = await safe_copy_message(from_chat_id=message.chat.id, message_id=message.message_id, to_chat_id=UPLOAD_CHANNEL_ID, protect_content=False)
    except BadRequest as e:
        logger.exception("BadRequest copying owner media to upload channel: %s", e)
        await message.reply("Failed to copy to upload channel (BadRequest). Ensure bot has permission.")
        return
    except Exception:
        logger.exception("Failed to copy owner media to upload channel")
        await message.reply("Failed to copy to upload channel. Ensure bot is admin there and not rate-limited.")
        return
    try:
        fid = await add_file_entry(session_id=sid,
                                   upload_channel_id=UPLOAD_CHANNEL_ID,
                                   upload_message_id=copied.message_id,
                                   caption=caption_text,
                                   buttons=buttons_json,
                                   protected=True,
                                   auto_delete_minutes=0,
                                   seq=seq)
        await message.reply(f"Saved file to upload channel (entry id={fid}).")
    except Exception:
        logger.exception("Failed to add file entry to DB")
        await message.reply("Failed to save file metadata; copy exists in upload channel but DB entry failed.")

@dp.message_handler(commands=["d"])
async def cmd_d(message: types.Message):
    if not is_owner(message.from_user.id):
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
            await message.reply("Session contains no files. Upload at least one before finalizing.")
            return
    awaiting_protect_choice[OWNER_ID] = sid
    awaiting_context[OWNER_ID] = {"session_id": sid}
    await message.reply("Protect content? Reply 'on' or 'off'.")

@dp.message_handler(lambda m: m.from_user.id == OWNER_ID, content_types=["text"])
async def owner_finalize_flow(message: types.Message):
    txt = (message.text or "").strip()
    if txt.startswith("/"):
        return
    # protect stage
    if OWNER_ID in awaiting_protect_choice and OWNER_ID not in awaiting_autodel_choice:
        low = txt.lower()
        if low not in ("on", "off"):
            await message.reply("Please reply 'on' or 'off' for protect content.")
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
            logger.exception("Finalize error")
            await message.reply("Failed to finalize session due to internal error.")
            return
        me = await bot.get_me()
        deep_link = f"https://t.me/{me.username}?start={token}"
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
            row.revoked = True
            s.add(row)
            await s.commit()
    active_sessions_cache.pop(OWNER_ID, None)
    awaiting_protect_choice.pop(OWNER_ID, None)
    awaiting_autodel_choice.pop(OWNER_ID, None)
    awaiting_context.pop(OWNER_ID, None)
    await message.reply("Session aborted. Uploaded copies remain in upload channel but are not linked.")

@dp.message_handler(commands=["setstart"])
async def cmd_setstart(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Owner-only.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or media) to set /start content.")
        return
    try:
        copied = await safe_copy_message(from_chat_id=message.reply_to_message.chat.id, message_id=message.reply_to_message.message_id, to_chat_id=UPLOAD_CHANNEL_ID, protect_content=False)
    except Exception:
        logger.exception("Failed to copy start pointer to upload channel")
        await message.reply("Failed to set start content. Ensure bot can post in upload channel.")
        return
    pointer = f"{UPLOAD_CHANNEL_ID}:{copied.message_id}"
    template = message.reply_to_message.caption or message.reply_to_message.text or ""
    await set_config("start_pointer", pointer)
    await set_config("start_template", template)
    await message.reply("Start content saved. /start will show this content and template.")

@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    target = message.reply_to_message
    if not target:
        await message.reply("Reply to a message to broadcast.")
        return
    try:
        copied = await safe_copy_message(from_chat_id=target.chat.id, message_id=target.message_id, to_chat_id=UPLOAD_CHANNEL_ID, protect_content=False)
    except Exception:
        logger.exception("Broadcast prep failed")
        await message.reply("Failed to prepare broadcast.")
        return
    src_chat_id = UPLOAD_CHANNEL_ID
    src_msg_id = copied.message_id
    async with AsyncSessionLocal() as s:
        q = select(User.tg_id)
        res = await s.execute(q)
        rows = [r[0] for r in res.fetchall()]
    sent, failed = 0, 0
    for uid in rows:
        try:
            try:
                await bot.copy_message(chat_id=uid, from_chat_id=src_chat_id, message_id=src_msg_id, protect_content=True)
            except TypeError:
                await bot.copy_message(chat_id=uid, from_chat_id=src_chat_id, message_id=src_msg_id)
            sent += 1
            await asyncio.sleep(0.08)
        except Exception:
            logger.warning("Broadcast failed for %s", uid)
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
        active_sessions = (await s.execute(select(func.count()).select_from(SessionModel).where(SessionModel.active == True, SessionModel.revoked == False))).scalar()
    await message.reply(f"Total users: {total_users}\nActive (2 days): {active_users}\nTotal files: {total_files}\nTotal sessions: {total_sessions}\nActive sessions: {active_sessions}")

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
    await message.reply(f"Session #{sess.id} revoked. The link will no longer work.")

@dp.message_handler(commands=["restart"])
async def cmd_restart(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    await message.reply("Restarting now...")
    await bot.close()
    logger.info("Owner requested restart; exiting.")
    os._exit(0)

@dp.message_handler(content_types=types.ContentType.ANY)
async def fallback(message: types.Message):
    if message.text and message.text.startswith("/"):
        return
    try:
        await upsert_user(message.from_user)
    except Exception:
        logger.exception("upsert_user fallback failed")

# ----------------------------
# Startup sequence: alembic or fallback ensures + background tasks
# ----------------------------
async def startup_sequence():
    cwd = os.getcwd()
    alembic_ini = os.path.join(cwd, "alembic.ini")
    alembic_dir = os.path.join(cwd, "alembic")
    if AUTO_RUN_ALEMBIC == "1" and os.path.exists(alembic_ini) and os.path.isdir(alembic_dir):
        success, out = run_alembic_upgrade_head(cwd=cwd)
        if not success:
            logger.warning("Alembic upgrade failed; fallback ensures will run. Output:\n%s", out)
            await ensure_schema_fallback()
    else:
        logger.info("Alembic not run; creating tables and applying fallback ensures.")
        await init_db_create_all()
        await ensure_schema_fallback()

async def on_startup(dp_obj):
    logger.info("Startup: deleting webhook and running migrations/ensures.")
    try:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception as e:
            logger.warning("delete_webhook failed: %s", e)
        await startup_sequence()
    except Exception:
        logger.exception("startup_sequence failed")
    loop = asyncio.get_event_loop()
    loop.create_task(deletion_worker())
    loop.create_task(start_webapp())
    logger.info("Startup complete")

# Ensure webhook deletion before polling to avoid TerminatedByOtherGetUpdates
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(bot.delete_webhook(drop_pending_updates=True))
    except Exception as e:
        logger.warning("delete_webhook on __main__ failed: %s", e)
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)