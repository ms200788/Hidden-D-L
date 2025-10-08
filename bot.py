# bot.py
"""
Verbose, deploy-ready Telegram Upload + Deep-link Bot
- Targeted for aiogram==2.25.0 with SQLAlchemy async (1.4.x) and asyncpg
- Designed for Render / PostgreSQL persistent storage
- Features:
  * Owner-only upload sessions (1..95 files) -> single deep link per session
  * /d interactive finalize flow (owner replies 'on'/'off' then minutes)
  * Files copied to an UPLOAD_CHANNEL_ID (private channel) — bot stores metadata only
  * Session header message in upload channel (shows deep link after finalize and file count)
  * Deep-link tokens are short (18..24 chars) to avoid Telegram truncation
  * /revoke <token|link> to mark a session revoked (inactive)
  * /setstart to set welcome content (supports {first_name} and {word|url})
  * /broadcast to send to all known users
  * /stats, /help, /restart (owner-only)
  * /health HTTP endpoint for UptimeRobot
  * Auto-migration for new columns used by code, to avoid runtime errors
"""

import os
import asyncio
import logging
import secrets
import html
import re
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

# aiogram (2.x)
from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode, InlineKeyboardMarkup
from aiogram.utils import executor
from aiogram.utils.exceptions import BadRequest

# SQLAlchemy async
from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, Boolean, DateTime, JSON, func, select, text
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker

# -----------------------
# Configuration (ENV VARS)
# -----------------------
# Required environment variables (set in Render or your host):
# TELEGRAM_TOKEN, OWNER_ID, DATABASE_URL, UPLOAD_CHANNEL_ID
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")        # Bot token
OWNER_ID = int(os.getenv("OWNER_ID", "0"))         # Bot owner's Telegram user id (numeric)
DATABASE_URL = os.getenv("DATABASE_URL")           # must be like: postgresql+asyncpg://user:pass@host:5432/dbname
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))  # channel id where uploads get copied (e.g. -1001234567890)
PING_SECRET = os.getenv("PING_SECRET", "")         # optional token for /health
PORT = int(os.getenv("PORT", "8000"))              # Render sets this automatically
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Validate config at process start
if not TELEGRAM_TOKEN or OWNER_ID == 0 or not DATABASE_URL or UPLOAD_CHANNEL_ID == 0:
    raise SystemExit("Set TELEGRAM_TOKEN, OWNER_ID, DATABASE_URL, and UPLOAD_CHANNEL_ID environment variables before starting.")

# -----------------------
# Logging config
# -----------------------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("upload_bot")

# -----------------------
# SQLAlchemy models
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
    """
    sessions table:
      - owner_id: the uploader (owner) who created the session
      - token: the short deep-link token (set when finalized)
      - header_message_id: message id in upload channel where header is posted
      - active: true while session is editable; set false on finalize or abort
      - revoked: set true by /revoke (makes deep link non-functional)
    """
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True)
    owner_id = Column(BigInteger, nullable=False)
    token = Column(String(255), unique=True, nullable=True)
    header_message_id = Column(BigInteger, nullable=True)
    active = Column(Boolean, default=True)
    revoked = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class FileEntry(Base):
    """
    files:
      stores references to files copied in upload channel
    """
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
    """
    deliveries:
      tracks messages the bot delivered to users so the deletion worker can remove them later
    """
    __tablename__ = "deliveries"
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, nullable=False)
    chat_id = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    delete_at = Column(DateTime(timezone=True), nullable=True)
    deleted = Column(Boolean, default=False)
    delivered_at = Column(DateTime(timezone=True), server_default=func.now())


class Config(Base):
    """key/value config table for storing start_pointer and start_template"""
    __tablename__ = "config"
    key = Column(String(128), primary_key=True)
    value = Column(Text)


# -----------------------
# Async DB engine & session factory
# -----------------------
# Use `future=True` for SQLAlchemy 1.4 style; ensure pool_pre_ping to avoid stale connections
engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True, future=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# -----------------------
# Bot setup
# -----------------------
bot = Bot(token=TELEGRAM_TOKEN, parse_mode=ParseMode.HTML)  # we will send HTML-formatted messages where needed
dp = Dispatcher(bot)

# -----------------------
# Runtime in-memory state for owner interactive flow
# (these are ephemeral and re-created after restarts)
# -----------------------
active_sessions_cache: Dict[int, int] = {}            # owner_id -> session_id (active)
awaiting_protect_choice: Dict[int, int] = {}          # owner_id -> session_id waiting for protect reply
awaiting_autodel_choice: Dict[int, int] = {}          # owner_id -> session_id waiting for autodel minutes
awaiting_context: Dict[int, dict] = {}                # owner_id -> context dict {'session_id':..., 'protect':...}

# -----------------------
# Utilities: parsing and keyboard helpers
# -----------------------
def parse_braces_links(text: Optional[str]) -> str:
    """
    Convert occurrences of {word|url} into HTML <a href="url">word</a>.
    If text is None, returns empty string.
    This function escapes other content to avoid HTML parsing errors.
    """
    if not text:
        return ""
    out = ""
    i = 0
    while i < len(text):
        if text[i] == "{":
            j = text.find("}", i)
            if j == -1:
                # unmatched brace -> escape the rest
                out += html.escape(text[i:])
                break
            inside = text[i+1:j]
            if "|" in inside:
                word, url = inside.split("|", 1)
                # escape the pieces properly
                word_esc = html.escape(word.strip())
                url_esc = html.escape(url.strip(), quote=True)
                out += f'<a href="{url_esc}">{word_esc}</a>'
            else:
                # not a link pattern, escape literally
                out += html.escape("{" + inside + "}")
            i = j + 1
        else:
            out += html.escape(text[i])
            i += 1
    return out


def substitute_first_name(text: Optional[str], first_name: Optional[str]) -> str:
    """
    Replace {first_name} placeholder with user's first name. The replacement is HTML-escaped.
    Uses parse_braces_links after substitution by caller if needed.
    """
    if not text:
        return ""
    return text.replace("{first_name}", html.escape(first_name or ""))


def keyboard_to_json(kb: Optional[InlineKeyboardMarkup]) -> Optional[List[List[Dict[str, Any]]]]:
    """
    Convert InlineKeyboardMarkup to a JSON-serializable structure for storing in DB.
    """
    if kb is None:
        return None
    rows = []
    for r in kb.inline_keyboard:
        row = []
        for btn in r:
            row.append({"text": btn.text, "url": getattr(btn, "url", None), "callback_data": getattr(btn, "callback_data", None)})
        rows.append(row)
    return rows


def json_to_keyboard(j: Optional[List[List[Dict[str, Any]]]]) -> Optional[InlineKeyboardMarkup]:
    """
    Convert stored JSON keyboard representation back to InlineKeyboardMarkup.
    """
    if not j:
        return None
    kb = InlineKeyboardMarkup()
    for r in j:
        buttons = []
        for b in r:
            if b.get("url"):
                buttons.append(types.InlineKeyboardButton(text=b.get("text", "link"), url=b.get("url")))
            elif b.get("callback_data"):
                buttons.append(types.InlineKeyboardButton(text=b.get("text", "btn"), callback_data=b.get("callback_data")))
            else:
                buttons.append(types.InlineKeyboardButton(text=b.get("text", "btn"), callback_data="noop"))
        kb.row(*buttons)
    return kb


# -----------------------
# DB helpers: create / read / write functions
# -----------------------
async def init_db():
    """
    Create tables if they don't exist. Called on startup.
    """
    # create all tables from models
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables ensured.")


async def ensure_schema_columns():
    """
    Some older deployments may lack columns we added later.
    This function attempts to ALTER TABLE IF NOT EXISTS for columns we rely on.
    Called at startup to avoid ProgrammingError crashes.
    """
    async with engine.begin() as conn:
        # Use raw SQL ALTER TABLE IF NOT EXISTS (Postgres supports IF NOT EXISTS for columns)
        try:
            await conn.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS header_message_id BIGINT"))
            await conn.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS revoked BOOLEAN DEFAULT FALSE"))
            await conn.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT TRUE"))
            # If other columns are added later, add them here similarly.
            logger.info("Schema migration (if needed) applied.")
        except Exception as e:
            # Log but continue; if this fails something else might be wrong.
            logger.exception("Error while ensuring schema columns: %s", e)


async def create_session(owner_id: int) -> int:
    """
    Create a new session row and post a header message in upload channel.
    Returns the new session id.
    """
    async with AsyncSessionLocal() as s:
        new = SessionModel(owner_id=owner_id, active=True, revoked=False)
        s.add(new)
        await s.commit()
        await s.refresh(new)
        # Post header message into upload channel for this session
        header_text = f"📦 New session #{new.id} (owner: {owner_id}) started.\nSend files into this session (1..95)."
        try:
            sent = await bot.send_message(chat_id=UPLOAD_CHANNEL_ID, text=header_text)
            new.header_message_id = sent.message_id
            s.add(new)
            await s.commit()
        except Exception as e:
            logger.exception("Failed to post header into upload channel for session %s: %s", new.id, e)
        active_sessions_cache[owner_id] = new.id
        logger.info("Created session %s for owner %s", new.id, owner_id)
        return new.id


async def get_active_session(owner_id: int) -> Optional[int]:
    """
    Return an active session id for the owner if present (cached or DB).
    Returns None if none found or revoked.
    """
    sid = active_sessions_cache.get(owner_id)
    if sid:
        async with AsyncSessionLocal() as s:
            row = await s.get(SessionModel, sid)
            if row and row.active and not row.revoked:
                return sid
            else:
                active_sessions_cache.pop(owner_id, None)
                return None
    # fallback to DB
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
    """
    Insert a new FileEntry referencing the copied message in the upload channel.
    """
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
        logger.debug("Added file entry %s for session %s (message %s in channel %s)", fe.id, session_id, upload_message_id, upload_channel_id)
        return fe.id


def make_short_token() -> str:
    """
    Generate a short token between 18 and 24 characters that is safe for Telegram deep links.
    Uses token_urlsafe which outputs base64url-like characters.
    """
    t = secrets.token_urlsafe(16)  # ~22 chars typically
    if len(t) < 18:
        t = secrets.token_urlsafe(17)
    if len(t) > 24:
        t = t[:24]
    return t


async def finalize_session(owner_id: int, session_id: int, protect: bool, autodel_minutes: int) -> str:
    """
    Finalize the session: set token, mark inactive, apply protect/autodel to all files,
    and edit/post header message in upload channel to display deep link and file count.
    Returns the generated token (string).
    """
    token = make_short_token()
    async with AsyncSessionLocal() as s:
        row = await s.get(SessionModel, session_id)
        if not row:
            raise RuntimeError("session not found")
        row.token = token
        row.active = False
        s.add(row)
        # update file entries for this session
        q = select(FileEntry).where(FileEntry.session_id == session_id)
        res = await s.execute(q)
        files = res.scalars().all()
        for f in files:
            f.protected = protect
            f.auto_delete_minutes = autodel_minutes
            s.add(f)
        # update header message in upload channel to show deep link and file count
        file_count = len(files)
        try:
            bot_info = await bot.get_me()
            deep_link = f"https://t.me/{bot_info.username}?start={token}"
            header_text = f"📦 Session #{row.id}\nDeep link: {deep_link}\nFiles: {file_count}"
            if row.header_message_id:
                try:
                    # edit original header message (best: keep uploads grouped under it)
                    await bot.edit_message_text(chat_id=UPLOAD_CHANNEL_ID, message_id=row.header_message_id, text=header_text)
                except Exception:
                    # if edit fails (e.g., missing rights), send a new message as fallback
                    await bot.send_message(chat_id=UPLOAD_CHANNEL_ID, text=header_text)
            else:
                await bot.send_message(chat_id=UPLOAD_CHANNEL_ID, text=header_text)
        except Exception as ex:
            logger.exception("Failed to update header message for session %s: %s", session_id, ex)
        await s.commit()
    # clear runtime ephemeral flow state
    active_sessions_cache.pop(owner_id, None)
    awaiting_protect_choice.pop(owner_id, None)
    awaiting_autodel_choice.pop(owner_id, None)
    awaiting_context.pop(owner_id, None)
    logger.info("Finalized session %s with token %s", session_id, token)
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
    """
    Insert or update a user record (updates last_active timestamp).
    """
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
# Deletion worker (background task)
# -----------------------
async def deletion_worker():
    """
    Background loop that periodically checks for deliveries due to delete and deletes them.
    Runs forever until process exits.
    """
    logger.info("Starting deletion worker; will poll every 30 seconds.")
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
                        logger.info("Deleted message %s in chat %s (delivery id %s)", d.message_id, d.chat_id, d.id)
                    except BadRequest as e:
                        # If we cannot delete (for example due to permissions), mark it deleted to avoid retry loop
                        d.deleted = True
                        s.add(d)
                        logger.warning("Could not delete message %s in chat %s: %s", d.message_id, d.chat_id, e)
                await s.commit()
        except Exception as e:
            logger.exception("Deletion worker encountered exception: %s", e)
        await asyncio.sleep(30)


# -----------------------
# Health HTTP endpoint for uptime pings
# -----------------------
from aiohttp import web

async def handle_health(request):
    if PING_SECRET:
        token = request.query.get("token", "")
        if token != PING_SECRET:
            return web.Response(text="unauthorized", status=401)
    return web.Response(text="ok")

async def start_webapp(app_host="0.0.0.0", app_port: int = None):
    """
    Start an aiohttp web server to serve /health. This runs in background.
    """
    if app_port is None:
        app_port = PORT
    app = web.Application()
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, app_host, app_port)
    await site.start()
    logger.info("HTTP health endpoint listening on %s:%s", app_host, app_port)


# -----------------------
# Handler utilities (owner-only check)
# -----------------------
def is_owner(message: types.Message) -> bool:
    return message.from_user and message.from_user.id == OWNER_ID


# -----------------------
# Command Handlers
# -----------------------

# /start handler: no-args shows start content; with args treat as deep-link token and deliver session files
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    # record user activity
    await upsert_user(message.from_user)

    args = message.get_args().strip()
    if args:
        # treat args as token
        token = args
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
            # fetch files for this session
            q2 = select(FileEntry).where(FileEntry.session_id == sess.id)
            res2 = await s.execute(q2)
            files = res2.scalars().all()
            if not files:
                await message.reply("No files found for this link.")
                return
            delivered = []
            max_autodel = 0
            # iterate and copy each file from upload channel into user's chat
            for f in files:
                try:
                    # try using protect_content param (some aiogram versions allow it)
                    try:
                        msg = await bot.copy_message(chat_id=message.chat.id,
                                                     from_chat_id=f.upload_channel_id,
                                                     message_id=f.upload_message_id,
                                                     protect_content=bool(f.protected))
                    except TypeError:
                        # fallback if protect_content is not accepted
                        msg = await bot.copy_message(chat_id=message.chat.id,
                                                     from_chat_id=f.upload_channel_id,
                                                     message_id=f.upload_message_id)
                    delivered.append((f, msg))
                    await asyncio.sleep(0.05)
                except Exception as e:
                    logger.exception("Failed delivering file id %s to %s: %s", getattr(f, "id", "?"), message.chat.id, e)
            # schedule deletions for messages if needed
            async with AsyncSessionLocal() as s2:
                for f, msg in delivered:
                    if f.auto_delete_minutes and f.auto_delete_minutes > 0:
                        delete_at = datetime.now(timezone.utc) + timedelta(minutes=int(f.auto_delete_minutes))
                        d = Delivery(file_id=f.id, chat_id=message.chat.id, message_id=msg.message_id, delete_at=delete_at)
                        s2.add(d)
                        max_autodel = max(max_autodel, f.auto_delete_minutes)
                await s2.commit()
            # inform user about deletion timer if present
            if max_autodel > 0:
                await message.reply(f"These files will be deleted in {max_autodel} minutes.")
            return

    # no args: show configured start message if present
    cfg_pointer = await get_config("start_pointer")
    if cfg_pointer:
        try:
            channel_id_str, msg_id_str = cfg_pointer.split(":")
            c_id = int(channel_id_str)
            m_id = int(msg_id_str)
            start_template = await get_config("start_template") or ""
            # substitute first_name and convert any {word|url} tokens into HTML anchors
            txt_with_name = substitute_first_name(start_template, message.from_user.first_name)
            html_text = parse_braces_links(txt_with_name)
            # copy the stored start message (which we previously copied to upload channel)
            try:
                await bot.copy_message(chat_id=message.chat.id, from_chat_id=c_id, message_id=m_id, protect_content=False)
            except TypeError:
                await bot.copy_message(chat_id=message.chat.id, from_chat_id=c_id, message_id=m_id)
            if html_text.strip():
                # send the start template text as HTML (it may contain <a> anchors)
                await bot.send_message(chat_id=message.chat.id, text=html_text, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
            return
        except Exception as exc:
            logger.exception("Error sending /start pointer: %s", exc)
            await message.reply("Welcome!")
            return
    # default fallback text
    await message.reply("Welcome! No start message configured.")

# /help owner-only text (avoid using {word|url} raw in this static text to prevent HTML parse errors)
@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    if not is_owner(message):
        await message.reply("Owner-only command.")
        return
    help_text = (
        "Owner commands:\n"
        "/upload - start upload session (owner only)\n"
        "/d - finalize session (owner only). After sending /d reply 'on' or 'off' to set protection, then send minutes for auto-delete.\n"
        "/e - abort session\n"
        "/setstart - reply to a message to set the welcome message and optional media\n"
        "/broadcast - reply to a message to broadcast it to all users\n"
        "/stats - show totals\n"
        "/revoke <token or full link> - revoke a deep link (owner only)\n"
        "/restart - restart the bot (owner only)\n"
        "/health - HTTP endpoint for uptime checks\n\n"
        "Notes:\n"
        "- Use {first_name} inside the start template to insert the user's first name.\n"
        "- Use syntax {word|https://example.com} inside captions or start template to create clickable links."
    )
    # escape braces for HTML safety - we intentionally show braces literally by escaping
    help_text_safe = html.escape(help_text)
    # The above escaping will neutralize braces; but we want the text readable. Instead send as preformatted code block:
    await message.reply("Owner commands:\n" +
                        "/upload\n/d\n/e\n/setstart\n/broadcast\n/stats\n/revoke <token>\n/restart\n/health\n\nUse {first_name} in start template, and {word|url} to make clickable links in templates.", parse_mode=ParseMode.HTML)

# /upload owner-only: create a session and instruct owner to send files
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if not is_owner(message):
        await message.reply("Owner-only.")
        return
    existing = await get_active_session(OWNER_ID)
    if existing:
        await message.reply(f"You already have an active session (id={existing}). Send /d to finalize or /e to abort.")
        return
    sid = await create_session(OWNER_ID)
    await message.reply(f"Upload session started (id={sid}). Send 1..95 files now. When finished, send /d. To abort send /e.")


# Owner-only media collector: handles only media content types so it won't capture text commands
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID, content_types=[
    "photo", "document", "video", "animation", "sticker", "audio", "voice"
])
async def owner_file_collector(message: types.Message):
    """
    Collects media messages sent by owner during an active upload session.
    Copies them into UPLOAD_CHANNEL_ID and stores metadata in DB.
    """
    sid = await get_active_session(OWNER_ID)
    if not sid:
        # Not in an upload session; ignore
        return
    # Limit to 95 files
    async with AsyncSessionLocal() as s:
        q = select(FileEntry).where(FileEntry.session_id == sid)
        res = await s.execute(q)
        existing = res.scalars().all()
        if len(existing) >= 95:
            await message.reply("Session reached the maximum of 95 files. Send /d to finalize or /e to abort.")
            return
    # capture caption (if any) and keyboard
    caption_text = message.caption or message.text or None
    buttons_json = None
    if message.reply_markup:
        try:
            buttons_json = keyboard_to_json(message.reply_markup)
        except Exception:
            buttons_json = None
    # copy message to upload channel
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
        logger.exception("Failed to forward file to upload channel: %s", e)
        await message.reply("Failed to save file to upload channel. Ensure the bot is admin in the upload channel with permission to post and edit messages.")


# /d - owner-only finalize: ask for protect on/off (owner replies with 'on' or 'off' afterwards)
@dp.message_handler(commands=["d"])
async def cmd_finalize(message: types.Message):
    if not is_owner(message):
        return
    sid = await get_active_session(OWNER_ID)
    if not sid:
        await message.reply("No active session.")
        return
    # check that session has at least one file
    async with AsyncSessionLocal() as s:
        q = select(FileEntry).where(FileEntry.session_id == sid)
        res = await s.execute(q)
        files = res.scalars().all()
        if not files:
            await message.reply("Session has no files. Upload at least one file before finalizing.")
            return
    # ask protect choice
    awaiting_protect_choice[OWNER_ID] = sid
    awaiting_context[OWNER_ID] = {"session_id": sid}
    await message.reply("Protect content? Reply with 'on' or 'off' (owner reply).")


# Owner finalize flow: handles 'on'/'off' then numeric minutes for auto-delete.
# This handler receives text messages from owner only and ignores messages that start with '/'
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID, content_types=["text"])
async def owner_finalize_flow(message: types.Message):
    txt_raw = (message.text or "").strip()
    # If owner just sent a command (like /e, /setstart), let command handlers manage it
    if txt_raw.startswith("/"):
        return

    # protect stage
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
            await message.reply("Please send an integer number of minutes between 0 and 10080.")
            return
        minutes = int(txt_raw)
        if minutes < 0 or minutes > 10080:
            await message.reply("Minutes must be between 0 and 10080.")
            return
        sid = awaiting_autodel_choice.pop(OWNER_ID)
        context = awaiting_context.pop(OWNER_ID, {})
        protect = context.get("protect", True)
        # finalize session
        try:
            token = await finalize_session(OWNER_ID, sid, protect=protect, autodel_minutes=minutes)
        except Exception as e:
            logger.exception("Error finalizing session: %s", e)
            await message.reply("Failed to finalize session due to internal error.")
            return
        bot_info = await bot.get_me()
        full_link = f"https://t.me/{bot_info.username}?start={token}"
        if minutes > 0:
            await bot.send_message(chat_id=OWNER_ID, text=f"Session finalized. Deep link:\n{full_link}\nThese files will be deleted in {minutes} minutes after delivery.")
        else:
            await bot.send_message(chat_id=OWNER_ID, text=f"Session finalized. Deep link:\n{full_link}")
        return

    # otherwise ignore (not part of finalize flow)
    return


# /e - abort session
@dp.message_handler(commands=["e"])
async def cmd_abort(message: types.Message):
    if not is_owner(message):
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
    # cleanup runtime states
    active_sessions_cache.pop(OWNER_ID, None)
    awaiting_protect_choice.pop(OWNER_ID, None)
    awaiting_autodel_choice.pop(OWNER_ID, None)
    awaiting_context.pop(OWNER_ID, None)
    await message.reply("Session aborted. Uploaded copies in upload channel remain but are not linked.")


# /setstart - owner-only: reply to a message to set the bot /start content (copy into upload channel and save pointer)
@dp.message_handler(commands=["setstart"])
async def cmd_setstart(message: types.Message):
    if not is_owner(message):
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
        await message.reply("Start content saved. /start will now show this content and template text (supports {first_name} and {word|url}).")
    except Exception as e:
        logger.exception("Failed to set start content: %s", e)
        await message.reply("Failed to set start content. Ensure the bot is admin in the upload channel with forward/post permissions.")


# /broadcast - owner-only: reply to a message; bot copies message to upload channel and then copies to all users known in DB
@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if not is_owner(message):
        return
    target = message.reply_to_message
    if not target:
        await message.reply("Reply to a message to broadcast it to all users.")
        return
    try:
        copied = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=target.chat.id, message_id=target.message_id)
        src_chat_id = UPLOAD_CHANNEL_ID
        src_msg_id = copied.message_id
    except Exception as e:
        logger.exception("Failed to prepare broadcast message: %s", e)
        await message.reply("Failed to prepare broadcast (maybe bot lacks permission in upload channel).")
        return
    # gather users
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
            logger.warning("Broadcast failed for %s: %s", uid, e)
            failed += 1
    await message.reply(f"Broadcast complete. Sent: {sent}, Failed: {failed}")


# /stats - owner-only system statistics (counts)
@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not is_owner(message):
        return
    two_days_ago = datetime.now(timezone.utc) - timedelta(days=2)
    async with AsyncSessionLocal() as s:
        total_users = (await s.execute(select(func.count()).select_from(User))).scalar()
        active_users = (await s.execute(select(func.count()).select_from(User).where(User.last_active >= two_days_ago))).scalar()
        total_files = (await s.execute(select(func.count()).select_from(FileEntry))).scalar()
        total_sessions = (await s.execute(select(func.count()).select_from(SessionModel))).scalar()
    await message.reply(f"Total users: {total_users}\nActive (2 days): {active_users}\nTotal files: {total_files}\nTotal sessions: {total_sessions}")


# /revoke <token|full_link> - owner-only to mark a session revoked
@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if not is_owner(message):
        return
    arg = message.get_args().strip()
    if not arg:
        await message.reply("Usage: /revoke <token or full link>")
        return
    # extract token if full link given
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
        # update header in upload channel if present
        try:
            if sess.header_message_id:
                txt = f"📦 Session #{sess.id} — REVOKED by owner."
                await bot.edit_message_text(chat_id=UPLOAD_CHANNEL_ID, message_id=sess.header_message_id, text=txt)
        except Exception:
            logger.info("Could not edit header message for revoked session (maybe permissions).")
    await message.reply(f"Session #{sess.id} revoked. Deep link no longer works.")


# /restart - owner-only restart (gracefully close and exit so Render restarts process)
@dp.message_handler(commands=["restart"])
async def cmd_restart(message: types.Message):
    if not is_owner(message):
        return
    await message.reply("Restarting now...")
    # attempt graceful shutdown of bot and exit to let Render restart container
    await bot.close()
    logger.info("Owner requested restart. Exiting process.")
    os._exit(0)


# Fallback handler: update user last_active for general messages from users
# IMPORTANT: We do not intercept commands here (text starting with '/')
@dp.message_handler(content_types=types.ContentType.ANY)
async def fallback_handler(message: types.Message):
    if message.text and message.text.startswith("/"):
        # Let command handlers process this; do nothing here
        return
    try:
        await upsert_user(message.from_user)
    except Exception as e:
        logger.exception("Failed to upsert user: %s", e)
    # do not reply to users for general messages


# -----------------------
# Startup: ensure DB, columns, workers, and web health endpoint
# -----------------------
async def restore_state_on_startup():
    """
    Called on startup. Ensures DB schema columns exist and schedules background workers.
    """
    logger.info("Startup: ensuring DB and schema...")
    await init_db()
    # ensure future-added columns exist to avoid ProgrammingError when code references them
    await ensure_schema_columns()
    logger.info("Startup: DB schema checked and ensured.")


async def on_startup(dp_obj):
    logger.info("Bot starting up...")
    await restore_state_on_startup()
    # spawn deletion worker
    loop = asyncio.get_event_loop()
    loop.create_task(deletion_worker())
    # spawn web server for /health endpoint
    loop.create_task(start_webapp(app_port=PORT))
    logger.info("Bot started and background tasks scheduled.")


# -----------------------
# Run: entry point
# -----------------------
if __name__ == "__main__":
    # start polling with on_startup hook
    # skip_updates=True to avoid processing backlog when redeploying
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)