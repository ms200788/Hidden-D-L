# bot.py
"""
Telegram content-delivery bot (aiogram).
- Uses async SQLAlchemy + PostgreSQL for persistent metadata.
- Files themselves are stored in Telegram: forwarded/copied to UPLOAD_CHANNEL_ID.
- One upload session -> one deep link token (random, long).
- Supports multi-file sessions (1..95), photos, videos, docs, stickers.
- Auto-delete timers (0..10080 minutes). If non-zero, bot schedules deletion of delivered messages.
- protect_content used when delivering to users (owner can bypass).
- /health endpoint for Uptime Robot.
"""

import os
import asyncio
import logging
import secrets
import html
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup
from aiogram.utils.exceptions import TelegramBadRequest
from aiohttp import web

from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, Boolean, DateTime, JSON, func, select
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# ---------- CONFIG ----------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL")  # must be like postgresql+asyncpg://...
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))  # -100...
PING_SECRET = os.getenv("PING_SECRET", "")
PORT = int(os.getenv("PORT", "8000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

if not TELEGRAM_TOKEN or OWNER_ID == 0 or DATABASE_URL is None or UPLOAD_CHANNEL_ID == 0:
    raise SystemExit("Set TELEGRAM_TOKEN, OWNER_ID, DATABASE_URL, UPLOAD_CHANNEL_ID env vars.")

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# ---------- DATABASE (async SQLAlchemy) ----------
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    tg_id = Column(BigInteger, unique=True, nullable=False, index=True)
    username = Column(String(255))
    first_name = Column(String(255))
    last_name = Column(String(255))
    last_active = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class Session(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True)
    owner_id = Column(BigInteger, nullable=False)
    token = Column(String(255), unique=True, nullable=True)  # deep link token after finalize
    active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class FileEntry(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer)  # FK omitted for simplicity in migrations
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

# engine & session
engine = create_async_engine(DATABASE_URL, future=True, echo=False, pool_pre_ping=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("DB initialized / tables ensured.")

# ---------- BOT SETUP ----------
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher(bot)

# In-memory: track active upload session per owner id (cached, but persisted in DB)
# This allows concurrent user deep-link fetchers; only owner uses upload sessions.
active_sessions_cache: Dict[int, int] = {}  # owner_id -> session_id

# helper: create a new upload session (owner)
async def create_session(owner_id: int) -> int:
    async with AsyncSessionLocal() as sess:
        new = Session(owner_id=owner_id, active=True)
        sess.add(new)
        await sess.commit()
        await sess.refresh(new)
        active_sessions_cache[owner_id] = new.id
        return new.id

async def get_active_session(owner_id: int) -> Optional[int]:
    # check cache then DB
    sid = active_sessions_cache.get(owner_id)
    if sid:
        # verify it's still active in DB
        async with AsyncSessionLocal() as s:
            res = await s.execute(select(Session).where(Session.id == sid))
            row = res.scalar_one_or_none()
            if row and row.active:
                return sid
            else:
                active_sessions_cache.pop(owner_id, None)
                return None
    # fallback DB find
    async with AsyncSessionLocal() as s:
        res = await s.execute(select(Session).where(Session.owner_id == owner_id, Session.active == True).order_by(Session.created_at.desc()))
        row = res.scalar_one_or_none()
        if row:
            active_sessions_cache[owner_id] = row.id
            return row.id
    return None

async def finalize_session(owner_id: int, session_id: int) -> str:
    # generate token with minimum length (we'll ensure it's long enough)
    token = secrets.token_urlsafe(64)  # usually ~86 chars base64-url-safe
    # ensure minimum length at least equal to example (we'll check length 44+)
    while len(token) < 44:
        token = secrets.token_urlsafe(64)
    async with AsyncSessionLocal() as s:
        await s.execute(
            # mark session token and deactivate it from further uploads
            select(Session).where(Session.id == session_id)
        )
        # update
        row = await s.get(Session, session_id)
        if not row:
            raise RuntimeError("session not found")
        row.token = token
        row.active = False
        s.add(row)
        await s.commit()
    active_sessions_cache.pop(owner_id, None)
    return token

# helper: add file entry (after copying into upload channel)
async def add_file_entry(session_id: int, upload_channel_id: int, upload_message_id: int,
                         caption: Optional[str], buttons: Optional[List[List[Dict[str,str]]]],
                         protected: bool, auto_delete_minutes: int):
    async with AsyncSessionLocal() as s:
        entry = FileEntry(
            session_id=session_id,
            upload_channel_id=upload_channel_id,
            upload_message_id=upload_message_id,
            caption=caption,
            buttons=buttons,
            protected=protected,
            auto_delete_minutes=auto_delete_minutes
        )
        s.add(entry)
        await s.commit()
        await s.refresh(entry)
        return entry.id

# helper to store config (like start message pointer)
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

# helper to upsert user
async def upsert_user(tg_user: types.User):
    async with AsyncSessionLocal() as s:
        stmt = select(User).where(User.tg_id == tg_user.id)
        r = await s.execute(stmt)
        row = r.scalar_one_or_none()
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

# ---------- TEXT PARSING UTILITIES ----------
def parse_braces_links(text: str) -> str:
    """
    Replace occurrences of {word|url} with HTML <a href="url">word</a>
    Escapes other content as needed. Supports multiple occurrences.
    """
    if not text:
        return text
    # naive parse: find {...|...}
    out = ""
    i = 0
    while i < len(text):
        if text[i] == "{":
            # find closing }
            j = text.find("}", i)
            if j == -1:
                out += html.escape(text[i:])
                break
            inside = text[i+1:j]
            if "|" in inside:
                word, url = inside.split("|", 1)
                word_esc = html.escape(word.strip())
                url_esc = html.escape(url.strip(), quote=True)
                out += f'<a href="{url_esc}">{word_esc}</a>'
            else:
                # not a link, escape whole
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

# helper to extract inline keyboard JSON from a message (if present)
def keyboard_to_json(kb: Optional[InlineKeyboardMarkup]) -> Optional[List[List[Dict[str,str]]]]:
    if not kb:
        return None
    rows = []
    for r in kb.inline_keyboard:
        row = []
        for btn in r:
            row.append({"text": btn.text, "url": btn.url, "callback_data": btn.callback_data})
        rows.append(row)
    return rows

# helper to reconstruct InlineKeyboardMarkup from stored json
def json_to_keyboard(j: Optional[List[List[Dict[str,str]]]]) -> Optional[InlineKeyboardMarkup]:
    if not j:
        return None
    kb = InlineKeyboardMarkup(row_width=1)
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

# ---------- AUTO-DELETE SCHEDULER ----------
async def deletion_worker():
    logger.info("Deletion worker started.")
    while True:
        try:
            now = datetime.now(timezone.utc)
            async with AsyncSessionLocal() as s:
                stmt = select(Delivery).where(Delivery.delete_at != None, Delivery.delete_at <= now, Delivery.deleted == False)
                res = await s.execute(stmt)
                rows = res.scalars().all()
                for delv in rows:
                    try:
                        await bot.delete_message(chat_id=delv.chat_id, message_id=delv.message_id)
                        delv.deleted = True
                        s.add(delv)
                        logger.info("Deleted message %s in chat %s", delv.message_id, delv.chat_id)
                    except TelegramBadRequest as e:
                        # message likely already deleted or bot lacks rights
                        delv.deleted = True
                        s.add(delv)
                        logger.warning("Could not delete message %s in chat %s: %s", delv.message_id, delv.chat_id, e)
                await s.commit()
        except Exception as e:
            logger.exception("Deletion worker error: %s", e)
        await asyncio.sleep(30)  # run every 30s

# ---------- HTTP HEALTH ----------
async def handle_health(request):
    if PING_SECRET:
        token = request.query.get("token", "")
        if token != PING_SECRET:
            return web.Response(text="unauthorized", status=401)
    return web.Response(text="ok")

async def start_web_app():
    app = web.Application()
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Health endpoint started on port %s", PORT)

# ---------- COMMANDS / HANDLERS ----------

# Start: supports plain /start and /start <token>
@dp.message_handler(Command("start"))
async def cmd_start(message: Message):
    # register user
    await upsert_user(message.from_user)

    args = message.get_args().strip()
    if args:
        token = args
        # try find session by token
        async with AsyncSessionLocal() as s:
            stmt = select(Session).where(Session.token == token)
            res = await s.execute(stmt)
            sess = res.scalar_one_or_none()
            if not sess:
                await message.reply("Invalid or expired link.")
                return
            # fetch files in this session
            stmt2 = select(FileEntry).where(FileEntry.session_id == sess.id)
            res2 = await s.execute(stmt2)
            files = res2.scalars().all()
            if not files:
                await message.reply("No files found for this link.")
                return
            # deliver all files (copy from upload channel to user)
            responses = []
            for f in files:
                try:
                    # copy_message preserves media + caption + buttons if copying from channel msg
                    # construct reply markup from stored buttons
                    reply_markup = json_to_keyboard(f.buttons)
                    # copy
                    msg = await bot.copy_message(chat_id=message.chat.id,
                                                 from_chat_id=f.upload_channel_id,
                                                 message_id=f.upload_message_id,
                                                 protect_content=f.protected)
                    responses.append((f, msg))
                    await asyncio.sleep(0.05)  # small throttle
                except Exception as e:
                    logger.exception("Failed to deliver file id %s to %s: %s", f.id, message.chat.id, e)
            # schedule deletions if any
            max_delete = 0
            for f, msg in responses:
                if f.auto_delete_minutes and f.auto_delete_minutes > 0:
                    delete_at = datetime.now(timezone.utc) + timedelta(minutes=int(f.auto_delete_minutes))
                    async with AsyncSessionLocal() as s:
                        d = Delivery(file_id=f.id, chat_id=message.chat.id, message_id=msg.message_id, delete_at=delete_at)
                        s.add(d)
                        await s.commit()
                    max_delete = max(max_delete, f.auto_delete_minutes)
            if max_delete > 0:
                await message.reply(f"These files will be deleted in {max_delete} minutes.")
            return

    # else no args: show configured start message (if exists)
    cfg = await get_config("start_pointer")
    if cfg:
        # stored as "<channel_id>:<msg_id>"
        try:
            channel_id_str, msg_id_str = cfg.split(":")
            c_id = int(channel_id_str)
            m_id = int(msg_id_str)
            # fetch optional start message template text (for first_name replacement)
            start_template = await get_config("start_template") or ""
            # substitute first_name and parse {word|url}
            txt = substitute_first_name(start_template, message.from_user.first_name)
            txt = parse_braces_links(txt)
            # copy message from channel to user preserving media & keyboard
            await bot.copy_message(chat_id=message.chat.id, from_chat_id=c_id, message_id=m_id, protect_content=False)
            if txt.strip():
                await bot.send_message(chat_id=message.chat.id, text=txt, parse_mode="HTML", disable_web_page_preview=False)
            return
        except Exception as e:
            logger.exception("Failed to send start pointer: %s", e)
            await message.reply("Welcome!")
            return
    # default
    await message.reply("Welcome! No start message configured.")

# Owner-only: help
@dp.message_handler(Command("help"))
async def cmd_help(message: Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Owner-only command.")
        return
    help_text = (
        "/upload - start an upload session (owner only)\n"
        "Then send files (1..95) in the same chat; they will be forwarded to the upload channel.\n"
        "/d - finalize session (generate deep link)\n"
        "/e - abort session\n"
        "/setstart - reply to a message (image+caption or text) to set /start content\n"
        "/broadcast - reply to a message to broadcast it to all users (owner only)\n"
        "/restart - restart bot (owner only)\n"
        "/stats - show totals (owner only)\n"
        "/health - health endpoint (for Uptime Robot)\n"
        "Deep links: /start <token> - fetch session files\n"
        "Formatting: use {first_name} in start; use {word|url} in start/broadcast/upload captions to create clickable links.\n"
        "Auto-delete: set per-file when uploading (minutes, 0..10080)."
    )
    await message.reply(help_text)

# /upload: initialize session
@dp.message_handler(Command("upload"))
async def cmd_upload(message: Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Owner-only.")
        return
    # ensure no active session exists (or allow new)
    existing = await get_active_session(OWNER_ID)
    if existing:
        await message.reply(f"You already have an active session (id={existing}). Use /d to finalize or /e to abort.")
        return
    sid = await create_session(OWNER_ID)
    await message.reply(f"Upload session started. Session id: {sid}. Send 1..95 files now. When done send /d. To abort send /e.")

# handle owner file messages while session active (capture photos, docs, video, audio, stickers)
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and m.chat.type == "private")
async def owner_message_handler(message: Message):
    # only accept files if session active
    sid = await get_active_session(OWNER_ID)
    if not sid:
        return  # ignore non-session owner messages (owner can still use commands)
    # Check message type: we accept photo, document, video, animation, sticker, audio, voice
    allowed = any([
        message.photo,
        message.document,
        message.video,
        message.animation,
        message.sticker,
        message.audio,
        message.voice
    ])
    if not allowed:
        # allow text messages as captions only? If owner sends only text as separate message, treat as caption for next file.
        # For simplicity, we require files.
        await message.reply("Send a file (photo, video, document, sticker, audio). To finish session send /d.")
        return
    # ensure session not exceeding 95 files
    async with AsyncSessionLocal() as s:
        count_stmt = select(FileEntry).where(FileEntry.session_id == sid)
        res = await s.execute(count_stmt)
        existing = res.scalars().all()
        if len(existing) >= 95:
            await message.reply("Session already has maximum 95 files. Send /d to finalize or /e to abort.")
            return
    # Ask owner for auto-delete minutes and protected flag in a simple format:
    # Owner can include in the caption a special line like: [autodel:10] [protected:yes/no]
    caption_text = None
    protected = True
    auto_delete_minutes = 0
    if message.caption:
        caption_text = message.caption
        # parse for [autodel:X] and [protected:yes/no] tokens anywhere in caption
        import re
        m_autodel = re.search(r"\[autodel:(\d{1,5})\]", caption_text)
        if m_autodel:
            try:
                v = int(m_autodel.group(1))
                if 0 <= v <= 10080:
                    auto_delete_minutes = v
                # remove token from caption
                caption_text = caption_text.replace(m_autodel.group(0), "").strip()
            except:
                pass
        m_prot = re.search(r"\[protected:(yes|no|true|false)\]", caption_text, flags=re.I)
        if m_prot:
            prot_val = m_prot.group(1).lower()
            protected = prot_val in ("yes", "true")
            caption_text = caption_text.replace(m_prot.group(0), "").strip()
    # copy the message to the upload channel (preserve file)
    try:
        copied = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
        # extract inline keyboard if owner attached (rare), we preserve by reading replied markup if exists (owner can use reply markup)
        buttons_json = keyboard_to_json(message.reply_markup) if message.reply_markup else None
        # store file entry
        fid = await add_file_entry(session_id=sid,
                                   upload_channel_id=UPLOAD_CHANNEL_ID,
                                   upload_message_id=copied.message_id,
                                   caption=caption_text,
                                   buttons=buttons_json,
                                   protected=protected,
                                   auto_delete_minutes=auto_delete_minutes)
        # feedback
        await message.reply(f"File saved to upload channel (id={fid}). Caption preserved. Protected={protected}. Auto-delete={auto_delete_minutes} min.")
    except Exception as e:
        logger.exception("Failed to copy file to upload channel: %s", e)
        await message.reply("Failed to save file to upload channel. Ensure bot is admin in upload channel.")

# /d finalize
@dp.message_handler(Command("d"))
async def cmd_done(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    sid = await get_active_session(OWNER_ID)
    if not sid:
        await message.reply("No active session to finalize.")
        return
    # ensure at least 1 file exists
    async with AsyncSessionLocal() as s:
        res = await s.execute(select(FileEntry).where(FileEntry.session_id == sid))
        files = res.scalars().all()
        if len(files) == 0:
            await message.reply("Session has no files. Abort with /e or send files.")
            return
    token = await finalize_session(OWNER_ID, sid)
    deep_link = token  # the token is long; full start link is: https://t.me/<bot_username>?start=<token>
    bot_username = (await bot.get_me()).username
    full_link = f"https://t.me/{bot_username}?start={deep_link}"
    await message.reply(f"Session finalized. Deep link (one link for all files in this session):\n{full_link}\nShare this with users. It will deliver all files in the session when used.")
    return

# /e abort
@dp.message_handler(Command("e"))
async def cmd_abort(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    sid = await get_active_session(OWNER_ID)
    if not sid:
        await message.reply("No active session.")
        return
    # mark session inactive and do not expose token; keep files in upload channel but not linked
    async with AsyncSessionLocal() as s:
        row = await s.get(Session, sid)
        if row:
            row.active = False
            row.token = None
            s.add(row)
            await s.commit()
    active_sessions_cache.pop(OWNER_ID, None)
    await message.reply("Session aborted and deactivated. Uploaded channel copies remain in channel but are not accessible via deep link.")

# /setstart - owner should reply to message to set start content
@dp.message_handler(Command("setstart"))
async def cmd_setstart(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message (text or media+caption) to set it as start content.")
        return
    # copy replied message to upload channel and store pointer
    try:
        copied = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.reply_to_message.chat.id, message_id=message.reply_to_message.message_id)
        pointer = f"{UPLOAD_CHANNEL_ID}:{copied.message_id}"
        await set_config("start_pointer", pointer)
        # also store template if the replied message had text/caption; we store caption as template for {first_name}
        template = message.reply_to_message.caption or message.reply_to_message.text or ""
        await set_config("start_template", template)
        await message.reply("Start message set. Bot will copy that message to users on /start and send template text (with {first_name} substitution).")
    except Exception as e:
        logger.exception("Failed to set start: %s", e)
        await message.reply("Failed to set start message. Ensure bot is admin in upload channel.")

# /broadcast - owner-only. If reply to a message, we'll copy that message to channel then broadcast from channel.
@dp.message_handler(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    # owner can reply to a message with media or text OR provide text after command
    target_message = None
    if message.reply_to_message:
        target_message = message.reply_to_message
    else:
        # use command argument as text
        txt = message.get_args().strip()
        if not txt:
            await message.reply("Usage: reply to a message and use /broadcast, or use /broadcast <text>")
            return
    # if target_message present, copy it to upload channel for stable source, then copy to all users
    src_chat_id = None
    src_msg_id = None
    if target_message:
        try:
            copied = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=target_message.chat.id, message_id=target_message.message_id)
            src_chat_id = UPLOAD_CHANNEL_ID
            src_msg_id = copied.message_id
        except Exception as e:
            logger.exception("Failed to copy broadcast message to channel: %s", e)
            await message.reply("Failed to prepare broadcast message.")
            return
    else:
        # create temporary message in upload channel with the plain text
        try:
            sent = await bot.send_message(chat_id=UPLOAD_CHANNEL_ID, text=txt)
            src_chat_id = UPLOAD_CHANNEL_ID
            src_msg_id = sent.message_id
        except Exception as e:
            logger.exception("Failed to create broadcast text in channel: %s", e)
            await message.reply("Failed to prepare broadcast message.")
            return
    # now iterate users and copy_message
    async with AsyncSessionLocal() as s:
        res = await s.execute(select(User.tg_id))
        rows = [r[0] for r in res.fetchall()]
    sent = 0
    failed = 0
    for uid in rows:
        try:
            await bot.copy_message(chat_id=uid, from_chat_id=src_chat_id, message_id=src_msg_id, protect_content=True)
            sent += 1
            await asyncio.sleep(0.08)  # throttle a bit
        except Exception as e:
            failed += 1
            logger.warning("Broadcast failed to %s: %s", uid, e)
    await message.reply(f"Broadcast complete. Sent: {sent}, Failed: {failed}")

# /stats - owner-only (total users, active in 2 days, total files, total sessions)
@dp.message_handler(Command("stats"))
async def cmd_stats(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    now = datetime.now(timezone.utc)
    two_days_ago = now - timedelta(days=2)
    async with AsyncSessionLocal() as s:
        total_users = (await s.execute(select(func.count()).select_from(User))).scalar()
        active_users = (await s.execute(select(func.count()).select_from(User).where(User.last_active >= two_days_ago))).scalar()
        total_files = (await s.execute(select(func.count()).select_from(FileEntry))).scalar()
        total_sessions = (await s.execute(select(func.count()).select_from(Session))).scalar()
    await message.reply(f"Total users: {total_users}\nActive (2 days): {active_users}\nTotal files: {total_files}\nTotal sessions: {total_sessions}")

# /restart - owner only
@dp.message_handler(Command("restart"))
async def cmd_restart(message: Message):
    if message.from_user.id != OWNER_ID:
        return
    await message.reply("Restarting now...")
    await bot.session.close()
    os._exit(0)

# Fallback: handle other messages (register users)
@dp.message_handler()
async def fallback(message: Message):
    # register users and update last_active
    await upsert_user(message.from_user)
    # ignore other public commands; only /start and deep link are relevant for users

# ---------- STARTUP / MAIN ----------
async def on_startup():
    await init_db()
    # start deletion worker
    asyncio.create_task(deletion_worker())
    # start web server
    asyncio.create_task(start_web_app())
    logger.info("Bot startup complete.")

async def main():
    await on_startup()
    # start polling
    try:
        await dp.start_polling()
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())