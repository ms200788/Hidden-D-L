#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Telegram File Sharing Bot (Webhook-only, aiogram v2 style)

Features:
- aiogram v2
- Neon Postgres (asyncpg)
- Webhook mode (suitable for Render)
- Health endpoint for UptimeRobot
- Private upload channel for storing files (bot posts files into channel)
- Deep link sharing of upload sessions
- Owner commands for managing messages, images, broadcasting, uploading
- Auto-delete logic for user views (scheduled and persisted)
- Stats (/stats)
- Persistent DB schema creation on startup

Author: Generated for the user's specification
Date: 2025-09-27
"""

import asyncio
import logging
import os
import sys
import json
import time
import secrets
import traceback
from typing import Optional, List, Dict, Any, Tuple, Union
from datetime import datetime, timedelta

# Third-party libs
# aiogram v2:
# pip install aiogram==2.23.1
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.utils.helper import Helper, HelperMode, List as AiList
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto, InputMediaDocument
import asyncpg
from aiohttp import web

# ==========
# Configuration & environment variables
# ==========

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN environment variable is required.", file=sys.stderr)
    sys.exit(1)

OWNER_ID = os.getenv("OWNER_ID")
if not OWNER_ID:
    print("ERROR: OWNER_ID environment variable is required.", file=sys.stderr)
    sys.exit(1)
try:
    OWNER_ID = int(OWNER_ID)
except Exception:
    print("ERROR: OWNER_ID must be an integer.", file=sys.stderr)
    sys.exit(1)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL environment variable is required.", file=sys.stderr)
    sys.exit(1)

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
if not RENDER_EXTERNAL_URL:
    print("WARNING: RENDER_EXTERNAL_URL not provided. Webhook will not be set automatically.")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", f"/webhook/{BOT_TOKEN}")
# On Render the port is usually provided by env var PORT, but default to 8000
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", os.getenv("PORT", 8000)))
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")

UPLOAD_CHANNEL_ID = os.getenv("UPLOAD_CHANNEL_ID")
if not UPLOAD_CHANNEL_ID:
    print("ERROR: UPLOAD_CHANNEL_ID environment variable is required (private channel where bot archives uploads).", file=sys.stderr)
    sys.exit(1)
try:
    UPLOAD_CHANNEL_ID = int(UPLOAD_CHANNEL_ID)
except Exception:
    print("ERROR: UPLOAD_CHANNEL_ID must be an integer (channel id).", file=sys.stderr)
    sys.exit(1)

# Optional behavior
AUTO_CLEAN_DB_ON_START = False  # keep False; only set True for debugging
MAX_BROADCAST_BATCH = 100  # number of users to try per transaction chunk (throttle)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s:%(name)s: %(message)s"
)
logger = logging.getLogger("telegram_bot")

# ==========
# Globals (runtime)
# ==========

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# Database pool (asyncpg)
PG_POOL: Optional[asyncpg.pool.Pool] = None

# In-memory upload sessions in progress keyed by owner chat id
# Each session: {
#   "session_id": str,
#   "files": [ {"file_id": str, "file_type": "photo|document|video|audio", "caption": str or None} ],
#   "owner_id": int,
#   "created_at": datetime,
#   "protect_content": bool,
#   "auto_delete_minutes": int,
# }
ACTIVE_UPLOAD_SESSIONS: Dict[int, Dict[str, Any]] = {}

# In-memory mapping of scheduled deletions for messages the bot sent to users:
# session_id -> list of dict(chat_id, message_id, scheduled_at, deletion_id)
SCHEDULED_DELETIONS: Dict[str, List[Dict[str, Any]]] = {}

# Limits / constants
MAX_AUTODELETE_MINUTES = 7 * 24 * 60  # 1 week = 10080 minutes, allowed range in spec
MAX_ALLOWED_AUTODELETE = 10080

# ==========
# Database helpers: schema and CRUD
# ==========

CREATE_TABLES_SQL = """
-- Users table
CREATE TABLE IF NOT EXISTS bot_users (
    user_id BIGINT PRIMARY KEY,
    join_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    last_active TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- Messages (start/help) table
CREATE TABLE IF NOT EXISTS bot_messages (
    key TEXT PRIMARY KEY,
    text TEXT,
    image_file_id TEXT,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- Upload sessions (persistent)
CREATE TABLE IF NOT EXISTS upload_sessions (
    session_id TEXT PRIMARY KEY,
    owner_id BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    protect_content BOOLEAN DEFAULT FALSE,
    auto_delete_minutes INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE
);

-- Files belonging to upload sessions
CREATE TABLE IF NOT EXISTS upload_files (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT REFERENCES upload_sessions(session_id) ON DELETE CASCADE,
    file_id TEXT NOT NULL,
    file_type TEXT,
    caption TEXT,
    file_size BIGINT,
    inserted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- Stats table for counters
CREATE TABLE IF NOT EXISTS bot_stats (
    key TEXT PRIMARY KEY,
    value BIGINT DEFAULT 0
);

-- Pending deletions for messages the bot sent to users (persistence)
CREATE TABLE IF NOT EXISTS pending_deletions (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT,
    chat_id BIGINT,
    message_id BIGINT,
    scheduled_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    processed BOOLEAN DEFAULT FALSE
);

-- Indexes for quick searches
CREATE INDEX IF NOT EXISTS idx_upload_sessions_owner ON upload_sessions(owner_id);
CREATE INDEX IF NOT EXISTS idx_upload_files_session ON upload_files(session_id);
CREATE INDEX IF NOT EXISTS idx_pending_deletions_session ON pending_deletions(session_id);
"""

async def init_db_pool():
    """
    Initialize asyncpg pool and create schema if necessary.
    """
    global PG_POOL
    logger.info("Initializing database pool...")
    try:
        PG_POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        async with PG_POOL.acquire() as conn:
            await conn.execute(CREATE_TABLES_SQL)
            # Ensure basic keys in bot_messages for 'start' and 'help'
            await conn.execute("""
                INSERT INTO bot_messages (key, text) VALUES ('start', 'Welcome!') 
                ON CONFLICT (key) DO NOTHING;
            """)
            await conn.execute("""
                INSERT INTO bot_messages (key, text) VALUES ('help', 'This is the help text. Use /start to begin.') 
                ON CONFLICT (key) DO NOTHING;
            """)
            # Ensure stats keys exist
            await conn.execute("""
                INSERT INTO bot_stats (key, value) VALUES ('total_users', 0)
                ON CONFLICT (key) DO NOTHING;
            """)
            await conn.execute("""
                INSERT INTO bot_stats (key, value) VALUES ('files_uploaded', 0)
                ON CONFLICT (key) DO NOTHING;
            """)
            await conn.execute("""
                INSERT INTO bot_stats (key, value) VALUES ('upload_sessions', 0)
                ON CONFLICT (key) DO NOTHING;
            """)
    except Exception as e:
        logger.exception("Failed to initialize DB pool: %s", e)
        raise

async def db_add_or_touch_user(user_id: int):
    """
    Insert user on first contact, update last_active.
    """
    global PG_POOL
    if PG_POOL is None:
        logger.error("PG_POOL is None in db_add_or_touch_user")
        return
    async with PG_POOL.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                INSERT INTO bot_users (user_id, join_date, last_active)
                VALUES ($1, now(), now())
                ON CONFLICT (user_id) DO UPDATE SET last_active = now();
            """, user_id)
            # increment total_users only on insert; to do this properly we attempt to get count and upsert
            # For simplicity we will recalc total_users later in stats query.

async def db_set_message(key: str, text: Optional[str] = None, image_file_id: Optional[str] = None):
    """
    Update start/help messages and optionally image file_id
    """
    global PG_POOL
    if PG_POOL is None:
        return
    async with PG_POOL.acquire() as conn:
        if text is not None and image_file_id is not None:
            await conn.execute("""
                INSERT INTO bot_messages (key, text, image_file_id, updated_at) VALUES ($1, $2, $3, now())
                ON CONFLICT (key) DO UPDATE SET text = $2, image_file_id = $3, updated_at = now()
            """, key, text, image_file_id)
        elif text is not None:
            await conn.execute("""
                INSERT INTO bot_messages (key, text, updated_at) VALUES ($1, $2, now())
                ON CONFLICT (key) DO UPDATE SET text = $2, updated_at = now()
            """, key, text)
        elif image_file_id is not None:
            await conn.execute("""
                INSERT INTO bot_messages (key, image_file_id, updated_at) VALUES ($1, $2, now())
                ON CONFLICT (key) DO UPDATE SET image_file_id = $2, updated_at = now()
            """, key, image_file_id)

async def db_get_message(key: str) -> Dict[str, Any]:
    """
    Return dict {text, image_file_id}
    """
    global PG_POOL
    if PG_POOL is None:
        return {"text": None, "image_file_id": None}
    async with PG_POOL.acquire() as conn:
        rec = await conn.fetchrow("SELECT text, image_file_id FROM bot_messages WHERE key = $1", key)
        if rec is None:
            return {"text": None, "image_file_id": None}
        return {"text": rec["text"], "image_file_id": rec["image_file_id"]}

async def db_create_upload_session(session_id: str, owner_id: int, protect_content: bool, auto_delete_minutes: int):
    """
    Create upload_sessions record.
    """
    global PG_POOL
    if PG_POOL is None:
        return
    async with PG_POOL.acquire() as conn:
        await conn.execute("""
            INSERT INTO upload_sessions (session_id, owner_id, protect_content, auto_delete_minutes, created_at, is_active)
            VALUES ($1, $2, $3, $4, now(), TRUE)
            ON CONFLICT (session_id) DO UPDATE SET owner_id = $2, protect_content = $3, auto_delete_minutes = $4, is_active = TRUE
        """, session_id, owner_id, protect_content, auto_delete_minutes)
        # increment upload_sessions and files_uploaded in stats
        await conn.execute("""
            UPDATE bot_stats SET value = value + 1 WHERE key = 'upload_sessions'
        """)

async def db_add_upload_file(session_id: str, file_id: str, file_type: str, caption: Optional[str], file_size: Optional[int]):
    global PG_POOL
    if PG_POOL is None:
        return
    async with PG_POOL.acquire() as conn:
        await conn.execute("""
            INSERT INTO upload_files (session_id, file_id, file_type, caption, file_size, inserted_at)
            VALUES ($1, $2, $3, $4, $5, now())
        """, session_id, file_id, file_type, caption, file_size)
        await conn.execute("""
            UPDATE bot_stats SET value = value + 1 WHERE key = 'files_uploaded'
        """)

async def db_get_upload_files(session_id: str) -> List[Dict[str, Any]]:
    global PG_POOL
    if PG_POOL is None:
        return []
    async with PG_POOL.acquire() as conn:
        rows = await conn.fetch("""
            SELECT file_id, file_type, caption, file_size, inserted_at FROM upload_files WHERE session_id = $1 ORDER BY id ASC
        """, session_id)
        return [dict(r) for r in rows]

async def db_get_session(session_id: str) -> Optional[Dict[str, Any]]:
    global PG_POOL
    if PG_POOL is None:
        return None
    async with PG_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT session_id, owner_id, protect_content, auto_delete_minutes, created_at, is_active FROM upload_sessions WHERE session_id = $1", session_id)
        if not row:
            return None
        return dict(row)

async def db_mark_session_inactive(session_id: str):
    global PG_POOL
    if PG_POOL is None:
        return
    async with PG_POOL.acquire() as conn:
        await conn.execute("UPDATE upload_sessions SET is_active = FALSE WHERE session_id = $1", session_id)

async def db_add_pending_deletion(session_id: str, chat_id: int, message_id: int, scheduled_at_ts: datetime):
    global PG_POOL
    if PG_POOL is None:
        return
    async with PG_POOL.acquire() as conn:
        await conn.execute("""
            INSERT INTO pending_deletions (session_id, chat_id, message_id, scheduled_at, created_at, processed)
            VALUES ($1, $2, $3, $4, now(), FALSE)
        """, session_id, chat_id, message_id, scheduled_at_ts)

async def db_get_pending_deletions() -> List[Dict[str, Any]]:
    """
    Return all pending deletions that are not processed.
    """
    global PG_POOL
    if PG_POOL is None:
        return []
    async with PG_POOL.acquire() as conn:
        rows = await conn.fetch("SELECT id, session_id, chat_id, message_id, scheduled_at FROM pending_deletions WHERE processed = FALSE")
        return [dict(r) for r in rows]

async def db_mark_deletion_processed(db_id: int):
    global PG_POOL
    if PG_POOL is None:
        return
    async with PG_POOL.acquire() as conn:
        await conn.execute("UPDATE pending_deletions SET processed = TRUE WHERE id = $1", db_id)

async def db_count_total_users() -> int:
    global PG_POOL
    if PG_POOL is None:
        return 0
    async with PG_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT count(1) AS cnt FROM bot_users")
        return row["cnt"] if row else 0

async def db_count_active_in_last(hours: int = 48) -> int:
    global PG_POOL
    if PG_POOL is None:
        return 0
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    async with PG_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT count(1) AS cnt FROM bot_users WHERE last_active >= $1", cutoff)
        return row["cnt"] if row else 0

async def db_count_files_uploaded() -> int:
    global PG_POOL
    if PG_POOL is None:
        return 0
    async with PG_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM bot_stats WHERE key = 'files_uploaded'")
        return int(row["value"]) if row else 0

async def db_count_upload_sessions() -> int:
    global PG_POOL
    if PG_POOL is None:
        return 0
    async with PG_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM bot_stats WHERE key = 'upload_sessions'")
        return int(row["value"]) if row else 0

# ==========
# Utility helpers
# ==========

def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def generate_session_id(nbytes: int = 18) -> str:
    # url-safe token
    return secrets.token_urlsafe(nbytes)

def make_deep_link(session_id: str) -> str:
    # t.me/<botusername>?start=<session_id>
    # We will use bot.get_me() at startup to know bot username, but constructing here generically:
    return f"https://t.me/{(bot.username if hasattr(bot, 'username') else 'your_bot')}?start={session_id}"

def build_yes_no_markup(yes_callback: str, no_callback: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ YES", callback_data=yes_callback),
        InlineKeyboardButton("❌ NO", callback_data=no_callback)
    )
    return kb

async def safe_send(chat_id: int, *args, **kwargs) -> Optional[types.Message]:
    """
    Wrapper around bot.send_message / send_photo / send_document to catch exceptions and log.
    We'll try to detect type from kwargs and args; but mostly callers will pass through explicit bot methods.
    """
    try:
        if kwargs.get("photo") is not None:
            # send_photo
            return await bot.send_photo(chat_id, kwargs["photo"], caption=kwargs.get("caption"), reply_markup=kwargs.get("reply_markup"))
        if kwargs.get("document") is not None:
            return await bot.send_document(chat_id, kwargs["document"], caption=kwargs.get("caption"), reply_markup=kwargs.get("reply_markup"))
        if kwargs.get("video") is not None:
            return await bot.send_video(chat_id, kwargs["video"], caption=kwargs.get("caption"), reply_markup=kwargs.get("reply_markup"))
        # default to send_message
        return await bot.send_message(chat_id, *args, **kwargs)
    except Exception as e:
        logger.exception("Failed to send message to %s: %s", chat_id, e)
        return None

# ==========
# Health endpoint & webhook handler (aiohttp)
# ==========

async def healthcheck_handler(request):
    return web.Response(text="ok")

# Keep a reference to the web app runner so we can shutdown gracefully
WEBAPP_RUNNER = None

# ==========
# Auto-delete scheduling and worker
# ==========

async def schedule_deletion_for_message(session_id: str, chat_id: int, message_id: int, delay_minutes: int):
    """
    Schedule deletion for a specific message by scheduling a DB record and in-memory task.
    Persist to DB so it survives restarts.
    """
    scheduled_at = datetime.utcnow() + timedelta(minutes=delay_minutes)
    await db_add_pending_deletion(session_id, chat_id, message_id, scheduled_at)
    # We'll schedule in-memory runner (best-effort); on restart pending deletions from DB will be scheduled.
    # Append to in-memory mapping
    lst = SCHEDULED_DELETIONS.setdefault(session_id, [])
    lst.append({"chat_id": chat_id, "message_id": message_id, "scheduled_at": scheduled_at, "db_id": None})
    logger.info("Scheduled deletion: session=%s chat=%s message=%s at %s", session_id, chat_id, message_id, scheduled_at)

async def _deletion_worker_loop():
    """
    Periodically poll DB for pending deletions and execute those whose time has come.
    This is a resilient loop that runs in background.
    """
    logger.info("Starting deletion worker loop")
    while True:
        try:
            rows = await db_get_pending_deletions()
            now = datetime.utcnow()
            for row in rows:
                db_id = row["id"]
                session_id = row["session_id"]
                chat_id = int(row["chat_id"])
                message_id = int(row["message_id"])
                scheduled_at = row["scheduled_at"]
                if scheduled_at is None:
                    scheduled_at = now
                if scheduled_at <= now:
                    # attempt delete
                    try:
                        await bot.delete_message(chat_id, message_id)
                    except Exception as e:
                        logger.warning("Could not delete message %s in chat %s: %s", message_id, chat_id, e)
                    await db_mark_deletion_processed(db_id)
                    logger.info("Processed pending deletion id=%s session=%s chat=%s msg=%s", db_id, session_id, chat_id, message_id)
            # Sleep short time
            await asyncio.sleep(6)
        except asyncio.CancelledError:
            logger.info("Deletion worker cancelled")
            break
        except Exception as e:
            logger.exception("Error in deletion worker: %s", e)
            # Backoff a little to avoid tight loop on persistent DB problems
            await asyncio.sleep(5)

# ==========
# Bot command handlers and flows
# ==========

# Command: /start
@dp.message_handler(commands=["start"])
async def handle_start(message: types.Message):
    """
    /start command
    If user uses deep link parameter (start <session_id>) it will open session files.
    Otherwise it will send the start message and optional start image.
    """
    user_id = message.from_user.id
    args = message.get_args().strip()
    await db_add_or_touch_user(user_id)

    # If deep link param present, try to serve upload session
    if args:
        session_id = args
        # Serve deep link content
        await handle_deep_link_request(message, session_id)
        return

    # Else send regular start message with inline Help button
    msg = await db_get_message("start")
    text = msg.get("text") or "Welcome!"
    image = msg.get("image_file_id")
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(text="Help", callback_data="open_help"))
    try:
        if image:
            # send photo + caption
            await bot.send_photo(message.chat.id, image, caption=text, reply_markup=kb)
        else:
            await bot.send_message(message.chat.id, text, reply_markup=kb)
    except Exception as e:
        logger.exception("Failed to send start message: %s", e)
        await safe_send(message.chat.id, "Welcome! (failed to load start image)")

# Callback handler for inline Help button on start
@dp.callback_query_handler(lambda c: c.data == "open_help")
async def on_open_help_callback(callback_query: types.CallbackQuery):
    await callback_query.answer()
    # send help text
    msg = await db_get_message("help")
    text = msg.get("text") or "Help text is not configured."
    image = msg.get("image_file_id")
    if image:
        await bot.send_photo(callback_query.from_user.id, image, caption=text)
    else:
        await bot.send_message(callback_query.from_user.id, text)

# Command: /help
@dp.message_handler(commands=["help"])
async def handle_help(message: types.Message):
    await db_add_or_touch_user(message.from_user.id)
    msg = await db_get_message("help")
    text = msg.get("text") or "Help text is not configured."
    image = msg.get("image_file_id")
    if image:
        await bot.send_photo(message.chat.id, image, caption=text)
    else:
        await bot.send_message(message.chat.id, text)

# Command: /setmessage (owner only)
# Owner usage: /setmessage start|help <text>   OR reply to a text message with /setmessage start|help
@dp.message_handler(commands=["setmessage"])
async def cmd_setmessage(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only the owner can use this command.")
        return
    args = message.get_args().strip()
    # Support two forms:
    # 1) /setmessage start <text...>
    # 2) Reply to a message containing the text with /setmessage start
    key = None
    text_value = None
    if args:
        parts = args.split(None, 1)
        if len(parts) >= 1:
            key = parts[0].strip().lower()
            if len(parts) > 1:
                text_value = parts[1]
    if not key and message.reply_to_message:
        # if user wrote "/setmessage start" as reply
        await message.reply("Please specify which key (start or help). Example: /setmessage start This is start text")
        return
    if key not in ("start", "help"):
        await message.reply("Valid keys: start, help. Example: /setmessage start Welcome to my bot!")
        return
    if message.reply_to_message and not text_value:
        # take text from replied message
        if message.reply_to_message.text:
            text_value = message.reply_to_message.text
        else:
            await message.reply("Replied message contains no text. Provide text inline with the command.")
            return
    if not text_value:
        await message.reply("No text provided. Example: /setmessage help This is help text.")
        return
    await db_set_message(key, text_value, None)
    await message.reply(f"{key} message updated.")

# Command: /setimage (owner only)
# Owner usage: reply to an image with "/setimage start" or "/setimage help"
@dp.message_handler(commands=["setimage"])
async def cmd_setimage(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only the owner can use this command.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a photo/document that you want to set as start/help image, then run: /setimage start OR /setimage help")
        return
    args = message.get_args().strip().lower()
    if args not in ("start", "help"):
        await message.reply("Usage: reply to an image with /setimage start OR /setimage help")
        return
    # Accept photo or document with image mime
    target = message.reply_to_message
    file_id = None
    if target.photo:
        # choose largest size
        file_id = target.photo[-1].file_id
    elif target.document:
        # check mime type begins with image/
        if target.document.mime_type and target.document.mime_type.startswith("image"):
            file_id = target.document.file_id
        else:
            await message.reply("Replied document is not an image. Please reply to an actual image file.")
            return
    else:
        await message.reply("Replied message has no photo/document. Please reply to an image.")
        return
    # Ask owner whether to set start or help OR use provided arg directly
    await db_set_message(args, None, file_id)
    await message.reply(f"Image for '{args}' updated (file_id saved).")

# Command: /broadcast (owner only)
@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    """
    Owner can call /broadcast and reply to a message to broadcast that message to all users.
    The bot will copy contents and inline buttons (no forward tags).
    """
    if not is_owner(message.from_user.id):
        await message.reply("Only the owner may use this command.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to the message you want to broadcast (text/photo/document) with /broadcast")
        return
    # Build a list of target users
    await message.reply("Broadcast starting... fetching user list (this may take a while).")
    # Fetch users
    global PG_POOL
    if PG_POOL is None:
        await message.reply("Database not initialized.")
        return
    async with PG_POOL.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM bot_users")
        user_ids = [int(r["user_id"]) for r in rows]
    total = len(user_ids)
    sent = 0
    failed = 0
    # We'll iterate and try to send; to avoid hitting rate limits, put small sleep occasionally
    replied = message.reply_to_message
    # Convert the replied message to copying method: if photo->send_photo, document->send_document, text->send_message, video->send_video
    for idx, uid in enumerate(user_ids):
        try:
            # Owner should not receive double broadcast, but he may. No special case.
            if replied.photo:
                # send photo with same caption and same file_id
                file_id = replied.photo[-1].file_id
                caption = replied.caption or ""
                await bot.send_photo(uid, file_id, caption=caption)
            elif replied.document:
                file_id = replied.document.file_id
                caption = replied.caption or ""
                await bot.send_document(uid, file_id, caption=caption)
            elif replied.video:
                file_id = replied.video.file_id
                caption = replied.caption or ""
                await bot.send_video(uid, file_id, caption=caption)
            else:
                text = replied.text or replied.caption or ""
                if hasattr(replied, "reply_markup") and replied.reply_markup:
                    # try to reproduce inline keyboard (safe copy)
                    await bot.send_message(uid, text, reply_markup=replied.reply_markup)
                else:
                    await bot.send_message(uid, text)
            sent += 1
        except Exception as e:
            logger.exception("Broadcast failed for user %s: %s", uid, e)
            failed += 1
        # Simple throttle
        if idx % 60 == 0:
            await asyncio.sleep(0.8)
    await message.reply(f"Broadcast complete. Attempted: {total}, Sent: {sent}, Failed: {failed}")

# Command: /stats (owner only)
@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only the owner may access stats.")
        return
    total = await db_count_total_users()
    active48 = await db_count_active_in_last(48)
    files_uploaded = await db_count_files_uploaded()
    upload_sessions = await db_count_upload_sessions()
    text = (
        f"📊 Bot Statistics\n\n"
        f"• Total users: {total}\n"
        f"• Active in last 48h: {active48}\n"
        f"• Files uploaded (total): {files_uploaded}\n"
        f"• Upload sessions created: {upload_sessions}\n"
    )
    await message.reply(text)

# Command: /upload (multi-step flow for owner)
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only the owner can start upload sessions.")
        return
    owner = message.from_user.id
    # Start a new in-memory session
    session_id = generate_session_id()
    ACTIVE_UPLOAD_SESSIONS[owner] = {
        "session_id": session_id,
        "files": [],
        "owner_id": owner,
        "created_at": datetime.utcnow(),
        "protect_content": False,
        "auto_delete_minutes": 0
    }
    await message.reply(
        f"Upload session started. Session ID: {session_id}\n\n"
        "Please send files (photos, documents, videos). Send as many as you want. "
        "When finished send /d (done) or /c (cancel)."
    )

# Help commands for owner to cancel upload
@dp.message_handler(commands=["c"])
async def cmd_upload_cancel(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    owner = message.from_user.id
    if owner not in ACTIVE_UPLOAD_SESSIONS:
        await message.reply("No active upload session to cancel.")
        return
    del ACTIVE_UPLOAD_SESSIONS[owner]
    await message.reply("Upload session cancelled and cleaned up.")

@dp.message_handler(commands=["d"])
async def cmd_upload_done(message: types.Message):
    """
    Owner signals that they finished sending files. Next we ask options (Protect Content? Auto-delete minutes).
    """
    if not is_owner(message.from_user.id):
        return
    owner = message.from_user.id
    sess = ACTIVE_UPLOAD_SESSIONS.get(owner)
    if not sess:
        await message.reply("No active upload session. Start one with /upload")
        return
    if not sess["files"]:
        await message.reply("You haven't sent any files. Upload files or cancel with /c")
        return
    # Ask Protect Content (YES/NO)
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Protect: YES", callback_data=f"protect_yes|{sess['session_id']}"),
        InlineKeyboardButton("Protect: NO", callback_data=f"protect_no|{sess['session_id']}")
    )
    await message.reply("Protect content? (If protected, viewers cannot forward/save via the normal client - attempt is made by sending media with protect flag where supported)", reply_markup=kb)

# Callback handlers for protect YES/NO selection
@dp.callback_query_handler(lambda c: c.data and c.data.startswith("protect_yes|") or (c.data and c.data.startswith("protect_no|")))
async def on_protect_choice(cb: types.CallbackQuery):
    await cb.answer()
    data = cb.data
    parts = data.split("|", 1)
    if len(parts) != 2:
        await cb.message.reply("Invalid callback.")
        return
    choice_raw, session_id = parts
    protect_choice = choice_raw == "protect_yes"
    # find session
    owner = cb.from_user.id
    sess = ACTIVE_UPLOAD_SESSIONS.get(owner)
    if not sess or sess["session_id"] != session_id:
        await cb.message.reply("Session not found or already processed.")
        return
    sess["protect_content"] = protect_choice
    # Ask for auto-delete timer
    await cb.message.reply("Set auto-delete timer in minutes (0 means never auto-delete). Reply with a number between 0 and 10080 (max 1 week).")

    # Set next expected state by associating input handler registration; we'll rely on message text handler to pick up numbers from owner chat.

# Number reply handler for auto-delete minutes (owner only)
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and m.text and m.text.strip().isdigit())
async def on_owner_set_autodelete(message: types.Message):
    owner = message.from_user.id
    sess = ACTIVE_UPLOAD_SESSIONS.get(owner)
    if not sess:
        # not always numeric messages are for autodel but this handler only triggers if owner and digits; ok to ignore
        return
    try:
        minutes = int(message.text.strip())
    except:
        await message.reply("Please reply with an integer number of minutes (0-10080).")
        return
    if minutes < 0 or minutes > MAX_ALLOWED_AUTODELETE:
        await message.reply(f"Please provide a number between 0 and {MAX_ALLOWED_AUTODELETE}.")
        return
    sess["auto_delete_minutes"] = minutes
    # Persist session in DB and copy files to upload channel
    session_id = sess["session_id"]
    protect_content = sess["protect_content"]
    auto_delete_minutes = sess["auto_delete_minutes"]
    # persist session
    await db_create_upload_session(session_id, owner, protect_content, auto_delete_minutes)
    # Copy files to upload channel (archive)
    files = sess["files"]
    if not files:
        await message.reply("No files found in session (this should not happen).")
        del ACTIVE_UPLOAD_SESSIONS[owner]
        return
    await message.reply("Archiving files to upload channel and generating deep link. This may take a little while...")
    deep_link = None
    try:
        for f in files:
            # Post each file to channel preserving caption
            ftype = f.get("file_type")
            fid = f.get("file_id")
            caption = f.get("caption") or ""
            try:
                if ftype == "photo":
                    await bot.send_photo(UPLOAD_CHANNEL_ID, fid, caption=caption)
                elif ftype == "video":
                    await bot.send_video(UPLOAD_CHANNEL_ID, fid, caption=caption)
                elif ftype == "audio":
                    await bot.send_audio(UPLOAD_CHANNEL_ID, fid, caption=caption)
                else:
                    # document or fallback
                    await bot.send_document(UPLOAD_CHANNEL_ID, fid, caption=caption)
            except Exception as e:
                logger.exception("Failed to archive file to channel for session %s: %s", session_id, e)
            # store file metadata in DB
            try:
                await db_add_upload_file(session_id, fid, ftype, caption, None)
            except Exception:
                logger.exception("db_add_upload_file failed for session %s", session_id)
        # Build deep link
        # Try to get bot username (bot.get_me()) potential network call; we'll create a t.me link using bot.username if available
        try:
            me = await bot.get_me()
            bot_username = me.username
        except Exception:
            bot_username = None
        if bot_username:
            deep_link = f"https://t.me/{bot_username}?start={session_id}"
        else:
            deep_link = f"https://t.me/your_bot?start={session_id}"
        # Persist done
        await message.reply(f"Upload session created. Deep link:\n\n{deep_link}\n\nAnyone with the link can access the uploaded files.")
    except Exception as e:
        logger.exception("Failed during finalize upload: %s", e)
        await message.reply("An error occurred while finalizing the upload session.")
    finally:
        # cleanup in-memory session
        del ACTIVE_UPLOAD_SESSIONS[owner]

# Handler for owner sending files during upload session
@dp.message_handler(content_types=types.ContentType.ANY)
async def catch_files_during_upload(message: types.Message):
    """
    This handler will accept files sent by the owner while an ACTIVE_UPLOAD_SESSIONS entry exists.
    It captures photos, documents, videos, audio, voice, etc.
    If the sender is not owner or no active session, it falls through for other handlers.
    """
    owner = message.from_user.id
    if owner not in ACTIVE_UPLOAD_SESSIONS:
        # Not in upload session; ignore and allow other handlers to process
        return
    # Only accept media types
    sess = ACTIVE_UPLOAD_SESSIONS[owner]
    added = False
    caption = getattr(message, "caption", None) or ""
    if message.photo:
        fid = message.photo[-1].file_id
        sess["files"].append({"file_id": fid, "file_type": "photo", "caption": caption})
        added = True
    elif message.document:
        fid = message.document.file_id
        sess["files"].append({"file_id": fid, "file_type": "document", "caption": caption})
        added = True
    elif message.video:
        fid = message.video.file_id
        sess["files"].append({"file_id": fid, "file_type": "video", "caption": caption})
        added = True
    elif message.audio:
        fid = message.audio.file_id
        sess["files"].append({"file_id": fid, "file_type": "audio", "caption": caption})
        added = True
    elif message.voice:
        fid = message.voice.file_id
        sess["files"].append({"file_id": fid, "file_type": "voice", "caption": caption})
        added = True
    else:
        # Not a supported upload file type; ignore
        return
    if added:
        await message.reply(f"File received and added to session {sess['session_id']}. Send /d when done or send more files.")

# Handle deep link requests: when user sends /start <session_id> or clicks the t.me link
async def handle_deep_link_request(message: types.Message, session_id: str):
    """
    Look up session, serve files to user.
    Apply protect_content and auto-delete logic (unless owner bypass).
    """
    await db_add_or_touch_user(message.from_user.id)
    session = await db_get_session(session_id)
    if not session or not session.get("is_active", True):
        await message.reply("Sorry, that session does not exist or has been disabled.")
        return
    files = await db_get_upload_files(session_id)
    if not files:
        await message.reply("No files found for this link.")
        return
    protect = bool(session.get("protect_content"))
    auto_delete_minutes = int(session.get("auto_delete_minutes") or 0)
    owner = int(session.get("owner_id"))
    user_id = message.from_user.id
    # Owner bypass:
    owner_bypass = (user_id == owner)
    sent_message_ids: List[int] = []
    # Send each file in the private chat
    for f in files:
        fid = f["file_id"]
        ftype = f.get("file_type") or "document"
        caption = f.get("caption") or ""
        try:
            # If protect_content is True and platform supports MIMETYPE_DISABLE_FORWARD, aiogram can't control that directly.
            # However, we can use `protect_content` param in send_message methods for newer Bot API versions.
            # aiogram v2 might not include protect_content argument; use try/except.
            if ftype == "photo":
                try:
                    # try to send with protect_content attribute if available
                    msg = await bot.send_photo(message.chat.id, fid, caption=caption, protect_content=protect)
                except TypeError:
                    msg = await bot.send_photo(message.chat.id, fid, caption=caption)
            elif ftype == "video":
                try:
                    msg = await bot.send_video(message.chat.id, fid, caption=caption, protect_content=protect)
                except TypeError:
                    msg = await bot.send_video(message.chat.id, fid, caption=caption)
            elif ftype == "audio":
                try:
                    msg = await bot.send_audio(message.chat.id, fid, caption=caption, protect_content=protect)
                except TypeError:
                    msg = await bot.send_audio(message.chat.id, fid, caption=caption)
            else:
                # document / other
                try:
                    msg = await bot.send_document(message.chat.id, fid, caption=caption, protect_content=protect)
                except TypeError:
                    msg = await bot.send_document(message.chat.id, fid, caption=caption)
            if msg:
                sent_message_ids.append(msg.message_id)
        except Exception as e:
            logger.exception("Failed to send file %s to user %s: %s", fid, message.from_user.id, e)
            await message.reply("Failed to send some files due to an error.")
    # If auto_delete_minutes > 0 and not owner bypass, schedule deletions for messages we just sent
    if auto_delete_minutes > 0 and not owner_bypass:
        for mid in sent_message_ids:
            try:
                # persist deletion and schedule worker will pick up and delete
                await schedule_deletion_for_message(session_id, message.chat.id, mid, auto_delete_minutes)
            except Exception:
                logger.exception("Failed to schedule deletion for message %s", mid)
        # Inform user about auto-delete
        await message.reply(f"These files will be automatically deleted from this chat after {auto_delete_minutes} minutes.")
    else:
        if owner_bypass and protect:
            # Inform owner bypass
            await message.reply("Owner bypass: protect and auto-delete options are ignored for the owner.")
        elif auto_delete_minutes == 0:
            await message.reply("These files will remain in this chat permanently (according to session settings).")

# ==========
# Startup & shutdown
# ==========

DELETION_WORKER_TASK: Optional[asyncio.Task] = None

async def on_startup(dp):
    """
    Called by executor.start_webhook; initialize DB, set webhook, schedule deletions worker.
    """
    global PG_POOL, DELETION_WORKER_TASK, bot
    logger.info("Starting up...")

    # Initialize DB pool
    try:
        await init_db_pool()
    except Exception:
        logger.exception("Database initialization failed on startup.")
        raise

    # Attempt to set webhook if RENDER_EXTERNAL_URL provided
    if RENDER_EXTERNAL_URL:
        webhook_url = f"{RENDER_EXTERNAL_URL}{WEBHOOK_PATH}"
        try:
            # set webhook to webhook_url
            await bot.set_webhook(webhook_url)
            logger.info("Webhook set to %s", webhook_url)
        except Exception as e:
            logger.exception("Failed to set webhook to %s: %s", webhook_url, e)

    # Cache bot username for deep link generation
    try:
        me = await bot.get_me()
        bot.username = me.username
        logger.info("Bot username cached: %s", me.username)
    except Exception as e:
        logger.exception("bot.get_me() failed: %s", e)

    # Start deletion worker
    loop = asyncio.get_event_loop()
    DELETION_WORKER_TASK = loop.create_task(_deletion_worker_loop())
    logger.info("Deletion worker started.")

async def on_shutdown(dp):
    """
    Clean shutdown: cancel background tasks, close DB pool, remove webhook.
    """
    global PG_POOL, DELETION_WORKER_TASK, bot
    logger.info("Shutting down...")

    if DELETION_WORKER_TASK:
        DELETION_WORKER_TASK.cancel()
        try:
            await DELETION_WORKER_TASK
        except Exception:
            pass

    # Remove webhook
    try:
        await bot.delete_webhook()
    except Exception:
        pass

    # Close DB pool
    if PG_POOL:
        await PG_POOL.close()
        logger.info("DB pool closed.")

    # Close bot session
    try:
        await bot.close()
    except Exception:
        pass

# ==========
# Web server for webhook & health
# ==========

def create_web_app():
    """
    Create aiohttp web app to serve health endpoint and pass webhook requests to aiogram.
    Note: executor.start_webhook uses its own aiohttp runner; we register health endpoints with dp.start_webhook.
    However, to be explicit for Render, we create an app to handle health path separately here if needed.
    """
    app = web.Application()
    app.router.add_get("/", healthcheck_handler)
    app.router.add_get("/health", healthcheck_handler)
    return app

# ==========
# Main entrypoint
# ==========

if __name__ == "__main__":
    """
    Run the webhook-only bot using aiogram's executor.start_webhook.
    Ensure Render will route HTTPS requests to our webhook path.
    """

    # Because executor.start_webhook will create its own webserver we provide required args.
    # We still create a small web app for health if desired, but aiogram will host this on the configured host/port.

    # Build webhook route path
    hook_path = WEBHOOK_PATH
    if not hook_path.startswith("/"):
        hook_path = "/" + hook_path

    logger.info("Starting bot in webhook mode. Hook path: %s, port: %s", hook_path, WEBHOOK_PORT)

    # Execute with executor.start_webhook
    try:
        # Ensure we have on_startup and on_shutdown coroutines defined
        executor.start_webhook(
            dispatcher=dp,
            webhook_path=hook_path,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            skip_updates=True,
            host=APP_HOST,
            port=WEBHOOK_PORT,
        )
    except Exception as e:
        logger.exception("Failed to start webhook: %s", e)
        # Attempt fallback: keep running
        sys.exit(1)

# End of file