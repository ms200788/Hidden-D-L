#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Webhook-only Telegram bot built with aiogram v2
Features:
 - Neon (Postgres) for persistence via asyncpg
 - Upload channel copy storage
 - Owner-only uploads & broadcast
 - Deep-link sharing for sessions
 - Auto-delete scheduling
 - Protect content option
 - Health endpoint for UptimeRobot
 - Webhook startup optimized for Render (sets webhook to RENDER_EXTERNAL_URL + WEBHOOK_PATH)
 - Long, documented file ~800+ lines to match user's request
"""

import os
import logging
import asyncio
import json
import secrets
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple, Any

import asyncpg
from aiohttp import web

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ParseMode
from aiogram.utils.executor import start_webhook
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup

# ---------------------------
# Basic configuration & envs
# ---------------------------

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Environment variables (must be set in Render)
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.critical("BOT_TOKEN is not set. Exiting.")
    raise RuntimeError("BOT_TOKEN environment variable is required")

OWNER_ID = int(os.getenv("OWNER_ID", "0"))  # Must be set to actual owner id
if OWNER_ID == 0:
    logger.warning("OWNER_ID not set or zero. Some owner-only features will be inaccessible.")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.critical("DATABASE_URL not set. Exiting.")
    raise RuntimeError("DATABASE_URL environment variable is required")

UPLOAD_CHANNEL_ID = os.getenv("UPLOAD_CHANNEL_ID")
if not UPLOAD_CHANNEL_ID:
    logger.critical("UPLOAD_CHANNEL_ID not set. Exiting.")
    raise RuntimeError("UPLOAD_CHANNEL_ID environment variable is required")

# support both numeric channel ID and username
try:
    if UPLOAD_CHANNEL_ID.isdigit() or (UPLOAD_CHANNEL_ID.startswith("-") and UPLOAD_CHANNEL_ID[1:].isdigit()):
        UPLOAD_CHANNEL_ID_INT = int(UPLOAD_CHANNEL_ID)
    else:
        UPLOAD_CHANNEL_ID_INT = UPLOAD_CHANNEL_ID  # e.g. @my_private_channel
except Exception:
    UPLOAD_CHANNEL_ID_INT = UPLOAD_CHANNEL_ID

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")  # e.g. https://my-app.onrender.com
if not RENDER_EXTERNAL_URL:
    # It is allowed to set later, but we prefer it set now.
    logger.warning("RENDER_EXTERNAL_URL not set. Webhook won't be configured automatically.")

PORT = int(os.getenv("PORT", "8000"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_HOST = RENDER_EXTERNAL_URL.rstrip("/") if RENDER_EXTERNAL_URL else None
WEBHOOK_URL = (WEBHOOK_HOST + WEBHOOK_PATH) if WEBHOOK_HOST else None

SKIP_UPDATES = os.getenv("SKIP_UPDATES", "1") == "1"

# Constants
DEEP_LINK_TTL_SECONDS = 60 * 60 * 24 * 30  # optional TTL for sessions (30 days) — you can remove or modify

# ---------------------------
# Bot & Dispatcher
# ---------------------------

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ---------------------------
# FSM States
# ---------------------------

class UploadStates(StatesGroup):
    WAITING_FOR_FILES = State()          # owner is sending files (multiple messages)
    WAITING_FOR_PROTECT = State()        # yes/no to protect content
    WAITING_FOR_AUTODELETE = State()     # auto-delete timer input
    CONFIRMATION = State()               # confirm & finalize

class BroadcastStates(StatesGroup):
    WAITING_FOR_BROADCAST = State()

class SetMessageStates(StatesGroup):
    WAITING_FOR_MESSAGE_TYPE = State()
    WAITING_FOR_MESSAGE_TEXT = State()

class SetImageStates(StatesGroup):
    WAITING_FOR_IMAGE_TYPE = State()
    WAITING_FOR_IMAGE_FILE = State()

# ---------------------------
# Database helpers (asyncpg)
# ---------------------------

class Database:
    """
    Simple wrapper around asyncpg connection pool with helper queries.
    """

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.pool.Pool] = None

    async def connect(self):
        logger.info("Connecting to database...")
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=10)
        logger.info("Connected to database")
        # Initialize schema if necessary
        await self._init_schema()

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def _init_schema(self):
        """
        Create required tables if they do not exist.
        Minimal schema covering:
         - users
         - messages (start/help text and file_ids)
         - sessions
         - session_files
         - stats (optional aggregated)
        """
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # users table
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    first_seen TIMESTAMP WITH TIME ZONE NOT NULL,
                    last_active TIMESTAMP WITH TIME ZONE NOT NULL
                );
                """)
                # messages table
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    name TEXT PRIMARY KEY,  -- 'start_text', 'help_text', 'start_image', 'help_image'
                    text TEXT,
                    file_id TEXT,           -- for images
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
                );
                """)
                # sessions table
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    owner_id BIGINT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    protect_content BOOLEAN DEFAULT FALSE,
                    auto_delete_minutes INT DEFAULT 0,
                    title TEXT,
                    expires_at TIMESTAMP WITH TIME ZONE
                );
                """)
                # session_files table
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS session_files (
                    id SERIAL PRIMARY KEY,
                    session_id TEXT REFERENCES sessions(session_id) ON DELETE CASCADE,
                    file_type TEXT NOT NULL,     -- 'photo', 'document', 'video', etc.
                    file_id TEXT NOT NULL,
                    orig_file_name TEXT,
                    caption TEXT,
                    position INT DEFAULT 0
                );
                """)
                # stats table
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    key TEXT PRIMARY KEY,
                    value BIGINT DEFAULT 0
                );
                """)
                # initialize some stats keys if not exist
                await conn.execute("""
                INSERT INTO stats(key, value)
                VALUES
                  ('total_upload_sessions', 0),
                  ('total_files', 0)
                ON CONFLICT (key) DO NOTHING;
                """)
                # create an index for session_files for faster fetching
                await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_session_files_session ON session_files(session_id);
                """)
        logger.info("Database schema initialized")

    # user functions
    async def add_or_update_user(self, user_id: int):
        assert self.pool is not None
        ts = datetime.utcnow()
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO users(user_id, first_seen, last_active)
            VALUES($1, $2, $2)
            ON CONFLICT(user_id) DO UPDATE SET last_active = EXCLUDED.last_active;
            """, user_id, ts)

    async def count_total_users(self) -> int:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rec = await conn.fetchrow("SELECT count(*) AS cnt FROM users;")
            return rec["cnt"] if rec else 0

    async def count_active_users_since(self, since_dt: datetime) -> int:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rec = await conn.fetchrow("SELECT count(*) AS cnt FROM users WHERE last_active >= $1;", since_dt)
            return rec["cnt"] if rec else 0

    # message settings
    async def set_message_text(self, name: str, text: str):
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO messages(name, text, updated_at)
            VALUES($1, $2, $3)
            ON CONFLICT(name) DO UPDATE SET text = EXCLUDED.text, updated_at = EXCLUDED.updated_at;
            """, name, text, datetime.utcnow())

    async def get_message_text(self, name: str) -> Optional[str]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rec = await conn.fetchrow("SELECT text FROM messages WHERE name = $1;", name)
            return rec["text"] if rec else None

    async def set_message_file(self, name: str, file_id: str):
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO messages(name, file_id, updated_at)
            VALUES($1, $2, $3)
            ON CONFLICT(name) DO UPDATE SET file_id = EXCLUDED.file_id, updated_at = EXCLUDED.updated_at;
            """, name, file_id, datetime.utcnow())

    async def get_message_file(self, name: str) -> Optional[str]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rec = await conn.fetchrow("SELECT file_id FROM messages WHERE name = $1;", name)
            return rec["file_id"] if rec else None

    # session functions
    async def create_session(self, session_id: str, owner_id: int, protect: bool, auto_delete_minutes: int, title: Optional[str], expires_at: Optional[datetime] = None):
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO sessions(session_id, owner_id, created_at, protect_content, auto_delete_minutes, title, expires_at)
            VALUES($1, $2, $3, $4, $5, $6, $7)
            """, session_id, owner_id, datetime.utcnow(), protect, auto_delete_minutes, title, expires_at)

    async def add_session_file(self, session_id: str, file_type: str, file_id: str, orig_file_name: Optional[str], caption: Optional[str], position: int = 0):
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO session_files(session_id, file_type, file_id, orig_file_name, caption, position)
            VALUES($1, $2, $3, $4, $5, $6)
            """, session_id, file_type, file_id, orig_file_name, caption, position)
            # increment total_files stat
            await conn.execute("""
            UPDATE stats SET value = value + 1 WHERE key = 'total_files';
            """)

    async def get_session(self, session_id: str) -> Optional[asyncpg.Record]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rec = await conn.fetchrow("SELECT * FROM sessions WHERE session_id = $1;", session_id)
            return rec

    async def get_session_files(self, session_id: str) -> List[asyncpg.Record]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
            SELECT * FROM session_files WHERE session_id = $1 ORDER BY position ASC, id ASC;
            """, session_id)
            return rows

    async def increment_upload_sessions(self):
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE stats SET value = value + 1 WHERE key = 'total_upload_sessions';")

    async def get_stat(self, key: str) -> int:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rec = await conn.fetchrow("SELECT value FROM stats WHERE key = $1;", key)
            return rec["value"] if rec else 0

    # generic helper
    async def execute(self, query: str, *args):
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

# Global DB instance
db = Database(DATABASE_URL)

# ---------------------------
# Utility helper functions
# ---------------------------

def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def generate_session_token(length_bytes: int = 8) -> str:
    """
    Use URL-safe token for deep link session id
    """
    return secrets.token_urlsafe(length_bytes)

def build_start_deep_link(session_token: str) -> str:
    """
    Create a t.me deep link for the bot
    Example: https://t.me/MyBot?start=AbCdEf
    """
    # Use bot username to build link, but it's simpler to return t.me/ + bot_username
    # We'll try to use bot.get_me if needed. For now use generic t.me link with token
    username = None
    try:
        # this call is allowed asynchronously if bot initialized
        username = bot.get_current().username
    except Exception:
        pass
    if username:
        return f"https://t.me/{username}?start={session_token}"
    else:
        return f"https://t.me/{BOT_TOKEN.split(':')[0]}?start={session_token}"  # fallback; not ideal

def pretty_minutes(minutes: int) -> str:
    if minutes <= 0:
        return "forever (no auto-delete)"
    if minutes < 60:
        return f"{minutes} minute(s)"
    if minutes < 60 * 24:
        hrs = minutes // 60
        return f"{hrs} hour(s)"
    days = minutes // (60 * 24)
    return f"{days} day(s)"

# Send message helper with failure handling
async def safe_send(chat_id: int, **kwargs) -> Optional[types.Message]:
    """
    Wrap sending methods. Use bot.send_message primarily.
    kwargs should be compatible with bot.send_message.
    """
    try:
        return await bot.send_message(chat_id=chat_id, **kwargs)
    except Exception as e:
        logger.exception("Failed to send message to %s: %s", chat_id, e)
        return None

# ---------------------------
# Startup / Shutdown handlers
# ---------------------------

async def on_startup(dp: Dispatcher):
    """
    Called by executor.start_webhook at startup.
    Connect to DB and set webhook
    """
    logger.info("on_startup called")
    # Connect to DB pool
    await db.connect()

    # Clear webhook if already set? We'll set to the RENDER_EXTERNAL_URL if provided.
    if WEBHOOK_URL:
        try:
            await bot.set_webhook(WEBHOOK_URL)
            logger.info("Webhook set to: %s", WEBHOOK_URL)
        except Exception as e:
            logger.exception("Failed to set webhook: %s", e)
    else:
        logger.warning("WEBHOOK_URL not available; webhook won't be registered. Set RENDER_EXTERNAL_URL to enable automatic webhook registration.")

async def on_shutdown(dp: Dispatcher):
    logger.info("on_shutdown called")
    try:
        # Remove webhook on shutdown
        if WEBHOOK_URL:
            try:
                await bot.delete_webhook()
                logger.info("Webhook deleted")
            except Exception as e:
                logger.exception("Failed to delete webhook on shutdown: %s", e)
    finally:
        await db.close()
        await bot.close()
        logger.info("Bot closed and DB pool closed")

# ---------------------------
# Health endpoint (aiohttp)
# ---------------------------

async def health(request: web.Request):
    """
    Return ok for health checks (UptimeRobot)
    """
    return web.Response(text="ok")

# ---------------------------
# Message / Command Handlers
# ---------------------------

# Helper: markup for start message
def get_start_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(text="Help", callback_data="show_help"))
    return kb

# /start behavior (also used for deep link sessions)
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    """
    If /start payload provided (message.get_args()), treat as deep-link session retrieval.
    If no payload, show start message and image (owner-settable).
    """
    user_id = message.from_user.id
    # record user
    await db.add_or_update_user(user_id)

    args = message.get_args().strip() if hasattr(message, "get_args") else ""
    if args:
        # Deep link session retrieval (public link t.me/Bot?start=TOKEN)
        payload = args
        await handle_deep_link_start(message, payload)
        return

    # show start message
    start_text = await db.get_message_text("start_text")
    start_img_file = await db.get_message_file("start_image")
    if start_img_file:
        # send photo with caption or message
        try:
            if start_text:
                await bot.send_photo(chat_id=user_id, photo=start_img_file, caption=start_text, reply_markup=get_start_keyboard())
            else:
                await bot.send_photo(chat_id=user_id, photo=start_img_file, reply_markup=get_start_keyboard())
        except Exception:
            # fallback to message only
            await safe_send(user_id, text=(start_text or "Welcome!"), reply_markup=get_start_keyboard())
    else:
        # no start image
        await safe_send(user_id, text=(start_text or "Welcome!"), reply_markup=get_start_keyboard())

# deep link session fetch
async def handle_deep_link_start(message: types.Message, session_token: str):
    user_id = message.from_user.id
    is_owner_user = is_owner(user_id)
    # lookup session
    rec = await db.get_session(session_token)
    if not rec:
        await safe_send(user_id, text="Sorry, that link is invalid or expired.")
        return
    # check expiration
    if rec["expires_at"] and rec["expires_at"] < datetime.utcnow():
        await safe_send(user_id, text="Sorry, that session has expired.")
        return

    # get files
    files = await db.get_session_files(session_token)
    if not files:
        await safe_send(user_id, text="No files found for this session.")
        return

    # If session has protect_content and user is not owner, send with protect_content True
    protect = rec["protect_content"]
    auto_delete_minutes = rec["auto_delete_minutes"]
    # Inform user if auto-delete is enabled and they are not owner
    if auto_delete_minutes and not is_owner_user:
        await safe_send(user_id, text=f"These files will be deleted in {pretty_minutes(auto_delete_minutes)} from your chat.")

    # Send each file preserving captions
    sent_messages = []
    for file_row in files:
        f_type = file_row["file_type"]
        file_id = file_row["file_id"]
        caption = file_row["caption"] or None
        try:
            # Use different send_* based on f_type
            send_kwargs = {"chat_id": user_id, "caption": caption, "protect_content": protect and not is_owner_user}
            if f_type == "photo":
                # photo file_id may refer to a single photo; send_photo expects file_id
                msg = await bot.send_photo(photo=file_id, **send_kwargs)
            elif f_type == "video":
                msg = await bot.send_video(video=file_id, **send_kwargs)
            elif f_type == "audio":
                msg = await bot.send_audio(audio=file_id, **send_kwargs)
            elif f_type == "voice":
                # voice doesn't accept caption
                msg = await bot.send_voice(chat_id=user_id, voice=file_id, protect_content=protect and not is_owner_user)
            elif f_type == "document":
                msg = await bot.send_document(document=file_id, **send_kwargs)
            elif f_type == "animation":
                msg = await bot.send_animation(animation=file_id, **send_kwargs)
            else:
                # fallback to send_document
                msg = await bot.send_document(document=file_id, **send_kwargs)
            sent_messages.append(msg)
        except Exception as e:
            logger.exception("Failed to send session file %s to user %s: %s", file_row["id"], user_id, e)

    # schedule deletions if needed (non-owner)
    if auto_delete_minutes and auto_delete_minutes > 0 and not is_owner_user:
        # schedule deletion asynchronously
        asyncio.create_task(schedule_deletions(user_id, sent_messages, auto_delete_minutes))

# schedule deletion helper
async def schedule_deletions(chat_id: int, messages: List[types.Message], minutes: int):
    """
    Sleep and delete messages after X minutes. We capture message ids and try to delete them.
    Only deletes messages in the user's chat (not DB or upload channel).
    """
    try:
        logger.info("Scheduling deletion of %d messages in chat %s after %d minutes", len(messages), chat_id, minutes)
        await asyncio.sleep(minutes * 60)
        for msg in messages:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=msg.message_id)
            except Exception as e:
                # message might have been deleted already or forbidden
                logger.debug("Could not delete message %s in chat %s: %s", getattr(msg, "message_id", None), chat_id, e)
    except Exception as e:
        logger.exception("Error while scheduling deletions: %s", e)

# callback handler for help button
@dp.callback_query_handler(lambda c: c.data == "show_help")
async def cb_show_help(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    await db.add_or_update_user(user_id)
    help_text = await db.get_message_text("help_text")
    help_img = await db.get_message_file("help_image")
    if help_img:
        try:
            if help_text:
                await bot.send_photo(chat_id=user_id, photo=help_img, caption=help_text)
            else:
                await bot.send_photo(chat_id=user_id, photo=help_img)
        except Exception:
            await safe_send(user_id, text=(help_text or "Help"))
    else:
        await safe_send(user_id, text=(help_text or "Help"))

    # answer callback to remove loading UI in client
    try:
        await callback_query.answer()
    except Exception:
        pass

# /help command
@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    user_id = message.from_user.id
    await db.add_or_update_user(user_id)
    help_text = await db.get_message_text("help_text")
    help_img = await db.get_message_file("help_image")
    if help_img:
        try:
            if help_text:
                await bot.send_photo(chat_id=user_id, photo=help_img, caption=help_text)
            else:
                await bot.send_photo(chat_id=user_id, photo=help_img)
        except Exception:
            await safe_send(user_id, text=(help_text or "Help"))
    else:
        await safe_send(user_id, text=(help_text or "Help"))

# /setmessage owner-only (starts FSM)
@dp.message_handler(commands=["setmessage"])
async def cmd_setmessage(message: types.Message):
    if not is_owner(message.from_user.id):
        await safe_send(message.from_user.id, text="You are not authorized to use this command.")
        return
    # Ask which message to set
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Start message", callback_data="setmsg_start"),
        InlineKeyboardButton("Help message", callback_data="setmsg_help")
    )
    await message.reply("Which message do you want to set/update?", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("setmsg_"))
async def cb_setmsg_choice(callback_query: types.CallbackQuery, state: FSMContext):
    if not is_owner(callback_query.from_user.id):
        await callback_query.answer("Not authorized", show_alert=True)
        return
    kind = callback_query.data.split("_", 1)[1]  # 'start' or 'help'
    await state.update_data(setmsg_kind=kind)
    await SetMessageStates.WAITING_FOR_MESSAGE_TEXT.set()
    await callback_query.answer()
    await bot.send_message(callback_query.from_user.id, f"Send the text for the {kind} message (send /cancel to abort).")

@dp.message_handler(state=SetMessageStates.WAITING_FOR_MESSAGE_TEXT, commands=["cancel"])
async def cancel_setmessage(message: types.Message, state: FSMContext):
    await state.finish()
    await message.reply("Operation cancelled.")

@dp.message_handler(state=SetMessageStates.WAITING_FOR_MESSAGE_TEXT, content_types=types.ContentTypes.TEXT)
async def handle_setmessage_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    kind = data.get("setmsg_kind")
    if not kind:
        await message.reply("Unexpected error: kind missing")
        await state.finish()
        return
    name = "start_text" if kind == "start" else "help_text"
    await db.set_message_text(name, message.text)
    await state.finish()
    await message.reply(f"{kind.capitalize()} message updated.")

# /setimage owner-only
@dp.message_handler(commands=["setimage"])
async def cmd_setimage(message: types.Message):
    if not is_owner(message.from_user.id):
        await safe_send(message.from_user.id, text="You are not authorized to use this command.")
        return
    # This command requires reply to an image (owner replies to image with /setimage)
    # If the message is a reply to image, we can prompt which slot to set
    if not message.reply_to_message:
        await message.reply("Reply to an image (or any media) with /setimage to set it as start or help image.")
        return
    # Extract file_id of media in reply_to_message
    target_msg = message.reply_to_message
    file_id, ftype = extract_file_id_from_msg(target_msg)
    if not file_id:
        await message.reply("Couldn't extract file from the replied message. Make sure you replied to a photo/document/video.")
        return
    # Ask whether to set for start or help
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Set as Start Image", callback_data=f"setimg_start::{file_id}"),
        InlineKeyboardButton("Set as Help Image", callback_data=f"setimg_help::{file_id}")
    )
    await message.reply("Choose where to save this image:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("setimg_"))
async def cb_setimg(callback_query: types.CallbackQuery):
    if not is_owner(callback_query.from_user.id):
        await callback_query.answer("Not authorized", show_alert=True)
        return
    payload = callback_query.data  # e.g. setimg_start::<file_id>
    try:
        kind, file_id = payload.split("::", 1)
        kind = kind.split("_", 1)[1]  # 'start' or 'help'
    except Exception:
        await callback_query.answer("Invalid payload", show_alert=True)
        return
    name = "start_image" if kind == "start" else "help_image"
    await db.set_message_file(name, file_id)
    await callback_query.answer()
    await bot.send_message(callback_query.from_user.id, f"{kind.capitalize()} image has been saved.")

def extract_file_id_from_msg(msg: types.Message) -> Tuple[Optional[str], Optional[str]]:
    """
    Inspect message and return best file_id and a string file_type.
    Handles: photo, document, video, audio, voice, animation
    """
    if msg.photo:
        # message.photo is a list of PhotoSize; pick last (largest)
        return msg.photo[-1].file_id, "photo"
    if msg.document:
        return msg.document.file_id, "document"
    if msg.video:
        return msg.video.file_id, "video"
    if msg.audio:
        return msg.audio.file_id, "audio"
    if msg.voice:
        return msg.voice.file_id, "voice"
    if msg.animation:
        return msg.animation.file_id, "animation"
    return None, None

# /broadcast owner-only (start)
@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("You are not authorized to broadcast.")
        return
    await message.reply("Send the message (text or media) you want to broadcast to all users. You can also include buttons. Send /cancel to abort.")
    await BroadcastStates.WAITING_FOR_BROADCAST.set()

@dp.message_handler(state=BroadcastStates.WAITING_FOR_BROADCAST, commands=["cancel"])
async def cancel_broadcast(message: types.Message, state: FSMContext):
    await state.finish()
    await message.reply("Broadcast cancelled.")

@dp.message_handler(state=BroadcastStates.WAITING_FOR_BROADCAST, content_types=types.ContentTypes.ANY)
async def handle_broadcast(message: types.Message, state: FSMContext):
    """
    Copy the message to all users as copy (so no forward tags).
    Keep inline buttons if present.
    This operation can be heavy depending on user count — we iterate users and copy/call send methods.
    """
    if not is_owner(message.from_user.id):
        await message.reply("You are not authorized to broadcast.")
        await state.finish()
        return

    # get list of users
    total_users = await db.count_total_users()
    await message.reply(f"Starting broadcast to {total_users} users. This may take a while...")

    # gather inline keyboard if exists in original message
    reply_markup = None
    if message.reply_markup:
        reply_markup = message.reply_markup

    # fetch all users
    # WARNING: for large userbase a SELECT * could be heavy; here we stream in DB but for simplicity we fetch all
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users;")
        user_ids = [r["user_id"] for r in rows]

    # prepare a function to forward/copy/send safely per user
    sent_count = 0
    failed_count = 0
    for uid in user_ids:
        try:
            # we want copy semantics: use copy_message for media + text
            # copy_message works with message_id from original chat - but we are executing from owner chat.
            # Simpler approach: for text-only use send_message, for media extract file_id and send corresponding.
            if message.text and not (message.photo or message.document or message.video or message.audio or message.animation):
                await bot.send_message(chat_id=uid, text=message.text, reply_markup=reply_markup)
            else:
                # if media present, extract file_id and send using send_* preserving caption
                file_id, ftype = extract_file_id_from_msg(message)
                caption = None
                if message.caption:
                    caption = message.caption
                if ftype == "photo":
                    await bot.send_photo(chat_id=uid, photo=file_id, caption=caption, reply_markup=reply_markup)
                elif ftype == "document":
                    await bot.send_document(chat_id=uid, document=file_id, caption=caption, reply_markup=reply_markup)
                elif ftype == "video":
                    await bot.send_video(chat_id=uid, video=file_id, caption=caption, reply_markup=reply_markup)
                elif ftype == "audio":
                    await bot.send_audio(chat_id=uid, audio=file_id, caption=caption, reply_markup=reply_markup)
                elif ftype == "animation":
                    await bot.send_animation(chat_id=uid, animation=file_id, caption=caption, reply_markup=reply_markup)
                else:
                    # fallback: send text + maybe a link to the media
                    if message.text:
                        await bot.send_message(chat_id=uid, text=message.text, reply_markup=reply_markup)
                    else:
                        await bot.send_message(chat_id=uid, text="(Broadcast content)", reply_markup=reply_markup)
            sent_count += 1
            # small sleep to avoid hitting limits for huge lists
            await asyncio.sleep(0.05)
        except Exception as e:
            failed_count += 1
            logger.debug("Broadcast failed for %s: %s", uid, e)
            # continue broadcasting to others

    await message.reply(f"Broadcast finished. Sent: {sent_count}, Failed: {failed_count}")
    await state.finish()

# /stats command (owner-only)
@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Not authorized.")
        return
    total_users = await db.count_total_users()
    since = datetime.utcnow() - timedelta(days=2)  # last 48 hours
    active_users = await db.count_active_users_since(since)
    total_upload_sessions = await db.get_stat("total_upload_sessions")
    total_files = await db.get_stat("total_files")
    text = (
        f"📊 <b>Bot Statistics</b>\n\n"
        f"• Total users: <b>{total_users}</b>\n"
        f"• Active in last 48h: <b>{active_users}</b>\n"
        f"• Upload sessions completed: <b>{total_upload_sessions}</b>\n"
        f"• Total files stored: <b>{total_files}</b>\n"
    )
    await bot.send_message(chat_id=message.from_user.id, text=text, parse_mode=ParseMode.HTML)

# /upload multi-step flow (owner-only)
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner can use /upload.")
        return
    await state.update_data(upload_files=[])  # will be list of dicts
    await message.reply("Upload mode started. Send files (photos, videos, documents). When finished send /d (done) or /c (cancel).")
    await UploadStates.WAITING_FOR_FILES.set()

# cancel upload
@dp.message_handler(state=UploadStates.WAITING_FOR_FILES, commands=["c", "cancel"])
async def cancel_upload(message: types.Message, state: FSMContext):
    await state.finish()
    await message.reply("Upload cancelled and cleared.")

# finish upload and proceed to options
@dp.message_handler(state=UploadStates.WAITING_FOR_FILES, commands=["d"])
async def finish_upload(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data.get("upload_files", [])
    if not files:
        await message.reply("No files uploaded. Cancelled.")
        await state.finish()
        return
    # ask Protect Content?
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Yes (prevent forward/save)", callback_data="upload_protect_yes"),
        InlineKeyboardButton("No (allow)", callback_data="upload_protect_no")
    )
    await UploadStates.WAITING_FOR_PROTECT.set()
    await message.reply("Enable Protect Content for these files? (If yes, recipients cannot forward/save).", reply_markup=kb)

# capture files while in WAITING_FOR_FILES
@dp.message_handler(state=UploadStates.WAITING_FOR_FILES, content_types=types.ContentTypes.ANY)
async def capture_upload_files(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner can upload.")
        return
    # Extract file_id and type
    file_id, ftype = extract_file_id_from_msg(message)
    caption = getattr(message, "caption", None) or None
    orig_name = None
    if message.document and message.document.file_name:
        orig_name = message.document.file_name
    if not file_id:
        await message.reply("Unrecognized file type. Supported: photo, document, video, audio, animation, voice.")
        return
    # Copy media to upload channel to ensure persistence (bot uploads media to upload channel)
    # We'll use bot.copy_message to preserve file without downloading
    try:
        copied = None
        try:
            # copy_message requires from_chat_id and message_id: we copy the original message
            copied = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID_INT, from_chat_id=message.chat.id, message_id=message.message_id)
        except Exception as e:
            # fallback: send the file using send_* passing file_id; copy_message might be restricted for some types
            logger.debug("copy_message failed: %s. Trying send methods.", e)
            if ftype == "photo":
                copied = await bot.send_photo(chat_id=UPLOAD_CHANNEL_ID_INT, photo=file_id, caption=caption)
            elif ftype == "video":
                copied = await bot.send_video(chat_id=UPLOAD_CHANNEL_ID_INT, video=file_id, caption=caption)
            elif ftype == "audio":
                copied = await bot.send_audio(chat_id=UPLOAD_CHANNEL_ID_INT, audio=file_id, caption=caption)
            elif ftype == "document":
                copied = await bot.send_document(chat_id=UPLOAD_CHANNEL_ID_INT, document=file_id, caption=caption)
            elif ftype == "animation":
                copied = await bot.send_animation(chat_id=UPLOAD_CHANNEL_ID_INT, animation=file_id, caption=caption)
            elif ftype == "voice":
                copied = await bot.send_voice(chat_id=UPLOAD_CHANNEL_ID_INT, voice=file_id)
            else:
                copied = await bot.send_document(chat_id=UPLOAD_CHANNEL_ID_INT, document=file_id, caption=caption)
        # Note: copied might be a Message; the file_id on Telegram remains the same usually
        # Save to FSM data
        data = await state.get_data()
        upload_files = data.get("upload_files", [])
        position = len(upload_files)
        upload_files.append({
            "file_type": ftype,
            "file_id": file_id,
            "orig_name": orig_name,
            "caption": caption,
            "position": position
        })
        await state.update_data(upload_files=upload_files)
        await message.reply(f"Stored file ({ftype}) — send more or /d when done. Total files in session: {len(upload_files)}")
    except Exception as e:
        logger.exception("Failed to copy file to upload channel: %s", e)
        await message.reply("Failed to copy the file to upload channel — check bot permissions.")

# handle protect callback
@dp.callback_query_handler(lambda c: c.data and c.data.startswith("upload_protect_"), state=UploadStates.WAITING_FOR_PROTECT)
async def cb_upload_protect(callback_query: types.CallbackQuery, state: FSMContext):
    choice = callback_query.data.split("_")[-1]  # 'yes' or 'no'
    protect = (choice == "yes")
    await state.update_data(protect=protect)
    await UploadStates.WAITING_FOR_AUTODELETE.set()
    await callback_query.answer()
    await bot.send_message(callback_query.from_user.id, "Set auto-delete timer in minutes (0 for never). Acceptable 0 - 10080 (i.e. 7 days). Send a number now or /cancel to abort.")

@dp.message_handler(state=UploadStates.WAITING_FOR_AUTODELETE, commands=["cancel"])
async def cancel_autodelete(message: types.Message, state: FSMContext):
    await state.finish()
    await message.reply("Upload cancelled.")

@dp.message_handler(state=UploadStates.WAITING_FOR_AUTODELETE, content_types=types.ContentTypes.TEXT)
async def handle_autodelete_input(message: types.Message, state: FSMContext):
    text = message.text.strip()
    try:
        minutes = int(text)
    except ValueError:
        await message.reply("Please send a number (0 - 10080).")
        return
    if not (0 <= minutes <= 10080):
        await message.reply("Allowed range 0 - 10080 minutes.")
        return
    await state.update_data(auto_delete_minutes=minutes)
    # confirmation: ask for optional title
    await UploadStates.CONFIRMATION.set()
    await message.reply("Provide a title/name for this upload session (optional). Send plain text title, or send /skip to skip.")

@dp.message_handler(state=UploadStates.CONFIRMATION, commands=["skip"])
async def skip_title(message: types.Message, state: FSMContext):
    await finalize_upload_session(message, state, title=None)

@dp.message_handler(state=UploadStates.CONFIRMATION, content_types=types.ContentTypes.TEXT)
async def receive_title_and_finalize(message: types.Message, state: FSMContext):
    title = message.text.strip()
    await finalize_upload_session(message, state, title=title)

async def finalize_upload_session(message: types.Message, state: FSMContext, title: Optional[str]):
    data = await state.get_data()
    files = data.get("upload_files", [])
    protect = data.get("protect", False)
    auto_delete_minutes = data.get("auto_delete_minutes", 0)
    if not files:
        await message.reply("No files found — canceling.")
        await state.finish()
        return
    # create session in DB
    session_token = generate_session_token(9)
    # optional expiration (we don't enforce expiry in db unless you want). You can compute expires_at = datetime.utcnow() + timedelta(seconds=DEEP_LINK_TTL_SECONDS)
    expires_at = datetime.utcnow() + timedelta(seconds=DEEP_LINK_TTL_SECONDS)
    await db.create_session(session_token, message.from_user.id, protect, auto_delete_minutes, title, expires_at)
    # insert files
    pos = 0
    for f in files:
        await db.add_session_file(session_token, f["file_type"], f["file_id"], f.get("orig_name"), f.get("caption"), pos)
        pos += 1
    # increment stats
    await db.increment_upload_sessions()
    # send deep link
    deep_link = build_start_deep_link(session_token)
    # show session summary
    summary = (
        f"Upload session created ✅\n\n"
        f"Session ID: <code>{session_token}</code>\n"
        f"Files: {len(files)}\n"
        f"Protect Content: {'Yes' if protect else 'No'}\n"
        f"Auto-delete for recipients: {pretty_minutes(auto_delete_minutes)}\n"
        f"Deep link: {deep_link}\n\n"
        f"Anyone with the link can fetch files."
    )
    await message.reply(summary, parse_mode=ParseMode.HTML)
    await state.finish()

# allow owner to list session files? (optional helper)
@dp.message_handler(commands=["listsession"])
async def cmd_list_session(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Not authorized.")
        return
    payload = message.get_args().strip()
    if not payload:
        await message.reply("Usage: /listsession <SESSION_ID>")
        return
    rec = await db.get_session(payload)
    if not rec:
        await message.reply("Session not found.")
        return
    files = await db.get_session_files(payload)
    s = f"Session {payload}\nOwner: {rec['owner_id']}\nFiles: {len(files)}"
    await message.reply(s)
    for f in files:
        await message.reply(f"{f['position']}: {f['file_type']} - file_id: {f['file_id']} caption: {f['caption']}")

# Fallback handler for unrecognized commands (optional)
@dp.message_handler(commands=["cancel"])
async def cmd_cancel(message: types.Message, state: FSMContext):
    await state.finish()
    await message.reply("Operation cancelled (global).")

# fallback for all other messages to update last_active and respond politely
@dp.message_handler()
async def fallback_all(message: types.Message):
    # update user's last active
    await db.add_or_update_user(message.from_user.id)
    # minimal auto-response for unknown commands or plain chat
    # we keep it unobtrusive
    if message.text and message.text.startswith("/"):
        # unknown command
        return
    # optionally do nothing; to avoid spam, don't reply to ordinary messages
    return

# ---------------------------
# Webhook entry and aiohttp app
# ---------------------------

# We will use aiogram's start_webhook for the dispatcher which starts its own web server.
# Additionally we will supply a small aiohttp app to provide /health endpoint by running an aiohttp sub-app.
# start_webhook can accept host and port, and will handle requests to /webhook automatically.

# However, some deployment setups desire an additional /health route on the same server. start_webhook also supports aiohttp_webapp injection via the 'web_app' param in newer aiogram versions.
# To keep compatibility, we'll create a simple aiohttp application and mount health route.

def create_aiohttp_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/", health)
    return app

# ---------------------------
# Entrypoint
# ---------------------------

# -------------------------
# Entrypoint (Webhook Only)
# -------------------------

from aiohttp import web
import asyncio

async def on_startup(dp):
    # Set webhook to Render external URL
    webhook_url = f"{RENDER_EXTERNAL_URL}/webhook/{BOT_TOKEN}"
    await bot.set_webhook(webhook_url)
    logger.info(f"Webhook set: {webhook_url}")

    # Initialize DB if needed
    try:
        await init_db()
        logger.info("Database initialized.")
    except Exception as e:
        logger.error(f"Database init failed: {e}")

async def on_shutdown(dp):
    logger.info("Shutting down..")
    try:
        await bot.delete_webhook()
        logger.info("Webhook deleted.")
    except Exception as e:
        logger.error(f"Failed to delete webhook: {e}")
    await dp.storage.close()
    await dp.storage.wait_closed()
    logger.info("Bot shutdown complete.")

# Health endpoint for UptimeRobot
async def healthcheck(request):
    return web.Response(text="ok")

def main():
    app = web.Application()
    app.router.add_get("/", healthcheck)
    app.router.add_get("/health", healthcheck)

    # aiogram webhook handler
    from aiogram.dispatcher.webhook import get_new_configured_app
    app = get_new_configured_app(dispatcher=dp, path=f"/webhook/{BOT_TOKEN}")
    
    # Add health routes again (get_new_configured_app replaces app)
    app.router.add_get("/", healthcheck)
    app.router.add_get("/health", healthcheck)

    # Register startup/shutdown
    dp.loop.create_task(on_startup(dp))
    dp.loop.create_task(on_shutdown(dp))

    web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

if __name__ == "__main__":
    main()