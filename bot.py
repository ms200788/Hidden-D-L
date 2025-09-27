#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Block 1/3
Core imports, configuration, database layer, FSM state definitions, and utility helpers.

Paste BLOCK 1, then BLOCK 2, then BLOCK 3 (I'll provide those next).
"""

# -------------------------
# Imports
# -------------------------
import os
import logging
import asyncio
import secrets
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple, Any

import asyncpg
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ParseMode, Message
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.executor import start_webhook

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------
# Environment / Configuration
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.critical("BOT_TOKEN is not set. Exiting.")
    raise RuntimeError("BOT_TOKEN environment variable is required")

OWNER_ID = int(os.getenv("OWNER_ID", "0"))
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

# support numeric channel ID and username string like @channel
try:
    if UPLOAD_CHANNEL_ID.lstrip("-").isdigit():
        UPLOAD_CHANNEL_ID_INT = int(UPLOAD_CHANNEL_ID)
    else:
        UPLOAD_CHANNEL_ID_INT = UPLOAD_CHANNEL_ID
except Exception:
    UPLOAD_CHANNEL_ID_INT = UPLOAD_CHANNEL_ID

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")  # preferred for webhook
PORT = int(os.getenv("PORT", "8080"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")  # will be combined with token
WEBHOOK_URL = (RENDER_EXTERNAL_URL.rstrip("/") + WEBHOOK_PATH + f"/{BOT_TOKEN}") if RENDER_EXTERNAL_URL else None

# Skip updates (useful on startup)
SKIP_UPDATES = os.getenv("SKIP_UPDATES", "1") == "1"

# deep link TTL default (30 days)
DEEP_LINK_TTL_SECONDS = int(os.getenv("DEEP_LINK_TTL_SECONDS", 60 * 60 * 24 * 30))

# -------------------------
# Bot & Dispatcher
# -------------------------
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# -------------------------
# FSM States (define all that will be used in later blocks)
# -------------------------
class UploadStates(StatesGroup):
    WAITING_FOR_FILES = State()
    WAITING_FOR_PROTECT = State()
    WAITING_FOR_AUTODELETE = State()
    CONFIRMATION = State()

class BroadcastStates(StatesGroup):
    WAITING_FOR_BROADCAST = State()

class SetMessageStates(StatesGroup):
    WAITING_FOR_MESSAGE_TEXT = State()

class SetImageStates(StatesGroup):
    WAITING_FOR_IMAGE_FILE = State()

class SetImageChoiceStates(StatesGroup):
    WAITING_FOR_CHOICE = State()

class GenericConfirmStates(StatesGroup):
    WAITING_CONFIRM = State()

# -------------------------
# Database wrapper (asyncpg)
# -------------------------
class Database:
    """
    Async wrapper around an asyncpg pool. Creates/initializes schema on connect.
    Schema includes:
      - users
      - messages (start/help text + images)
      - sessions (session_token as TEXT primary key)
      - session_files
      - stats
    """

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.pool.Pool] = None

    async def connect(self):
        logger.info("Connecting to database...")
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=10)
        logger.info("Connected to database")
        await self._init_schema()

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("Database pool closed")

    async def _init_schema(self):
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # users table
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_seen TIMESTAMP WITH TIME ZONE NOT NULL,
                    last_active TIMESTAMP WITH TIME ZONE NOT NULL
                );
                """)
                # messages table
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    name TEXT PRIMARY KEY,  -- 'start_text', 'help_text', 'start_image', 'help_image'
                    text TEXT,
                    file_id TEXT,
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
                );
                """)
                # sessions table (string token as primary key)
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_token TEXT PRIMARY KEY,
                    owner_id BIGINT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    protect_content BOOLEAN DEFAULT FALSE,
                    auto_delete_minutes INT DEFAULT 0,
                    title TEXT,
                    expires_at TIMESTAMP WITH TIME ZONE
                );
                """)
                # session_files table referencing sessions(session_token)
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS session_files (
                    id SERIAL PRIMARY KEY,
                    session_token TEXT REFERENCES sessions(session_token) ON DELETE CASCADE,
                    file_type TEXT NOT NULL,
                    file_id TEXT NOT NULL,
                    orig_file_name TEXT,
                    caption TEXT,
                    position INT DEFAULT 0
                );
                """)
                # stats
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    key TEXT PRIMARY KEY,
                    value BIGINT DEFAULT 0
                );
                """)
                # initialize stats keys
                await conn.execute("""
                INSERT INTO stats(key, value)
                VALUES
                  ('total_upload_sessions', 0),
                  ('total_files', 0)
                ON CONFLICT (key) DO NOTHING;
                """)
                # index for session_files
                await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_session_files_session ON session_files(session_token);
                """)

        logger.info("Database schema initialized")

    # -----------------------
    # Users
    # -----------------------
    async def add_or_update_user(self, user_id: int, username: Optional[str] = None):
        assert self.pool is not None
        ts = datetime.utcnow()
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO users(user_id, username, first_seen, last_active)
            VALUES($1, $2, $3, $3)
            ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, last_active = EXCLUDED.last_active;
            """, user_id, username, ts)

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

    # -----------------------
    # Messages (start/help text and files)
    # -----------------------
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

    # -----------------------
    # Sessions & files
    # -----------------------
    async def create_session(self, session_token: str, owner_id: int, protect: bool, auto_delete_minutes: int, title: Optional[str], expires_at: Optional[datetime] = None):
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO sessions(session_token, owner_id, created_at, protect_content, auto_delete_minutes, title, expires_at)
            VALUES($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (session_token) DO NOTHING;
            """, session_token, owner_id, datetime.utcnow(), protect, auto_delete_minutes, title, expires_at)

    async def add_session_file(self, session_token: str, file_type: str, file_id: str, orig_file_name: Optional[str], caption: Optional[str], position: int = 0):
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO session_files(session_token, file_type, file_id, orig_file_name, caption, position)
            VALUES($1, $2, $3, $4, $5, $6)
            """, session_token, file_type, file_id, orig_file_name, caption, position)
            # increment total_files stat
            await conn.execute("""
            UPDATE stats SET value = value + 1 WHERE key = 'total_files';
            """)

    async def get_session(self, session_token: str) -> Optional[asyncpg.Record]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rec = await conn.fetchrow("SELECT * FROM sessions WHERE session_token = $1;", session_token)
            return rec

    async def get_session_files(self, session_token: str) -> List[asyncpg.Record]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
            SELECT * FROM session_files WHERE session_token = $1 ORDER BY position ASC, id ASC;
            """, session_token)
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

# global DB instance
db = Database(DATABASE_URL)

# -------------------------
# Utility helpers
# -------------------------
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def generate_session_token(length_bytes: int = 8) -> str:
    """
    Use URL-safe token for deep link session id
    """
    return secrets.token_urlsafe(length_bytes)

async def build_start_deep_link(session_token: str) -> str:
    """
    Create a t.me deep link for the bot
    """
    try:
        me = await bot.get_me()
        username = getattr(me, "username", None)
    except Exception:
        username = None
    if username:
        return f"https://t.me/{username}?start={session_token}"
    else:
        bot_id_prefix = BOT_TOKEN.split(":")[0]
        return f"https://t.me/{bot_id_prefix}?start={session_token}"

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

# safe send wrapper
async def safe_send(chat_id: int, **kwargs) -> Optional[types.Message]:
    try:
        return await bot.send_message(chat_id=chat_id, **kwargs)
    except Exception as e:
        logger.exception("Failed to send message to %s: %s", chat_id, e)
        return None

# -------------------------
# End of Block 1/3
# -------------------------
# =========================
# BLOCK 2/3: Handlers & Flows
# (paste after BLOCK 1/3)
# =========================

from functools import partial

# -------------------------
# Small helpers (DB direct SQL helpers)
# -------------------------
async def db_get_setting(key: str) -> Optional[str]:
    assert db.pool is not None
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM messages WHERE name=$1", key)
        # Note: using the messages table for small key/value entries (start_text/help_text etc.)
        return row["value"] if row and "value" in row else row["text"] if row and "text" in row else None

async def db_set_setting(key: str, value: str):
    assert db.pool is not None
    async with db.pool.acquire() as conn:
        # messages table used as key/value store for now
        await conn.execute("""
        INSERT INTO messages(name, text, updated_at) VALUES ($1, $2, $3)
        ON CONFLICT (name) DO UPDATE SET text = EXCLUDED.text, updated_at = EXCLUDED.updated_at
        """, key, value, datetime.utcnow())

# Ensure sessions tables exist (lazy-init helper used by upload flow)
async def ensure_sessions_tables():
    assert db.pool is not None
    async with db.pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_token TEXT PRIMARY KEY,
            owner_id BIGINT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL,
            protect_content BOOLEAN DEFAULT FALSE,
            auto_delete_minutes INT DEFAULT 0,
            title TEXT,
            expires_at TIMESTAMP WITH TIME ZONE
        )""")
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS session_files (
            id SERIAL PRIMARY KEY,
            session_token TEXT REFERENCES sessions(session_token) ON DELETE CASCADE,
            file_type TEXT NOT NULL,
            file_id TEXT NOT NULL,
            orig_file_name TEXT,
            caption TEXT,
            position INT DEFAULT 0
        )""")

# -------------------------
# Generic small UI helpers
# -------------------------
def get_start_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(text="Help", callback_data="show_help"))
    return kb

def extract_file_id_from_msg(msg: types.Message) -> Tuple[Optional[str], Optional[str]]:
    if msg.photo:
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

# -------------------------
# /start and deep-link handling
# -------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    # store user
    try:
        await db.add_or_update_user(message.from_user.id, getattr(message.from_user, "username", None))
    except Exception as e:
        logger.debug("add_or_update_user failed: %s", e)

    # check payload (deep link)
    parts = message.get_args().strip() if hasattr(message, "get_args") else ""
    if parts:
        await handle_deep_link_start(message, parts)
        return

    # no payload: show start message from settings (if any)
    start_text = await db.get_message_text("start_text")
    start_image = await db.get_message_file("start_image")
    try:
        if start_image:
            if start_text:
                await bot.send_photo(chat_id=message.chat.id, photo=start_image, caption=start_text, reply_markup=get_start_keyboard())
            else:
                await bot.send_photo(chat_id=message.chat.id, photo=start_image, reply_markup=get_start_keyboard())
        else:
            await bot.send_message(chat_id=message.chat.id, text=start_text or "Welcome!", reply_markup=get_start_keyboard())
    except Exception:
        # fallback
        await bot.send_message(chat_id=message.chat.id, text=start_text or "Welcome!", reply_markup=get_start_keyboard())

# Deep link retrieval: send stored session files to requester
async def handle_deep_link_start(message: types.Message, session_token: str):
    # Ensure sessions tables exist
    await ensure_sessions_tables()
    assert db.pool is not None
    async with db.pool.acquire() as conn:
        rec = await conn.fetchrow("SELECT * FROM sessions WHERE session_token=$1", session_token)
        if not rec:
            await message.reply("Sorry, that link is invalid or expired.")
            return
        # check expiration
        if rec["expires_at"] and rec["expires_at"] < datetime.utcnow():
            await message.reply("Sorry, that session has expired.")
            return
        files = await conn.fetch("SELECT * FROM session_files WHERE session_token=$1 ORDER BY position,id", session_token)
    if not files:
        await message.reply("No files found for this session.")
        return

    protect = rec["protect_content"]
    auto_delete_minutes = rec["auto_delete_minutes"]
    is_owner_user = is_owner(message.from_user.id)
    if auto_delete_minutes and not is_owner_user:
        await message.reply(f"These files will be deleted in {pretty_minutes(auto_delete_minutes)} from your chat.")

    sent_messages = []
    for row in files:
        f_type = row["file_type"]
        f_id = row["file_id"]
        caption = row["caption"] or None
        try:
            if f_type == "photo":
                msg = await bot.send_photo(chat_id=message.chat.id, photo=f_id, caption=caption, protect_content=(protect and not is_owner_user))
            elif f_type == "video":
                msg = await bot.send_video(chat_id=message.chat.id, video=f_id, caption=caption, protect_content=(protect and not is_owner_user))
            elif f_type == "audio":
                msg = await bot.send_audio(chat_id=message.chat.id, audio=f_id, caption=caption, protect_content=(protect and not is_owner_user))
            elif f_type == "voice":
                msg = await bot.send_voice(chat_id=message.chat.id, voice=f_id, protect_content=(protect and not is_owner_user))
            elif f_type == "document":
                msg = await bot.send_document(chat_id=message.chat.id, document=f_id, caption=caption, protect_content=(protect and not is_owner_user))
            elif f_type == "animation":
                msg = await bot.send_animation(chat_id=message.chat.id, animation=f_id, caption=caption, protect_content=(protect and not is_owner_user))
            else:
                msg = await bot.send_document(chat_id=message.chat.id, document=f_id, caption=caption, protect_content=(protect and not is_owner_user))
            sent_messages.append(msg)
        except Exception as e:
            logger.exception("Failed to send session file: %s", e)

    if auto_delete_minutes and auto_delete_minutes > 0 and not is_owner_user:
        asyncio.create_task(schedule_deletions(message.chat.id, sent_messages, auto_delete_minutes))

# -------------------------
# schedule deletions
# -------------------------
async def schedule_deletions(chat_id: int, messages: List[Message], minutes: int):
    try:
        logger.info("Scheduling deletion in %s minutes for chat %s", minutes, chat_id)
        await asyncio.sleep(minutes * 60)
        for m in messages:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=m.message_id)
            except Exception as e:
                logger.debug("Could not delete message: %s", e)
    except Exception as e:
        logger.exception("Error in schedule_deletions: %s", e)

# -------------------------
# Help callback & /help command
# -------------------------
@dp.callback_query_handler(lambda c: c.data == "show_help")
async def cb_show_help(callback_query: types.CallbackQuery):
    try:
        await db.add_or_update_user(callback_query.from_user.id, getattr(callback_query.from_user, "username", None))
    except Exception as e:
        logger.debug("add_or_update_user failed: %s", e)
    help_text = await db.get_message_text("help_text")
    help_img = await db.get_message_file("help_image")
    try:
        if help_img:
            if help_text:
                await bot.send_photo(callback_query.from_user.id, photo=help_img, caption=help_text)
            else:
                await bot.send_photo(callback_query.from_user.id, photo=help_img)
        else:
            await bot.send_message(callback_query.from_user.id, help_text or "Help")
    except Exception:
        await safe_send(callback_query.from_user.id, text=help_text or "Help")
    try:
        await callback_query.answer()
    except Exception:
        pass

@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    try:
        await db.add_or_update_user(message.from_user.id, getattr(message.from_user, "username", None))
    except Exception as e:
        logger.debug("add_or_update_user failed: %s", e)
    help_text = await db.get_message_text("help_text")
    help_img = await db.get_message_file("help_image")
    try:
        if help_img:
            if help_text:
                await bot.send_photo(message.chat.id, photo=help_img, caption=help_text)
            else:
                await bot.send_photo(message.chat.id, photo=help_img)
        else:
            await bot.send_message(message.chat.id, help_text or "Help")
    except Exception:
        await safe_send(message.chat.id, text=help_text or "Help")

# -------------------------
# Owner-only: setmessage (start/help texts)
# -------------------------
@dp.message_handler(commands=["setmessage"])
async def cmd_setmessage(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.reply("Not authorized.")
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Start message", callback_data="setmsg_start"),
        InlineKeyboardButton("Help message", callback_data="setmsg_help"),
    )
    await message.reply("Which message to set?", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("setmsg_"))
async def cb_setmsg_choice(callback_query: types.CallbackQuery, state: FSMContext):
    if not is_owner(callback_query.from_user.id):
        return await callback_query.answer("Not authorized", show_alert=True)
    kind = callback_query.data.split("_",1)[1]  # start or help
    await state.update_data(setmsg_kind=kind)
    await SetMessageStates.WAITING_FOR_MESSAGE_TEXT.set()
    await callback_query.answer()
    await bot.send_message(callback_query.from_user.id, f"Send the text for {kind} message (or /cancel).")

@dp.message_handler(state=SetMessageStates.WAITING_FOR_MESSAGE_TEXT, commands=["cancel"])
async def cancel_setmsg(message: types.Message, state: FSMContext):
    await state.finish()
    await message.reply("Cancelled.")

@dp.message_handler(state=SetMessageStates.WAITING_FOR_MESSAGE_TEXT, content_types=types.ContentTypes.TEXT)
async def handle_setmsg_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    kind = data.get("setmsg_kind")
    if not kind:
        await message.reply("Error: kind missing")
        await state.finish()
        return
    key = "start_text" if kind == "start" else "help_text"
    await db_set_setting(key, message.text)
    await state.finish()
    await message.reply(f"{kind.capitalize()} message saved.")

# -------------------------
# Owner-only: setimage (reply to media)
# -------------------------
@dp.message_handler(commands=["setimage"])
async def cmd_setimage(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.reply("Not authorized.")
    if not message.reply_to_message:
        return await message.reply("Reply to a media message with /setimage to save it as start/help image.")
    target = message.reply_to_message
    file_id, ftype = extract_file_id_from_msg(target)
    if not file_id:
        return await message.reply("Could not extract file from the replied message.")
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Set as Start Image", callback_data=f"setimg_start::{file_id}"),
        InlineKeyboardButton("Set as Help Image", callback_data=f"setimg_help::{file_id}")
    )
    await message.reply("Choose where to save this image:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("setimg_"))
async def cb_setimg(callback_query: types.CallbackQuery):
    if not is_owner(callback_query.from_user.id):
        return await callback_query.answer("Not authorized", show_alert=True)
    payload = callback_query.data
    try:
        kind, file_id = payload.split("::",1)
        kind = kind.split("_",1)[1]
    except Exception:
        return await callback_query.answer("Invalid payload", show_alert=True)
    key = "start_image" if kind == "start" else "help_image"
    await db.set_message_file(key, file_id)
    await callback_query.answer()
    await bot.send_message(callback_query.from_user.id, f"{kind.capitalize()} image saved.")

# -------------------------
# Upload flow (owner only) - multi-file session creation
# -------------------------
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return await message.reply("Only owner can use /upload.")
    await state.update_data(upload_files=[])
    await message.reply("Upload mode started. Send files (photos, documents, videos). Send /d when done or /c to cancel.")
    await UploadStates.WAITING_FOR_FILES.set()

@dp.message_handler(state=UploadStates.WAITING_FOR_FILES, commands=["c","cancel"])
async def cancel_upload(message: types.Message, state: FSMContext):
    await state.finish()
    await message.reply("Upload cancelled.")

@dp.message_handler(state=UploadStates.WAITING_FOR_FILES, commands=["d"])
async def finish_upload(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data.get("upload_files", [])
    if not files:
        await message.reply("No files uploaded, cancelling.")
        await state.finish()
        return
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Yes (protect)", callback_data="upload_protect_yes"),
        InlineKeyboardButton("No (allow)", callback_data="upload_protect_no")
    )
    await UploadStates.WAITING_FOR_PROTECT.set()
    await message.reply("Enable Protect Content for recipients? (Yes/No)", reply_markup=kb)

@dp.message_handler(state=UploadStates.WAITING_FOR_FILES, content_types=types.ContentTypes.ANY)
async def capture_upload_files(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return await message.reply("Only owner can upload.")
    file_id, ftype = extract_file_id_from_msg(message)
    caption = getattr(message, "caption", None) or None
    orig_name = None
    if message.document and message.document.file_name:
        orig_name = message.document.file_name
    if not file_id:
        return await message.reply("Unsupported media. Use photo, document, video, audio, voice or animation.")
    # copy to upload channel
    try:
        copied = None
        try:
            copied = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID_INT, from_chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            # fallback send
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
        data = await state.get_data()
        upload_files = data.get("upload_files", [])
        upload_files.append({
            "file_type": ftype,
            "file_id": file_id,
            "orig_name": orig_name,
            "caption": caption
        })
        await state.update_data(upload_files=upload_files)
        await message.reply(f"Stored file ({ftype}). Send more or /d when done. Total: {len(upload_files)}")
    except Exception as e:
        logger.exception("Failed to copy file to upload channel: %s", e)
        await message.reply("Failed to copy file to upload channel — ensure bot is admin and channel ID is correct.")

# protect choice handler
@dp.callback_query_handler(lambda c: c.data and c.data.startswith("upload_protect_"), state=UploadStates.WAITING_FOR_PROTECT)
async def cb_upload_protect(callback_query: types.CallbackQuery, state: FSMContext):
    choice = callback_query.data.split("_")[-1]
    protect = (choice == "yes")
    await state.update_data(protect=protect)
    await UploadStates.WAITING_FOR_AUTODELETE.set()
    await callback_query.answer()
    await bot.send_message(callback_query.from_user.id, "Set auto-delete in minutes (0 = forever). Send a number (0 - 10080) or /cancel.")

@dp.message_handler(state=UploadStates.WAITING_FOR_AUTODELETE, commands=["cancel"])
async def cancel_autodel(message: types.Message, state: FSMContext):
    await state.finish()
    await message.reply("Upload cancelled.")

@dp.message_handler(state=UploadStates.WAITING_FOR_AUTODELETE, content_types=types.ContentTypes.TEXT)
async def handle_autodelete_input(message: types.Message, state: FSMContext):
    txt = message.text.strip()
    try:
        minutes = int(txt)
    except ValueError:
        return await message.reply("Send a number (0 - 10080).")
    if not (0 <= minutes <= 10080):
        return await message.reply("Allowed range 0 - 10080 minutes.")
    await state.update_data(auto_delete_minutes=minutes)
    await UploadStates.CONFIRMATION.set()
    await message.reply("Optional: send a title for this session or /skip to skip.")

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
        await message.reply("No files — cancelling.")
        await state.finish()
        return

    await ensure_sessions_tables()
    token = generate_session_token(12)
    expires_at = datetime.utcnow() + timedelta(seconds=DEEP_LINK_TTL_SECONDS)
    # insert session
    assert db.pool is not None
    async with db.pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO sessions(session_token, owner_id, created_at, protect_content, auto_delete_minutes, title, expires_at)
        VALUES($1,$2,$3,$4,$5,$6,$7)
        """, token, message.from_user.id, datetime.utcnow(), protect, auto_delete_minutes, title, expires_at)
        # insert files
        pos = 0
        for f in files:
            await conn.execute("""
            INSERT INTO session_files(session_token, file_type, file_id, orig_file_name, caption, position)
            VALUES($1,$2,$3,$4,$5,$6)
            """, token, f["file_type"], f["file_id"], f.get("orig_name"), f.get("caption"), pos)
            pos += 1
        await conn.execute("UPDATE stats SET value = value + 1 WHERE key='total_upload_sessions'")

    await db.increment_upload_sessions()
    deep_link = await build_start_deep_link(token)
    await message.reply(
        f"Session created ✅\n\nSession token: <code>{token}</code>\nFiles: {len(files)}\nProtect: {'Yes' if protect else 'No'}\nAuto-delete: {pretty_minutes(auto_delete_minutes)}\nLink: {deep_link}",
        parse_mode=ParseMode.HTML
    )
    await state.finish()

# -------------------------
# Broadcast (owner)
# -------------------------
@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.reply("Not authorized.")
    await message.reply("Send the message/media to broadcast to all users. /cancel to abort.")
    await BroadcastStates.WAITING_FOR_BROADCAST.set()

@dp.message_handler(state=BroadcastStates.WAITING_FOR_BROADCAST, commands=["cancel"])
async def cancel_broadcast(message: types.Message, state: FSMContext):
    await state.finish()
    await message.reply("Broadcast cancelled.")

@dp.message_handler(state=BroadcastStates.WAITING_FOR_BROADCAST, content_types=types.ContentTypes.ANY)
async def handle_broadcast(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await state.finish()
        return await message.reply("Not authorized.")
    # fetch users
    assert db.pool is not None
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
        user_ids = [r["user_id"] for r in rows]
    total = len(user_ids)
    await message.reply(f"Starting broadcast to {total} users...")
    sent = 0
    failed = 0
    for uid in user_ids:
        try:
            # simple approach: if text only
            if message.text and not (message.photo or message.document or message.video or message.audio or message.animation):
                await bot.send_message(chat_id=uid, text=message.text)
            else:
                file_id, ftype = extract_file_id_from_msg(message)
                caption = getattr(message, "caption", None)
                if ftype == "photo":
                    await bot.send_photo(uid, photo=file_id, caption=caption)
                elif ftype == "document":
                    await bot.send_document(uid, document=file_id, caption=caption)
                elif ftype == "video":
                    await bot.send_video(uid, video=file_id, caption=caption)
                elif ftype == "audio":
                    await bot.send_audio(uid, audio=file_id, caption=caption)
                elif ftype == "animation":
                    await bot.send_animation(uid, animation=file_id, caption=caption)
                else:
                    await bot.send_message(uid, text=message.text or "(content)")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.debug("Broadcast fail for %s: %s", uid, e)
    await message.reply(f"Broadcast finished. Sent: {sent}, Failed: {failed}")
    await state.finish()

# -------------------------
# /stats command (owner)
# -------------------------
@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    users = await db.count_total_users()
    files = await db.get_stat("total_files")
    upload_sessions = await db.get_stat("total_upload_sessions")
    since = datetime.utcnow() - timedelta(days=2)
    active_users = await db.count_active_users_since(since)
    text = (
        f"📊 <b>Bot Stats</b>\n\n"
        f"• Total users: <b>{users}</b>\n"
        f"• Active in last 48h: <b>{active_users}</b>\n"
        f"• Upload sessions completed: <b>{upload_sessions}</b>\n"
        f"• Total files stored: <b>{files}</b>\n"
    )
    await bot.send_message(chat_id=message.from_user.id, text=text, parse_mode=ParseMode.HTML)

# -------------------------
# listsession (owner)
# -------------------------
@dp.message_handler(commands=["listsession"])
async def cmd_list_session(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.reply("Not authorized.")
    token = message.get_args().strip() if hasattr(message, "get_args") else ""
    if not token:
        return await message.reply("Usage: /listsession <SESSION_TOKEN>")
    await ensure_sessions_tables()
    assert db.pool is not None
    async with db.pool.acquire() as conn:
        rec = await conn.fetchrow("SELECT * FROM sessions WHERE session_token=$1", token)
        if not rec:
            return await message.reply("Session not found.")
        files = await conn.fetch("SELECT * FROM session_files WHERE session_token=$1 ORDER BY position,id", token)
    s = f"Session {token}\nOwner: {rec['owner_id']}\nFiles: {len(files)}"
    await message.reply(s)
    for f in files:
        await message.reply(f"{f['position']}: {f['file_type']} - {f['file_id']} caption:{f['caption']}")

# -------------------------------------------------------------------
# Fallback Handlers & Error Logging
# -------------------------------------------------------------------

@dp.message_handler()
async def fallback_handler(message: types.Message):
    """Catch-all fallback for unknown messages."""
    await message.answer("⚠️ Unknown command. Use /help to see available options.")


@dp.errors_handler()
async def errors_handler(update, exception):
    logging.error(f"Update: {update} caused error: {exception}")
    return True


# -------------------------------------------------------------------
# Health Check Endpoint for Render + UptimeRobot
# -------------------------------------------------------------------

async def healthcheck(request):
    return web.Response(text="ok")


async def on_startup_app(app: web.Application):
    """Executed on app startup: DB connect + webhook set."""
    logging.info("Starting up...")
    try:
        await db.connect()
    except Exception as e:
        logging.error(f"Database init failed: {e}")

    webhook_url = f"{os.environ.get('RENDER_EXTERNAL_URL')}/webhook/{API_TOKEN}"
    await bot.set_webhook(webhook_url)
    logging.info(f"Webhook set to {webhook_url}")


async def on_shutdown_app(app: web.Application):
    """Executed on app shutdown: close DB + drop webhook."""
    logging.info("Shutting down...")
    try:
        await bot.delete_webhook()
        await db.close()
    except Exception as e:
        logging.error(f"Shutdown error: {e}")


# -------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------

def main():
    app = web.Application()
    app.router.add_get("/", healthcheck)
    app.router.add_get("/health", healthcheck)

    # Webhook route
    app.router.add_post(f"/webhook/{API_TOKEN}", dp.middleware(WebhookRequestHandler(dp)))

    # Startup & shutdown
    app.on_startup.append(on_startup_app)
    app.on_shutdown.append(on_shutdown_app)

    logging.info(f"Starting bot on port {PORT}...")
    web.run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()