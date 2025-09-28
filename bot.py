#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram File Sharing Bot - Webhook-only (aiogram v2) with Neon (Postgres)
Author: Generated for user
Notes:
 - Webhook only (no polling)
 - Upload channel stores originals; DB stores file_ids/captions only
 - Deep links: t.me/<bot_username>?start=<session_id>
 - Owner-only commands and user commands per spec
"""

import os
import sys
import asyncio
import logging
import json
import uuid
import time
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta

# aiogram v2 imports
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ContentType
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
from aiogram.utils.exceptions import BotBlocked, ChatNotFound, RetryAfter, TelegramAPIError, MessageNotModified

# aiohttp for webhook server
from aiohttp import web

# asyncpg for Neon/Postgres
import asyncpg

# ------------------------------
# Configuration
# ------------------------------
class Config:
    BOT_TOKEN: str = os.getenv('BOT_TOKEN', '').strip()
    OWNER_ID: int = int(os.getenv('OWNER_ID', '0') or 0)
    DATABASE_URL: str = os.getenv('DATABASE_URL', '').strip()
    RENDER_EXTERNAL_URL: str = os.getenv('RENDER_EXTERNAL_URL', '').strip()  # e.g. https://your-app.onrender.com
    PORT: int = int(os.getenv('PORT', 5000))
    UPLOAD_CHANNEL_ID: int = int(os.getenv('UPLOAD_CHANNEL_ID', '0') or 0)  # must be channel id where uploads are stored
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO').upper()
    # Safety defaults
    MAX_BROADCAST_PER_MINUTE: int = int(os.getenv('MAX_BROADCAST_PER_MINUTE', '120'))
    DB_MIN_POOL: int = int(os.getenv('DB_MIN_POOL', '1'))
    DB_MAX_POOL: int = int(os.getenv('DB_MAX_POOL', '10'))
    # Session ID length
    SESSION_ID_LEN: int = int(os.getenv('SESSION_ID_LEN', '8'))

# Validate some required envs early
if not Config.BOT_TOKEN:
    print("FATAL: BOT_TOKEN is required in environment variables", file=sys.stderr)
    sys.exit(1)
if not Config.OWNER_ID:
    print("FATAL: OWNER_ID is required and must be numeric", file=sys.stderr)
    sys.exit(1)
if not Config.DATABASE_URL:
    print("FATAL: DATABASE_URL is required", file=sys.stderr)
    sys.exit(1)
if not Config.RENDER_EXTERNAL_URL:
    print("FATAL: RENDER_EXTERNAL_URL is required for webhook mode", file=sys.stderr)
    sys.exit(1)
if not Config.UPLOAD_CHANNEL_ID:
    print("FATAL: UPLOAD_CHANNEL_ID is required (private channel id to store uploaded files)", file=sys.stderr)
    sys.exit(1)

# ------------------------------
# Logging
# ------------------------------
numeric_level = getattr(logging, Config.LOG_LEVEL, None)
if not isinstance(numeric_level, int):
    numeric_level = logging.INFO

logging.basicConfig(
    level=numeric_level,
    format='%(asctime)s | %(levelname)-7s | %(name)s:%(lineno)d | %(message)s'
)
logger = logging.getLogger("file_share_bot")

# ------------------------------
# Bot & Dispatcher setup
# ------------------------------
bot = Bot(token=Config.BOT_TOKEN, parse_mode='HTML')
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

# make bot instance accessible in contexts if needed
Bot.set_current(bot)

# Database pool (initialized on startup)
db_pool: Optional[asyncpg.pool.Pool] = None

# ------------------------------
# FSM States
# ------------------------------
class UploadStates(StatesGroup):
    waiting_for_files = State()
    waiting_for_options = State()

class MessageStates(StatesGroup):
    waiting_for_message_type = State()
    waiting_for_text = State()

class BroadcastStates(StatesGroup):
    waiting_for_broadcast_confirmation = State()

# ------------------------------
# Utility helpers
# ------------------------------
async def pg_try_connect(url: str, min_size: int = 1, max_size: int = 10, retries: int = 5, delay: int = 2) -> asyncpg.pool.Pool:
    """
    Create asyncpg pool with retries. Raises exception on failure.
    """
    last_exc = None
    for attempt in range(1, retries+1):
        try:
            pool = await asyncpg.create_pool(dsn=url, min_size=min_size, max_size=max_size)
            logger.info("Connected to PostgreSQL (asyncpg pool ready).")
            return pool
        except Exception as e:
            last_exc = e
            logger.warning(f"Database connection attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                await asyncio.sleep(delay)
    logger.error("All database connection attempts failed.")
    raise last_exc

def make_response_text(s: str) -> str:
    """Small helper to ensure a non-empty string is present"""
    return s or ""

def gen_session_id(length: int = Config.SESSION_ID_LEN) -> str:
    """Generate a short random session id"""
    return uuid.uuid4().hex[:length]

# Send message safely with retries and respecting common Telegram errors
async def safe_send_message(chat_id: int, text: str, **kwargs) -> bool:
    try:
        await bot.send_message(chat_id, text, **kwargs)
        return True
    except BotBlocked:
        logger.warning("BotBlocked: cannot send message to %s", chat_id)
        return False
    except ChatNotFound:
        logger.warning("ChatNotFound: cannot find chat %s", chat_id)
        return False
    except RetryAfter as e:
        logger.warning("Rate limited. Retry after %s seconds", e.timeout)
        await asyncio.sleep(e.timeout)
        return await safe_send_message(chat_id, text, **kwargs)
    except TelegramAPIError as e:
        logger.error("TelegramAPIError sending message to %s: %s", chat_id, e)
        return False
    except Exception as e:
        logger.exception("Unexpected error sending message to %s: %s", chat_id, e)
        return False

# Copy message safely (preserve inline buttons/media), uses copyMessage if available
async def safe_copy_message(chat_id: int, from_chat_id: int, message_id: int) -> bool:
    try:
        # aiogram v2 supports copy_message via bot.copy_message if Telegram API supports
        await bot.copy_message(chat_id, from_chat_id, message_id)
        return True
    except RetryAfter as e:
        logger.warning("Rate limited on copy_message. Sleeping %s", e.timeout)
        await asyncio.sleep(e.timeout)
        return await safe_copy_message(chat_id, from_chat_id, message_id)
    except Exception as e:
        logger.warning("copy_message failed for %s -> %s: %s", from_chat_id, chat_id, e)
        return False

# Simple keyboard factories
def help_button_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("Help", callback_data="help_cmd"))
    return kb

def create_session_options_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=3)
    kb.add(
        InlineKeyboardButton("✅ Protect", callback_data="protect_yes"),
        InlineKeyboardButton("❌ No Protect", callback_data="protect_no")
    )
    kb.add(
        InlineKeyboardButton("No delete (0m)", callback_data="delete_0"),
        InlineKeyboardButton("60m", callback_data="delete_60"),
        InlineKeyboardButton("1d", callback_data="delete_1440")
    )
    # Add more choices, including 1 week
    kb.add(
        InlineKeyboardButton("1w", callback_data="delete_10080"),
        InlineKeyboardButton("Create Session", callback_data="create_session")
    )
    return kb

def choose_msg_type_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Set Start Message", callback_data="msg_start"),
        InlineKeyboardButton("Set Help Message", callback_data="msg_help")
    )
    return kb

# Utility to schedule deletion of a message in the user's chat after delay seconds
async def schedule_deletion(chat_id: int, message_id: int, delay_seconds: int):
    """Schedule deletion after delay in background (per-user message cleanup)."""
    if delay_seconds <= 0:
        return
    async def _delete_after():
        await asyncio.sleep(delay_seconds)
        try:
            await bot.delete_message(chat_id, message_id)
        except Exception as e:
            logger.debug("Failed to delete message %s in chat %s: %s", message_id, chat_id, e)
    # Fire-and-forget — safe because it's local coroutine
    asyncio.create_task(_delete_after())

# ------------------------------
# Database Access Layer
# ------------------------------
class Database:
    # The design uses asyncpg and JSONB columns for lists
    @staticmethod
    async def init_db():
        """
        Create tables if they do not exist. Non-destructive.
        Tables:
          - users
          - messages
          - upload_sessions
          - statistics
        """
        global db_pool
        if db_pool is None:
            logger.error("init_db called without db_pool")
            return False
        async with db_pool.acquire() as conn:
            # users
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT UNIQUE NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    join_date TIMESTAMP DEFAULT NOW(),
                    last_active TIMESTAMP DEFAULT NOW(),
                    is_banned BOOLEAN DEFAULT FALSE
                )
            ''')
            # messages
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    message_type VARCHAR(50) UNIQUE NOT NULL,
                    text TEXT,
                    image_file_id TEXT,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            # upload_sessions
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS upload_sessions (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(100) UNIQUE NOT NULL,
                    owner_id BIGINT NOT NULL,
                    file_ids JSONB NOT NULL,
                    captions JSONB,
                    protect_content BOOLEAN DEFAULT TRUE,
                    auto_delete_minutes INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW(),
                    access_count INTEGER DEFAULT 0,
                    is_active BOOLEAN DEFAULT TRUE
                )
            ''')
            # statistics (single-row aggregator)
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS statistics (
                    id SERIAL PRIMARY KEY,
                    total_users INTEGER DEFAULT 0,
                    active_users INTEGER DEFAULT 0,
                    files_uploaded INTEGER DEFAULT 0,
                    sessions_completed INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT NOW()
                )
            ''')
            # Ensure at least one statistics row exists
            row = await conn.fetchrow('SELECT id FROM statistics LIMIT 1')
            if not row:
                await conn.execute('INSERT INTO statistics (total_users, active_users, files_uploaded, sessions_completed) VALUES (0,0,0,0)')
            # Ensure default messages exist
            for mtype, text in [('start', '👋 Welcome to File Sharing Bot! Use /help for instructions.'), ('help', '📖 Help - Use /upload (owner) to upload files.')]:
                r = await conn.fetchrow('SELECT id FROM messages WHERE message_type = $1', mtype)
                if not r:
                    await conn.execute('INSERT INTO messages (message_type, text) VALUES ($1, $2)', mtype, text)
        logger.info("Database schema ensured.")
        return True

    @staticmethod
    async def create_or_update_user(user: types.User):
        """Insert new user or update last_active and basic info"""
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO users (user_id, username, first_name, last_name)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id) DO UPDATE
                SET last_active = NOW(), username = EXCLUDED.username, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name
            ''', user.id, user.username, user.first_name, user.last_name)
            # Optionally update statistics total_users: we will compute counts on demand for /stats

    @staticmethod
    async def update_user_activity(user_id: int):
        async with db_pool.acquire() as conn:
            await conn.execute('UPDATE users SET last_active = NOW() WHERE user_id = $1', user_id)

    @staticmethod
    async def get_message_item(message_type: str) -> Optional[dict]:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM messages WHERE message_type = $1', message_type)
            if row:
                return dict(row)
            return None

    @staticmethod
    async def update_message_text(message_type: str, text: str):
        async with db_pool.acquire() as conn:
            await conn.execute('UPDATE messages SET text = $1, updated_at = NOW() WHERE message_type = $2', text, message_type)

    @staticmethod
    async def update_message_image(message_type: str, file_id: str):
        async with db_pool.acquire() as conn:
            await conn.execute('UPDATE messages SET image_file_id = $1, updated_at = NOW() WHERE message_type = $2', file_id, message_type)

    @staticmethod
    async def create_upload_session(owner_id: int, file_ids: List[str], captions: List[str], protect: bool, auto_delete_minutes: int) -> Optional[str]:
        """
        Store a session. file_ids and captions will be stored as JSONB arrays.
        Returns session_id or None on error.
        """
        session_id = gen_session_id()
        try:
            async with db_pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO upload_sessions (session_id, owner_id, file_ids, captions, protect_content, auto_delete_minutes)
                    VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6)
                ''', session_id, owner_id, json.dumps(file_ids), json.dumps(captions), protect, auto_delete_minutes)
                # update statistics counters
                await conn.execute('''
                    UPDATE statistics SET files_uploaded = files_uploaded + $1, sessions_completed = sessions_completed + 1, last_updated = NOW()
                ''', len(file_ids))
            return session_id
        except Exception as e:
            logger.exception("Failed to create upload session: %s", e)
            return None

    @staticmethod
    async def get_upload_session(session_id: str) -> Optional[dict]:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM upload_sessions WHERE session_id = $1 AND is_active = TRUE', session_id)
            if not row:
                return None
            # increment access_count
            await conn.execute('UPDATE upload_sessions SET access_count = access_count + 1 WHERE id = $1', row['id'])
            return dict(row)

    @staticmethod
    async def deactivate_session(session_id: str):
        async with db_pool.acquire() as conn:
            await conn.execute('UPDATE upload_sessions SET is_active = FALSE WHERE session_id = $1', session_id)

    @staticmethod
    async def get_all_active_users() -> List[int]:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch('SELECT user_id FROM users WHERE is_banned = FALSE')
            return [r['user_id'] for r in rows]

    @staticmethod
    async def get_stats() -> Dict[str, int]:
        async with db_pool.acquire() as conn:
            total_users = await conn.fetchval('SELECT COUNT(*) FROM users') or 0
            active_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE last_active > NOW() - INTERVAL '48 hours'") or 0
            stats = await conn.fetchrow('SELECT files_uploaded, sessions_completed FROM statistics ORDER BY id DESC LIMIT 1')
            return {
                'total_users': total_users,
                'active_users': active_users,
                'files_uploaded': stats['files_uploaded'] if stats else 0,
                'sessions_completed': stats['sessions_completed'] if stats else 0
            }

# ------------------------------
# Startup / Shutdown hooks (aiohttp web app)
# ------------------------------
async def on_startup(app: web.Application):
    """
    Called when web server starts. Initializes DB pool and sets webhook.
    """
    global db_pool
    logger.info("Starting bot startup...")
    # DB connect
    db_pool = await pg_try_connect(Config.DATABASE_URL, min_size=Config.DB_MIN_POOL, max_size=Config.DB_MAX_POOL)
    # Initialize DB schema
    ok = await Database.init_db()
    if not ok:
        logger.error("Database initialization failed; exiting.")
        # Fail hard (Render will show failure)
        raise RuntimeError("Database initialization failed")
    # Set webhook
    webhook_url = f"{Config.RENDER_EXTERNAL_URL.rstrip('/')}/webhook/{Config.BOT_TOKEN}"
    try:
        await bot.set_webhook(webhook_url)
        logger.info("Webhook set to: %s", webhook_url)
    except Exception as e:
        logger.exception("Failed to set webhook: %s", e)
        raise
    logger.info("Startup complete.")

async def on_shutdown(app: web.Application):
    logger.info("Shutting down...")
    # Remove webhook (optional)
    try:
        await bot.delete_webhook()
    except Exception as e:
        logger.debug("Error deleting webhook: %s", e)
    # Close DB pool
    global db_pool
    if db_pool:
        await db_pool.close()
        logger.info("Database pool closed.")
    # Close bot session
    try:
        await bot.session.close()
    except Exception as e:
        logger.debug("Error closing bot session: %s", e)
    logger.info("Shutdown complete.")
# ------------------------------
# Web handlers: /health and webhook entry
# ------------------------------
async def health(request):
    return web.Response(text="ok")

async def webhook_handler(request):
    """
    Receive Telegram updates via webhook and forward to aiogram Dispatcher.
    """
    try:
        token = request.match_info.get('token') or request.path.split('/')[-1]
        if token != Config.BOT_TOKEN:
            return web.Response(status=403, text="invalid token")
        data = await request.json()
        update = types.Update.to_object(data)
        # Process update with dispatcher
        await dp.process_update(types.Update(**data))
        return web.Response(status=200)
    except Exception as e:
        logger.exception("Webhook processing failed: %s", e)
        return web.Response(status=500, text="error")

# ------------------------------
# Helper: verify owner
# ------------------------------
def is_owner(user_id: int) -> bool:
    return user_id == Config.OWNER_ID

# ------------------------------
# /start and deep link handler
# ------------------------------
@dp.message_handler(commands=['start'])
async def cmd_start(message: Message):
    """
    /start - for normal use and deep link handling.
    If args exist -> treat as deep link session id.
    """
    try:
        user = message.from_user
        await Database.create_or_update_user(user)
        await Database.update_user_activity(user.id)

        args = message.get_args().strip() if hasattr(message, 'get_args') else (message.text.split(maxsplit=1)[1] if ' ' in message.text else '')
        if args:
            # treat args as session id
            session_id = args.split()[0].strip()
            await handle_deep_link_access(user.id, session_id)
            return

        # Normal start - show start message with optional image + Help inline button
        msg = await Database.get_message_item('start')
        text = msg['text'] if msg and msg.get('text') else "Welcome!"
        img = msg.get('image_file_id') if msg else None

        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("Help", callback_data="help_cmd"))

        if img:
            try:
                # Send as photo with caption
                await message.answer_photo(img, caption=text, reply_markup=kb)
                return
            except Exception as e:
                logger.warning("Failed to send start image: %s", e)

        await message.answer(text, reply_markup=kb)
    except Exception as e:
        logger.exception("/start handler failed: %s", e)
        await message.answer("An error occurred during /start.")

# Help command (users)
@dp.message_handler(commands=['help'])
async def cmd_help(message: Message):
    try:
        user = message.from_user
        await Database.update_user_activity(user.id)

        msg = await Database.get_message_item('help')
        text = msg['text'] if msg and msg.get('text') else "Help info"
        img = msg.get('image_file_id') if msg else None

        if img:
            try:
                await message.answer_photo(img, caption=text)
                return
            except Exception as e:
                logger.warning("Failed to send help image: %s", e)

        await message.answer(text)
    except Exception as e:
        logger.exception("/help handler failed: %s", e)
        await message.answer("An error occurred while fetching help.")

# Callback for inline 'Help' button
@dp.callback_query_handler(lambda c: c.data == 'help_cmd')
async def callback_help(c: CallbackQuery):
    try:
        await c.answer()  # remove loading
        await cmd_help(c.message)
    except Exception as e:
        logger.exception("help callback failed: %s", e)
        try:
            await c.answer("Error")
        except Exception:
            pass

# ------------------------------
# /setmessage (owner only)
# ------------------------------
@dp.message_handler(commands=['setmessage'])
async def cmd_setmessage(message: Message):
    """
    Owner starts /setmessage -> choose start/help, then send text.
    """
    try:
        if not is_owner(message.from_user.id):
            await message.answer("Unauthorized.")
            return
        # Set FSM state to select type
        state = dp.current_state(chat=message.chat.id, user=message.from_user.id)
        await state.set_state(MessageStates.waiting_for_message_type.state)
        await state.update_data(setting_image=False)
        await message.answer("Select which message to update:", reply_markup=choose_msg_type_keyboard())
    except Exception as e:
        logger.exception("/setmessage failed: %s", e)
        await message.answer("Error starting setmessage.")

# /setimage (owner only)
@dp.message_handler(commands=['setimage'])
async def cmd_setimage(message: Message):
    """
    Owner replies to an image with /setimage. The bot stores the replied image file_id in FSM and asks which message to set.
    """
    try:
        if not is_owner(message.from_user.id):
            await message.answer("Unauthorized.")
            return
        if not message.reply_to_message:
            await message.answer("Please reply to the image you want to set as start/help image using /setimage.")
            return

        # Extract file_id from replied message (photo or image document)
        replied = message.reply_to_message
        file_id = None
        if replied.photo:
            file_id = replied.photo[-1].file_id
        elif replied.document and replied.document.mime_type and replied.document.mime_type.startswith('image/'):
            file_id = replied.document.file_id
        else:
            await message.answer("Replied message does not contain an image. Reply to a photo or image document.")
            return

        # store file_id in FSM
        state = dp.current_state(chat=message.chat.id, user=message.from_user.id)
        await state.set_state(MessageStates.waiting_for_message_type.state)
        await state.update_data(setting_image=True, image_info={'file_id': file_id})
        await message.answer("Do you want to set this as the start image or help image?", reply_markup=choose_msg_type_keyboard())
    except Exception as e:
        logger.exception("/setimage failed: %s", e)
        await message.answer("Error starting setimage.")

# Callback handler for selecting message type (start/help) — handles both text and image flows
@dp.callback_query_handler(lambda c: c.data in ['msg_start', 'msg_help'], state=MessageStates.waiting_for_message_type)
async def callback_msg_type(c: CallbackQuery, state: FSMContext):
    try:
        await c.answer()
        data = await state.get_data()
        is_image_flow = data.get('setting_image', False)
        msg_type = 'start' if c.data == 'msg_start' else 'help'

        if is_image_flow:
            image_info = data.get('image_info') or {}
            file_id = image_info.get('file_id')
            if not file_id:
                await c.message.answer("No image found in state. Retry /setimage by replying to an image.")
                await state.finish()
                return
            # Store in DB
            await Database.update_message_image(msg_type, file_id)
            await c.message.answer(f"✅ {msg_type} image updated.")
            await state.finish()
            return

        # else text flow
        await state.update_data(msg_type=msg_type)
        await MessageStates.waiting_for_text.set()
        await c.message.answer(f"Send the new text for <b>{msg_type}</b> message now (send /cancel to abort).")
    except Exception as e:
        logger.exception("callback_msg_type error: %s", e)
        try:
            await c.answer("Error")
        except Exception:
            pass
        await state.finish()

# Text handler for storing custom start/help texts
@dp.message_handler(state=MessageStates.waiting_for_text)
async def handler_message_text(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        msg_type = data.get('msg_type')
        if not msg_type:
            await message.answer("Unknown message type. Aborting.")
            await state.finish()
            return
        text = message.text or ""
        await Database.update_message_text(msg_type, text)
        await message.answer(f"✅ {msg_type} message updated.")
        await state.finish()
    except Exception as e:
        logger.exception("handler_message_text failed: %s", e)
        await state.finish()
        await message.answer("Error updating message text.")

# /cancel to abort any FSM flow (owner only)
@dp.message_handler(commands=['cancel'])
async def cmd_cancel(message: Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    try:
        state = dp.current_state(chat=message.chat.id, user=message.from_user.id)
        current = await state.get_state()
        if current:
            await state.finish()
            await message.answer("Operation cancelled.")
        else:
            await message.answer("No active operation.")
    except Exception as e:
        logger.exception("/cancel error: %s", e)
        await message.answer("Error cancelling.")
# ------------------------------
# /upload multi-step flow (owner-only)
# Steps:
# 1) /upload -> set FSM waiting_for_files
# 2) owner sends multiple files to UPLOAD_CHANNEL (the bot should copy files to UPLOAD_CHANNEL and store file_ids in FSM)
#    Note: To ensure originals are in the upload channel, the bot will copy the file from the owner's chat to the upload channel and store that file_id.
# 3) Owner sends /d (done) to finish files input, or /c to cancel.
# 4) Bot asks protect and auto-delete options (via callback buttons)
# 5) Owner presses 'Create Session'
# 6) Bot stores session in DB and provides deep link
# ------------------------------
@dp.message_handler(commands=['upload'])
async def cmd_upload(message: Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    try:
        # Initialize FSM
        state = dp.current_state(chat=message.chat.id, user=message.from_user.id)
        await state.set_state(UploadStates.waiting_for_files.state)
        await state.update_data(files=[], captions=[])
        await message.answer("📤 Upload mode started.\nPlease send files (documents, photos, videos). Reply with /d when done or /c to cancel.")
    except Exception as e:
        logger.exception("/upload failed: %s", e)
        await message.answer("Error starting upload.")

# Cancel for upload flow
@dp.message_handler(commands=['c'], state=UploadStates.waiting_for_files)
async def cmd_cancel_upload(message: Message, state: FSMContext):
    try:
        await state.finish()
        await message.answer("Upload cancelled.")
    except Exception as e:
        logger.exception("cmd_cancel_upload failed: %s", e)
        await message.answer("Error cancelling upload.")

# Done for upload flow
@dp.message_handler(commands=['d'], state=UploadStates.waiting_for_files)
async def cmd_done_upload(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        files = data.get('files', [])
        if not files:
            await message.answer("No files uploaded. Upload cancelled.")
            await state.finish()
            return
        # move to options selection
        await UploadStates.waiting_for_options.set()
        # Default options
        await state.update_data(protect_content=True, auto_delete_minutes=0)
        await message.answer(f"📁 Received {len(files)} files.\nChoose options:", reply_markup=create_session_options_keyboard())
    except Exception as e:
        logger.exception("cmd_done_upload failed: %s", e)
        await state.finish()
        await message.answer("Error finishing upload.")

# Handler for file messages during upload: the owner sends files and the bot copies them to UPLOAD_CHANNEL and stores the resulting file_id
@dp.message_handler(content_types=[ContentType.DOCUMENT, ContentType.PHOTO, ContentType.VIDEO], state=UploadStates.waiting_for_files)
async def handler_upload_files(message: Message, state: FSMContext):
    """
    Accepts file message from owner. Copy file to the UPLOAD_CHANNEL to persist in channel.
    Stores the channel-copied file_id in FSM data to create sessions that point to channel file ids.
    """
    try:
        if not is_owner(message.from_user.id):
            await message.answer("Unauthorized.")
            return
        # Determine file to copy (photo/video/document)
        file_to_copy = None
        content_type = None
        if message.photo:
            # pick highest resolution
            file_to_copy = message.photo[-1].file_id
            content_type = 'photo'
        elif message.video:
            file_to_copy = message.video.file_id
            content_type = 'video'
        elif message.document:
            file_to_copy = message.document.file_id
            content_type = 'document'
        else:
            await message.answer("Unsupported file type.")
            return

        # copy to upload channel
        try:
            # We use copy_message to preserve file without reupload
            copied = await bot.copy_message(Config.UPLOAD_CHANNEL_ID, message.chat.id, message.message_id)
            # copied is a Message instance. Extract file_id based on returned fields.
            # Note: copy_message returns a Message with new message_id in channel
            # We need to fetch the message from the channel to inspect available attributes.
            # However aiogram returns a Message object so we can inspect it.
            channel_msg = copied
            # Determine file_id in channel message
            channel_file_id = None
            if channel_msg.photo:
                channel_file_id = channel_msg.photo[-1].file_id
            elif channel_msg.video:
                channel_file_id = channel_msg.video.file_id
            elif channel_msg.document:
                channel_file_id = channel_msg.document.file_id

            if not channel_file_id:
                logger.warning("Copied message did not contain detectible file_id.")
                await message.answer("Failed to store file in upload channel.")
                return

            # Save file id and caption into FSM
            data = await state.get_data()
            files: List[str] = data.get('files', [])
            captions: List[str] = data.get('captions', [])
            files.append(channel_file_id)
            captions.append(message.caption or "")
            await state.update_data(files=files, captions=captions)
            await message.answer(f"✅ Stored file #{len(files)} in upload channel.")
        except Exception as e:
            logger.exception("Failed to copy message to upload channel: %s", e)
            await message.answer("Failed to save file to upload channel. Ensure bot is admin in the channel and has permission to post.")
    except Exception as e:
        logger.exception("handler_upload_files unexpected: %s", e)
        await message.answer("Error while storing file.")

# Callback handlers for option buttons during upload (protect and delete choices)
@dp.callback_query_handler(lambda c: c.data and (c.data.startswith('protect_') or c.data.startswith('delete_')), state=UploadStates.waiting_for_options)
async def callback_upload_options(c: CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        if c.data.startswith('protect_'):
            protect = c.data == 'protect_yes'
            await state.update_data(protect_content=protect)
            await c.answer(text=f"Protect set to {'YES' if protect else 'NO'}")
            return
        if c.data.startswith('delete_'):
            parts = c.data.split('_')
            minutes = int(parts[1]) if len(parts) > 1 else 0
            await state.update_data(auto_delete_minutes=minutes)
            label = f"{minutes} min" if minutes < 1440 else f"{minutes//1440} day(s)"
            await c.answer(text=f"Auto-delete set to {label}")
            return
    except Exception as e:
        logger.exception("callback_upload_options failed: %s", e)
        try:
            await c.answer("Error")
        except Exception:
            pass

# Create session callback
@dp.callback_query_handler(lambda c: c.data == 'create_session', state=UploadStates.waiting_for_options)
async def callback_create_session(c: CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        files = data.get('files', [])
        captions = data.get('captions', [])
        protect = data.get('protect_content', True)
        auto_delete = data.get('auto_delete_minutes', 0)
        owner_id = c.from_user.id

        if not files:
            await c.message.answer("No files to create a session.")
            await state.finish()
            await c.answer()
            return

        session_id = await Database.create_upload_session(owner_id, files, captions, protect, auto_delete)
        if not session_id:
            await c.message.answer("Failed to create session.")
            await state.finish()
            await c.answer()
            return

        # Generate deep link (t.me/<bot_username>?start=<session_id>)
        try:
            bot_username = (await bot.get_me()).username
            deep_link = f"https://t.me/{bot_username}?start={session_id}"
        except Exception as e:
            # fallback: use render url style (still works via start param)
            deep_link = f"https://t.me/{session_id}"

        await c.message.answer(f"✅ Session created!\nFiles: {len(files)}\nProtect: {'Yes' if protect else 'No'}\nAuto-delete: {auto_delete} min\n\nShare link:\n{deep_link}")
        await state.finish()
        await c.answer()
    except Exception as e:
        logger.exception("callback_create_session failed: %s", e)
        try:
            await c.answer("Error creating session")
        except Exception:
            pass
        await state.finish()

# ------------------------------
# Deep link access handling
# ------------------------------
async def handle_deep_link_access(user_id: int, session_id: str):
    """
    Retrieve session from DB by session_id and send files to user.
    Honor protect_content and auto-delete settings unless owner (owner bypass).
    """
    try:
        session = await Database.get_upload_session(session_id)
        if not session:
            await safe_send_message(user_id, "❌ Session not found or expired.")
            return

        file_ids = session.get('file_ids') or []
        # file_ids might be stored as JSON string; ensure list
        if isinstance(file_ids, str):
            try:
                file_ids = json.loads(file_ids)
            except Exception:
                # if parsing fails, wrap single element list
                file_ids = [file_ids]

        captions = session.get('captions') or []
        if isinstance(captions, str):
            try:
                captions = json.loads(captions)
            except Exception:
                captions = [captions]

        protect = bool(session.get('protect_content', True))
        auto_delete = int(session.get('auto_delete_minutes', 0))

        is_owner_fetch = (user_id == Config.OWNER_ID)
        if is_owner_fetch:
            protect = False
            auto_delete = 0

        # Send each file with appropriate send method attempts
        for idx, file_id in enumerate(file_ids):
            caption = captions[idx] if idx < len(captions) else ""
            sent_msg = None
            # Attempt send_photo -> send_video -> send_document
            try:
                # try photo
                try:
                    sent_msg = await bot.send_photo(user_id, file_id, caption=caption, protect_content=protect)
                except Exception:
                    # try video
                    try:
                        sent_msg = await bot.send_video(user_id, file_id, caption=caption, protect_content=protect)
                    except Exception:
                        # try document
                        sent_msg = await bot.send_document(user_id, file_id, caption=caption, protect_content=protect)
                # schedule deletion if needed
                if sent_msg and auto_delete > 0 and not is_owner_fetch:
                    await schedule_deletion(sent_msg.chat.id, sent_msg.message_id, auto_delete * 60)
            except Exception as e:
                logger.exception("Failed to send session file %s to %s: %s", file_id, user_id, e)
                # continue to next file

        # Inform user about auto-delete if relevant
        if auto_delete > 0 and not is_owner_fetch:
            # Prepare human readable label
            if auto_delete >= 1440:
                time_label = f"{auto_delete//1440} day(s)"
            elif auto_delete >= 60:
                time_label = f"{auto_delete//60} hour(s)"
            else:
                time_label = f"{auto_delete} minute(s)"
            notice = await bot.send_message(user_id, f"⚠️ Files will auto-delete in {time_label}.")
            await schedule_deletion(notice.chat.id, notice.message_id, auto_delete * 60)
    except Exception as e:
        logger.exception("handle_deep_link_access failed: %s", e)
        await safe_send_message(user_id, "An error occurred while accessing this session.")

# ------------------------------
# /broadcast - owner only (copy message to all users)
# Owner replies to a message with /broadcast to begin
# ------------------------------
@dp.message_handler(commands=['broadcast'])
async def cmd_broadcast(message: Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    try:
        if not message.reply_to_message:
            await message.answer("Please reply to the message you want to broadcast and use /broadcast.")
            return
        # Confirm broadcast
        state = dp.current_state(chat=message.chat.id, user=message.from_user.id)
        await state.set_state(BroadcastStates.waiting_for_broadcast_confirmation.state)
        await state.update_data(broadcast_original={'chat_id': message.reply_to_message.chat.id, 'message_id': message.reply_to_message.message_id})
        await message.answer("⚠️ Confirm broadcast: Type YES to send to all users, or /cancel to abort.")
    except Exception as e:
        logger.exception("/broadcast error: %s", e)
        await message.answer("Error starting broadcast.")

@dp.message_handler(state=BroadcastStates.waiting_for_broadcast_confirmation)
async def handler_broadcast_confirm(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        await state.finish()
        return
    try:
        if (message.text or "").strip().upper() != "YES":
            await message.answer("Broadcast cancelled.")
            await state.finish()
            return
        data = await state.get_data()
        orig = data.get('broadcast_original', {})
        from_chat = orig.get('chat_id')
        msg_id = orig.get('message_id')
        if not from_chat or not msg_id:
            await message.answer("Original message info missing.")
            await state.finish()
            return

        users = await Database.get_all_active_users()
        total = len(users)
        status = await message.answer(f"Broadcast starting to {total} users...")
        success = 0
        failed = 0
        start_time = time.time()

        # We will attempt to copy the message to users (preserve media and remove forwarded tag)
        for idx, uid in enumerate(users, start=1):
            try:
                ok = await safe_copy_message(uid, from_chat, msg_id)
                if ok:
                    success += 1
                else:
                    # fallback: try to send a small notice
                    await safe_send_message(uid, "Message from bot owner.")
                    failed += 1
            except Exception as e:
                failed += 1
                logger.debug("Broadcast to %s failed: %s", uid, e)

            # light throttling to avoid hitting rate limits
            if idx % 20 == 0:
                await asyncio.sleep(1)

            # update status occasionally
            if idx % 50 == 0 or idx == total:
                try:
                    elapsed = int(time.time() - start_time)
                    await status.edit_text(f"Broadcast progress: {idx}/{total} (success {success}, failed {failed}) - elapsed {elapsed}s")
                except MessageNotModified:
                    pass
                except Exception:
                    pass

        await status.edit_text(f"Broadcast completed. Success: {success}, Failed: {failed}")
        await state.finish()
    except Exception as e:
        logger.exception("handler_broadcast_confirm failed: %s", e)
        await message.answer("Broadcast failed.")
        await state.finish()

# ------------------------------
# /stats owner-only
# ------------------------------
@dp.message_handler(commands=['stats'])
async def cmd_stats(message: Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    try:
        s = await Database.get_stats()
        text = (
            f"📊 <b>Bot Statistics</b>\n\n"
            f"👥 Total users: {s['total_users']}\n"
            f"🟢 Active (48h): {s['active_users']}\n"
            f"📁 Files uploaded: {s['files_uploaded']}\n"
            f"🔗 Sessions completed: {s['sessions_completed']}\n"
        )
        await message.answer(text)
    except Exception as e:
        logger.exception("/stats failed: %s", e)
        await message.answer("Error fetching stats.")

# ------------------------------
# Global error handler for dispatcher (avoid crashes)
# ------------------------------
@dp.errors_handler()
async def global_error_handler(update, exception):
    logger.exception("Dispatcher error: update=%s exception=%s", update, exception)
    # Returning True prevents further propagation
    return True

# ------------------------------
# Web app runner
# ------------------------------
def build_app():
    app = web.Application()
    app.router.add_get('/', health)
    app.router.add_get('/health', health)
    app.router.add_post('/webhook/{token}', webhook_handler)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app

# Entry point
if __name__ == '__main__':
    app = build_app()
    port = Config.PORT
    # Use web.run_app to start aiohttp server (webhook-only mode)
    web.run_app(app, host='0.0.0.0', port=port)