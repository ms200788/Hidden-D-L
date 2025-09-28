#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram File Sharing Bot - Webhook-only (aiogram v2) with Neon (Postgres)
Fixed: upload channel checks, robust file-copying, BOT_USERNAME capture, restore_db
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

from aiogram import Bot, Dispatcher, types
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ContentType
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
from aiogram.utils.exceptions import BotBlocked, ChatNotFound, RetryAfter, TelegramAPIError, MessageNotModified

from aiohttp import web
import asyncpg

# ------------------------------
# Configuration
# ------------------------------
class Config:
    BOT_TOKEN: str = os.getenv('BOT_TOKEN', '').strip()
    OWNER_ID: int = int(os.getenv('OWNER_ID', '0') or 0)
    DATABASE_URL: str = os.getenv('DATABASE_URL', '').strip()
    RENDER_EXTERNAL_URL: str = os.getenv('RENDER_EXTERNAL_URL', '').strip()
    PORT: int = int(os.getenv('PORT', 5000))
    UPLOAD_CHANNEL_ID: int = int(os.getenv('UPLOAD_CHANNEL_ID', '0') or 0)
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO').upper()
    DB_MIN_POOL: int = int(os.getenv('DB_MIN_POOL', '1'))
    DB_MAX_POOL: int = int(os.getenv('DB_MAX_POOL', '10'))
    SESSION_ID_LEN: int = int(os.getenv('SESSION_ID_LEN', '8'))

if not Config.BOT_TOKEN:
    print("FATAL: BOT_TOKEN required", file=sys.stderr); sys.exit(1)
if not Config.OWNER_ID:
    print("FATAL: OWNER_ID required", file=sys.stderr); sys.exit(1)
if not Config.DATABASE_URL:
    print("FATAL: DATABASE_URL required", file=sys.stderr); sys.exit(1)
if not Config.RENDER_EXTERNAL_URL:
    print("FATAL: RENDER_EXTERNAL_URL required", file=sys.stderr); sys.exit(1)
if not Config.UPLOAD_CHANNEL_ID:
    print("FATAL: UPLOAD_CHANNEL_ID required", file=sys.stderr); sys.exit(1)

# ------------------------------
# Logging
# ------------------------------
numeric_level = getattr(logging, Config.LOG_LEVEL, logging.INFO)
logging.basicConfig(level=numeric_level, format='%(asctime)s | %(levelname)s | %(name)s:%(lineno)d | %(message)s')
logger = logging.getLogger("file_share_bot")

# ------------------------------
# Bot & Dispatcher
# ------------------------------
bot = Bot(token=Config.BOT_TOKEN, parse_mode='HTML')
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())
Bot.set_current(bot)

# Globals set at startup
db_pool: Optional[asyncpg.pool.Pool] = None
BOT_USERNAME: Optional[str] = None
BOT_ID: Optional[int] = None
UPLOAD_CHANNEL_READY: bool = False

# ------------------------------
# FSM states
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
# Utilities
# ------------------------------
def gen_session_id(length: int = Config.SESSION_ID_LEN) -> str:
    return uuid.uuid4().hex[:length]

async def pg_try_connect(url: str, min_size: int = 1, max_size: int = 10, retries: int = 5, delay: int = 2) -> asyncpg.pool.Pool:
    last = None
    for i in range(1, retries+1):
        try:
            pool = await asyncpg.create_pool(dsn=url, min_size=min_size, max_size=max_size)
            logger.info("Connected to PostgreSQL")
            return pool
        except Exception as e:
            last = e
            logger.warning("DB connect attempt %d failed: %s", i, e)
            if i < retries:
                await asyncio.sleep(delay)
    logger.error("DB connection failed after %d attempts", retries)
    raise last

async def safe_send_message(chat_id: int, text: str, **kwargs) -> bool:
    try:
        await bot.send_message(chat_id, text, **kwargs)
        return True
    except RetryAfter as e:
        logger.warning("Rate limited, sleeping %s", e.timeout)
        await asyncio.sleep(e.timeout)
        return await safe_send_message(chat_id, text, **kwargs)
    except (BotBlocked, ChatNotFound) as e:
        logger.warning("Cannot message %s: %s", chat_id, e)
        return False
    except Exception as e:
        logger.exception("Unexpected error sending message: %s", e)
        return False

async def safe_copy_message(to_chat: int, from_chat: int, message_id: int) -> Optional[types.Message]:
    try:
        msg = await bot.copy_message(to_chat, from_chat, message_id)
        return msg
    except RetryAfter as e:
        await asyncio.sleep(e.timeout)
        return await safe_copy_message(to_chat, from_chat, message_id)
    except Exception as e:
        logger.debug("copy_message failed: %s", e)
        return None

def create_session_options_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=3)
    kb.add(InlineKeyboardButton("✅ Protect", callback_data="protect_yes"),
           InlineKeyboardButton("❌ No Protect", callback_data="protect_no"))
    kb.add(InlineKeyboardButton("No delete (0m)", callback_data="delete_0"),
           InlineKeyboardButton("60m", callback_data="delete_60"),
           InlineKeyboardButton("1d", callback_data="delete_1440"))
    kb.add(InlineKeyboardButton("1w", callback_data="delete_10080"),
           InlineKeyboardButton("Create Session", callback_data="create_session"))
    return kb

def choose_msg_type_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("Set Start Message", callback_data="msg_start"),
           InlineKeyboardButton("Set Help Message", callback_data="msg_help"))
    return kb

# ------------------------------
# Database layer
# ------------------------------
class Database:
    @staticmethod
    async def init_db():
        global db_pool
        if db_pool is None:
            logger.error("init_db called without db_pool")
            return False
        async with db_pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT UNIQUE NOT NULL,
                    username TEXT, first_name TEXT, last_name TEXT,
                    join_date TIMESTAMP DEFAULT NOW(),
                    last_active TIMESTAMP DEFAULT NOW(),
                    is_banned BOOLEAN DEFAULT FALSE
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    message_type VARCHAR(50) UNIQUE NOT NULL,
                    text TEXT,
                    image_file_id TEXT,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')
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
            # ensure statistics row exists
            r = await conn.fetchrow('SELECT id FROM statistics LIMIT 1')
            if not r:
                await conn.execute('INSERT INTO statistics (total_users, active_users, files_uploaded, sessions_completed) VALUES (0,0,0,0)')
            # ensure start/help messages exist
            for mt, txt in [('start', '👋 Welcome to File Sharing Bot! Use /help for instructions.'), ('help', '📖 Help - Use /upload (owner) to upload files.')]:
                r = await conn.fetchrow('SELECT id FROM messages WHERE message_type = $1', mt)
                if not r:
                    await conn.execute('INSERT INTO messages (message_type, text) VALUES ($1, $2)', mt, txt)
        logger.info("Database ensured")
        return True

    @staticmethod
    async def create_or_update_user(user: types.User):
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO users (user_id, username, first_name, last_name)
                VALUES ($1,$2,$3,$4)
                ON CONFLICT (user_id) DO UPDATE SET last_active = NOW(), username = EXCLUDED.username, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name
            ''', user.id, user.username, user.first_name, user.last_name)

    @staticmethod
    async def update_user_activity(user_id: int):
        async with db_pool.acquire() as conn:
            await conn.execute('UPDATE users SET last_active = NOW() WHERE user_id = $1', user_id)

    @staticmethod
    async def get_message_item(message_type: str) -> Optional[dict]:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM messages WHERE message_type = $1', message_type)
            return dict(row) if row else None

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
        session_id = gen_session_id()
        try:
            async with db_pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO upload_sessions (session_id, owner_id, file_ids, captions, protect_content, auto_delete_minutes)
                    VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6)
                ''', session_id, owner_id, json.dumps(file_ids), json.dumps(captions), protect, auto_delete_minutes)
                await conn.execute('UPDATE statistics SET files_uploaded = files_uploaded + $1, sessions_completed = sessions_completed + 1, last_updated = NOW()', len(file_ids))
            return session_id
        except Exception as e:
            logger.exception("create_upload_session failed: %s", e)
            return None

    @staticmethod
    async def get_upload_session(session_id: str) -> Optional[dict]:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM upload_sessions WHERE session_id = $1 AND is_active = TRUE', session_id)
            if not row:
                return None
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
# Startup/shutdown hooks with channel checks and BOT_NAME capture
# ------------------------------
async def on_startup(app: web.Application):
    global db_pool, BOT_USERNAME, BOT_ID, UPLOAD_CHANNEL_READY
    logger.info("Startup: connecting to DB")
    db_pool = await pg_try_connect(Config.DATABASE_URL, min_size=Config.DB_MIN_POOL, max_size=Config.DB_MAX_POOL)
    ok = await Database.init_db()
    if not ok:
        logger.critical("DB init failed, exiting")
        raise RuntimeError("DB init failed")

    # Capture bot identity
    try:
        me = await bot.get_me()
        BOT_USERNAME = me.username
        BOT_ID = me.id
        logger.info("Bot identity: @%s id=%s", BOT_USERNAME, BOT_ID)
    except Exception as e:
        logger.exception("Failed to get bot identity: %s", e)
        # We can't proceed reliably without this; rethrow
        raise

    # Check upload channel membership and basic permissions
    UPLOAD_CHANNEL_READY = False
    try:
        # get_chat_member: will raise if bot not in channel or has no permission to access
        cm = await bot.get_chat_member(Config.UPLOAD_CHANNEL_ID, BOT_ID)
        logger.info("Upload channel chat member status: %s", cm.status)
        # For channels, bot must be administrator with post permissions; we check status
        if cm.status in ('administrator', 'creator'):
            # If administrator, consider it ready
            UPLOAD_CHANNEL_READY = True
            logger.info("Upload channel ready: bot is admin")
        else:
            # not admin (member), still might be able to post if channel allows? for safety require admin
            logger.warning("Bot is not admin in upload channel; posting may fail. Status: %s", cm.status)
            UPLOAD_CHANNEL_READY = False
    except Exception as e:
        logger.exception("Cannot access upload channel %s: %s", Config.UPLOAD_CHANNEL_ID, e)
        UPLOAD_CHANNEL_READY = False

    # Set webhook
    webhook_url = f"{Config.RENDER_EXTERNAL_URL.rstrip('/')}/webhook/{Config.BOT_TOKEN}"
    try:
        await bot.set_webhook(webhook_url)
        logger.info("Webhook set to: %s", webhook_url)
    except Exception as e:
        logger.exception("Failed to set webhook: %s", e)
        raise

    # If upload channel is not ready, inform owner via DM (best effort)
    if not UPLOAD_CHANNEL_READY:
        try:
            await safe_send_message(Config.OWNER_ID, f"⚠️ Bot is not admin or cannot access the upload channel (ID: {Config.UPLOAD_CHANNEL_ID}). Please add the bot as an admin to that channel with permission to post messages. Uploads will fail until fixed.")
        except Exception:
            logger.debug("Could not message owner about upload channel readiness.")

    logger.info("Startup complete.")

async def on_shutdown(app: web.Application):
    global db_pool
    logger.info("Shutting down...")
    try:
        await bot.delete_webhook()
    except Exception as e:
        logger.debug("delete_webhook failed: %s", e)
    if db_pool:
        await db_pool.close()
        logger.info("DB pool closed")
    try:
        await bot.session.close()
    except Exception:
        pass
    logger.info("Shutdown complete.")
# ------------------------------
# Web handlers
# ------------------------------
async def health(request):
    return web.Response(text="ok")

async def webhook_handler(request):
    try:
        token = request.match_info.get('token') or request.path.split('/')[-1]
        if token != Config.BOT_TOKEN:
            return web.Response(status=403, text="invalid token")
        data = await request.json()
        await dp.process_update(types.Update(**data))
        return web.Response(status=200)
    except Exception as e:
        logger.exception("Webhook handler error: %s", e)
        return web.Response(status=500, text="error")

# ------------------------------
# simple helpers
# ------------------------------
def is_owner(uid: int) -> bool:
    return uid == Config.OWNER_ID

# ------------------------------
# /start command and deep link support
# ------------------------------
@dp.message_handler(commands=['start'])
async def cmd_start(message: Message):
    try:
        user = message.from_user
        await Database.create_or_update_user(user)
        await Database.update_user_activity(user.id)

        args = message.get_args().strip() if hasattr(message, 'get_args') else ''
        if args:
            # treat first arg as session id
            session_id = args.split()[0].strip()
            await handle_deep_link_access(user.id, session_id)
            return

        msg = await Database.get_message_item('start')
        text = msg.get('text') if msg else "Welcome!"
        img = msg.get('image_file_id') if msg else None
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("Help", callback_data="help_cmd"))

        if img:
            try:
                await message.answer_photo(img, caption=text, reply_markup=kb)
                return
            except Exception as e:
                logger.warning("start image send failed: %s", e)
        await message.answer(text, reply_markup=kb)
    except Exception as e:
        logger.exception("/start failed: %s", e)
        await message.answer("An error occurred during /start.")

@dp.callback_query_handler(lambda c: c.data == 'help_cmd')
async def callback_help(c: CallbackQuery):
    await c.answer()
    await cmd_help(c.message)

@dp.message_handler(commands=['help'])
async def cmd_help(message: Message):
    try:
        user = message.from_user
        await Database.update_user_activity(user.id)
        msg = await Database.get_message_item('help')
        text = msg.get('text') if msg else "Help info"
        img = msg.get('image_file_id') if msg else None
        if img:
            try:
                await message.answer_photo(img, caption=text)
                return
            except Exception as e:
                logger.warning("help image send failed: %s", e)
        await message.answer(text)
    except Exception as e:
        logger.exception("/help failed: %s", e)
        await message.answer("An error occurred while fetching help.")

# ------------------------------
# /setmessage and /setimage flows (owner only)
# ------------------------------
@dp.message_handler(commands=['setmessage'])
async def cmd_setmessage(message: Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    try:
        state = dp.current_state(chat=message.chat.id, user=message.from_user.id)
        await state.set_state(MessageStates.waiting_for_message_type.state)
        await state.update_data(setting_image=False)
        await message.answer("Select which message to update:", reply_markup=choose_msg_type_keyboard())
    except Exception as e:
        logger.exception("/setmessage error: %s", e)
        await message.answer("Error starting setmessage.")

@dp.message_handler(commands=['setimage'])
async def cmd_setimage(message: Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    if not message.reply_to_message:
        await message.answer("Please reply to an image message with /setimage.")
        return
    replied = message.reply_to_message
    file_id = None
    if replied.photo:
        file_id = replied.photo[-1].file_id
    elif replied.document and replied.document.mime_type and replied.document.mime_type.startswith('image/'):
        file_id = replied.document.file_id
    else:
        await message.answer("Replied message is not a photo/image document.")
        return
    try:
        state = dp.current_state(chat=message.chat.id, user=message.from_user.id)
        await state.set_state(MessageStates.waiting_for_message_type.state)
        await state.update_data(setting_image=True, image_info={'file_id': file_id})
        await message.answer("Choose which message to set this image for:", reply_markup=choose_msg_type_keyboard())
    except Exception as e:
        logger.exception("/setimage error: %s", e)
        await message.answer("Error starting setimage.")

@dp.callback_query_handler(lambda c: c.data in ['msg_start','msg_help'], state=MessageStates.waiting_for_message_type)
async def callback_msg_type(c: CallbackQuery, state: FSMContext):
    await c.answer()
    try:
        data = await state.get_data()
        is_image_flow = data.get('setting_image', False)
        msg_type = 'start' if c.data == 'msg_start' else 'help'
        if is_image_flow:
            image_info = data.get('image_info') or {}
            file_id = image_info.get('file_id')
            if not file_id:
                await c.message.answer("No image found in state. Retry /setimage.")
                await state.finish()
                return
            await Database.update_message_image(msg_type, file_id)
            await c.message.answer(f"✅ {msg_type} image updated.")
            await state.finish()
            return
        # text flow
        await state.update_data(msg_type=msg_type)
        await state.set_state(MessageStates.waiting_for_text.state)
        await c.message.answer(f"Send the new text for <b>{msg_type}</b> now.")
    except Exception as e:
        logger.exception("callback_msg_type error: %s", e)
        await state.finish()
        try:
            await c.answer("Error")
        except Exception:
            pass

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
        await message.answer("Error updating message.")

@dp.message_handler(commands=['cancel'])
async def cmd_cancel(message: Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    try:
        state = dp.current_state(chat=message.chat.id, user=message.from_user.id)
        cur = await state.get_state()
        if cur:
            await state.finish()
            await message.answer("Operation cancelled.")
        else:
            await message.answer("No active operation.")
    except Exception as e:
        logger.exception("/cancel error: %s", e)
        await message.answer("Error cancelling.")
# ------------------------------
# /upload multi-step flow (owner only) with robust channel copy logic
# ------------------------------
@dp.message_handler(commands=['upload'])
async def cmd_upload(message: Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    try:
        state = dp.current_state(chat=message.chat.id, user=message.from_user.id)
        await state.set_state(UploadStates.waiting_for_files.state)
        await state.update_data(files=[], captions=[])
        await message.answer("📤 Upload started. Send files (photo/video/document). When finished send /d (done) or /c (cancel).")
    except Exception as e:
        logger.exception("/upload failed: %s", e)
        await message.answer("Error starting upload.")

@dp.message_handler(commands=['c'], state=UploadStates.waiting_for_files)
async def cmd_cancel_upload(message: Message, state: FSMContext):
    try:
        await state.finish()
        await message.answer("Upload cancelled.")
    except Exception as e:
        logger.exception("cmd_cancel_upload failed: %s", e)
        await message.answer("Error cancelling upload.")

@dp.message_handler(commands=['d'], state=UploadStates.waiting_for_files)
async def cmd_done_upload(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        files = data.get('files', [])
        if not files:
            await message.answer("No files uploaded; cancelled.")
            await state.finish()
            return
        await UploadStates.waiting_for_options.set()
        await state.update_data(protect_content=True, auto_delete_minutes=0)
        await message.answer(f"📁 {len(files)} files received. Select options:", reply_markup=create_session_options_keyboard())
    except Exception as e:
        logger.exception("cmd_done_upload failed: %s", e)
        await state.finish()
        await message.answer("Error finishing upload.")

@dp.message_handler(content_types=[ContentType.DOCUMENT, ContentType.PHOTO, ContentType.VIDEO], state=UploadStates.waiting_for_files)
async def handler_upload_files(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    # If upload channel is not ready, inform owner clearly and abort storing
    if not UPLOAD_CHANNEL_READY:
        await message.answer("❌ Failed to save file to upload channel. Bot is not admin or cannot post to the upload channel. Please add the bot as an admin with post permissions. Upload aborted.")
        logger.warning("Attempt to save file while UPLOAD_CHANNEL_READY=False")
        return

    # extract the file_id from the received message (client-side)
    try:
        channel_file_id = None
        channel_msg_obj = None

        # Try copy_message first (preserves original file and does not add forward tag)
        try:
            copied = await bot.copy_message(Config.UPLOAD_CHANNEL_ID, message.chat.id, message.message_id)
            channel_msg_obj = copied
            logger.debug("copy_message succeeded to upload channel")
        except Exception as e_copy:
            logger.warning("copy_message failed, attempting forward_message: %s", e_copy)
            # Try forward_message
            try:
                forwarded = await bot.forward_message(Config.UPLOAD_CHANNEL_ID, message.chat.id, message.message_id)
                channel_msg_obj = forwarded
                logger.debug("forward_message succeeded to upload channel")
            except Exception as e_forward:
                logger.warning("forward_message failed, attempting send_* fallback: %s", e_forward)
                # As a last resort, try sending the file to the channel using send_document/send_photo/send_video with the original file_id
                # Determine original file_id
                orig_file_id = None
                if message.photo:
                    orig_file_id = message.photo[-1].file_id
                elif message.video:
                    orig_file_id = message.video.file_id
                elif message.document:
                    orig_file_id = message.document.file_id

                if not orig_file_id:
                    await message.answer("Could not determine file id to save.")
                    logger.error("No original file id available for fallback send.")
                    return

                # Try send appropriate method
                try:
                    if message.photo:
                        channel_msg_obj = await bot.send_photo(Config.UPLOAD_CHANNEL_ID, orig_file_id, caption=message.caption or "")
                    elif message.video:
                        channel_msg_obj = await bot.send_video(Config.UPLOAD_CHANNEL_ID, orig_file_id, caption=message.caption or "")
                    else:
                        channel_msg_obj = await bot.send_document(Config.UPLOAD_CHANNEL_ID, orig_file_id, caption=message.caption or "")
                    logger.debug("Fallback send_* to upload channel succeeded")
                except Exception as e_send:
                    logger.exception("Failed to save file to upload channel via any method: %s", e_send)
                    await message.answer("❌ Failed to save file to upload channel. Ensure bot is admin and can post to the channel.")
                    return

        # Extract file id from channel_msg_obj
        if channel_msg_obj:
            if getattr(channel_msg_obj, 'photo', None):
                channel_file_id = channel_msg_obj.photo[-1].file_id
            elif getattr(channel_msg_obj, 'video', None):
                channel_file_id = channel_msg_obj.video.file_id
            elif getattr(channel_msg_obj, 'document', None):
                channel_file_id = channel_msg_obj.document.file_id

        if not channel_file_id:
            logger.error("Copied/forwarded message did not contain file_id")
            await message.answer("❌ Failed to detect file id in upload channel message.")
            return

        # Store file_id and caption in FSM
        data = await state.get_data()
        files = data.get('files', [])
        caps = data.get('captions', [])
        files.append(channel_file_id)
        caps.append(message.caption or "")
        await state.update_data(files=files, captions=caps)
        await message.answer(f"✅ Stored file #{len(files)} in upload channel.")
    except Exception as e:
        logger.exception("handler_upload_files unexpected error: %s", e)
        await message.answer("Error while storing file.")

# Callback handlers for options
@dp.callback_query_handler(lambda c: c.data and (c.data.startswith('protect_') or c.data.startswith('delete_')), state=UploadStates.waiting_for_options)
async def callback_upload_options(c: CallbackQuery, state: FSMContext):
    await c.answer()
    try:
        data = await state.get_data()
        if c.data.startswith('protect_'):
            protect = c.data == 'protect_yes'
            await state.update_data(protect_content=protect)
            await c.message.answer(f"Protect set to {'YES' if protect else 'NO'}")
            return
        if c.data.startswith('delete_'):
            parts = c.data.split('_')
            minutes = int(parts[1]) if len(parts) > 1 else 0
            await state.update_data(auto_delete_minutes=minutes)
            await c.message.answer(f"Auto-delete set to {minutes} minutes")
            return
    except Exception as e:
        logger.exception("callback_upload_options failed: %s", e)
        try:
            await c.answer("Error")
        except Exception:
            pass

@dp.callback_query_handler(lambda c: c.data == 'create_session', state=UploadStates.waiting_for_options)
async def callback_create_session(c: CallbackQuery, state: FSMContext):
    await c.answer()
    try:
        data = await state.get_data()
        files = data.get('files', [])
        captions = data.get('captions', [])
        protect = data.get('protect_content', True)
        auto_delete = data.get('auto_delete_minutes', 0)
        owner_id = c.from_user.id
        if not files:
            await c.message.answer("No files to create session.")
            await state.finish()
            return
        session_id = await Database.create_upload_session(owner_id, files, captions, protect, auto_delete)
        if not session_id:
            await c.message.answer("Failed to create session. Check logs.")
            await state.finish()
            return
        # Build deep link using BOT_USERNAME captured at startup
        if BOT_USERNAME:
            deep_link = f"https://t.me/{BOT_USERNAME}?start={session_id}"
        else:
            # fallback but unlikely because BOT_USERNAME is set at startup
            deep_link = f"https://t.me/{session_id}"
        await c.message.answer(f"✅ Session created!\nFiles: {len(files)}\nProtect: {'Yes' if protect else 'No'}\nAuto-delete: {auto_delete} min\n\nShare link:\n{deep_link}")
        await state.finish()
    except Exception as e:
        logger.exception("callback_create_session failed: %s", e)
        await c.message.answer("Error creating session.")
        try:
            await state.finish()
        except Exception:
            pass

# ------------------------------
# deep link access
# ------------------------------
async def schedule_deletion(chat_id: int, message_id: int, delay_seconds: int):
    if delay_seconds <= 0:
        return
    async def _del():
        await asyncio.sleep(delay_seconds)
        try:
            await bot.delete_message(chat_id, message_id)
        except Exception as e:
            logger.debug("delete failed %s:%s -> %s", chat_id, message_id, e)
    asyncio.create_task(_del())

async def handle_deep_link_access(user_id: int, session_id: str):
    try:
        session = await Database.get_upload_session(session_id)
        if not session:
            await safe_send_message(user_id, "❌ Session not found or expired.")
            return
        file_ids = session.get('file_ids') or []
        if isinstance(file_ids, str):
            try:
                file_ids = json.loads(file_ids)
            except Exception:
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
        for idx, fid in enumerate(file_ids):
            caption = captions[idx] if idx < len(captions) else ""
            sent_msg = None
            try:
                try:
                    sent_msg = await bot.send_photo(user_id, fid, caption=caption, protect_content=protect)
                except Exception:
                    try:
                        sent_msg = await bot.send_video(user_id, fid, caption=caption, protect_content=protect)
                    except Exception:
                        sent_msg = await bot.send_document(user_id, fid, caption=caption, protect_content=protect)
                if sent_msg and auto_delete > 0 and not is_owner_fetch:
                    await schedule_deletion(sent_msg.chat.id, sent_msg.message_id, auto_delete * 60)
            except Exception as e:
                logger.exception("Failed to send file %s to %s: %s", fid, user_id, e)
        if auto_delete > 0 and not is_owner_fetch:
            # human-readable
            if auto_delete >= 1440:
                time_label = f"{auto_delete//1440} day(s)"
            elif auto_delete >= 60:
                time_label = f"{auto_delete//60} hour(s)"
            else:
                time_label = f"{auto_delete} minute(s)"
            notice = await bot.send_message(user_id, f"⚠️ Files will auto-delete in {time_label}.")
            await schedule_deletion(notice.chat.id, notice.message_id, auto_delete * 60)
    except Exception as e:
        logger.exception("handle_deep_link_access error: %s", e)
        await safe_send_message(user_id, "Error while accessing files.")

# ------------------------------
# /broadcast (owner) - copy to all users
# ------------------------------
@dp.message_handler(commands=['broadcast'])
async def cmd_broadcast(message: Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    if not message.reply_to_message:
        await message.answer("Reply to a message to broadcast it, then send /broadcast.")
        return
    try:
        state = dp.current_state(chat=message.chat.id, user=message.from_user.id)
        await state.set_state(BroadcastStates.waiting_for_broadcast_confirmation.state)
        await state.update_data(broadcast_original={'chat_id': message.reply_to_message.chat.id, 'message_id': message.reply_to_message.message_id})
        await message.answer("Type YES to confirm broadcast to all users, or /cancel to abort.")
    except Exception as e:
        logger.exception("/broadcast start error: %s", e)
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
        from_chat = orig.get('chat_id'); msg_id = orig.get('message_id')
        if not from_chat or not msg_id:
            await message.answer("Original message missing.")
            await state.finish()
            return
        users = await Database.get_all_active_users()
        total = len(users)
        status = await message.answer(f"Broadcasting to {total} users...")
        success = failed = 0; start = time.time()
        for idx, uid in enumerate(users, start=1):
            try:
                copied = await safe_copy_message(uid, from_chat, msg_id)
                if copied:
                    success += 1
                else:
                    # fallback small notice
                    await safe_send_message(uid, "Message from bot owner.")
                    failed += 1
            except Exception as e:
                failed += 1
                logger.debug("broadcast to %s failed: %s", uid, e)
            if idx % 20 == 0:
                await asyncio.sleep(1)
            if idx % 50 == 0 or idx == total:
                try:
                    await status.edit_text(f"Broadcast progress: {idx}/{total} (success {success}, failed {failed})")
                except Exception:
                    pass
        await status.edit_text(f"Broadcast complete. Success {success}, Failed {failed}")
        await state.finish()
    except Exception as e:
        logger.exception("handler_broadcast_confirm failed: %s", e)
        await message.answer("Broadcast failed.")
        await state.finish()

# ------------------------------
# /stats (owner)
# ------------------------------
@dp.message_handler(commands=['stats'])
async def cmd_stats(message: Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    try:
        s = await Database.get_stats()
        text = (f"📊 <b>Bot Statistics</b>\n\n"
                f"👥 Total users: {s['total_users']}\n"
                f"🟢 Active (48h): {s['active_users']}\n"
                f"📁 Files uploaded: {s['files_uploaded']}\n"
                f"🔗 Sessions completed: {s['sessions_completed']}\n")
        await message.answer(text)
    except Exception as e:
        logger.exception("/stats failed: %s", e)
        await message.answer("Error fetching stats.")

# ------------------------------
# /restore_db (owner) - non-destructive ensure schema
# ------------------------------
@dp.message_handler(commands=['restore_db'])
async def cmd_restore_db(message: Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    try:
        ok = await Database.init_db()
        if ok:
            await message.answer("✅ Database schema ensured (non-destructive).")
        else:
            await message.answer("❌ Database init failed. Check logs.")
    except Exception as e:
        logger.exception("/restore_db failed: %s", e)
        await message.answer("Error running restore_db. Check logs.")

# ------------------------------
# Global error handler
# ------------------------------
@dp.errors_handler()
async def global_error_handler(update, exception):
    logger.exception("Dispatcher error: update=%s exception=%s", update, exception)
    return True

# ------------------------------
# Web app builder and run
# ------------------------------
def build_app():
    app = web.Application()
    app.router.add_get('/', health)
    app.router.add_get('/health', health)
    app.router.add_post('/webhook/{token}', webhook_handler)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app

if __name__ == '__main__':
    app = build_app()
    web.run_app(app, host='0.0.0.0', port=Config.PORT)