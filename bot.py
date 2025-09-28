#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Webhook-only Telegram File Sharing Bot (aiogram v2) with Neon (Postgres)
Single-file deploy (webhook mode). Uses UPLOAD_CHANNEL_ID to store originals.
Author: Combined & fixed version
"""

import os
import sys
import asyncio
import logging
import json
import uuid
import secrets
import string
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any

import asyncpg
from aiohttp import web

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils.exceptions import RetryAfter, BotBlocked, ChatNotFound, TelegramAPIError, MessageNotModified

# -------------------------
# Configuration (env vars)
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").strip()
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0") or 0)
PORT = int(os.getenv("PORT", "5000") or 5000)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# minimal checks
if not BOT_TOKEN:
    print("FATAL: BOT_TOKEN env required", file=sys.stderr); sys.exit(1)
if not OWNER_ID:
    print("FATAL: OWNER_ID env required (numeric)", file=sys.stderr); sys.exit(1)
if not DATABASE_URL:
    print("FATAL: DATABASE_URL env required", file=sys.stderr); sys.exit(1)
if not RENDER_EXTERNAL_URL:
    print("FATAL: RENDER_EXTERNAL_URL env required", file=sys.stderr); sys.exit(1)
if not UPLOAD_CHANNEL_ID:
    print("FATAL: UPLOAD_CHANNEL_ID env required (channel id, negative integer)", file=sys.stderr); sys.exit(1)

# -------------------------
# Logging
# -------------------------
numeric_level = getattr(logging, LOG_LEVEL, logging.INFO)
logging.basicConfig(
    level=numeric_level,
    format='%(asctime)s | %(levelname)-7s | %(name)s:%(lineno)d | %(message)s'
)
logger = logging.getLogger("file_share_bot")

# -------------------------
# Bot & Dispatcher
# -------------------------
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

# Globals
db_pool: Optional[asyncpg.pool.Pool] = None
BOT_USERNAME: Optional[str] = None
BOT_ID: Optional[int] = None
UPLOAD_CHANNEL_READY: bool = False

# -------------------------
# FSM States
# -------------------------
class UploadStates(StatesGroup):
    waiting_for_files = State()
    waiting_for_options = State()

class MessageStates(StatesGroup):
    waiting_for_message_type = State()
    waiting_for_text = State()

class BroadcastStates(StatesGroup):
    waiting_for_broadcast_confirmation = State()

# -------------------------
# Utilities
# -------------------------
def gen_session_id(length: int = 8) -> str:
    return uuid.uuid4().hex[:length]

def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

async def pg_connect_pool(dsn: str, min_size: int = 1, max_size: int = 10, retries: int = 5, delay: int = 2):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            pool = await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size)
            logger.info("Connected to database (asyncpg pool ready).")
            return pool
        except Exception as e:
            last_exc = e
            logger.warning("DB connect attempt %d/%d failed: %s", attempt, retries, e)
            if attempt < retries:
                await asyncio.sleep(delay)
    logger.error("All DB connection attempts failed.")
    raise last_exc

# safe send message wrapper with retries on rate limit
async def safe_send_message(chat_id: int, text: str, **kwargs) -> bool:
    try:
        await bot.send_message(chat_id, text, **kwargs)
        return True
    except RetryAfter as e:
        logger.warning("Rate limited, sleeping %s seconds", e.timeout)
        await asyncio.sleep(e.timeout)
        return await safe_send_message(chat_id, text, **kwargs)
    except (BotBlocked, ChatNotFound) as e:
        logger.warning("Cannot send message to %s: %s", chat_id, e)
        return False
    except Exception as e:
        logger.exception("Unexpected error sending message to %s: %s", chat_id, e)
        return False

# safe copy message wrapper
async def safe_copy_message(chat_id: int, from_chat_id: int, message_id: int) -> Optional[types.Message]:
    try:
        msg = await bot.copy_message(chat_id, from_chat_id, message_id)
        return msg
    except RetryAfter as e:
        await asyncio.sleep(e.timeout)
        return await safe_copy_message(chat_id, from_chat_id, message_id)
    except Exception as e:
        logger.debug("copy_message failed (%s->%s message %s): %s", from_chat_id, chat_id, message_id, e)
        return None

# keyboard factories
def help_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Help", callback_data="help_cmd"))
    return kb

def session_options_kb() -> InlineKeyboardMarkup:
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
    kb.add(
        InlineKeyboardButton("1w", callback_data="delete_10080"),
        InlineKeyboardButton("Create Session", callback_data="create_session")
    )
    return kb

def choose_msg_type_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Set Start Message", callback_data="msg_start"),
        InlineKeyboardButton("Set Help Message", callback_data="msg_help")
    )
    return kb

# schedule deletion helper (fire-and-forget)
async def schedule_deletion(chat_id: int, message_id: int, delay_seconds: int):
    if delay_seconds <= 0:
        return
    async def _job():
        await asyncio.sleep(delay_seconds)
        try:
            await bot.delete_message(chat_id, message_id)
        except Exception as e:
            logger.debug("Failed to delete message %s in chat %s: %s", message_id, chat_id, e)
    asyncio.create_task(_job())

# -------------------------
# Database access layer
# -------------------------
class Database:
    @staticmethod
    async def init_schema():
        global db_pool
        if db_pool is None:
            raise RuntimeError("db_pool not initialized")
        async with db_pool.acquire() as conn:
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
            # Ensure a statistics row exists
            row = await conn.fetchrow('SELECT id FROM statistics LIMIT 1')
            if not row:
                await conn.execute('INSERT INTO statistics (total_users, active_users, files_uploaded, sessions_completed) VALUES (0,0,0,0)')
            # Ensure default messages exist
            for mtype, default_text in [('start', '👋 Welcome to File Sharing Bot! Use /help.'), ('help', '📖 Help: Owners can use /upload to share files.')]:
                r = await conn.fetchrow('SELECT id FROM messages WHERE message_type = $1', mtype)
                if not r:
                    await conn.execute('INSERT INTO messages (message_type, text) VALUES ($1, $2)', mtype, default_text)

    @staticmethod
    async def create_or_update_user(user: types.User):
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO users (user_id, username, first_name, last_name)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id) DO UPDATE SET last_active = NOW(), username = EXCLUDED.username, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name
            ''', user.id, user.username, user.first_name, user.last_name)

    @staticmethod
    async def update_user_activity(user_id: int):
        async with db_pool.acquire() as conn:
            await conn.execute('UPDATE users SET last_active = NOW() WHERE user_id = $1', user_id)

    @staticmethod
    async def get_message_item(message_type: str):
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
    async def create_upload_session(owner_id: int, file_ids: List[str], captions: List[str], protect: bool, auto_delete: int) -> Optional[str]:
        sid = gen = gen_session_id()
        try:
            async with db_pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO upload_sessions (session_id, owner_id, file_ids, captions, protect_content, auto_delete_minutes)
                    VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6)
                ''', sid, owner_id, json.dumps(file_ids), json.dumps(captions), protect, auto_delete)
                await conn.execute('UPDATE statistics SET files_uploaded = files_uploaded + $1, sessions_completed = sessions_completed + 1, last_updated = NOW()', len(file_ids))
            return sid
        except Exception as e:
            logger.exception("create_upload_session error: %s", e)
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
    async def get_all_users() -> List[int]:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch('SELECT user_id FROM users WHERE is_banned = FALSE')
            return [r['user_id'] for r in rows]

    @staticmethod
    async def get_stats() -> Dict[str, int]:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval('SELECT COUNT(*) FROM users') or 0
            active = await conn.fetchval("SELECT COUNT(*) FROM users WHERE last_active > NOW() - INTERVAL '48 hours'") or 0
            stats = await conn.fetchrow('SELECT files_uploaded, sessions_completed FROM statistics ORDER BY id DESC LIMIT 1')
            return {
                'total_users': total,
                'active_users': active,
                'files_uploaded': stats['files_uploaded'] if stats else 0,
                'sessions_completed': stats['sessions_completed'] if stats else 0
            }

# -------------------------
# Startup / Shutdown hooks
# -------------------------
async def on_startup(app: web.Application):
    global db_pool, BOT_USERNAME, BOT_ID, UPLOAD_CHANNEL_READY
    logger.info("Starting up: connecting to DB")
    db_pool = await pg_connect_pool(DATABASE_URL, min_size=1, max_size=10)
    # Ensure schema
    try:
        await Database.init_schema()
    except Exception as e:
        logger.exception("DB schema init failed: %s", e)
        raise

    # capture bot identity
    try:
        me = await bot.get_me()
        BOT_USERNAME = me.username
        BOT_ID = me.id
        logger.info("Bot identity: @%s (%s)", BOT_USERNAME, BOT_ID)
    except Exception as e:
        logger.exception("Failed to fetch bot identity: %s", e)
        raise

    # Check upload channel membership & permissions
    UPLOAD_CHANNEL_READY = False
    try:
        cm = await bot.get_chat_member(UPLOAD_CHANNEL_ID, BOT_ID)
        logger.info("Upload channel membership status: %s", cm.status)
        if cm.status in ('administrator', 'creator'):
            UPLOAD_CHANNEL_READY = True
            logger.info("Upload channel is ready (bot is admin).")
        else:
            UPLOAD_CHANNEL_READY = False
            logger.warning("Bot is not admin in upload channel; may not be able to post.")
    except Exception as e:
        UPLOAD_CHANNEL_READY = False
        logger.exception("Cannot access upload channel (ensure correct ID and bot is member/admin): %s", e)

    # set webhook
    webhook_url = f"{RENDER_EXTERNAL_URL.rstrip('/')}/webhook/{BOT_TOKEN}"
    try:
        await bot.set_webhook(webhook_url)
        logger.info("Webhook set to %s", webhook_url)
    except Exception as e:
        logger.exception("Failed to set webhook: %s", e)
        raise

    # Notify owner if upload channel not ready
    if not UPLOAD_CHANNEL_READY:
        try:
            await safe_send_message(OWNER_ID, f"⚠️ Bot cannot post to UPLOAD_CHANNEL_ID ({UPLOAD_CHANNEL_ID}). Add bot as admin to the channel with post permissions.")
        except Exception:
            logger.debug("Failed to notify owner about upload channel readiness.")

async def on_shutdown(app: web.Application):
    logger.info("Shutting down: deleting webhook & closing resources")
    try:
        await bot.delete_webhook()
    except Exception as e:
        logger.debug("delete_webhook: %s", e)
    if db_pool:
        await db_pool.close()
        logger.info("DB pool closed")
    try:
        await bot.session.close()
    except Exception:
        pass

# -------------------------
# Web server handlers
# -------------------------
async def health(request):
    return web.Response(text="ok")

# Custom webhook handler to log payloads, then pass to dispatcher
async def webhook_handler(request):
    try:
        body = await request.text()
        logger.debug("Incoming webhook payload: %s", body)
        try:
            data = await request.json()
        except Exception:
            # fallback: try to parse text
            data = json.loads(body) if body else {}
        # Process update via Dispatcher
        update = types.Update.to_object(data)
        await dp.process_update(types.Update(**data))
        return web.Response(status=200, text="OK")
    except Exception as e:
        logger.exception("Webhook handler error: %s", e)
        return web.Response(status=500, text="error")

# -------------------------
# Handlers: start/help/setmessage/setimage
# -------------------------
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    try:
        await Database.create_or_update_user(message.from_user)
        await Database.update_user_activity(message.from_user.id)

        args = message.get_args().strip() if hasattr(message, 'get_args') else ''
        if args:
            # deep link
            session_id = args.split()[0].strip()
            await handle_deep_link_access(message.from_user.id, session_id)
            return

        msg = await Database.get_message_item('start')
        text = msg['text'] if msg and msg.get('text') else "Welcome!"
        img = msg.get('image_file_id') if msg else None

        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("Help", callback_data="help_cmd"))

        if img:
            try:
                await message.answer_photo(img, caption=text, reply_markup=kb)
                return
            except Exception as e:
                logger.warning("Failed to send start image: %s", e)

        await message.answer(text, reply_markup=kb)
    except Exception as e:
        logger.exception("/start failed: %s", e)
        await message.answer("An error occurred during /start.")

@dp.callback_query_handler(lambda c: c.data == 'help_cmd')
async def cb_help_button(c: types.CallbackQuery):
    try:
        await c.answer()
        # reuse help handler logic
        msg = await Database.get_message_item('help')
        text = msg['text'] if msg and msg.get('text') else "Help information"
        img = msg.get('image_file_id') if msg else None
        if img:
            try:
                await c.message.answer_photo(img, caption=text)
                return
            except Exception:
                pass
        await c.message.answer(text)
    except Exception as e:
        logger.exception("help callback failed: %s", e)
        try:
            await c.answer("Error")
        except Exception:
            pass

@dp.message_handler(commands=['help'])
async def cmd_help(message: types.Message):
    try:
        await Database.update_user_activity(message.from_user.id)
        msg = await Database.get_message_item('help')
        text = msg['text'] if msg and msg.get('text') else "Help info"
        img = msg.get('image_file_id') if msg else None
        if img:
            try:
                await message.answer_photo(img, caption=text)
                return
            except Exception:
                pass
        await message.answer(text)
    except Exception as e:
        logger.exception("/help failed: %s", e)
        await message.answer("An error occurred while fetching help.")

@dp.message_handler(commands=['setmessage'])
async def cmd_setmessage(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    try:
        state = dp.current_state(chat=message.chat.id, user=message.from_user.id)
        await state.set_state(MessageStates.waiting_for_message_type.state)
        await state.update_data(setting_image=False)
        await message.answer("Select which message to update:", reply_markup=choose_msg_type_kb())
    except Exception as e:
        logger.exception("/setmessage error: %s", e)
        await message.answer("Error starting setmessage.")

@dp.message_handler(commands=['setimage'])
async def cmd_setimage(message: types.Message):
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
        await message.answer("Choose which message to set this image for:", reply_markup=choose_msg_type_kb())
    except Exception as e:
        logger.exception("/setimage error: %s", e)
        await message.answer("Error starting setimage.")

@dp.callback_query_handler(lambda c: c.data in ['msg_start', 'msg_help'], state=MessageStates.waiting_for_message_type)
async def cb_msg_type(c: types.CallbackQuery, state: FSMContext):
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
        await state.update_data(msg_type=msg_type)
        await state.set_state(MessageStates.waiting_for_text.state)
        await c.message.answer(f"Send the new text for <b>{msg_type}</b> now.")
    except Exception as e:
        logger.exception("cb_msg_type error: %s", e)
        await state.finish()
        try:
            await c.answer("Error")
        except Exception:
            pass

@dp.message_handler(state=MessageStates.waiting_for_text)
async def handler_message_text(message: types.Message, state: FSMContext):
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

# -------------------------
# Upload flow (owner-only)
# -------------------------
# We'll use FSM and keep files/captions in state; we store channel file_ids (copied message ids or file_ids)
@dp.message_handler(commands=['upload'])
async def cmd_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    try:
        state = dp.current_state(chat=message.chat.id, user=message.from_user.id)
        await state.set_state(UploadStates.waiting_for_files.state)
        await state.update_data(files=[], captions=[])
        await message.answer("📤 Upload started. Send files (photos/videos/documents). When done send /d, or /c to cancel.")
    except Exception as e:
        logger.exception("/upload failed: %s", e)
        await message.answer("Error starting upload.")

@dp.message_handler(commands=['c'], state='*')
async def cmd_cancel_any(message: types.Message, state: FSMContext):
    # owner-only cancel for FSM flows
    if not is_owner(message.from_user.id):
        return
    try:
        current = await state.get_state()
        if current:
            await state.finish()
            await message.answer("Operation cancelled.")
        else:
            await message.answer("No active operation.")
    except Exception as e:
        logger.exception("/c cancel failed: %s", e)
        await message.answer("Error cancelling.")

@dp.message_handler(content_types=[types.ContentType.PHOTO, types.ContentType.DOCUMENT, types.ContentType.VIDEO], state=UploadStates.waiting_for_files)
async def handler_upload_files(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    # Check channel readiness
    if not UPLOAD_CHANNEL_READY:
        await message.answer("❌ Failed to save file to upload channel. Bot is not admin or cannot post. Please add bot as admin to the upload channel.")
        logger.warning("Upload attempt while UPLOAD_CHANNEL_READY=False")
        return
    try:
        # Try copy_message (best: preserves media without forwarding)
        channel_msg = None
        try:
            channel_msg = await bot.copy_message(UPLOAD_CHANNEL_ID, message.chat.id, message.message_id)
            logger.debug("copy_message to upload channel succeeded")
        except Exception as e_copy:
            logger.warning("copy_message failed: %s", e_copy)
            # Try forward_message
            try:
                channel_msg = await bot.forward_message(UPLOAD_CHANNEL_ID, message.chat.id, message.message_id)
                logger.debug("forward_message to upload channel succeeded")
            except Exception as e_fwd:
                logger.warning("forward_message failed: %s", e_fwd)
                # Try send_* fallback using original file_id(s)
                orig_file_id = None
                if message.photo:
                    orig_file_id = message.photo[-1].file_id
                elif message.video:
                    orig_file_id = message.video.file_id
                elif message.document:
                    orig_file_id = message.document.file_id
                if not orig_file_id:
                    await message.answer("Could not determine original file id to send to channel.")
                    return
                try:
                    if message.photo:
                        channel_msg = await bot.send_photo(UPLOAD_CHANNEL_ID, orig_file_id, caption=message.caption or "")
                    elif message.video:
                        channel_msg = await bot.send_video(UPLOAD_CHANNEL_ID, orig_file_id, caption=message.caption or "")
                    else:
                        channel_msg = await bot.send_document(UPLOAD_CHANNEL_ID, orig_file_id, caption=message.caption or "")
                    logger.debug("Fallback send_* to upload channel succeeded")
                except Exception as e_send:
                    logger.exception("Fallback send to upload channel failed: %s", e_send)
                    await message.answer("❌ Failed to save file to upload channel. Ensure bot is admin and can post.")
                    return

        # extract the channel file_id to store in DB session
        channel_file_id = None
        if getattr(channel_msg, 'photo', None):
            channel_file_id = channel_msg.photo[-1].file_id
        elif getattr(channel_msg, 'video', None):
            channel_file_id = channel_msg.video.file_id
        elif getattr(channel_msg, 'document', None):
            channel_file_id = channel_msg.document.file_id
        else:
            # Sometimes copy_message returns without media fields; try fetching message from channel? For now fallback to message_id (we'll copy by message_id when delivering)
            # We'll store the channel message_id to copy later.
            # When using copy by message_id, we will call copy_message(chat_id, from_chat_id=UPLOAD_CHANNEL_ID, message_id=channel_msg.message_id)
            # Save a special dict entry to indicate message_id-based copy
            pass

        # Save into FSM state: store either file_id string or a dict with message_id marker
        data = await state.get_data()
        files = data.get('files', [])
        captions = data.get('captions', [])
        if channel_file_id:
            files.append({'type': 'file_id', 'value': channel_file_id})
        else:
            # store by message_id if file_id not available
            files.append({'type': 'channel_message', 'value': getattr(channel_msg, 'message_id', None)})
        captions.append(message.caption or "")
        await state.update_data(files=files, captions=captions)
        await message.answer(f"✅ Stored file #{len(files)} in upload channel.")
    except Exception as e:
        logger.exception("handler_upload_files unexpected: %s", e)
        await message.answer("Error while storing file.")

@dp.message_handler(commands=['d'], state=UploadStates.waiting_for_files)
async def cmd_done_upload(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    try:
        data = await state.get_data()
        files = data.get('files', [])
        if not files:
            await message.answer("No files uploaded. Use /upload to start again.")
            await state.finish()
            return
        # Go to options
        await UploadStates.waiting_for_options.set()
        # default options
        await state.update_data(protect_content=True, auto_delete_minutes=0)
        await message.answer(f"📁 Files received: {len(files)}\nChoose options:", reply_markup=session_options_kb())
    except Exception as e:
        logger.exception("cmd_done_upload failed: %s", e)
        await state.finish()
        await message.answer("Error finishing upload.")

@dp.callback_query_handler(lambda c: c.data and (c.data.startswith('protect_') or c.data.startswith('delete_')), state=UploadStates.waiting_for_options)
async def cb_upload_options(c: types.CallbackQuery, state: FSMContext):
    await c.answer()
    try:
        if c.data.startswith('protect_'):
            protect = c.data == 'protect_yes'
            await state.update_data(protect_content=protect)
            await c.message.answer(f"Protect set to {'YES' if protect else 'NO'}")
            return
        if c.data.startswith('delete_'):
            minutes = int(c.data.split('_')[1])
            await state.update_data(auto_delete_minutes=minutes)
            await c.message.answer(f"Auto-delete set to {minutes} minutes")
            return
    except Exception as e:
        logger.exception("cb_upload_options failed: %s", e)
        try:
            await c.answer("Error")
        except Exception:
            pass

@dp.callback_query_handler(lambda c: c.data == 'create_session', state=UploadStates.waiting_for_options)
async def cb_create_session(c: types.CallbackQuery, state: FSMContext):
    await c.answer()
    try:
        data = await state.get_data()
        files_meta = data.get('files', [])
        captions = data.get('captions', [])
        protect = data.get('protect_content', True)
        auto_delete = data.get('auto_delete_minutes', 0)
        owner_id = c.from_user.id

        # normalize files for DB: for each entry either store file_id string or store channel_message:<message_id>
        db_file_refs = []
        for f in files_meta:
            if isinstance(f, dict):
                if f.get('type') == 'file_id':
                    db_file_refs.append({'kind': 'file_id', 'value': f.get('value')})
                else:
                    # channel_message
                    db_file_refs.append({'kind': 'channel_message', 'value': f.get('value')})
            else:
                db_file_refs.append({'kind': 'file_id', 'value': f})

        session_id = gen_session_id()
        # store in DB
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO upload_sessions (session_id, owner_id, file_ids, captions, protect_content, auto_delete_minutes)
                VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6)
            ''', session_id, owner_id, json.dumps(db_file_refs), json.dumps(captions), protect, auto_delete)
            await conn.execute('UPDATE statistics SET files_uploaded = files_uploaded + $1, sessions_completed = sessions_completed + 1, last_updated = NOW()', len(db_file_refs))

        # Build deep link using captured BOT_USERNAME
        if BOT_USERNAME:
            deep_link = f"https://t.me/{BOT_USERNAME}?start={session_id}"
        else:
            deep_link = f"https://t.me/{session_id}"

        await c.message.answer(f"✅ Session created!\nFiles: {len(db_file_refs)}\nProtect: {'Yes' if protect else 'No'}\nAuto-delete: {auto_delete} min\n\nShare link:\n{deep_link}")
        await state.finish()
    except Exception as e:
        logger.exception("cb_create_session failed: %s", e)
        try:
            await c.message.answer("Failed to create session. Check logs.")
        except Exception:
            pass
        await state.finish()

# -------------------------
# Deep link access handler
# -------------------------
async def try_send_file_by_ref(user_id: int, ref: dict, caption: str, protect: bool, auto_delete_min: int, is_owner_fetch: bool):
    """
    ref is dict: {'kind': 'file_id'|'channel_message', 'value': ...}
    If kind=file_id -> send_photo/send_video/send_document by file_id heuristics (try in order)
    If kind=channel_message -> use bot.copy_message to copy the message from UPLOAD_CHANNEL_ID by message_id
    """
    sent_msg = None
    try:
        if ref.get('kind') == 'channel_message':
            channel_msg_id = ref.get('value')
            if channel_msg_id:
                try:
                    sent = await bot.copy_message(user_id, UPLOAD_CHANNEL_ID, channel_msg_id)
                    sent_msg = sent
                except Exception as e:
                    logger.debug("copy_message from channel failed: %s", e)
                    # Try to fetch the file_id from the channel message? (we can't fetch arbitrary message easily without Telegram bot API getChat)
                    # Fallback: attempt to send by treating value as file_id
                    try:
                        sent = await bot.send_document(user_id, ref.get('value'), caption=caption, protect_content=protect if not is_owner_fetch else False)
                        sent_msg = sent
                    except Exception as e2:
                        logger.debug("fallback send by value failed: %s", e2)
            else:
                logger.debug("channel_message ref missing message id")
        else:
            file_id = ref.get('value')
            if not file_id:
                return None
            # try as photo
            try:
                sent_msg = await bot.send_photo(user_id, file_id, caption=caption, protect_content=protect if not is_owner_fetch else False)
            except Exception:
                try:
                    sent_msg = await bot.send_video(user_id, file_id, caption=caption, protect_content=protect if not is_owner_fetch else False)
                except Exception:
                    sent_msg = await bot.send_document(user_id, file_id, caption=caption, protect_content=protect if not is_owner_fetch else False)
    except Exception as e:
        logger.exception("try_send_file_by_ref failed: %s", e)
    # schedule deletion if required
    if sent_msg and auto_delete_min > 0 and not is_owner_fetch:
        try:
            await schedule_deletion(sent_msg.chat.id, sent_msg.message_id, auto_delete_min * 60)
        except Exception:
            pass
    return sent_msg

async def handle_deep_link_access(user_id: int, session_id: str):
    try:
        session = await Database.get_upload_session(session_id)
        if not session:
            await safe_send_message(user_id, "❌ Session not found or expired.")
            return
        # parse file refs
        file_refs = session.get('file_ids') or []
        if isinstance(file_refs, str):
            file_refs = json.loads(file_refs)
        captions = session.get('captions') or []
        if isinstance(captions, str):
            captions = json.loads(captions)
        protect = bool(session.get('protect_content', True))
        auto_delete = int(session.get('auto_delete_minutes', 0))
        owner_id = int(session.get('owner_id', 0))
        is_owner_fetch = (user_id == OWNER_ID) or (user_id == owner_id)
        # Send each file
        for idx, ref in enumerate(file_refs):
            caption = captions[idx] if idx < len(captions) else ""
            try:
                await try_send_file_by_ref(user_id, ref, caption, protect, auto_delete, is_owner_fetch)
            except Exception as e:
                logger.exception("Error sending file ref %s: %s", ref, e)
                continue
        # auto-delete notice
        if auto_delete > 0 and not is_owner_fetch:
            if auto_delete >= 1440:
                label = f"{auto_delete//1440} day(s)"
            elif auto_delete >= 60:
                label = f"{auto_delete//60} hour(s)"
            else:
                label = f"{auto_delete} minute(s)"
            notice = await bot.send_message(user_id, f"⚠️ Files will auto-delete in {label}.")
            await schedule_deletion(notice.chat.id, notice.message_id, auto_delete * 60)
    except Exception as e:
        logger.exception("handle_deep_link_access failed: %s", e)
        await safe_send_message(user_id, "An error occurred while accessing the session.")

# -------------------------
# Broadcast & Stats
# -------------------------
@dp.message_handler(commands=['broadcast'])
async def cmd_broadcast(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    if not message.reply_to_message:
        await message.answer("Reply to a message with /broadcast to start.")
        return
    try:
        users = await Database.get_all_users()
        total = len(users)
        status_msg = await message.answer(f"Broadcasting to {total} users...")
        success = failed = 0
        start = time.time()
        for idx, uid in enumerate(users, start=1):
            try:
                ok = await safe_copy_message(uid, message.reply_to_message.chat.id, message.reply_to_message.message_id)
                if ok:
                    success += 1
                else:
                    # fallback small notice
                    await safe_send_message(uid, "Message from bot owner.")
                    failed += 1
            except Exception as e:
                failed += 1
                logger.debug("Broadcast to %s failed: %s", uid, e)
            if idx % 20 == 0:
                await asyncio.sleep(1)
            if idx % 50 == 0 or idx == total:
                try:
                    await status_msg.edit_text(f"Broadcast progress: {idx}/{total} (success {success}, failed {failed})")
                except MessageNotModified:
                    pass
                except Exception:
                    pass
        await status_msg.edit_text(f"Broadcast complete. Success: {success}, Failed: {failed}")
    except Exception as e:
        logger.exception("/broadcast failed: %s", e)
        await message.answer("Broadcast failed. See logs.")

@dp.message_handler(commands=['stats'])
async def cmd_stats(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    try:
        s = await Database.get_stats()
        await message.answer(
            f"📊 <b>Bot Statistics</b>\n\n"
            f"👥 Total users: {s['total_users']}\n"
            f"🟢 Active (48h): {s['active_users']}\n"
            f"📁 Files uploaded: {s['files_uploaded']}\n"
            f"🔗 Sessions completed: {s['sessions_completed']}\n"
        )
    except Exception as e:
        logger.exception("/stats failed: %s", e)
        await message.answer("Error fetching stats.")

@dp.message_handler(commands=['restore_db'])
async def cmd_restore_db(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("Unauthorized.")
        return
    try:
        await Database.init_schema()
        await message.answer("✅ Database schema ensured (non-destructive).")
    except Exception as e:
        logger.exception("restore_db failed: %s", e)
        await message.answer("Failed to restore DB schema. Check logs.")

# -------------------------
# Global error handler
# -------------------------
@dp.errors_handler()
async def global_error_handler(update, exception):
    logger.exception("Dispatcher error: update=%s exception=%s", update, exception)
    return True

# -------------------------
# Build web app and run
# -------------------------
def build_app():
    app = web.Application()
    # Log incoming requests at the aiohttp level as well
    async def log_request_middleware(app, handler):
        async def middleware_handler(request):
            try:
                logger.debug("Request %s %s from %s", request.method, request.path, request.remote)
            except Exception:
                pass
            return await handler(request)
        return middleware_handler
    app.middlewares.append(log_request_middleware)

    # routes
    app.router.add_get('/', health)
    app.router.add_get('/health', health)
    app.router.add_post(f'/webhook/{BOT_TOKEN}', webhook_handler)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app

if __name__ == '__main__':
    app = build_app()
    web.run_app(app, host='0.0.0.0', port=PORT)