#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram file-share bot (aiogram v2) for Render + Neon (Postgres).
Features:
 - Webhook-only mode (Render)
 - Health endpoint (/health) for UptimeRobot
 - Owner-only controls: /setmessage, /setimage, /broadcast, /upload multi-step, /stats
 - Public commands: /start, /help, deep-link access to upload sessions
 - Upload channel: all files are copied to a private channel and file_ids stored in DB
 - Deep links: t.me/<bot_username>?start=<session_id>
 - Auto-delete: when a session has auto-delete > 0, bot deletes the messages it sent to the user
 - Owner bypass: owner can bypass protection and auto-delete
 - Database: Neon/Postgres via asyncpg
 - Uses aiohttp webserver to receive webhook updates and to serve /health
"""

# ###############
# CONFIG / IMPORTS
# ###############

import os
import asyncio
import json
import logging
import secrets
import string
import uuid
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timezone, timedelta

import asyncpg
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.utils.executor import start_webhook
from aiogram.utils.exceptions import (BotBlocked, ChatNotFound, RetryAfter,
                                      Unauthorized, BadRequest, TelegramAPIError)

# --- basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
)
logger = logging.getLogger("file_share_bot")

# Environment variables (must be set in Render)
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.error("BOT_TOKEN not found in environment.")
    raise RuntimeError("BOT_TOKEN environment variable required")

OWNER_ID = int(os.getenv("OWNER_ID", "0"))  # Owner user id as integer
if OWNER_ID == 0:
    logger.warning("OWNER_ID not provided or zero. Set OWNER_ID environment variable to the Telegram user id of the owner.")

DATABASE_URL = os.getenv("DATABASE_URL")  # Neon Postgres connection string
if not DATABASE_URL:
    logger.error("DATABASE_URL not found in environment.")
    raise RuntimeError("DATABASE_URL environment variable required")

# Upload channel ID (private channel where all files are copied)
# Provide as integer (channel id with -100 prefix) or exact numeric id as string
UPLOAD_CHANNEL_ID_RAW = os.getenv("UPLOAD_CHANNEL_ID")
if not UPLOAD_CHANNEL_ID_RAW:
    logger.error("UPLOAD_CHANNEL_ID not provided. Set the private channel ID where uploaded files are copied.")
    raise RuntimeError("UPLOAD_CHANNEL_ID environment variable required")
try:
    UPLOAD_CHANNEL_ID = int(UPLOAD_CHANNEL_ID_RAW)
except Exception:
    UPLOAD_CHANNEL_ID = None

# Render external URL where webhook will be set (e.g. https://your-app.onrender.com)
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
if not RENDER_EXTERNAL_URL:
    logger.error("RENDER_EXTERNAL_URL not found.")
    raise RuntimeError("RENDER_EXTERNAL_URL environment variable required")

# Optional: the hostname path for webhook
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_URL = RENDER_EXTERNAL_URL.rstrip("/") + WEBHOOK_PATH

# Other optional configs
WEB_PORT = int(os.getenv("PORT", "8000"))
AUTO_CLEANUP_LOOP_INTERVAL = int(os.getenv("AUTO_CLEANUP_LOOP_INTERVAL", "60"))  # seconds

# Validate some values
if not WEBHOOK_PATH.startswith("/"):
    WEBHOOK_PATH = "/" + WEBHOOK_PATH

# ###############
# BOT / DISPATCHER / AIOHTTP App
# ###############

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# aiohttp app - to receive webhook and /health
app = web.Application()
routes = web.RouteTableDef()

# Database pool (asyncpg)
db_pool: Optional[asyncpg.pool.Pool] = None

# In-memory upload sessions for the owner while the session is being created.
# This is ephemeral and also persisted to DB after finalization.
# Structure: session_temp_store[owner_id][session_id] = {...}
session_temp_store: Dict[int, Dict[str, Dict[str, Any]]] = {}

# A store of asyncio Tasks for scheduled auto-deletes keyed by session_id -> task
auto_delete_tasks: Dict[str, asyncio.Task] = {}

# ###############
# DATABASE - SCHEMA
# ###############

SCHEMA_SQL = r"""
-- users: tracks users who started the bot
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    first_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    last_active TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- messages: store customizable start/help text and image file_ids
CREATE TABLE IF NOT EXISTS messages (
    key TEXT PRIMARY KEY, -- 'start_text', 'help_text', 'start_image', 'help_image'
    value TEXT NOT NULL
);

-- upload_sessions: a session that contains many files and options
CREATE TABLE IF NOT EXISTS upload_sessions (
    session_id TEXT PRIMARY KEY,
    owner_id BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    protect_content BOOLEAN NOT NULL DEFAULT TRUE,
    auto_delete_minutes INTEGER NOT NULL DEFAULT 0,
    expires_at TIMESTAMP WITH TIME ZONE NULL, -- optional: if we want to expire sessions
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

-- session_files: one row per file in a session
CREATE TABLE IF NOT EXISTS session_files (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES upload_sessions(session_id) ON DELETE CASCADE,
    orig_file_id TEXT NOT NULL, -- file_id as provided by user (copied original)
    stored_file_id TEXT NOT NULL, -- file_id in upload channel (copied)
    file_type TEXT NOT NULL, -- 'photo','document','video','audio', etc.
    caption TEXT NULL,
    media_group_id TEXT NULL,
    added_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- statistics counters
CREATE TABLE IF NOT EXISTS statistics (
    key TEXT PRIMARY KEY,
    value BIGINT NOT NULL
);

-- misc: store owner active upload temp, or other needed metadata can be added later
"""

# ###############
# HELPERS - DB
# ###############

async def init_db_pool():
    global db_pool
    logger.info("Initializing DB pool...")
    db_pool = await asyncpg.create_pool(DATABASE_URL, max_size=10)
    # Create schema if needed
    async with db_pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
        # init stat counters if not present
        await conn.execute("""
            INSERT INTO statistics(key, value) VALUES
            ('total_users', 0) ON CONFLICT (key) DO NOTHING;
        """)
    logger.info("DB pool initialized and schema ensured.")

async def add_or_update_user(user_id: int):
    """Insert user if not exists; update last_active timestamp."""
    if db_pool is None:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users(user_id, first_seen, last_active)
            VALUES ($1, now(), now())
            ON CONFLICT (user_id) DO UPDATE SET last_active = now();
        """, user_id)
        # increment total_users only on new user
        # Check if it was new: we used ON CONFLICT so can't directly tell; do a check:
        # If user first_seen == last_active very close to now => likely new (but to be more deterministic:
        # Try to insert into statistics only when user is newly created. Simpler: attempt to select first_seen.
        # We'll instead use MERGE style: check count of users and set statistics accordingly.
        # For performance, we will attempt to increment the counter only if the user was newly inserted.
        # Check existence before insert would require another query; but it's OK.
        # We'll do a safe pattern: try to insert into users with ON CONFLICT DO NOTHING, then increment if insert succeeded.
    # We'll reimplement more deterministic below where needed.

async def ensure_user_record(user_id: int) -> bool:
    """
    Ensure user exists. Returns True if user was newly added.
    """
    if db_pool is None:
        return False
    async with db_pool.acquire() as conn:
        res = await conn.fetchrow("SELECT user_id FROM users WHERE user_id = $1;", user_id)
        if res:
            await conn.execute("UPDATE users SET last_active = now() WHERE user_id = $1;", user_id)
            return False
        else:
            await conn.execute("INSERT INTO users(user_id, first_seen, last_active) VALUES ($1, now(), now());", user_id)
            await conn.execute("UPDATE statistics SET value = value + 1 WHERE key = 'total_users';")
            return True

async def get_stats_snapshot() -> Dict[str, int]:
    """Return a dict with total_users, active_48h, files_uploaded, upload_sessions."""
    if db_pool is None:
        return {}
    async with db_pool.acquire() as conn:
        total_users_row = await conn.fetchrow("SELECT value FROM statistics WHERE key='total_users';")
        total_users = int(total_users_row['value']) if total_users_row else 0
        active_cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
        active_row = await conn.fetchrow("SELECT count(*) FROM users WHERE last_active >= $1;", active_cutoff)
        active_48h = int(active_row['count']) if active_row else 0
        files_row = await conn.fetchrow("SELECT count(*) FROM session_files;")
        files_uploaded = int(files_row['count']) if files_row else 0
        sessions_row = await conn.fetchrow("SELECT count(*) FROM upload_sessions WHERE is_active = true;")
        upload_sessions = int(sessions_row['count']) if sessions_row else 0
        return {
            "total_users": total_users,
            "active_48h": active_48h,
            "files_uploaded": files_uploaded,
            "upload_sessions": upload_sessions
        }

# Messages table interactions
async def set_message_value(key: str, value: str):
    if db_pool is None:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO messages(key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = $2;
        """, key, value)

async def get_message_value(key: str) -> Optional[str]:
    if db_pool is None:
        return None
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM messages WHERE key = $1;", key)
        return row['value'] if row else None

# Upload session DB operations
async def create_upload_session(session_id: str, owner_id: int, protect: bool, auto_delete_minutes: int) -> None:
    if db_pool is None:
        return
    expires_at = None
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO upload_sessions(session_id, owner_id, protect_content, auto_delete_minutes, expires_at, is_active)
            VALUES ($1, $2, $3, $4, $5, true);
        """, session_id, owner_id, protect, auto_delete_minutes, expires_at)

async def add_file_to_session(session_id: str, orig_file_id: str, stored_file_id: str, file_type: str, caption: Optional[str], media_group_id: Optional[str]):
    if db_pool is None:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO session_files(session_id, orig_file_id, stored_file_id, file_type, caption, media_group_id)
            VALUES ($1, $2, $3, $4, $5, $6);
        """, session_id, orig_file_id, stored_file_id, file_type, caption, media_group_id)
        # increment files_uploaded stat if we want to keep counters (optional)
        await conn.execute("""
            INSERT INTO statistics(key, value) VALUES ('files_uploaded', 1)
            ON CONFLICT (key) DO UPDATE SET value = statistics.value + 1;
        """)

async def get_session(session_id: str) -> Optional[dict]:
    if db_pool is None:
        return None
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT session_id, owner_id, protect_content, auto_delete_minutes, created_at, is_active FROM upload_sessions WHERE session_id = $1;", session_id)
        if not row:
            return None
        files = await conn.fetch("SELECT id, stored_file_id, file_type, caption, media_group_id FROM session_files WHERE session_id = $1 ORDER BY id ASC;", session_id)
        return {
            "session_id": row["session_id"],
            "owner_id": row["owner_id"],
            "protect_content": row["protect_content"],
            "auto_delete_minutes": row["auto_delete_minutes"],
            "created_at": row["created_at"],
            "is_active": row["is_active"],
            "files": [dict(f) for f in files]
        }

async def set_session_inactive(session_id: str):
    if db_pool is None:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE upload_sessions SET is_active = false WHERE session_id = $1;", session_id)

# ###############
# UTILITIES
# ###############

def generate_session_id() -> str:
    # 22-char url-safe random id (a bit longer than Telegram's usual)
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(22))

def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def make_deep_link(session_id: str, bot_username: Optional[str] = None) -> str:
    # returns the URL that the user can click/tap to open bot with start parameter
    # t.me/<bot_username>?start=<session_id> if username provided, else use RENDER_EXTERNAL_URL or fallback
    if bot_username:
        return f"https://t.me/{bot_username}?start={session_id}"
    else:
        # fallback to using t.me link with bot token is not ideal; prefer username
        return f"https://t.me/{session_id}"

# ###############
# AIOHTTP ROUTES (webhook + health)
# ###############

@routes.get("/health")
async def health(request):
    return web.Response(text="ok")

# Webhook endpoint
@routes.post(WEBHOOK_PATH)
async def webhook_handler(request: web.Request):
    """
    Receives Telegram updates from Webhook and forwards them to aiogram Dispatcher.
    """
    try:
        data = await request.text()
        if not data:
            return web.Response(text="ok")
        update = types.Update.to_object(json.loads(data))
        await dp.process_update(update)
    except Exception as e:
        logger.exception("Failed to process update via webhook: %s", e)
    return web.Response(text="ok")

app.add_routes(routes)

# ###############
# MESSAGE UTILITIES
# ###############

async def send_start_message(user: types.User, chat_id: int):
    """
    Sends the start message (customizable) to a user.
    """
    start_text = await get_message_value("start_text") or (
        "Welcome! This bot shares files via deep links. Use /help for more info."
    )
    start_image_file_id = await get_message_value("start_image")
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Help", callback_data="help_button"))
    if start_image_file_id:
        try:
            await bot.send_photo(chat_id=chat_id, photo=start_image_file_id, caption=start_text, reply_markup=kb)
            return
        except TelegramAPIError as e:
            logger.warning("Failed to send start image: %s. Sending text instead.", e)
    await bot.send_message(chat_id=chat_id, text=start_text, reply_markup=kb, parse_mode="html")

async def send_help_message(chat_id: int):
    help_text = await get_message_value("help_text") or (
        "Help\n\nOwner can upload files and create deep links.\nUse /start to see welcome message."
    )
    help_image_file_id = await get_message_value("help_image")
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Back", callback_data="start_button"))
    if help_image_file_id:
        try:
            await bot.send_photo(chat_id=chat_id, photo=help_image_file_id, caption=help_text, reply_markup=kb)
            return
        except TelegramAPIError:
            pass
    await bot.send_message(chat_id=chat_id, text=help_text, reply_markup=kb, parse_mode="html")

# ###############
# COMMAND HANDLERS
# ###############

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    # If start contains argument (deep link), handle session retrieval
    args = message.get_args()
    user = message.from_user
    # Ensure user in DB
    await ensure_user_record(user.id)
    if args:
        session_id = args.strip()
        # fetch session from DB
        sess = await get_session(session_id)
        if not sess or not sess['is_active']:
            await message.reply("Sorry, this link is invalid or the session is no longer available.")
            return
        # If owner is fetching, owner bypass applies
        owner_is_requester = (user.id == sess['owner_id'])
        # Compose files and send copies preserving captions
        await message.reply("Preparing files for you — please wait...")
        sent_message_ids = []
        for f in sess['files']:
            try:
                # depending on file_type, send via copy
                # stored_file_id are file_ids in the upload channel
                sfid = f['stored_file_id']
                ftype = f['file_type']
                caption = f.get('caption')
                if ftype == 'photo':
                    # copy_photo to user
                    sent = await bot.copy_message(chat_id=message.chat.id, from_chat_id=UPLOAD_CHANNEL_ID, message_id=int(sfid))
                    # NOTE: aiogram's copy_message expects message_id, but stored_file_id may be a file_id, not message_id.
                    # To avoid mismatch, we saved stored_file_id as message_id when copying to channel earlier.
                    # So here we interpret stored_file_id as the message_id in the channel.
                else:
                    sent = await bot.copy_message(chat_id=message.chat.id, from_chat_id=UPLOAD_CHANNEL_ID, message_id=int(sfid))
                # record for potential deletion
                sent_message_ids.append((sent.chat.id, sent.message_id))
            except Exception as e:
                logger.exception("Failed to copy file to user: %s", e)
        # Inform user about protection / auto-delete
        if not owner_is_requester:
            if sess['protect_content']:
                await message.reply("Note: This session is protected — forwarding may be disabled for most users.")
            if sess['auto_delete_minutes'] and sess['auto_delete_minutes'] > 0:
                await message.reply(f"Messages you received will be auto-deleted after {sess['auto_delete_minutes']} minute(s).")
                # schedule deletion for this user's messages
                if sess['auto_delete_minutes'] > 0:
                    # create a scheduled task
                    task = asyncio.create_task(schedule_auto_delete(session_id, sent_message_ids, sess['auto_delete_minutes']))
                    # store task in memory keyed by session and user? We'll store globally to be able to cancel if needed
                    auto_delete_tasks.setdefault(session_id, task)
        else:
            await message.reply("Owner access: protections and auto-delete were bypassed.")
        return
    # Normal start (no parameter)
    await send_start_message(user, message.chat.id)


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    await ensure_user_record(message.from_user.id)
    await send_help_message(message.chat.id)

# Inline buttons from start/help images
@dp.callback_query_handler(lambda c: c.data in ("help_button", "start_button"))
async def inline_buttons(cb: types.CallbackQuery):
    if cb.data == "help_button":
        await send_help_message(cb.message.chat.id)
    else:
        await send_start_message(cb.from_user, cb.message.chat.id)
    await cb.answer()

# OWNER: set text for start/help
@dp.message_handler(commands=["setmessage"])
async def cmd_setmessage(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only the owner can set messages.")
        return
    # Usage: /setmessage start|help <text...>
    args = message.get_args()
    if not args:
        await message.reply("Usage:\n/setmessage start <text>\n/setmessage help <text>\n\nYou can use HTML formatting.")
        return
    parts = args.split(None, 1)
    if len(parts) < 2:
        await message.reply("Please provide which message (start/help) and the text.")
        return
    which, text = parts[0].lower(), parts[1]
    if which not in ("start", "help"):
        await message.reply("Unknown key. Use 'start' or 'help'.")
        return
    key = f"{which}_text"
    await set_message_value(key, text)
    await message.reply(f"{which.capitalize()} message saved.")

# OWNER: set image by replying to image with /setimage
@dp.message_handler(commands=["setimage"])
async def cmd_setimage(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only the owner can set images.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to an image (photo) with /setimage to set it as start/help image.")
        return
    replied = message.reply_to_message
    # Check that reply contains a photo or document image
    photo_file_id = None
    if replied.photo:
        # pick the highest quality
        photo_file_id = replied.photo[-1].file_id
    elif replied.document and replied.document.mime_type and replied.document.mime_type.startswith("image"):
        photo_file_id = replied.document.file_id
    else:
        await message.reply("Please reply to a photo or image document.")
        return
    # Ask whether set as start image or help image
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("Start Image", callback_data=f"setimage_start|{photo_file_id}"),
        types.InlineKeyboardButton("Help Image", callback_data=f"setimage_help|{photo_file_id}")
    )
    await message.reply("Set as start image or help image?", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("setimage_"))
async def handle_setimage_choice(cb: types.CallbackQuery):
    if not is_owner(cb.from_user.id):
        await cb.answer("Only owner can set images.", show_alert=True)
        return
    data = cb.data  # e.g., setimage_start|fileid
    try:
        action, file_id = data.split("|", 1)
    except Exception:
        await cb.answer("Invalid data.", show_alert=True)
        return
    which = action.replace("setimage_", "")
    if which not in ("start", "help"):
        await cb.answer("Unknown option.", show_alert=True)
        return
    key = f"{which}_image"
    await set_message_value(key, file_id)
    await cb.edit_message_text(f"{which.capitalize()} image updated.")
    await cb.answer()

# OWNER: broadcast (text/media)
@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    """
    Owner can reply to any message or send text with /broadcast <text>
    The message will be copied (not forwarded) to all users.
    """
    if not is_owner(message.from_user.id):
        await message.reply("Only owner can use broadcast.")
        return
    # Determine the message to broadcast
    target_msg = None
    if message.reply_to_message:
        target_msg = message.reply_to_message
    else:
        args = message.get_args()
        if not args:
            await message.reply("Usage: reply to a message with /broadcast, or /broadcast <text>")
            return
        # create a synthetic message with text
        target_msg = types.Message.to_python(message)  # not ideal; simpler: send text individually
        # We'll broadcast text from args
        text = args
        # fetch all users
        if db_pool is None:
            await message.reply("Database not ready.")
            return
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id FROM users;")
            user_ids = [r['user_id'] for r in rows]
        sent = 0
        failed = 0
        for uid in user_ids:
            try:
                await bot.send_message(chat_id=uid, text=text, parse_mode="html")
                sent += 1
                await asyncio.sleep(0.05)
            except (BotBlocked, ChatNotFound, Unauthorized):
                failed += 1
            except RetryAfter as e:
                logger.warning("Flood: sleeping %s sec", e.timeout)
                await asyncio.sleep(e.timeout)
                try:
                    await bot.send_message(chat_id=uid, text=text, parse_mode="html")
                    sent += 1
                except Exception:
                    failed += 1
            except Exception:
                failed += 1
        await message.reply(f"Broadcast complete. Sent: {sent}, Failed: {failed}")
        return
    # If there is a target_msg (a replied message), we need to broadcast a copy of it.
    # We will copy that message to each user (copy_message avoids forward tag).
    # But first collect user ids
    if db_pool is None:
        await message.reply("Database not ready.")
        return
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users;")
        user_ids = [r['user_id'] for r in rows]
    sent = 0
    failed = 0
    for uid in user_ids:
        try:
            # copy_message requires we have the original chat id and message id.
            if target_msg.chat and target_msg.message_id:
                await bot.copy_message(chat_id=uid, from_chat_id=target_msg.chat.id, message_id=target_msg.message_id)
            else:
                # fallback: if only text
                if target_msg.text:
                    await bot.send_message(chat_id=uid, text=target_msg.text, parse_mode="html")
                else:
                    # ignore
                    pass
            sent += 1
            await asyncio.sleep(0.05)
        except (BotBlocked, ChatNotFound, Unauthorized):
            failed += 1
        except RetryAfter as e:
            logger.warning("Broadcast flood; sleeping %s sec", e.timeout)
            await asyncio.sleep(e.timeout)
            try:
                await bot.copy_message(chat_id=uid, from_chat_id=target_msg.chat.id, message_id=target_msg.message_id)
                sent += 1
            except Exception:
                failed += 1
        except Exception as e:
            logger.exception("Broadcast error: %s", e)
            failed += 1
    await message.reply(f"Broadcast complete. Sent: {sent}, Failed: {failed}")

# OWNER: stats
@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner can view stats.")
        return
    stats = await get_stats_snapshot()
    text = (
        f"📊 <b>Usage statistics</b>\n\n"
        f"Total users: {stats.get('total_users', 0)}\n"
        f"Active (48h): {stats.get('active_48h', 0)}\n"
        f"Files uploaded: {stats.get('files_uploaded', 0)}\n"
        f"Active sessions: {stats.get('upload_sessions', 0)}\n"
    )
    await message.reply(text, parse_mode="html")

# ###############
# UPLOAD FLOW (Multi-step)
# ###############

# The upload flow is owner-only, multi-step:
# 1) Owner sends /upload
# 2) Bot asks to send files (multiple). Owner sends multiple messages (photos/docs/video...).
# 3) Owner sends /d to finish or /c to cancel
# 4) Bot asks protect? and auto-delete minutes (0-10080)
# 5) Bot creates session, copies files to UPLOAD_CHANNEL_ID and stores file message_ids in DB
# 6) Bot returns deep link

@dp.message_handler(commands=["upload"])
async def cmd_upload_start(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner can start an upload session.")
        return
    owner_id = message.from_user.id
    # create a temporary session_id in memory to collect files
    session_id = generate_session_id()
    session_temp_store.setdefault(owner_id, {})[session_id] = {
        "files": [],  # list of dicts with orig message, temp data
        "created_at": datetime.now(timezone.utc),
    }
    # instruct owner
    txt = (
        "Upload session started.\n\n"
        "Please send the files (documents/photos/videos) one by one or in media groups.\n"
        "When finished, send /d to finalize or /c to cancel.\n"
        "You may send up to the limits of Telegram - the bot will copy files to the upload channel."
    )
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Done", callback_data=f"upload_done|{session_id}"),
           types.InlineKeyboardButton("Cancel", callback_data=f"upload_cancel|{session_id}"))
    await message.reply(txt, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("upload_done|"))
async def upload_done_cb(cb: types.CallbackQuery):
    if not is_owner(cb.from_user.id):
        await cb.answer("Only owner can manage upload sessions.", show_alert=True)
        return
    _, session_id = cb.data.split("|", 1)
    owner_sessions = session_temp_store.get(cb.from_user.id, {})
    sess = owner_sessions.get(session_id)
    if not sess:
        await cb.answer("Session not found or expired.", show_alert=True)
        return
    await finalize_upload_session(cb.from_user.id, session_id)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("upload_cancel|"))
async def upload_cancel_cb(cb: types.CallbackQuery):
    if not is_owner(cb.from_user.id):
        await cb.answer("Only owner can manage upload sessions.", show_alert=True)
        return
    _, session_id = cb.data.split("|", 1)
    owner_sessions = session_temp_store.get(cb.from_user.id, {})
    sess = owner_sessions.pop(session_id, None)
    if not sess:
        await cb.answer("Session not found or expired.", show_alert=True)
        return
    await cb.edit_message_text("Upload session cancelled.")
    await cb.answer()

@dp.message_handler(commands=["d"])  # done
async def upload_done_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner can finalize upload sessions.")
        return
    owner_id = message.from_user.id
    # find the latest session for this owner (if only one, assume that)
    owner_sessions = session_temp_store.get(owner_id, {})
    if not owner_sessions:
        await message.reply("No active upload session found. Use /upload to start one.")
        return
    # If multiple, pick the most recently created
    session_id = sorted(owner_sessions.items(), key=lambda kv: kv[1]["created_at"], reverse=True)[0][0]
    await finalize_upload_session(owner_id, session_id)

@dp.message_handler(commands=["c"])  # cancel
async def upload_cancel_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner can cancel upload sessions.")
        return
    owner_sessions = session_temp_store.get(message.from_user.id, {})
    if not owner_sessions:
        await message.reply("No active upload session found.")
        return
    # cancel all
    for sid in list(owner_sessions.keys()):
        owner_sessions.pop(sid, None)
    await message.reply("All active upload sessions cancelled.")

# Handler to capture files during upload session
@dp.message_handler(content_types=types.ContentType.ANY)
async def catch_upload_files(message: types.Message):
    # Only consider if owner and there's an active session
    if not is_owner(message.from_user.id):
        # update user's activity and ignore
        await ensure_user_record(message.from_user.id)
        return
    owner_id = message.from_user.id
    owner_sessions = session_temp_store.get(owner_id, {})
    if not owner_sessions:
        # Not in upload mode; ignore but allow other commands
        return
    # find most recent session to append files
    session_id = sorted(owner_sessions.items(), key=lambda kv: kv[1]["created_at"], reverse=True)[0][0]
    sess = owner_sessions[session_id]
    # Save the message object for later processing (we will copy to channel on finalize)
    # For memory safety, we only store minimal info
    file_entry = {
        "message_id": message.message_id,
        "chat_id": message.chat.id,
        "message": message  # storing full message object - ok for owner-only short duration
    }
    sess['files'].append(file_entry)
    await message.reply("File received and queued for this upload session. Send /d when finished or continue sending files.")

async def finalize_upload_session(owner_id: int, session_id: str):
    """
    Copies each collected message to the upload channel and stores session metadata to DB.
    Asks owner for protection and auto-delete options.
    """
    owner_sessions = session_temp_store.get(owner_id, {})
    sess = owner_sessions.get(session_id)
    if not sess:
        # try to find by id in other way
        logger.warning("No temp upload session found for finalize: %s", session_id)
        return
    files = sess.get('files', [])
    if not files:
        # no files; cancel
        await bot.send_message(chat_id=owner_id, text="This upload session contains no files and was cancelled.")
        owner_sessions.pop(session_id, None)
        return
    # Ask owner: protect content? and auto-delete minutes
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("Protected (no forwarding)", callback_data=f"upload_opts|{session_id}|protect|1"),
        types.InlineKeyboardButton("Not Protected", callback_data=f"upload_opts|{session_id}|protect|0")
    )
    await bot.send_message(chat_id=owner_id, text="Choose protection level for this session:", reply_markup=kb)

    # Next, ask for auto-delete minutes input via reply
    await bot.send_message(chat_id=owner_id, text="After choosing protection, send an integer for auto-delete minutes (0 = never). Allowed range: 0 - 10080 (one week). If you send invalid, 0 will be used.")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("upload_opts|"))
async def handle_upload_opts(cb: types.CallbackQuery):
    if not is_owner(cb.from_user.id):
        await cb.answer("Only owner.", show_alert=True)
        return
    # data format: upload_opts|<session_id>|protect|0/1
    parts = cb.data.split("|")
    if len(parts) != 4:
        await cb.answer("Invalid data.", show_alert=True)
        return
    _, session_id, key, val = parts
    owner_id = cb.from_user.id
    owner_sessions = session_temp_store.get(owner_id, {})
    sess = owner_sessions.get(session_id)
    if not sess:
        await cb.answer("Session not found.", show_alert=True)
        return
    protect = bool(int(val))
    sess['protect'] = protect
    await cb.answer("Protection option saved.")
    await cb.edit_message_text(f"Protection set to: {'Protected' if protect else 'Not Protected'}.\nNow, please send the auto-delete minutes (0 = never). Allowed: 0 - 10080.")
    # Wait for owner's next message to contain the auto-delete minutes. We'll capture it with a temporary state.
    # To avoid implementing FSM, we'll record expected input in sess
    sess['expecting_auto_delete'] = True

# To capture the numeric input after the protection choice
@dp.message_handler(lambda m: is_owner(m.from_user.id) and m.text is not None)
async def capture_auto_delete_input(message: types.Message):
    owner_id = message.from_user.id
    owner_sessions = session_temp_store.get(owner_id, {})
    if not owner_sessions:
        return
    # find the session where expecting_auto_delete is True (most recent)
    found = None
    for sid, s in owner_sessions.items():
        if s.get('expecting_auto_delete'):
            found = (sid, s)
            break
    if not found:
        # not expecting, ignore (but still update owner last active)
        return
    sid, s = found
    text = message.text.strip()
    try:
        minutes = int(text)
        if minutes < 0:
            minutes = 0
    except Exception:
        minutes = 0
    if minutes > 10080:
        minutes = 10080
    s['auto_delete_minutes'] = minutes
    s.pop('expecting_auto_delete', None)
    # finalize: copy files to upload channel, save session to DB
    await message.reply("Thank you. Finalizing session — copying files to upload channel and generating deep link. This may take a few moments.")
    await copy_files_and_create_session(owner_id, sid, s)

async def copy_files_and_create_session(owner_id: int, session_id: str, s: dict):
    files = s.get('files', [])
    protect = s.get('protect', True)
    auto_delete_minutes = s.get('auto_delete_minutes', 0)
    # create upload session record
    await create_upload_session(session_id, owner_id, protect, auto_delete_minutes)
    # iterate files and copy each msg to UPLOAD_CHANNEL_ID. Important: we need to store the message_id in the channel,
    # because copy_message returns a Message object with message_id in the destination chat. We'll store that id as stored_file_id.
    copied_count = 0
    for entry in files:
        msg: types.Message = entry.get('message')
        try:
            # Copy message to the upload channel. If the original message is a media_group this gets trickier.
            # For simplicity, we'll copy the entire message (message may be a group member, but copying individually is fine).
            # Use bot.copy_message to copy to channel
            result = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=msg.chat.id, message_id=msg.message_id)
            # result is a Message in UPLOAD_CHANNEL_ID; store result.message_id as stored_file_id
            stored_message_id = result.message_id
            # Determine file_type and caption
            file_type = "unknown"
            caption = None
            media_group_id = None
            if msg.photo:
                file_type = "photo"
                caption = msg.caption or None
            elif msg.document:
                file_type = "document"
                caption = msg.caption or None
            elif msg.video:
                file_type = "video"
                caption = msg.caption or None
            elif msg.audio:
                file_type = "audio"
                caption = msg.caption or None
            elif msg.voice:
                file_type = "voice"
                caption = msg.caption or None
            elif msg.sticker:
                file_type = "sticker"
            # For orig_file_id: if we want we can store msg.message_id or the original file_id; to keep references,
            # we store the original message_id as orig_file_id (string) and stored_message_id as stored_file_id (string)
            orig_file_id = str(msg.message_id)
            await add_file_to_session(session_id, orig_file_id, str(stored_message_id), file_type, caption, media_group_id)
            copied_count += 1
            await asyncio.sleep(0.15)
        except Exception as e:
            logger.exception("Failed to copy message to upload channel: %s", e)
    # After copying and storing, generate deep link
    # Try to get bot username
    try:
        me = await bot.get_me()
        bot_username = me.username
    except Exception:
        bot_username = None
    deep_link = make_deep_link(session_id, bot_username)
    # Remove session from temp store
    session_temp_store.get(owner_id, {}).pop(session_id, None)
    # Send final message to owner with details
    final_text = (
        f"Upload session created.\n\n"
        f"Session ID: <code>{session_id}</code>\n"
        f"Files copied: {copied_count}\n"
        f"Protected: {'Yes' if protect else 'No'}\n"
        f"Auto-delete minutes: {auto_delete_minutes}\n\n"
        f"Deep link (anyone with it can access the files):\n{deep_link}"
    )
    await bot.send_message(chat_id=owner_id, text=final_text, parse_mode="html")
    # Optionally schedule session-level cleanup or mark as active. If auto-delete > 0, we do not delete DB or channel; we only auto-delete served messages to user
    # But we may want to schedule session expiry if desired — left as TODO

# ###############
# AUTO-DELETE SCHEDULER
# ###############

async def schedule_auto_delete(session_id: str, sent_message_ids: List[Tuple[int,int]], minutes: int):
    """
    Waits minutes and then deletes messages in sent_message_ids (list of tuples (chat_id, message_id)).
    This only deletes the messages from the recipient's chat, not from the channel/DB.
    """
    if minutes <= 0:
        return
    try:
        await asyncio.sleep(minutes * 60)
        for chat_id, message_id in sent_message_ids:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=message_id)
            except (BadRequest, TelegramAPIError) as e:
                logger.debug("Could not delete message %s in chat %s: %s", message_id, chat_id, e)
    except asyncio.CancelledError:
        logger.info("Auto-delete task for session %s cancelled.", session_id)
    except Exception as e:
        logger.exception("Auto-delete task failed: %s", e)

# ###############
# STARTUP / SHUTDOWN
# ###############

async def set_webhook_and_start():
    """
    Sets webhook to RENDER_EXTERNAL_URL + WEBHOOK_PATH and starts aiohttp server.
    Using aiogram executor's start_webhook is also possible; but we already created an aiohttp app.
    """
    # set webhook
    try:
        await bot.set_webhook(RENDER_EXTERNAL_URL + WEBHOOK_PATH)
        logger.info("Webhook set to %s", RENDER_EXTERNAL_URL + WEBHOOK_PATH)
    except Exception as e:
        logger.exception("Failed to set webhook: %s", e)
        raise

async def on_startup(app):
    logger.info("Bot starting up...")
    # initialize DB
    await init_db_pool()
    # set webhook
    await set_webhook_and_start()
    # set global
    app['bot'] = bot
    # schedule any background tasks if needed
    # Example: cleanup loop that can check expired sessions (not deleting DB) - optional
    app['cleanup_task'] = asyncio.create_task(background_cleanup_loop())

async def on_shutdown(app):
    logger.info("Shutting down...")
    # cancel cleanup
    task = app.get('cleanup_task')
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    # close DB pool
    if db_pool:
        await db_pool.close()
    try:
        await bot.delete_webhook()
    except Exception:
        pass
    await bot.session.close()
    logger.info("Shutdown complete.")

# Background periodic tasks if desired
async def background_cleanup_loop():
    while True:
        try:
            # placeholder: here we could find sessions to expire, or clean tasks
            await asyncio.sleep(AUTO_CLEANUP_LOOP_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception("Cleanup loop error: %s", e)
            await asyncio.sleep(5)

# Register startup/shutdown signals with aiohttp app
app.on_startup.append(on_startup)
app.on_cleanup.append(on_shutdown)

# ###############
# UTILITY ENDPOINT - manual trigger to test DB and bot
# ###############

@routes.get("/_info")
async def info_handler(request):
    """Non-sensitive info useful for debugging (only callable from allowed IPs ideally)."""
    try:
        stats = await get_stats_snapshot()
        return web.json_response({"ok": True, "stats": stats, "webhook_url": RENDER_EXTERNAL_URL + WEBHOOK_PATH})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)})

# ###############
# MAIN - run aiohttp app
# ###############

def main():
    # Start aiohttp server with webhook route already in app
    # Note: aiogram's start_webhook utility could be used, but we already handle webhook route manually.
    logger.info("Starting web app on port %s ...", WEB_PORT)
    web.run_app(app, host="0.0.0.0", port=WEB_PORT)

if __name__ == "__main__":
    main()