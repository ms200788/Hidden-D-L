# bot.py
"""
Aiogram v2 persistent upload bot.
Designed for Render + Neon (Postgres) + Telegram upload channel as storage.
"""

import os
import sys
import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List

import asyncpg
from aiohttp import web

from aiogram import Bot, Dispatcher, types, executor
from aiogram.utils.callback_data import CallbackData
from aiogram.dispatcher.filters import Text
from aiogram.dispatcher.filters.builtin import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# -------------------------
# Config from env
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))

USE_WEBHOOK = os.getenv("USE_WEBHOOK", "true").lower() in ("1", "true", "yes")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE") or os.getenv("RENDER_EXTERNAL_URL")  # e.g. https://hidden-d-l.onrender.com
PORT = int(os.getenv("PORT", os.getenv("RENDER_PORT", "8080")))

if not BOT_TOKEN or not DATABASE_URL or not OWNER_ID or not UPLOAD_CHANNEL_ID:
    logging.error("Missing required environment variables. BOT_TOKEN, DATABASE_URL, OWNER_ID, UPLOAD_CHANNEL_ID are required.")
    sys.exit(1)

WEBHOOK_URL = None
if WEBHOOK_BASE:
    WEBHOOK_URL = WEBHOOK_BASE.rstrip("/") + WEBHOOK_PATH

# -------------------------
# Logging
# -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("vault-bot")

# -------------------------
# Bot & Dispatcher
# -------------------------
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

# -------------------------
# DB pool
# -------------------------
DB_POOL: Optional[asyncpg.pool.Pool] = None

# -------------------------
# SQL: create tables
# -------------------------
CREATE_TABLES_SQL = r"""
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS users (
  user_id BIGINT PRIMARY KEY,
  first_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  last_active TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  sessions INT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS links (
  batch_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  owner_id BIGINT NOT NULL,
  protect_content BOOLEAN NOT NULL DEFAULT FALSE,
  auto_delete_minutes INT NOT NULL DEFAULT 0,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS files (
  id SERIAL PRIMARY KEY,
  batch_id UUID REFERENCES links(batch_id) ON DELETE CASCADE,
  channel_message_id BIGINT NOT NULL,
  file_type TEXT NOT NULL,
  caption TEXT
);

CREATE TABLE IF NOT EXISTS messages (
  type VARCHAR(10) PRIMARY KEY,  -- 'start' or 'help'
  text TEXT,
  image_file_id TEXT
);

CREATE TABLE IF NOT EXISTS deliveries (
  id SERIAL PRIMARY KEY,
  batch_id UUID REFERENCES links(batch_id) ON DELETE CASCADE,
  to_user_id BIGINT NOT NULL,
  delivered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  expire_at TIMESTAMP WITH TIME ZONE,
  message_ids JSONB NOT NULL,
  deleted BOOLEAN NOT NULL DEFAULT FALSE
);
"""

# -------------------------
# DB init
# -------------------------
async def init_db():
    global DB_POOL
    if DB_POOL:
        return
    log.info("Connecting to database...")
    DB_POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=8)
    async with DB_POOL.acquire() as conn:
        await conn.execute(CREATE_TABLES_SQL)
        # ensure default messages exist
        await conn.execute(
            "INSERT INTO messages (type, text) VALUES ($1, $2) ON CONFLICT (type) DO NOTHING",
            "start", "Welcome! Use /help to see usage."
        )
        await conn.execute(
            "INSERT INTO messages (type, text) VALUES ($1, $2) ON CONFLICT (type) DO NOTHING",
            "help", "This bot shares files via deep-links."
        )
    log.info("Database ready.")


# -------------------------
# DB helper functions
# -------------------------
async def upsert_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, first_seen, last_active, sessions)
            VALUES ($1, now(), now(), 1)
            ON CONFLICT (user_id) DO UPDATE
            SET last_active = now(), sessions = users.sessions + 1
        """, user_id)

async def touch_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, first_seen, last_active, sessions)
            VALUES ($1, now(), now(), 1)
            ON CONFLICT (user_id) DO UPDATE
            SET last_active = now()
        """, user_id)

async def save_message_setting(type_: str, text: Optional[str], image_file_id: Optional[str]):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
            INSERT INTO messages (type, text, image_file_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (type) DO UPDATE SET text = EXCLUDED.text, image_file_id = EXCLUDED.image_file_id
        """, type_, text, image_file_id)

async def get_message_setting(type_: str) -> Dict[str, Optional[str]]:
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT text, image_file_id FROM messages WHERE type = $1", type_)
        if not row:
            return {"text": "", "image_file_id": None}
        return {"text": row["text"], "image_file_id": row["image_file_id"]}

# -------------------------
# In-memory upload sessions (owner only)
# -------------------------
UPLOAD_SESSIONS: Dict[int, Dict[str, Any]] = {}
# each entry: { "batch_id": str, "collected": [ {chat_id,msg_id,file_type,caption} ], "finished": False, "protect_content": False }

def start_upload_session(owner_id: int) -> Dict[str, Any]:
    batch = str(uuid.uuid4())
    sess = {"batch_id": batch, "collected": [], "finished": False}
    UPLOAD_SESSIONS[owner_id] = sess
    return sess

def add_to_session(owner_id: int, item: Dict[str, Any]):
    UPLOAD_SESSIONS.setdefault(owner_id, {"batch_id": str(uuid.uuid4()), "collected": [], "finished": False})
    UPLOAD_SESSIONS[owner_id]["collected"].append(item)

def end_upload_session(owner_id: int) -> Optional[Dict[str, Any]]:
    if owner_id not in UPLOAD_SESSIONS:
        return None
    UPLOAD_SESSIONS[owner_id]["finished"] = True
    return UPLOAD_SESSIONS[owner_id]

def cancel_upload_session(owner_id: int) -> Optional[Dict[str, Any]]:
    return UPLOAD_SESSIONS.pop(owner_id, None)

# -------------------------
# Helper: forward message to upload channel and record files row (for finalization)
# -------------------------
async def forward_and_record(batch_id: str, src_chat_id: int, src_message_id: int, file_type: str, caption: Optional[str]):
    forwarded = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=src_chat_id, message_id=src_message_id)
    channel_msg_id = forwarded.message_id
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
            INSERT INTO files (batch_id, channel_message_id, file_type, caption)
            VALUES ($1::uuid, $2, $3, $4)
        """, batch_id, channel_msg_id, file_type, caption)
    return channel_msg_id

# -------------------------
# Owner check
# -------------------------
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

# -------------------------
# Handlers
# -------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    # deep link handling
    args = None
    if message.text:
        parts = message.text.split(None, 1)
        if len(parts) > 1:
            args = parts[1]
    if args:
        await touch_user(message.from_user.id)
        await handle_deep_link(message, args)
        return

    await upsert_user(message.from_user.id)
    setting = await get_message_setting("start")
    text = setting.get("text") or "Welcome!"
    image_file_id = setting.get("image_file_id")
    keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("Help", callback_data="show_help"))
    if image_file_id:
        try:
            await bot.send_photo(chat_id=message.chat.id, photo=image_file_id, caption=text, reply_markup=keyboard)
            return
        except Exception:
            pass
    await message.reply(text, reply_markup=keyboard)

@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    await touch_user(message.from_user.id)
    setting = await get_message_setting("help")
    text = setting.get("text") or "Help"
    image_file_id = setting.get("image_file_id")
    if image_file_id:
        try:
            await bot.send_photo(chat_id=message.chat.id, photo=image_file_id, caption=text)
            return
        except Exception:
            pass
    await message.reply(text)

@dp.callback_query_handler(lambda c: c.data == "show_help")
async def cb_show_help(call: types.CallbackQuery):
    await call.answer()
    # reuse cmd_help by building a fake message-like object not necessary -> call the same logic
    await cmd_help(call.message)

# ---------- setimage ----------
# Usage: owner replies to an image/document with /setimage
@dp.message_handler(commands=["setimage"])
async def cmd_setimage(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner can use this.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to an image/document with /setimage")
        return
    replied = message.reply_to_message
    file_ref = None
    if replied.photo:
        file_ref = replied.photo[-1].file_id
    elif replied.document:
        file_ref = replied.document.file_id
    else:
        await message.reply("Reply to a photo or document containing an image.")
        return

    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("Set as START image", callback_data=f"setimg:start:{file_ref}"))
    kb.add(InlineKeyboardButton("Set as HELP image", callback_data=f"setimg:help:{file_ref}"))
    await message.reply("Choose where to set this image:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("setimg:"))
async def cb_setimg(call: types.CallbackQuery):
    if not is_owner(call.from_user.id):
        await call.answer("Only owner.")
        return
    try:
        _, typ, file_ref = call.data.split(":", 2)
    except Exception:
        await call.answer("Invalid data.")
        return
    await save_message_setting(typ, None, file_ref)
    await call.answer(f"{typ} image saved.")
    try:
        await call.message.edit_text(f"{typ} image updated.")
    except Exception:
        pass

# ---------- setmessage ----------
# Usage: reply to a text message with /setmessage OR: /setmessage start <text>
@dp.message_handler(commands=["setmessage"])
async def cmd_setmessage(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner can use this.")
        return
    args = None
    if message.text:
        parts = message.text.split(None, 2)
        if len(parts) >= 3 and parts[1] in ("start", "help"):
            # /setmessage start Some Text...
            typ = parts[1]
            text = parts[2]
            await save_message_setting(typ, text, None)
            await message.reply(f"{typ} message updated.")
            return

    if message.reply_to_message and message.reply_to_message.text:
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("Set as START text", callback_data="setmsg:start"))
        kb.add(InlineKeyboardButton("Set as HELP text", callback_data="setmsg:help"))
        # store pending text in session memory keyed by owner id
        UPLOAD_SESSIONS[message.from_user.id] = UPLOAD_SESSIONS.get(message.from_user.id, {})
        UPLOAD_SESSIONS[message.from_user.id]["pending_text"] = message.reply_to_message.text
        await message.reply("Choose where to set this text:", reply_markup=kb)
        return
    await message.reply("Usage: reply to a text message with /setmessage or use /setmessage start <text>")

# -------------------------
# callback for setmsg (v2)
# -------------------------
@dp.callback_query_handler(lambda c: c.data and c.data.startswith("setmsg:"))
def callback_setmsg_cb(query):
    user_id = query.from_user.id
    if not is_owner(user_id):
        query.answer("Only owner.")
        return
    typ = query.data.split(":")[1]
    pending = UPLOAD_SESSIONS.get(user_id, {}).get("pending_text")
    if not pending:
        query.answer("No pending text found. Reply to a message with /setmessage and try again.")
        return
    # save into DB (run in background)
    async def _save():
        await save_message_setting(typ, pending, None)
    asyncio.ensure_future(_save())
    UPLOAD_SESSIONS.get(user_id, {}).pop("pending_text", None)
    try:
        query.answer(f"{typ} text saved.")
        query.message.edit_text(f"{typ} text updated.")
    except Exception:
        pass

# -------------------------
# stats (v2)
# -------------------------
@dp.message_handler(commands=["stats"])
def cmd_stats_v2(message: types.Message):
    if not is_owner(message.from_user.id):
        message.reply("Only owner can use this.")
        return

    async def _stats():
        async with DB_POOL.acquire() as conn:
            total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
            active_2d = await conn.fetchval("SELECT COUNT(*) FROM users WHERE last_active >= now() - INTERVAL '2 days'")
            total_files = await conn.fetchval("SELECT COUNT(*) FROM files")
            total_sessions = await conn.fetchval("SELECT SUM(sessions) FROM users")
        await bot.send_message(chat_id=message.chat.id,
                               text=f"Users: {total_users}\nActive (2d): {active_2d}\nFiles: {total_files}\nTotal sessions: {total_sessions or 0}")

    asyncio.ensure_future(_stats())

# -------------------------
# broadcast (v2)
# -------------------------
@dp.message_handler(commands=["broadcast"])
def cmd_broadcast_v2(message: types.Message):
    if not is_owner(message.from_user.id):
        message.reply("Only owner can use this.")
        return
    if not message.reply_to_message:
        message.reply("Reply to the message you want to broadcast with /broadcast")
        return

    async def _broadcast():
        async with DB_POOL.acquire() as conn:
            rows = await conn.fetch("SELECT user_id FROM users")
        total = len(rows)
        await bot.send_message(chat_id=message.chat.id, text=f"Broadcasting to {total} users...")
        sent = 0
        for r in rows:
            uid = r["user_id"]
            try:
                # copy_message works in v2 too
                await bot.copy_message(chat_id=uid, from_chat_id=message.reply_to_message.chat.id,
                                       message_id=message.reply_to_message.message_id)
                sent += 1
                # small sleep to avoid flood limits
                await asyncio.sleep(0.05)
            except Exception as e:
                log.warning("Broadcast to %s failed: %s", uid, e)
        await bot.send_message(chat_id=message.chat.id, text=f"Broadcast complete. Sent: {sent}/{total}")

    asyncio.ensure_future(_broadcast())


# -------------------------
# Upload flow (owner) (v2)
# -------------------------
@dp.message_handler(commands=["upload"])
def cmd_upload_v2(message: types.Message):
    if not is_owner(message.from_user.id):
        message.reply("Only owner.")
        return
    start_upload_session(message.from_user.id)
    message.reply("Upload session started. Send files (photos, videos, documents) in as many messages as you want.\nWhen finished send /e. To cancel send /d.")

@dp.message_handler(commands=["d"])
def cmd_cancel_upload_v2(message: types.Message):
    if not is_owner(message.from_user.id):
        message.reply("Only owner.")
        return
    canceled = cancel_upload_session(message.from_user.id)
    if canceled:
        message.reply("Upload session cancelled.")
    else:
        message.reply("No active upload session.")

@dp.message_handler(commands=["e"])
def cmd_end_upload_v2(message: types.Message):
    if not is_owner(message.from_user.id):
        message.reply("Only owner.")
        return
    sess = end_upload_session(message.from_user.id)
    if not sess:
        message.reply("No active upload session.")
        return
    batch_id = sess["batch_id"]
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Protect ON (disable forward/save)", callback_data=f"upload_protect:on:{batch_id}"))
    kb.add(InlineKeyboardButton("Protect OFF", callback_data=f"upload_protect:off:{batch_id}"))
    message.reply("Protect content?", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("upload_protect:"))
def callback_upload_protect_cb(query):
    user_id = query.from_user.id
    if not is_owner(user_id):
        query.answer("Only owner.")
        return
    parts = query.data.split(":")
    if len(parts) != 3:
        query.answer("Invalid data.")
        return
    _, val, batch_id = parts
    sess = UPLOAD_SESSIONS.get(user_id)
    if not sess or sess.get("batch_id") != batch_id:
        query.answer("Upload session not found.")
        return
    UPLOAD_SESSIONS[user_id]["protect_content"] = (val == "on")
    try:
        query.message.edit_text("Set auto-delete timer in minutes (0 to 10080). Send a number as a message in chat, or reply '0' to disable.")
    except Exception:
        pass
    query.answer()

# numeric input in owner upload flow
@dp.message_handler(lambda message: is_owner(message.from_user.id) and message.text and message.text.isdigit())
def handle_auto_delete_input_v2(message: types.Message):
    sess = UPLOAD_SESSIONS.get(message.from_user.id)
    if not sess or "protect_content" not in sess:
        # not in upload flow expecting number
        return
    minutes = int(message.text)
    if minutes < 0 or minutes > 10080:
        message.reply("Invalid value. Must be 0..10080.")
        return
    batch_id = sess["batch_id"]
    protect = sess.get("protect_content", False)
    collected = sess.get("collected", [])
    if not collected:
        message.reply("No files were uploaded in this session.")
        cancel_upload_session(message.from_user.id)
        return

    async def _finalize():
        # forward each collected original message (owner chat) to upload channel and store records
        for msg_data in collected:
            try:
                orig_chat_id = msg_data["chat_id"]
                orig_msg_id = msg_data["message_id"]
                forwarded = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=orig_chat_id, message_id=orig_msg_id)
                channel_msg_id = forwarded.message_id
                file_type = msg_data.get("file_type", "other")
                caption = msg_data.get("caption")
                async with DB_POOL.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO files (batch_id, channel_message_id, file_type, caption)
                        VALUES ($1::uuid, $2, $3, $4)
                    """, batch_id, channel_msg_id, file_type, caption)
            except Exception as e:
                log.exception("Failed to forward file: %s", e)
        # persist link metadata
        async with DB_POOL.acquire() as conn:
            await conn.execute("""
                INSERT INTO links (batch_id, owner_id, protect_content, auto_delete_minutes, active)
                VALUES ($1::uuid, $2, $3, $4, TRUE)
            """, batch_id, message.from_user.id, protect, minutes)
        # prepare deep link
        bot_me = await bot.get_me()
        deep_link = f"https://t.me/{bot_me.username}?start={batch_id}"
        cancel_upload_session(message.from_user.id)
        await bot.send_message(chat_id=message.chat.id, text=f"Upload saved. Deep link:\n{deep_link}")

    asyncio.ensure_future(_finalize())

# capture files owner sends during upload session
@dp.message_handler(content_types=['photo', 'video', 'document', 'audio'])
def capture_owner_uploads_v2(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    sess = UPLOAD_SESSIONS.get(message.from_user.id)
    if not sess or sess.get("finished"):
        return
    data = {
        "chat_id": message.chat.id,
        "message_id": message.message_id,
        "file_type": "photo" if message.photo else ("video" if message.video else ("document" if message.document else ("audio" if message.audio else "other"))),
        "caption": message.caption or None
    }
    add_to_session(message.from_user.id, data)
    message.reply("File recorded for upload session. Send more or /e to finish, /d to cancel.")


# -------------------------
# Deep link handling (v2)
# -------------------------
async def _handle_deep_link_async(message: types.Message, start_payload: str):
    try:
        batch_uuid = uuid.UUID(start_payload)
    except Exception:
        await bot.send_message(chat_id=message.chat.id, text="Invalid link.")
        return
    async with DB_POOL.acquire() as conn:
        link = await conn.fetchrow("SELECT * FROM links WHERE batch_id = $1::uuid AND active = TRUE", str(batch_uuid))
        if not link:
            await bot.send_message(chat_id=message.chat.id, text="This link is not active or doesn't exist.")
            return
        files = await conn.fetch("SELECT channel_message_id, file_type, caption FROM files WHERE batch_id = $1::uuid ORDER BY id ASC", str(batch_uuid))
    if not files:
        await bot.send_message(chat_id=message.chat.id, text="No files found for this link.")
        return

    protect = link["protect_content"]
    if message.from_user.id == link["owner_id"]:
        protect = False

    sent_message_ids = []
    for f in files:
        try:
            channel_msg_id = f["channel_message_id"]
            copied = await bot.copy_message(chat_id=message.chat.id, from_chat_id=UPLOAD_CHANNEL_ID, message_id=channel_msg_id)
            sent_message_ids.append(copied.message_id)
            # small pause
            await asyncio.sleep(0.05)
        except Exception as e:
            log.exception("Failed to copy file to user: %s", e)

    auto_minutes = link["auto_delete_minutes"]
    expire_at = None
    if auto_minutes and auto_minutes > 0:
        expire_at = datetime.now(timezone.utc) + timedelta(minutes=auto_minutes)
        info = await bot.send_message(chat_id=message.chat.id, text=f"⚠️ These files will be auto-deleted from this chat in {auto_minutes} minutes.")
        sent_message_ids.append(info.message_id)

    async with DB_POOL.acquire() as conn:
        await conn.execute("""
            INSERT INTO deliveries (batch_id, to_user_id, delivered_at, expire_at, message_ids, deleted)
            VALUES ($1::uuid, $2, now(), $3, $4::jsonb, FALSE)
        """, str(batch_uuid), message.from_user.id, expire_at, json.dumps(sent_message_ids))


@dp.message_handler(commands=['start'])
def cmd_start_v2(message: types.Message):
    # This handler will still receive payloads in message.text like "/start <payload>" for deep-links
    parts = message.text.split(maxsplit=1)
    if len(parts) > 1:
        payload = parts[1].strip()
        asyncio.ensure_future(_handle_deep_link_async(message, payload))
        # update user touched
        asyncio.ensure_future(touch_user(message.from_user.id))
        return
    # normal start
    async def _normal():
        await upsert_user(message.from_user.id)
        setting = await get_message_setting("start")
        text = setting.get("text") or "Welcome!"
        image = setting.get("image_file_id")
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Help", callback_data="show_help"))
        if image:
            try:
                await bot.send_photo(chat_id=message.chat.id, photo=image, caption=text, reply_markup=kb)
                return
            except Exception:
                pass
        await bot.send_message(chat_id=message.chat.id, text=text, reply_markup=kb)
    asyncio.ensure_future(_normal())

# help callback
@dp.callback_query_handler(lambda c: c.data == "show_help")
def callback_show_help_v2(query):
    # reuse /help handler logic
    async def _reply_help():
        await touch_user(query.from_user.id)
        setting = await get_message_setting("help")
        text = setting.get("text") or "Help"
        image = setting.get("image_file_id")
        if image:
            try:
                await bot.send_photo(chat_id=query.message.chat.id, photo=image, caption=text)
                return
            except Exception:
                pass
        await bot.send_message(chat_id=query.message.chat.id, text=text)
    asyncio.ensure_future(_reply_help())
    try:
        query.answer()
    except Exception:
        pass

# -------------------------
# Deliveries sweeper (v2)
# -------------------------
async def deliveries_sweeper_loop(stop_event: asyncio.Event):
    log.info("Deliveries sweeper started (v2)")
    while not stop_event.is_set():
        try:
            async with DB_POOL.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT id, to_user_id, message_ids
                    FROM deliveries
                    WHERE expire_at IS NOT NULL AND expire_at <= now() AND deleted = FALSE
                    FOR UPDATE SKIP LOCKED
                """)
                for r in rows:
                    did = r["id"]
                    to_user = r["to_user_id"]
                    message_ids = r["message_ids"] or []
                    for mid in message_ids:
                        try:
                            await bot.delete_message(chat_id=to_user, message_id=int(mid))
                        except TelegramBadRequest:
                            log.debug("Could not delete %s:%s", to_user, mid)
                        except Exception as e:
                            log.exception("Error deleting %s:%s -> %s", to_user, mid, e)
                    await conn.execute("UPDATE deliveries SET deleted = TRUE WHERE id = $1", did)
        except Exception as e:
            log.exception("Deliveries sweeper failed: %s", e)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            continue
    log.info("Deliveries sweeper stopped (v2)")

# -------------------------
# Webhook & aiohttp glue (v2)
# -------------------------
# We'll use aiogram's start_webhook (v2) to integrate with aiohttp.
WEBHOOK_FULL_URL = None
if RENDER_EXTERNAL_URL:
    WEBHOOK_FULL_URL = RENDER_EXTERNAL_URL.rstrip("/") + WEBHOOK_PATH
elif os.getenv("WEBHOOK_URL"):
    WEBHOOK_FULL_URL = os.getenv("WEBHOOK_URL")

# health route handler for aiohttp; aiogram's start_webhook will create its own webserver
async def health_handler(request):
    return web.Response(text="ok")

# When using aiogram v2's start_webhook, provide on_startup/on_shutdown callbacks that set up DB and sweeper.
async def on_startup_webhook(dp):
    log.info("on_startup_webhook: initializing DB")
    await init_db()
    # start sweeper
    stop_event = asyncio.Event()
    # store in global-ish place: attach to dp for v2
    dp.sweeper_stop_event = stop_event
    dp.sweeper_task = asyncio.ensure_future(deliveries_sweeper_loop(stop_event))
    # set webhook if provided
    if USE_WEBHOOK and WEBHOOK_FULL_URL:
        try:
            await bot.set_webhook(WEBHOOK_FULL_URL)
            log.info("Webhook set to %s", WEBHOOK_FULL_URL)
        except Exception as e:
            log.exception("Failed to set webhook: %s", e)
    elif not USE_WEBHOOK:
        # Start polling in background (if chosen)
        log.info("Starting polling in background (USE_WEBHOOK=false)")
        dp.polling_task = asyncio.ensure_future(dp.start_polling())

async def on_shutdown_webhook(dp):
    log.info("on_shutdown_webhook: cleaning up")
    # stop sweeper
    if getattr(dp, "sweeper_stop_event", None):
        dp.sweeper_stop_event.set()
        try:
            await dp.sweeper_task
        except Exception:
            pass
    # cancel polling if exists
    if getattr(dp, "polling_task", None):
        dp.polling_task.cancel()
        try:
            await dp.polling_task
        except Exception:
            pass
    if USE_WEBHOOK and WEBHOOK_FULL_URL:
        try:
            await bot.delete_webhook()
        except Exception:
            pass
    try:
        await bot.session.close()
    except Exception:
        pass
    if DB_POOL:
        await DB_POOL.close()
    log.info("Shutdown finished")

# -------------------------
# Entrypoint for v2 on Render
# -------------------------
def run_app_v2():
    # aiogram v2's start_webhook needs webhook_path and host/port
    webhook_path = WEBHOOK_PATH
    # host & port:
    host = "0.0.0.0"
    port = int(os.getenv("PORT", PORT))

    # we also create a small aiohttp app for the health route (aiogram will mount its own web app)
    from aiohttp import web as _web

    async def _health(request):
        return _web.Response(text="ok")

    _web_app = _web.Application()
    _web_app.router.add_get("/", _health)
    _web_app.router.add_get("/health", _health)

    # start_webhook will create and run its own web server; we just call it.
    # Provide the on_startup and on_shutdown hooks we defined.
    log.info("Starting aiogram v2 webhook on %s:%s%s", host, port, webhook_path)
    # Note: start_webhook has signature: start_webhook(dispatcher, webhook_path, skip_updates, host, port, on_startup, on_shutdown)
    executor.start_webhook(dispatcher=dp,
                           webhook_path=webhook_path,
                           skip_updates=True,
                           host=host,
                           port=port,
                           on_startup=on_startup_webhook,
                           on_shutdown=on_shutdown_webhook)

if __name__ == "__main__":
    run_app_v2()