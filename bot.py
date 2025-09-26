# bot.py
"""
Fixed Telegram upload + persistent bot for Render + Neon.
Features:
- Neon/Postgres metadata (asyncpg)
- Upload channel storage (bot copies files to private channel and stores channel_message_id)
- Owner-only upload/broadcast/stats/setimage/setmessage
- Users: /start, /help, deep link access t.me/YourBot?start=<batch_uuid>
- Auto-delete of delivered messages using DB-backed schedule and sweeper task that survives restarts
- Webhook + aiohttp server (binds to PORT) for Render; polling fallback when USE_WEBHOOK=false
"""

import os
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import uuid
import signal

import asyncpg
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.filters import Command, Text
from aiogram.exceptions import TelegramBadRequest

# -------------------------
# Configuration from env
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))

# Webhook config
USE_WEBHOOK = os.getenv("USE_WEBHOOK", "true").lower() in ("1", "true", "yes")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")  # default webhook path
# prefer RENDER_EXTERNAL_URL, else WEBHOOK_BASE or WEBHOOK_URL
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("WEBHOOK_BASE") or os.getenv("WEBHOOK_URL")

PORT = int(os.getenv("PORT", "8080"))

if not BOT_TOKEN or not DATABASE_URL or not OWNER_ID or not UPLOAD_CHANNEL_ID:
    raise RuntimeError("Required env vars missing: BOT_TOKEN, DATABASE_URL, OWNER_ID, UPLOAD_CHANNEL_ID")

# -------------------------
# Logging
# -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# -------------------------
# Bot & Dispatcher
# -------------------------
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

# -------------------------
# Database (asyncpg)
# -------------------------
DB_POOL: Optional[asyncpg.pool.Pool] = None

CREATE_TABLES_SQL = """
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
    type VARCHAR(10) PRIMARY KEY,
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

async def init_db():
    global DB_POOL
    if DB_POOL:
        return
    log.info("Connecting to DB...")
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
    log.info("DB initialized.")

# -------------------------
# DB utility functions
# -------------------------
async def upsert_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, first_seen, last_active, sessions)
            VALUES ($1, now(), now(), 1)
            ON CONFLICT (user_id) DO
              UPDATE SET last_active = now(), sessions = users.sessions + 1
        """, user_id)

async def touch_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, first_seen, last_active, sessions)
            VALUES ($1, now(), now(), 1)
            ON CONFLICT (user_id) DO
              UPDATE SET last_active = now()
        """, user_id)

async def save_message_setting(type_: str, text: Optional[str], image_file_id: Optional[str]):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
            INSERT INTO messages (type, text, image_file_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (type) DO UPDATE SET text = EXCLUDED.text, image_file_id = EXCLUDED.image_file_id
        """, type_, text, image_file_id)

async def get_message_setting(type_: str):
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT text, image_file_id FROM messages WHERE type = $1", type_)
        return dict(row) if row else {"text": "", "image_file_id": None}

# -------------------------
# In-memory upload session state (owner only)
# -------------------------
UPLOAD_SESSIONS: Dict[int, Dict[str, Any]] = {}

def start_upload_session(owner_id: int):
    batch = str(uuid.uuid4())
    UPLOAD_SESSIONS[owner_id] = {"batch_id": batch, "collected": [], "finished": False}
    return UPLOAD_SESSIONS[owner_id]

def add_to_session(owner_id: int, item: Dict[str, Any]):
    UPLOAD_SESSIONS.setdefault(owner_id, {"batch_id": str(uuid.uuid4()), "collected": [], "finished": False})
    UPLOAD_SESSIONS[owner_id]["collected"].append(item)

def end_upload_session(owner_id: int):
    if owner_id not in UPLOAD_SESSIONS:
        return None
    UPLOAD_SESSIONS[owner_id]["finished"] = True
    return UPLOAD_SESSIONS[owner_id]

def cancel_upload_session(owner_id: int):
    return UPLOAD_SESSIONS.pop(owner_id, None)

# -------------------------
# Helper: forward and record file message to upload channel
# -------------------------
async def forward_and_record(batch_id: str, msg: Message):
    forwarded = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=msg.chat.id, message_id=msg.message_id)
    channel_msg_id = forwarded.message_id
    file_type = "other"
    if msg.photo:
        file_type = "photo"
    elif msg.video:
        file_type = "video"
    elif msg.document:
        file_type = "document"
    elif msg.audio:
        file_type = "audio"
    caption = msg.caption or None
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
@dp.message(Command("start"))
async def cmd_start(message: Message, command: Command):
    args = command.args or ""
    if args:
        await touch_user(message.from_user.id)
        await handle_deep_link(message, args)
        return

    await upsert_user(message.from_user.id)
    setting = await get_message_setting("start")
    text = setting.get("text") or "Welcome!"
    image = setting.get("image_file_id")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Help", callback_data="show_help")]])
    if image:
        try:
            await bot.send_photo(chat_id=message.chat.id, photo=image, caption=text, reply_markup=keyboard)
            return
        except TelegramBadRequest:
            pass
    await message.answer(text, reply_markup=keyboard)

@dp.message(Command("help"))
async def cmd_help(message: Message):
    await touch_user(message.from_user.id)
    setting = await get_message_setting("help")
    text = setting.get("text") or "Help"
    image = setting.get("image_file_id")
    if image:
        try:
            await bot.send_photo(chat_id=message.chat.id, photo=image, caption=text)
            return
        except TelegramBadRequest:
            pass
    await message.answer(text)

@dp.callback_query(Text("show_help"))
async def callback_show_help(query: types.CallbackQuery):
    await query.answer()
    # reuse cmd_help semantics
    message = query.message
    # Create a fake Message-like object for cmd_help compatibility
    class _M:
        chat = message.chat
        from_user = message.from_user
    await cmd_help(_M())

# ---------- setimage ----------
@dp.message(Command("setimage"))
async def cmd_setimage(message: Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner can use this.")
        return
    if not message.reply_to_message or not (message.reply_to_message.photo or message.reply_to_message.document):
        await message.reply("Usage: reply to a photo (or document) with /setimage")
        return

    # extract stable file_id immediately from replied message
    file_ref = None
    if message.reply_to_message.photo:
        file_ref = message.reply_to_message.photo[-1].file_id
    elif message.reply_to_message.document:
        file_ref = message.reply_to_message.document.file_id

    # ask which to set
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton("Set as START image", callback_data=f"setimg:start:{file_ref}")],
        [InlineKeyboardButton("Set as HELP image", callback_data=f"setimg:help:{file_ref}")]
    ])
    await message.reply("Choose where to set this image:", reply_markup=kb)

@dp.callback_query(Text(startswith="setimg:"))
async def callback_setimg(query: types.CallbackQuery):
    if not is_owner(query.from_user.id):
        await query.answer("Only owner.")
        return
    try:
        _, typ, file_ref = query.data.split(":", 2)
    except Exception:
        await query.answer("Invalid data.")
        return
    # Save into messages table
    await save_message_setting(typ, None, file_ref)
    await query.answer(f"{typ} image saved.")
    try:
        await query.message.edit_text(f"{typ} image updated.")
    except Exception:
        pass

# ---------- setmessage ----------
@dp.message(Command("setmessage"))
async def cmd_setmessage(message: Message, command: Command):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner can use this.")
        return
    args = command.args or ""
    if args:
        parts = args.split(None, 1)
        if parts[0] in ("start", "help") and len(parts) > 1:
            typ = parts[0]
            text = parts[1]
            await save_message_setting(typ, text, None)
            await message.reply(f"{typ} message updated.")
            return
    if message.reply_to_message and message.reply_to_message.text:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton("Set as START text", callback_data=f"setmsg:start")],
            [InlineKeyboardButton("Set as HELP text", callback_data=f"setmsg:help")]
        ])
        UPLOAD_SESSIONS[message.from_user.id] = UPLOAD_SESSIONS.get(message.from_user.id, {})
        UPLOAD_SESSIONS[message.from_user.id]["pending_text"] = message.reply_to_message.text
        await message.reply("Choose where to set this text:", reply_markup=kb)
        return
    await message.reply("Usage: reply to a text message with /setmessage, or use /setmessage start <text>")

@dp.callback_query(Text(startswith="setmsg:"))
async def callback_setmsg(query: types.CallbackQuery):
    if not is_owner(query.from_user.id):
        await query.answer("Only owner.")
        return
    typ = query.data.split(":")[1]
    pending = UPLOAD_SESSIONS.get(query.from_user.id, {}).get("pending_text")
    if not pending:
        await query.answer("No pending text found. Reply to a message with /setmessage and try again.")
        return
    await save_message_setting(typ, pending, None)
    UPLOAD_SESSIONS.get(query.from_user.id, {}).pop("pending_text", None)
    await query.answer(f"{typ} text saved.")
    try:
        await query.message.edit_text(f"{typ} text updated.")
    except Exception:
        pass

# ---------- stats ----------
@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner can use this.")
        return
    async with DB_POOL.acquire() as conn:
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
        active_2d = await conn.fetchval("SELECT COUNT(*) FROM users WHERE last_active >= now() - INTERVAL '2 days'")
        total_files = await conn.fetchval("SELECT COUNT(*) FROM files")
        total_sessions = await conn.fetchval("SELECT SUM(sessions) FROM users")
    await message.reply(
        f"Users: {total_users}\nActive (2d): {active_2d}\nFiles: {total_files}\nTotal sessions: {total_sessions or 0}"
    )

# ---------- broadcast ----------
@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner can use this.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to the message you want to broadcast with /broadcast")
        return
    async with DB_POOL.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
    total = len(rows)
    await message.reply(f"Broadcasting to {total} users...")
    sent = 0
    for r in rows:
        uid = r["user_id"]
        try:
            await bot.copy_message(chat_id=uid, from_chat_id=message.reply_to_message.chat.id, message_id=message.reply_to_message.message_id)
            sent += 1
        except Exception as e:
            log.warning("Broadcast to %s failed: %s", uid, e)
            # continue
    await message.reply(f"Broadcast complete. Sent: {sent}/{total}")

# -------------------------
# Upload flow (owner)
# -------------------------
@dp.message(Command("upload"))
async def cmd_upload(message: Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner.")
        return
    start_upload_session(message.from_user.id)
    await message.reply("Upload session started. Send files (photos, videos, documents) in as many messages as you want.\nWhen finished send /e. To cancel send /d.")

@dp.message(Command("d"))
async def cmd_cancel_upload(message: Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner.")
        return
    canceled = cancel_upload_session(message.from_user.id)
    if canceled:
        await message.reply("Upload session cancelled.")
    else:
        await message.reply("No active upload session.")

@dp.message(Command("e"))
async def cmd_end_upload(message: Message):
    if not is_owner(message.from_user.id):
        await message.reply("Only owner.")
        return
    sess = end_upload_session(message.from_user.id)
    if not sess:
        await message.reply("No active upload session.")
        return
    batch_id = sess["batch_id"]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton("Protect ON (disable forward/save)", callback_data=f"upload_protect:on:{batch_id}")],
        [InlineKeyboardButton("Protect OFF", callback_data=f"upload_protect:off:{batch_id}")]
    ])
    await message.reply("Protect content?", reply_markup=kb)

@dp.callback_query(Text(startswith="upload_protect:"))
async def callback_upload_protect(query: types.CallbackQuery):
    if not is_owner(query.from_user.id):
        await query.answer("Only owner.")
        return
    _, val, batch_id = query.data.split(":")
    sess = UPLOAD_SESSIONS.get(query.from_user.id)
    if not sess or sess["batch_id"] != batch_id:
        await query.answer("Upload session not found.")
        return
    UPLOAD_SESSIONS[query.from_user.id]["protect_content"] = (val == "on")
    await query.message.edit_text("Set auto-delete timer in minutes (0 to 10080). Send a number as a message in chat, or reply '0' to disable.")
    await query.answer()

@dp.message(lambda message: is_owner(message.from_user.id) and message.text and message.text.isdigit())
async def handle_auto_delete_input(message: Message):
    sess = UPLOAD_SESSIONS.get(message.from_user.id)
    if not sess or "protect_content" not in sess:
        return
    minutes = int(message.text)
    if minutes < 0 or minutes > 10080:
        await message.reply("Invalid value. Must be 0..10080.")
        return
    batch_id = sess["batch_id"]
    protect = sess.get("protect_content", False)
    collected = sess.get("collected", [])
    if not collected:
        await message.reply("No files were uploaded in this session.")
        cancel_upload_session(message.from_user.id)
        return
    # forward each collected original message (from owner chat) to upload channel and store DB records
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
    deep_link = f"https://t.me/{(await bot.get_me()).username}?start={batch_id}"
    cancel_upload_session(message.from_user.id)
    await message.reply(f"Upload saved. Deep link:\n{deep_link}")

@dp.message(lambda message: message.chat.type == "private")
async def capture_owner_uploads(message: Message):
    if not is_owner(message.from_user.id):
        return
    sess = UPLOAD_SESSIONS.get(message.from_user.id)
    if not sess or sess.get("finished"):
        return
    if message.photo or message.video or message.document or message.audio:
        data = {
            "chat_id": message.chat.id,
            "message_id": message.message_id,
            "file_type": "photo" if message.photo else ("video" if message.video else ("document" if message.document else ("audio" if message.audio else "other"))),
            "caption": message.caption or None
        }
        add_to_session(message.from_user.id, data)
        await message.reply("File recorded for upload session. Send more or /e to finish, /d to cancel.")

# -------------------------
# Deep-link handling
# -------------------------
async def handle_deep_link(message: Message, start_payload: str):
    try:
        batch_uuid = uuid.UUID(start_payload)
    except Exception:
        await message.reply("Invalid link.")
        return
    async with DB_POOL.acquire() as conn:
        link = await conn.fetchrow("SELECT * FROM links WHERE batch_id = $1::uuid AND active = TRUE", str(batch_uuid))
        if not link:
            await message.reply("This link is not active or doesn't exist.")
            return
        files = await conn.fetch("SELECT channel_message_id, file_type, caption FROM files WHERE batch_id = $1::uuid ORDER BY id ASC", str(batch_uuid))
    if not files:
        await message.reply("No files found for this link.")
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
        except Exception as e:
            log.exception("Failed to copy file to user: %s", e)

    auto_minutes = link["auto_delete_minutes"]
    expire_at = None
    if auto_minutes and auto_minutes > 0:
        expire_at = datetime.now(timezone.utc) + timedelta(minutes=auto_minutes)
        info = await bot.send_message(chat_id=message.chat.id,
                                      text=f"⚠️ These files will be auto-deleted from this chat in {auto_minutes} minutes.")
        sent_message_ids.append(info.message_id)

    async with DB_POOL.acquire() as conn:
        await conn.execute("""
            INSERT INTO deliveries (batch_id, to_user_id, delivered_at, expire_at, message_ids, deleted)
            VALUES ($1::uuid, $2, now(), $3, $4::jsonb, FALSE)
        """, str(batch_uuid), message.from_user.id, expire_at, json.dumps(sent_message_ids))

# -------------------------
# Deliveries sweeper
# -------------------------
async def deliveries_sweeper_task(stop_event: asyncio.Event):
    # stop_event lets us exit gracefully
    log.info("Deliveries sweeper started")
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
                        except TelegramBadRequest as e:
                            log.debug("Delete failed for %s:%s -> %s", to_user, mid, e)
                        except Exception as e:
                            log.exception("Failed to delete message %s for %s: %s", mid, to_user, e)
                    await conn.execute("UPDATE deliveries SET deleted = TRUE WHERE id = $1", did)
        except Exception as e:
            log.exception("Deliveries sweeper failed: %s", e)
        # sleep in small increments to allow quick shutdown
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            continue
    log.info("Deliveries sweeper stopped")

# -------------------------
# Startup / Shutdown for aiohttp web app
# -------------------------
async def on_startup(app: web.Application):
    log.info("on_startup: initializing DB and background tasks")
    await init_db()
    # start sweeper task
    stop_event = asyncio.Event()
    app["sweeper_stop_event"] = stop_event
    app["sweeper_task"] = asyncio.create_task(deliveries_sweeper_task(stop_event))
    # set webhook if configured
    if USE_WEBHOOK:
        # build webhook URL from RENDER_EXTERNAL_URL or WEBHOOK_BASE
        if RENDER_EXTERNAL_URL:
            webhook_base = RENDER_EXTERNAL_URL.rstrip("/")
            webhook_url = f"{webhook_base}{WEBHOOK_PATH}"
        else:
            webhook_url = os.getenv("WEBHOOK_URL") or os.getenv("WEBHOOK_BASE") or None
            if webhook_url and not webhook_url.startswith("http"):
                webhook_url = None
        if webhook_url:
            try:
                await bot.set_webhook(webhook_url)
                log.info("Webhook set to %s", webhook_url)
            except Exception as e:
                log.exception("Failed to set webhook: %s", e)
        else:
            log.warning("No webhook URL provided; webhook not set. Set RENDER_EXTERNAL_URL or WEBHOOK_URL env var.")
    else:
        # If not using webhook, start polling in background
        log.info("USE_WEBHOOK is false; starting polling in background")
        app["polling_task"] = asyncio.create_task(dp.start_polling(bot))

async def on_shutdown(app: web.Application):
    log.info("on_shutdown: shutting down")
    # stop sweeper
    stop_event: Optional[asyncio.Event] = app.get("sweeper_stop_event")
    if stop_event:
        stop_event.set()
    sweeper_task: Optional[asyncio.Task] = app.get("sweeper_task")
    if sweeper_task:
        await sweeper_task
    polling_task: Optional[asyncio.Task] = app.get("polling_task")
    if polling_task:
        polling_task.cancel()
        try:
            await polling_task
        except Exception:
            pass
    # delete webhook if set
    if USE_WEBHOOK:
        try:
            await bot.delete_webhook()
        except Exception:
            pass
    # close bot and db
    try:
        await bot.session.close()
    except Exception:
        pass
    if DB_POOL:
        await DB_POOL.close()
    log.info("Shutdown complete")

# -------------------------
# aiohttp webhook handlers & health
# -------------------------
async def handle_webhook(request: web.Request):
    try:
        data = await request.read()
        if not data:
            return web.Response(text="no data", status=400)
        body = json.loads(data.decode("utf-8"))
        update = types.Update.to_object(body)
        # process update
        await dp.process_update(types.Update(**body))
    except Exception as e:
        log.exception("Error in webhook handler: %s", e)
        # still return 200 so Telegram doesn't retry aggressively on parse errors; you might want to return 500 for debugging
        return web.Response(text="error", status=200)
    return web.Response(text="ok")

async def healthcheck(request: web.Request):
    return web.Response(text="ok")

# -------------------------
# Final app & run
# -------------------------
def build_and_run_app():
    app = web.Application()
    # routes
    app.router.add_get("/", healthcheck)
    app.router.add_get("/health", healthcheck)
    # webhook path: use WEBHOOK_PATH
    app.router.add_post(WEBHOOK_PATH, handle_webhook)

    # startup/shutdown
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_shutdown)

    # run app
    port = int(os.getenv("PORT", PORT))
    host = "0.0.0.0"
    log.info("Starting web app on %s:%s (WEBHOOK_PATH=%s)", host, port, WEBHOOK_PATH)
    web.run_app(app, host=host, port=port)

if __name__ == "__main__":
    try:
        build_and_run_app()
    except (KeyboardInterrupt, SystemExit):
        log.info("Exited by signal")