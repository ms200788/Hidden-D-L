# =====================================
# Telegram Bot (Webhook-only for Render)
# Features: Uploads, Deep-linking, Stats, Broadcast, Sessions, Auto-delete
# Database: Neon (Postgres via asyncpg)
# Hosting: Render (aiohttp web server + UptimeRobot healthcheck)
# =====================================

import os
import logging
import asyncio
import secrets
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple, Any

from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ParseMode,
    Message,
)
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.executor import start_webhook

import asyncpg
from aiohttp import web

# -------------------------------------
# Logging Setup
# -------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------------------------
# Environment Variables
# -------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL", "")
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))

WEBHOOK_HOST = os.getenv("RENDER_EXTERNAL_URL", "http://localhost:8000")
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.getenv("PORT", "8080"))

# -------------------------------------
# Bot + Dispatcher
# -------------------------------------
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# -------------------------------------
# Database Layer
# -------------------------------------
class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.pool.Pool] = None

    async def connect(self):
        logger.info("Connecting to database...")
        self.pool = await asyncpg.create_pool(dsn=self.dsn)
        logger.info("Connected to database")
        await self._init_schema()

    async def _init_schema(self):
        async with self.pool.acquire() as conn:
            # Users table
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                joined TIMESTAMP DEFAULT NOW(),
                last_active TIMESTAMP DEFAULT NOW()
            )
            """)
            # Files table
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                file_id TEXT PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                file_unique_id TEXT,
                file_type TEXT,
                caption TEXT,
                created TIMESTAMP DEFAULT NOW()
            )
            """)
            # Broadcast sessions
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS broadcasts (
                id SERIAL PRIMARY KEY,
                started TIMESTAMP DEFAULT NOW(),
                finished TIMESTAMP,
                total_users INT,
                success INT,
                failed INT
            )
            """)
            # Settings table
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """)

    async def add_or_update_user(self, user: types.User):
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO users (user_id, username, first_name, last_name, joined, last_active)
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            ON CONFLICT (user_id) DO UPDATE
            SET username=EXCLUDED.username,
                first_name=EXCLUDED.first_name,
                last_name=EXCLUDED.last_name,
                last_active=NOW()
            """, user.id, user.username, user.first_name, user.last_name)

    async def add_file(self, file_id: str, file_unique_id: str, file_type: str, user_id: int, caption: Optional[str]):
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO files (file_id, user_id, file_unique_id, file_type, caption)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (file_id) DO NOTHING
            """, file_id, user_id, file_unique_id, file_type, caption)

    async def get_user_count(self) -> int:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COUNT(*) AS c FROM users")
            return row["c"]

    async def get_file_count(self) -> int:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COUNT(*) AS c FROM files")
            return row["c"]

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None


# Global DB instance
db = Database(DATABASE_URL)

# -------------------------------------
# Utility Functions
# -------------------------------------
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def build_channel_buttons(optional_list: List[Dict[str, str]], forced_list: List[Dict[str, str]]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    for ch in forced_list + optional_list:
        kb.add(InlineKeyboardButton(text=ch["title"], url=ch["url"]))
    return kb

def generate_token(length: int = 8) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# -------------------------------------
# FSM States (will be expanded later)
# -------------------------------------
class BroadcastStates(StatesGroup):
    waiting_for_message = State()
    confirm = State()

class UploadStates(StatesGroup):
    waiting_for_file = State()
    waiting_for_caption = State()

# =========================
# BLOCK 2: Handlers & Flows
# (paste after BLOCK 1)
# =========================

from functools import partial

# -------------------------
# Small helpers (DB direct SQL helpers)
# -------------------------
async def db_get_setting(key: str) -> Optional[str]:
    assert db.pool is not None
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return row["value"] if row else None

async def db_set_setting(key: str, value: str):
    assert db.pool is not None
    async with db.pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO settings(key, value) VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, key, value)

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
    await db.add_or_update_user(message.from_user)

    # check payload (deep link)
    parts = message.get_args().strip() if hasattr(message, "get_args") else ""
    if parts:
        await handle_deep_link_start(message, parts)
        return

    # no payload: show start message from settings (if any)
    start_text = await db_get_setting("start_text")
    start_image = await db_get_setting("start_image_file_id")
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
    await db.add_or_update_user(callback_query.from_user)
    help_text = await db_get_setting("help_text")
    help_img = await db_get_setting("help_image_file_id")
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
    await db.add_or_update_user(message.from_user)
    help_text = await db_get_setting("help_text")
    help_img = await db_get_setting("help_image_file_id")
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
    key = "start_image_file_id" if kind == "start" else "help_image_file_id"
    await db_set_setting(key, file_id)
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
            copied = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            # fallback send
            if ftype == "photo":
                copied = await bot.send_photo(chat_id=UPLOAD_CHANNEL_ID, photo=file_id, caption=caption)
            elif ftype == "video":
                copied = await bot.send_video(chat_id=UPLOAD_CHANNEL_ID, video=file_id, caption=caption)
            elif ftype == "audio":
                copied = await bot.send_audio(chat_id=UPLOAD_CHANNEL_ID, audio=file_id, caption=caption)
            elif ftype == "document":
                copied = await bot.send_document(chat_id=UPLOAD_CHANNEL_ID, document=file_id, caption=caption)
            elif ftype == "animation":
                copied = await bot.send_animation(chat_id=UPLOAD_CHANNEL_ID, animation=file_id, caption=caption)
            elif ftype == "voice":
                copied = await bot.send_voice(chat_id=UPLOAD_CHANNEL_ID, voice=file_id)
            else:
                copied = await bot.send_document(chat_id=UPLOAD_CHANNEL_ID, document=file_id, caption=caption)
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
    token = generate_token(12)
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
    deep_link = f"https://t.me/{(await bot.get_me()).username}?start={token}"
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
    await BroadcastStates.waiting_for_message.set()

@dp.message_handler(state=BroadcastStates.waiting_for_message, commands=["cancel"])
async def cancel_broadcast(message: types.Message, state: FSMContext):
    await state.finish()
    await message.reply("Broadcast cancelled.")

@dp.message_handler(state=BroadcastStates.waiting_for_message, content_types=types.ContentTypes.ANY)
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
    users = await db.get_user_count()
    files = await db.get_file_count()
    text = (
        f"📊 <b>Bot Stats</b>\n\n"
        f"• Total users: <b>{users}</b>\n"
        f"• Total files: <b>{files}</b>\n"
    )
    await bot.send_message(message.chat.id, text, parse_mode=ParseMode.HTML)

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

# =========================
# BLOCK 3: Startup, webhook wiring, utilities, and entrypoint
# (paste this after BLOCK 2)
# =========================

import signal
from aiohttp import web_exceptions

# A safety default for skipping updates
SKIP_UPDATES = True

# -------------------------
# Misc helpers & fallback handlers
# -------------------------
@dp.message_handler(commands=["cancel"])
async def cmd_cancel(message: types.Message, state: FSMContext):
    try:
        await state.finish()
    except Exception:
        pass
    await message.reply("Operation cancelled.")

@dp.message_handler()
async def fallback_all(message: types.Message):
    """
    Minimal fallback: update/insert user into DB last_active and do not spam replies.
    """
    try:
        await db.add_or_update_user(message.from_user)
    except Exception as e:
        logger.debug("Failed updating user last_active: %s", e)
    # do not reply to ordinary messages to avoid loops
    return

# Error logger for unhandled exceptions in handlers
@dp.errors_handler()
async def global_error_handler(update, exception):
    logger.exception("Unhandled error: %s", exception)
    # Returning True tells aiogram exceptions have been handled
    return True

# -------------------------
# Health endpoint for UptimeRobot / Render
# -------------------------
async def health(request: web.Request):
    # lightweight DB check optional
    try:
        if db.pool:
            async with db.pool.acquire() as conn:
                await conn.fetchrow("SELECT 1")
    except Exception as e:
        logger.warning("DB healthcheck failed: %s", e)
        # still respond 200 so Render/UptimeRobot sees service alive; change if you want stricter
    return web.Response(text="ok")

# -------------------------
# Startup & Shutdown for webhook + DB
# -------------------------
async def on_startup(app: web.Application):
    logger.info("on_startup: connecting to DB...")
    try:
        await db.connect()
    except Exception as e:
        logger.exception("Failed to initialize DB on startup: %s", e)
        # If DB init fails, we still continue to try to set webhook (optional)
    # Ensure sessions tables exist (safe no-op if already created)
    try:
        await ensure_sessions_tables()
    except Exception:
        logger.debug("ensure_sessions_tables skipped or failed (maybe already exist).")
    # set webhook if RENDER_EXTERNAL_URL provided
    try:
        if WEBHOOK_URL:
            await bot.set_webhook(WEBHOOK_URL)
            logger.info("Webhook set to %s", WEBHOOK_URL)
        else:
            logger.warning("WEBHOOK_URL not configured; webhook not set.")
    except Exception as e:
        logger.exception("Failed to set webhook: %s", e)

async def on_shutdown(app: web.Application):
    logger.info("on_shutdown: cleaning up.")
    try:
        # try deleting webhook
        try:
            await bot.delete_webhook()
            logger.info("Webhook deleted.")
        except Exception as e:
            logger.debug("Failed to delete webhook: %s", e)
        # close DB
        await db.close()
    except Exception as e:
        logger.exception("Error during shutdown: %s", e)
    # close dispatcher storage
    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
    except Exception:
        pass
    # close bot session
    try:
        await bot.close()
    except Exception:
        pass
    logger.info("Shutdown complete.")

# -------------------------
# Graceful stop helper (for local dev)
# -------------------------
def _register_signal_handlers(loop):
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, lambda s=s: asyncio.create_task(_shutdown_signal(s)))
        except NotImplementedError:
            # Windows or environment may not support signal handlers
            pass

async def _shutdown_signal(sig):
    logger.info("Received exit signal %s", sig)
    # Stop webhook server by stopping the event loop gracefully
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    logger.info("Cancelled %d tasks", len(tasks))

# -------------------------
# App creation using aiogram.start_webhook + aiohttp web_app injection
# -------------------------
def build_web_app() -> web.Application:
    app = web.Application()
    # Attach health endpoints
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    # Hook aiogram dispatcher into aiohttp app by providing web_app to start_webhook
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app

# -------------------------
# Entrypoint
# -------------------------
def main():
    # Build the aiohttp web app with health endpoints
    web_app = build_web_app()

    # register signal handlers for graceful shutdown where supported
    loop = asyncio.get_event_loop()
    _register_signal_handlers(loop)

    # Use aiogram's start_webhook which will run aiohttp under the hood
    # and mount our web_app by passing it into start_webhook's web_app parameter.
    try:
        logger.info("Starting webhook (host=%s port=%s path=%s)...", WEBAPP_HOST, WEBAPP_PORT, WEBHOOK_PATH)
        start_webhook(
            dispatcher=dp,
            webhook_path=WEBHOOK_PATH,
            skip_updates=SKIP_UPDATES,
            host=WEBAPP_HOST,
            port=WEBAPP_PORT,
            on_startup=[on_startup],
            on_shutdown=[on_shutdown],
            webhook_url=WEBHOOK_URL,
            web_app=web_app
        )
    except web_exceptions.HTTPException as e:
        logger.exception("HTTP exception while starting webhook: %s", e)
    except Exception as e:
        logger.exception("Failed to start webhook: %s", e)

if __name__ == "__main__":
    main()