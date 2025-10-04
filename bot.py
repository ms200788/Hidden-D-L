#!/usr/bin/env python3
# main.py
"""
Render-deployable Telegram bot
- Uses python-telegram-bot (async)
- Uses asyncpg to connect to Neon Postgres
- Owner-only upload sessions; multi-file upload; deep links 64-128 chars
- Auto-delete delivered messages in user's chat after minutes (per session)
- Protect content (prevent forwarding/saving) for non-owner recipients (protect_content=True)
- Owner bypasses protection (protect_content=False)
- Uploads are forwarded/copied into UPLOAD_CHANNEL_ID (bot must be admin there)
"""

import os
import asyncio
import logging
import secrets
import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone

import asyncpg
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaDocument,
    MessageEntity,
    ParseMode,
)
from telegram.constants import ChatAction
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackQueryHandler,
    ConversationHandler,
)

# ---------- CONFIG ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))
# deep link token length (number of bytes for token_urlsafe -> roughly length*1.3)
DEEP_LINK_BYTES = 96  # produces a token ~ 128 chars (base64-url)
# ---------- LOGGING ----------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ---------- DB helpers ----------
class DB:
    pool: Optional[asyncpg.pool.Pool] = None

    @classmethod
    async def init(cls):
        if not DATABASE_URL:
            logger.error("DATABASE_URL not set")
            raise RuntimeError("DATABASE_URL not set")
        cls.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=6)
        # create tables
        async with cls.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    last_seen TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    id SERIAL PRIMARY KEY,
                    owner_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    ended BOOLEAN DEFAULT FALSE,
                    deep_link_token TEXT,
                    auto_delete_minutes INT DEFAULT NULL,
                    note TEXT
                );
                CREATE TABLE IF NOT EXISTS files (
                    id SERIAL PRIMARY KEY,
                    session_id INT REFERENCES sessions(id) ON DELETE CASCADE,
                    upload_channel_msg_id BIGINT NOT NULL,
                    upload_channel_chat_id BIGINT NOT NULL,
                    original_caption TEXT,
                    original_file_type TEXT,
                    original_file_id TEXT,
                    created_at TIMESTAMP NOT NULL
                );
                CREATE TABLE IF NOT EXISTS start_config (
                    id INT PRIMARY KEY DEFAULT 1,
                    welcome_text TEXT,
                    welcome_image_file_id TEXT
                );
                """
            )
            # ensure default start_config row
            r = await conn.fetchval("SELECT COUNT(*) FROM start_config;")
            if r == 0:
                await conn.execute(
                    "INSERT INTO start_config(id, welcome_text, welcome_image_file_id) VALUES (1, NULL, NULL);"
                )

    @classmethod
    async def add_or_update_user(cls, user_id, username, first_name, last_name):
        async with cls.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO users(user_id, username, first_name, last_name, last_seen)
                VALUES($1,$2,$3,$4,$5)
                ON CONFLICT (user_id) DO UPDATE SET
                  username = EXCLUDED.username,
                  first_name = EXCLUDED.first_name,
                  last_name = EXCLUDED.last_name,
                  last_seen = EXCLUDED.last_seen;
                """,
                user_id,
                username,
                first_name,
                last_name,
                datetime.utcnow(),
            )

    @classmethod
    async def new_session(cls, owner_id, auto_delete_minutes=None, note=None):
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO sessions(owner_id, created_at, auto_delete_minutes, note)
                VALUES ($1, $2, $3, $4) RETURNING *;
                """,
                owner_id,
                datetime.utcnow(),
                auto_delete_minutes,
                note,
            )
            return row

    @classmethod
    async def get_active_session_for_owner(cls, owner_id):
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM sessions WHERE owner_id=$1 AND ended=FALSE ORDER BY id DESC LIMIT 1;",
                owner_id,
            )
            return row

    @classmethod
    async def end_session(cls, session_id):
        async with cls.pool.acquire() as conn:
            await conn.execute(
                "UPDATE sessions SET ended=TRUE WHERE id=$1;", session_id
            )

    @classmethod
    async def add_file(cls, session_id, upload_channel_chat_id, upload_channel_msg_id, caption, file_type, file_id):
        async with cls.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO files(session_id, upload_channel_msg_id, upload_channel_chat_id, original_caption, original_file_type, original_file_id, created_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7);
                """,
                session_id,
                upload_channel_msg_id,
                upload_channel_chat_id,
                caption,
                file_type,
                file_id,
                datetime.utcnow(),
            )

    @classmethod
    async def set_session_deeplink(cls, session_id, token):
        async with cls.pool.acquire() as conn:
            await conn.execute(
                "UPDATE sessions SET deep_link_token=$1 WHERE id=$2;", token, session_id
            )

    @classmethod
    async def get_session_by_token(cls, token):
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM sessions WHERE deep_link_token=$1;", token)
            return row

    @classmethod
    async def get_files_for_session(cls, session_id):
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM files WHERE session_id=$1 ORDER BY id ASC;", session_id)
            return rows

    @classmethod
    async def get_stats(cls):
        async with cls.pool.acquire() as conn:
            total_users = await conn.fetchval("SELECT COUNT(*) FROM users;")
            two_days_ago = datetime.utcnow() - timedelta(days=2)
            active_users_2days = await conn.fetchval("SELECT COUNT(*) FROM users WHERE last_seen >= $1;", two_days_ago)
            total_files = await conn.fetchval("SELECT COUNT(*) FROM files;")
            total_sessions = await conn.fetchval("SELECT COUNT(*) FROM sessions;")
            return {
                "total_users": total_users,
                "active_2days": active_users_2days,
                "total_files": total_files,
                "total_sessions": total_sessions,
            }

    @classmethod
    async def set_start_config(cls, text: Optional[str], image_file_id: Optional[str]):
        async with cls.pool.acquire() as conn:
            await conn.execute(
                "UPDATE start_config SET welcome_text=$1, welcome_image_file_id=$2 WHERE id=1;",
                text,
                image_file_id,
            )

    @classmethod
    async def get_start_config(cls):
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT welcome_text, welcome_image_file_id FROM start_config WHERE id=1;")
            return row

# ---------- UTIL ----------
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def make_deep_link_token() -> str:
    # produce a long url-safe token ~ 100-140 chars
    return secrets.token_urlsafe(DEEP_LINK_BYTES)

# ---------- HANDLERS ----------
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    args = context.args or []
    # log/update user
    await DB.add_or_update_user(user.id, getattr(user, "username", None), getattr(user, "first_name", None), getattr(user, "last_name", None))

    # If start param is provided -> deep link
    if args:
        token = args[0]
        session = await DB.get_session_by_token(token)
        if not session:
            await update.message.reply_text("🔍 Invalid or expired link.")
            return
        # fetch files for session
        files = await DB.get_files_for_session(session["id"])
        if not files:
            await update.message.reply_text("⚠️ This link has no files.")
            return
        auto_delete = session["auto_delete_minutes"]
        owner_flag = is_owner(user.id)
        # For each file: copy from upload channel to user's chat (preserve caption)
        delivered_message_ids = []
        for f in files:
            try:
                # copy_message supports protect_content param
                copy = await context.bot.copy_message(
                    chat_id=update.effective_chat.id,
                    from_chat_id=f["upload_channel_chat_id"],
                    message_id=f["upload_channel_msg_id"],
                    caption=f["original_caption"] or None,
                    parse_mode=ParseMode.HTML,
                    protect_content=(not owner_flag),
                )
                delivered_message_ids.append(copy.message_id)
            except Exception as e:
                logger.exception("Failed to deliver file via deep link: %s", e)
                await update.message.reply_text("Failed to deliver some files.")
        # If auto_delete minutes set, schedule deletion of each delivered message in user's chat
        if auto_delete and int(auto_delete) > 0:
            minutes = int(auto_delete)
            await update.message.reply_text(f"⏳ Files will be deleted in {minutes} minutes.")
            # schedule deletion tasks
            for mid in delivered_message_ids:
                asyncio.create_task(schedule_delete(context.bot, update.effective_chat.id, mid, minutes))
        return

    # Normal start (no args) => send configured welcome
    cfg = await DB.get_start_config()
    welcome_text = cfg["welcome_text"]
    welcome_image_file_id = cfg["welcome_image_file_id"]
    if welcome_image_file_id and welcome_text:
        await context.bot.send_photo(chat_id=update.effective_chat.id, photo=welcome_image_file_id, caption=welcome_text, parse_mode=ParseMode.HTML)
    elif welcome_image_file_id:
        await context.bot.send_photo(chat_id=update.effective_chat.id, photo=welcome_image_file_id)
    elif welcome_text:
        # replace {first_name}
        text = welcome_text.replace("{first_name}", getattr(user, "first_name", "") or "")
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("Welcome! Use deep links to fetch files.")

async def schedule_delete(bot, chat_id: int, message_id: int, minutes: int):
    try:
        await asyncio.sleep(minutes * 60)
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.info("Could not delete message %s:%s (%s)", chat_id, message_id, e)

# ---------- OWNER UPLOAD FLOW ----------
# We'll use a simple pattern: owner runs /upload -> bot creates a session and instructs owner to send files
# Owner can set timer with /settimer <minutes> while session active
# Owner sends files (document, photo, audio, video, sticker, voice) - bot copies them to UPLOAD_CHANNEL_ID and stores references
# Owner runs /d to finalize (generate deep link) -- deep link token is created and saved to session
# Owner runs /e to end session without creating link (or after finalizing) -> session ended

async def upload_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("You are not allowed to use this.")
        return
    # create new session
    session = await DB.new_session(owner_id=update.effective_user.id)
    context.user_data["current_session_id"] = session["id"]
    await update.message.reply_text(
        "📦 Upload session started.\nSend files you want to include (documents, photos, videos). "
        "When done: send /d to generate deep link. To cancel/end session: /e\n"
        "You can set auto-delete for delivered user messages with /settimer <minutes> (optional)."
    )

async def settimer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("You are not allowed to use this.")
        return
    if "current_session_id" not in context.user_data:
        await update.message.reply_text("No active session. Start one with /upload")
        return
    try:
        minutes = int(context.args[0])
        if minutes < 0:
            raise ValueError()
    except Exception:
        await update.message.reply_text("Usage: /settimer <minutes>  — number of minutes (integer).")
        return
    # update DB session row
    async with DB.pool.acquire() as conn:
        await conn.execute("UPDATE sessions SET auto_delete_minutes=$1 WHERE id=$2;", minutes, context.user_data["current_session_id"])
    await update.message.reply_text(f"Auto-delete set to {minutes} minutes for this session.")

async def upload_file_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Only owner can upload
    if not is_owner(update.effective_user.id):
        # optionally allow users to send to bot? We'll ignore
        return
    if "current_session_id" not in context.user_data:
        await update.message.reply_text("No active session. Start one with /upload")
        return
    session_id = context.user_data["current_session_id"]
    # Accept document or photo or video or audio or voice
    message = update.message
    forwarded_msg = None
    caption = message.caption or ""
    file_type = "unknown"
    original_file_id = None

    try:
        if message.document:
            file_type = "document"
            # copy to upload channel as copy (so the bot stores file and bot will have message_id in channel)
            forwarded_msg = await context.bot.copy_message(
                chat_id=UPLOAD_CHANNEL_ID,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
                caption=caption or None,
                parse_mode=ParseMode.HTML,
            )
            original_file_id = message.document.file_id
        elif message.photo:
            file_type = "photo"
            # photo is an array, take largest
            forwarded_msg = await context.bot.copy_message(
                chat_id=UPLOAD_CHANNEL_ID,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
                caption=caption or None,
                parse_mode=ParseMode.HTML,
            )
            original_file_id = message.photo[-1].file_id
        elif message.video:
            file_type = "video"
            forwarded_msg = await context.bot.copy_message(
                chat_id=UPLOAD_CHANNEL_ID,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
                caption=caption or None,
                parse_mode=ParseMode.HTML,
            )
            original_file_id = message.video.file_id
        elif message.audio:
            file_type = "audio"
            forwarded_msg = await context.bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id, caption=caption or None)
            original_file_id = message.audio.file_id
        elif message.voice:
            file_type = "voice"
            forwarded_msg = await context.bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id, caption=caption or None)
            original_file_id = message.voice.file_id
        elif message.sticker:
            file_type = "sticker"
            forwarded_msg = await context.bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=message.chat.id, message_id=message.message_id)
            original_file_id = message.sticker.file_id
        else:
            await message.reply_text("Unsupported message type. Send documents, photos, videos, audio, voice, or stickers.")
            return

        # store file record
        await DB.add_file(session_id=session_id,
                          upload_channel_chat_id=forwarded_msg.chat.id,
                          upload_channel_msg_id=forwarded_msg.message_id,
                          caption=caption,
                          file_type=file_type,
                          file_id=original_file_id)
        await message.reply_text("✅ File added to session.")
    except Exception as e:
        logger.exception("Error while handling upload file: %s", e)
        await message.reply_text("Failed to copy file to upload channel. Make sure bot is admin in upload channel.")

async def done_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("You are not allowed to use this.")
        return
    if "current_session_id" not in context.user_data:
        await update.message.reply_text("No active session.")
        return
    session_id = context.user_data["current_session_id"]
    # check that session has files
    async with DB.pool.acquire() as conn:
        files_count = await conn.fetchval("SELECT COUNT(*) FROM files WHERE session_id=$1;", session_id)
    if files_count == 0:
        await update.message.reply_text("No files in this session. Upload some files first, or use /e to end.")
        return
    # generate deep link token
    token = make_deep_link_token()
    await DB.set_session_deeplink(session_id, token)
    # end the session
    await DB.end_session(session_id)
    # cleanup user_data
    context.user_data.pop("current_session_id", None)
    deep_link = f"https://t.me/{(await context.bot.get_me()).username}?start={token}"
    # we will provide a short, safe preview text but token is long
    await update.message.reply_text(
        "✅ Session finalized. Deep link has been generated.\n"
        f"Deep link:\n{deep_link}\n\n"
        "Anyone with this link can fetch the files. Share as needed."
    )

async def end_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("You are not allowed to use this.")
        return
    if "current_session_id" not in context.user_data:
        await update.message.reply_text("No active session.")
        return
    session_id = context.user_data["current_session_id"]
    await DB.end_session(session_id)
    context.user_data.pop("current_session_id", None)
    await update.message.reply_text("Session ended (no deep link created).")

# ---------- SET START (welcome) ----------
# /setstart: owner replies to a message (text or photo) to set welcome.
# Behavior requested: if owner replies to message with both image and message, set both; if only image replied then only image and previous message remains etc.
async def setstart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return
    # we support replying to a message to set welcome. If args present treat them as text.
    if update.message.reply_to_message:
        rm = update.message.reply_to_message
        cfg = await DB.get_start_config()
        # decide what was replied
        # if the reply has photo -> set image_file_id; if reply has text -> set text
        new_text = cfg["welcome_text"]
        new_image_id = cfg["welcome_image_file_id"]
        if rm.photo:
            new_image_id = rm.photo[-1].file_id
        if rm.text or rm.caption:
            new_text = (rm.caption or rm.text)
        await DB.set_start_config(new_text, new_image_id)
        await update.message.reply_text("✅ Start/welcome set.")
        return
    # else if user sends /setstart <text> (not reply), set text only
    if context.args:
        text = " ".join(context.args)
        await DB.set_start_config(text, (await DB.get_start_config())["welcome_image_file_id"])
        await update.message.reply_text("✅ Start text set.")
        return
    await update.message.reply_text("Usage: reply to a message containing the image/text to set welcome. Or: /setstart <welcome text>")

# ---------- BROADCAST ----------
# Simple two-step broadcast: owner sends /broadcast then follows with the content message
BROADCAST_WAITING = {}

async def broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return
    # ask for message content. Owner can include inline button definitions in form:
    # First line: message text (can be multiple lines). If owner includes a line starting with BUTTONS: then subsequent lines are label|url
    await update.message.reply_text(
        "📣 Send the broadcast message now. If you want inline buttons, append a line with `BUTTONS:` then each subsequent line `Label|https://...`.\n\n"
        "Example:\nHello everyone!\nBUTTONS:\nVisit|https://example.com\nSupport|https://t.me/your_support"
    )
    BROADCAST_WAITING[update.effective_user.id] = True

async def broadcast_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    if not BROADCAST_WAITING.get(update.effective_user.id):
        return
    # parse message and optional buttons
    text = update.message.text or update.message.caption or ""
    lines = (text or "").splitlines()
    buttons = []
    if "BUTTONS:" in lines:
        idx = lines.index("BUTTONS:")
        msg_lines = lines[:idx]
        btn_lines = lines[idx+1:]
        for bl in btn_lines:
            if "|" in bl:
                label, url = bl.split("|", 1)
                buttons.append((label.strip(), url.strip()))
        msg_text = "\n".join(msg_lines).strip()
    else:
        msg_text = text
    # if no msg_text but owner sent a photo/message with media, we support sending as-is
    # gather user list and send
    stats = await DB.get_stats()
    users_count = stats["total_users"]
    await update.message.reply_text(f"Broadcast starting to {users_count} users. This may take a while.")
    # build inline keyboard if buttons
    markup = None
    if buttons:
        kb = [[InlineKeyboardButton(label, url=url)] for (label, url) in buttons]
        markup = InlineKeyboardMarkup(kb)
    # fetch all users
    async with DB.pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users;")
    users = [r["user_id"] for r in rows]
    # send messages sequentially with small delay to respect rate limits
    sent = 0
    failed = 0
    for uid in users:
        try:
            await context.bot.send_message(chat_id=uid, text=msg_text or " ", reply_markup=markup, parse_mode=ParseMode.HTML)
            sent += 1
        except Exception as e:
            failed += 1
            logger.debug("Broadcast to %s failed: %s", uid, e)
        await asyncio.sleep(0.05)
    BROADCAST_WAITING.pop(update.effective_user.id, None)
    await update.message.reply_text(f"Broadcast finished. Sent: {sent}, Failed: {failed}")

# ---------- STATS ----------
async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return
    s = await DB.get_stats()
    await update.message.reply_text(
        f"📊 Stats:\n\nTotal users: {s['total_users']}\nActive in 2 days: {s['active_2days']}\nTotal files: {s['total_files']}\nTotal sessions: {s['total_sessions']}"
    )

# ---------- DEEP LINK FETCH API (alternate) ----------
# We also expose a simple /deep <token> command that behaves like start?start=token
async def deep_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /deep <token>")
        return
    token = context.args[0].strip()
    # emulate start with arg
    update_parsed = update  # reuse
    # call start handler logic (but ensure user logged)
    await start_handler(update, context)

# ---------- MISC ----------
async def unknown_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # allow /start and deep link only for users; other commands to be blocked for users
    await update.message.reply_text("Unknown command.")

# ---------- APP SETUP ----------
async def on_startup(application):
    # initialize DB
    await DB.init()
    logger.info("DB initialized.")
    # print bot username
    me = await application.bot.get_me()
    logger.info("Bot started as %s (@%s)", me.first_name, me.username)

def build_app():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN env var required")
    app = ApplicationBuilder().token(BOT_TOKEN).concurrent_updates(True).build()

    # Handlers
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("setstart", setstart_cmd))
    app.add_handler(CommandHandler("upload", upload_start))
    app.add_handler(CommandHandler("settimer", settimer))
    app.add_handler(CommandHandler("d", done_session))
    app.add_handler(CommandHandler("e", end_session))
    app.add_handler(CommandHandler("broadcast", broadcast_start))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("deep", deep_cmd))

    # message handlers for owner uploads
    # accept documents, photos, videos, audio, voice, stickers
    file_filters = filters.Document.ALL | filters.PHOTO | filters.VIDEO | filters.AUDIO | filters.VOICE | filters.STICKER
    app.add_handler(MessageHandler(file_filters & filters.User(user_id=OWNER_ID), upload_file_received))

    # broadcast content receiver (owner)
    app.add_handler(MessageHandler(filters.TEXT & filters.User(user_id=OWNER_ID), broadcast_receive))

    # unknown fallback
    app.add_handler(MessageHandler(filters.COMMAND, unknown_cmd))

    app.post_init = on_startup
    return app

# ---------- MAIN ----------
if __name__ == "__main__":
    application = build_app()
    application.run_polling()