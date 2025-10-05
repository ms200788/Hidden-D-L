#!/usr/bin/env python3
# bot.py
"""
Render-deployable Telegram bot (single-file)
- Uses python-telegram-bot (async) and asyncpg (Neon/Postgres)
- Webhook mode (auto-registers webhook at WEBHOOK_URL + /webhook)
- Owner-only multi-file upload sessions (copies to UPLOAD_CHANNEL_ID)
- Deep links: very long random tokens
- Auto-delete delivered messages (deletions persisted in DB so they survive restarts)
- Protect_content for non-owner deliveries (owner bypass)
- /setstart supports replying to a message (text/photo) to set welcome
- /broadcast supports inline buttons using {Label | https://...} syntax inside message text
- /stats shows totals
- All configuration via environment variables:
    BOT_TOKEN, DATABASE_URL, OWNER_ID, UPLOAD_CHANNEL_ID, WEBHOOK_URL, PORT (optional)
"""

import os
import asyncio
import logging
import secrets
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, List

import asyncpg
from aiohttp import web
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters,
)

# ---------------- CONFIG ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # e.g. https://your-app.onrender.com
PORT = int(os.getenv("PORT", os.getenv("RENDER_PORT", "8000")))

# entropy for deep links (bytes -> token_urlsafe length ~ 1.3*bytes)
DEEP_LINK_BYTES = int(os.getenv("DEEP_LINK_BYTES", "72"))  # ~96+ chars token

if not BOT_TOKEN or not DATABASE_URL:
    raise RuntimeError("BOT_TOKEN and DATABASE_URL env vars are required")

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("upload-bot")

# ---------------- DB POOL ----------------
db_pool: Optional[asyncpg.pool.Pool] = None

async def init_db_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=6)
    # create tables
    async with db_pool.acquire() as conn:
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
                auto_delete_minutes INT,
                protect_content BOOLEAN DEFAULT TRUE,
                note TEXT
            );
            CREATE TABLE IF NOT EXISTS files (
                id SERIAL PRIMARY KEY,
                session_id INT REFERENCES sessions(id) ON DELETE CASCADE,
                upload_chat_id BIGINT NOT NULL,
                upload_msg_id BIGINT NOT NULL,
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
            CREATE TABLE IF NOT EXISTS deletions (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                delete_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP NOT NULL
            );
            """
        )
        # ensure a default start_config row
        cnt = await conn.fetchval("SELECT COUNT(*) FROM start_config;")
        if cnt == 0:
            await conn.execute("INSERT INTO start_config(id, welcome_text, welcome_image_file_id) VALUES (1, NULL, NULL);")

# ---------------- UTIL ----------------
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def make_deep_link_token() -> str:
    return secrets.token_urlsafe(DEEP_LINK_BYTES)

async def add_or_update_user(user):
    if not user:
        return
    async with db_pool.acquire() as conn:
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
            user.id,
            getattr(user, "username", None),
            getattr(user, "first_name", None),
            getattr(user, "last_name", None),
            datetime.utcnow(),
        )

# ---------------- PERSISTENT DELETION WORKER ----------------
async def schedule_deletion_record(chat_id: int, message_id: int, minutes_from_now: int):
    """Persist a deletion job to DB (survives restarts)."""
    delete_at = datetime.utcnow() + timedelta(minutes=minutes_from_now)
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO deletions (chat_id, message_id, delete_at, created_at) VALUES ($1,$2,$3,$4);",
            chat_id,
            message_id,
            delete_at,
            datetime.utcnow(),
        )

async def deletion_worker(app):
    """Background task: runs every 15 seconds and deletes due messages."""
    bot = app.bot
    logger.info("Deletion worker started.")
    while True:
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT id, chat_id, message_id FROM deletions WHERE delete_at <= $1 ORDER BY delete_at ASC LIMIT 50;", datetime.utcnow())
                if rows:
                    logger.info("Found %d deletions due.", len(rows))
                for r in rows:
                    did = r["id"]
                    chat_id = r["chat_id"]
                    message_id = r["message_id"]
                    try:
                        await bot.delete_message(chat_id=chat_id, message_id=message_id)
                    except Exception as e:
                        # log but still remove the record to avoid repeat attempts
                        logger.debug("delete_message failed for %s:%s — %s", chat_id, message_id, e)
                    # remove record
                    await conn.execute("DELETE FROM deletions WHERE id=$1;", did)
        except Exception as e:
            logger.exception("Deletion worker error: %s", e)
        await asyncio.sleep(15)

# ---------------- HANDLERS ----------------

# /start — normal welcome or deep link retrieval
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await add_or_update_user(user)
    args = context.args or []
    if args:
        token = args[0]
        # fetch session only if ended = true (finalized)
        async with db_pool.acquire() as conn:
            session = await conn.fetchrow("SELECT * FROM sessions WHERE deep_link_token=$1 AND ended=TRUE;", token)
            if not session:
                await update.message.reply_text("🔍 Invalid or expired link.")
                return
            files = await conn.fetch("SELECT * FROM files WHERE session_id=$1 ORDER BY id ASC;", session["id"])

        if not files:
            await update.message.reply_text("⚠️ No files found for this link.")
            return

        auto_delete = session["auto_delete_minutes"]
        protect_flag = session["protect_content"]
        owner_flag = is_owner(user.id)
        delivered = []
        for f in files:
            try:
                copy = await context.bot.copy_message(
                    chat_id=update.effective_chat.id,
                    from_chat_id=f["upload_chat_id"],
                    message_id=f["upload_msg_id"],
                    caption=f["original_caption"] or None,
                    parse_mode=ParseMode.HTML,
                    protect_content=(False if owner_flag else (False if protect_flag is False else True)),
                )
                delivered.append(copy.message_id)
                # persist deletion if session had auto_delete_minutes set and >0
                if auto_delete and int(auto_delete) > 0 and not owner_flag:
                    await schedule_deletion_record(update.effective_chat.id, copy.message_id, int(auto_delete))
            except Exception as e:
                logger.exception("Failed to deliver file: %s", e)
        if auto_delete and int(auto_delete) > 0:
            await update.message.reply_text(f"⏳ Files will be deleted in {auto_delete} minutes.")
        return

    # Normal welcome
    async with db_pool.acquire() as conn:
        cfg = await conn.fetchrow("SELECT welcome_text, welcome_image_file_id FROM start_config WHERE id=1;")
    welcome_text = cfg["welcome_text"]
    welcome_image = cfg["welcome_image_file_id"]
    name = getattr(user, "first_name", "") or ""
    if welcome_image and welcome_text:
        text = welcome_text.replace("{first_name}", name)
        await context.bot.send_photo(chat_id=update.effective_chat.id, photo=welcome_image, caption=text, parse_mode=ParseMode.HTML)
    elif welcome_image:
        await context.bot.send_photo(chat_id=update.effective_chat.id, photo=welcome_image)
    elif welcome_text:
        text = welcome_text.replace("{first_name}", name)
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("Welcome — use deep links to fetch files.")

# /setstart — owner replies to a message (text or photo)
async def setstart_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return

    if update.message.reply_to_message:
        rm = update.message.reply_to_message
        new_text = None
        new_image = None
        if rm.photo:
            new_image = rm.photo[-1].file_id
        if rm.text or rm.caption:
            new_text = (rm.caption or rm.text)
        async with db_pool.acquire() as conn:
            cur = await conn.fetchrow("SELECT welcome_text, welcome_image_file_id FROM start_config WHERE id=1;")
            prev_text = cur["welcome_text"]
            prev_img = cur["welcome_image_file_id"]
            final_text = new_text if new_text is not None else prev_text
            final_img = new_image if new_image is not None else prev_img
            await conn.execute("UPDATE start_config SET welcome_text=$1, welcome_image_file_id=$2 WHERE id=1;", final_text, final_img)
        await update.message.reply_text("✅ Welcome config updated.")
        return

    # set via args as text
    if context.args:
        text = " ".join(context.args)
        async with db_pool.acquire() as conn:
            cur = await conn.fetchrow("SELECT welcome_image_file_id FROM start_config WHERE id=1;")
            prev_img = cur["welcome_image_file_id"]
            await conn.execute("UPDATE start_config SET welcome_text=$1, welcome_image_file_id=$2 WHERE id=1;", text, prev_img)
        await update.message.reply_text("✅ Welcome text set.")
        return

    await update.message.reply_text("Usage: reply to a message with /setstart or use /setstart <text>")

# /upload — owner starts session
async def upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("INSERT INTO sessions(owner_id, created_at, ended) VALUES ($1,$2,FALSE) RETURNING *;", OWNER_ID, datetime.utcnow())
    context.user_data["current_session_id"] = row["id"]
    await update.message.reply_text(
        "📦 Upload session started.\nSend files now (documents, photos, videos, audio, voice, stickers).\nWhen finished send /d to finalize and generate deep link. Send /e to cancel.\nOptional: /settimer <minutes> and /protect on|off for this session."
    )

# /settimer <minutes> - set auto-delete for current session
async def settimer_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return
    if "current_session_id" not in context.user_data:
        await update.message.reply_text("No active upload session. Use /upload.")
        return
    try:
        minutes = int(context.args[0])
        if minutes < 0:
            raise ValueError()
    except Exception:
        await update.message.reply_text("Usage: /settimer <minutes>  — integer minutes (0 allowed).")
        return
    sid = context.user_data["current_session_id"]
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE sessions SET auto_delete_minutes=$1 WHERE id=$2;", minutes, sid)
    await update.message.reply_text(f"Auto-delete set to {minutes} minutes for this session.")

# /protect on|off - set protect_content for copies delivered to users (default on)
async def protect_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return
    if "current_session_id" not in context.user_data:
        await update.message.reply_text("No active upload session. Use /upload.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /protect on  OR  /protect off")
        return
    val = context.args[0].lower()
    if val not in ("on", "off"):
        await update.message.reply_text("Usage: /protect on  OR  /protect off")
        return
    flag = True if val == "on" else False
    sid = context.user_data["current_session_id"]
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE sessions SET protect_content=$1 WHERE id=$2;", flag, sid)
    await update.message.reply_text(f"Protect content set to {'ON' if flag else 'OFF'} for this session.")

# Owner files handler — accept any non-command message from owner during session
async def owner_file_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    if "current_session_id" not in context.user_data:
        await update.message.reply_text("No active session. Start with /upload")
        return
    session_id = context.user_data["current_session_id"]
    msg = update.message
    caption = msg.caption or None
    try:
        # copy message to upload channel (bot must be admin)
        copied = await context.bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=msg.chat.id, message_id=msg.message_id, caption=caption or None, parse_mode=ParseMode.HTML)
        # determine type and original file id best-effort
        file_type = "unknown"
        original_file_id = None
        if msg.document:
            file_type = "document"
            original_file_id = msg.document.file_id
        elif msg.photo:
            file_type = "photo"
            original_file_id = msg.photo[-1].file_id
        elif msg.video:
            file_type = "video"
            original_file_id = msg.video.file_id
        elif msg.audio:
            file_type = "audio"
            original_file_id = msg.audio.file_id
        elif msg.voice:
            file_type = "voice"
            original_file_id = msg.voice.file_id
        elif msg.sticker:
            file_type = "sticker"
            original_file_id = msg.sticker.file_id

        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO files(session_id, upload_chat_id, upload_msg_id, original_caption, original_file_type, original_file_id, created_at)
                VALUES($1,$2,$3,$4,$5,$6,$7);
                """,
                session_id,
                copied.chat.id,
                copied.message_id,
                caption,
                file_type,
                original_file_id,
                datetime.utcnow(),
            )
        await update.message.reply_text("✅ File added to session.")
    except Exception as e:
        logger.exception("Failed to copy file to upload channel: %s", e)
        await update.message.reply_text("❌ Failed to copy file to upload channel. Ensure bot is admin in upload channel and has post/manage rights.")

# /d finalize: generate deep link, mark session ended
async def done_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return
    if "current_session_id" not in context.user_data:
        await update.message.reply_text("No active session.")
        return
    session_id = context.user_data["current_session_id"]
    async with db_pool.acquire() as conn:
        files_count = await conn.fetchval("SELECT COUNT(*) FROM files WHERE session_id=$1;", session_id)
    if files_count == 0:
        await update.message.reply_text("No files uploaded in this session. Upload files or use /e to cancel.")
        return
    token = make_deep_link_token()
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE sessions SET deep_link_token=$1, ended=TRUE WHERE id=$2;", token, session_id)
    context.user_data.pop("current_session_id", None)
    bot_user = await context.bot.get_me()
    deep_link = f"https://t.me/{bot_user.username}?start={token}"
    await update.message.reply_text(f"✅ Session finalized. Deep link:\n\n{deep_link}\n\nAnyone with this link can fetch the files.")

# /e end session without creating link
async def end_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return
    if "current_session_id" not in context.user_data:
        await update.message.reply_text("No active session.")
        return
    sid = context.user_data["current_session_id"]
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE sessions SET ended=TRUE WHERE id=$1;", sid)
    context.user_data.pop("current_session_id", None)
    await update.message.reply_text("Upload session terminated (no deep link created).")

# /broadcast — owner replies to a message to broadcast. supports {Label | url}
async def broadcast_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return
    if not update.message.reply_to_message:
        await update.message.reply_text("Reply to a message (text/photo/doc) with /broadcast to send it to all users.")
        return
    msg_to_send = update.message.reply_to_message
    text_source = (msg_to_send.text or msg_to_send.caption or "") + ""
    buttons = []
    for match in re.finditer(r"\{([^{}|]+)\s*\|\s*(https?://[^\s{}]+)\}", text_source):
        label = match.group(1).strip()
        url = match.group(2).strip()
        buttons.append((label, url))
    markup = None
    if buttons:
        kb = [[InlineKeyboardButton(label, url=url)] for (label, url) in buttons]
        markup = InlineKeyboardMarkup(kb)

    # fetch recipients
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users;")
    users = [r["user_id"] for r in rows]
    sent = 0
    failed = 0
    await update.message.reply_text(f"📣 Broadcasting to {len(users)} users (starting).")
    for uid in users:
        try:
            if msg_to_send.photo or msg_to_send.document or msg_to_send.video or msg_to_send.audio:
                await msg_to_send.copy(chat_id=uid, caption=msg_to_send.caption, parse_mode=ParseMode.HTML, reply_markup=markup)
            else:
                await context.bot.send_message(chat_id=uid, text=msg_to_send.text or msg_to_send.caption or " ", parse_mode=ParseMode.HTML, reply_markup=markup)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await update.message.reply_text(f"Broadcast complete. Sent: {sent}, Failed: {failed}")

# /stats owner-only
async def stats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return
    async with db_pool.acquire() as conn:
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users;")
        two_days_ago = datetime.utcnow() - timedelta(days=2)
        active_2days = await conn.fetchval("SELECT COUNT(*) FROM users WHERE last_seen >= $1;", two_days_ago)
        total_files = await conn.fetchval("SELECT COUNT(*) FROM files;")
        total_sessions = await conn.fetchval("SELECT COUNT(*) FROM sessions;")
    await update.message.reply_text(
        f"📊 Stats:\nTotal users: {total_users}\nActive (2 days): {active_2days}\nTotal files: {total_files}\nTotal sessions: {total_sessions}"
    )

# /deep <token> alternative
async def deep_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /deep <token>")
        return
    # reuse start_handler by placing args
    await start_handler(update, context)

# fallback unknown command
async def unknown_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Unknown command or not allowed.")

# ---------------- WEBHOOK / APP SETUP ----------------
async def on_startup(app):
    # initialize DB
    await init_db_pool()
    # start deletion worker
    app.create_task(deletion_worker(app))
    # set webhook if WEBHOOK_URL provided
    if WEBHOOK_URL:
        # ensure webhook path ends with /webhook
        webhook_path = "/webhook"
        url = WEBHOOK_URL
        if url.endswith("/"):
            url = url.rstrip("/")
        hook_url = url + webhook_path
        try:
            await app.bot.set_webhook(hook_url)
            logger.info("Webhook set to %s", hook_url)
        except Exception as e:
            logger.exception("Failed to set webhook: %s", e)
    else:
        logger.warning("WEBHOOK_URL not provided — the bot will fail to receive updates via webhook.")

def build_app():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("setstart", setstart_handler))
    app.add_handler(CommandHandler("upload", upload_handler))
    app.add_handler(CommandHandler("settimer", settimer_handler))
    app.add_handler(CommandHandler("protect", protect_handler))
    app.add_handler(CommandHandler("d", done_handler))
    app.add_handler(CommandHandler("e", end_handler))
    app.add_handler(CommandHandler("broadcast", broadcast_handler))
    app.add_handler(CommandHandler("stats", stats_handler))
    app.add_handler(CommandHandler("deep", deep_cmd))

    # Owner file receiver: any non-command message from owner will be treated as upload content when session active
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND & filters.User(user_id=OWNER_ID), owner_file_handler))

    # Unknown commands
    app.add_handler(MessageHandler(filters.COMMAND, unknown_handler))

    app.post_init = on_startup
    return app

# ---------------- MAIN (run webhook server) ----------------
async def main():
    app = build_app()

    # run_webhook - Application.run_webhook will start aiohttp server internally
    listen = "0.0.0.0"
    webhook_path = "/webhook"
    port = PORT
    logger.info("Starting webhook server on %s:%s%s", listen, port, webhook_path)
    # run_webhook is blocking and will return when shutdown
    await app.run_webhook(listen=listen, port=port, webhook_path=webhook_path)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down.")