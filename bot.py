#!/usr/bin/env python3
# main.py
"""
Render-deployable Telegram bot with Neon Postgres (asyncpg).
Features:
 - Owner-only multi-file upload sessions (copy to UPLOAD_CHANNEL)
 - Deep links (long random tokens) produced on /d
 - Auto-delete delivered messages (minutes) per session
 - Protect content (prevent forwarding/saving) for non-owner deliveries
 - /setstart to set welcome image/text by replying to a message
 - /broadcast supports inline buttons using {Label | url} syntax
 - /stats for owner
 - Auto-register webhook at startup and run webhook server (Render)
"""

import os
import asyncio
import logging
import secrets
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import asyncpg
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ParseMode,
)
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
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # e.g. https://your-app.onrender.com/webhook
PORT = int(os.getenv("PORT", os.getenv("RENDER_PORT", "8000")))

# deep link token entropy (bytes passed to token_urlsafe)
DEEP_LINK_BYTES = int(os.getenv("DEEP_LINK_BYTES", "72"))  # ~ 96+ chars token
# ----------------------------------------

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env var required")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL env var required")

if not WEBHOOK_URL:
    # we still allow polling if WEBHOOK_URL is not provided, but user asked auto-register — warn
    logging.warning("WEBHOOK_URL not provided; bot will attempt to run webhook only if WEBHOOK_URL set")

# Logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger("telegram-upload-bot")

# ---------------- DB POOL ----------------
db_pool: Optional[asyncpg.pool.Pool] = None


async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with db_pool.acquire() as conn:
        # Create tables if not exists
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
            """
        )
        # ensure a default start_config row
        r = await conn.fetchval("SELECT COUNT(*) FROM start_config;")
        if r == 0:
            await conn.execute("INSERT INTO start_config(id, welcome_text, welcome_image_file_id) VALUES (1, NULL, NULL);")


# ---------------- UTIL ----------------
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID


def make_deep_link_token() -> str:
    # use token_urlsafe to produce URL-safe token of requested entropy
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


# ---------------- SCHEDULED DELETION ----------------
# Note: scheduled deletions are in-memory tasks. If the process restarts before deletion fires,
# those deletions are lost. (You can build a persisted scheduler later if desired.)
async def schedule_delete(bot, chat_id: int, message_id: int, minutes: int):
    try:
        await asyncio.sleep(minutes * 60)
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.debug("schedule_delete failed: %s", e)


# ---------------- HANDLERS ----------------
# /start - normal welcome or deep-link retrieval
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await add_or_update_user(user)

    args = context.args or []
    # if deep link token provided
    if args:
        token = args[0]
        async with db_pool.acquire() as conn:
            session = await conn.fetchrow("SELECT * FROM sessions WHERE deep_link_token=$1 AND ended=TRUE;", token)
            # require session ended (finalized) — safety
            if not session:
                await update.message.reply_text("🔍 Invalid or expired link.")
                return
            files = await conn.fetch("SELECT * FROM files WHERE session_id=$1 ORDER BY id ASC;", session["id"])
        if not files:
            await update.message.reply_text("⚠️ No files available for this link.")
            return

        auto_delete = session["auto_delete_minutes"]
        owner_flag = is_owner(user.id)
        delivered_ids = []
        for f in files:
            try:
                copy = await context.bot.copy_message(
                    chat_id=update.effective_chat.id,
                    from_chat_id=f["upload_chat_id"],
                    message_id=f["upload_msg_id"],
                    caption=f["original_caption"] or None,
                    parse_mode=ParseMode.HTML,
                    protect_content=(False if owner_flag else True),
                )
                delivered_ids.append(copy.message_id)
            except Exception as e:
                logger.exception("Failed to deliver file via deep link: %s", e)
                # continue delivering remaining items

        if auto_delete and int(auto_delete) > 0:
            minutes = int(auto_delete)
            await update.message.reply_text(f"⏳ Files will be deleted in {minutes} minutes.")
            for mid in delivered_ids:
                asyncio.create_task(schedule_delete(context.bot, update.effective_chat.id, mid, minutes))
        return

    # normal start (no args)
    async with db_pool.acquire() as conn:
        cfg = await conn.fetchrow("SELECT welcome_text, welcome_image_file_id FROM start_config WHERE id=1;")
    welcome_text = cfg["welcome_text"]
    welcome_image = cfg["welcome_image_file_id"]
    if welcome_image and welcome_text:
        text = welcome_text.replace("{first_name}", user.first_name or "")
        await context.bot.send_photo(chat_id=update.effective_chat.id, photo=welcome_image, caption=text, parse_mode=ParseMode.HTML)
    elif welcome_image:
        await context.bot.send_photo(chat_id=update.effective_chat.id, photo=welcome_image)
    elif welcome_text:
        text = welcome_text.replace("{first_name}", user.first_name or "")
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("Welcome! Use deep links to fetch files.")


# /setstart - owner replies to a message (text/photo) to set welcome
async def setstart_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return

    # If replying to a message, inspect it
    if update.message.reply_to_message:
        rm = update.message.reply_to_message
        cfg_text = None
        cfg_image = None
        if rm.photo:
            cfg_image = rm.photo[-1].file_id
        if rm.text or rm.caption:
            cfg_text = (rm.caption or rm.text)
        # update DB accordingly: if rm has only image -> keep previous text, update image; if only text -> update text
        async with db_pool.acquire() as conn:
            cur = await conn.fetchrow("SELECT welcome_text, welcome_image_file_id FROM start_config WHERE id=1;")
            prev_text = cur["welcome_text"]
            prev_img = cur["welcome_image_file_id"]
            new_text = cfg_text if cfg_text is not None else prev_text
            new_img = cfg_image if cfg_image is not None else prev_img
            await conn.execute("UPDATE start_config SET welcome_text=$1, welcome_image_file_id=$2 WHERE id=1;", new_text, new_img)
        await update.message.reply_text("✅ Welcome configuration updated.")
        return

    # else if provided args, set text only
    if context.args:
        text = " ".join(context.args)
        async with db_pool.acquire() as conn:
            cur = await conn.fetchrow("SELECT welcome_image_file_id FROM start_config WHERE id=1;")
            prev_img = cur["welcome_image_file_id"]
            await conn.execute("UPDATE start_config SET welcome_text=$1, welcome_image_file_id=$2 WHERE id=1;", text, prev_img)
        await update.message.reply_text("✅ Welcome text set.")
        return

    await update.message.reply_text("Usage: reply to a message (text or photo) with /setstart to set welcome. Or: /setstart <text>")


# Upload flow: owner-only
# /upload starts session, owner sends files; use /settimer <minutes> to set auto-delete for session
async def upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return
    # create a session row
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("INSERT INTO sessions(owner_id, created_at, ended) VALUES ($1,$2,FALSE) RETURNING *;", OWNER_ID, datetime.utcnow())
    context.user_data["current_session_id"] = row["id"]
    await update.message.reply_text(
        "📦 Upload session started.\nSend files (documents, photos, videos). Send /d when done to create deep-link. Send /e to terminate without link.\nUse /settimer <minutes> to set auto-delete (minutes) for delivered user messages for this session (optional).\nUse /protect <on|off> to set whether delivered copies should be protect_content (default: on)."
    )


async def settimer_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return
    if "current_session_id" not in context.user_data:
        await update.message.reply_text("No active upload session. Use /upload to start.")
        return
    try:
        minutes = int(context.args[0])
        if minutes < 0:
            raise ValueError()
    except Exception:
        await update.message.reply_text("Usage: /settimer <minutes>  — integer minutes (0 or positive).")
        return
    session_id = context.user_data["current_session_id"]
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE sessions SET auto_delete_minutes=$1 WHERE id=$2;", minutes, session_id)
    await update.message.reply_text(f"Auto-delete set to {minutes} minutes for the current upload session.")


async def protect_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return
    if "current_session_id" not in context.user_data:
        await update.message.reply_text("No active upload session. Use /upload.")
        return
    val = (context.args[0].lower() if context.args else "").strip()
    if val not in ("on", "off"):
        await update.message.reply_text("Usage: /protect on  OR  /protect off")
        return
    flag = True if val == "on" else False
    session_id = context.user_data["current_session_id"]
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE sessions SET protect_content=$1 WHERE id=$2;", flag, session_id)
    await update.message.reply_text(f"Protect content set to {'ON' if flag else 'OFF'} for this session.")


# files sent by owner during session -> copy to UPLOAD_CHANNEL_ID and record
async def owner_file_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Only owner allowed to add files
    if not is_owner(update.effective_user.id):
        return
    if "current_session_id" not in context.user_data:
        await update.message.reply_text("No active session. Start one with /upload")
        return
    session_id = context.user_data["current_session_id"]
    msg = update.message
    caption = msg.caption or None
    try:
        # copy message to upload channel so upload channel holds files and bot has message ids
        forwarded = await context.bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=msg.chat.id, message_id=msg.message_id, caption=caption or None, parse_mode=ParseMode.HTML)
        # determine original file id and type for record (best-effort)
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
                forwarded.chat.id,
                forwarded.message_id,
                caption,
                file_type,
                original_file_id,
                datetime.utcnow(),
            )
        await update.message.reply_text("✅ File added to session.")
    except Exception as e:
        logger.exception("Error copying file to upload channel: %s", e)
        await update.message.reply_text("❌ Failed to copy file to upload channel. Make sure the bot is admin in the upload channel.")


# /d - finalize session, create deep link token and mark session ended
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
        await update.message.reply_text("No files in this session. Upload files first or use /e to cancel.")
        return

    # generate token (very long)
    token = make_deep_link_token()
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE sessions SET deep_link_token=$1, ended=TRUE WHERE id=$2;", token, session_id)
    # remove session id from user_data
    context.user_data.pop("current_session_id", None)
    # build deep link (long)
    bot_user = await context.bot.get_me()
    deep_link = f"https://t.me/{bot_user.username}?start={token}"
    # To give more characters in display, also provide plain token text if desired
    await update.message.reply_text(f"✅ Session finalized. Deep link created:\n\n{deep_link}\n\nAnyone with this link can fetch the files.")

# /e - end session without creating deep link
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
    await update.message.reply_text("Upload session ended (no deep link created).")


# /broadcast - owner replies to a message to broadcast; supports {Label | url} button syntax inside text
async def broadcast_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Not allowed.")
        return
    if not update.message.reply_to_message:
        await update.message.reply_text("Reply to the message (text/photo/document) you want to broadcast with /broadcast.")
        return
    msg_to_send = update.message.reply_to_message
    # parse inline buttons present in the text of the *replying* message only (or present inside the message text itself)
    # We will support occurrences like {Label | https://...} or {Label|https://...}
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
    await update.message.reply_text(f"📣 Broadcasting to {len(users)} users... starting.")
    for uid in users:
        try:
            # If message has media, copy it; otherwise send text
            if msg_to_send.photo or msg_to_send.document or msg_to_send.video or msg_to_send.audio:
                await msg_to_send.copy(chat_id=uid, caption=msg_to_send.caption, parse_mode=ParseMode.HTML, reply_markup=markup)
            else:
                await context.bot.send_message(chat_id=uid, text=msg_to_send.text or msg_to_send.caption or " ", parse_mode=ParseMode.HTML, reply_markup=markup)
            sent += 1
        except Exception as e:
            failed += 1
        await asyncio.sleep(0.05)  # gentle pacing
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


# /deep <token> alternative to start?start=token
async def deep_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /deep <token>")
        return
    # reuse start handler logic by placing token into args and calling start_handler
    await start_handler(update, context)


# Catch-all unknown commands for users
async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Unknown command or not allowed.")


# ---------------- APP STARTUP / WEBHOOK ----------------
async def on_startup(app):
    # init DB pool
    await init_db()
    logger.info("DB initialized.")
    # ensure bot username known
    me = await app.bot.get_me()
    logger.info("Bot running as %s (@%s)", me.first_name, me.username)
    # try to set webhook automatically if WEBHOOK_URL present
    if WEBHOOK_URL:
        try:
            # expected path ends with /webhook
            # If WEBHOOK_URL contains trailing slash, don't duplicate
            webhook_path = "/webhook"
            url = WEBHOOK_URL
            if not url.endswith(webhook_path):
                if url.endswith("/"):
                    url = url.rstrip("/") + webhook_path
                else:
                    url = url + webhook_path
            await app.bot.set_webhook(url)
            logger.info("Webhook set to %s", url)
        except Exception as e:
            logger.exception("Failed to set webhook: %s", e)
    else:
        logger.warning("WEBHOOK_URL not set — starting in webhook mode requires WEBHOOK_URL env var.")

# ---------------- BUILD & RUN ----------------
def build_app():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Command handlers
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("setstart", setstart_handler))
    app.add_handler(CommandHandler("upload", upload_handler))
    app.add_handler(CommandHandler("settimer", settimer_handler))
    app.add_handler(CommandHandler("protect", protect_handler))
    app.add_handler(CommandHandler("d", done_handler))
    app.add_handler(CommandHandler("e", end_handler))
    app.add_handler(CommandHandler("broadcast", broadcast_handler))
    app.add_handler(CommandHandler("stats", stats_handler))
    app.add_handler(CommandHandler("deep", deep_command))

    # Owner-only file receiver (during session)
    # Accept documents, photos, videos, audio, voice, stickers
    file_filters = filters.Document.ALL | filters.PHOTO | filters.VIDEO | filters.AUDIO | filters.VOICE | filters.STICKER
    app.add_handler(MessageHandler(file_filters & filters.User(user_id=OWNER_ID), owner_file_received))

    # Unknown commands
    app.add_handler(MessageHandler(filters.COMMAND, unknown_command))

    app.post_init = on_startup
    return app


async def main():
    app = build_app()

    # Decide running mode: run_webhook (preferred) so Render can accept HTTP
    # run_webhook will start webserver that listens to PORT and route receives at /webhook
    listen = "0.0.0.0"
    webhook_path = "/webhook"
    # some platforms set PORT in env; Render sets PORT env var
    port = PORT
    # start app with webhook
    logger.info("Starting webhook listener on %s:%s%s", listen, port, webhook_path)
    # The run_webhook method registers handlers and runs until process ends.
    # It will use the bot token's get_me() username automatically.
    await app.run_webhook(listen=listen, port=port, webhook_path=webhook_path)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down.")