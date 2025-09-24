#!/usr/bin/env python3
# bot.py
# Vault-style Telegram bot implementing upload sessions, deep links, persistent auto-delete jobs,
# DB backups, admin commands, auto-delete scheduling, and health endpoint for Render/UptimeRobot.
#
# Notes:
# - Designed to run with aiogram 2.25.1 (polling), aiohttp 3.8.6, APScheduler 3.10.4.
# - Uses SQLite for persistent metadata (DB backups pinned in DB channel).
# - Deep links are randomized tokens (separate from numeric session id).
# - Broadcast will remove users who blocked the bot or no longer exist and will notify owner.
# - Health endpoint exposed on PORT so Render/UptimeRobot can ping it.

import os
import logging
import asyncio
import json
import sqlite3
import tempfile
import secrets
import string
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, BotCommand
from aiogram.dispatcher.handler import CancelHandler
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.callback_data import CallbackData

# executor for polling
from aiogram.utils import executor

# exceptions from aiogram (import specific ones to avoid import issues)
from aiogram.utils.exceptions import (
    BotBlocked,
    ChatNotFound,
    RetryAfter,
    BadRequest,
    MessageToDeleteNotFound,
)

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

import aiohttp
from aiohttp import web

# -------------------------
# Environment configuration
# -------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = int(os.environ.get("OWNER_ID") or 0)
UPLOAD_CHANNEL_ID = int(os.environ.get("UPLOAD_CHANNEL_ID") or 0)
DB_CHANNEL_ID = int(os.environ.get("DB_CHANNEL_ID") or 0)
DB_PATH = os.environ.get("DB_PATH", "/data/database.sqlite3")
JOB_DB_PATH = os.environ.get("JOB_DB_PATH", "/data/jobs.sqlite")
PORT = int(os.environ.get("PORT", "10000"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
BROADCAST_CONCURRENCY = int(os.environ.get("BROADCAST_CONCURRENCY", "12"))
AUTO_BACKUP_HOURS = int(os.environ.get("AUTO_BACKUP_HOURS", "12"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if OWNER_ID == 0:
    raise RuntimeError("OWNER_ID is required")
if UPLOAD_CHANNEL_ID == 0:
    raise RuntimeError("UPLOAD_CHANNEL_ID is required")
if DB_CHANNEL_ID == 0:
    raise RuntimeError("DB_CHANNEL_ID is required")

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("vaultbot")

# -------------------------
# Bot & Dispatcher
# -------------------------
# IMPORTANT: Set default parse_mode=None so stored user-provided text is sent as plain text.
# This prevents CantParseEntities errors when the DB text contains custom tags.
bot = Bot(token=BOT_TOKEN, parse_mode=None)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# -------------------------
# Ensure job DB folder exists (for SQLAlchemy jobstore)
# -------------------------
def _ensure_dir_for_path(path: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

_ensure_dir_for_path(JOB_DB_PATH)
_ensure_dir_for_path(DB_PATH)

# -------------------------
# Scheduler with persistent jobstore
# -------------------------
jobstores = {
    'default': SQLAlchemyJobStore(url=f"sqlite:///{JOB_DB_PATH}")
}
scheduler = AsyncIOScheduler(jobstores=jobstores)
scheduler.configure(timezone="UTC")

# -------------------------
# Callback data factories
# -------------------------
cb_choose_protect = CallbackData("protect", "session", "choice")
cb_retry = CallbackData("retry", "session")
cb_help_button = CallbackData("helpbtn", "action")

# -------------------------
# DB schema
# -------------------------
SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    last_seen TEXT
);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id INTEGER,
    created_at TEXT,
    protect INTEGER DEFAULT 0,
    auto_delete_minutes INTEGER DEFAULT 0,
    title TEXT,
    revoked INTEGER DEFAULT 0,
    header_msg_id INTEGER,
    header_chat_id INTEGER,
    deep_link TEXT
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER,
    file_type TEXT,
    file_id TEXT,
    caption TEXT,
    original_msg_id INTEGER,
    vault_msg_id INTEGER,
    FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS delete_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER,
    target_chat_id INTEGER,
    message_ids TEXT,
    run_at TEXT,
    created_at TEXT,
    status TEXT DEFAULT 'scheduled'
);
"""

# -------------------------
# Database initialization
# -------------------------
db: sqlite3.Connection  # global

def init_db(path: str = DB_PATH):
    global db
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    need_init = not os.path.exists(path)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    db = conn
    if need_init:
        conn.executescript(SCHEMA)
        conn.commit()
    return conn

db = init_db(DB_PATH)

# -------------------------
# DB helpers
# -------------------------
def db_set(key: str, value: str):
    cur = db.cursor()
    cur.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key, value))
    db.commit()

def db_get(key: str, default=None):
    cur = db.cursor()
    cur.execute("SELECT value FROM settings WHERE key=?", (key,))
    r = cur.fetchone()
    return r["value"] if r else default

def sql_insert_session(owner_id:int, protect:int, auto_delete_minutes:int, title:str, header_chat_id:int, header_msg_id:int, deep_link_token:str)->int:
    cur = db.cursor()
    cur.execute(
        "INSERT INTO sessions (owner_id,created_at,protect,auto_delete_minutes,title,header_chat_id,header_msg_id,deep_link) VALUES (?,?,?,?,?,?,?,?)",
        (owner_id, datetime.utcnow().isoformat(), protect, auto_delete_minutes, title, header_chat_id, header_msg_id, deep_link_token)
    )
    db.commit()
    return cur.lastrowid

def sql_add_file(session_id:int, file_type:str, file_id:str, caption:str, original_msg_id:int, vault_msg_id:int):
    cur = db.cursor()
    cur.execute(
        "INSERT INTO files (session_id,file_type,file_id,caption,original_msg_id,vault_msg_id) VALUES (?,?,?,?,?,?)",
        (session_id, file_type, file_id, caption, original_msg_id, vault_msg_id)
    )
    db.commit()
    return cur.lastrowid

def sql_list_sessions(limit=50):
    cur = db.cursor()
    cur.execute("SELECT * FROM sessions ORDER BY created_at DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    return [dict(r) for r in rows]

def sql_get_session_by_id(session_id:int):
    cur = db.cursor()
    cur.execute("SELECT * FROM sessions WHERE id=?", (session_id,))
    r = cur.fetchone()
    return dict(r) if r else None

def sql_get_session_by_token(token: str):
    cur = db.cursor()
    cur.execute("SELECT * FROM sessions WHERE deep_link=?", (token,))
    r = cur.fetchone()
    return dict(r) if r else None

def sql_get_session_files(session_id:int):
    cur = db.cursor()
    cur.execute("SELECT * FROM files WHERE session_id=? ORDER BY id", (session_id,))
    rows = cur.fetchall()
    return [dict(r) for r in rows]

def sql_set_session_revoked(session_id:int, revoked:int=1):
    cur = db.cursor()
    cur.execute("UPDATE sessions SET revoked=? WHERE id=?", (revoked, session_id))
    db.commit()

def sql_add_user(user: types.User):
    cur = db.cursor()
    cur.execute("INSERT OR REPLACE INTO users (id,username,first_name,last_name,last_seen) VALUES (?,?,?,?,?)",
                (user.id, user.username or "", user.first_name or "", user.last_name or "", datetime.utcnow().isoformat()))
    db.commit()

def sql_update_user_lastseen(user_id:int, username:str="", first_name:str="", last_name:str=""):
    cur = db.cursor()
    cur.execute("INSERT OR REPLACE INTO users (id,username,first_name,last_name,last_seen) VALUES (?,?,?,?,?)",
                (user_id, username or "", first_name or "", last_name or "", datetime.utcnow().isoformat()))
    db.commit()

def sql_remove_user(user_id:int):
    cur = db.cursor()
    cur.execute("DELETE FROM users WHERE id=?", (user_id,))
    db.commit()

def sql_stats():
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) as cnt FROM users")
    total_users = cur.fetchone()["cnt"]
    cur.execute("SELECT COUNT(*) as active FROM users WHERE last_seen >= ?", ((datetime.utcnow()-timedelta(days=2)).isoformat(),))
    row = cur.fetchone()
    active = row["active"] if row else 0
    cur.execute("SELECT COUNT(*) as files FROM files")
    files = cur.fetchone()["files"]
    cur.execute("SELECT COUNT(*) as sessions FROM sessions")
    sessions = cur.fetchone()["sessions"]
    return {"total_users": total_users, "active_2d": active, "files": files, "sessions": sessions}

def sql_add_delete_job(session_id:int, target_chat_id:int, message_ids:List[int], run_at:datetime):
    cur = db.cursor()
    cur.execute("INSERT INTO delete_jobs (session_id,target_chat_id,message_ids,run_at,created_at) VALUES (?,?,?,?,?)",
                (session_id, target_chat_id, json.dumps(message_ids), run_at.isoformat(), datetime.utcnow().isoformat()))
    db.commit()
    return cur.lastrowid

def sql_list_pending_jobs():
    cur = db.cursor()
    cur.execute("SELECT * FROM delete_jobs WHERE status='scheduled'")
    return [dict(r) for r in cur.fetchall()]

def sql_mark_job_done(job_id:int):
    cur = db.cursor()
    cur.execute("UPDATE delete_jobs SET status='done' WHERE id=?", (job_id,))
    db.commit()

# -------------------------
# In-memory upload sessions
# -------------------------
active_uploads: Dict[int, Dict[str, Any]] = {}

def start_upload_session(owner_id:int, exclude_text:bool):
    active_uploads[owner_id] = {
        "messages": [], "exclude_text": exclude_text, "started_at": datetime.utcnow()
    }

def cancel_upload_session(owner_id:int):
    active_uploads.pop(owner_id, None)

def append_upload_message(owner_id:int, msg: types.Message):
    if owner_id not in active_uploads:
        return
    active_uploads[owner_id]["messages"].append(msg)

def get_upload_messages(owner_id:int) -> List[types.Message]:
    return active_uploads.get(owner_id, {}).get("messages", [])

# -------------------------
# Utilities
# -------------------------
async def safe_send(chat_id, text=None, **kwargs):
    try:
        if text is None:
            return None
        # we rely on default parse_mode=None (plain text); caller may pass parse_mode if desired
        return await bot.send_message(chat_id, text, **kwargs)
    except BotBlocked:
        logger.warning("Bot blocked by %s", chat_id)
    except ChatNotFound:
        logger.warning("Chat not found: %s", chat_id)
    except RetryAfter as e:
        logger.warning("Flood wait %s", e.timeout)
        await asyncio.sleep(e.timeout + 1)
        return await safe_send(chat_id, text, **kwargs)
    except Exception:
        logger.exception("Failed to send message")
    return None

async def safe_copy(to_chat_id:int, from_chat_id:int, message_id:int, **kwargs):
    try:
        return await bot.copy_message(to_chat_id, from_chat_id, message_id, **kwargs)
    except RetryAfter as e:
        logger.warning("RetryAfter copying: %s", e.timeout)
        await asyncio.sleep(e.timeout + 1)
        return await safe_copy(to_chat_id, from_chat_id, message_id, **kwargs)
    except Exception:
        logger.exception("safe_copy failed")
        return None

async def resolve_channel_link(link: str) -> Optional[int]:
    link = (link or "").strip()
    if not link:
        return None
    try:
        # direct ID provided
        if link.startswith("-100") or (link.startswith("-") and link[1:].isdigit()):
            return int(link)
        # remove query params or trailing slashes
        if "t.me" in link:
            base = link.split("?")[0]
            name = base.rstrip("/").split("/")[-1]
            if name:
                if name.startswith("@"):
                    name = name[1:]
                ch = await bot.get_chat(name)
                return ch.id
        if link.startswith("@"):
            name = link[1:]
            ch = await bot.get_chat(name)
            return ch.id
        ch = await bot.get_chat(link)
        return ch.id
    except ChatNotFound:
        logger.warning("resolve_channel_link: chat not found %s", link)
        return None
    except Exception as e:
        logger.warning("resolve_channel_link error %s : %s", link, e)
        return None

# -------------------------
# DB backup & restore
# -------------------------
async def backup_db_to_channel():
    try:
        if DB_CHANNEL_ID == 0:
            logger.error("DB_CHANNEL_ID not set")
            return None
        if not os.path.exists(DB_PATH):
            logger.error("Local DB missing for backup")
            return None
        with open(DB_PATH, "rb") as f:
            sent = await bot.send_document(DB_CHANNEL_ID, InputFile(f, filename=os.path.basename(DB_PATH)),
                                           caption=f"DB backup {datetime.utcnow().isoformat()}",
                                           disable_notification=True)
        try:
            await bot.pin_chat_message(DB_CHANNEL_ID, sent.message_id, disable_notification=True)
        except ChatNotFound:
            logger.error("ChatNotFound while pinning DB. Bot might not be in the DB channel.")
        except Exception:
            logger.exception("Failed to pin DB backup")
        return sent
    except Exception:
        logger.exception("backup_db_to_channel failed")
        return None

async def restore_db_from_pinned():
    global db
    try:
        if os.path.exists(DB_PATH):
            logger.info("Local DB present; skipping restore.")
            return True
        logger.info("Attempting DB restore from pinned in DB channel")
        try:
            chat = await bot.get_chat(DB_CHANNEL_ID)
        except ChatNotFound:
            logger.error("DB channel not found during restore")
            return False
        pinned = getattr(chat, "pinned_message", None)
        if pinned and getattr(pinned, "document", None):
            file_id = pinned.document.file_id
            file = await bot.get_file(file_id)
            file_path = getattr(file, "file_path", None)
            if not file_path:
                logger.error("Pinned DB file has no file_path")
                return False
            try:
                file_bytes = await bot.download_file(file_path)
            except Exception:
                try:
                    file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
                    async with aiohttp.ClientSession() as sess:
                        async with sess.get(file_url) as resp:
                            if resp.status == 200:
                                file_bytes = await resp.read()
                            else:
                                logger.error("Failed to fetch file from file_url; status %s", resp.status)
                                return False
                except Exception:
                    logger.exception("Failed to download pinned DB via fallback")
                    return False
            tmp = tempfile.NamedTemporaryFile(delete=False)
            try:
                with open(tmp.name, "wb") as out:
                    out.write(file_bytes)
                try:
                    db.close()
                except Exception:
                    pass
                os.replace(tmp.name, DB_PATH)
                db = init_db(DB_PATH)
                logger.info("DB restored from pinned")
                return True
            finally:
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass
        logger.error("No pinned DB document found; aborting restore.")
        return False
    except Exception:
        logger.exception("restore_db_from_pinned failed")
        return False

# -------------------------
# Delete job executor
# -------------------------
async def execute_delete_job(job_id:int, job_row:Dict[str,Any]):
    try:
        msg_ids = json.loads(job_row["message_ids"])
        target_chat = int(job_row["target_chat_id"])
        for mid in msg_ids:
            try:
                await bot.delete_message(target_chat, int(mid))
            except MessageToDeleteNotFound:
                pass
            except ChatNotFound:
                logger.warning("Chat not found when deleting messages for job %s", job_id)
            except BotBlocked:
                logger.warning("Bot blocked when deleting messages for job %s", job_id)
            except Exception:
                logger.exception("Error deleting message %s in %s", mid, target_chat)
        sql_mark_job_done(job_id)
        try:
            scheduler.remove_job(f"deljob_{job_id}")
        except Exception:
            pass
        logger.info("Executed delete job %s", job_id)
    except Exception:
        logger.exception("Failed delete job %s", job_id)

async def restore_pending_jobs_and_schedule():
    logger.info("Restoring pending delete jobs")
    pending = sql_list_pending_jobs()
    for job in pending:
        try:
            run_at = datetime.fromisoformat(job["run_at"])
            now = datetime.utcnow()
            job_id = job["id"]
            if run_at <= now:
                asyncio.create_task(execute_delete_job(job_id, job))
            else:
                scheduler.add_job(execute_delete_job, 'date', run_date=run_at, args=(job_id, job), id=f"deljob_{job_id}")
                logger.info("Scheduled delete job %s at %s", job_id, run_at.isoformat())
        except Exception:
            logger.exception("Failed to restore job %s", job.get("id"))

# -------------------------
# Health endpoint (aiohttp)
# -------------------------
async def handle_health(request):
    return web.Response(text="ok")

async def run_health_app():
    try:
        app = web.Application()
        app.add_routes([web.get('/', handle_health), web.get('/health', handle_health)])
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        logger.info("Health endpoint running on 0.0.0.0:%s/health", PORT)
    except Exception:
        logger.exception("Failed to start health server")

# -------------------------
# Utilities for buttons and owner check
# -------------------------
def is_owner(user_id:int)->bool:
    return user_id == OWNER_ID

def build_channel_buttons(optional_list:List[Dict[str,str]], forced_list:List[Dict[str,str]]):
    kb = InlineKeyboardMarkup()
    for ch in (optional_list or [])[:4]:
        kb.add(InlineKeyboardButton(ch.get("name","Channel"), url=ch.get("link")))
    for ch in (forced_list or [])[:3]:
        kb.add(InlineKeyboardButton(ch.get("name","Join"), url=ch.get("link")))
    kb.add(InlineKeyboardButton("Help", callback_data=cb_help_button.new(action="open")))
    return kb

# -------------------------
# Deep-link token generator
# -------------------------
def generate_token(length: int = 8) -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# -------------------------
# Command handlers
# -------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    try:
        sql_add_user(message.from_user)
        args = message.get_args().strip()
        payload = args if args else None

        start_text = db_get("start_text", "Welcome, {first_name}!")
        # make friendly replacement, don't assume HTML
        start_text = start_text.replace("{username}", message.from_user.username or "").replace("{first_name}", message.from_user.first_name or "")
        optional_json = db_get("optional_channels", "[]")
        forced_json = db_get("force_channels", "[]")
        try:
            optional = json.loads(optional_json)
        except Exception:
            optional = []
        try:
            forced = json.loads(forced_json)
        except Exception:
            forced = []
        kb = build_channel_buttons(optional, forced)

        if not payload:
            # send plain text (no HTML parsing)
            await message.answer(start_text, reply_markup=kb)
            return

        # payload may be numeric id or token
        s = None
        try:
            sid = int(payload)
            s = sql_get_session_by_id(sid)
        except Exception:
            s = sql_get_session_by_token(payload)

        if not s or s.get("revoked"):
            await message.answer("This session link is invalid or revoked.")
            return

        blocked = False
        unresolved = []
        for ch in (forced or [])[:3]:
            link = ch.get("link")
            resolved = await resolve_channel_link(link)
            if resolved:
                try:
                    member = await bot.get_chat_member(resolved, message.from_user.id)
                    if getattr(member, "status", None) in ("left", "kicked"):
                        blocked = True
                        break
                except BadRequest:
                    blocked = True
                    break
                except ChatNotFound:
                    unresolved.append(link)
                except Exception:
                    unresolved.append(link)
            else:
                unresolved.append(link)

        if blocked:
            kb2 = InlineKeyboardMarkup()
            for ch in (forced or [])[:3]:
                kb2.add(InlineKeyboardButton(ch.get("name","Join"), url=ch.get("link")))
            kb2.add(InlineKeyboardButton("Retry", callback_data=cb_retry.new(session=s["id"])))
            await message.answer("You must join the required channels first.", reply_markup=kb2)
            return

        if unresolved:
            kb2 = InlineKeyboardMarkup()
            for ch in (forced or [])[:3]:
                kb2.add(InlineKeyboardButton(ch.get("name","Join"), url=ch.get("link")))
            kb2.add(InlineKeyboardButton("Retry", callback_data=cb_retry.new(session=s["id"])))
            await message.answer("Some channels could not be automatically verified. Please join them and press Retry.", reply_markup=kb2)
            return

        files = sql_get_session_files(s["id"])
        delivered_msg_ids = []
        owner_is_requester = (message.from_user.id == s.get("owner_id"))
        protect_flag = s.get("protect", 0)
        for f in files:
            try:
                if f["file_type"] == "text":
                    m = await bot.send_message(message.chat.id, f.get("caption") or "")
                    delivered_msg_ids.append(m.message_id)
                else:
                    try:
                        m = await bot.copy_message(message.chat.id, UPLOAD_CHANNEL_ID, f["vault_msg_id"],
                                                   caption=f.get("caption") or "",
                                                   protect_content=bool(protect_flag) and not owner_is_requester)
                        delivered_msg_ids.append(m.message_id)
                    except Exception:
                        if f["file_type"] == "photo":
                            sent = await bot.send_photo(message.chat.id, f["file_id"], caption=f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
                        elif f["file_type"] == "video":
                            sent = await bot.send_video(message.chat.id, f["file_id"], caption=f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
                        elif f["file_type"] == "document":
                            sent = await bot.send_document(message.chat.id, f["file_id"], caption=f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
                        elif f["file_type"] == "sticker":
                            try:
                                sent = await bot.send_sticker(message.chat.id, f["file_id"])
                                delivered_msg_ids.append(sent.message_id)
                            except Exception:
                                pass
                        else:
                            sent = await bot.send_message(message.chat.id, f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
            except Exception:
                logger.exception("Error delivering file in session %s", s["id"])

        minutes = int(s.get("auto_delete_minutes", 0) or 0)
        if minutes and delivered_msg_ids:
            run_at = datetime.utcnow() + timedelta(minutes=minutes)
            job_db_id = sql_add_delete_job(s["id"], message.chat.id, delivered_msg_ids, run_at)
            scheduler.add_job(execute_delete_job, 'date', run_date=run_at,
                              args=(job_db_id, {"id": job_db_id, "message_ids": json.dumps(delivered_msg_ids),
                                                "target_chat_id": message.chat.id, "run_at": run_at.isoformat()}),
                              id=f"deljob_{job_db_id}")
            await message.answer(f"Messages will be auto-deleted in {minutes} minutes.")

        await message.answer("Delivery complete.")
    except Exception:
        logger.exception("Error in /start handler")
        await message.reply("An error occurred while processing your request.", parse_mode=None)

# -------------------------
# Upload commands (owner only)
# -------------------------
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    args = message.get_args().strip().lower()
    exclude_text = False
    if "exclude_text" in args:
        exclude_text = True
    start_upload_session(OWNER_ID, exclude_text)
    await message.reply("Upload session started. Send media/text you want included. Use /d to finalize, /e to cancel.", parse_mode=None)

@dp.message_handler(commands=["e"])
async def cmd_cancel_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    cancel_upload_session(OWNER_ID)
    await message.reply("Upload canceled.", parse_mode=None)

@dp.message_handler(commands=["d"])
async def cmd_finalize_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    upload = active_uploads.get(OWNER_ID)
    if not upload:
        await message.reply("No active upload session.", parse_mode=None)
        return
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("Protect ON", callback_data=cb_choose_protect.new(session="pending", choice="1")),
           InlineKeyboardButton("Protect OFF", callback_data=cb_choose_protect.new(session="pending", choice="0")))
    await message.reply("Choose Protect setting:", reply_markup=kb)
    upload["_finalize_requested"] = True

@dp.callback_query_handler(cb_choose_protect.filter())
async def _on_choose_protect(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    try:
        choice = int(callback_data.get("choice", "0"))
        if OWNER_ID not in active_uploads:
            await call.message.answer("Upload session expired.", parse_mode=None)
            return
        active_uploads[OWNER_ID]["_protect_choice"] = choice
        await call.message.answer("Enter auto-delete timer in minutes (0-10080). 0 = no auto-delete. Reply with a number (e.g., 60).", parse_mode=None)
    except Exception:
        logger.exception("Error in choose_protect callback")

@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and "_finalize_requested" in active_uploads.get(OWNER_ID, {}), content_types=types.ContentTypes.TEXT)
async def _receive_minutes(m: types.Message):
    try:
        txt = m.text.strip()
        try:
            mins = int(txt)
            if mins < 0 or mins > 10080:
                raise ValueError()
        except Exception:
            await m.reply("Please send a valid integer between 0 and 10080.", parse_mode=None)
            return
        upload = active_uploads.get(OWNER_ID)
        if not upload:
            await m.reply("Upload session missing.", parse_mode=None)
            return
        messages: List[types.Message] = upload.get("messages", [])
        protect = upload.get("_protect_choice", 0)

        try:
            header = await bot.send_message(UPLOAD_CHANNEL_ID, "Uploading session...")
        except ChatNotFound:
            await m.reply("Upload channel not found. Please ensure the bot is in the UPLOAD_CHANNEL.", parse_mode=None)
            logger.error("ChatNotFound uploading to UPLOAD_CHANNEL_ID")
            return
        header_msg_id = header.message_id
        header_chat_id = header.chat.id

        token = generate_token(8)
        attempt = 0
        while sql_get_session_by_token(token) is not None and attempt < 5:
            token = generate_token(8)
            attempt += 1

        session_temp_id = sql_insert_session(OWNER_ID, protect, mins, "Untitled", header_chat_id, header_msg_id, token)

        me = await bot.get_me()
        bot_username = me.username or db_get("bot_username") or ""
        deep_link = f"https://t.me/{bot_username}?start={token}"

        try:
            await bot.edit_message_text(f"Session {session_temp_id}\n{deep_link}", UPLOAD_CHANNEL_ID, header_msg_id)
        except Exception:
            pass

        for m0 in messages:
            try:
                if getattr(m0, "text", None) and m0.text.strip().startswith("/"):
                    continue

                if getattr(m0, "text", None) and (not upload.get("exclude_text")) and not (getattr(m0, "photo", None) or getattr(m0, "video", None) or getattr(m0, "document", None) or getattr(m0, "sticker", None) or getattr(m0, "animation", None)):
                    sent = await bot.send_message(UPLOAD_CHANNEL_ID, m0.text)
                    sql_add_file(session_temp_id, "text", "", m0.text or "", m0.message_id, sent.message_id)
                elif getattr(m0, "photo", None):
                    file_id = m0.photo[-1].file_id
                    sent = await bot.send_photo(UPLOAD_CHANNEL_ID, file_id, caption=m0.caption or "")
                    sql_add_file(session_temp_id, "photo", file_id, m0.caption or "", m0.message_id, sent.message_id)
                elif getattr(m0, "video", None):
                    file_id = m0.video.file_id
                    sent = await bot.send_video(UPLOAD_CHANNEL_ID, file_id, caption=m0.caption or "")
                    sql_add_file(session_temp_id, "video", file_id, m0.caption or "", m0.message_id, sent.message_id)
                elif getattr(m0, "document", None):
                    file_id = m0.document.file_id
                    sent = await bot.send_document(UPLOAD_CHANNEL_ID, file_id, caption=m0.caption or "")
                    sql_add_file(session_temp_id, "document", file_id, m0.caption or "", m0.message_id, sent.message_id)
                elif getattr(m0, "sticker", None):
                    file_id = m0.sticker.file_id
                    sent = await bot.send_sticker(UPLOAD_CHANNEL_ID, file_id)
                    sql_add_file(session_temp_id, "sticker", file_id, "", m0.message_id, sent.message_id)
                elif getattr(m0, "animation", None):
                    file_id = m0.animation.file_id
                    sent = await bot.send_animation(UPLOAD_CHANNEL_ID, file_id, caption=m0.caption or "")
                    sql_add_file(session_temp_id, "animation", file_id, m0.caption or "", m0.message_id, sent.message_id)
                else:
                    try:
                        sent = await bot.copy_message(UPLOAD_CHANNEL_ID, m0.chat.id, m0.message_id)
                        caption = getattr(m0, "caption", None) or getattr(m0, "text", "") or ""
                        sql_add_file(session_temp_id, "other", "", caption or "", m0.message_id, sent.message_id)
                    except Exception:
                        logger.exception("Failed copying message during finalize")
            except Exception:
                logger.exception("Error copying message during finalize")

        cur = db.cursor()
        cur.execute("UPDATE sessions SET deep_link=?, header_msg_id=?, header_chat_id=? WHERE id=?", (token, header_msg_id, header_chat_id, session_temp_id))
        db.commit()

        await backup_db_to_channel()

        cancel_upload_session(OWNER_ID)
        await m.reply(f"Session finalized: {deep_link}", parse_mode=None)
        try:
            active_uploads.pop(OWNER_ID, None)
        except Exception:
            pass
        raise CancelHandler()
    except CancelHandler:
        raise
    except Exception:
        logger.exception("Error finalizing upload")
        await m.reply("An error occurred during finalization.", parse_mode=None)

# -------------------------
# Settings: setmessage, setimage, setchannel, help
# -------------------------
@dp.message_handler(commands=["setmessage"])
async def cmd_setmessage(message: types.Message):
    """
    Owner can set start/help texts.
    Usage:
      - Reply to a message with /setmessage start
      - Or send /setmessage start Your text here
    """
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    args_raw = message.get_args().strip()
    # if replied and no args, interpret target from command text
    if message.reply_to_message and (not args_raw):
        parts = message.text.strip().split()
        if len(parts) >= 2:
            target = parts[1].lower()
        else:
            await message.reply("Usage: reply to a text with `/setmessage start` or `/setmessage help`, or use `/setmessage start <text>`.", parse_mode=None)
            return
        if getattr(message.reply_to_message, "text", None):
            db_set(f"{target}_text", message.reply_to_message.text)
            await message.reply(f"{target} message updated.", parse_mode=None)
            return
    parts = args_raw.split(" ", 1)
    if not parts or not parts[0]:
        await message.reply("Usage: /setmessage start <text> OR reply to a message with `/setmessage start`.", parse_mode=None)
        return
    target = parts[0].lower()
    if len(parts) == 1:
        await message.reply("Provide the message text after the target or reply to a message containing the text.", parse_mode=None)
        return
    txt = parts[1]
    db_set(f"{target}_text", txt)
    await message.reply(f"{target} message updated.", parse_mode=None)

@dp.message_handler(commands=["setimage"])
async def cmd_setimage(message: types.Message):
    """
    Owner can set image for start/help by replying with a photo/document/sticker/animation.
    Usage:
      - Reply to the media with /setimage start
      - Reply to the media with /setimage help
    """
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    if not message.reply_to_message:
        await message.reply("Reply to a photo/document/sticker/animation with `/setimage start` or `/setimage help`.", parse_mode=None)
        return
    parts = message.get_args().strip().split()
    target = parts[0].lower() if parts else "start"
    rt = message.reply_to_message
    file_id = None
    if getattr(rt, "photo", None):
        file_id = rt.photo[-1].file_id
    elif getattr(rt, "document", None):
        file_id = rt.document.file_id
    elif getattr(rt, "sticker", None):
        file_id = rt.sticker.file_id
    elif getattr(rt, "animation", None):
        file_id = rt.animation.file_id
    else:
        await message.reply("Reply must contain a photo, image document, sticker, or animation.", parse_mode=None)
        return
    db_set(f"{target}_image", file_id)
    await message.reply(f"{target} image set.", parse_mode=None)

@dp.message_handler(commands=["setchannel"])
async def cmd_setchannel(message: types.Message):
    """
    Set forced-join channels (max 3). Usage:
      /setchannel <name> <channel_link>
      /setchannel none  -> clears forced channels
    The bot will require joining these channels to access sessions.
    """
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /setchannel <name> <channel_link> OR /setchannel none", parse_mode=None)
        return
    if args.lower() == "none":
        db_set("force_channels", json.dumps([]))
        await message.reply("Forced channels cleared.", parse_mode=None)
        return
    parts = args.split(" ", 1)
    if len(parts) < 2:
        await message.reply("Provide name and link.", parse_mode=None)
        return
    name, link = parts[0].strip(), parts[1].strip()
    try:
        arr = json.loads(db_get("force_channels", "[]"))
    except Exception:
        arr = []
    updated = False
    for entry in arr:
        if entry.get("name") == name or entry.get("link") == link:
            entry["name"] = name
            entry["link"] = link
            updated = True
            break
    if not updated:
        if len(arr) >= 3:
            await message.reply("Max 3 forced channels allowed.", parse_mode=None)
            return
        arr.append({"name": name, "link": link})
    db_set("force_channels", json.dumps(arr))
    await message.reply("Forced channels updated.", parse_mode=None)

# -------------------------
# Help handlers
# -------------------------
@dp.callback_query_handler(cb_help_button.filter())
async def cb_help(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    txt = db_get("help_text", "Help is not set.")
    img = db_get("help_image")
    try:
        if img:
            # send photo with caption as plain text (no HTML parsing)
            await bot.send_photo(call.from_user.id, img, caption=txt)
        else:
            await bot.send_message(call.from_user.id, txt)
    except Exception:
        logger.exception("Failed to send help to user")
        try:
            await call.message.answer("Failed to open help.", parse_mode=None)
        except Exception:
            pass

@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    txt = db_get("help_text", "Help is not set.")
    img = db_get("help_image")
    if img:
        try:
            await message.reply_photo(img, caption=txt)
        except Exception:
            await message.reply(txt, parse_mode=None)
    else:
        await message.reply(txt, parse_mode=None)

# -------------------------
# Admin & utility commands
# -------------------------
@dp.message_handler(commands=["adminp"])
async def cmd_adminp(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    s = sql_stats()
    txt = (
        "Owner panel:\n"
        "/upload - start upload session\n"
        "/d - finalize upload (choose protect + minutes)\n"
        "/e - cancel upload\n"
        "/setmessage - set start/help text\n"
        "/setimage - set start/help image (reply to a photo/sticker/document)\n"
        "/setchannel - add forced join channel\n"
        "/stats - show stats\n"
        "/list_sessions - list sessions\n"
        "/revoke <id> - revoke a session\n"
        "/broadcast - reply to message to broadcast\n"
        "/backup_db - backup DB to DB channel\n"
        "/restore_db - restore DB from pinned\n\n"
        f"Stats: Active(2d): {s['active_2d']}  Total users: {s['total_users']}  Files: {s['files']}  Sessions: {s['sessions']}"
    )
    await message.reply(txt, parse_mode=None)

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    s = sql_stats()
    await message.reply(f"Active(2d): {s['active_2d']}\nTotal users: {s['total_users']}\nTotal files: {s['files']}\nSessions: {s['sessions']}", parse_mode=None)

@dp.message_handler(commands=["list_sessions"])
async def cmd_list_sessions(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    rows = sql_list_sessions(200)
    if not rows:
        await message.reply("No sessions.", parse_mode=None)
        return
    out = []
    for r in rows:
        out.append(f"ID:{r['id']} created:{r['created_at']} protect:{r['protect']} auto_min:{r['auto_delete_minutes']} revoked:{r['revoked']} token:{r['deep_link']}")
    msg = "\n".join(out)
    if len(msg) > 4000:
        await message.reply("Too many sessions to display.", parse_mode=None)
    else:
        await message.reply(msg, parse_mode=None)

@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /revoke <id>", parse_mode=None)
        return
    try:
        sid = int(args)
    except Exception:
        await message.reply("Invalid id", parse_mode=None)
        return
    sql_set_session_revoked(sid, 1)
    await message.reply(f"Session {sid} revoked.", parse_mode=None)

@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    if not message.reply_to_message:
        await message.reply("Reply to the message you want to broadcast.", parse_mode=None)
        return

    cur = db.cursor()
    cur.execute("SELECT id FROM users")
    users = [r["id"] for r in cur.fetchall()]
    if not users:
        await message.reply("No users to broadcast to.", parse_mode=None)
        return
    await message.reply(f"Starting broadcast to {len(users)} users.", parse_mode=None)
    sem = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    lock = asyncio.Lock()
    stats = {"success": 0, "failed": 0, "removed": []}

    async def worker(uid):
        nonlocal stats
        async with sem:
            try:
                await bot.copy_message(uid, message.chat.id, message.reply_to_message.message_id)
                async with lock:
                    stats["success"] += 1
            except BotBlocked:
                sql_remove_user(uid)
                async with lock:
                    stats["removed"].append(uid)
            except ChatNotFound:
                sql_remove_user(uid)
                async with lock:
                    stats["removed"].append(uid)
            except BadRequest:
                async with lock:
                    stats["failed"] += 1
            except RetryAfter as e:
                logger.warning("Broadcast RetryAfter %s seconds", e.timeout)
                await asyncio.sleep(e.timeout + 1)
                try:
                    await bot.copy_message(uid, message.chat.id, message.reply_to_message.message_id)
                    async with lock:
                        stats["success"] += 1
                except Exception:
                    async with lock:
                        stats["failed"] += 1
            except Exception:
                async with lock:
                    stats["failed"] += 1

    tasks = [worker(u) for u in users]
    await asyncio.gather(*tasks)
    removed_count = len(stats["removed"])
    await message.reply(f"Broadcast complete. Success: {stats['success']} Failed: {stats['failed']} Removed: {removed_count}", parse_mode=None)
    if removed_count:
        r_sample = stats["removed"][:10]
        await bot.send_message(OWNER_ID, f"Broadcast removed {removed_count} users (e.g. {r_sample}). These users were removed from DB.", parse_mode=None)

@dp.message_handler(commands=["backup_db"])
async def cmd_backup_db(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    sent = await backup_db_to_channel()
    if sent:
        await message.reply("DB backed up.", parse_mode=None)
    else:
        await message.reply("Backup failed.", parse_mode=None)

@dp.message_handler(commands=["restore_db"])
async def cmd_restore_db(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    ok = await restore_db_from_pinned()
    if ok:
        await message.reply("DB restored.", parse_mode=None)
    else:
        await message.reply("Restore failed.", parse_mode=None)

@dp.message_handler(commands=["del_session"])
async def cmd_del_session(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /del_session <id>", parse_mode=None)
        return
    try:
        sid = int(args)
    except Exception:
        await message.reply("Invalid id", parse_mode=None)
        return
    cur = db.cursor()
    cur.execute("DELETE FROM sessions WHERE id=?", (sid,))
    db.commit()
    await message.reply("Session deleted.", parse_mode=None)

# -------------------------
# Callback retry handler
# -------------------------
@dp.callback_query_handler(cb_retry.filter())
async def cb_retry_handler(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    session_id = int(callback_data.get("session"))
    await call.message.answer("Please re-open the deep link you received (tap it in chat) to retry delivery. If channels are joined, delivery should proceed.", parse_mode=None)

# -------------------------
# Error handler
# -------------------------
@dp.errors_handler()
async def global_error_handler(update, exception):
    logger.exception("Update handling failed: %s", exception)
    return True

# -------------------------
# Catch-all uploader & user-lastseen handler (SAFE)
# -------------------------
def _upload_or_noncommand_filter(m: types.Message) -> bool:
    if m.from_user and m.from_user.id == OWNER_ID:
        return OWNER_ID in active_uploads
    if getattr(m, "text", None):
        return not m.text.startswith("/")
    return True

@dp.message_handler(_upload_or_noncommand_filter, content_types=types.ContentTypes.ANY)
async def catch_all_store_uploads(message: types.Message):
    try:
        if message.from_user.id != OWNER_ID:
            sql_update_user_lastseen(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "", message.from_user.last_name or "")
            return
        if OWNER_ID in active_uploads:
            if getattr(message, "text", None) and message.text.strip().startswith("/"):
                return
            if message.text and active_uploads[OWNER_ID].get("exclude_text"):
                pass
            else:
                append_upload_message(OWNER_ID, message)
                try:
                    await message.reply("Stored in upload session.", parse_mode=None)
                except Exception:
                    pass
    except Exception:
        logger.exception("Error in catch_all_store_uploads")

# -------------------------
# Startup & shutdown
# -------------------------
async def auto_backup_job():
    try:
        await backup_db_to_channel()
    except Exception:
        logger.exception("Auto backup failed")

async def on_startup(dispatcher):
    try:
        await restore_db_from_pinned()
    except Exception:
        logger.exception("restore_db_from_pinned error on startup")
    try:
        scheduler.start()
    except Exception:
        logger.exception("Scheduler start error")
    try:
        await restore_pending_jobs_and_schedule()
    except Exception:
        logger.exception("restore_pending_jobs_and_schedule error")
    try:
        try:
            scheduler.add_job(auto_backup_job, 'interval', hours=AUTO_BACKUP_HOURS, id="auto_backup")
        except Exception:
            pass
    except Exception:
        logger.exception("Failed scheduling auto_backup_job")
    try:
        asyncio.create_task(run_health_app())
    except Exception:
        logger.exception("Failed to start health app task")
    try:
        await bot.get_chat(UPLOAD_CHANNEL_ID)
    except ChatNotFound:
        logger.error("Upload channel not found. Please add the bot to the upload channel.")
    except Exception:
        logger.exception("Error checking upload channel")
    try:
        await bot.get_chat(DB_CHANNEL_ID)
    except ChatNotFound:
        logger.error("DB channel not found. Please add the bot to the DB channel.")
    except Exception:
        logger.exception("Error checking DB channel")
    try:
        me = await bot.get_me()
        db_set("bot_username", me.username or "")
    except Exception:
        logger.exception("Failed to get bot info on startup")
    if db_get("start_text") is None:
        db_set("start_text", "Welcome, {first_name}!")
    if db_get("help_text") is None:
        db_set("help_text", "This bot delivers sessions.")

    try:
        commands = [
            BotCommand("start", "Start / open deep link"),
            BotCommand("help", "Show help"),
            BotCommand("upload", "Start upload session (owner)"),
            BotCommand("d", "Finalize upload (owner)"),
            BotCommand("e", "Cancel upload (owner)"),
            BotCommand("setmessage", "Set start/help messages (owner)"),
            BotCommand("setimage", "Set start/help images (owner)"),
            BotCommand("setchannel", "Set forced channel (owner)"),
            BotCommand("adminp", "Owner panel"),
            BotCommand("stats", "Stats (owner)"),
            BotCommand("list_sessions", "List sessions (owner)"),
            BotCommand("revoke", "Revoke session (owner)"),
            BotCommand("broadcast", "Broadcast (owner)"),
            BotCommand("backup_db", "Backup DB (owner)"),
            BotCommand("restore_db", "Restore DB (owner)"),
            BotCommand("del_session", "Delete a session (owner)")
        ]
        await bot.set_my_commands(commands)
    except Exception:
        logger.exception("Couldn't set bot commands")

    logger.info("on_startup complete")

async def on_shutdown(dispatcher):
    logger.info("Shutting down")
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    try:
        await bot.close()
    except Exception:
        pass

# -------------------------
# Run (polling)
# -------------------------
if __name__ == "__main__":
    try:
        executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown, skip_updates=True)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Stopped by user")
    except Exception:
        logger.exception("Fatal error")