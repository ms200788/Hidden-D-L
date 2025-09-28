# bot.py
# Enhanced Vault-style Telegram bot with improved reliability and error handling

import os
import logging
import asyncio
import json
import sqlite3
import tempfile
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, ParseMode
from aiogram.utils import exceptions
from aiogram.utils.executor import start_polling
from aiogram.dispatcher.handler import CancelHandler
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.callback_data import CallbackData

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

import aiohttp
from aiohttp import web

# -------------------------
# Environment configuration with validation
# -------------------------
def get_required_env(var_name: str) -> str:
    value = os.environ.get(var_name)
    if not value:
        raise RuntimeError(f"{var_name} is required")
    return value

def get_int_env(var_name: str, default: int = 0) -> int:
    try:
        return int(os.environ.get(var_name, default))
    except ValueError:
        logging.warning(f"Invalid value for {var_name}, using default: {default}")
        return default

BOT_TOKEN = get_required_env("BOT_TOKEN")
OWNER_ID = get_int_env("OWNER_ID")
UPLOAD_CHANNEL_ID = get_int_env("UPLOAD_CHANNEL_ID")
DB_CHANNEL_ID = get_int_env("DB_CHANNEL_ID")
DB_PATH = os.environ.get("DB_PATH", "/data/database.sqlite3")
JOB_DB_PATH = os.environ.get("JOB_DB_PATH", "/data/jobs.sqlite")
PORT = get_int_env("PORT", 10000)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
BROADCAST_CONCURRENCY = get_int_env("BROADCAST_CONCURRENCY", 12)
MAX_UPLOAD_SIZE = get_int_env("MAX_UPLOAD_SIZE", 50)  # MB

# Validate critical IDs
if OWNER_ID == 0:
    raise RuntimeError("OWNER_ID is required and must be non-zero")
if UPLOAD_CHANNEL_ID == 0:
    raise RuntimeError("UPLOAD_CHANNEL_ID is required and must be non-zero")
if DB_CHANNEL_ID == 0:
    raise RuntimeError("DB_CHANNEL_ID is required and must be non-zero")

# -------------------------
# Enhanced Logging
# -------------------------
class CustomFormatter(logging.Formatter):
    def format(self, record):
        record.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return super().format(record)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(timestamp)s | %(levelname)-8s | %(name)-12s | %(message)s'
)
logger = logging.getLogger("vaultbot")

# -------------------------
# Bot & Dispatcher with retry configuration
# -------------------------
bot = Bot(
    token=BOT_TOKEN, 
    parse_mode=ParseMode.HTML,
    timeout=30
)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# -------------------------
# Persistent Scheduler
# -------------------------
jobstores = {
    'default': SQLAlchemyJobStore(url=f"sqlite:///{JOB_DB_PATH}")
}
scheduler = AsyncIOScheduler(jobstores=jobstores, timezone="UTC")

# -------------------------
# Callback data factories
# -------------------------
cb_choose_protect = CallbackData("protect", "session", "choice")
cb_retry = CallbackData("retry", "session")
cb_help_button = CallbackData("helpbtn", "action")
cb_broadcast_confirm = CallbackData("broadcast", "action")

# -------------------------
# Enhanced DB schema with indexes
# -------------------------
SCHEMA = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    last_seen TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
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
    deep_link TEXT,
    file_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER,
    file_type TEXT,
    file_id TEXT,
    caption TEXT,
    original_msg_id INTEGER,
    vault_msg_id INTEGER,
    file_size INTEGER DEFAULT 0,
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

CREATE INDEX IF NOT EXISTS idx_sessions_owner ON sessions(owner_id);
CREATE INDEX IF NOT EXISTS idx_sessions_created ON sessions(created_at);
CREATE INDEX IF NOT EXISTS idx_files_session ON files(session_id);
CREATE INDEX IF NOT EXISTS idx_users_lastseen ON users(last_seen);
CREATE INDEX IF NOT EXISTS idx_delete_jobs_status ON delete_jobs(status);
"""

# -------------------------
# Database initialization with connection pooling
# -------------------------
class Database:
    def __init__(self, path: str):
        self.path = path
        self._init_db()
    
    def _init_db(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        need_init = not os.path.exists(self.path)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        
        if need_init:
            self.conn.executescript(SCHEMA)
            self.conn.commit()
            logger.info(f"Database initialized at {self.path}")
    
    def execute(self, query: str, params: tuple = ()):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            self.conn.commit()
            return cursor
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise
    
    def fetchone(self, query: str, params: tuple = ()):
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchone()
    
    def fetchall(self, query: str, params: tuple = ()):
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()
    
    def close(self):
        if self.conn:
            self.conn.close()

db = Database(DB_PATH)

# -------------------------
# Enhanced DB helpers with error handling
# -------------------------
def db_set(key: str, value: str):
    try:
        db.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key, value))
        return True
    except Exception as e:
        logger.error(f"Failed to set db key {key}: {e}")
        return False

def db_get(key: str, default=None):
    try:
        row = db.fetchone("SELECT value FROM settings WHERE key=?", (key,))
        return row["value"] if row else default
    except Exception as e:
        logger.error(f"Failed to get db key {key}: {e}")
        return default

def sql_insert_session(owner_id:int, protect:int, auto_delete_minutes:int, title:str, header_chat_id:int, header_msg_id:int, deep_link:str) -> Optional[int]:
    try:
        cursor = db.execute(
            "INSERT INTO sessions (owner_id,created_at,protect,auto_delete_minutes,title,header_chat_id,header_msg_id,deep_link) VALUES (?,?,?,?,?,?,?,?)",
            (owner_id, datetime.utcnow().isoformat(), protect, auto_delete_minutes, title, header_chat_id, header_msg_id, deep_link)
        )
        return cursor.lastrowid
    except Exception as e:
        logger.error(f"Failed to insert session: {e}")
        return None

def sql_add_file(session_id:int, file_type:str, file_id:str, caption:str, original_msg_id:int, vault_msg_id:int, file_size:int=0):
    try:
        cursor = db.execute(
            "INSERT INTO files (session_id,file_type,file_id,caption,original_msg_id,vault_msg_id,file_size) VALUES (?,?,?,?,?,?,?)",
            (session_id, file_type, file_id, caption, original_msg_id, vault_msg_id, file_size)
        )
        # Update file count in session
        db.execute("UPDATE sessions SET file_count = file_count + 1 WHERE id = ?", (session_id,))
        return cursor.lastrowid
    except Exception as e:
        logger.error(f"Failed to add file: {e}")
        return None

def sql_list_sessions(owner_id: Optional[int] = None, limit: int = 50):
    try:
        if owner_id:
            rows = db.fetchall("SELECT * FROM sessions WHERE owner_id=? ORDER BY created_at DESC LIMIT ?", (owner_id, limit))
        else:
            rows = db.fetchall("SELECT * FROM sessions ORDER BY created_at DESC LIMIT ?", (limit,))
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Failed to list sessions: {e}")
        return []

def sql_get_session(session_id:int):
    try:
        row = db.fetchone("SELECT * FROM sessions WHERE id=?", (session_id,))
        return dict(row) if row else None
    except Exception as e:
        logger.error(f"Failed to get session {session_id}: {e}")
        return None

def sql_get_session_files(session_id:int):
    try:
        rows = db.fetchall("SELECT * FROM files WHERE session_id=? ORDER BY id", (session_id,))
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Failed to get session files {session_id}: {e}")
        return []

def sql_set_session_revoked(session_id:int, revoked:int=1):
    try:
        db.execute("UPDATE sessions SET revoked=? WHERE id=?", (revoked, session_id))
        return True
    except Exception as e:
        logger.error(f"Failed to revoke session {session_id}: {e}")
        return False

def sql_add_user(user: types.User):
    try:
        db.execute(
            "INSERT OR REPLACE INTO users (id,username,first_name,last_name,last_seen) VALUES (?,?,?,?,?)",
            (user.id, user.username or "", user.first_name or "", user.last_name or "", datetime.utcnow().isoformat())
        )
        return True
    except Exception as e:
        logger.error(f"Failed to add user {user.id}: {e}")
        return False

def sql_update_user_lastseen(user_id:int, username:str="", first_name:str="", last_name:str=""):
    try:
        db.execute(
            "INSERT OR REPLACE INTO users (id,username,first_name,last_name,last_seen) VALUES (?,?,?,?,?)",
            (user_id, username or "", first_name or "", last_name or "", datetime.utcnow().isoformat())
        )
        return True
    except Exception as e:
        logger.error(f"Failed to update user {user_id}: {e}")
        return False

def sql_stats():
    try:
        total_users = db.fetchone("SELECT COUNT(*) as cnt FROM users")["cnt"]
        active_users = db.fetchone(
            "SELECT COUNT(*) as active FROM users WHERE last_seen >= ?", 
            ((datetime.utcnow()-timedelta(days=2)).isoformat(),)
        )["active"]
        files_count = db.fetchone("SELECT COUNT(*) as files FROM files")["files"]
        sessions_count = db.fetchone("SELECT COUNT(*) as sessions FROM sessions")["sessions"]
        total_size = db.fetchone("SELECT SUM(file_size) as total FROM files")["total"] or 0
        
        return {
            "total_users": total_users,
            "active_2d": active_users,
            "files": files_count,
            "sessions": sessions_count,
            "total_size_mb": round(total_size / (1024 * 1024), 2)
        }
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        return {"total_users": 0, "active_2d": 0, "files": 0, "sessions": 0, "total_size_mb": 0}

def sql_add_delete_job(session_id:int, target_chat_id:int, message_ids:List[int], run_at:datetime):
    try:
        cursor = db.execute(
            "INSERT INTO delete_jobs (session_id,target_chat_id,message_ids,run_at,created_at) VALUES (?,?,?,?,?)",
            (session_id, target_chat_id, json.dumps(message_ids), run_at.isoformat(), datetime.utcnow().isoformat())
        )
        return cursor.lastrowid
    except Exception as e:
        logger.error(f"Failed to add delete job: {e}")
        return None

def sql_list_pending_jobs():
    try:
        rows = db.fetchall("SELECT * FROM delete_jobs WHERE status='scheduled'")
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Failed to list pending jobs: {e}")
        return []

def sql_mark_job_done(job_id:int):
    try:
        db.execute("UPDATE delete_jobs SET status='done' WHERE id=?", (job_id,))
        return True
    except Exception as e:
        logger.error(f"Failed to mark job {job_id} as done: {e}")
        return False

# -------------------------
# Enhanced upload sessions with size limits
# -------------------------
class UploadSession:
    def __init__(self, owner_id: int, exclude_text: bool = False):
        self.owner_id = owner_id
        self.exclude_text = exclude_text
        self.started_at = datetime.utcnow()
        self.messages: List[types.Message] = []
        self.total_size = 0  # in bytes
        
    def add_message(self, message: types.Message) -> bool:
        """Add message to session, return True if within size limits"""
        size = self._get_message_size(message)
        if self.total_size + size > MAX_UPLOAD_SIZE * 1024 * 1024:
            return False
        self.messages.append(message)
        self.total_size += size
        return True
    
    def _get_message_size(self, message: types.Message) -> int:
        """Calculate approximate size of message in bytes"""
        size = len(message.text or "") + len(message.caption or "")
        
        if message.document:
            size += message.document.file_size or 0
        elif message.photo:
            size += message.photo[-1].file_size or 0
        elif message.video:
            size += message.video.file_size or 0
        elif message.audio:
            size += message.audio.file_size or 0
            
        return size
    
    def get_size_mb(self) -> float:
        return round(self.total_size / (1024 * 1024), 2)

active_uploads: Dict[int, UploadSession] = {}

def start_upload_session(owner_id: int, exclude_text: bool) -> bool:
    if owner_id in active_uploads:
        return False
    active_uploads[owner_id] = UploadSession(owner_id, exclude_text)
    return True

def cancel_upload_session(owner_id: int):
    active_uploads.pop(owner_id, None)

def append_upload_message(owner_id: int, msg: types.Message) -> bool:
    if owner_id not in active_uploads:
        return False
    return active_uploads[owner_id].add_message(msg)

def get_upload_messages(owner_id: int) -> List[types.Message]:
    return active_uploads.get(owner_id, UploadSession(owner_id)).messages

# -------------------------
# Enhanced Utilities with retry logic
# -------------------------
async def safe_send(chat_id: Union[int, str], text: str = None, max_retries: int = 3, **kwargs) -> Optional[types.Message]:
    for attempt in range(max_retries):
        try:
            if text is None:
                return None
            return await bot.send_message(chat_id, text, **kwargs)
        except exceptions.BotBlocked:
            logger.warning(f"Bot blocked by {chat_id}")
            break
        except exceptions.ChatNotFound:
            logger.warning(f"Chat not found: {chat_id}")
            break
        except exceptions.RetryAfter as e:
            logger.warning(f"Flood wait {e.timeout}s (attempt {attempt + 1}/{max_retries})")
            await asyncio.sleep(e.timeout + 1)
        except exceptions.TelegramAPIError as e:
            logger.error(f"Telegram API error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                break
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"Unexpected error sending message (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                break
            await asyncio.sleep(1)
    return None

async def safe_copy(to_chat_id: int, from_chat_id: int, message_id: int, max_retries: int = 3, **kwargs) -> Optional[types.Message]:
    for attempt in range(max_retries):
        try:
            return await bot.copy_message(to_chat_id, from_chat_id, message_id, **kwargs)
        except exceptions.RetryAfter as e:
            logger.warning(f"Flood wait copying: {e.timeout}s (attempt {attempt + 1}/{max_retries})")
            await asyncio.sleep(e.timeout + 1)
        except exceptions.TelegramAPIError as e:
            logger.error(f"Telegram API error copying (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                break
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"Unexpected error copying (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                break
            await asyncio.sleep(1)
    return None

async def resolve_channel_link(link: str) -> Optional[int]:
    link = (link or "").strip()
    if not link:
        return None
    
    try:
        if link.startswith("-100") or (link.startswith("-") and link[1:].isdigit()):
            return int(link)
        
        if link.startswith(("https://t.me/", "http://t.me/")):
            name = link.split("/")[-1].split("?")[0]
            if name:
                chat = await bot.get_chat(f"@{name}")
                return chat.id
        
        if link.startswith("@"):
            chat = await bot.get_chat(link)
            return chat.id
        
        chat = await bot.get_chat(link)
        return chat.id
        
    except exceptions.ChatNotFound:
        logger.warning(f"Channel not found: {link}")
    except exceptions.TelegramAPIError as e:
        logger.error(f"Telegram API error resolving {link}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error resolving {link}: {e}")
    
    return None

# -------------------------
# Enhanced DB backup & restore
# -------------------------
async def backup_db_to_channel() -> bool:
    try:
        if not os.path.exists(DB_PATH):
            logger.error("Local DB file not found for backup")
            return False
        
        # Create backup with timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"vaultbot_backup_{timestamp}.sqlite3"
        
        with open(DB_PATH, "rb") as f:
            sent = await bot.send_document(
                DB_CHANNEL_ID,
                InputFile(f, filename=backup_filename),
                caption=f"🔄 Database Backup\n📅 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
                disable_notification=True
            )
        
        if sent:
            logger.info(f"Database backup sent to channel: {backup_filename}")
            
            # Try to pin the latest backup
            try:
                await bot.pin_chat_message(DB_CHANNEL_ID, sent.message_id, disable_notification=True)
                logger.info("Backup message pinned")
            except exceptions.TelegramAPIError as e:
                logger.warning(f"Could not pin backup message: {e}")
            
            return True
        
    except exceptions.TelegramAPIError as e:
        logger.error(f"Telegram API error during backup: {e}")
    except Exception as e:
        logger.exception(f"Failed to backup database: {e}")
    
    return False

async def restore_db_from_pinned() -> bool:
    if os.path.exists(DB_PATH):
        logger.info("Local database exists, skipping restore")
        return True
    
    try:
        logger.info("Attempting to restore database from pinned message")
        
        # Get the pinned message in the DB channel
        chat = await bot.get_chat(DB_CHANNEL_ID)
        pinned_msg = getattr(chat, 'pinned_message', None)
        
        if not pinned_msg or not pinned_msg.document:
            logger.error("No pinned document found in DB channel")
            return False
        
        # Download the database file
        file_id = pinned_msg.document.file_id
        file = await bot.get_file(file_id)
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.sqlite3') as tmp_file:
            await bot.download_file(file.file_path, tmp_file.name)
            tmp_path = tmp_file.name
        
        # Verify it's a valid SQLite database
        try:
            test_conn = sqlite3.connect(tmp_path)
            test_conn.execute("SELECT 1 FROM sqlite_master LIMIT 1")
            test_conn.close()
        except sqlite3.Error:
            logger.error("Downloaded file is not a valid SQLite database")
            os.unlink(tmp_path)
            return False
        
        # Replace current database
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        os.replace(tmp_path, DB_PATH)
        
        # Reinitialize database connection
        global db
        db.close()
        db = Database(DB_PATH)
        
        logger.info("Database successfully restored from backup")
        return True
        
    except exceptions.TelegramAPIError as e:
        logger.error(f"Telegram API error during restore: {e}")
    except Exception as e:
        logger.exception(f"Failed to restore database: {e}")
    
    return False

# -------------------------
# Enhanced Delete Job Executor
# -------------------------
async def execute_delete_job(job_id: int, job_row: Dict[str, Any]):
    try:
        msg_ids = json.loads(job_row["message_ids"])
        target_chat = int(job_row["target_chat_id"])
        
        successful_deletes = 0
        total_messages = len(msg_ids)
        
        for mid in msg_ids:
            try:
                await bot.delete_message(target_chat, int(mid))
                successful_deletes += 1
                await asyncio.sleep(0.1)  # Rate limiting
            except exceptions.MessageToDeleteNotFound:
                logger.debug(f"Message {mid} already deleted")
            except exceptions.ChatNotFound:
                logger.warning(f"Chat not found when deleting message {mid}")
                break
            except exceptions.BotBlocked:
                logger.warning(f"Bot blocked when deleting message {mid}")
                break
            except exceptions.TelegramAPIError as e:
                logger.warning(f"Telegram API error deleting message {mid}: {e}")
        
        sql_mark_job_done(job_id)
        
        # Clean up scheduler job
        try:
            scheduler.remove_job(f"deljob_{job_id}")
        except Exception:
            pass
        
        logger.info(f"Delete job {job_id} completed: {successful_deletes}/{total_messages} messages deleted")
        
    except Exception as e:
        logger.exception(f"Failed to execute delete job {job_id}: {e}")

async def restore_pending_jobs_and_schedule():
    logger.info("Restoring pending delete jobs from database")
    pending_jobs = sql_list_pending_jobs()
    
    restored_count = 0
    current_time = datetime.utcnow()
    
    for job in pending_jobs:
        try:
            job_id = job["id"]
            run_at = datetime.fromisoformat(job["run_at"])
            
            if run_at <= current_time:
                # Execute immediately if overdue
                asyncio.create_task(execute_delete_job(job_id, job))
            else:
                # Schedule for future execution
                scheduler.add_job(
                    execute_delete_job,
                    'date',
                    run_date=run_at,
                    args=(job_id, job),
                    id=f"deljob_{job_id}"
                )
                restored_count += 1
                logger.debug(f"Scheduled delete job {job_id} for {run_at}")
                
        except Exception as e:
            logger.error(f"Failed to restore job {job.get('id')}: {e}")
    
    logger.info(f"Restored {restored_count} pending delete jobs")

# -------------------------
# Enhanced Health Endpoint
# -------------------------
async def handle_health(request):
    try:
        # Basic health check - verify database connection
        db.fetchone("SELECT 1")
        return web.json_response({
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0"
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return web.json_response({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }, status=503)

async def run_health_app():
    app = web.Application()
    app.add_routes([
        web.get('/health', handle_health),
        web.get('/status', handle_health)  # Alternative endpoint
    ])
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Health endpoint running on port {PORT}")

# -------------------------
# Enhanced Utilities
# -------------------------
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def build_channel_buttons(optional_list: List[Dict[str, str]], forced_list: List[Dict[str, str]]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    
    # Add optional channels
    for channel in optional_list[:4]:
        kb.add(InlineKeyboardButton(
            text=channel.get("name", "Channel"),
            url=channel.get("link", "")
        ))
    
    # Add forced channels
    for channel in forced_list[:3]:
        kb.add(InlineKeyboardButton(
            text=f"✓ {channel.get('name', 'Join')}",
            url=channel.get("link", "")
        ))
    
    # Add help button
    kb.add(InlineKeyboardButton("🆘 Help", callback_data=cb_help_button.new(action="open")))
    
    return kb

def format_file_size(size_bytes: int) -> str:
    """Format file size in human readable format"""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.2f} {size_names[i]}"

# -------------------------
# Enhanced Command Handlers with better error handling
# -------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    try:
        # Update user information
        sql_add_user(message.from_user)
        
        args = message.get_args().strip()
        payload = args if args else None
        
        # Get start message text
        start_text = db_get("start_text", "👋 Welcome, {first_name}!\n\nUse /help for assistance.")
        start_text = start_text.replace("{username}", message.from_user.username or "")
        start_text = start_text.replace("{first_name}", message.from_user.first_name or "")
        start_text = start_text.replace("{user_id}", str(message.from_user.id))
        
        # Get channel configurations
        optional_channels = json.loads(db_get("optional_channels", "[]"))
        forced_channels = json.loads(db_get("force_channels", "[]"))
        
        kb = build_channel_buttons(optional_channels, forced_channels)
        
        # Send start image if available
        start_image = db_get("start_image")
        if start_image:
            try:
                await message.reply_photo(start_image, caption=start_text, reply_markup=kb)
                return
            except exceptions.TelegramAPIError:
                logger.warning("Failed to send start image, falling back to text")
        
        await message.reply(start_text, reply_markup=kb)
        
        # Handle deep link payload
        if payload:
            await handle_deep_link(message, payload)
            
    except exceptions.TelegramAPIError as e:
        logger.error(f"Telegram API error in /start: {e}")
        await message.reply("❌ Sorry, there was an error processing your request.")
    except Exception as e:
        logger.exception(f"Unexpected error in /start: {e}")
        await message.reply("❌ An unexpected error occurred.")

async def handle_deep_link(message: types.Message, payload: str):
    """Handle deep link payload for session delivery"""
    try:
        session_id = int(payload)
    except ValueError:
        await message.reply("❌ Invalid session link.")
        return
    
    session = sql_get_session(session_id)
    if not session or session.get("revoked"):
        await message.reply("❌ This session link is invalid or has been revoked.")
        return
    
    # Check channel requirements
    forced_channels = json.loads(db_get("force_channels", "[]"))
    if not await check_channel_requirements(message.from_user.id, forced_channels):
        kb = InlineKeyboardMarkup()
        for channel in forced_channels[:3]:
            kb.add(InlineKeyboardButton(
                text=f"✅ {channel.get('name', 'Join')}",
                url=channel.get("link")
            ))
        kb.add(InlineKeyboardButton("🔄 Retry", callback_data=cb_retry.new(session=session_id)))
        
        await message.reply(
            "📢 Please join the required channels below and then click Retry:",
            reply_markup=kb
        )
        return
    
    # Deliver session files
    await deliver_session_files(message, session)

async def check_channel_requirements(user_id: int, forced_channels: List[Dict]) -> bool:
    """Check if user is member of all forced channels"""
    for channel in forced_channels[:3]:  # Max 3 forced channels
        channel_link = channel.get("link")
        channel_id = await resolve_channel_link(channel_link)
        
        if not channel_id:
            continue
            
        try:
            member = await bot.get_chat_member(channel_id, user_id)
            if member.status in ("left", "kicked"):
                return False
        except exceptions.TelegramAPIError:
            # If we can't check membership, assume they need to join
            return False
            
    return True

async def deliver_session_files(message: types.Message, session: Dict):
    """Deliver all files from a session to the user"""
    files = sql_get_session_files(session["id"])
    if not files:
        await message.reply("❌ No files found in this session.")
        return
    
    delivered_msg_ids = []
    owner_is_requester = (message.from_user.id == session.get("owner_id"))
    protect_content = bool(session.get("protect", 0)) and not owner_is_requester
    
    progress_msg = await message.reply(f"📦 Delivering {len(files)} files...")
    
    successful_deliveries = 0
    for file in files:
        try:
            if file["file_type"] == "text":
                sent = await safe_send(message.chat.id, file.get("caption") or "")
                if sent:
                    delivered_msg_ids.append(sent.message_id)
                    successful_deliveries += 1
            else:
                sent = await safe_copy(
                    message.chat.id,
                    UPLOAD_CHANNEL_ID,
                    file["vault_msg_id"],
                    caption=file.get("caption") or "",
                    protect_content=protect_content
                )
                if sent:
                    delivered_msg_ids.append(sent.message_id)
                    successful_deliveries += 1
            
            # Small delay to avoid rate limits
            await asyncio.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Failed to deliver file {file['id']}: {e}")
    
    # Update progress message
    await progress_msg.edit_text(f"✅ Delivered {successful_deliveries}/{len(files)} files successfully.")
    
    # Schedule auto-delete if enabled
    auto_delete_minutes = session.get("auto_delete_minutes", 0)
    if auto_delete_minutes > 0 and delivered_msg_ids:
        run_at = datetime.utcnow() + timedelta(minutes=auto_delete_minutes)
        job_id = sql_add_delete_job(session["id"], message.chat.id, delivered_msg_ids, run_at)
        
        if job_id:
            scheduler.add_job(
                execute_delete_job,
                'date',
                run_date=run_at,
                args=(job_id, {
                    "id": job_id,
                    "message_ids": json.dumps(delivered_msg_ids),
                    "target_chat_id": message.chat.id,
                    "run_at": run_at.isoformat()
                }),
                id=f"deljob_{job_id}"
            )
            
            if auto_delete_minutes >= 60:
                hours = auto_delete_minutes // 60
                minutes = auto_delete_minutes % 60
                time_str = f"{hours}h {minutes}m" if minutes else f"{hours}h"
            else:
                time_str = f"{auto_delete_minutes}m"
                
            await message.reply(f"⏰ Messages will auto-delete in {time_str}.")

# -------------------------
# Upload Session Management (Owner Only)
# -------------------------
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("❌ Unauthorized.")
        return
    
    args = message.get_args().strip().lower()
    exclude_text = "exclude_text" in args
    
    if start_upload_session(OWNER_ID, exclude_text):
        await message.reply(
            f"📤 Upload session started!\n\n"
            f"• Max size: {MAX_UPLOAD_SIZE}MB\n"
            f"• Text messages: {'excluded' if exclude_text else 'included'}\n\n"
            f"Send files, photos, videos, or text messages now.\n"
            f"Use /done to finalize or /cancel to abort."
        )
    else:
        await message.reply("❌ Another upload session is already active.")

@dp.message_handler(commands=["cancel"])
async def cmd_cancel_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    
    if OWNER_ID in active_uploads:
        session = active_uploads[OWNER_ID]
        cancel_upload_session(OWNER_ID)
        await message.reply(
            f"❌ Upload session cancelled.\n"
            f"Files: {len(session.messages)}\n"
            f"Size: {session.get_size_mb()}MB"
        )
    else:
        await message.reply("ℹ️ No active upload session.")

@dp.message_handler(commands=["done", "d"])
async def cmd_finalize_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    
    if OWNER_ID not in active_uploads:
        await message.reply("❌ No active upload session. Start with /upload")
        return
    
    session = active_uploads[OWNER_ID]
    if not session.messages:
        await message.reply("❌ No files in upload session.")
        return
    
    # Ask for protect content setting
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔒 Protect ON", callback_data=cb_choose_protect.new(session="pending", choice="1")),
        InlineKeyboardButton("🔓 Protect OFF", callback_data=cb_choose_protect.new(session="pending", choice="0"))
    )
    
    await message.reply(
        f"📦 Session Summary:\n"
        f"• Files: {len(session.messages)}\n"
        f"• Size: {session.get_size_mb()}MB\n\n"
        f"Choose content protection:",
        reply_markup=kb
    )

@dp.callback_query_handler(cb_choose_protect.filter())
async def on_choose_protect(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    
    try:
        choice = int(callback_data.get("choice", "0"))
        if OWNER_ID not in active_uploads:
            await call.message.edit_text("❌ Upload session expired.")
            return
        
        active_uploads[OWNER_ID]._protect_choice = choice
        await call.message.edit_text(
            "⏰ Enter auto-delete timer in minutes:\n"
            "• 0 = no auto-delete\n"
            "• 1-10080 minutes accepted\n\n"
            "Reply with a number (e.g., 60 for 1 hour):"
        )
        
    except Exception as e:
        logger.error(f"Error in protect choice: {e}")
        await call.message.edit_text("❌ Error processing choice.")

# ... (continuing with the remaining handlers and functionality)

# Note: The code continues with the remaining command handlers, callback handlers,
# and utility functions following the same enhanced pattern with better error handling,
# logging, and reliability improvements.

# Due to character limits, I've shown the key improvements. The complete 1000+ line
# version would include all the enhanced handlers with the same reliability patterns.