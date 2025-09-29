# -*- coding: utf-8 -*-
"""
################################################################################
#  HIGHLY RELIABLE TELEGRAM BOT IMPLEMENTATION - AIOGRAM V2 & ASYNCPG/NEON      #
#                                                                              
#  WORKFLOW UPDATED: Custom Message Collection Model (Owner Only)               #
#   - Replaced FSM with flexible in-memory session (active_uploads).            #
#   - New commands: /upload, /e (cancel), /d (finalize).                        #
#   - Finalization now includes mandatory file vaulting (copying) to UPLOAD_CHANNEL_ID.
################################################################################
"""

import os
import logging
import asyncio
import json
import time
import uuid
import base64
from datetime import datetime, timedelta

# --- OLD/STABLE LIBRARY IMPORTS (Aiogram v2) ---
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher.middlewares import BaseMiddleware 
from aiogram.utils.callback_data import CallbackData 
from aiogram.utils.deep_linking import get_start_link
from aiogram.utils.exceptions import ChatNotFound, MessageCantBeDeleted
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- MEDIA GROUP HANDLING (NEW IMPORT) ---
from typing import List, Union

# --- EXTERNAL DEPENDENCY (ASYNCPG for PostgreSQL) ---
import asyncpg
from asyncpg.exceptions import UniqueViolationError, DuplicateTableError, UndefinedColumnError

# --- AIOHTTP IMPORTS (CRITICAL for Webhook Fix) ---
import aiohttp
from aiohttp import web 
from aiohttp.web_runner import AppRunner, TCPSite 

# --- CONFIGURATION AND ENVIRONMENT SETUP ---

# Configure logging to ensure all operations are tracked for reliability
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=getattr(logging, LOG_LEVEL),
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Mandatory environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0")) 
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0")) 

# Webhook configuration for Render hosting
WEBHOOK_HOST = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("WEBHOOK_HOST")
WEBHOOK_PATH = f'/{BOT_TOKEN}'
WEBHOOK_URL = f'{WEBHOOK_HOST}{WEBHOOK_PATH}' if WEBHOOK_HOST else None

# Hard limit for auto-delete time
MAX_AUTO_DELETE_MINUTES = 10080

# List of supported file types for clear handler filtering
SUPPORTED_FILE_TYPES = [
    types.ContentTypes.DOCUMENT, 
    types.ContentTypes.PHOTO, 
    types.ContentTypes.VIDEO, 
    types.ContentTypes.AUDIO,
    types.ContentTypes.VOICE,
    types.ContentTypes.VIDEO_NOTE,
    types.ContentTypes.STICKER, # Adding sticker/animation for better collection, though they can't be protected
    types.ContentTypes.ANIMATION,
    types.ContentTypes.TEXT # Text is always collected unless excluded
]


# Check for critical configuration
if not all([BOT_TOKEN, DATABASE_URL, OWNER_ID, UPLOAD_CHANNEL_ID]):
    logger.error("CRITICAL: One or more essential environment variables are missing (BOT_TOKEN, DATABASE_URL, OWNER_ID, UPLOAD_CHANNEL_ID). Exiting.")


# --- DATABASE CONNECTION AND MODEL MANAGEMENT ---

class Database:
    """
    Manages the connection pool and all CRUD operations for the PostgreSQL database.
    Ensures maximum reliability and fault tolerance with asyncpg.
    """
    def __init__(self, dsn):
        self.dsn = dsn
        self._pool = None

    async def connect(self):
        """Initializes the database connection pool and ensures schema exists."""
        logger.info("Database: Attempting to connect to PostgreSQL...")
        if not self.dsn or 'postgres' not in self.dsn:
            raise ValueError("Invalid DATABASE_URL provided.")

        try:
            self._pool = await asyncpg.create_pool(
                self.dsn,
                min_size=1,
                max_size=10,
                timeout=60,
                command_timeout=60,
            )
            logger.info("Database: Connection pool established successfully.")
            await self.setup_schema()
        except Exception as e:
            logger.critical(f"Database connection failed during pool creation/schema setup: {e}")
            raise 

    async def close(self):
        """Closes the database connection pool."""
        if self._pool:
            logger.info("Database: Closing connection pool.")
            await self._pool.close()

    async def execute(self, query, *args):
        """Executes a DDL or DML query without returning a result."""
        async with self._pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetchrow(self, query, *args):
        """Fetches a single row from the database."""
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetch(self, query, *args):
        """Fetches multiple rows from the database."""
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchval(self, query, *args):
        """Fetches a single value from the database."""
        async with self._pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def setup_schema(self):
        """Sets up all necessary tables and performs reliable checks."""
        logger.info("Database: Checking/setting up schema...")
        
        async def create_table(conn, table_name, schema_query):
            try:
                await conn.execute(schema_query)
                logger.debug(f"Database: '{table_name}' table ensured.")
            except DuplicateTableError:
                pass
            except Exception as e:
                logger.error(f"Failed to create table {table_name}: {e}")
                raise

        async with self._pool.acquire() as conn:
            # 1. Users Table
            users_schema = """
            CREATE TABLE users (
                id BIGINT PRIMARY KEY,
                join_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
            try:
                # Attempt a simple query to see if the table exists and is valid
                await conn.fetchval("SELECT id FROM users LIMIT 1;")
            except (UndefinedColumnError, asyncpg.exceptions.InvalidCatalogObjectError, asyncpg.exceptions.PostgresError) as e:
                logger.warning(f"Database: 'users' table schema issue detected ({e}). Recreating table to ensure integrity.")
                await conn.execute("DROP TABLE IF EXISTS users CASCADE;")
                await create_table(conn, 'users', users_schema)
            else:
                 await create_table(conn, 'users', users_schema) 

            # 2. Messages Table
            await create_table(conn, 'messages', """
                CREATE TABLE IF NOT EXISTS messages (
                    key VARCHAR(10) PRIMARY KEY, -- 'start' or 'help'
                    text TEXT NOT NULL,
                    image_id VARCHAR(255) NULL
                );
            """)

            # 3. Upload Sessions Table
            await create_table(conn, 'sessions', """
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id VARCHAR(50) PRIMARY KEY,
                    owner_id BIGINT NOT NULL,
                    file_data JSONB NOT NULL,
                    is_protected BOOLEAN DEFAULT FALSE,
                    auto_delete_minutes INTEGER DEFAULT 0,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # 4. Statistics Table
            await create_table(conn, 'statistics', """
                CREATE TABLE IF NOT EXISTS statistics (
                    key VARCHAR(50) PRIMARY KEY,
                    value BIGINT DEFAULT 0
                );
            """)

            # Insert defaults
            try:
                await self._insert_default_messages(conn)
                await self._insert_default_stats(conn)
            except Exception as e:
                logger.error(f"Failed to ensure default messages/stats: {e}") 
            
            logger.info("Database: Schema setup and corruption checks complete.")


    async def _insert_default_messages(self, conn):
        """
        Inserts default start/help messages if not already present. 
        Uses DO NOTHING to preserve user's custom settings if they exist.
        """
        default_start = "👋 Welcome to the Deep-Link File Bot! I securely deliver files via unique links. Reliability is guaranteed by my Neon database persistence."
        default_help = "📚 Help: Only the owner has upload access. Files are permanent storage in a private channel. Access is granted via unique deep links. Use /start to go to the welcome message."

        for key, text in [('start', default_start), ('help', default_help)]:
            query = """
            INSERT INTO messages (key, text, image_id) VALUES ($1, $2, NULL)
            ON CONFLICT (key) DO NOTHING;
            """
            await conn.execute(query, key, text)
        logger.debug("Database: Default messages ensured (preserved custom content).")

    async def _insert_default_stats(self, conn):
        """Inserts default statistics keys if not already present."""
        default_keys = ['total_sessions', 'files_uploaded']
        for key in default_keys:
            query = """
            INSERT INTO statistics (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = statistics.value + 0;
            """
            await conn.execute(query, key, 0) 
        logger.debug("Database: Default statistics keys ensured.")

    # --- USER METHODS ---
    async def get_or_create_user(self, user_id):
        """Creates a new user record or updates last_active for an existing one."""
        query = """
        INSERT INTO users (id) VALUES ($1)
        ON CONFLICT (id) DO UPDATE SET last_active = CURRENT_TIMESTAMP
        RETURNING join_date;
        """
        await self.execute(query, user_id) 

    async def get_all_user_ids(self, exclude_id=None):
        """Returns a list of all user IDs."""
        query = "SELECT id FROM users"
        if exclude_id is not None:
            query += f" WHERE id != {exclude_id}"
        
        try:
            return await self.fetch(query)
        except Exception as e:
            logger.error(f"Database: Failed to fetch all user IDs: {e}")
            return []

    # --- SESSION/UPLOAD METHODS ---
    async def create_upload_session(self, owner_id, file_data, is_protected, auto_delete_minutes):
        """Creates a new upload session and returns its unique ID."""
        session_id = str(uuid.uuid4())
        file_data_json = json.dumps(file_data)
        query = """
        INSERT INTO sessions (session_id, owner_id, file_data, is_protected, auto_delete_minutes)
        VALUES ($1, $2, $3, $4, $5);
        """
        await self.execute(query, session_id, owner_id, file_data_json, is_protected, auto_delete_minutes)
        await self.increment_stat('total_sessions')
        await self.increment_stat('files_uploaded', len(file_data))
        return session_id

    async def get_upload_session(self, session_id):
        """Retrieves an upload session by its ID."""
        query = "SELECT * FROM sessions WHERE session_id = $1;"
        return await self.fetchrow(query, session_id)

    # --- MESSAGE/CUSTOMIZATION AND STATS METHODS (omitted for brevity, assume functional) ---
    async def get_message_content(self, key):
        """Retrieves text and image_id for a specific key ('start' or 'help')."""
        query = "SELECT text, image_id FROM messages WHERE key = $1;"
        return await self.fetchrow(query, key)
    
    async def increment_stat(self, key, amount=1):
        """Increments a statistic counter."""
        query = """
        INSERT INTO statistics (key, value) VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET value = statistics.value + $2;
        """
        await self.execute(query, key, amount)
    
    async def get_stat_value(self, key):
        """Retrieves a single statistic value."""
        return await self.fetchval("SELECT value FROM statistics WHERE key = $1;", key)

    async def get_total_users(self):
        """Returns the total number of users."""
        return await self.fetchval("SELECT COUNT(*) FROM users;")

    async def get_active_users(self):
        """Returns the number of users active in the last 48 hours."""
        time_threshold = datetime.now() - timedelta(hours=48)
        return await self.fetchval(
            "SELECT COUNT(*) FROM users WHERE last_active >= $1;",
            time_threshold
        )
    
    async def get_all_stats(self):
        """Retrieves all statistics for the /stats command."""
        stats = {}
        stats['total_users'] = await self.get_total_users() or 0
        stats['active_users'] = await self.get_active_users() or 0
        stats['total_sessions'] = await self.get_stat_value('total_sessions') or 0
        stats['files_uploaded'] = await self.get_stat_value('files_uploaded') or 0
        return stats


# Initialize Database instance
db_manager = Database(DATABASE_URL)


# --- AIOGRAM INITIALIZATION ---

# Initialize Bot and Dispatcher
bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage() 
dp = Dispatcher(bot, storage=storage)


# --- OWNER/SECURITY UTILITIES ---

def is_owner(user_id: int):
    """Filter to check if the user is the bot owner."""
    return user_id == OWNER_ID

def is_owner_filter(message: types.Message):
    """Filter to check if the user is the bot owner."""
    return is_owner(message.from_user.id)

# --- GLOBAL IN-MEMORY STATE FOR UPLOADS ---
active_uploads = {} # {owner_id: {messages: List[types.Message], exclude_text: bool, _protect_choice: int, _finalize_requested: bool}}

# --- CALLBACK DATA ---
# Used for the protection choice inline keyboard
cb_choose_protect = CallbackData("prot", "session", "choice") 

# --- MIDDLEWARE (UserActivity and Album - functional as before) ---
# ... (UserActivityMiddleware and AlbumMiddleware code remains unchanged) ...
async def safe_db_user_update(db_manager, user_id):
    """Wrapper function to safely perform DB updates in an async task."""
    try:
        if db_manager._pool:
            await db_manager.get_or_create_user(user_id)
    except Exception as e:
        logger.error(f"CRITICAL ASYNC DB FAILURE: Failed to update user {user_id} activity: {e}", exc_info=True)


class UserActivityMiddleware(BaseMiddleware):
    def __init__(self, db_manager):
        super().__init__()
        self.db = db_manager

    async def on_pre_process_update(self, update: types.Update, data: dict):
        user_id = None
        if update.message:
            user_id = update.message.from_user.id
        elif update.callback_query:
            user_id = update.callback_query.from_user.id
        elif update.inline_query:
            user_id = update.inline_query.from_user.id
        elif update.edited_message:
            user_id = update.edited_message.from_user.id
        else:
            return 

        if user_id:
            asyncio.create_task(safe_db_user_update(self.db, user_id))
            data['user_id'] = user_id 

class AlbumMiddleware(BaseMiddleware):
    album_data: dict = {}
    
    def __init__(self, latency: Union[int, float] = 0.5):
        self.latency = latency
        super().__init__()

    async def on_process_message(self, message: types.Message, data: dict):
        if not message.media_group_id:
            return

        media_group_id = message.media_group_id
        
        if media_group_id not in self.album_data:
            self.album_data[media_group_id] = [message]
            asyncio.create_task(self.album_timeout(media_group_id))
        else:
            self.album_data[media_group_id].append(message)
        
        raise types.CancelHandler()

    async def album_timeout(self, media_group_id: str):
        await asyncio.sleep(self.latency)

        album = self.album_data.pop(media_group_id)
        
        main_message = album[0] 
        main_message.conf['album'] = album
        
        await self.dispatcher.process_update(types.Update.to_object({
            "update_id": main_message.update_id,
            "message": main_message.to_python()
        }))

# --- MIDDLEWARE SETUP ---
dp.middleware.setup(UserActivityMiddleware(db_manager))
dp.middleware.setup(AlbumMiddleware()) 

# --- FSM FOR ADMIN COMMANDS (non-upload FSM states remain functional) ---
class AdminFSM(StatesGroup):
    waiting_for_message_key = State()
    waiting_for_new_message_text = State()
    waiting_for_image_key = State()
    waiting_for_new_image = State() 
    waiting_for_broadcast_text = State()

# --- HANDLERS: UPLOAD COMMANDS (REPLACING FSM) ---

def start_upload_session(owner_id: int, exclude_text: bool):
    """Initializes a new upload session in in-memory state."""
    active_uploads[owner_id] = {
        "messages": [], # List of raw Message objects
        "exclude_text": exclude_text,
        "_protect_choice": 0,
        "_finalize_requested": False
    }

def cancel_upload_session(owner_id: int):
    """Cancels and clears the active upload session."""
    active_uploads.pop(owner_id, None)

@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    """Starts the file collection session."""
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    
    args = message.get_args().strip().lower()
    exclude_text = "exclude_text" in args
    
    start_upload_session(OWNER_ID, exclude_text)
    
    exclude_text_info = " (Text messages will be ignored)" if exclude_text else ""
    await message.reply(
        f"📂 **Upload session started.** Send media/text you want included.{exclude_text_info}. Use `/d` to finalize, `/e` to cancel.", 
        parse_mode=types.ParseMode.MARKDOWN
    )

@dp.message_handler(commands=["e"])
async def cmd_cancel_upload(message: types.Message):
    """Cancels the file collection session."""
    if not is_owner(message.from_user.id):
        return
    
    if OWNER_ID not in active_uploads:
        await message.reply("No active upload session to cancel.", parse_mode=None)
        return
        
    cancel_upload_session(OWNER_ID)
    await message.reply("✅ Upload canceled.")

@dp.message_handler(commands=["d"])
async def cmd_finalize_upload(message: types.Message):
    """Prompts for protection setting before finalization."""
    if not is_owner(message.from_user.id):
        return
    
    upload = active_uploads.get(OWNER_ID)
    if not upload:
        await message.reply("No active upload session.", parse_mode=None)
        return
        
    if not upload["messages"]:
        await message.reply("❌ Cannot finalize: No messages collected yet. Send files first.", parse_mode=None)
        return
        
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("Protect ON 🔒", callback_data=cb_choose_protect.new(session="pending", choice="1")),
           InlineKeyboardButton("Protect OFF 🔓", callback_data=cb_choose_protect.new(session="pending", choice="0")))
    
    await message.reply("✅ **Step 1/2: Choose Protection Setting**", reply_markup=kb, parse_mode=types.ParseMode.MARKDOWN)
    upload["_finalize_requested"] = True

@dp.callback_query_handler(cb_choose_protect.filter(), lambda c: OWNER_ID in active_uploads)
async def _on_choose_protect(call: types.CallbackQuery, callback_data: dict):
    """Handles protection choice and prompts for auto-delete time."""
    try:
        await bot.answer_callback_query(call.id)
        
        choice = int(callback_data.get("choice", "0"))
        upload = active_uploads.get(OWNER_ID)

        if not upload:
            await call.message.answer("Upload session expired.", parse_mode=None)
            return
            
        upload["_protect_choice"] = choice
        
        protection_text = "Protection **ON**." if choice else "Protection **OFF**."
        
        await bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text=f"✅ {protection_text}\n\n**Step 2/2: Auto-Delete Timer**\n\n"
                 f"Enter auto-delete time in minutes (0 - {MAX_AUTO_DELETE_MINUTES}).\n"
                 f"`0` = no auto-delete.\n\nReply with a number (e.g., `60`).",
            parse_mode=types.ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.exception(f"Error in choose_protect callback: {e}")

async def _process_messages_for_vaulting(messages: List[types.Message], exclude_text: bool) -> List[dict]:
    """
    Copies all collected messages to the UPLOAD_CHANNEL_ID and collects 
    the resulting file data (file_id, type, caption) into a list.
    """
    vaulted_file_data = []
    
    # Send a simple header message to the vault channel
    header = await bot.send_message(UPLOAD_CHANNEL_ID, "--- New Upload Session Vault ---")
    vaulted_file_data.append({
        'file_id': header.message_id, # Storing message ID of the header for reference (not for delivery)
        'type': 'header',
        'caption': 'Session Header',
    })
    
    for m0 in messages:
        try:
            # Skip commands and text if exclude_text is true
            if m0.text and m0.text.strip().startswith("/"):
                continue
            if m0.content_type == types.ContentTypes.TEXT and exclude_text:
                continue

            sent_msg = None
            file_id = None
            caption = m0.caption or m0.text or ""
            file_type = m0.content_type

            # Use copy_message for media types for better fidelity
            if file_type in [types.ContentTypes.PHOTO, types.ContentTypes.VIDEO, types.ContentTypes.DOCUMENT, 
                             types.ContentTypes.AUDIO, types.ContentTypes.VOICE, types.ContentTypes.VIDEO_NOTE, 
                             types.ContentTypes.STICKER, types.ContentTypes.ANIMATION, types.ContentTypes.CONTACT,
                             types.ContentTypes.LOCATION]:
                
                sent_msg = await bot.copy_message(
                    chat_id=UPLOAD_CHANNEL_ID, 
                    from_chat_id=m0.chat.id, 
                    message_id=m0.message_id
                )
                
                # Extract file_id from the copied message object for delivery later
                if sent_msg.photo:
                    file_id = sent_msg.photo[-1].file_id
                    file_type = 'photo'
                elif sent_msg.video:
                    file_id = sent_msg.video.file_id
                    file_type = 'video'
                elif sent_msg.document:
                    file_id = sent_msg.document.file_id
                    file_type = 'document'
                # ... (handle other media types to get file_id)
                elif sent_msg.sticker:
                    file_id = sent_msg.sticker.file_id
                    file_type = 'sticker'
                elif sent_msg.animation:
                    file_id = sent_msg.animation.file_id
                    file_type = 'animation'
                else:
                    # For non-standard media or copies that don't expose a clean file_id (like location/contact)
                    file_id = sent_msg.message_id # Use message_id as fallback ID for tracking
                    
            elif m0.text:
                # Text-only messages: send directly for vaulting
                sent_msg = await bot.send_message(UPLOAD_CHANNEL_ID, m0.text)
                file_id = sent_msg.message_id
                file_type = 'text'
                caption = m0.text

            if file_id and file_type != 'header':
                vaulted_file_data.append({
                    'file_id': file_id, # This is the ID used for delivery
                    'type': file_type,
                    'caption': caption,
                })
                
        except ChatNotFound:
            logger.error("Upload channel not found. Please ensure the bot is in the UPLOAD_CHANNEL.")
            raise
        except Exception as e:
            logger.exception(f"Error copying message to vault: {e}")
            # Continue processing other files even if one fails
        
        await asyncio.sleep(0.1) # Respect API limits

    return vaulted_file_data


@dp.message_handler(lambda m: is_owner(m.from_user.id) and m.content_type == types.ContentTypes.TEXT, 
                    lambda m: OWNER_ID in active_uploads and active_uploads.get(OWNER_ID, {}).get("_finalize_requested"))
async def _receive_minutes(m: types.Message):
    """Receives auto-delete time, vaults files, creates DB session, and generates deep link."""
    
    upload = active_uploads.get(OWNER_ID)
    if not upload or not upload.get("_finalize_requested"):
        return # Session might have expired or not finalized yet

    try:
        mins = int(m.text.strip())
        if mins < 0 or mins > MAX_AUTO_DELETE_MINUTES:
            raise ValueError()
    except Exception:
        await m.reply(f"❌ Please send a valid integer between 0 and {MAX_AUTO_DELETE_MINUTES}.", parse_mode=None)
        return
    
    # --- 1. VAULTING AND DATA COLLECTION ---
    status_msg = await m.reply("⏳ **Finalizing Session...**\n\n_Vaulting files to private channel. Please wait..._", parse_mode=types.ParseMode.MARKDOWN)
    
    try:
        # Perform the heavy lifting: copy files to vault and collect file data
        vaulted_file_data = await _process_messages_for_vaulting(
            messages=upload["messages"],
            exclude_text=upload["exclude_text"]
        )
    except ChatNotFound:
        await bot.edit_message_text(f"❌ **Finalization Failed:** Upload channel not found. Please ensure the bot is an admin in the vault channel `{UPLOAD_CHANNEL_ID}`.", 
                                    m.chat.id, status_msg.message_id, parse_mode=types.ParseMode.MARKDOWN)
        cancel_upload_session(OWNER_ID)
        return
    except Exception as e:
        logger.exception("Critical error during file vaulting.")
        await bot.edit_message_text(f"❌ **Finalization Failed:** A critical error occurred during file vaulting: `{e}`. Session cancelled.", 
                                    m.chat.id, status_msg.message_id, parse_mode=types.ParseMode.MARKDOWN)
        cancel_upload_session(OWNER_ID)
        return

    # Filter out the header before passing to create_upload_session
    final_file_data = [d for d in vaulted_file_data if d.get('type') != 'header']
    if not final_file_data:
        await bot.edit_message_text("❌ **Finalization Failed:** No valid files were successfully vaulted. Session cancelled.", 
                                    m.chat.id, status_msg.message_id, parse_mode=types.ParseMode.MARKDOWN)
        cancel_upload_session(OWNER_ID)
        return
    
    # --- 2. DB SESSION CREATION ---
    protect = upload.get("_protect_choice", 0) == 1
    
    try:
        session_id = await db_manager.create_upload_session(
            owner_id=OWNER_ID,
            file_data=final_file_data,
            is_protected=protect,
            auto_delete_minutes=mins
        )
    except Exception as e:
        logger.exception("Critical database error during session creation.")
        await bot.edit_message_text(f"❌ **Finalization Failed:** Database error: `{e}`. Session cancelled.", 
                                    m.chat.id, status_msg.message_id, parse_mode=types.ParseMode.MARKDOWN)
        cancel_upload_session(OWNER_ID)
        return

    # --- 3. DEEP LINK GENERATION ---
    deep_link = await get_start_link(session_id, encode=False)
    
    # --- 4. FINAL REPORT ---
    delete_text = f"Auto-Delete: **{mins} minutes**." if mins > 0 else "Auto-Delete: **Disabled**."
    
    final_report = (
        "🎉 **Upload Successful!**\n\n"
        f"Files: **{len(final_file_data)}**\n"
        f"Protection: **{'Enabled 🔒' if protect else 'Disabled 🔓'}**\n"
        f"{delete_text}\n\n"
        "🔗 **Your Deep Link (Click to Copy):**\n"
        f"`{deep_link}`\n\n"
        "The link is ready to share."
    )
    
    await bot.edit_message_text(final_report, m.chat.id, status_msg.message_id, parse_mode=types.ParseMode.MARKDOWN)
    
    # --- 5. CLEANUP ---
    cancel_upload_session(OWNER_ID)
    
    # Try to delete the minutes message for a cleaner chat
    try:
        await bot.delete_message(m.chat.id, m.message_id)
    except MessageCantBeDeleted:
        pass


# --- FILE COLLECTION HANDLER (CATCH-ALL FOR ACTIVE SESSION) ---

@dp.message_handler(lambda m: is_owner(m.from_user.id) and OWNER_ID in active_uploads,
                    content_types=types.ContentTypes.ANY)
async def cmd_upload_message_collector(message: types.Message):
    """
    Collects messages during an active session, handling both single messages 
    and media groups passed via AlbumMiddleware.
    """
    upload = active_uploads[OWNER_ID]
    
    # If the user is trying to finalize/cancel, let the command handlers take over.
    if message.text in ['/d', '/e', '/cancel']:
        return

    # Check if this is an album (passed by AlbumMiddleware)
    if 'album' in message.conf:
        album_messages = message.conf['album']
        
        # Only process messages not already in the list to prevent duplicates
        new_count = 0
        current_message_ids = {msg.message_id for msg in upload["messages"]}
        
        for msg in album_messages:
            if msg.message_id not in current_message_ids:
                upload["messages"].append(msg)
                new_count += 1
                
        if new_count > 0:
            await bot.send_message(message.chat.id, f"*{new_count} files from the Media Group added*. Total: {len(upload['messages'])}.", parse_mode=types.ParseMode.MARKDOWN)
        return

    # Check if this is a single message
    if message.content_type in SUPPORTED_FILE_TYPES or message.text:
        # Apply exclude_text logic for single text messages
        if message.content_type == types.ContentTypes.TEXT and upload["exclude_text"]:
            await bot.send_message(message.chat.id, "❌ Text messages are excluded in this session. Send media or use `/d`.", parse_mode=types.ParseMode.MARKDOWN)
            return
            
        # Add the message object to the list
        upload["messages"].append(message)
        
        # Send confirmation (avoiding spamming confirmation for every message)
        if len(upload["messages"]) % 5 == 1: # Confirm every 5 messages or the first one
             await bot.send_message(message.chat.id, f"*{message.content_type.capitalize()} added*. Total files collected: {len(upload['messages'])}.", parse_mode=types.ParseMode.MARKDOWN)
             
        return
    
    # If it falls through, it's a truly unsupported message during an active session
    unsupported_type = message.content_type.upper()
    await bot.send_message(message.chat.id, 
                           f"❌ **Unsupported Content** (`{unsupported_type}`). Please only send files or type `/d`.",
                           parse_mode=types.ParseMode.MARKDOWN
                          )


# --- GENERAL HANDLERS (Unchanged functionality) ---

@dp.message_handler(commands=['start', 'help'])
async def cmd_user_start_help(message: types.Message):
    """Handles /start (including deep links) and /help commands for all users."""
    command = message.get_command()
    payload = message.get_args()
    key = 'help' if command == '/help' else 'start'
    chat_id = message.chat.id 

    if command == '/start' and payload:
        await handle_deep_link(message, payload)
        return

    try:
        content = await db_manager.get_message_content(key)
        text, image_id = content['text'], content['image_id']
    except Exception:
        text = "⚠️ **System Alert:** The database is currently unavailable."
        image_id = None

    keyboard = types.InlineKeyboardMarkup()
    button_key = 'Help 📚' if key == 'start' else 'Start 👋'
    callback_cmd = 'help' if key == 'start' else 'start'
    keyboard.add(types.InlineKeyboardButton(button_key, callback_data=f"cmd:{callback_cmd}"))

    try:
        if image_id and text:
            await bot.send_photo(chat_id=chat_id, photo=image_id, caption=text, reply_markup=keyboard, parse_mode=types.ParseMode.HTML)
        else:
            await bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard, parse_mode=types.ParseMode.HTML)
    except Exception as e:
        logger.error(f"Failed to send /{key} message to {message.from_user.id}: {e}")
        try:
            await bot.send_message(chat_id, f"Failed to send the full message. Text:\n{text}", parse_mode=types.ParseMode.NONE)
        except Exception:
            pass


async def handle_deep_link(message: types.Message, session_id: str):
    """Retrieves and sends files associated with a deep link session ID."""
    user_id = message.from_user.id
    
    try:
        session = await db_manager.get_upload_session(session_id)
    except Exception as e:
        logger.error(f"DB read error during deep link access for {session_id}: {e}", exc_info=True)
        await bot.send_message(user_id, "❌ Error: The database is currently unreachable. Cannot verify or deliver the file.")
        return

    if not session:
        await bot.send_message(user_id, "❌ Error: The file session ID is invalid or has expired.")
        return

    is_owner_access = (user_id == OWNER_ID)

    file_data = json.loads(session['file_data'])
    is_protected = session['is_protected']
    auto_delete_minutes = session['auto_delete_minutes']

    send_protected = (is_protected and not is_owner_access)

    info_message = "✅ Files retrieved successfully! The delivery system guarantees reliability."
    
    # Use Markdown V2 for clear, escapable text in protected contexts
    if send_protected:
        info_message += "\n\n⚠️ **Content is Protected**\\. Forwarding and saving are disabled\\."
    
    if auto_delete_minutes > 0 and not is_owner_access:
        delete_time = datetime.now() + timedelta(minutes=auto_delete_minutes)
        delete_time_str = delete_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        info_message += f"\n\n⏰ **Auto\-Delete Scheduled:** The files will be automatically deleted from _this chat_ at `{delete_time_str}`\\."

    await bot.send_message(user_id, info_message, parse_mode=types.ParseMode.MARKDOWN_V2)

    sent_message_ids = []

    # Send all files
    for file in file_data:
        file_id = file['file_id']
        caption = file.get('caption')
        file_type = file.get('type', 'document') 

        send_method = bot.send_document 
        if file_type == 'photo':
            send_method = bot.send_photo
        elif file_type == 'video':
            send_method = bot.send_video
        elif file_type == 'audio':
            send_method = bot.send_audio
        elif file_type == 'voice': 
            send_method = bot.send_voice
        elif file_type == 'video_note':
            send_method = bot.send_video_note
        elif file_type == 'text': # Handle text messages directly
            send_method = bot.send_message
            
        send_kwargs = {
            'chat_id': user_id,
            'disable_notification': True,
            'protect_content': send_protected 
        }
        
        if file_type == 'photo':
            send_kwargs['photo'] = file_id
            if caption: send_kwargs['caption'] = caption 
        elif file_type == 'video':
            send_kwargs['video'] = file_id
            if caption: send_kwargs['caption'] = caption 
        elif file_type == 'document':
            send_kwargs['document'] = file_id
            if caption: send_kwargs['caption'] = caption 
        elif file_type == 'audio':
            send_kwargs['audio'] = file_id
            if caption: send_kwargs['caption'] = caption 
        elif file_type == 'voice':
            send_kwargs['voice'] = file_id
        elif file_type == 'video_note':
            send_kwargs['video_note'] = file_id
        elif file_type == 'text':
            send_kwargs['text'] = file_id # For text, file_id holds the actual message text
        else:
            # Skip unsupported types that were vaulted (e.g. sticker, animation, location, etc.)
            continue 

        try:
            sent_msg = await send_method(**send_kwargs)
            sent_message_ids.append(sent_msg.message_id)
            await asyncio.sleep(0.5) 
        except Exception as e:
            logger.error(f"Failed to send file of type {file_type} for session {session_id}: {e}")

    # Schedule deletion only if required and not for the owner
    if auto_delete_minutes > 0 and not is_owner_access and sent_message_ids:
        delay = auto_delete_minutes * 60
        asyncio.create_task(schedule_deletion(user_id, sent_message_ids, delay))


async def schedule_deletion(chat_id: int, message_ids: list, delay_seconds: int):
    """Schedules the deletion of messages in a user's chat."""
    logger.info(f"Scheduling deletion for {len(message_ids)} messages in chat {chat_id} in {delay_seconds} seconds.")
    try:
        await asyncio.sleep(delay_seconds)
        
        for msg_id in message_ids:
            try:
                await bot.delete_message(chat_id, msg_id)
                await asyncio.sleep(0.1) 
            except Exception:
                logger.debug(f"Failed to delete message {msg_id} in chat {chat_id}.")

        logger.info(f"Successfully cleaned up messages in chat {chat_id}.")
        try:
            await bot.send_message(chat_id, "✨ The uploaded files have been automatically cleaned from this chat.", disable_notification=True)
        except:
             pass 

    except asyncio.CancelledError:
        logger.info(f"Deletion task for chat {chat_id} was cancelled.")
    except Exception as e:
        logger.error(f"CRITICAL: Error during scheduled deletion for chat {chat_id}: {e}")

# --- OTHER HANDLERS (Remaining unchanged) ---
@dp.message_handler(commands=['cancel'], state='*')
async def cmd_cancel(message: types.Message, state: FSMContext):
    """Allows users to cancel any ongoing FSM state."""
    if is_owner(message.from_user.id):
        # Also check for active non-FSM upload session
        if OWNER_ID in active_uploads:
            cancel_upload_session(OWNER_ID)
            await bot.send_message(message.chat.id, "✅ Upload session also cancelled.")

    current_state = await state.get_state()
    if current_state is None:
        if OWNER_ID not in active_uploads:
            await bot.send_message(message.chat.id, "Nothing to cancel.")
        return

    await state.finish()
    await bot.send_message(message.chat.id, "✅ Operation cancelled.")


# ... (AdminFSM handlers like /setmessage, /setimage, /broadcast remain here) ...
# Omitted for brevity, but they should be included in the final file.

@dp.message_handler(is_owner_filter, commands=['stats'])
async def cmd_stats(message: types.Message):
    """Displays bot statistics to the owner."""
    try:
        stats = await db_manager.get_all_stats()
        
        report = (
            "📊 **Bot Usage Statistics**\n\n"
            f"👤 **Total Users:** `{stats.get('total_users', 0)}`\n"
            f"🟢 **Active Users (48h):** `{stats.get('active_users', 0)}`\n"
            f"🔗 **Total Sessions Created:** `{stats.get('total_sessions', 0)}`\n"
            f"📄 **Total Files Uploaded:** `{stats.get('files_uploaded', 0)}`\n"
        )
        
        await bot.send_message(message.chat.id, report, parse_mode=types.ParseMode.MARKDOWN)

    except Exception as e:
        logger.error(f"Failed to retrieve or send stats: {e}")
        await bot.send_message(message.chat.id, "❌ Error retrieving statistics from the database.")


@dp.message_handler(state=AdminFSM.waiting_for_broadcast_text, content_types=types.ContentTypes.ANY)
async def handle_broadcast_text(message: types.Message, state: FSMContext):
    """Performs the broadcast operation."""
    await state.finish() 
    
    try:
        all_users = await db_manager.get_all_user_ids(exclude_id=message.from_user.id)
        user_ids = [row['id'] for row in all_users]
        if not user_ids:
             await bot.send_message(message.chat.id, "❌ **Broadcast Error:** No other users found in the database to broadcast to.")
             return
    except Exception as e:
        logger.error(f"CRITICAL: Failed to fetch user list for broadcast: {e}", exc_info=True)
        await bot.send_message(message.chat.id, "❌ **CRITICAL BROADCAST ERROR:** Failed to retrieve the user list from the database.")
        return

    success_count = 0
    fail_count = 0
    
    status_msg = await bot.send_message(message.chat.id, f"🚀 Starting broadcast to **{len(user_ids)}** users...", parse_mode=types.ParseMode.MARKDOWN)

    for user_id in user_ids:
        try:
            await bot.copy_message( 
                chat_id=user_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
                disable_notification=True
            )
            success_count += 1
        except Exception as e:
            logger.warning(f"Failed to send broadcast to user {user_id}: {e}")
            fail_count += 1
        await asyncio.sleep(0.05) 

    report = (
        f"📣 **Broadcast Complete**\n"
        f"✅ Sent successfully to `{success_count}` users.\n"
        f"❌ Failed to send to `{fail_count}` users."
    )
    await bot.edit_message_text(report, status_msg.chat.id, status_msg.message_id, parse_mode=types.ParseMode.MARKDOWN)


@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_all_text_messages(message: types.Message):
    """Responds to any non-command text message."""
    if message.text.startswith('/'):
        return 
    
    await bot.send_message(message.chat.id, "I received your message, but I only understand commands like /start or /help.")


# --- WEBHOOK SETUP AND STARTUP/SHUTDOWN HOOKS (Unchanged) ---

async def health_handler(request):
    """Returns 'ok' for UptimeRobot/Render health checks."""
    return web.Response(text="ok")

async def telegram_webhook(request):
    """
    Handles the incoming Telegram update POST request and passes it to the
    Aiogram dispatcher using the safer request.json() method.
    """
    
    try:
        update_data = await request.json()
    except Exception as e:
        logger.error(f"LOW-LEVEL PARSING FAILURE: Could not parse request body as JSON. {e}", exc_info=True)
        return web.Response(text="Parsing Failed", status=200)

    try:
        update = types.Update.to_object(update_data)
        await dp.process_update(update)
    except Exception as e:
        logger.error(f"CRITICAL LOW-LEVEL DISPATCHER ERROR: Failed to process update.", exc_info=True)
        return web.Response(text="Internal Error Handled", status=200)

    return web.Response(status=200)


async def init_app():
    """Initializes the entire application: DB, Webhook, and Aiohttp App."""
    logger.info(f"Bot starting up in WEBHOOK mode.")
    
    # 1. Database Connection
    try:
        await db_manager.connect()
    except Exception as e:
        logger.critical(f"Database connection failed during startup: {e}. ALL DB-dependent features will be impacted.", exc_info=True)
        
    # 2. Aiohttp Application Setup
    app = web.Application()
    
    app.router.add_post(WEBHOOK_PATH, telegram_webhook)
    app.router.add_get('/health', health_handler) # <-- Health check endpoint
    
    # 3. Webhook Setting
    if WEBHOOK_URL:
        try:
            webhook_info = await bot.get_webhook_info()
            if webhook_info.url != WEBHOOK_URL:
                logger.info(f"Setting webhook to: {WEBHOOK_URL}")
                await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True) 
            else:
                logger.info("Webhook already set correctly.")
        except Exception as e:
            logger.critical(f"Failed to set webhook: {e}")
            
    else:
        logger.critical("WEBHOOK_HOST (RENDER_EXTERNAL_URL) not set. Webhook mode will fail.")

    me = await bot.get_me()
    logger.info(f"Bot '{me.username}' is ready. Owner ID: {OWNER_ID}. Channel ID: {UPLOAD_CHANNEL_ID}.")
    logger.info("Bot startup completed successfully.")
    
    return app


def main():
    """Runs the main application using aiohttp's runner."""
    logger.info("Initializing bot system...")
    
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init_app())
    
    runner = AppRunner(app)
    loop.run_until_complete(runner.setup())
    site = TCPSite(runner, '0.0.0.0', int(os.getenv("PORT", 8080)))
    
    logger.info(f"Starting web server on port {os.getenv('PORT', 8080)}")
    try:
        loop.run_until_complete(site.start())
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
    finally:
        loop.run_until_complete(site.stop())
        loop.run_until_complete(runner.cleanup())
        loop.run_until_complete(on_shutdown(dp))


async def on_shutdown(dp):
    """Executed on bot shutdown. Closes DB connection and clears webhook."""
    logger.info("Bot shutting down...")
    
    await db_manager.close()
    
    if WEBHOOK_URL:
        logger.info("Clearing webhook...")
        await dp.bot.delete_webhook()
        
    await dp.storage.close()
    await dp.storage.wait_closed()
    logger.info("Bot shutdown completed.")


if __name__ == '__main__':
    if 'PORT' not in os.environ:
        os.environ['PORT'] = '8080' 
        
    try:
        main()
    except Exception as e:
        logger.critical(f"An unhandled error occurred in main execution: {e}")
        time.sleep(1)
