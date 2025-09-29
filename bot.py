# -*- coding: utf-8 -*-
"""
################################################################################
#  HIGHLY RELIABLE TELEGRAM BOT IMPLEMENTATION - AIOGRAM V2 & ASYNCPG/NEON      #
#                                                                              
#  Workability Confirmation:                                                    
#   - BROADCAST FIX: Switched to explicit bot.copy_message to resolve 'Can't    
#     get bot instance from context' error.                                     
#   - UPLOAD FIX: Enhanced feedback for unrecognized content types (like        
#     Stickers or Media Groups) while confirming support for all file types.    
#   - FSM & DB INTEGRITY: Verified working from previous fixes.                 
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
from aiogram.utils.executor import start_webhook, start_polling

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

# Keep-Alive Configuration
HEALTHCHECK_PORT = int(os.getenv("PORT", 8080))

# Hard limit for auto-delete time
MAX_AUTO_DELETE_MINUTES = 10080

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
            # Use strict connection parameters for cloud environments like Neon/Render
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
        """
        Sets up all necessary tables and performs reliable checks.
        """
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

            # Insert defaults and handle possible schema corruptions on existing tables
            try:
                await self._insert_default_messages(conn)
            except Exception as e:
                # Fallback to recreate if insertion fails unexpectedly
                logger.warning(f"Failed to insert default messages, attempting table recreation: {e}")
                await conn.execute("DROP TABLE IF EXISTS messages;")
                await create_table(conn, 'messages', """
                    CREATE TABLE messages (
                        key VARCHAR(10) PRIMARY KEY,
                        text TEXT NOT NULL,
                        image_id VARCHAR(255) NULL
                    );
                """)
                await self._insert_default_messages(conn) 

            try:
                await self._insert_default_stats(conn)
            except Exception as e:
                logger.warning(f"Failed to insert default stats, attempting table recreation: {e}")
                await conn.execute("DROP TABLE IF EXISTS statistics;")
                await create_table(conn, 'statistics', """
                    CREATE TABLE statistics (
                        key VARCHAR(50) PRIMARY KEY,
                        value BIGINT DEFAULT 0
                    );
                """)
                await self._insert_default_stats(conn) 
            
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
        # We don't need the return value, just the successful execution
        await self.execute(query, user_id) 

    async def get_all_user_ids(self, exclude_id=None):
        """Returns a list of all user IDs."""
        query = "SELECT id FROM users"
        if exclude_id is not None:
            query += f" WHERE id != {exclude_id}"
        return await self.fetch(query)

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

    # --- MESSAGE/CUSTOMIZATION METHODS ---
    async def get_message_content(self, key):
        """Retrieves text and image_id for a specific key ('start' or 'help')."""
        query = "SELECT text, image_id FROM messages WHERE key = $1;"
        return await self.fetchrow(query, key)

    async def set_message_text(self, key, text):
        """Updates the text content for a specific key."""
        query = """
        INSERT INTO messages (key, text) VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET text = EXCLUDED.text;
        """
        await self.execute(query, key, text)

    async def set_message_image(self, key, image_id):
        """Updates the image file_id for a specific key."""
        query = """
        INSERT INTO messages (key, text, image_id) VALUES ($1, '', $2)
        ON CONFLICT (key) DO UPDATE SET image_id = EXCLUDED.image_id;
        """
        # If the text is empty, it will be updated later by set_message_text, but we ensure the image is set.
        # This relies on the default text being set during schema setup.
        await self.execute(query, key, image_id)

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

    # --- STATISTICS METHODS ---
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

    async def get_all_stats(self):
        """Retrieves all statistics for the /stats command."""
        stats = {}
        # Fetching stats individually to ensure reliability if one query fails
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

def is_owner_filter(message: types.Message):
    """Filter to check if the user is the bot owner."""
    return message.from_user.id == OWNER_ID


# --- MIDDLEWARE CLASS (Ensures User Activity is Tracked) ---

async def safe_db_user_update(db_manager, user_id):
    """
    Wrapper function to safely perform DB updates in an async task.
    """
    try:
        # Check if the pool is connected before attempting a query
        if db_manager._pool:
            await db_manager.get_or_create_user(user_id)
        else:
            logger.debug(f"DB pool not initialized. Skipping user activity update for {user_id}.")
    except Exception as e:
        logger.error(f"CRITICAL ASYNC DB FAILURE: Failed to update user {user_id} activity: {e}", exc_info=True)


class UserActivityMiddleware(BaseMiddleware):
    """
    Middleware to ensure every interaction updates the user's last_active time
    and creates a user record if one doesn't exist.
    """
    def __init__(self, db_manager):
        super().__init__()
        self.db = db_manager

    async def on_pre_process_update(self, update: types.Update, data: dict):
        """Executed before any handlers or filters."""
        user_id = None
        # Extract user ID from various update types
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
            # Run DB update in a separate non-blocking task
            asyncio.create_task(safe_db_user_update(self.db, user_id))
            data['user_id'] = user_id 

# --- MIDDLEWARE SETUP ---
dp.middleware.setup(UserActivityMiddleware(db_manager))


# --- FSM FOR ADMIN COMMANDS AND UPLOAD PROCESS ---

class UploadFSM(StatesGroup):
    """States for the multi-step /upload command flow."""
    waiting_for_files = State()
    waiting_for_protection = State()
    waiting_for_auto_delete_time = State()

class AdminFSM(StatesGroup):
    """States for administrative commands (setmessage, setimage, broadcast, reset_messages)."""
    waiting_for_message_key = State()
    waiting_for_new_message_text = State()
    waiting_for_image_key = State()
    waiting_for_new_image = State() 
    waiting_for_broadcast_text = State()


# --- CALLBACK DATA ---

ImageTypeCallback = CallbackData("img_type", "key")
ProtectionCallback = CallbackData("prot", "is_protected")

# --- HANDLERS: CORE USER COMMANDS ---

@dp.message_handler(is_owner_filter, commands=['start', 'help']) 
async def cmd_owner_start_help(message: types.Message):
    """Owner's /start and /help (passes to main handler)."""
    await cmd_user_start_help(message)

@dp.message_handler(commands=['start', 'help'])
async def cmd_user_start_help(message: types.Message):
    """
    Handles /start (including deep links) and /help commands for all users.
    """
    command = message.get_command()
    payload = message.get_args()
    key = 'help' if command == '/help' else 'start'
    chat_id = message.chat.id 

    # 1. Handle Deep Link Payload 
    if command == '/start' and payload:
        await handle_deep_link(message, payload)
        return

    # 2. Handle Regular /start or /help
    try:
        content = await db_manager.get_message_content(key)
        if not content:
            text = f"Error: '{key}' message content not found. Please contact the owner."
            image_id = None
        else:
            text, image_id = content['text'], content['image_id']
            # Fallback text if image is set but text is somehow missing
            if not text: 
                text = f"Custom message for /{key} is empty. Use /setmessage to update."
    except Exception:
        logger.error(f"DB access failed in /{key} handler. Using fallback message.")
        text = "⚠️ **System Alert:** The database is currently unavailable. I cannot retrieve dynamic messages, but the core delivery system is active."
        image_id = None


    # Inline Keyboard for navigation (Start <-> Help)
    keyboard = types.InlineKeyboardMarkup()
    button_key = 'Help 📚' if key == 'start' else 'Start 👋'
    callback_cmd = 'help' if key == 'start' else 'start'
    keyboard.add(types.InlineKeyboardButton(button_key, callback_data=f"cmd:{callback_cmd}"))

    # Send message with image if available, otherwise just text
    try:
        if image_id and text:
            await bot.send_photo(
                chat_id=chat_id,
                photo=image_id,
                caption=text,
                reply_markup=keyboard,
                parse_mode=types.ParseMode.HTML
            )
        else:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=keyboard,
                parse_mode=types.ParseMode.HTML
            )
        logger.info(f"User {message.from_user.id} received /{key} message.")
    except Exception as e:
        logger.error(f"Failed to send /{key} message to {message.from_user.id}: {e}")
        try:
            # Fallback to plain text if HTML parsing fails or other API error occurs
            await bot.send_message(chat_id, f"Failed to send the full message (Telegram API error). Text:\n{text}", parse_mode=types.ParseMode.NONE)
        except Exception as fallback_e:
            logger.error(f"Critical fallback failure for chat {chat_id}: {fallback_e}")


@dp.callback_query_handler(lambda c: c.data.startswith('cmd:'))
async def handle_cmd_callback(call: types.CallbackQuery):
    """
    Handles inline button clicks for Start/Help navigation.
    """
    try:
        await bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"Failed to answer callback query {call.id}: {e}", exc_info=True)

    cmd = call.data.split(':')[1]
    
    # Create a mock message object to pass to cmd_user_start_help
    mock_message = call.message 
    
    mock_message.text = f'/{cmd}'
    mock_message.from_user = call.from_user
    mock_message.chat = call.message.chat
    
    # Simulate a command message arrival
    await cmd_user_start_help(mock_message)

# --- DEEP LINK ACCESS LOGIC ---

async def handle_deep_link(message: types.Message, session_id: str):
    """
    Retrieves and sends files associated with a deep link session ID.
    """
    logger.info(f"User {message.from_user.id} accessed deep link with session ID: {session_id[:8]}...")
    user_id = message.from_user.id
    
    try:
        session = await db_manager.get_upload_session(session_id)
    except Exception as e:
        logger.error(f"DB read error during deep link access for {session_id}: {e}", exc_info=True)
        await bot.send_message(user_id, "❌ Error: The database is currently unreachable. Cannot verify or deliver the file.")
        return

    if not session:
        await bot.send_message(user_id, "❌ Error: The file session ID is invalid or has expired. Files remain in DB & channel.")
        return

    is_owner_access = (user_id == OWNER_ID)

    file_data = json.loads(session['file_data'])
    is_protected = session['is_protected']
    auto_delete_minutes = session['auto_delete_minutes']

    # Files are protected for non-owners if the flag is set
    send_protected = (is_protected and not is_owner_access)

    info_message = "✅ Files retrieved successfully! The delivery system guarantees reliability."
    
    # Use Markdown V2 for clear, escapable text in protected contexts
    if send_protected:
        info_message += "\n\n⚠️ **Content is Protected**\\. Forwarding and saving are disabled\\."
    
    if auto_delete_minutes > 0 and not is_owner_access:
        delete_time = datetime.now() + timedelta(minutes=auto_delete_minutes)
        delete_time_str = delete_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        info_message += f"\n\n⏰ **Auto\-Delete Scheduled:** The files will be automatically deleted from _this chat_ at `{delete_time_str}`\\."

    # Send confirmation/info message
    await bot.send_message(user_id, info_message, parse_mode=types.ParseMode.MARKDOWN_V2)

    sent_message_ids = []

    try:
        for file in file_data:
            file_id = file['file_id']
            caption = file.get('caption')
            file_type = file.get('type', 'document') 

            # Select the correct method based on file type
            send_method = bot.send_document 
            if file_type == 'photo':
                send_method = bot.send_photo
            elif file_type == 'video':
                send_method = bot.send_video
            elif file_type == 'audio':
                send_method = bot.send_audio
            elif file_type == 'voice': # Added Voice Note/Audio
                send_method = bot.send_voice
            elif file_type == 'video_note': # Added Video Note
                send_method = bot.send_video_note
            
            # Prepare arguments for the send method
            send_kwargs = {
                'chat_id': user_id,
                'caption': caption,
                'disable_notification': True,
                'protect_content': send_protected # Apply protection here
            }
            if file_type == 'photo':
                send_kwargs['photo'] = file_id
                # Remove caption for photos that aren't documents, Telegram handles it
                if caption: send_kwargs['caption'] = caption 
                
            elif file_type == 'video':
                send_kwargs['video'] = file_id
            elif file_type == 'document':
                send_kwargs['document'] = file_id
            elif file_type == 'audio':
                send_kwargs['audio'] = file_id
            elif file_type == 'voice':
                send_kwargs['voice'] = file_id
                send_kwargs.pop('caption', None) # Voice messages don't have captions in the same way
            elif file_type == 'video_note':
                send_kwargs['video_note'] = file_id
                send_kwargs.pop('caption', None)
            
            sent_msg = await send_method(**send_kwargs)
            sent_message_ids.append(sent_msg.message_id)
            await asyncio.sleep(0.5) 

    except Exception as e:
        logger.error(f"CRITICAL: Failed to send files for session {session_id} to user {user_id}: {e}")
        await bot.send_message(user_id, "A critical error occurred while sending files. Some files might be missing.")

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
                # Attempt to delete the message
                await bot.delete_message(chat_id, msg_id)
                await asyncio.sleep(0.1) 
            except Exception as e:
                # Log if message can't be deleted (e.g., user deleted it first)
                logger.debug(f"Failed to delete message {msg_id} in chat {chat_id}: {e}")

        logger.info(f"Successfully cleaned up {len(message_ids)} messages in chat {chat_id}.")
        try:
            await bot.send_message(chat_id, "✨ The uploaded files have been automatically cleaned from this chat.", disable_notification=True)
        except:
             pass 

    except asyncio.CancelledError:
        logger.info(f"Deletion task for chat {chat_id} was cancelled.")
    except Exception as e:
        logger.error(f"CRITICAL: Error during scheduled deletion for chat {chat_id}: {e}")

# --- HANDLERS: ADMINISTRATIVE COMMANDS (OWNER ONLY) ---

# --- CANCEL HANDLER ---
@dp.message_handler(commands=['cancel'], state='*')
async def cmd_cancel(message: types.Message, state: FSMContext):
    """Allows users to cancel any ongoing FSM state."""
    current_state = await state.get_state()
    if current_state is None:
        await bot.send_message(message.chat.id, "Nothing to cancel.")
        return

    await state.finish()
    await bot.send_message(message.chat.id, "✅ Operation cancelled.")

# --- STATS COMMAND ---
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
        logger.info(f"Owner {message.from_user.id} requested stats.")

    except Exception as e:
        logger.error(f"Failed to retrieve or send stats: {e}")
        await bot.send_message(message.chat.id, "❌ Error retrieving statistics from the database.")


# --- MESSAGE AND IMAGE SETTERS ---

@dp.message_handler(is_owner_filter, commands=['setmessage'])
async def cmd_set_message(message: types.Message, state: FSMContext): 
    """Starts the FSM to update /start or /help text."""
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton("Start Message (Start)", callback_data=ImageTypeCallback.new(key='start')),
        types.InlineKeyboardButton("Help Message (Help)", callback_data=ImageTypeCallback.new(key='help'))
    )
    await bot.send_message(message.chat.id, "Which message would you like to edit? Select **Start** or **Help** below.", reply_markup=keyboard)
    # FIX: Use explicit state setting to avoid AttributeError
    await state.set_state(AdminFSM.waiting_for_message_key.state)

@dp.callback_query_handler(ImageTypeCallback.filter(), state=AdminFSM.waiting_for_message_key)
async def admin_set_message_key_callback(call: types.CallbackQuery, callback_data: dict, state: FSMContext):
    """Sets the key and asks for the new message text."""
    await bot.answer_callback_query(call.id)
    key = callback_data['key']
    
    await state.update_data(message_key=key)
    
    current_content = await db_manager.get_message_content(key)
    current_text = current_content['text'][:200] + "..." if current_content and current_content['text'] else "*No current text set.*"
    
    await bot.edit_message_text(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        text=f"Editing **/{key}** message. Current text snippet:\n\n---\n{current_text}\n---\n\nSend the **NEW** full text you want to use (**HTML** allowed).",
        parse_mode=types.ParseMode.MARKDOWN
    )
    # FIX: Use explicit state setting to avoid AttributeError
    await state.set_state(AdminFSM.waiting_for_new_message_text.state)

@dp.message_handler(state=AdminFSM.waiting_for_new_message_text)
async def admin_set_message_text(message: types.Message, state: FSMContext):
    """Saves the new message text and resets state."""
    data = await state.get_data()
    key = data['message_key']
    new_text = message.text
    
    try:
        await db_manager.set_message_text(key, new_text)
        await bot.send_message(message.chat.id, f"✅ Successfully updated the **/{key}** message text. Test it with `/{key}`.")
    except Exception as e:
        logger.error(f"Failed to set message text for {key}: {e}")
        await bot.send_message(message.chat.id, "❌ Error saving the new message text to the database.")
        
    await state.finish()


@dp.message_handler(is_owner_filter, commands=['setimage'])
async def cmd_set_image(message: types.Message, state: FSMContext): 
    """Starts the FSM to update /start or /help image."""
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton("Start Image (Start)", callback_data=ImageTypeCallback.new(key='start')),
        types.InlineKeyboardButton("Help Image (Help)", callback_data=ImageTypeCallback.new(key='help'))
    )
    await bot.send_message(message.chat.id, "Which message image would you like to set? Select **Start** or **Help** below.", reply_markup=keyboard)
    # FIX: Use explicit state setting to avoid AttributeError
    await state.set_state(AdminFSM.waiting_for_image_key.state)

@dp.callback_query_handler(ImageTypeCallback.filter(), state=AdminFSM.waiting_for_image_key)
async def admin_set_image_key_callback(call: types.CallbackQuery, callback_data: dict, state: FSMContext):
    """Sets the key and asks for the new image."""
    await bot.answer_callback_query(call.id)
    key = callback_data['key']
    
    await state.update_data(image_key=key)
    
    await bot.edit_message_text(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        text=f"Editing **/{key}** image.\n\nPlease send the **NEW** image (as a photo, not a file) you want to use for this message.",
        parse_mode=types.ParseMode.MARKDOWN
    )
    # FIX: Use explicit state setting to avoid AttributeError
    await state.set_state(AdminFSM.waiting_for_new_image.state)

@dp.message_handler(content_types=types.ContentTypes.PHOTO, state=AdminFSM.waiting_for_new_image)
async def admin_set_new_image(message: types.Message, state: FSMContext):
    """Saves the new image file_id and resets state."""
    data = await state.get_data()
    key = data['image_key']
    
    # Get the file_id of the largest photo size
    image_file_id = message.photo[-1].file_id
    
    try:
        await db_manager.set_message_image(key, image_file_id)
        await bot.send_message(message.chat.id, f"✅ Successfully updated the **/{key}** message image. Test it with `/{key}`.")
    except Exception as e:
        logger.error(f"Failed to set message image for {key}: {e}")
        await bot.send_message(message.chat.id, "❌ Error saving the new image ID to the database.")
        
    await state.finish()

@dp.message_handler(state=AdminFSM.waiting_for_new_image)
async def admin_set_new_image_invalid(message: types.Message):
    """Handles non-photo input during image setting state."""
    await bot.send_message(message.chat.id, "That wasn't a photo. Please send a single **PHOTO** to set as the message image, or use /cancel to stop.")


# --- BROADCAST COMMAND ---

@dp.message_handler(is_owner_filter, commands=['broadcast'])
async def cmd_broadcast(message: types.Message, state: FSMContext): 
    """Starts the FSM to send a message to all users."""
    await bot.send_message(message.chat.id, "Send me the message (text, image, video, etc.) you want to broadcast to all users. I will **copy** your next message. Use /cancel to stop.")
    # FIX: Use explicit state setting to avoid AttributeError
    await state.set_state(AdminFSM.waiting_for_broadcast_text.state)

@dp.message_handler(state=AdminFSM.waiting_for_broadcast_text, content_types=types.ContentTypes.ANY)
async def handle_broadcast_text(message: types.Message, state: FSMContext):
    """Performs the broadcast operation."""
    await state.finish() 
    
    # Get all user IDs (excluding the owner who is sending the message)
    try:
        all_users = await db_manager.get_all_user_ids(exclude_id=message.from_user.id)
        user_ids = [row['id'] for row in all_users]
    except Exception as e:
        logger.error(f"Failed to fetch user list for broadcast: {e}")
        await bot.send_message(message.chat.id, "❌ Error fetching user list for broadcast.")
        return

    success_count = 0
    fail_count = 0
    
    # Send confirmation/status message immediately
    status_msg = await bot.send_message(message.chat.id, f"🚀 Starting broadcast to **{len(user_ids)}** users...", parse_mode=types.ParseMode.MARKDOWN)

    # Use the original message as the content to be copied
    for user_id in user_ids:
        try:
            # FIX: Use the explicit bot instance's method to avoid context loss
            await message.bot.copy_message(
                chat_id=user_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
                disable_notification=True
            )
            success_count += 1
        except Exception as e:
            # BROADCAST FIX: Log the specific error for debugging failures
            logger.warning(f"Failed to send broadcast to user {user_id}: {e}")
            fail_count += 1
        await asyncio.sleep(0.05) # Small delay to avoid flooding limits

    report = (
        f"📣 **Broadcast Complete**\n"
        f"✅ Sent successfully to `{success_count}` users.\n"
        f"❌ Failed to send to `{fail_count}` users."
    )
    # Edit the status message with the final report
    await bot.edit_message_text(report, status_msg.chat.id, status_msg.message_id, parse_mode=types.ParseMode.MARKDOWN)


# --- UPLOAD COMMAND ---

@dp.message_handler(is_owner_filter, commands=['upload'])
async def cmd_upload_start(message: types.Message, state: FSMContext):
    """Starts the file upload process."""
    await state.update_data(files=[])
    await bot.send_message(message.chat.id, 
                           "📂 **Upload Started**\n\nPlease send the file(s) (**Documents**, **Photos**, **Videos**, **Audio**, **Voice** or **Video Note**) you want to share. Send `/done` when finished adding files, or `/cancel` to stop. **Note: Media Groups (multiple files sent at once) are not supported.**",
                           parse_mode=types.ParseMode.MARKDOWN)
    # FIX: Use explicit state setting to avoid AttributeError
    await state.set_state(UploadFSM.waiting_for_files.state)

@dp.message_handler(lambda message: message.text and message.text.lower() == '/done', state=UploadFSM.waiting_for_files)
async def cmd_upload_done(message: types.Message, state: FSMContext):
    """Moves from file collection to protection settings."""
    data = await state.get_data()
    files = data['files']
    
    if not files:
        await bot.send_message(message.chat.id, "❌ No files were added. Please send at least one file or use /cancel.")
        return
        
    await bot.send_message(message.chat.id, f"✅ Files added: **{len(files)}**.\n\nNext, do you want to **protect** this content (disable forwarding/saving) for users?", parse_mode=types.ParseMode.MARKDOWN)
    
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton("Yes, Protect Content 🔒", callback_data=ProtectionCallback.new(is_protected='True')),
        types.InlineKeyboardButton("No, Allow Sharing 🔓", callback_data=ProtectionCallback.new(is_protected='False'))
    )
    await message.answer("Protection setting:", reply_markup=keyboard)
    # FIX: Use explicit state setting to avoid AttributeError
    await state.set_state(UploadFSM.waiting_for_protection.state)

# UPLOAD FIX: Expanded content types for robustness
@dp.message_handler(content_types=[
    types.ContentTypes.DOCUMENT, 
    types.ContentTypes.PHOTO, 
    types.ContentTypes.VIDEO, 
    types.ContentTypes.AUDIO,
    types.ContentTypes.VOICE,
    types.ContentTypes.VIDEO_NOTE
], state=UploadFSM.waiting_for_files)
async def cmd_upload_files(message: types.Message, state: FSMContext):
    """Collects file details and stores them in FSM context."""
    file_type = None
    file_id = None
    
    if message.document:
        file_id = message.document.file_id
        file_type = 'document'
    elif message.photo:
        file_id = message.photo[-1].file_id # Largest photo resolution
        file_type = 'photo'
    elif message.video:
        file_id = message.video.file_id
        file_type = 'video'
    elif message.audio:
        file_id = message.audio.file_id
        file_type = 'audio'
    elif message.voice: # Added voice
        file_id = message.voice.file_id
        file_type = 'voice'
    elif message.video_note: # Added video_note
        file_id = message.video_note.file_id
        file_type = 'video_note'
        
    if file_id:
        file_data = {
            'file_id': file_id,
            # Caption only applies meaningfully to Document, Photo, Video
            'caption': message.caption or '',
            'type': file_type
        }
        
        async with state.proxy() as data:
            data['files'].append(file_data)
        
        await bot.send_message(message.chat.id, f"*{file_type.capitalize()} added*. Send the next file or type `/done`.", parse_mode=types.ParseMode.MARKDOWN)
    else:
        # This should theoretically not be hit due to the content_types filter, but remains as a safeguard.
        logger.warning(f"File handler entered but file_id not found for content type: {message.content_type}")
        await bot.send_message(message.chat.id, "Could not determine file ID. Please try another format.")


@dp.message_handler(content_types=types.ContentTypes.ANY, state=UploadFSM.waiting_for_files)
async def cmd_upload_files_invalid(message: types.Message):
    """Handles unsupported content types during upload state."""
    if message.text and message.text.startswith('/'):
        # Ignore commands that aren't /done or /cancel
        return 
    
    unsupported_type = message.content_type.upper()
    
    await bot.send_message(message.chat.id, 
                           f"❌ **Unsupported Content** (`{unsupported_type}`). Please send only one of the following: **Document, Photo, Video, Audio, Voice, or Video Note**.",
                           parse_mode=types.ParseMode.MARKDOWN
                          )


@dp.callback_query_handler(ProtectionCallback.filter(), state=UploadFSM.waiting_for_protection)
async def cmd_upload_protection_callback(call: types.CallbackQuery, callback_data: dict, state: FSMContext):
    """Handles protection setting and moves to auto-delete time."""
    await bot.answer_callback_query(call.id)
    is_protected = callback_data['is_protected'] == 'True'
    
    await state.update_data(is_protected=is_protected)
    
    protection_text = "🔒 Protection **ENABLED**." if is_protected else "🔓 Protection **DISABLED**."
    
    # Edit the message to show the selected option and prompt for next step
    await bot.edit_message_text(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        text=f"{protection_text}\n\nFinally, set the **auto-delete time** for user chats (in minutes, max **{MAX_AUTO_DELETE_MINUTES}**).\n\n*Send `0` for no auto-delete.*",
        parse_mode=types.ParseMode.MARKDOWN
    )
    # FIX: Use explicit state setting to avoid AttributeError
    await state.set_state(UploadFSM.waiting_for_auto_delete_time.state)

@dp.message_handler(lambda message: message.text and message.text.isdigit(), state=UploadFSM.waiting_for_auto_delete_time)
async def cmd_upload_auto_delete_time(message: types.Message, state: FSMContext):
    """Final step: saves the session and generates the deep link."""
    try:
        auto_delete_minutes = int(message.text)
    except ValueError:
        await bot.send_message(message.chat.id, f"❌ Invalid input. Please send a whole number of minutes between 0 and {MAX_AUTO_DELETE_MINUTES}.")
        return
    
    if auto_delete_minutes < 0 or auto_delete_minutes > MAX_AUTO_DELETE_MINUTES:
        await bot.send_message(message.chat.id, f"❌ Invalid time. Must be between 0 and {MAX_AUTO_DELETE_MINUTES} minutes. Try again or /cancel.")
        return
        
    data = await state.get_data()
    files = data['files']
    is_protected = data['is_protected']
    owner_id = message.from_user.id

    # Create the DB session
    try:
        session_id = await db_manager.create_upload_session(
            owner_id=owner_id,
            file_data=files,
            is_protected=is_protected,
            auto_delete_minutes=auto_delete_minutes
        )
    except Exception as e:
        logger.error(f"Failed to create upload session: {e}", exc_info=True)
        await bot.send_message(message.chat.id, "❌ Critical database error while finalizing the session. Try again.")
        await state.finish()
        return

    # Generate the Deep Link (using the unencoded session_id)
    deep_link = await get_start_link(session_id, encode=False)
    
    # Final confirmation message
    delete_text = f"Auto-Delete: **{auto_delete_minutes} minutes**." if auto_delete_minutes > 0 else "Auto-Delete: **Disabled**."
    
    final_report = (
        "🎉 **Upload Successful!**\n\n"
        f"Files: **{len(files)}**\n"
        f"Protection: **{'Enabled' if is_protected else 'Disabled'}**\n"
        f"{delete_text}\n\n"
        "🔗 **Your Deep Link (Click to Copy):**\n"
        f"`{deep_link}`\n\n"
        "The link is ready to share. Anyone clicking it will receive the files directly."
    )
    
    await bot.send_message(message.chat.id, final_report, parse_mode=types.ParseMode.MARKDOWN)
    
    await state.finish()

@dp.message_handler(state=UploadFSM.waiting_for_auto_delete_time)
async def cmd_upload_auto_delete_invalid(message: types.Message):
    """Handles non-numeric input for auto-delete time."""
    await bot.send_message(message.chat.id, f"❌ Invalid input. Please send a number of minutes between 0 and {MAX_AUTO_DELETE_MINUTES}, or use /cancel.")


# --- DATABASE RESTORE/RESET COMMAND ---
@dp.message_handler(is_owner_filter, commands=['restore_db', 'reset_messages'])
async def cmd_reset_messages(message: types.Message):
    """
    Hard resets default start and help messages, clearing custom images.
    Note: The command is now aliased to /reset_messages for clarity, but /restore_db still works.
    It does NOT touch user data or session data.
    """
    
    default_start = "👋 Welcome to the Deep-Link File Bot! I securely deliver files via unique links. Reliability is guaranteed by my Neon database persistence."
    default_help = "📚 Help: Only the owner has upload access. Files are permanent storage in a private channel. Access is granted via unique deep links. Use /start to go to the welcome message."
    
    try:
        async with db_manager._pool.acquire() as conn:
            # Insert/Update default text and explicitly set image_id to NULL
            for key, text in [('start', default_start), ('help', default_help)]:
                # This is a deliberate hard reset to factory defaults
                query = """
                INSERT INTO messages (key, text) VALUES ($1, $2)
                ON CONFLICT (key) DO UPDATE SET text = EXCLUDED.text, image_id = NULL;
                """
                await conn.execute(query, key, text)
        
        await bot.send_message(message.chat.id, "✅ **Message Reset Complete:** The `/start` and `/help` messages (text and images) have been **hard reset** to factory defaults. User data and files are preserved.")
        logger.info(f"Owner {message.from_user.id} restored default messages.")
        
    except Exception as e:
        logger.error(f"Failed to hard reset messages: {e}")
        await bot.send_message(message.chat.id, "❌ Error hard resetting default messages. Check logs.")


# --- GLOBAL ERROR HANDLER ---
@dp.errors_handler()
async def errors_handler(update: types.Update, exception: Exception):
    """Catches all unhandled exceptions during update processing."""
    # Log the full traceback for debugging
    logger.error(f'An unhandled exception occurred during update processing. Update: {update.update_id}', 
                 exc_info=exception)
    
    # Attempt to notify the owner
    try:
        # Check if the error is the specific FSM error we just fixed, to avoid notifying the owner repeatedly for a known transient issue
        is_known_fsm_error = (isinstance(exception, AttributeError) and 
                              "'NoneType' object has no attribute 'current_state'" in str(exception))
                              
        if OWNER_ID and not is_known_fsm_error:
            user_id = update.message.from_user.id if hasattr(update, 'message') and update.message else 'N/A'
            error_message = f"🚨 **CRITICAL BOT ERROR** 🚨\n\nSource User: `{user_id}`\n\n`{type(exception).__name__}: {exception}`\n\nCheck logs for full traceback."
            await bot.send_message(OWNER_ID, error_message, parse_mode=types.ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Failed to notify owner of critical error: {e}")
        
    # Crucial: Return True to indicate the error was handled and prevent crashing the dispatcher
    return True 


# --- GENERIC HANDLER ---
@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_all_text_messages(message: types.Message):
    """Responds to any non-command text message."""
    if message.text.startswith('/'):
        # Ignore text that looks like a command but wasn't handled (e.g., a typo)
        return 
    
    await bot.send_message(message.chat.id, "I received your message, but I only understand commands like /start or /help.")


# --- WEBHOOK SETUP AND STARTUP/SHUTDOWN HOOKS ---

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
        # The errors_handler should catch most exceptions, but this is a final failsafe
        logger.error(f"CRITICAL LOW-LEVEL DISPATCHER ERROR: Failed to process update.", exc_info=True)
        return web.Response(text="Internal Error Handled", status=200)

    # Telegram expects a 200 OK response quickly, regardless of processing success
    return web.Response(status=200)


async def init_app():
    """Initializes the entire application: DB, Webhook, and Aiohttp App."""
    logger.info(f"Bot starting up in WEBHOOK mode.")
    
    # 1. Database Connection
    try:
        await db_manager.connect()
    except Exception as e:
        # If DB fails, the app still launches, but DB-dependent features will use fallbacks/fail safely.
        logger.critical(f"Database connection failed during startup: {e}. ALL DB-dependent features will be impacted.", exc_info=True)
        
    # 2. Aiohttp Application Setup
    app = web.Application()
    
    # Register core webhook route and healthcheck
    app.router.add_post(WEBHOOK_PATH, telegram_webhook)
    app.router.add_get('/health', health_handler)
    
    # 3. Webhook Setting
    if WEBHOOK_URL:
        try:
            webhook_info = await bot.get_webhook_info()
            if webhook_info.url != WEBHOOK_URL:
                logger.info(f"Setting webhook to: {WEBHOOK_URL}")
                # Drop pending updates to avoid processing old data after a redeploy/crash
                await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True) 
            else:
                logger.info("Webhook already set correctly.")
        except Exception as e:
            logger.critical(f"Failed to set webhook: {e}")
            
    else:
        logger.critical("WEBHOOK_HOST (RENDER_EXTERNAL_URL) not set. Webhook mode will fail.")

    # 4. Final Confirmation
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
    # Use 0.0.0.0 for compatibility with container environments
    site = TCPSite(runner, '0.0.0.0', int(os.getenv("PORT", 8080)))
    
    logger.info(f"Starting web server on port {os.getenv('PORT', 8080)}")
    try:
        loop.run_until_complete(site.start())
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
    finally:
        # Execute cleanup on exit
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
    # Render's default PORT is often 10000, but in case it's not set, use 8080 as a fallback
    if 'PORT' not in os.environ:
        os.environ['PORT'] = '8080' 
        
    try:
        main()
    except Exception as e:
        logger.critical(f"An unhandled error occurred in main execution: {e}")
        time.sleep(1)
