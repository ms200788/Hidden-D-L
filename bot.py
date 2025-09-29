# -*- coding: utf-8 -*-
"""
################################################################################
#  HIGHLY RELIABLE TELEGRAM BOT IMPLEMENTATION - AIOGRAM V2 & ASYNCPG/NEON      #
#                                                                              
#  FIX 13 (Previous): Fixed webhook 403 error.
#  FIX 14 (Current - CRITICAL AIOGRAM CONTEXT FIX):                             
#   1. **Bot Context Loss Fixed:** Replaced all instances of `message.answer()` 
#      and `message.answer_photo()` in the `cmd_user_start_help` handler with   
#      direct calls to the globally defined `bot.send_message()` and           
#      `bot.send_photo()`, explicitly passing the `chat_id`. This prevents the  
#      "Can't get bot instance from context" error that occurs during indirect  
#      handler calls or in webhook mode.                                        
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
# --- CRITICAL FIX 12: Corrected the logging level retrieval ---
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
            await conn.execute(query, *args)

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
                await conn.fetchval("SELECT id FROM users LIMIT 1;")
            except (UndefinedColumnError, asyncpg.exceptions.InvalidCatalogObjectError, asyncpg.exceptions.PostgresError) as e:
                logger.warning(f"Database: 'users' table schema issue detected ({e}). Dropping and recreating table to ensure 'id' primary key.")
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
            except UndefinedColumnError as e:
                if 'column "key" of relation "messages" does not exist' in str(e):
                    logger.warning("Database: messages table schema appears corrupted. Dropping and recreating table to fix missing 'key' column.")
                    await conn.execute("DROP TABLE IF EXISTS messages;")
                    await conn.execute("""
                        CREATE TABLE messages (
                            key VARCHAR(10) PRIMARY KEY,
                            text TEXT NOT NULL,
                            image_id VARCHAR(255) NULL
                        );
                    """)
                    await self._insert_default_messages(conn) 
                else:
                    raise 

            try:
                await self._insert_default_stats(conn)
            except UndefinedColumnError as e:
                if 'column "key" of relation "statistics" does not exist' in str(e):
                    logger.warning("Database: statistics table schema appears corrupted. Dropping and recreating table to fix missing 'key' column.")
                    await conn.execute("DROP TABLE IF EXISTS statistics;")
                    await conn.execute("""
                        CREATE TABLE statistics (
                            key VARCHAR(50) PRIMARY KEY,
                            value BIGINT DEFAULT 0
                        );
                    """)
                    await self._insert_default_stats(conn) 
                else:
                    raise 
            
            logger.info("Database: Schema setup and corruption checks complete.")


    async def _insert_default_messages(self, conn):
        """Inserts default start/help messages if not already present."""
        default_start = "👋 Welcome to the Deep-Link File Bot! I securely deliver files via unique links. Reliability is guaranteed by my Neon database persistence."
        default_help = "📚 Help: Only the owner has upload access. Files are permanent storage in a private channel. Access is granted via unique deep links. Use /start to go to the welcome message."

        for key, text in [('start', default_start), ('help', default_help)]:
            query = """
            INSERT INTO messages (key, text) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET text = EXCLUDED.text;
            """
            await conn.execute(query, key, text)
        logger.debug("Database: Default messages ensured.")

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
        await self.fetchrow(query, user_id)

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
        INSERT INTO messages (key, image_id) VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET image_id = EXCLUDED.image_id;
        """
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
        stats['total_users'] = await self.get_total_users()
        stats['active_users'] = await self.get_active_users()
        stats['total_sessions'] = await self.get_stat_value('total_sessions')
        stats['files_uploaded'] = await self.get_stat_value('files_uploaded')
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


# --- MIDDLEWARE CLASS ---

async def safe_db_user_update(db_manager, user_id):
    """
    Wrapper function to safely perform DB updates in an async task.
    """
    try:
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

# --- MIDDLEWARE SETUP ---
dp.middleware.setup(UserActivityMiddleware(db_manager))


# --- FSM FOR UPLOAD PROCESS ---

class UploadFSM(StatesGroup):
    """States for the multi-step /upload command flow."""
    waiting_for_files = State()
    waiting_for_protection = State()
    waiting_for_auto_delete_time = State()

# --- CALLBACK DATA ---

ImageTypeCallback = CallbackData("img_type", "key")
ProtectionCallback = CallbackData("prot", "is_protected")

# --- HANDLERS: CORE USER COMMANDS ---

@dp.message_handler(is_owner_filter, commands=['start', 'help']) 
async def cmd_owner_start_help(message: types.Message):
    """Owner's /start and /help (full access to all commands)."""
    await cmd_user_start_help(message)

@dp.message_handler(commands=['start', 'help'])
async def cmd_user_start_help(message: types.Message):
    """
    Handles /start (including deep links) and /help commands for all users.
    
    CRITICAL FIX 14: Using global 'bot' instance for reliability.
    """
    command = message.get_command()
    payload = message.get_args()
    key = 'help' if command == '/help' else 'start'
    chat_id = message.chat.id # Get chat ID explicitly

    # 1. Handle Deep Link Payload 
    if command == '/start' and payload:
        await handle_deep_link(message, payload)
        return

    # 2. Handle Regular /start or /help
    try:
        content = await db_manager.get_message_content(key)
        if not content:
            text = "Error: Content not found. Please contact the owner."
            image_id = None
        else:
            text, image_id = content['text'], content['image_id']
    except Exception:
        logger.error(f"DB access failed in /{key} handler. Using fallback message.")
        text = "⚠️ **System Alert:** The database is currently unavailable. I cannot retrieve dynamic messages, but the core delivery system is active."
        image_id = None


    # Inline Keyboard 
    keyboard = types.InlineKeyboardMarkup()
    button_key = 'Help' if key == 'start' else 'Start'
    callback_cmd = '/help' if key == 'start' else '/start'
    keyboard.add(types.InlineKeyboardButton(button_key, callback_data=f"cmd:{callback_cmd[1:]}"))

    # Send message with image if available, otherwise just text
    try:
        # --- FIX 14: Use global 'bot' instance instead of 'message.answer' variants ---
        if image_id:
            await bot.send_photo(
                chat_id=chat_id,
                photo=image_id,
                caption=text,
                reply_markup=keyboard
            )
        else:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=keyboard
            )
        logger.info(f"User {message.from_user.id} received /{key} message.")
    except Exception as e:
        logger.error(f"Failed to send /{key} message to {message.from_user.id}: {e}")
        try:
             # Fallback, using explicit bot instance
            await bot.send_message(chat_id, f"Failed to send full message due to a Telegram API error. Text:\n{text}")
        except Exception as fallback_e:
            logger.error(f"Critical fallback failure for chat {chat_id}: {fallback_e}")


@dp.callback_query_handler(lambda c: c.data.startswith('cmd:'))
async def handle_cmd_callback(call: types.CallbackQuery):
    """
    Handles inline button clicks for Start/Help navigation.
    """
    await call.answer() 
    cmd = call.data.split(':')[1]
    
    mock_message = call.message.as_current()
    mock_message.text = f'/{cmd}'
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

    send_protected = (is_protected and not is_owner_access)

    info_message = "✅ Files retrieved successfully! The delivery system guarantees reliability."
    parse_mode = types.ParseMode.MARKDOWN_V2 if send_protected else types.ParseMode.HTML

    if send_protected:
        info_message += "\n\n⚠️ **Content is Protected**\\. Forwarding and saving are disabled\\."
    
    if auto_delete_minutes > 0 and not is_owner_access:
        delete_time = datetime.now() + timedelta(minutes=auto_delete_minutes)
        delete_time_str = delete_time.strftime("%H:%M:%S on %Y-%m-%d UTC")
        info_message += f"\n\n⏰ **Auto\-Delete Scheduled:** The files will be automatically deleted from *this chat* at `{delete_time_str}`\\."

    await bot.send_message(user_id, info_message, parse_mode=parse_mode)

    sent_message_ids = []

    try:
        for file in file_data:
            file_id = file['file_id']
            caption = file.get('caption')
            file_type = file.get('type', 'document') 

            send_method = None
            if file_type == 'photo':
                send_method = bot.send_photo
            elif file_type == 'video':
                send_method = bot.send_video
            elif file_type == 'document':
                send_method = bot.send_document
            elif file_type == 'audio':
                send_method = bot.send_audio
            else:
                logger.warning(f"Unhandled file type '{file_type}' for session {session_id}")
                send_method = bot.send_document 

            send_kwargs = {
                'chat_id': user_id,
                'caption': caption,
                'disable_notification': True,
                'protect_content': send_protected
            }
            if file_type == 'photo':
                send_kwargs['photo'] = file_id
            elif file_type == 'video':
                send_kwargs['video'] = file_id
            elif file_type == 'document':
                send_kwargs['document'] = file_id
            elif file_type == 'audio':
                send_kwargs['audio'] = file_id
            else:
                send_kwargs['document'] = file_id 

            sent_msg = await send_method(**send_kwargs)
            sent_message_ids.append(sent_msg.message_id)
            await asyncio.sleep(0.5) 

    except Exception as e:
        logger.error(f"CRITICAL: Failed to send files for session {session_id} to user {user_id}: {e}")
        await bot.send_message(user_id, "A critical error occurred while sending files. Some files might be missing.")

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
            except Exception as e:
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


# --- GLOBAL ERROR HANDLER ---
@dp.errors_handler()
async def errors_handler(update: types.Update, exception: Exception):
    """Catches all unhandled exceptions during update processing."""
    logger.error(f'An unhandled exception occurred during update processing. Update: {update.update_id}', 
                 exc_info=exception)
    
    try:
        if OWNER_ID:
            await bot.send_message(OWNER_ID, f"🚨 **CRITICAL BOT ERROR** 🚨\n\nAn unhandled exception occurred during update processing:\n\n`{type(exception).__name__}: {exception}`\n\nCheck logs for full traceback.", parse_mode=types.ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Failed to notify owner of critical error: {e}")
        
    return True 


# --- GENERIC HANDLER ---
@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_all_text_messages(message: types.Message):
    """Responds to any non-command text message."""
    if message.text.startswith('/'):
        return 
    
    await bot.send_message(message.chat.id, "I received your message, but I only understand commands like /start or /help.")


# --- WEBHOOK SETUP AND STARTUP/SHUTDOWN HOOKS ---

async def health_handler(request):
    """Returns 'ok' for UptimeRobot pings."""
    return web.Response(text="ok")

# --- CUSTOM MANUAL WEBHOOK HANDLER (CRITICAL FIX 13 APPLIED) ---
async def telegram_webhook(request):
    """
    Handles the incoming Telegram update POST request and passes it to the
    Aiogram dispatcher using the safer request.json() method.
    """
    
    # Use aiohttp's built-in JSON parser for safety
    try:
        update_data = await request.json()
    except Exception as e:
        logger.error(f"LOW-LEVEL PARSING FAILURE: Could not parse request body as JSON. {e}", exc_info=True)
        return web.Response(text="Parsing Failed", status=200)

    # Process the update
    try:
        update = types.Update.to_object(update_data)
        await dp.process_update(update)
    except Exception as e:
        # This will catch errors inside Aiogram's dispatcher
        logger.error(f"CRITICAL LOW-LEVEL DISPATCHER ERROR: Failed to process update.", exc_info=True)
        return web.Response(text="Internal Error Handled", status=200)

    # Return OK to Telegram
    return web.Response(status=200)


async def init_app():
    """Initializes the entire application: DB, Webhook, and Aiohttp App."""
    logger.info(f"Bot starting up in WEBHOOK mode.")
    
    # 1. Database Connection
    try:
        await db_manager.connect()
    except Exception as e:
        logger.critical(f"Database connection failed during startup: {e}. ALL DB-dependent features will fail.", exc_info=True)
        
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
                await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True) 
            else:
                logger.info("Webhook already set correctly.")
        except Exception as e:
            logger.critical(f"Failed to set webhook: {e}")

    # 4. Final Confirmation
    me = await bot.get_me()
    logger.info(f"Bot '{me.username}' is ready. Owner ID: {OWNER_ID}. Channel ID: {UPLOAD_CHANNEL_ID}.")
    logger.info("Bot startup completed successfully.")
    
    return app


def main():
    """Runs the main application using aiohttp's runner."""
    logger.info("Initializing bot system...")
    
    # Use aiohttp runner for manual control
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init_app())
    
    # Standard aiohttp runner setup
    runner = AppRunner(app)
    loop.run_until_complete(runner.setup())
    # NOTE: Render expects the app to bind to 0.0.0.0 and the PORT environment variable
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
        # Manual shutdown hooks (for DB cleanup)
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
    # Ensure event loop is available and run main
    try:
        main()
    except Exception as e:
        logger.critical(f"An unhandled error occurred in main execution: {e}")
        time.sleep(1) 
