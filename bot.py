# -*- coding: utf-8 -*-
"""
################################################################################
#  HIGHLY RELIABLE TELEGRAM BOT IMPLEMENTATION - AIOGRAM V2 & ASYNCPG/NEON      #
#                                                                              #
#  This bot utilizes aiogram v2 for stability and asyncpg for robust, non-      #
#  blocking interaction with a Neon (PostgreSQL) database. It includes a        #
#  complex multi-step upload flow, deep linking, dynamic content, and owner     #
#  management, designed for deployment on Render via webhooks.                  #
#                                                                              #
#  FIX 1-3 (Previous): Database schema repair and initial setup.                
#  FIX 4 (Current): Refactored webhook setup by removing the conflicting        
#       `web_app` argument from `executor.start_webhook` and injecting the      
#       `/health` endpoint via the `dp.on_startup` signal handler, resolving    
#       the "unexpected keyword argument 'web_app'" error.                      
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
from aiohttp import web # Used for the healthcheck response and application type

# --- CONFIGURATION AND ENVIRONMENT SETUP ---

# Configure logging to ensure all operations are tracked for reliability
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=getattr(logging, LOG_LEVEL),
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Mandatory environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))  # Owner for full control
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0")) # Channel to store files

# Webhook configuration for Render hosting (16. WEBHOOK ONLY)
WEBHOOK_HOST = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("WEBHOOK_HOST")
WEBHOOK_PATH = f'/{BOT_TOKEN}'
WEBHOOK_URL = f'{WEBHOOK_HOST}{WEBHOOK_PATH}' if WEBHOOK_HOST else None

# Keep-Alive Configuration (15. HEALTHCHECK ENDPOINT)
HEALTHCHECK_PORT = int(os.getenv("PORT", 8080))

# Hard limit for auto-delete time (10080 minutes = 7 days)
MAX_AUTO_DELETE_MINUTES = 10080

# Check for critical configuration
if not all([BOT_TOKEN, DATABASE_URL, OWNER_ID, UPLOAD_CHANNEL_ID]):
    logger.error("CRITICAL: One or more essential environment variables are missing (BOT_TOKEN, DATABASE_URL, OWNER_ID, UPLOAD_CHANNEL_ID). Exiting.")
    # exit(1)


# --- DATABASE CONNECTION AND MODEL MANAGEMENT (11. DATABASE) ---

class Database:
    """
    Manages the connection pool and all CRUD operations for the PostgreSQL database (Neon).
    """
    def __init__(self, dsn):
        self.dsn = dsn
        self._pool = None

    async def connect(self):
        """Initializes the database connection pool and ensures schema exists."""
        logger.info("Database: Attempting to connect to PostgreSQL...")
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
            logger.critical(f"Database connection failed during startup: {e}")
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
        """Fetches multiple rows from the database (e.g., all user IDs for broadcast)."""
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchval(self, query, *args):
        """Fetches a single value from the database (e.g., stat counts)."""
        async with self._pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def setup_schema(self):
        """
        Sets up all necessary tables and performs reliable checks to ensure
        the schema is correct, implementing repair logic for common corruption.
        """
        logger.info("Database: Checking/setting up schema...")
        schema_queries = [
            # 1. Users Table
            """
            CREATE TABLE IF NOT EXISTS users (
                id BIGINT PRIMARY KEY,
                join_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            # 2. Messages Table
            """
            CREATE TABLE IF NOT EXISTS messages (
                key VARCHAR(10) PRIMARY KEY, -- 'start' or 'help'
                text TEXT NOT NULL,
                image_id VARCHAR(255) NULL
            );
            """,
            # 3. Upload Sessions Table
            """
            CREATE TABLE IF NOT EXISTS sessions (
                session_id VARCHAR(50) PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                file_data JSONB NOT NULL, -- [{'file_id': '...', 'caption': '...', 'type': '...'}]
                is_protected BOOLEAN DEFAULT FALSE,
                auto_delete_minutes INTEGER DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            # 4. Statistics Table
            """
            CREATE TABLE IF NOT EXISTS statistics (
                key VARCHAR(50) PRIMARY KEY,
                value BIGINT DEFAULT 0
            );
            """
        ]

        async with self._pool.acquire() as conn:
            # Step 1: Attempt to create all tables
            for query in schema_queries:
                try:
                    # Execute DDL in autocommit mode
                    await conn.execute(query)
                except DuplicateTableError:
                    pass 

            logger.info("Database: Schema setup complete (initial creation attempts).")

            # Step 2: Ensure initial default messages exist and fix corruption if found
            try:
                await self._insert_default_messages(conn)
            except UndefinedColumnError as e:
                # Fix for corrupted 'messages' table
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
                    logger.info("Database: messages table successfully reset and populated.")
                else:
                    raise 

            # Step 3: Ensure stats are initialized and fix corruption if found
            try:
                await self._insert_default_stats(conn)
            except UndefinedColumnError as e:
                # NEW FIX: Fix for corrupted 'statistics' table
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
                    logger.info("Database: statistics table successfully reset and populated.")
                else:
                    raise # Re-raise if it's another UndefinedColumnError


    async def _insert_default_messages(self, conn):
        """Inserts default start/help messages if not already present."""
        default_start = "👋 Welcome to the Deep-Link File Bot! I securely deliver files via unique links. Reliability is guaranteed by my Neon database persistence."
        default_help = "📚 Help: Only the owner has upload access. Files are permanent storage in a private channel (12. UPLOAD CHANNEL). Access is granted via unique deep links. Use /start to go to the welcome message."

        for key, text in [('start', default_start), ('help', default_help)]:
            query = """
            INSERT INTO messages (key, text) VALUES ($1, $2)
            ON CONFLICT (key) DO NOTHING;
            """
            await conn.execute(query, key, text)
        logger.debug("Database: Default messages ensured.")

    async def _insert_default_stats(self, conn):
        """Inserts default statistics keys if not already present."""
        default_keys = ['total_sessions', 'files_uploaded']
        for key in default_keys:
            query = """
            INSERT INTO statistics (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO NOTHING;
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
        try:
            await self.fetchrow(query, user_id)
        except Exception as e:
            logger.error(f"DB Error in get_or_create_user for {user_id}: {e}")

    async def get_total_users(self):
        """Returns the total number of users (8. /stats)."""
        return await self.fetchval("SELECT COUNT(*) FROM users;")

    async def get_active_users(self):
        """Returns the number of users active in the last 48 hours (8. /stats)."""
        time_threshold = datetime.now() - timedelta(hours=48)
        return await self.fetchval(
            "SELECT COUNT(*) FROM users WHERE last_active >= $1;",
            time_threshold
        )

    # --- MESSAGE/CUSTOMIZATION METHODS (5. /setimage, 6. /setmessage) ---
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

    # --- SESSION/UPLOAD METHODS (9. /upload, 10. DEEP LINK ACCESS) ---
    async def create_upload_session(self, owner_id, file_data, is_protected, auto_delete_minutes):
        """Creates a new upload session and returns its unique ID (9. Step 6)."""
        # Ensure deep link is random (10. DEEP LINK ACCESS)
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

    # --- STATISTICS METHODS (8. /stats) ---
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
        # Derived from user table
        stats['total_users'] = await self.get_total_users()
        stats['active_users'] = await self.get_active_users()
        # Derived from statistics table
        stats['total_sessions'] = await self.get_stat_value('total_sessions')
        stats['files_uploaded'] = await self.get_stat_value('files_uploaded')
        return stats


# Initialize Database instance
db_manager = Database(DATABASE_URL)


# --- AIOGRAM INITIALIZATION (1. CORE SETUP) ---

# Initialize Bot and Dispatcher
bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage() # Using in-memory storage for FSM state (fast)
dp = Dispatcher(bot, storage=storage)


# --- OWNER/SECURITY UTILITIES (2. COMMANDS – OWNER VS USERS) ---

def is_owner_filter(message: types.Message):
    """Filter to check if the user is the bot owner (defined by OWNER_ID)."""
    return message.from_user.id == OWNER_ID


# --- MIDDLEWARE CLASS ---

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
        else:
            return # Ignore non-message/callback updates

        if user_id:
            # Non-blocking update to avoid delaying user response
            asyncio.create_task(self.db.get_or_create_user(user_id))
            data['user_id'] = user_id

# --- MIDDLEWARE SETUP ---
# Apply the middleware globally before starting the bot
dp.middleware.setup(UserActivityMiddleware(db_manager))


# --- FSM FOR UPLOAD PROCESS (9. /upload Multi-Step Flow) ---

class UploadFSM(StatesGroup):
    """States for the multi-step /upload command flow."""
    waiting_for_files = State()
    waiting_for_protection = State()
    waiting_for_auto_delete_time = State()

# --- CALLBACK DATA ---

ImageTypeCallback = CallbackData("img_type", "key")
ProtectionCallback = CallbackData("prot", "is_protected")

# --- HANDLERS: CORE USER COMMANDS (2. USERS) ---

@dp.message_handler(is_owner_filter, commands=['start', 'help']) 
async def cmd_owner_start_help(message: types.Message):
    """Owner's /start and /help (full access to all commands)."""
    await cmd_user_start_help(message)

@dp.message_handler(commands=['start', 'help'])
async def cmd_user_start_help(message: types.Message):
    """
    Handles /start (including deep links) (3. /start) and /help (4. /help)
    commands for all users. Retrieves dynamic content from the database.
    """
    command = message.get_command()
    payload = message.get_args()
    key = 'help' if command == '/help' else 'start'

    # 1. Handle Deep Link Payload (10. DEEP LINK ACCESS)
    if command == '/start' and payload:
        await handle_deep_link(message, payload)
        return

    # 2. Handle Regular /start or /help
    content = await db_manager.get_message_content(key)
    if not content:
        # Reliability Fallback
        text = "Error: Content not found. Please contact the owner."
        image_id = None
    else:
        text, image_id = content['text'], content['image_id']

    # Inline Keyboard (3. /start - "Help" button)
    keyboard = types.InlineKeyboardMarkup()
    button_key = 'Help' if key == 'start' else 'Start'
    callback_cmd = '/help' if key == 'start' else '/start'
    keyboard.add(types.InlineKeyboardButton(button_key, callback_data=f"cmd:{callback_cmd[1:]}"))

    # Send message with image if available, otherwise just text
    try:
        if image_id:
            await message.answer_photo(
                photo=image_id,
                caption=text,
                reply_markup=keyboard
            )
        else:
            await message.answer(
                text=text,
                reply_markup=keyboard
            )
        logger.info(f"User {message.from_user.id} received /{key} message.")
    except Exception as e:
        logger.error(f"Failed to send /{key} message to {message.from_user.id}: {e}")
        await message.answer(f"Failed to send full message due to an error. Text:\n{text}")


@dp.callback_query_handler(lambda c: c.data.startswith('cmd:'))
async def handle_cmd_callback(call: types.CallbackQuery):
    """Handles inline button clicks for Start/Help navigation."""
    cmd = call.data.split(':')[1]
    await call.answer()
    # Mock a message object to reuse the main handlers
    mock_message = call.message.as_current()
    mock_message.text = f'/{cmd}'
    await cmd_user_start_help(mock_message)

# --- DEEP LINK ACCESS LOGIC (10. DEEP LINK ACCESS, 14. OWNER BYPASS) ---

async def handle_deep_link(message: types.Message, session_id: str):
    """
    Retrieves and sends files associated with a deep link session ID.
    Handles owner bypass and auto-delete scheduling.
    """
    logger.info(f"User {message.from_user.id} accessed deep link with session ID: {session_id[:8]}...")
    user_id = message.from_user.id
    session = await db_manager.get_upload_session(session_id)

    if not session:
        await message.answer("❌ Error: The file session ID is invalid or has expired. Files remain in DB & channel.")
        return

    # Owner Bypass Check (14. OWNER BYPASS)
    is_owner_access = (user_id == OWNER_ID)

    file_data = json.loads(session['file_data'])
    is_protected = session['is_protected']
    auto_delete_minutes = session['auto_delete_minutes']

    # Apply protection only if content is marked protected AND the user is NOT the owner
    send_protected = (is_protected and not is_owner_access)

    # 1. Inform user about protection/deletion
    info_message = "✅ Files retrieved successfully! The delivery system guarantees reliability."
    parse_mode = types.ParseMode.MARKDOWN_V2 if send_protected else types.ParseMode.HTML

    if send_protected:
        # Owner Bypass: Protect Content ignored (can forward/save)
        # Note: MarkdownV2 requires escaping periods
        info_message += "\n\n⚠️ **Content is Protected**\\. Forwarding and saving are disabled\\."
    
    # Auto-delete not applied to owner (14. OWNER BYPASS)
    if auto_delete_minutes > 0 and not is_owner_access:
        delete_time = datetime.now() + timedelta(minutes=auto_delete_minutes)
        delete_time_str = delete_time.strftime("%H:%M:%S on %Y-%m-%d UTC")
        # 13. AUTO DELETE LOGIC: Only user’s chat is cleaned
        info_message += f"\n\n⏰ **Auto\-Delete Scheduled:** The files will be automatically deleted from *this chat* at `{delete_time_str}`\\."

    # Use a direct response without parse_mode if no Markdown is needed to avoid escaping issues
    await message.answer(info_message, parse_mode=parse_mode)

    # 2. Send the files (reliable method: send one by one)
    sent_message_ids = []

    try:
        for file in file_data:
            file_id = file['file_id']
            caption = file.get('caption')
            file_type = file.get('type', 'document') # Default for safety

            # Determine the correct sending method (Reliable File Dispatch)
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
                send_method = bot.send_document # Default to document

            # Execute the send command
            # Prepare arguments dynamically based on file type
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
                send_kwargs['document'] = file_id # Fallback

            sent_msg = await send_method(**send_kwargs)
            sent_message_ids.append(sent_msg.message_id)
            await asyncio.sleep(0.5) # Throttle to prevent flooding/API limits

    except Exception as e:
        logger.error(f"CRITICAL: Failed to send files for session {session_id} to user {user_id}: {e}")
        await message.answer("A critical error occurred while sending files. Some files might be missing.")

    # 3. Schedule Auto-Deletion (13. AUTO DELETE LOGIC)
    if auto_delete_minutes > 0 and not is_owner_access and sent_message_ids:
        delay = auto_delete_minutes * 60
        # Schedule the deletion task to run independently
        asyncio.create_task(schedule_deletion(user_id, sent_message_ids, delay))


async def schedule_deletion(chat_id: int, message_ids: list, delay_seconds: int):
    """
    Schedules the deletion of messages in a user's chat after a specified delay.
    Only user’s chat is cleaned; DB & channel untouched.
    """
    logger.info(f"Scheduling deletion for {len(message_ids)} messages in chat {chat_id} in {delay_seconds} seconds.")
    try:
        await asyncio.sleep(delay_seconds)
        
        # Attempt to delete the message batch
        for msg_id in message_ids:
            try:
                await bot.delete_message(chat_id, msg_id)
                await asyncio.sleep(0.1) # Throttle
            except Exception as e:
                # Log but continue with the rest of the batch (reliability)
                logger.debug(f"Failed to delete message {msg_id} in chat {chat_id}: {e}")

        logger.info(f"Successfully cleaned up {len(message_ids)} messages in chat {chat_id}.")
        # Optional cleanup notification
        try:
            await bot.send_message(chat_id, "✨ The uploaded files have been automatically cleaned from this chat.", disable_notification=True)
        except:
             pass 

    except asyncio.CancelledError:
        logger.info(f"Deletion task for chat {chat_id} was cancelled.")
    except Exception as e:
        logger.error(f"CRITICAL: Error during scheduled deletion for chat {chat_id}: {e}")


# --- WEBHOOK SETUP AND STARTUP/SHUTDOWN HOOKS (15. HEALTHCHECK, 16. WEBHOOK ONLY) ---

# Handler for the health check
async def health_handler(request):
    """Returns 'ok' for UptimeRobot pings (15. HEALTHCHECK ENDPOINT)."""
    return web.Response(text="ok")

async def setup_web_routes(app: web.Application):
    """
    Registers the /health route on the aiohttp web application instance.
    This function is called by the executor's on_startup signal.
    """
    app.router.add_get('/health', health_handler)
    logger.info("Healthcheck route '/health' registered successfully via setup_web_routes.")


async def on_startup(dp):
    """Executed on bot startup. Sets webhook and connects to DB."""
    logger.info(f"Bot starting up in {'WEBHOOK' if WEBHOOK_URL else 'POLLING'} mode.")
    
    # 1. Database Connection (CRITICAL for reliability)
    try:
        await db_manager.connect()
    except Exception as e:
        logger.critical(f"Database connection failed during startup: {e}")
        # Rely on the host environment to handle the crash/restart
        raise 
        
    # 2. Webhook Setup
    if WEBHOOK_URL:
        # Note: Webhook is set here to ensure the latest URL is used if it changes
        try:
            webhook_info = await dp.bot.get_webhook_info()
            if webhook_info.url != WEBHOOK_URL:
                logger.info(f"Setting webhook to: {WEBHOOK_URL}")
                await dp.bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True) 
            else:
                logger.info("Webhook already set correctly.")
        except Exception as e:
            logger.critical(f"Failed to set webhook: {e}")

    # 3. Final Check and confirmation
    me = await dp.bot.get_me()
    logger.info(f"Bot '{me.username}' is ready. Owner ID: {OWNER_ID}. Channel ID: {UPLOAD_CHANNEL_ID}.")

    logger.info("Bot startup completed successfully.")


async def on_shutdown(dp):
    """Executed on bot shutdown. Closes DB connection and clears webhook."""
    logger.info("Bot shutting down...")
    
    # 1. Database Disconnection
    await db_manager.close()
    
    # 2. Webhook Cleanup
    if WEBHOOK_URL:
        logger.info("Clearing webhook...")
        await dp.bot.delete_webhook()

    # 3. Stop Aiohttp Runner (Handled by executor since we pass 'app')
        
    await dp.storage.close()
    await dp.storage.wait_closed()
    logger.info("Bot shutdown completed.")


def main():
    """Main function to start the bot using the appropriate executor."""
    logger.info("Initializing bot system...")
    
    is_webhook_mode = bool(WEBHOOK_URL)
    
    if is_webhook_mode:
        
        # CRITICAL FIX: Inject the custom /health route using the dispatcher's 
        # on_startup signal BEFORE calling the executor. The executor calls 
        # these signals when the aiohttp app is available.
        dp.on_startup.add_handler(setup_web_routes)

        logger.info("Starting bot in Webhook Mode (Render Hosting).")
        
        executor.start_webhook(
            dispatcher=dp,
            webhook_path=WEBHOOK_PATH,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            skip_updates=True,
            host='0.0.0.0',
            port=HEALTHCHECK_PORT, 
            # REMOVED: web_app=app -- This caused the unexpected keyword argument error.
        )
    else:
        # Polling fallback (disabled per request but useful for local testing)
        logger.warning("WEBHOOK_HOST not set. Starting bot in Polling Mode (for local testing only).")
        executor.start_polling(
            dp,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            skip_updates=True
        )


if __name__ == '__main__':
    # Ensure event loop is available and run main
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
    except Exception as e:
        logger.critical(f"An unhandled error occurred in main execution: {e}")
        time.sleep(1) # Pause before final exit
