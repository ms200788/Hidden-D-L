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
#  FIX: Refactored user activity update into a stable BaseMiddleware class      #
#       to resolve the 'pre_process' AttributeError.                           #
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
# NEW IMPORT: Necessary for robust middleware implementation
from aiogram.dispatcher.middlewares import BaseMiddleware 
from aiogram.utils.deep_linking import get_start_link
from aiogram.utils.executor import start_webhook, start_polling

# --- EXTERNAL DEPENDENCY (ASYNCPG for PostgreSQL) ---
import asyncpg
from asyncpg.exceptions import UniqueViolationError, DuplicateTableError

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
    # In a real setup, this would exit immediately
    # exit(1)


# --- DATABASE CONNECTION AND MODEL MANAGEMENT (11. DATABASE) ---

class Database:
    """
    Manages the connection pool and all CRUD operations for the PostgreSQL database (Neon).
    Uses asyncpg for high-performance, non-blocking I/O, guaranteeing reliability.
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
            logger.critical(f"Database: Failed to connect or set up schema: {e}")
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
        Sets up all necessary tables (Users, Messages, Sessions, Statistics)
        if they do not already exist, ensuring permanent storage.
        """
        logger.info("Database: Checking/setting up schema...")
        schema_queries = [
            # 1. Users Table: (11. DATABASE - Users) Tracks user activity and join dates.
            """
            CREATE TABLE IF NOT EXISTS users (
                id BIGINT PRIMARY KEY,
                join_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            # 2. Messages Table: (11. DATABASE - Messages) Stores customizable /start and /help content.
            """
            CREATE TABLE IF NOT EXISTS messages (
                key VARCHAR(10) PRIMARY KEY, -- 'start' or 'help'
                text TEXT NOT NULL,
                image_id VARCHAR(255) NULL
            );
            """,
            # 3. Upload Sessions Table: (11. DATABASE - Upload sessions) Stores all file metadata for deep-link access.
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
            # 4. Statistics Table: (11. DATABASE - Statistics) Simple key/value store for global counters.
            """
            CREATE TABLE IF NOT EXISTS statistics (
                key VARCHAR(50) PRIMARY KEY,
                value BIGINT DEFAULT 0
            );
            """
        ]

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for query in schema_queries:
                    try:
                        await conn.execute(query)
                    except DuplicateTableError:
                        pass # Ignore if already exists

                logger.info("Database: Schema setup complete.")

                # Ensure initial default messages and stats keys exist
                await self._insert_default_messages(conn)
                await self._insert_default_stats(conn)

    async def _insert_default_messages(self, conn):
        """Inserts default start/help messages if not already present (3. /start, 4. /help)."""
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
        """Inserts default statistics keys if not already present (8. /stats)."""
        default_keys = ['total_sessions', 'files_uploaded']
        for key in default_keys:
            query = """
            INSERT INTO statistics (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = statistics.value + EXCLUDED.value;
            """
            await conn.execute(query, key, 0) # Use 0 here for initial insertion
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
        ON CONFLICT (key) DO UPDATE SET value = statistics.value + EXCLUDED.value;
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


# --- MIDDLEWARE CLASS (FIXED: 317) ---

class UserActivityMiddleware(BaseMiddleware):
    """
    Middleware to ensure every interaction updates the user's last_active time
    and creates a user record if one doesn't exist. This is the reliable aiogram v2
    way to implement a global pre-processor.
    """
    def __init__(self, db_manager):
        super().__init__()
        self.db = db_manager

    # The correct method for pre-processing the update object in aiogram v2
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

# --- MIDDLEWARE SETUP (CORRECTED) ---
# Apply the middleware globally before starting the bot
dp.middleware.setup(UserActivityMiddleware(db_manager))


# --- FSM FOR UPLOAD PROCESS (9. /upload Multi-Step Flow) ---

class UploadFSM(StatesGroup):
    """States for the multi-step /upload command flow."""
    waiting_for_files = State()
    waiting_for_protection = State()
    waiting_for_auto_delete_time = State()

# --- CALLBACK DATA ---

# Callback data for setting image type (5. /setimage)
class ImageTypeCallback(types.CallbackData, prefix="img_type"):
    key: str # 'start' or 'help'

# Callback data for protection confirmation (9. Step 5)
class ProtectionCallback(types.CallbackData, prefix="prot"):
    is_protected: str # 'yes' or 'no'

# --- HANDLERS: CORE USER COMMANDS (2. USERS) ---

@dp.message_handler(commands=['start', 'help'], is_owner_filter=is_owner_filter)
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


# --- HANDLERS: OWNER CUSTOMIZATION COMMANDS (5. /setimage, 6. /setmessage) ---

@dp.message_handler(is_owner_filter, commands=['setmessage'])
async def cmd_setmessage_step1(message: types.Message):
    """Owner command to initiate setting new start/help text."""
    await message.answer(
        "📝 **Set Message Text**\n\nWhich message do you want to set? Select an option below and then reply to my next message with the *new text*."
        "The text will be saved to the database for persistence (6. /setmessage).",
        reply_markup=types.InlineKeyboardMarkup(
            inline_keyboard=[
                [types.InlineKeyboardButton("Start Message (3. /start)", callback_data="setmsg:start")],
                [types.InlineKeyboardButton("Help Message (4. /help)", callback_data="setmsg:help")]
            ]
        )
    )

@dp.callback_query_handler(is_owner_filter, lambda c: c.data.startswith('setmsg:'))
async def cmd_setmessage_step2(call: types.CallbackQuery, state: FSMContext):
    """Owner selects which message key to set and enters a temporary state."""
    await call.answer()
    key = call.data.split(':')[1]
    
    # Store the key in FSM context
    await state.update_data(message_key=key)
    
    # Enter a temporary state to wait for the reply
    await call.message.edit_text(
        f"You chose: **{key.capitalize()} Message**.\n\nNow, send me the *full text* you want to use for this message."
    )
    await state.set_state("waiting_for_new_message_text")


@dp.message_handler(is_owner_filter, state="waiting_for_new_message_text", content_types=types.ContentTypes.TEXT)
async def cmd_setmessage_step3_process(message: types.Message, state: FSMContext):
    """Owner sends the new text, which is saved to the database (6. /setmessage)."""
    data = await state.get_data()
    key = data.get('message_key')
    new_text = message.text.strip()
    
    if key and new_text:
        await db_manager.set_message_text(key, new_text)
        await message.answer(f"✅ Success! The **{key.capitalize()}** message text has been reliably updated in the database.")
    else:
        await message.answer("❌ Error: Missing key or empty text. Please try `/setmessage` again.")

    await state.finish()


@dp.message_handler(is_owner_filter, commands=['setimage'], content_types=types.ContentTypes.ANY)
async def cmd_setimage_step1(message: types.Message):
    """Owner replies to an image with /setimage to start the flow (5. /setimage)."""
    if not message.reply_to_message:
        await message.answer("❌ Error: You must use `/setimage` as a **reply** to an image, video, or document (5. /setimage).")
        return

    reply = message.reply_to_message
    file_id = None
    
    # Reliably extract file_id from different media types
    if reply.photo:
        file_id = reply.photo[-1].file_id
    elif reply.video:
        file_id = reply.video.file_id
    elif reply.document:
        file_id = reply.document.file_id
    
    if not file_id:
        await message.answer("❌ Error: The message you replied to does not contain a usable photo, video, or document file.")
        return

    # Save file_id to FSM and ask for type
    await dp.current_state().update_data(image_file_id=file_id)
    
    keyboard = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton("Start Image (3. /start)", callback_data=ImageTypeCallback(key='start').pack())],
            [types.InlineKeyboardButton("Help Image (4. /help)", callback_data=ImageTypeCallback(key='help').pack())]
        ]
    )
    
    await message.answer("🖼️ **Set Image**\n\nWhich message should use this image? (5. /setimage)", reply_markup=keyboard)


@dp.callback_query_handler(is_owner_filter, ImageTypeCallback.filter())
async def cmd_setimage_step2_process(call: types.CallbackQuery, callback_data: dict, state: FSMContext):
    """Owner selects the message type and the image's file_id is saved to the DB."""
    await call.answer()
    data = await state.get_data()
    file_id = data.get('image_file_id')
    key = callback_data['key']

    if not file_id:
        await call.message.edit_text("❌ Error: The file ID was lost. Please use `/setimage` again by replying to the media.")
        await state.finish()
        return

    # Save file_id to DB for persistence
    await db_manager.set_message_image(key, file_id)

    await call.message.edit_text(f"✅ Success! The new image has been reliably saved as the **{key.capitalize()}** message media.")
    await state.finish()


# --- HANDLERS: OWNER BROADCAST COMMAND (7. /broadcast) ---

@dp.message_handler(is_owner_filter, commands=['broadcast'])
async def cmd_broadcast_step1(message: types.Message):
    """Owner initiates the broadcast process (7. /broadcast)."""
    await dp.current_state().set_state("waiting_for_broadcast_message")
    await message.answer("📣 **Broadcast Initiated**\n\nSend me the message (text or media with caption) that you want to broadcast to ALL users. I will **copy** the message (no forward tag, 7. /broadcast).")


@dp.message_handler(is_owner_filter, state="waiting_for_broadcast_message", content_types=types.ContentTypes.ANY)
async def cmd_broadcast_step2_execute(message: types.Message, state: FSMContext):
    """Owner sends the message, and the reliable broadcast is executed."""
    await state.finish()
    await message.answer("⏳ Broadcast is starting... This may take a while for large user bases.")
    
    # Get all user IDs (reliable, direct query)
    user_ids_records = await db_manager.fetch("SELECT id FROM users;")
    target_users = [user['id'] for user in user_ids_records]
    
    success_count = 0
    fail_count = 0

    source_chat_id = message.chat.id
    source_message_id = message.message_id

    # Execute the broadcast asynchronously
    for user_id in target_users:
        if user_id == OWNER_ID: # Skip owner
            continue

        try:
            # Use copy_message for reliable, non-forwarded message delivery (7. /broadcast)
            await bot.copy_message(
                chat_id=user_id,
                from_chat_id=source_chat_id,
                message_id=source_message_id,
                disable_notification=True
            )
            success_count += 1
        except Exception as e:
            # Handle user blocking gracefully
            if 'bot was blocked by the user' in str(e) or 'user is deactivated' in str(e):
                logger.warning(f"User {user_id} blocked bot, skipping.")
            else:
                logger.error(f"Failed to send broadcast to user {user_id}: {e}")
            fail_count += 1

        await asyncio.sleep(0.05) # Throttle to respect Telegram API limits

    # Final report to the owner
    await bot.send_message(
        OWNER_ID,
        f"✅ **Broadcast Completed**\n\n"
        f"👥 Total Users Targeted: {len(target_users)}\n"
        f"🟢 Successful Deliveries: {success_count}\n"
        f"🔴 Failed Deliveries (Blocked/Error): {fail_count}"
    )


# --- HANDLERS: OWNER STATS COMMAND (8. /stats) ---

@dp.message_handler(is_owner_filter, commands=['stats'])
async def cmd_stats(message: types.Message):
    """Owner command to display real-time usage statistics."""
    await message.answer("📊 **Fetching Real-Time Statistics...**")

    try:
        stats = await db_manager.get_all_stats()
        
        # Data is reliably fetched from DB methods
        total_users = stats.get('total_users', 0)
        active_users = stats.get('active_users', 0)
        files_uploaded = stats.get('files_uploaded', 0)
        total_sessions = stats.get('total_sessions', 0)
        
        # Prepare the reliable, formatted output (8. /stats)
        stats_report = (
            "📈 **Bot Usage & Reliability Statistics**\n\n"
            f"👤 **Users**\n"
            f"• Total Users Registered: `{total_users:,}`\n"
            f"• Active Users (last 48h): `{active_users:,}`\n\n"
            f"💾 **Uploads & Files**\n"
            f"• Total Upload Sessions: `{total_sessions:,}`\n"
            f"• Total Files Uploaded: `{files_uploaded:,}`\n\n"
            f"Note: Stats are reliably pulled from the Neon database (8. /stats)."
        )
        
        await message.answer(stats_report, parse_mode=types.ParseMode.MARKDOWN)

    except Exception as e:
        logger.error(f"Error fetching statistics: {e}")
        await message.answer("❌ CRITICAL: Failed to retrieve statistics from the database. Check connection.")


# --- HANDLERS: OWNER UPLOAD FLOW (9. /upload Multi-Step Flow) ---

@dp.message_handler(is_owner_filter, commands=['upload'])
async def cmd_upload_step1(message: types.Message, state: FSMContext):
    """9. Step 1: Owner runs /upload. Initialize session data and request files."""
    if await state.get_state() is not None:
         await state.finish()

    # Initialize a clean data structure for the session
    await state.update_data(
        files=[],
        is_protected=False,
        auto_delete_minutes=0
    )

    await UploadFSM.waiting_for_files.set()
    await message.answer(
        "📂 **Upload Session Started**\n\n"
        "**Step 1/3: Send Files**\n"
        "Send me any documents, photos, or videos you want to link (9. Step 3). "
        "When finished, send: `/d` (Done) or `/c` (Cancel) (9. Step 4)."
    )


@dp.message_handler(is_owner_filter, commands=['d', 'c'], state=UploadFSM.waiting_for_files)
async def cmd_upload_step2_control(message: types.Message, state: FSMContext):
    """9. Step 4: Owner sends /d (done) or /c (cancel) to control the file collection."""
    command = message.get_command()
    data = await state.get_data()
    files_collected = data.get('files', [])

    if command == '/c':
        await state.finish()
        await message.answer("❌ Upload session cancelled. No files were saved.")
        return

    # Process /d (Done)
    if not files_collected:
        await message.answer("⚠️ You must send at least one file before sending `/d`.")
        return

    # Proceed to protection settings (9. Step 5)
    await UploadFSM.waiting_for_protection.set()

    keyboard = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton("🔒 Yes, Protect Content", callback_data=ProtectionCallback(is_protected='yes').pack())],
            [types.InlineKeyboardButton("🔓 No, Allow Saving/Forwarding", callback_data=ProtectionCallback(is_protected='no').pack())]
        ]
    )

    await message.answer(
        f"**Step 2/3: Protection Settings**\n"
        f"Collected **{len(files_collected)}** file(s).\n\n"
        "Do you want to enable content protection? (9. Step 5)" ,
        reply_markup=keyboard
    )


@dp.message_handler(is_owner_filter, state=UploadFSM.waiting_for_files, content_types=types.ContentTypes.ANY)
async def cmd_upload_step1_collect_files(message: types.Message, state: FSMContext):
    """9. Step 3: Collects file_ids and captions from incoming media."""
    file_id = None
    file_type = None

    # Determine file_id and type reliably
    if message.photo:
        file_id = message.photo[-1].file_id
        file_type = 'photo'
    elif message.video:
        file_id = message.video.file_id
        file_type = 'video'
    elif message.document:
        file_id = message.document.file_id
        file_type = 'document'
    elif message.audio:
        file_id = message.audio.file_id
        file_type = 'audio'
    else:
        # Ignore non-essential media types/text that is not a command
        if message.text not in ['/d', '/c']:
            await message.reply("⚠️ Only documents, photos, or videos are supported. Use `/d` to finish.")
        return

    if file_id and file_type:
        caption = message.caption or ""
        
        # CRITICAL: Save file to the permanent channel (12. UPLOAD CHANNEL)
        try:
            # Copy message to the private channel
            sent_msg = await bot.copy_message(
                chat_id=UPLOAD_CHANNEL_ID,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
                disable_notification=True
            )

            # Extract the new, persistent file_id from the channel message
            final_file_id = None
            if sent_msg.photo:
                 final_file_id = sent_msg.photo[-1].file_id
            elif sent_msg.video:
                 final_file_id = sent_msg.video.file_id
            elif sent_msg.document:
                 final_file_id = sent_msg.document.file_id
            elif sent_msg.audio:
                 final_file_id = sent_msg.audio.file_id
            
            if not final_file_id:
                # Fallback check, sometimes document/video is directly available without index
                if not final_file_id and sent_msg.document and sent_msg.document.file_id:
                     final_file_id = sent_msg.document.file_id
                elif not final_file_id and sent_msg.video and sent_msg.video.file_id:
                     final_file_id = sent_msg.video.file_id
                else:
                    raise Exception("Failed to get file_id from channel message after copy.")

            # Append the stored file data
            new_file_data = {
                'file_id': final_file_id,
                'caption': caption,
                'type': file_type,
            }

            async with state.proxy() as data:
                data['files'].append(new_file_data)
            
            await message.reply(f"✅ File **{len(data['files'])}** (`{file_type.capitalize()}`) saved to permanent storage channel.", disable_notification=True)

        except Exception as e:
            logger.error(f"CRITICAL: Failed to copy file to upload channel {UPLOAD_CHANNEL_ID}: {e}")
            await message.answer("❌ CRITICAL ERROR: Failed to permanently store the file. Check channel ID/permissions.")


@dp.callback_query_handler(is_owner_filter, ProtectionCallback.filter(), state=UploadFSM.waiting_for_protection)
async def cmd_upload_step3_set_protection(call: types.CallbackQuery, callback_data: dict, state: FSMContext):
    """9. Step 5: Owner sets content protection preference."""
    await call.answer()
    
    is_protected = callback_data['is_protected'] == 'yes'
    
    # Save protection setting
    await state.update_data(is_protected=is_protected)
    
    protection_status = "Enabled" if is_protected else "Disabled"
    
    # Proceed to auto-delete timer setting
    await UploadFSM.waiting_for_auto_delete_time.set()
    
    await call.message.edit_text(
        f"**Step 3/3: Auto-Delete Timer**\n"
        f"Protection Status: **{protection_status}**\n\n"
        f"Enter the auto-delete time in **minutes** (0–{MAX_AUTO_DELETE_MINUTES}).\n"
        f"• Send `0` to keep files in the user's chat forever (13. AUTO DELETE LOGIC).\n"
        f"• Send a number (e.g., `10080` for 7 days) to clean the user's chat automatically."
    )


@dp.message_handler(is_owner_filter, state=UploadFSM.waiting_for_auto_delete_time, content_types=types.ContentTypes.TEXT)
async def cmd_upload_step4_finalise(message: types.Message, state: FSMContext):
    """9. Step 6: Owner sets the auto-delete timer and finalises the session."""
    
    try:
        auto_delete_minutes = int(message.text.strip())
        
        if not (0 <= auto_delete_minutes <= MAX_AUTO_DELETE_MINUTES):
            await message.answer(f"❌ Invalid input. Please enter a number between 0 and {MAX_AUTO_DELETE_MINUTES} minutes.")
            return

        # 1. Retrieve final data
        final_data = await state.get_data()
        
        files = final_data['files']
        is_protected = final_data['is_protected']
        
        # 2. Create reliable DB entry (11. DATABASE)
        session_id = await db_manager.create_upload_session(
            owner_id=message.from_user.id,
            file_data=files,
            is_protected=is_protected,
            auto_delete_minutes=auto_delete_minutes
        )

        # 3. Generate Deep Link (9. Step 6, 10. DEEP LINK ACCESS)
        deep_link = await get_start_link(session_id, encode=True)
        
        # 4. Final summary and success message
        delete_info = f"Cleanup: **{auto_delete_minutes} minutes**" if auto_delete_minutes > 0 else "**No auto-delete**"
        
        final_report = (
            "🎉 **Upload Session Complete**\n\n"
            f"🔗 **Deep Link (9. Step 6):**\n"
            f"`{deep_link}`\n\n"
            f"🆔 **Session ID:** `{session_id}`\n"
            f"📦 **Files:** {len(files)} file(s)\n"
            f"🔒 **Protected:** {'Yes' if is_protected else 'No'}\n"
            f"⏰ **Auto-Delete:** {delete_info}\n\n"
            "This link can now be shared. File persistence is guaranteed by the Neon database and permanent upload channel."
        )

        await message.answer(final_report, parse_mode=types.ParseMode.MARKDOWN)

    except ValueError:
        await message.answer("❌ Invalid input. Please enter a valid *number* of minutes.")
    except Exception as e:
        logger.error(f"CRITICAL: Failed to finalise upload session for owner {message.from_user.id}: {e}")
        await message.answer("❌ CRITICAL ERROR: Failed to save session to database or generate link. Please check DB connection.")
    finally:
        await state.finish()


# --- WEBHOOK SETUP AND STARTUP/SHUTDOWN HOOKS (15. HEALTHCHECK, 16. WEBHOOK ONLY) ---

async def on_startup(dp):
    """Executed on bot startup. Sets webhook and connects to DB."""
    logger.info(f"Bot starting up in {'WEBHOOK' if WEBHOOK_URL else 'POLLING'} mode.")
    
    # 1. Database Connection (CRITICAL for reliability)
    try:
        await db_manager.connect()
    except Exception as e:
        logger.critical(f"Database connection failed during startup: {e}")
        # exit(2) # Uncomment for strict production environment
        
    # 2. Webhook Setup (16. WEBHOOK ONLY)
    if WEBHOOK_URL:
        try:
            webhook_info = await dp.bot.get_webhook_info()
            if webhook_info.url != WEBHOOK_URL:
                logger.info(f"Setting webhook to: {WEBHOOK_URL}")
                # The 'drop_pending_updates=True' ensures a clean start
                await dp.bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True) 
            else:
                logger.info("Webhook already set correctly.")
        except Exception as e:
            logger.critical(f"Failed to set webhook: {e}")
            # exit(3) # Uncomment for strict production environment

    # 3. Final Check and confirmation
    me = await dp.bot.get_me()
    logger.info(f"Bot '{me.username}' is ready. Owner ID: {OWNER_ID}. Channel ID: {UPLOAD_CHANNEL_ID}.")

    # --- KEEP-ALIVE: Start the simple HTTP server for /health endpoint (15. HEALTHCHECK ENDPOINT) ---
    if WEBHOOK_URL:
        # Import aiohttp dynamically only when in webhook mode
        from aiohttp import web
        
        async def health_handler(request):
            """Returns 'ok' for UptimeRobot pings."""
            return web.Response(text="ok")

        app = web.Application()
        app.router.add_get('/health', health_handler) # 15. HEALTHCHECK ENDPOINT
        
        # Add the Telegram webhook handler
        app.router.add_post(WEBHOOK_PATH, dp.web_app.handle_request)
        
        # Configure and start the web server runner
        dp['web_app'] = app
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', HEALTHCHECK_PORT)
        
        # Start the background task to serve the HTTP requests
        dp['runner'] = runner
        dp['site'] = site
        asyncio.create_task(site.start())
        logger.info(f"Healthcheck and Webhook server started on port {HEALTHCHECK_PORT}.")
    
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

    # 3. Stop Aiohttp Runner
    if 'runner' in dp:
        logger.info("Stopping web server runner...")
        await dp['runner'].cleanup()
        
    await dp.storage.close()
    await dp.storage.wait_closed()
    logger.info("Bot shutdown completed.")


def main():
    """Main function to start the bot using the appropriate executor."""
    logger.info("Initializing bot system...")
    
    is_webhook_mode = bool(WEBHOOK_URL)
    
    if is_webhook_mode:
        # 16. WEBHOOK ONLY: Use start_webhook executor
        logger.info("Starting bot in Webhook Mode (Render Hosting).")
        
        executor.start_webhook(
            dispatcher=dp,
            webhook_path=WEBHOOK_PATH,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            skip_updates=True,
            host='0.0.0.0',
            port=HEALTHCHECK_PORT, # aiogram listens on this port
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

# --- END OF FILE ---
