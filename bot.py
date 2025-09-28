# ----------------------------------------------------------------------------------
# TELEGRAM UPLOAD BOT - AIOGRAM V2, WEBHOOK, ASYNCPG (NEON) ARCHITECTURE
# ----------------------------------------------------------------------------------
# IMPORTANT: This code is structured for a production environment using asyncpg.
# The user MUST install 'asyncpg' and set the DATABASE_URL environment variable
# for this code to connect to their Neon database instance.
# ----------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------
# SECTION 1: IMPORTS AND CONFIGURATION
# ----------------------------------------------------------------------------------
import os
import asyncio
import logging
import time
import json
import uuid
import datetime
import aiohttp
import asyncpg # <-- REQUIRED LIBRARY FOR NEON POSTGRESQL

from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils.deep_linking import get_start_link
from aiogram.utils.executor import start_webhook

# Configure logging
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# --- Environment Variables (Required for Deployment) ---
# NOTE: In a real Render environment, these are loaded from os.environ.get()
API_TOKEN = os.environ.get("API_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")
OWNER_ID = int(os.environ.get("OWNER_ID", 123456789)) # Replace with your actual Telegram User ID
UPLOAD_CHANNEL_ID = int(os.environ.get("UPLOAD_CHANNEL_ID", -1001234567890)) # Replace with your actual Channel ID
RENDER_URL = os.environ.get("RENDER_URL", "https://your-render-app-name.onrender.com")

# Neon Database URL
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:pass@host:port/dbname")

# --- Webhook Settings ---
WEBAPP_HOST = '0.0.0.0'
WEBAPP_PORT = int(os.environ.get('PORT', 5000))
WEBHOOK_PATH = f'/webhook/{API_TOKEN}'
WEBHOOK_URL = f'{RENDER_URL}{WEBHOOK_PATH}'
HEALTHCHECK_PATH = '/health'

# --- General Constants ---
MAX_AUTO_DELETE_MINUTES = 10080  # 7 days in minutes
DEFAULT_WELCOME_TEXT = "👋 Welcome! I am your reliable file upload bot. Use the commands below to navigate."
DEFAULT_HELP_TEXT = "📚 Help: Use /upload to start a new session. Share the deep link to give others access! Owner commands are restricted."

# ----------------------------------------------------------------------------------
# SECTION 2: POSTGRES MANAGER (NEON DATABASE IMPLEMENTATION)
#
# This class uses the 'asyncpg' library to manage connections and execute
# all database queries, making it production-ready for Neon/PostgreSQL.
# ----------------------------------------------------------------------------------

class PostgresManager:
    """
    Manages all asynchronous database operations using asyncpg for Neon PostgreSQL.
    """
    def __init__(self, db_url: str):
        """Initializes the manager, but defers connection until setup is called."""
        self.db_url = db_url
        self.pool = None
        logger.info(f"PostgresManager initialized for URL: {db_url[:20]}...")

    async def setup(self):
        """Creates the connection pool and initializes tables."""
        try:
            self.pool = await asyncpg.create_pool(self.db_url)
            logger.info("Database connection pool established successfully.")
            await self._create_tables()
        except Exception as e:
            logger.error(f"FATAL: Failed to establish DB connection or create tables: {e}")
            raise

    # --- DDL (Data Definition Language) ---
    async def _create_tables(self):
        """
        Executes the necessary SQL commands to set up the database structure.
        Uses explicit SQL commands required for PostgreSQL.
        """
        logger.info("Executing DDL to ensure all tables exist...")
        
        async with self.pool.acquire() as conn:
            # 1. Users Table
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id BIGINT PRIMARY KEY,
                join_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """)

            # 2. Messages Table (for /start and /help content)
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                type TEXT PRIMARY KEY, -- 'start' or 'help'
                text TEXT,
                image_file_id TEXT
            );
            """)
            
            # 3. Upload Sessions Table
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS upload_sessions (
                session_id TEXT PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                file_data JSONB, -- Stores array of {channel_message_id, caption} objects
                is_protected BOOLEAN DEFAULT FALSE,
                auto_delete_minutes INTEGER DEFAULT 0,
                creation_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """)
            
            # 4. Statistics Table
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS statistics (
                key TEXT PRIMARY KEY, -- e.g., 'files_uploaded', 'sessions_completed'
                value BIGINT DEFAULT 0
            );
            """)

            # 5. Insert defaults if needed (check for 'start' message existence)
            await conn.execute("""
            INSERT INTO messages (type, text, image_file_id) VALUES 
                ('start', $1, NULL),
                ('help', $2, NULL)
            ON CONFLICT (type) DO NOTHING;
            """, DEFAULT_WELCOME_TEXT, DEFAULT_HELP_TEXT)
            
        logger.info("DDL and default message setup complete.")


    # ------------------------------------
    # USER MANAGEMENT METHODS
    # ------------------------------------
    async def get_or_create_user(self, user_id: int):
        """Inserts user if new, updates last_active for all users."""
        now = datetime.datetime.now(datetime.timezone.utc)
        await self.pool.execute("""
        INSERT INTO users (id, join_date, last_active) 
        VALUES ($1, $2, $2)
        ON CONFLICT (id) 
        DO UPDATE SET last_active = $2;
        """, user_id, now)

    async def get_all_user_ids(self) -> list[int]:
        """Fetches all user IDs for broadcasting."""
        records = await self.pool.fetch("SELECT id FROM users;")
        return [r['id'] for r in records]

    async def get_total_users_count(self) -> int:
        """Returns the total number of users."""
        result = await self.pool.fetchval("SELECT COUNT(*) FROM users;")
        return result or 0

    async def get_active_users_count(self) -> int:
        """Counts users active in the last 48 hours."""
        time_limit = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=48)
        result = await self.pool.fetchval("SELECT COUNT(*) FROM users WHERE last_active > $1;", time_limit)
        return result or 0


    # ------------------------------------
    # MESSAGE & IMAGE MANAGEMENT METHODS
    # ------------------------------------
    async def get_message_content(self, message_type: str) -> dict:
        """Fetches the text and image_file_id for start or help messages."""
        record = await self.pool.fetchrow("SELECT text, image_file_id FROM messages WHERE type = $1;", message_type)
        if record:
            return {'text': record['text'], 'image_file_id': record['image_file_id']}
        return {}

    async def set_message_text(self, message_type: str, text: str):
        """Updates the text content of a message type."""
        await self.pool.execute("""
        INSERT INTO messages (type, text) 
        VALUES ($1, $2) 
        ON CONFLICT (type) 
        DO UPDATE SET text = $2;
        """, message_type, text)
        logger.info(f"Updated {message_type} message text in DB.")

    async def set_message_image(self, message_type: str, file_id: str):
        """Updates the image file ID for a message type."""
        await self.pool.execute("""
        INSERT INTO messages (type, image_file_id) 
        VALUES ($1, $2) 
        ON CONFLICT (type) 
        DO UPDATE SET image_file_id = $2;
        """, message_type, file_id)
        logger.info(f"Updated {message_type} message image in DB.")


    # ------------------------------------
    # UPLOAD SESSION METHODS
    # ------------------------------------
    async def create_upload_session(self, owner_id: int, files: list, is_protected: bool, auto_delete_minutes: int) -> str:
        """Creates a new upload session and returns the unique session ID."""
        session_id = str(uuid.uuid4())
        # asyncpg handles JSON/JSONB serialization automatically for dictionaries/lists
        
        await self.pool.execute("""
        INSERT INTO upload_sessions 
            (session_id, owner_id, file_data, is_protected, auto_delete_minutes)
        VALUES 
            ($1, $2, $3, $4, $5);
        """, session_id, owner_id, files, is_protected, auto_delete_minutes)

        logger.info(f"Created new upload session: {session_id[:8]}")
        
        # Update statistics
        await self.increment_stat('sessions_completed', 1)
        await self.increment_stat('files_uploaded', len(files))

        return session_id

    async def get_upload_session(self, session_id: str) -> dict or None:
        """Retrieves a specific upload session by its ID."""
        record = await self.pool.fetchrow("""
        SELECT owner_id, file_data, is_protected, auto_delete_minutes, creation_time 
        FROM upload_sessions 
        WHERE session_id = $1;
        """, session_id)
        
        if record:
            return {
                'owner_id': record['owner_id'],
                'file_data': record['file_data'], # This will be the parsed Python list/dict from JSONB
                'is_protected': record['is_protected'],
                'auto_delete_minutes': record['auto_delete_minutes'],
                'creation_time': record['creation_time']
            }
        return None

    # ------------------------------------
    # STATISTICS METHODS
    # ------------------------------------
    async def increment_stat(self, key: str, amount: int = 1):
        """Increments a single statistic counter."""
        await self.pool.execute("""
        INSERT INTO statistics (key, value) 
        VALUES ($1, $2)
        ON CONFLICT (key) 
        DO UPDATE SET value = statistics.value + $2;
        """, key, amount)

    async def get_stat_value(self, key: str) -> int:
        """Retrieves the value of a specific statistic."""
        # Use COALESCE to return 0 if the key does not exist yet
        result = await self.pool.fetchval("""
        SELECT COALESCE(value, 0) 
        FROM statistics 
        WHERE key = $1;
        """, key)
        return result or 0

# Initialize DB Manager globally with the Neon URL
DB = PostgresManager(db_url=DATABASE_URL)

# ----------------------------------------------------------------------------------
# SECTION 3: FINITE STATE MACHINE (FSM)
# ----------------------------------------------------------------------------------

class UploadStates(StatesGroup):
    """States for the multi-step /upload wizard."""
    waiting_for_files = State()
    waiting_for_options = State() # Protect content & Auto-delete timer

class MessageSetterStates(StatesGroup):
    """States for /setimage and /setmessage commands."""
    waiting_for_message_type = State()
    waiting_for_image_file = State()
    waiting_for_text_content = State()

class BroadcastStates(StatesGroup):
    """State for the /broadcast command."""
    waiting_for_content = State()

# ----------------------------------------------------------------------------------
# SECTION 4: UTILITY FUNCTIONS AND HELPERS
# ----------------------------------------------------------------------------------

def is_owner(user_id: int) -> bool:
    """Check if the user ID matches the OWNER_ID."""
    return user_id == OWNER_ID

def get_session_id_from_link_payload(payload: str) -> str or None:
    """Extracts the session ID from the deep link payload."""
    if len(payload) == 36 and all(c.isalnum() or c == '-' for c in payload):
        try:
            uuid.UUID(payload)
            return payload
        except ValueError:
            return None
    return None

async def shutdown(dispatcher: Dispatcher):
    """Graceful shutdown sequence."""
    logger.warning('Closing DB pool and shutting down bot...')
    if DB.pool:
        await DB.pool.close()
    await dispatcher.storage.close()
    await dispatcher.storage.wait_closed()
    logger.warning('Bot finished shutting down.')

# ----------------------------------------------------------------------------------
# SECTION 5: INITIALIZATION AND WEBHOOK SETUP
# ----------------------------------------------------------------------------------

bot = Bot(token=API_TOKEN, parse_mode=types.ParseMode.HTML)
# Use MemoryStorage, or Redis/Mongo if high load is expected (for aiogram v2)
storage = MemoryStorage() 
dp = Dispatcher(bot, storage=storage)

async def on_startup(dispatcher: Dispatcher):
    """Actions to perform when the bot starts up."""
    logger.info("Starting up bot, initializing DB, and setting webhook...")

    # 1. Initialize DB connection pool and tables
    try:
        await DB.setup()
        logger.info("Database initialization successful and tables ready.")
    except Exception as e:
        logger.critical(f"Bot failed to start due to DB error: {e}")
        # In a production environment, you might want to exit here
        pass 

    # 2. Set Webhook
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info(f"Webhook successfully set to: {WEBHOOK_URL}")
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}")

    # 3. Log Owner
    logger.info(f"Bot Owner ID: {OWNER_ID}")

# ----------------------------------------------------------------------------------
# SECTION 6: USER HANDLERS (/start, /help, DEEP LINK ACCESS)
# ----------------------------------------------------------------------------------

@dp.message_handler(commands=['start'])
async def handle_start(message: types.Message):
    """
    Handles /start command, registers user, and checks for deep link payload.
    """
    try:
        await DB.get_or_create_user(message.from_user.id)
        payload = message.get_args()
        session_id = get_session_id_from_link_payload(payload)

        if session_id:
            # User clicked a deep link - proceed to file retrieval
            await handle_deep_link_access(message, session_id)
            return

        # Standard /start message
        content = await DB.get_message_content('start')
        text = content.get('text', DEFAULT_WELCOME_TEXT)
        image_id = content.get('image_file_id')

        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(types.InlineKeyboardButton("Help", callback_data='show_help'))

        if image_id:
            await message.answer_photo(
                image_id, 
                caption=text, 
                reply_markup=markup,
                protect_content=False
            )
        else:
            await message.answer(text, reply_markup=markup)
            
    except Exception as e:
        logger.error(f"Error in /start handler for {message.from_user.id}: {e}")
        await message.answer("An internal error occurred. Please try again later.")

@dp.callback_query_handler(lambda c: c.data == 'show_help')
@dp.message_handler(commands=['help'])
async def handle_help(query_or_message: types.Union[types.CallbackQuery, types.Message]):
    """
    Handles /help command or callback from /start.
    """
    if isinstance(query_or_message, types.CallbackQuery):
        message = query_or_message.message
        await query_or_message.answer()
    else:
        message = query_or_message

    try:
        content = await DB.get_message_content('help')
        text = content.get('text', DEFAULT_HELP_TEXT)
        image_id = content.get('image_file_id')

        if image_id:
            await message.answer_photo(
                image_id, 
                caption=text,
                protect_content=False
            )
        else:
            await message.answer(text)
    except Exception as e:
        logger.error(f"Error in /help handler for {message.chat.id}: {e}")
        await message.answer(f"An internal error occurred. Default help: {DEFAULT_HELP_TEXT}")

async def handle_deep_link_access(message: types.Message, session_id: str):
    """
    Handles file retrieval based on the deep link session ID.
    """
    user_id = message.from_user.id
    is_owner_access = is_owner(user_id)

    try:
        session = await DB.get_upload_session(session_id)
    except Exception as e:
        logger.error(f"DB Error fetching session {session_id[:8]} for user {user_id}: {e}")
        await message.answer("❌ **Database Error:** Could not retrieve file session. The service might be temporarily down.")
        return

    if not session:
        await message.answer("❌ **Error:** That file sharing session ID is invalid or has expired.")
        return

    # Extract session data
    file_data = session['file_data']
    is_protected = session['is_protected']
    auto_delete_minutes = session['auto_delete_minutes']
    owner_id = session['owner_id']

    # OWNER BYPASS LOGIC: Owner gets full access, no protection, no auto-delete
    if is_owner_access:
        is_protected = False
        auto_delete_minutes = 0

    # Determine protection level
    protect_content = is_protected and not is_owner_access

    # 1. Inform the user
    intro_message = f"✅ **Files Retrieved!**\n\n"
    intro_message += f"Source User ID: `{owner_id}` (Owner bypass: {'ON' if is_owner_access else 'OFF'}).\n"
    
    if protect_content:
        intro_message += "⚠️ **Content Protected:** Forwarding, saving, and screenshots are restricted.\n"
    
    delete_delay_s = auto_delete_minutes * 60
    if auto_delete_minutes > 0:
        time_to_delete = str(datetime.timedelta(minutes=auto_delete_minutes)).split('.')[0]
        intro_message += f"⏳ **Auto-Delete Timer Set:** These messages will self-destruct in your chat after **{time_to_delete}**.\n"
    else:
        intro_message += "💾 **Files are permanent** in this chat.\n"

    try:
        info_msg = await message.answer(intro_message)
    except Exception as e:
        logger.error(f"Failed to send intro message to {user_id}: {e}")
        return

    # 2. Send the files
    sent_messages = [info_msg]
    for file_info in file_data:
        # file_id here is actually the channel_message_id
        channel_message_id = file_info.get('file_id') 
        caption = file_info.get('caption', '')
        
        try:
            # Use copy_message to send the file from the UPLOAD_CHANNEL_ID (source) to the user
            msg = await bot.copy_message(
                chat_id=user_id,
                from_chat_id=UPLOAD_CHANNEL_ID, 
                message_id=channel_message_id, # Channel message ID is required for copy
                caption=caption,
                protect_content=protect_content
            )
            
            sent_messages.append(msg)
            await asyncio.sleep(0.5) 

        except Exception as e:
            logger.error(f"Failed to copy channel message {channel_message_id} to {user_id}: {e}")
            await message.answer(f"❌ **Error:** Could not send file from channel. It might be due to bot permissions or a deleted source message.")

    # 3. Schedule auto-deletion if required
    if auto_delete_minutes > 0:
        bot.loop.create_task(
            schedule_delete_messages(
                user_id, 
                [m.message_id for m in sent_messages], 
                delete_delay_s
            )
        )
        logger.info(f"Scheduled deletion for {len(sent_messages)} messages for user {user_id} in {auto_delete_minutes} minutes.")

async def schedule_delete_messages(chat_id: int, message_ids: list[int], delay_seconds: int):
    """
    Schedules the deletion of a list of messages after a delay.
    """
    await asyncio.sleep(delay_seconds)
    logger.info(f"Executing scheduled deletion for {chat_id} (messages: {message_ids[:3]}...)")
    try:
        for msg_id in message_ids:
            # Note: Deleting one by one is necessary as Telegram API doesn't support bulk delete
            await bot.delete_message(chat_id, msg_id)
            await asyncio.sleep(0.1) 
        logger.info(f"Successfully deleted {len(message_ids)} messages for {chat_id}.")
    except Exception as e:
        logger.error(f"Failed to delete message(s) for {chat_id}: {e}")

# ----------------------------------------------------------------------------------
# SECTION 7: OWNER HANDLERS (ADMIN COMMANDS)
# ----------------------------------------------------------------------------------

# --- Decorator for Owner Access ---
def is_owner_filter(message: types.Message):
    """A filter function to restrict access to owner only."""
    return is_owner(message.from_user.id)

@dp.message_handler(commands=['stats'], is_owner_filter=is_owner_filter)
async def handle_stats(message: types.Message):
    """Displays real-time usage statistics."""
    try:
        total_users = await DB.get_total_users_count()
        active_users = await DB.get_active_users_count()
        files_uploaded = await DB.get_stat_value('files_uploaded')
        sessions_completed = await DB.get_stat_value('sessions_completed')

        stats_text = (
            "📊 **Bot Usage Statistics**\n\n"
            f"👤 **Total Users:** `{total_users:,}`\n"
            f"🟢 **Active Users (48h):** `{active_users:,}`\n"
            f"💾 **Files Uploaded:** `{files_uploaded:,}`\n"
            f"📦 **Upload Sessions Completed:** `{sessions_completed:,}`\n"
            "\n_All metrics sourced from Neon DB (PostgreSQL)._"
        )
        await message.answer(stats_text)
    except Exception as e:
        logger.error(f"Error fetching stats from DB: {e}")
        await message.answer("❌ **Error:** Failed to retrieve statistics from the database.")


# --- Broadcast Command ---

@dp.message_handler(commands=['broadcast'], is_owner_filter=is_owner_filter)
async def cmd_broadcast(message: types.Message):
    """Initiates the broadcast flow."""
    await BroadcastStates.waiting_for_content.set()
    await message.answer("📝 **Broadcast Mode:** Please send the message or media you want to broadcast to **ALL** users. Inline buttons are supported.\n\nType /c to cancel.")

@dp.message_handler(commands=['c'], state=BroadcastStates.waiting_for_content, is_owner_filter=is_owner_filter)
async def cancel_broadcast(message: types.Message, state: FSMContext):
    """Cancels the broadcast flow."""
    await state.finish()
    await message.answer("❌ Broadcast cancelled.")

@dp.message_handler(content_types=types.ContentType.ANY, state=BroadcastStates.waiting_for_content, is_owner_filter=is_owner_filter)
async def execute_broadcast(message: types.Message, state: FSMContext):
    """Sends the content to all users."""
    await state.finish()
    
    try:
        user_ids = await DB.get_all_user_ids()
    except Exception as e:
        logger.error(f"DB Error fetching user IDs for broadcast: {e}")
        await message.answer("❌ **Error:** Failed to retrieve user list from the database.")
        return
    
    total_users = len(user_ids)
    sent_count = 0
    fail_count = 0
    
    progress_msg = await message.answer(f"🚀 Starting broadcast to **{total_users}** users...")

    for user_id in user_ids:
        if user_id == message.from_user.id:
            continue
            
        try:
            # Use copy_message to preserve media type and inline buttons, and remove forward tag
            await bot.copy_message(
                chat_id=user_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id
            )
            sent_count += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.debug(f"Broadcast failed to {user_id}: {e}")
            fail_count += 1

    final_text = (
        "✅ **Broadcast Complete!**\n\n"
        f"👥 Total Users: `{total_users}`\n"
        f"🟢 Sent Successfully: `{sent_count}`\n"
        f"🔴 Failed/Blocked: `{fail_count}`"
    )
    
    try:
        await bot.edit_message_text(final_text, progress_msg.chat.id, progress_msg.message_id)
    except Exception as e:
        # Fallback if the original message cannot be edited (e.g., too old)
        await message.answer(final_text)


# --- Set Message/Image Commands (Logic unchanged, now calls PostgresManager) ---

@dp.message_handler(commands=['setmessage'], is_owner_filter=is_owner_filter)
async def cmd_setmessage_start(message: types.Message):
    """Starts the flow to set a start or help message text."""
    await MessageSetterStates.waiting_for_message_type.set()
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("Start Message", callback_data='setmsg_start'),
        types.InlineKeyboardButton("Help Message", callback_data='setmsg_help')
    )
    await message.answer("📝 **Set Message:** Which message would you like to edit?", reply_markup=markup)

@dp.callback_query_handler(lambda c: c.data.startswith('setmsg_'), state=MessageSetterStates.waiting_for_message_type, is_owner_filter=is_owner_filter)
async def setmessage_select_type(call: types.CallbackQuery, state: FSMContext):
    """Captures the message type and asks for the new content."""
    msg_type = call.data.split('_')[1]
    await call.message.edit_text(f"Please send the **new text** for the **/{msg_type}** command.")
    
    await state.update_data(target_type=msg_type)
    await MessageSetterStates.waiting_for_text_content.set()
    await call.answer()

@dp.message_handler(content_types=types.ContentType.TEXT, state=MessageSetterStates.waiting_for_text_content, is_owner_filter=is_owner_filter)
async def setmessage_save_text(message: types.Message, state: FSMContext):
    """Saves the new message text to the database."""
    data = await state.get_data()
    msg_type = data.get('target_type')
    
    if not msg_type:
        await message.answer("Error: Message type not found. Please restart with /setmessage.")
        await state.finish()
        return

    try:
        await DB.set_message_text(msg_type, message.text)
        await message.answer(f"✅ **Success!** The text for the **/{msg_type}** command has been updated.")
    except Exception as e:
        logger.error(f"DB Error saving text: {e}")
        await message.answer("❌ **Error:** Failed to save the message text to the database.")

    await state.finish()

@dp.message_handler(commands=['setimage'], is_owner_filter=is_owner_filter)
async def cmd_setimage_start(message: types.Message):
    """Starts the flow to set a start or help message image."""
    await MessageSetterStates.waiting_for_image_file.set()
    await message.answer("🖼️ **Set Image:** Please reply to the image you want to use with this command.")

@dp.message_handler(content_types=types.ContentType.PHOTO | types.ContentType.DOCUMENT, state=MessageSetterStates.waiting_for_image_file, is_owner_filter=is_owner_filter)
async def setimage_select_file(message: types.Message, state: FSMContext):
    """Captures the image file_id and asks where to apply it."""
    
    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.document and message.document.mime_type and message.document.mime_type.startswith('image/'):
        file_id = message.document.file_id
    else:
        await message.answer("❌ **Invalid:** Please reply to a **photo** or a **document** (that is an image file).")
        return

    await state.update_data(image_file_id=file_id)
    
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("Start Image", callback_data='setimg_start'),
        types.InlineKeyboardButton("Help Image", callback_data='setimg_help')
    )
    await message.answer("❓ **Where should this image be used?**", reply_markup=markup)
    await MessageSetterStates.waiting_for_message_type.set()

@dp.callback_query_handler(lambda c: c.data.startswith('setimg_'), state=MessageSetterStates.waiting_for_message_type, is_owner_filter=is_owner_filter)
async def setimage_save_image(call: types.CallbackQuery, state: FSMContext):
    """Saves the image file_id for the selected message type."""
    data = await state.get_data()
    image_file_id = data.get('image_file_id')
    msg_type = call.data.split('_')[1]
    
    if not image_file_id:
        await call.message.edit_text("Error: Image file ID not found. Please restart with /setimage.")
        await state.finish()
        await call.answer()
        return

    try:
        await DB.set_message_image(msg_type, image_file_id)
        await call.message.edit_text(f"✅ **Success!** The image for the **/{msg_type}** command has been updated.")
    except Exception as e:
        logger.error(f"DB Error saving image ID: {e}")
        await call.message.edit_text("❌ **Error:** Failed to save the image to the database.")

    await state.finish()
    await call.answer()


# ----------------------------------------------------------------------------------
# SECTION 8: UPLOAD WIZARD (/upload, /d, /c)
# ----------------------------------------------------------------------------------

@dp.message_handler(commands=['upload'], is_owner_filter=is_owner_filter)
async def cmd_upload_start(message: types.Message, state: FSMContext):
    """Initiates the file upload flow."""
    await state.update_data(uploaded_files=[])
    await UploadStates.waiting_for_files.set()
    await message.answer(
        "📂 **Upload Session Started!**\n\n"
        "Please send all the files (photos, videos, documents) you wish to include. Captions will be saved.\n\n"
        "When finished:\n"
        "Type /d (Done) to proceed to options.\n"
        "Type /c (Cancel) to abort the session."
    )

@dp.message_handler(commands=['d'], state=UploadStates.waiting_for_files, is_owner_filter=is_owner_filter)
async def cmd_upload_done(message: types.Message, state: FSMContext):
    """User is done sending files, proceed to options."""
    data = await state.get_data()
    files = data.get('uploaded_files', [])
    
    if not files:
        await message.answer("❌ You must upload at least one file before finishing. Send your files now, or type /c to cancel.")
        return
        
    await UploadStates.waiting_for_options.set()
    
    markup_protect = types.InlineKeyboardMarkup(row_width=2)
    markup_protect.add(
        types.InlineKeyboardButton("🔒 Yes, Protect Content", callback_data='protect_yes'),
        types.InlineKeyboardButton("🔓 No, Allow Forwarding", callback_data='protect_no')
    )
    
    await message.answer(
        f"✅ **{len(files)} file(s) received!**\n\n"
        "**Step 1/2:** Do you want to **Protect Content** (disable forwarding/saving) for non-owner users?", 
        reply_markup=markup_protect
    )

@dp.message_handler(commands=['c'], state='*', is_owner_filter=is_owner_filter)
async def cmd_upload_cancel(message: types.Message, state: FSMContext):
    """Cancels any active state."""
    await state.finish()
    await message.answer("❌ Upload session cancelled and all temporary data cleared.")

@dp.message_handler(content_types=types.ContentType.ANY, state=UploadStates.waiting_for_files, is_owner_filter=is_owner_filter)
async def receive_files(message: types.Message, state: FSMContext):
    """Collects file_ids and captions from the owner, forwarding to the channel."""
    
    # We don't need the file_id here, only the message_id after forwarding.
    if not (message.photo or message.video or message.document or message.audio or message.animation):
        if not message.text.startswith('/'):
            await message.answer("⚠️ Only media files (photo, video, document, audio, animation) are saved.")
        return
    
    # Step 1: Forward the message to the UPLOAD_CHANNEL_ID
    try:
        forwarded_msg = await bot.forward_message(
            chat_id=UPLOAD_CHANNEL_ID,
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )
        # The channel message ID is crucial for bot.copy_message later
        channel_message_id = forwarded_msg.message_id
        
        # Step 2: Save the channel message ID and original caption
        file_info = {
            'file_id': channel_message_id, # Stored in DB as the reference ID
            'caption': message.caption or ''
        }
        
        async with state.proxy() as data:
            data['uploaded_files'].append(file_info)
        
        await message.reply(f"✅ File saved. Files collected so far: {len(data['uploaded_files'])}")
        
    except Exception as e:
        logger.error(f"Failed to forward file to channel {UPLOAD_CHANNEL_ID}: {e}")
        await message.answer("❌ **Error:** Failed to save file. Check the `UPLOAD_CHANNEL_ID` and ensure the bot is an administrator with posting rights.")
            

# --- Upload Options (Step 5) ---

@dp.callback_query_handler(lambda c: c.data.startswith('protect_'), state=UploadStates.waiting_for_options, is_owner_filter=is_owner_filter)
async def upload_set_protection(call: types.CallbackQuery, state: FSMContext):
    """Captures the protection option and proceeds to the auto-delete timer."""
    is_protected = call.data == 'protect_yes'
    
    await state.update_data(is_protected=is_protected)
    
    # Ask for auto-delete timer
    markup_timer = types.InlineKeyboardMarkup(row_width=3)
    markup_timer.add(
        types.InlineKeyboardButton("Keep Forever (0 min)", callback_data='timer_0'),
        types.InlineKeyboardButton("1 Hour (60 min)", callback_data='timer_60'),
        types.InlineKeyboardButton("1 Day (1440 min)", callback_data='timer_1440'),
        types.InlineKeyboardButton("7 Days (10080 min)", callback_data='timer_10080')
    )
    
    await call.message.edit_text(
        f"🔒 Protection: **{'ON' if is_protected else 'OFF'}**\n\n"
        "**Step 2/2:** Set **Auto-Delete Timer** (in minutes). This timer cleans up the **user's chat**.\n\n"
        f"Select a preset or send a custom number (0 to {MAX_AUTO_DELETE_MINUTES} minutes).",
        reply_markup=markup_timer
    )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('timer_'), state=UploadStates.waiting_for_options, is_owner_filter=is_owner_filter)
async def upload_set_timer_preset(call: types.CallbackQuery, state: FSMContext):
    """Captures a preset auto-delete timer and finalizes the session."""
    timer_minutes = int(call.data.split('_')[1])
    # Edit the message to show the final result and the deep link
    await call.message.delete()
    await finalize_upload_session(call.message, state, timer_minutes)
    await call.answer()

@dp.message_handler(content_types=types.ContentType.TEXT, state=UploadStates.waiting_for_options, is_owner_filter=is_owner_filter)
async def upload_set_timer_custom(message: types.Message, state: FSMContext):
    """Captures a custom auto-delete timer and finalizes the session."""
    try:
        timer_minutes = int(message.text.strip())
        if not 0 <= timer_minutes <= MAX_AUTO_DELETE_MINUTES:
            raise ValueError("Time out of range")
            
        await finalize_upload_session(message, state, timer_minutes)
        
    except ValueError:
        await message.answer(f"❌ Invalid value. Please send a whole number between 0 and {MAX_AUTO_DELETE_MINUTES} minutes.")
    except Exception as e:
        logger.error(f"Error finalizing custom timer upload: {e}")
        await message.answer("❌ An unexpected error occurred while finalizing the session.")

async def finalize_upload_session(message: types.Message, state: FSMContext, timer_minutes: int):
    """Step 6: Saves session to DB, generates deep link, and sends final confirmation."""
    data = await state.get_data()
    files = data.get('uploaded_files')
    is_protected = data.get('is_protected', False)
    
    if not files:
        await message.answer("Fatal Error: Files list is empty. Session aborted.")
        await state.finish()
        return

    try:
        # Create the session in the database
        session_id = await DB.create_upload_session(
            owner_id=message.from_user.id,
            files=files,
            is_protected=is_protected,
            auto_delete_minutes=timer_minutes
        )
    except Exception as e:
        logger.error(f"DB Error creating session for {message.from_user.id}: {e}")
        await message.answer("❌ **Database Error:** Failed to save session data. Please check your Neon connection.")
        await state.finish()
        return
    
    # Generate deep link
    deep_link = await get_start_link(session_id, encode=False)

    # Format auto-delete display
    if timer_minutes == 0:
        delete_display = "No (Files stay in user chat forever)"
    else:
        time_delta = str(datetime.timedelta(minutes=timer_minutes)).split('.')[0]
        delete_display = f"Yes (Files delete after **{time_delta}**)"
        
    # Send final confirmation
    final_message = (
        "🎉 **Session Finalized!**\n\n"
        f"🆔 Session ID: `{session_id}`\n"
        f"💾 Files Count: `{len(files)}`\n"
        f"🔒 Protection: **{'ON' if is_protected else 'OFF'}**\n"
        f"⏳ Auto-Delete: {delete_display}\n\n"
        
        "🔗 **DEEP LINK FOR SHARING**\n"
        f"```\n{deep_link}\n```\n\n"
        
        "👆 Share this link with anyone to grant access to the uploaded files."
    )
    
    await message.answer(final_message)
    await state.finish()
    logger.info(f"Session {session_id[:8]} finalized and deep link generated.")

# ----------------------------------------------------------------------------------
# SECTION 9: WEBHOOK AND HEALTHCHECK APPLICATION SETUP
# ----------------------------------------------------------------------------------

async def health_handler(request):
    """
    Handles the /health endpoint for UptimeRobot keep-alive pings.
    """
    return aiohttp.web.Response(text="ok", status=200)

async def start_web_app():
    """
    Sets up the aiohttp web application for webhook and healthcheck.
    """
    app = dp.web_app
    
    # Add the healthcheck route
    app.router.add_get(HEALTHCHECK_PATH, health_handler)
    
    # Start the webhook handler
    start_webhook(
        dispatcher=dp,
        webhook_path=WEBHOOK_PATH,
        on_startup=on_startup,
        on_shutdown=shutdown,
        skip_updates=True,
        host=WEBAPP_HOST,
        port=WEBAPP_PORT,
    )

# ----------------------------------------------------------------------------------
# SECTION 10: EXECUTION
# ----------------------------------------------------------------------------------

if __name__ == '__main__':
    # Log starting application information
    logger.info("-" * 50)
    logger.info(f"Starting bot in WEBHOOK mode.")
    logger.info(f"Webhook URL: {WEBHOOK_URL}")
    logger.info("-" * 50)
    
    # Run the aiohttp web server to listen for webhooks
    try:
        asyncio.run(start_web_app())
    except KeyboardInterrupt:
        logger.warning("Bot stopped by KeyboardInterrupt.")
    except Exception as e:
        logger.critical(f"Unhandled critical error during startup: {e}")
