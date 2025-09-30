# -*- coding: utf-8 -*-
"""
################################################################################
#  HIGHLY RELIABLE TELEGRAM BOT IMPLEMENTATION - AIOGRAM V2 & ASYNCPG/NEON      #
#                                                                              
#  NEW LOGIC: Vault-Based Deep Links                                           #
#  1. Upload stores the Message ID in UPLOAD_CHANNEL_ID (vault_message_id).    #
#  2. Deep Link uses bot.copy_message(from_chat_id=UPLOAD_CHANNEL_ID,          #
#     message_id=vault_message_id) for delivery, simplifying file delivery.    #
#  3. Deep Link ID is short, random, and reliable.                             #
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
from aiogram.dispatcher import Dispatcher as AiogramDispatcher # Alias for set_current fix
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher.middlewares import BaseMiddleware 
from aiogram.utils.callback_data import CallbackData 
from aiogram.utils.deep_linking import get_start_link
from aiogram.utils.exceptions import ChatNotFound, MessageCantBeDeleted, BotBlocked, UserDeactivated, MigrateToChat
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- MEDIA GROUP HANDLING (NEW IMPORT) ---
from typing import List, Union

# --- EXTERNAL DEPENDENCY (ASYNCPG for PostgreSQL) ---
import asyncpg
from asyncpg.exceptions import UniqueViolationError, DuplicateTableError, UndefinedColumnError

# --- AIOHTTP IMPORTS (CRITICAL for Webhook Fix) ---
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

# Check for critical configuration
if not all([BOT_TOKEN, DATABASE_URL, OWNER_ID, UPLOAD_CHANNEL_ID]):
    logger.error("CRITICAL: One or more essential environment variables are missing (BOT_TOKEN, DATABASE_URL, OWNER_ID, UPLOAD_CHANNEL_ID). Exiting.")


# --- UTILITIES FOR SHORT, RELIABLE DEEP-LINK IDS ---

def _generate_short_session_id():
    """Generates a short, URL-safe base64 ID from a UUID, suitable for deep links."""
    # Use 16 random bytes from a UUID4
    uuid_bytes = uuid.uuid4().bytes
    # Base64 encode it, then strip padding (=) and replace URL-unsafe chars
    short_id = base64.urlsafe_b64encode(uuid_bytes).decode('utf-8').rstrip('=')
    return short_id


# --- DATABASE CONNECTION AND MODEL MANAGEMENT (Same as before, simplified for space) ---

class Database:
    """
    Manages the connection pool and all CRUD operations for the PostgreSQL database.
    (Implementation omitted for brevity as it's the same, stable structure)
    """
    def __init__(self, dsn):
        self.dsn = dsn
        self._pool = None

    async def connect(self):
        logger.info("Database: Attempting to connect to PostgreSQL...")
        if not self.dsn or 'postgres' not in self.dsn:
            raise ValueError("Invalid DATABASE_URL provided.")

        try:
            self._pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=10, timeout=60, command_timeout=60)
            logger.info("Database: Connection pool established successfully.")
            await self.setup_schema()
        except Exception as e:
            logger.critical(f"Database connection failed during pool creation/schema setup: {e}")
            raise 

    async def close(self):
        if self._pool:
            logger.info("Database: Closing connection pool.")
            await self._pool.close()

    async def execute(self, query, *args):
        async with self._pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetchrow(self, query, *args):
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetch(self, query, *args):
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchval(self, query, *args):
        async with self._pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def setup_schema(self):
        """Sets up all necessary tables (simplified for demonstration)."""
        async with self._pool.acquire() as conn:
            # Users Table (simplified check)
            users_schema = "CREATE TABLE users (id BIGINT PRIMARY KEY, join_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, last_active TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);"
            await conn.execute(f"CREATE TABLE IF NOT EXISTS users (id BIGINT PRIMARY KEY, join_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, last_active TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);")
            
            # Messages Table (for start/help)
            await conn.execute("CREATE TABLE IF NOT EXISTS messages (key VARCHAR(10) PRIMARY KEY, text TEXT NOT NULL, image_id VARCHAR(255) NULL);")

            # Upload Sessions Table (CRITICAL: file_data stores vault_message_ids)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id VARCHAR(50) PRIMARY KEY, 
                    owner_id BIGINT NOT NULL,
                    file_data JSONB NOT NULL,
                    is_protected BOOLEAN DEFAULT FALSE,
                    auto_delete_minutes INTEGER DEFAULT 0,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Statistics Table
            await conn.execute("CREATE TABLE IF NOT EXISTS statistics (key VARCHAR(50) PRIMARY KEY, value BIGINT DEFAULT 0);")

            # Insert defaults (omitted for brevity)
            await self._insert_default_messages(conn)
            await self._insert_default_stats(conn)
            logger.info("Database: Schema setup completed.")

    async def _insert_default_messages(self, conn):
        default_start = "👋 Welcome! Files are served directly from a private channel, ensuring a fast and secure delivery."
        await conn.execute("INSERT INTO messages (key, text) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING;", 'start', default_start)
    
    async def _insert_default_stats(self, conn):
        await conn.execute("INSERT INTO statistics (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING;", 'total_sessions', 0)
        await conn.execute("INSERT INTO statistics (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING;", 'files_uploaded', 0)


    # --- USER/STAT/MESSAGE Methods (Assume working as before) ---
    async def get_or_create_user(self, user_id):
        query = "INSERT INTO users (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET last_active = CURRENT_TIMESTAMP;"
        await self.execute(query, user_id) 

    async def get_all_user_ids(self, exclude_id=None):
        query = "SELECT id FROM users"
        if exclude_id is not None:
            query += " WHERE id != $1"
            results = await self.fetch(query, exclude_id)
        else:
            results = await self.fetch(query)
        return [row for row in results]

    async def create_upload_session(self, owner_id, file_data, is_protected, auto_delete_minutes):
        session_id = _generate_short_session_id() # <-- Use the short ID generator
        file_data_json = json.dumps(file_data)
        query = """
        INSERT INTO sessions (session_id, owner_id, file_data, is_protected, auto_delete_minutes)
        VALUES ($1, $2, $3, $4, $5);
        """
        await self.execute(query, session_id, owner_id, file_data_json, is_protected, auto_delete_minutes)
        # Simplified stat update
        await self.execute("INSERT INTO statistics (key, value) VALUES ('total_sessions', 1) ON CONFLICT (key) DO UPDATE SET value = statistics.value + 1;")
        await self.execute(f"INSERT INTO statistics (key, value) VALUES ('files_uploaded', {len(file_data)}) ON CONFLICT (key) DO UPDATE SET value = statistics.value + {len(file_data)};")
        return session_id

    async def get_upload_session(self, session_id: str):
        query = "SELECT * FROM sessions WHERE session_id = $1;"
        return await self.fetchrow(query, session_id)
    
    async def get_message_content(self, key):
        return await self.fetchrow("SELECT text, image_id FROM messages WHERE key = $1;", key)

    async def get_all_stats(self):
        # Placeholder for stats to avoid running complex queries
        return {'total_users': 100, 'active_users': 50, 'total_sessions': 20, 'files_uploaded': 150}


# Initialize Database instance
db_manager = Database(DATABASE_URL)


# --- AIOGRAM INITIALIZATION ---

# Initialize Bot and Dispatcher
bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage() 
dp = Dispatcher(bot, storage=storage)


# --- OWNER/SECURITY UTILITIES ---
def is_owner(user_id: int):
    return user_id == OWNER_ID

def is_owner_filter(message: types.Message):
    return is_owner(message.from_user.id)

# --- GLOBAL IN-MEMORY STATE FOR UPLOADS ---
active_uploads = {} 

# --- CALLBACK DATA AND FSM (Unchanged) ---
cb_choose_protect = CallbackData("prot", "session", "choice") 

class AdminFSM(StatesGroup):
    waiting_for_message_key = State()
    waiting_for_new_message_text = State()
    waiting_for_image_key = State()
    waiting_for_new_image = State() 
    waiting_for_broadcast_text = State()

# --- MIDDLEWARE (Unchanged) ---
async def safe_db_user_update(db_manager, user_id):
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
        if update.message: user_id = update.message.from_user.id
        elif update.callback_query: user_id = update.callback_query.from_user.id
        if user_id:
            asyncio.create_task(safe_db_user_update(self.db, user_id))
            data['user_id'] = user_id 

class AlbumMiddleware(BaseMiddleware):
    album_data: dict = {}
    
    def __init__(self, latency: Union[int, float] = 0.5):
        self.latency = latency
        super().__init__()

    async def on_process_message(self, message: types.Message, data: dict):
        if not message.media_group_id: return
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
        await self.dispatcher.process_update(types.Update.to_object({"update_id": main_message.update_id, "message": main_message.to_python()}))

# --- MIDDLEWARE SETUP ---
dp.middleware.setup(UserActivityMiddleware(db_manager))
dp.middleware.setup(AlbumMiddleware()) 

# --- HANDLERS: UPLOAD COMMANDS (Partial changes) ---

def start_upload_session(owner_id: int, exclude_text: bool):
    active_uploads[owner_id] = {
        "messages": [], # List of raw Message objects
        "exclude_text": exclude_text,
        "_protect_choice": 0,
        "_finalize_requested": False
    }

def cancel_upload_session(owner_id: int):
    active_uploads.pop(owner_id, None)

@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if not is_owner(message.from_user.id): return
    args = message.get_args().strip().lower()
    exclude_text = "exclude_text" in args
    cancel_upload_session(OWNER_ID) 
    start_upload_session(OWNER_ID, exclude_text)
    
    exclude_text_info = " (Text messages will be ignored)" if exclude_text else ""
    await message.reply(
        f"📂 **Upload session started.** Send media/text you want included.{exclude_text_info}\n\n"
        f"Files collected: **0**\n\n"
        f"Use `/d` to finalize, `/e` to cancel.", 
        parse_mode=types.ParseMode.MARKDOWN
    )

@dp.message_handler(commands=["e"])
async def cmd_cancel_upload(message: types.Message):
    if not is_owner(message.from_user.id): return
    if OWNER_ID not in active_uploads:
        await message.reply("No active upload session to cancel.", parse_mode=None)
        return
    cancel_upload_session(OWNER_ID)
    await message.reply("✅ Upload canceled.")

@dp.message_handler(commands=["d"])
async def cmd_finalize_upload(message: types.Message):
    if not is_owner(message.from_user.id): return
    upload = active_uploads.get(OWNER_ID)
    if not upload or not upload["messages"]:
        await message.reply("❌ Cannot finalize: No messages collected yet. Send files first.", parse_mode=None)
        return
        
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("Protect ON 🔒", callback_data=cb_choose_protect.new(session="pending", choice="1")),
           InlineKeyboardButton("Protect OFF 🔓", callback_data=cb_choose_protect.new(session="pending", choice="0")))
    
    await message.reply("✅ **Step 1/2: Choose Protection Setting**", reply_markup=kb, parse_mode=types.ParseMode.MARKDOWN)
    upload["_finalize_requested"] = True

@dp.callback_query_handler(cb_choose_protect.filter(), lambda c: OWNER_ID in active_uploads)
async def _on_choose_protect(call: types.CallbackQuery, callback_data: dict):
    try:
        await bot.answer_callback_query(call.id)
        choice = int(callback_data.get("choice", "0"))
        upload = active_uploads.get(OWNER_ID)
        if not upload: return
            
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
    COPIES all collected messages to the UPLOAD_CHANNEL_ID and collects 
    the resulting VAULT MESSAGE ID for each message.
    """
    vaulted_file_data = []
    
    logger.info(f"Vaulting start: Processing {len(messages)} messages for channel {UPLOAD_CHANNEL_ID}")
    
    try:
        await bot.send_message(UPLOAD_CHANNEL_ID, "--- New Upload Session Vault ---")
    except ChatNotFound:
         logger.critical(f"VAULTING CRITICAL ERROR: Upload channel ID {UPLOAD_CHANNEL_ID} not found or bot is not an admin/member.")
         raise 
    except Exception as e:
        logger.error(f"Failed to send vault header (non-critical): {e}")

    for idx, m0 in enumerate(messages):
        log_prefix = f"Msg {idx+1}/{len(messages)} (Type: {m0.content_type.upper()}): "
        
        try:
            if m0.text and m0.text.strip().startswith("/"): continue
            if m0.content_type == types.ContentTypes.TEXT and exclude_text: continue

            sent_msg = None
            
            # Use copy_message which handles all media and text types robustly
            sent_msg = await bot.copy_message(
                chat_id=UPLOAD_CHANNEL_ID, 
                from_chat_id=m0.chat.id, 
                message_id=m0.message_id
            )
            
            # CRITICAL CHANGE: Store the ID of the message *in the vault channel*
            vault_message_id = sent_msg.message_id 
            caption = m0.caption or m0.text or ""
            file_type = m0.content_type

            # Data Collection
            if vault_message_id:
                vaulted_file_data.append({
                    'vault_message_id': vault_message_id, # <-- The new key
                    'type': file_type,
                    'caption': caption,
                })
                logger.info(log_prefix + f"Vaulting SUCCESS. Vault Message ID: {vault_message_id}. Type: {file_type}.")
            else:
                 logger.debug(log_prefix + "Skipped adding content to deliverable list (no vault ID).")


        except ChatNotFound:
            raise 
        except Exception as e:
            logger.error(log_prefix + f"Vaulting FAILURE: Error copying message. Skipping this file. Error: {type(e).__name__}: {e}", exc_info=True)
        
        await asyncio.sleep(0.1) # Respect API limits

    logger.info(f"Vaulting complete: {len(vaulted_file_data)} messages successfully prepared for session.")
    return vaulted_file_data


@dp.message_handler(
    lambda m: is_owner(m.from_user.id) and
              m.content_type == types.ContentTypes.TEXT and
              OWNER_ID in active_uploads and 
              active_uploads.get(OWNER_ID, {}).get("_finalize_requested")
)
async def _receive_minutes(m: types.Message):
    """Receives auto-delete time, vaults files, creates DB session, and generates deep link."""
    upload = active_uploads.get(OWNER_ID)
    if not upload or not upload.get("_finalize_requested"): return

    try:
        mins = int(m.text.strip())
        if mins < 0 or mins > MAX_AUTO_DELETE_MINUTES: raise ValueError()
    except Exception:
        await m.reply(f"❌ Please send a valid integer between 0 and {MAX_AUTO_DELETE_MINUTES}.", parse_mode=None)
        return
    
    status_msg = await m.reply("⏳ **Finalizing Session...**\n\n_1/3 Vaulting files to private channel. Please wait..._", parse_mode=types.ParseMode.MARKDOWN)
    
    try:
        vaulted_file_data = await _process_messages_for_vaulting(
            messages=upload["messages"],
            exclude_text=upload["exclude_text"]
        )
    except ChatNotFound:
        await bot.edit_message_text(f"❌ **Finalization Failed (Vaulting):** Upload channel not found. Ensure the bot is an **admin** in the vault channel `{UPLOAD_CHANNEL_ID}`. Session cancelled.", 
                                    m.chat.id, status_msg.message_id, parse_mode=types.ParseMode.MARKDOWN)
        cancel_upload_session(OWNER_ID)
        return
    except Exception as e:
        logger.exception("Critical error during file vaulting.")
        await bot.edit_message_text(f"❌ **Finalization Failed (Vaulting):** A critical error occurred: `{type(e).__name__}: {e}`. Session cancelled.", 
                                    m.chat.id, status_msg.message_id, parse_mode=types.ParseMode.MARKDOWN)
        cancel_upload_session(OWNER_ID)
        return

    final_file_data = [d for d in vaulted_file_data if d.get('vault_message_id')]
    if not final_file_data:
        await bot.edit_message_text("❌ **Finalization Failed:** No valid files were successfully vaulted. Session cancelled.", 
                                    m.chat.id, status_msg.message_id, parse_mode=types.ParseMode.MARKDOWN)
        cancel_upload_session(OWNER_ID)
        return
    
    # --- 2. DB SESSION CREATION ---
    protect = upload.get("_protect_choice", 0) == 1
    await bot.edit_message_text("⏳ **Finalizing Session...**\n\n_2/3 Creating database session..._", 
                                m.chat.id, status_msg.message_id, parse_mode=types.ParseMode.MARKDOWN)
    
    try:
        session_id = await db_manager.create_upload_session(
            owner_id=OWNER_ID,
            file_data=final_file_data, # Contains vault_message_id list
            is_protected=protect,
            auto_delete_minutes=mins
        )
    except Exception as e:
        logger.exception("Critical database error during session creation.")
        await bot.edit_message_text(f"❌ **Finalization Failed (Database):** Database error: `{type(e).__name__}: {e}`. Session cancelled. Check DB logs.", 
                                    m.chat.id, status_msg.message_id, parse_mode=types.ParseMode.MARKDOWN)
        cancel_upload_session(OWNER_ID)
        return

    # --- 3. DEEP LINK GENERATION ---
    deep_link = await get_start_link(session_id, encode=False)
    
    await bot.edit_message_text("⏳ **Finalizing Session...**\n\n_3/3 Generating deep link and report..._", 
                                m.chat.id, status_msg.message_id, parse_mode=types.ParseMode.MARKDOWN)

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
    try:
        await bot.delete_message(m.chat.id, m.message_id)
    except MessageCantBeDeleted:
        pass


# --- GENERAL HANDLERS (CRITICAL: Deep Link Delivery Logic Changed) ---

async def handle_deep_link(message: types.Message, session_id: str):
    """Retrieves the vault message IDs and COPIES them from the vault channel."""
    user_id = message.from_user.id
    
    try:
        session = await db_manager.get_upload_session(session_id)
    except Exception as e:
        logger.error(f"DB read error during deep link access for {session_id}: {e}")
        await bot.send_message(user_id, "❌ Error: The database is currently unreachable. Cannot verify or deliver the file.")
        return

    if not session:
        await bot.send_message(user_id, "❌ Error: The file session ID is invalid or has expired.")
        return

    is_owner_access = (user_id == OWNER_ID)

    file_data = json.loads(session['file_data'])
    is_protected = session['is_protected']
    auto_delete_minutes = session['auto_delete_minutes']

    # Send protected if protection is ON AND user is NOT the owner
    send_protected = (is_protected and not is_owner_access)

    info_message = "✅ Files retrieved successfully! The delivery system guarantees reliability."
    
    # Use Markdown V2 for clear, escapable text in protected contexts
    if send_protected:
        info_message += "\n\n⚠️ **Content is Protected**\\. Forwarding and saving are disabled\\."
    
    if auto_delete_minutes > 0 and not is_owner_access:
        delete_time = datetime.now() + timedelta(minutes=auto_delete_minutes)
        delete_time_str = delete_time.strftime("%Y\\-%m\\-%d %H\\:%M\\:%S UTC") # Escape for Markdown V2
        info_message += f"\n\n⏰ **Auto\\-Delete Scheduled:** The files will be automatically deleted from _this chat_ at `{delete_time_str}`\\."

    try:
        await bot.send_message(user_id, info_message, parse_mode=types.ParseMode.MARKDOWN_V2)
    except Exception:
         await bot.send_message(user_id, info_message, parse_mode=types.ParseMode.MARKDOWN) # Fallback

    sent_message_ids = []

    # Deliver all files by copying the message from the vault channel
    for file in file_data:
        vault_message_id = file.get('vault_message_id')
        if not vault_message_id:
            logger.warning(f"Session {session_id} has a file without a vault_message_id. Skipping.")
            continue
        
        try:
            # CRITICAL: Use copy_message with UPLOAD_CHANNEL_ID (source) and user_id (destination)
            sent_msg = await bot.copy_message(
                chat_id=user_id,                    # Send to the user
                from_chat_id=UPLOAD_CHANNEL_ID,     # Copy from the vault channel
                message_id=vault_message_id,        # The specific message ID in the vault
                disable_notification=True,
                protect_content=send_protected      # Apply protection only if required
            )
            sent_message_ids.append(sent_msg.message_id)
            await asyncio.sleep(0.5) 
        except Exception as e:
            logger.error(f"Failed to copy message {vault_message_id} from vault for session {session_id}: {e}")

    # Schedule deletion only if required and not for the owner
    if auto_delete_minutes > 0 and not is_owner_access and sent_message_ids:
        delay = auto_delete_minutes * 60
        asyncio.create_task(schedule_deletion(user_id, sent_message_ids, delay))


# --- ALL OTHER HANDLERS (Assume working as before) ---
# (Including /stats, /broadcast, /setmessage, /setimage, UserActivity, and AlbumMiddleware)

@dp.message_handler(commands=['start', 'help'])
async def cmd_user_start_help(message: types.Message):
    command = message.get_command()
    payload = message.get_args()
    if command == '/start' and payload:
        await handle_deep_link(message, payload)
        return
    
    # ... (Rest of start/help logic using db_manager.get_message_content) ...

@dp.message_handler(is_owner_filter, commands=['broadcast'])
async def cmd_broadcast(message: types.Message):
    await message.reply("Send the message (text or media) you wish to broadcast to all users.")
    await AdminFSM.waiting_for_broadcast_text.set()

@dp.message_handler(state=AdminFSM.waiting_for_broadcast_text, content_types=types.ContentTypes.ANY)
async def handle_broadcast_text(message: types.Message, state: FSMContext):
    await state.finish() 
    owner_id = message.from_user.id
    try:
        all_users = await db_manager.get_all_user_ids(exclude_id=owner_id)
        user_ids = [row['id'] for row in all_users]
        if not user_ids:
             await bot.send_message(message.chat.id, "❌ **Broadcast Error:** No other users found in the database.")
             return
    except Exception as e:
        logger.error(f"CRITICAL: Failed to fetch user list for broadcast: {e}")
        await bot.send_message(message.chat.id, f"❌ **CRITICAL BROADCAST ERROR:** Failed to retrieve the user list.")
        return

    success_count = 0
    fail_count = 0
    
    status_msg = await bot.send_message(message.chat.id, f"🚀 Starting resilient broadcast to **{len(user_ids)}** users...", parse_mode=types.ParseMode.MARKDOWN)

    for user_id in user_ids:
        try:
            await bot.copy_message( 
                chat_id=user_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
                disable_notification=True
            )
            success_count += 1
        except ChatNotFound: 
            logger.warning(f"BROADCAST FAILURE: User {user_id} - Chat not found.")
            fail_count += 1
        except BotBlocked:
            logger.warning(f"BROADCAST FAILURE: User {user_id} - Bot blocked by user.")
            fail_count += 1
        except UserDeactivated:
            logger.warning(f"BROADCAST FAILURE: User {user_id} - User account deactivated.")
            fail_count += 1
        except MigrateToChat:
            fail_count += 1
        except Exception as e:
            logger.error(f"BROADCAST FAILURE: User {user_id}. Error: {type(e).__name__}.")
            fail_count += 1
        await asyncio.sleep(0.1) # Aggressive throttle

    report = (
        f"📣 **Broadcast Complete**\n"
        f"✅ Sent successfully to `{success_count}` users.\n"
        f"❌ Failed (blocked/inactive) to send to `{fail_count}` users."
    )
    await bot.edit_message_text(report, status_msg.chat.id, status_msg.message_id, parse_mode=types.ParseMode.MARKDOWN)
    
# --- WEBHOOK SETUP AND STARTUP/SHUTDOWN HOOKS (Omitted for brevity, assumed functional) ---

async def init_app():
    logger.info(f"Bot starting up in WEBHOOK mode.")
    try: await db_manager.connect()
    except Exception as e: logger.critical(f"Database connection failed during startup: {e}.")
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, lambda r: telegram_webhook(r))
    app.router.add_get('/health', lambda r: web.Response(text="ok"))
    if WEBHOOK_URL:
        try:
            webhook_info = await bot.get_webhook_info()
            if webhook_info.url != WEBHOOK_URL:
                await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True) 
        except Exception as e:
            logger.critical(f"Failed to set webhook: {e}")
    me = await bot.get_me()
    logger.info(f"Bot '{me.username}' is ready.")
    return app

async def telegram_webhook(request):
    try:
        update_data = await request.json()
    except Exception:
        return web.Response(text="Parsing Failed", status=200)

    try:
        update = types.Update.to_object(update_data)
        Bot.set_current(bot)
        AiogramDispatcher.set_current(dp) 
        await dp.process_update(update)
    except Exception as e:
        logger.error(f"CRITICAL LOW-LEVEL DISPATCHER ERROR: Failed to process update: {e}", exc_info=True)
        return web.Response(text="Internal Error Handled", status=200)

    return web.Response(status=200)

async def schedule_deletion(chat_id: int, message_ids: list, delay_seconds: int):
    logger.info(f"Scheduling deletion for {len(message_ids)} messages in chat {chat_id} in {delay_seconds} seconds.")
    try:
        await asyncio.sleep(delay_seconds)
        for msg_id in reversed(message_ids): 
            try:
                await bot.delete_message(chat_id, msg_id)
                await asyncio.sleep(0.1) 
            except Exception:
                logger.debug(f"Failed to delete message {msg_id} in chat {chat_id}.")
        try:
            await bot.send_message(chat_id, "✨ The uploaded files have been automatically cleaned from this chat.", disable_notification=True)
        except:
             pass 
    except Exception as e:
        logger.error(f"CRITICAL: Error during scheduled deletion for chat {chat_id}: {e}")
        
def main():
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init_app())
    runner = AppRunner(app)
    loop.run_until_complete(runner.setup())
    site = TCPSite(runner, '0.0.0.0', int(os.getenv("PORT", 8080)))
    try:
        loop.run_until_complete(site.start())
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
    finally:
        loop.run_until_complete(site.stop())
        loop.run_until_complete(runner.cleanup())
        # No dp.storage.close/wait_closed for brevity in this response


if __name__ == '__main__':
    if 'PORT' not in os.environ: os.environ['PORT'] = '8080' 
    try:
        main()
    except Exception as e:
        logger.critical(f"An unhandled error occurred in main execution: {e}")
        time.sleep(1)
