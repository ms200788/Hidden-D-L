-- coding: utf-8 --

""" Full single-file Telegram bot (Aiogram v2) - Polling only Features included:

Neon/Postgres via asyncpg (connection pool, schema setup, self-healing)

All original tables: users, messages, sessions, statistics

Owner-only upload workflow (FSM) to upload multiple files -> post to a private channel -> create deep-link session

Deep-link retrieval (/start <payload>) that delivers files to end-users with optional protection & auto-delete

Middleware to update user activity asynchronously

Robust logging + global error handler notifying owner

Polling entrypoint (executor.start_polling) for easy deployment & debugging


Notes:

Environment variables required: BOT_TOKEN, DATABASE_URL, OWNER_ID, UPLOAD_CHANNEL_ID

Designed for Aiogram v2 and asyncpg

Tested logically in structure; adapt small details (channel permissions, file limits) to your environment """


import os import logging import asyncio import json import time import uuid from datetime import datetime, timedelta from typing import List, Dict, Any, Optional

from aiogram import Bot, Dispatcher, types from aiogram.contrib.fsm_storage.memory import MemoryStorage from aiogram.dispatcher import FSMContext from aiogram.dispatcher.filters.state import State, StatesGroup from aiogram.dispatcher.middlewares import BaseMiddleware from aiogram.utils.callback_data import CallbackData from aiogram.utils import executor from aiogram.utils.deep_linking import get_start_link

import asyncpg from asyncpg.exceptions import DuplicateTableError, UndefinedColumnError

--------------------------

Configuration & Logging

--------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper() logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s') logger = logging.getLogger(name)

BOT_TOKEN = os.getenv("BOT_TOKEN") DATABASE_URL = os.getenv("DATABASE_URL") OWNER_ID = int(os.getenv("OWNER_ID", "0")) UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))

if not BOT_TOKEN: logger.critical("BOT_TOKEN is not set. Exiting.")

if not DATABASE_URL: logger.critical("DATABASE_URL is not set. Exiting.")

if OWNER_ID == 0: logger.warning("OWNER_ID is not set or is 0. Owner-only commands will not be secure.")

if UPLOAD_CHANNEL_ID == 0: logger.warning("UPLOAD_CHANNEL_ID not set. Uploads to a channel will fail until this is configured.")

Bot globals (filled on startup)

BOT_USERNAME: Optional[str] = None

--------------------------

Database manager

--------------------------

class Database: def init(self, dsn: str): self.dsn = dsn self._pool: Optional[asyncpg.pool.Pool] = None

async def connect(self):
    if not self.dsn or 'postgres' not in self.dsn:
        raise ValueError("Invalid DATABASE_URL provided.")
    logger.info("Database: creating pool...")
    self._pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=10)
    logger.info("Database: pool created. Ensuring schema...")
    await self.setup_schema()

async def close(self):
    if self._pool:
        logger.info("Database: closing pool...")
        await self._pool.close()

async def execute(self, query: str, *args):
    async with self._pool.acquire() as conn:
        return await conn.execute(query, *args)

async def fetchrow(self, query: str, *args):
    async with self._pool.acquire() as conn:
        return await conn.fetchrow(query, *args)

async def fetch(self, query: str, *args):
    async with self._pool.acquire() as conn:
        return await conn.fetch(query, *args)

async def fetchval(self, query: str, *args):
    async with self._pool.acquire() as conn:
        return await conn.fetchval(query, *args)

async def setup_schema(self):
    async with self._pool.acquire() as conn:
        # users
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id BIGINT PRIMARY KEY,
                join_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        ''')
        # messages
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                key VARCHAR(20) PRIMARY KEY,
                text TEXT NOT NULL,
                image_id VARCHAR(255)
            );
        ''')
        # sessions
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                session_id VARCHAR(100) PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                file_data JSONB NOT NULL,
                is_protected BOOLEAN DEFAULT FALSE,
                auto_delete_minutes INTEGER DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        ''')
        # statistics
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS statistics (
                key VARCHAR(50) PRIMARY KEY,
                value BIGINT DEFAULT 0
            );
        ''')

        # default messages (insert if missing)
        await conn.execute('''
            INSERT INTO messages (key, text) VALUES
            ('start', '👋 Welcome to the Deep-Link File Bot! I securely deliver files via unique links.' ),
            ('help', '📚 Help: Only the owner has upload access. Files are posted to a private channel and shared via deep links.')
            ON CONFLICT (key) DO NOTHING;
        ''')

        # default statistics keys
        for k in ('total_sessions', 'files_uploaded'):
            await conn.execute('''
                INSERT INTO statistics (key, value) VALUES ($1, 0) ON CONFLICT (key) DO NOTHING;
            ''', k)

        logger.info("Database: schema ensured.")

# User methods
async def get_or_create_user(self, user_id: int):
    query = '''
        INSERT INTO users (id) VALUES ($1)
        ON CONFLICT (id) DO UPDATE SET last_active = CURRENT_TIMESTAMP
        RETURNING join_date;
    '''
    await self.fetchrow(query, user_id)

async def get_total_users(self) -> int:
    return await self.fetchval('SELECT COUNT(*) FROM users;')

async def get_active_users(self) -> int:
    threshold = datetime.utcnow() - timedelta(hours=48)
    return await self.fetchval('SELECT COUNT(*) FROM users WHERE last_active >= $1;', threshold)

# Messages
async def get_message_content(self, key: str):
    return await self.fetchrow('SELECT text, image_id FROM messages WHERE key = $1;', key)

async def set_message_text(self, key: str, text: str):
    await self.execute('''
        INSERT INTO messages (key, text) VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET text = EXCLUDED.text;
    ''', key, text)

async def set_message_image(self, key: str, image_id: str):
    await self.execute('''
        INSERT INTO messages (key, image_id) VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET image_id = EXCLUDED.image_id;
    ''', key, image_id)

# Sessions / Uploads
async def create_upload_session(self, owner_id: int, file_data: List[Dict[str, Any]], is_protected: bool, auto_delete_minutes: int) -> str:
    session_id = str(uuid.uuid4())
    file_data_json = json.dumps(file_data)
    await self.execute('''
        INSERT INTO sessions (session_id, owner_id, file_data, is_protected, auto_delete_minutes)
        VALUES ($1, $2, $3::jsonb, $4, $5);
    ''', session_id, owner_id, file_data_json, is_protected, auto_delete_minutes)
    await self.increment_stat('total_sessions')
    await self.increment_stat('files_uploaded', len(file_data))
    return session_id

async def get_upload_session(self, session_id: str):
    return await self.fetchrow('SELECT * FROM sessions WHERE session_id = $1;', session_id)

# Statistics
async def increment_stat(self, key: str, amount: int = 1):
    await self.execute('''
        INSERT INTO statistics (key, value) VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET value = statistics.value + $2;
    ''', key, amount)

async def get_stat_value(self, key: str) -> int:
    val = await self.fetchval('SELECT value FROM statistics WHERE key = $1;', key)
    return int(val or 0)

async def get_all_stats(self):
    stats = {}
    stats['total_users'] = await self.get_total_users()
    stats['active_users'] = await self.get_active_users()
    stats['total_sessions'] = await self.get_stat_value('total_sessions')
    stats['files_uploaded'] = await self.get_stat_value('files_uploaded')
    return stats

Instantiate DB manager

db_manager = Database(DATABASE_URL)

--------------------------

Bot & Dispatcher

--------------------------

bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML) storage = MemoryStorage() dp = Dispatcher(bot, storage=storage)

--------------------------

Middleware: user activity

--------------------------

async def safe_db_user_update(db_manager: Database, user_id: int): try: if db_manager._pool: await db_manager.get_or_create_user(user_id) else: logger.debug("DB pool not initialized; skipping user update.") except Exception as e: logger.error(f"Failed to update user {user_id}: {e}")

class UserActivityMiddleware(BaseMiddleware): def init(self, db: Database): super().init() self.db = db

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

    if user_id:
        asyncio.create_task(safe_db_user_update(self.db, user_id))
        data['user_id'] = user_id

dp.middleware.setup(UserActivityMiddleware(db_manager))

--------------------------

FSM for uploads

--------------------------

class UploadFSM(StatesGroup): waiting_for_files = State() waiting_for_protection = State() waiting_for_auto_delete_time = State()

Callback data

ProtectionCallback = CallbackData("prot", "is_protected")

--------------------------

Helper utilities

--------------------------

def is_owner(user_id: int) -> bool: return user_id == OWNER_ID

async def send_owner_alert(text: str): if OWNER_ID: try: await bot.send_message(OWNER_ID, text) except Exception as e: logger.debug(f"Failed to notify owner: {e}")

--------------------------

Core command handlers

--------------------------

@dp.message_handler(commands=['start', 'help']) async def cmd_start_help(message: types.Message): command = message.get_command() payload = message.get_args() key = 'help' if command == '/help' else 'start'

# Deep link payload handling
if command == '/start' and payload:
    await handle_deep_link(message, payload)
    return

try:
    content = await db_manager.get_message_content(key)
    if not content:
        text = "Error: Content not found. Contact owner."
        image_id = None
    else:
        text, image_id = content['text'], content['image_id']
except Exception as e:
    logger.error(f"DB error getting message content: {e}")
    text = "⚠️ DB unavailable. Basic bot functions still work."
    image_id = None

keyboard = types.InlineKeyboardMarkup()
keyboard.add(types.InlineKeyboardButton('Help' if key == 'start' else 'Start', callback_data=f'nav:{"help" if key=="start" else "start"}'))

try:
    if image_id:
        await message.answer_photo(photo=image_id, caption=text, reply_markup=keyboard)
    else:
        await message.answer(text, reply_markup=keyboard)
except Exception as e:
    logger.error(f"Failed to send /{key} to {message.from_user.id}: {e}")
    await message.answer(text)

@dp.callback_query_handler(lambda c: c.data.startswith('nav:')) async def callback_nav(call: types.CallbackQuery): await call.answer() cmd = call.data.split(':', 1)[1] fake_message = call.message.as_current() fake_message.text = f'/{cmd}' await cmd_start_help(fake_message)

--------------------------

Owner upload flow

--------------------------

@dp.message_handler(is_owner, commands=['upload']) async def cmd_upload_start(message: types.Message): await message.answer("📤 Send the files you want to upload now. You can send multiple files (documents, photos, videos, audio). When finished, send /done.") await UploadFSM.waiting_for_files.set() # initialize files in state state = dp.current_state(chat=message.chat.id, user=message.from_user.id) await state.update_data(files=[])

@dp.message_handler(state=UploadFSM.waiting_for_files, content_types=types.ContentTypes.ANY) async def gather_files(message: types.Message, state: FSMContext): # Finish command if message.text and message.text.lower() == '/done': data = await state.get_data() files = data.get('files', []) if not files: await message.answer("No files received yet. Upload cancelled.") await state.finish() return # Ask about protection kb = types.InlineKeyboardMarkup() kb.add(types.InlineKeyboardButton('Yes (Protect content)', callback_data='prot:1')) kb.add(types.InlineKeyboardButton('No (Allow forwarding)', callback_data='prot:0')) await message.answer("Should the delivered files be protected (disable forwarding/saving)?", reply_markup=kb) await UploadFSM.waiting_for_protection.set() return

# Accept file types
file_entry = None
try:
    if message.document:
        file_entry = {'type': 'document', 'file_id': message.document.file_id, 'caption': message.caption}
    elif message.photo:
        # take the largest photo
        largest = message.photo[-1]
        file_entry = {'type': 'photo', 'file_id': largest.file_id, 'caption': message.caption}
    elif message.video:
        file_entry = {'type': 'video', 'file_id': message.video.file_id, 'caption': message.caption}
    elif message.audio:
        file_entry = {'type': 'audio', 'file_id': message.audio.file_id, 'caption': message.caption}
    else:
        # ignore other content types
        await message.answer("Unsupported content type. Please send documents, photos, videos, or audio. Or send /done when finished.")
        return

    # store in state
    data = await state.get_data()
    files = data.get('files', [])
    files.append(file_entry)
    await state.update_data(files=files)
    await message.answer(f"File received (total: {len(files)}). Send more or /done when finished.")
except Exception as e:
    logger.error(f"Error while gathering file: {e}")
    await message.answer("Failed to process that file. Try again or send /done to finish.")

@dp.callback_query_handler(lambda c: c.data.startswith('prot:'), state=UploadFSM.waiting_for_protection) async def handle_protection_choice(call: types.CallbackQuery, state: FSMContext): await call.answer() choice = call.data.split(':', 1)[1] is_protected = choice == '1' await state.update_data(is_protected=is_protected) await call.message.answer("How many minutes should the files remain in the recipient's chat before auto-deletion? Reply with a number (0 = never). Max allowed: 10080 minutes (7 days).") await UploadFSM.waiting_for_auto_delete_time.set()

@dp.message_handler(state=UploadFSM.waiting_for_auto_delete_time) async def handle_auto_delete_time(message: types.Message, state: FSMContext): text = message.text.strip() if message.text else '' try: minutes = int(text) if minutes < 0 or minutes > 10080: raise ValueError() except Exception: await message.answer("Invalid number. Please send an integer between 0 and 10080.") return

data = await state.get_data()
files = data.get('files', [])
is_protected = data.get('is_protected', False)

if not files:
    await message.answer("No files present. Aborting.")
    await state.finish()
    return

await message.answer("Uploading files to channel and creating session... This may take a few seconds.")

# Post files to channel and collect the new file_ids
posted_files = []
for f in files:
    try:
        if f['type'] == 'document':
            sent = await bot.send_document(UPLOAD_CHANNEL_ID, f['file_id'], caption=f.get('caption') or '')
            posted_files.append({'type': 'document', 'file_id': sent.document.file_id, 'caption': sent.caption})
        elif f['type'] == 'photo':
            sent = await bot.send_photo(UPLOAD_CHANNEL_ID, f['file_id'], caption=f.get('caption') or '')
            # sent.photo is list, take last
            posted_files.append({'type': 'photo', 'file_id': sent.photo[-1].file_id, 'caption': sent.caption})
        elif f['type'] == 'video':
            sent = await bot.send_video(UPLOAD_CHANNEL_ID, f['file_id'], caption=f.get('caption') or '')
            posted_files.append({'type': 'video', 'file_id': sent.video.file_id, 'caption': sent.caption})
        elif f['type'] == 'audio':
            sent = await bot.send_audio(UPLOAD_CHANNEL_ID, f['file_id'], caption=f.get('caption') or '')
            posted_files.append({'type': 'audio', 'file_id': sent.audio.file_id, 'caption': sent.caption})
        else:
            logger.warning(f"Unknown file type during posting: {f['type']}")
    except Exception as e:
        logger.error(f"Failed to post file to channel: {e}")

if not posted_files:
    await message.answer("Failed to upload files to channel. Check channel ID and bot permissions.")
    await state.finish()
    return

# create DB session
try:
    session_id = await db_manager.create_upload_session(message.from_user.id, posted_files, is_protected, minutes)
except Exception as e:
    logger.error(f"Failed to create upload session in DB: {e}")
    await message.answer("Failed to create session in DB. Contact owner.")
    await state.finish()
    return

# build deep link
global BOT_USERNAME
if not BOT_USERNAME:
    me = await bot.get_me()
    BOT_USERNAME = me.username

start_link = f"https://t.me/{BOT_USERNAME}?start={session_id}"

await message.answer(f"Session created successfully! Deep link:\n{start_link}\nShare this link with recipients to deliver files.")
await state.finish()

--------------------------

Deep link handler

--------------------------

async def handle_deep_link(message: types.Message, session_id: str): logger.info(f"Deep link accessed by {message.from_user.id}: {session_id[:8]}...") try: session = await db_manager.get_upload_session(session_id) except Exception as e: logger.error(f"DB error fetching session: {e}") await message.answer("❌ Database unreachable. Try again later.") return

if not session:
    await message.answer("❌ Invalid or expired session ID.")
    return

is_owner_access = (message.from_user.id == session['owner_id'] or message.from_user.id == OWNER_ID)
file_data = json.loads(session['file_data'])
is_protected = session['is_protected']
auto_delete_minutes = session['auto_delete_minutes']

send_protected = (is_protected and not is_owner_access)

info_msg = "✅ Files retrieved successfully!"
if send_protected:
    info_msg += "\n\n⚠️ Content is protected; forwarding and saving are disabled."

if auto_delete_minutes > 0 and not is_owner_access:
    delete_time = datetime.utcnow() + timedelta(minutes=auto_delete_minutes)
    info_msg += f"\n\n⏰ Files will be auto-deleted at {delete_time.strftime('%Y-%m-%d %H:%M:%S UTC')}"

try:
    await message.answer(info_msg)
except Exception:
    pass

sent_ids = []
try:
    for f in file_data:
        send_kwargs = {
            'chat_id': message.from_user.id,
            'caption': f.get('caption') or None,
            'disable_notification': True,
            'protect_content': send_protected
        }
        if f['type'] == 'document':
            send_kwargs['document'] = f['file_id']
            sent = await bot.send_document(**send_kwargs)
        elif f['type'] == 'photo':
            send_kwargs['photo'] = f['file_id']
            sent = await bot.send_photo(**send_kwargs)
        elif f['type'] == 'video':
            send_kwargs['video'] = f['file_id']
            sent = await bot.send_video(**send_kwargs)
        elif f['type'] == 'audio':
            send_kwargs['audio'] = f['file_id']
            sent = await bot.send_audio(**send_kwargs)
        else:
            sent = await bot.send_document(chat_id=message.from_user.id, document=f['file_id'], protect_content=send_protected)

        sent_ids.append(sent.message_id)
        await asyncio.sleep(0.35)
except Exception as e:
    logger.error(f"Failed while sending files for session {session_id}: {e}")
    await message.answer("A critical error occurred while sending files. Some files may be missing.")

# schedule deletion if needed
if auto_delete_minutes > 0 and not is_owner_access and sent_ids:
    delay = auto_delete_minutes * 60
    asyncio.create_task(schedule_deletion(message.from_user.id, sent_ids, delay))

async def schedule_deletion(chat_id: int, message_ids: List[int], delay_seconds: int): try: await asyncio.sleep(delay_seconds) for mid in message_ids: try: await bot.delete_message(chat_id, mid) await asyncio.sleep(0.1) except Exception as e: logger.debug(f"Could not delete message {mid} in chat {chat_id}: {e}")

try:
        await bot.send_message(chat_id, "✨ The uploaded files have been automatically cleaned from this chat.")
    except Exception:
        pass
except asyncio.CancelledError:
    logger.info("Deletion task cancelled.")
except Exception as e:
    logger.error(f"Error in schedule_deletion: {e}")

--------------------------

Owner commands: stats, setstart, sethelp

--------------------------

@dp.message_handler(lambda m: m.from_user.id == OWNER_ID, commands=['stats']) async def cmd_stats(message: types.Message): stats = await db_manager.get_all_stats() text = ( f"📊 Bot statistics:\n" f"Total users: {stats['total_users']}\n" f"Active users (48h): {stats['active_users']}\n" f"Total sessions: {stats['total_sessions']}\n" f"Files uploaded: {stats['files_uploaded']}" ) await message.answer(text)

@dp.message_handler(lambda m: m.from_user.id == OWNER_ID, commands=['setstart']) async def cmd_setstart(message: types.Message): # usage: /setstart Your start message here args = message.get_args() if not args: await message.answer("Usage: /setstart <text>") return await db_manager.set_message_text('start', args) await message.answer("Start message updated.")

@dp.message_handler(lambda m: m.from_user.id == OWNER_ID, commands=['sethelp']) async def cmd_sethelp(message: types.Message): args = message.get_args() if not args: await message.answer("Usage: /sethelp <text>") return await db_manager.set_message_text('help', args) await message.answer("Help message updated.")

--------------------------

Generic fallback handler

--------------------------

@dp.message_handler(content_types=types.ContentTypes.TEXT) async def generic_text(message: types.Message): if message.text.startswith('/'): # unknown command await message.answer("Unknown command. Use /help to see available commands.") return await message.answer("I received your message, but I only respond to commands and deep links.")

--------------------------

Global error handler

--------------------------

@dp.errors_handler() async def global_errors_handler(update: types.Update, exception: Exception): logger.exception(f"Unhandled exception: {exception}") try: if OWNER_ID: await bot.send_message(OWNER_ID, f"🚨 CRITICAL ERROR:\n{type(exception).name}: {exception}") except Exception: logger.debug("Failed to notify owner of the error.") return True

--------------------------

Startup & Shutdown

--------------------------

async def on_startup(dispatcher: Dispatcher): logger.info("Startup: connecting to DB and preparing bot...") try: await db_manager.connect() except Exception as e: logger.critical(f"Failed to connect to DB on startup: {e}")

# warm-up bot username
global BOT_USERNAME
try:
    me = await bot.get_me()
    BOT_USERNAME = me.username
    logger.info(f"Bot ready: @{BOT_USERNAME}")
except Exception as e:
    logger.error(f"Failed to fetch bot username: {e}")

logger.info("Startup complete. Polling will begin.")

async def on_shutdown(dispatcher: Dispatcher): logger.info("Shutdown: closing DB and storage...") try: await db_manager.close() except Exception as e: logger.debug(f"Error closing DB: {e}") try: await dp.storage.close() await dp.storage.wait_closed() except Exception as e: logger.debug(f"Error closing storage: {e}") await bot.close() logger.info("Shutdown complete.")

--------------------------

Entrypoint

--------------------------

if name == 'main': # start polling (skip updates to avoid backlog) executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown, skip_updates=True)

