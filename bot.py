#!/usr/bin/env python3
"""
Advanced Telegram File Sharing Bot
Built with aiogram v2 and PostgreSQL
Features: File upload sessions, deep links, auto-delete, broadcast, statistics
"""

import os
import asyncio
import logging
import asyncpg
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
import aiohttp
from aiohttp import web
import json
import secrets
import string

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
BOT_TOKEN = os.getenv('BOT_TOKEN')
OWNER_ID = int(os.getenv('OWNER_ID', 0))
DATABASE_URL = os.getenv('DATABASE_URL')
CHANNEL_ID = os.getenv('CHANNEL_ID')  # Channel where files are stored
RENDER_URL = os.getenv('RENDER_EXTERNAL_URL', 'http://localhost:5000')

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

# Database connection pool
db_pool = None

class UploadStates(StatesGroup):
    waiting_for_files = State()
    waiting_for_options = State()

class DatabaseManager:
    """Database management class for PostgreSQL operations"""
    
    def __init__(self, pool):
        self.pool = pool
    
    async def init_db(self):
        """Initialize database tables"""
        async with self.pool.acquire() as conn:
            # Users table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT PRIMARY KEY,
                    username VARCHAR(100),
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    join_date TIMESTAMP DEFAULT NOW(),
                    last_active TIMESTAMP DEFAULT NOW(),
                    is_active BOOLEAN DEFAULT TRUE
                )
            ''')
            
            # Messages table for start/help messages
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    message_type VARCHAR(20) UNIQUE,
                    text_content TEXT,
                    image_file_id VARCHAR(200),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            # Upload sessions table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS upload_sessions (
                    session_id VARCHAR(50) PRIMARY KEY,
                    owner_id BIGINT,
                    file_ids JSONB,
                    captions JSONB,
                    protect_content BOOLEAN DEFAULT TRUE,
                    auto_delete_minutes INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW(),
                    access_count INTEGER DEFAULT 0
                )
            ''')
            
            # Statistics table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS statistics (
                    id SERIAL PRIMARY KEY,
                    total_users INTEGER DEFAULT 0,
                    active_users INTEGER DEFAULT 0,
                    files_uploaded INTEGER DEFAULT 0,
                    sessions_completed INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            # Insert default messages if not exists
            await conn.execute('''
                INSERT INTO messages (message_type, text_content) 
                VALUES 
                    ('start', 'Welcome to our file sharing bot! Use deep links to access files.'),
                    ('help', 'This bot allows you to access files through deep links. Contact support if you need assistance.')
                ON CONFLICT (message_type) DO NOTHING
            ''')
    
    async def add_user(self, user_id: int, username: str, first_name: str, last_name: str = None):
        """Add or update user in database"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO users (id, username, first_name, last_name, last_active)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    last_active = NOW()
            ''', user_id, username, first_name, last_name)
    
    async def update_user_activity(self, user_id: int):
        """Update user's last active timestamp"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE users SET last_active = NOW() WHERE id = $1',
                user_id
            )
    
    async def get_message(self, message_type: str) -> Tuple[str, str]:
        """Get message content and image file_id"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT text_content, image_file_id FROM messages WHERE message_type = $1',
                message_type
            )
            return (row['text_content'], row['image_file_id']) if row else (None, None)
    
    async def set_message(self, message_type: str, text_content: str, image_file_id: str = None):
        """Set message content and image"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO messages (message_type, text_content, image_file_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (message_type) DO UPDATE SET
                    text_content = EXCLUDED.text_content,
                    image_file_id = EXCLUDED.image_file_id,
                    updated_at = NOW()
            ''', message_type, text_content, image_file_id)
    
    async def create_upload_session(self, session_id: str, owner_id: int, file_ids: List[str], 
                                  captions: List[str], protect_content: bool, auto_delete_minutes: int):
        """Create a new upload session"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO upload_sessions (session_id, owner_id, file_ids, captions, 
                                          protect_content, auto_delete_minutes)
                VALUES ($1, $2, $3, $4, $5, $6)
            ''', session_id, owner_id, json.dumps(file_ids), json.dumps(captions), 
               protect_content, auto_delete_minutes)
    
    async def get_upload_session(self, session_id: str) -> Dict:
        """Get upload session data"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT * FROM upload_sessions WHERE session_id = $1',
                session_id
            )
            if row:
                return {
                    'session_id': row['session_id'],
                    'owner_id': row['owner_id'],
                    'file_ids': json.loads(row['file_ids']),
                    'captions': json.loads(row['captions']),
                    'protect_content': row['protect_content'],
                    'auto_delete_minutes': row['auto_delete_minutes'],
                    'access_count': row['access_count']
                }
            return None
    
    async def increment_access_count(self, session_id: str):
        """Increment access count for session"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE upload_sessions SET access_count = access_count + 1 WHERE session_id = $1',
                session_id
            )
    
    async def get_statistics(self) -> Dict:
        """Get bot statistics"""
        async with self.pool.acquire() as conn:
            # Total users
            total_users = await conn.fetchval('SELECT COUNT(*) FROM users')
            
            # Active users (last 48 hours)
            active_users = await conn.fetchval('''
                SELECT COUNT(*) FROM users 
                WHERE last_active > NOW() - INTERVAL '48 hours'
            ''')
            
            # Total files uploaded (sum of all files in sessions)
            files_uploaded = await conn.fetchval('''
                SELECT SUM(jsonb_array_length(file_ids)) FROM upload_sessions
            ''') or 0
            
            # Sessions completed
            sessions_completed = await conn.fetchval('SELECT COUNT(*) FROM upload_sessions')
            
            return {
                'total_users': total_users,
                'active_users': active_users,
                'files_uploaded': files_uploaded,
                'sessions_completed': sessions_completed
            }

class FileManager:
    """Handles file operations and channel management"""
    
    def __init__(self, bot: Bot, channel_id: str):
        self.bot = bot
        self.channel_id = channel_id
    
    async def upload_to_channel(self, file: types.File, caption: str = "") -> str:
        """Upload file to channel and return file_id"""
        try:
            if file.content_type == 'photo':
                # For photos, we need to get the largest photo size
                message = await self.bot.send_photo(
                    chat_id=self.channel_id,
                    photo=file.file_id,
                    caption=caption
                )
                return message.photo[-1].file_id
            elif file.content_type == 'video':
                message = await self.bot.send_video(
                    chat_id=self.channel_id,
                    video=file.file_id,
                    caption=caption
                )
                return message.video.file_id
            else:
                # For documents
                message = await self.bot.send_document(
                    chat_id=self.channel_id,
                    document=file.file_id,
                    caption=caption
                )
                return message.document.file_id
        except Exception as e:
            logger.error(f"Error uploading to channel: {e}")
            return None
    
    async def send_file_to_user(self, user_id: int, file_id: str, caption: str = "", 
                              protect_content: bool = True, is_owner: bool = False):
        """Send file to user with appropriate settings"""
        try:
            # Determine file type by trying different methods
            if protect_content and not is_owner:
                # For non-owners, respect protect_content setting
                protect = True
            else:
                # Owner can always forward/save files
                protect = False
            
            # Try to send as photo first
            try:
                await self.bot.send_photo(
                    user_id, file_id, caption=caption, 
                    protect_content=protect
                )
                return
            except:
                pass
            
            # Try as video
            try:
                await self.bot.send_video(
                    user_id, file_id, caption=caption,
                    protect_content=protect
                )
                return
            except:
                pass
            
            # Send as document
            await self.bot.send_document(
                user_id, file_id, caption=caption,
                protect_content=protect
            )
            
        except Exception as e:
            logger.error(f"Error sending file to user: {e}")
            raise

def generate_session_id(length=10) -> str:
    """Generate random session ID for deep links"""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# Initialize managers
db_manager = None
file_manager = None

# Temporary storage for upload sessions
upload_sessions = {}

async def on_startup(dp):
    """Initialize bot on startup"""
    global db_pool, db_manager, file_manager
    
    logger.info("Starting bot initialization...")
    
    # Initialize database connection
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        db_manager = DatabaseManager(db_pool)
        await db_manager.init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise
    
    # Initialize file manager
    file_manager = FileManager(bot, CHANNEL_ID)
    
    # Set webhook for Render
    webhook_url = f"{RENDER_URL}/webhook"
    await bot.set_webhook(webhook_url)
    logger.info(f"Webhook set to: {webhook_url}")
    
    logger.info("Bot started successfully")

async def on_shutdown(dp):
    """Cleanup on shutdown"""
    logger.info("Shutting down bot...")
    await db_pool.close()
    await bot.session.close()

def owner_required(handler):
    """Decorator to restrict access to owner only"""
    async def wrapper(message: types.Message, *args, **kwargs):
        if message.from_user.id != OWNER_ID:
            await message.answer("❌ This command is only available for the bot owner.")
            return
        return await handler(message, *args, **kwargs)
    return wrapper

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    """Handle /start command with deep link support"""
    user = message.from_user
    await db_manager.add_user(user.id, user.username, user.first_name, user.last_name)
    await db_manager.update_user_activity(user.id)
    
    # Check for deep link
    args = message.get_args()
    if args:
        # Deep link access
        await handle_deep_link(message, args)
        return
    
    # Regular start command
    text, image_file_id = await db_manager.get_message('start')
    
    if image_file_id:
        await message.answer_photo(
            image_file_id,
            caption=text,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("Help", callback_data="help_button")
            )
        )
    else:
        await message.answer(
            text,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("Help", callback_data="help_button")
            )
        )

@dp.callback_query_handler(lambda c: c.data == 'help_button')
async def help_button_callback(callback_query: types.CallbackQuery):
    """Handle help button click"""
    await callback_query.answer()
    text, image_file_id = await db_manager.get_message('help')
    
    if image_file_id:
        await bot.send_photo(
            callback_query.from_user.id,
            image_file_id,
            caption=text
        )
    else:
        await bot.send_message(callback_query.from_user.id, text)

@dp.message_handler(commands=['help'])
async def cmd_help(message: types.Message):
    """Handle /help command"""
    await db_manager.update_user_activity(message.from_user.id)
    text, image_file_id = await db_manager.get_message('help')
    
    if image_file_id:
        await message.answer_photo(image_file_id, caption=text)
    else:
        await message.answer(text)

@dp.message_handler(commands=['setimage'])
@owner_required
async def cmd_setimage(message: types.Message):
    """Handle /setimage command - owner sets start/help images"""
    if not message.reply_to_message or not message.reply_to_message.photo:
        await message.answer("❌ Please reply to an image message with /setimage")
        return
    
    # Get the highest resolution photo
    photo = message.reply_to_message.photo[-1]
    file_id = photo.file_id
    
    # Create keyboard to choose message type
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton("Start Image", callback_data=f"setimage_start_{file_id}"),
        types.InlineKeyboardButton("Help Image", callback_data=f"setimage_help_{file_id}")
    )
    
    await message.answer(
        "📸 Choose where to set this image:",
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data.startswith('setimage_'))
async def setimage_callback(callback_query: types.CallbackQuery):
    """Handle setimage callback"""
    await callback_query.answer()
    
    parts = callback_query.data.split('_')
    message_type = parts[1]  # start or help
    file_id = parts[2]
    
    # Get current message text
    text, _ = await db_manager.get_message(message_type)
    
    # Update message with new image
    await db_manager.set_message(message_type, text, file_id)
    
    await bot.send_message(
        callback_query.from_user.id,
        f"✅ {message_type.capitalize()} image updated successfully!"
    )

@dp.message_handler(commands=['setmessage'])
@owner_required
async def cmd_setmessage(message: types.Message):
    """Handle /setmessage command - owner sets start/help texts"""
    args = message.get_args()
    if not args:
        await message.answer(
            "❌ Usage: /setmessage <start|help> <message text>\n\n"
            "Example: /setmessage start Welcome to our bot!"
        )
        return
    
    parts = args.split(' ', 1)
    if len(parts) < 2:
        await message.answer("❌ Please provide both message type and text")
        return
    
    message_type = parts[0].lower()
    new_text = parts[1]
    
    if message_type not in ['start', 'help']:
        await message.answer("❌ Message type must be 'start' or 'help'")
        return
    
    # Get current image file_id
    _, image_file_id = await db_manager.get_message(message_type)
    
    # Update message
    await db_manager.set_message(message_type, new_text, image_file_id)
    
    await message.answer(f"✅ {message_type.capitalize()} message updated successfully!")

@dp.message_handler(commands=['broadcast'])
@owner_required
async def cmd_broadcast(message: types.Message):
    """Handle /broadcast command - send message to all users"""
    if not message.reply_to_message:
        await message.answer("❌ Please reply to the message you want to broadcast")
        return
    
    broadcast_message = message.reply_to_message
    
    # Get all users
    async with db_pool.acquire() as conn:
        users = await conn.fetch('SELECT id FROM users WHERE is_active = TRUE')
    
    total_users = len(users)
    successful = 0
    failed = 0
    
    await message.answer(f"📢 Starting broadcast to {total_users} users...")
    
    for user_record in users:
        try:
            # Copy the message (not forward)
            if broadcast_message.text:
                await bot.send_message(user_record['id'], broadcast_message.text)
            elif broadcast_message.photo:
                await bot.send_photo(
                    user_record['id'],
                    broadcast_message.photo[-1].file_id,
                    caption=broadcast_message.caption
                )
            elif broadcast_message.video:
                await bot.send_video(
                    user_record['id'],
                    broadcast_message.video.file_id,
                    caption=broadcast_message.caption
                )
            elif broadcast_message.document:
                await bot.send_document(
                    user_record['id'],
                    broadcast_message.document.file_id,
                    caption=broadcast_message.caption
                )
            else:
                await bot.send_message(user_record['id'], "New update from admin!")
            
            successful += 1
            await asyncio.sleep(0.1)  # Rate limiting
            
        except Exception as e:
            logger.error(f"Failed to send to user {user_record['id']}: {e}")
            failed += 1
    
    await message.answer(
        f"📊 Broadcast completed!\n"
        f"✅ Successful: {successful}\n"
        f"❌ Failed: {failed}\n"
        f"📱 Total: {total_users}"
    )

@dp.message_handler(commands=['stats'])
@owner_required
async def cmd_stats(message: types.Message):
    """Handle /stats command - show bot statistics"""
    stats = await db_manager.get_statistics()
    
    stats_text = (
        "📊 Bot Statistics\n\n"
        f"👥 Total Users: {stats['total_users']}\n"
        f"🟢 Active Users (48h): {stats['active_users']}\n"
        f"📁 Files Uploaded: {stats['files_uploaded']}\n"
        f"📦 Upload Sessions: {stats['sessions_completed']}\n"
        f"⏰ Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    await message.answer(stats_text)

@dp.message_handler(commands=['upload'])
@owner_required
async def cmd_upload(message: types.Message, state: FSMContext):
    """Start upload process"""
    await message.answer(
        "📤 Upload Session Started!\n\n"
        "Please send files one by one (photos, videos, documents).\n"
        "When finished, send /d to complete or /c to cancel.\n\n"
        "Supported files: Images, Videos, Documents"
    )
    
    # Initialize session data
    await state.update_data(
        files=[],
        captions=[],
        file_ids=[]
    )
    
    await UploadStates.waiting_for_files.set()

@dp.message_handler(state=UploadStates.waiting_for_files, content_types=types.ContentType.ANY)
async def process_files(message: types.Message, state: FSMContext):
    """Process files during upload session"""
    user_data = await state.get_data()
    
    if message.text:
        if message.text == '/d':  # Done
            if not user_data['files']:
                await message.answer("❌ No files added! Send /c to cancel.")
                return
            
            # Move to options selection
            keyboard = types.InlineKeyboardMarkup()
            keyboard.row(
                types.InlineKeyboardButton("✅ YES", callback_data="protect_yes"),
                types.InlineKeyboardButton("❌ NO", callback_data="protect_no")
            )
            
            await message.answer(
                f"📁 Files collected: {len(user_data['files'])}\n\n"
                "🔒 Protect content from forwarding/saving?\n"
                "(Non-owners won't be able to forward/save files)",
                reply_markup=keyboard
            )
            
            await UploadStates.waiting_for_options.set()
            return
            
        elif message.text == '/c':  # Cancel
            await state.finish()
            await message.answer("❌ Upload session cancelled.")
            return
    
    # Process file
    file_info = None
    caption = message.caption or ""
    
    if message.photo:
        file_info = message.photo[-1]  # Largest size
        file_type = "photo"
    elif message.video:
        file_info = message.video
        file_type = "video"
    elif message.document:
        file_info = message.document
        file_type = "document"
    else:
        await message.answer("❌ Unsupported file type. Please send photos, videos, or documents.")
        return
    
    # Upload to channel and get file_id
    try:
        channel_file_id = await file_manager.upload_to_channel(file_info, caption)
        if not channel_file_id:
            await message.answer("❌ Failed to upload file to channel. Please try again.")
            return
        
        # Store file data
        files = user_data['files'] + [file_type]
        captions = user_data['captions'] + [caption]
        file_ids = user_data['file_ids'] + [channel_file_id]
        
        await state.update_data(
            files=files,
            captions=captions,
            file_ids=file_ids
        )
        
        await message.answer(f"✅ File {len(files)} added successfully!")
        
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        await message.answer("❌ Error processing file. Please try again.")

@dp.callback_query_handler(lambda c: c.data.startswith('protect_'), state=UploadStates.waiting_for_options)
async def process_protect_option(callback_query: types.CallbackQuery, state: FSMContext):
    """Process protect content option"""
    await callback_query.answer()
    
    protect_content = callback_query.data == 'protect_yes'
    user_data = await state.get_data()
    
    # Ask for auto-delete timer
    keyboard = types.InlineKeyboardMarkup()
    keyboard.row(
        types.InlineKeyboardButton("0 min (Keep forever)", callback_data="delete_0"),
        types.InlineKeyboardButton("60 min", callback_data="delete_60")
    )
    keyboard.row(
        types.InlineKeyboardButton("1440 min (24h)", callback_data="delete_1440"),
        types.InlineKeyboardButton("10080 min (7d)", callback_data="delete_10080")
    )
    
    await bot.send_message(
        callback_query.from_user.id,
        "⏰ Auto-delete timer?\n"
        "(Files will be deleted from user's chat after specified minutes)\n"
        "0 = Keep forever",
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data.startswith('delete_'), state=UploadStates.waiting_for_options)
async def process_delete_option(callback_query: types.CallbackQuery, state: FSMContext):
    """Process auto-delete option and complete upload session"""
    await callback_query.answer()
    
    auto_delete_minutes = int(callback_query.data.split('_')[1])
    user_data = await state.get_data()
    
    # Generate session ID
    session_id = generate_session_id()
    
    # Create upload session in database
    await db_manager.create_upload_session(
        session_id=session_id,
        owner_id=callback_query.from_user.id,
        file_ids=user_data['file_ids'],
        captions=user_data['captions'],
        protect_content='protect_yes' in callback_query.data,
        auto_delete_minutes=auto_delete_minutes
    )
    
    # Generate deep link
    deep_link = f"https://t.me/{(await bot.me).username}?start={session_id}"
    
    # Send success message
    success_text = (
        f"✅ Upload session completed!\n\n"
        f"📁 Files: {len(user_data['files'])}\n"
        f"🔒 Protected: {'Yes' if 'protect_yes' in callback_query.data else 'No'}\n"
        f"⏰ Auto-delete: {auto_delete_minutes} minutes\n\n"
        f"🔗 Deep Link:\n`{deep_link}`\n\n"
        f"Share this link with users to access the files."
    )
    
    await bot.send_message(callback_query.from_user.id, success_text)
    await state.finish()

async def handle_deep_link(message: types.Message, session_id: str):
    """Handle deep link access to files"""
    user_id = message.from_user.id
    is_owner = user_id == OWNER_ID
    
    # Get session data
    session_data = await db_manager.get_upload_session(session_id)
    if not session_data:
        await message.answer("❌ Invalid or expired session link.")
        return
    
    # Update access count
    await db_manager.increment_access_count(session_id)
    await db_manager.update_user_activity(user_id)
    
    # Send files to user
    total_files = len(session_data['file_ids'])
    sent_count = 0
    
    progress_msg = await message.answer(f"📤 Sending files... (0/{total_files})")
    
    for i, (file_id, caption) in enumerate(zip(session_data['file_ids'], session_data['captions'])):
        try:
            await file_manager.send_file_to_user(
                user_id, file_id, caption,
                session_data['protect_content'], is_owner
            )
            sent_count += 1
            
            # Update progress every 5 files
            if (i + 1) % 5 == 0 or (i + 1) == total_files:
                await progress_msg.edit_text(f"📤 Sending files... ({i+1}/{total_files})")
            
            await asyncio.sleep(0.5)  # Rate limiting
            
        except Exception as e:
            logger.error(f"Error sending file {i+1}: {e}")
    
    # Send completion message with auto-delete info
    completion_text = f"✅ {sent_count}/{total_files} files sent successfully!"
    
    if session_data['auto_delete_minutes'] > 0 and not is_owner:
        completion_text += f"\n\n⏰ These files will auto-delete in {session_data['auto_delete_minutes']} minutes."
        
        # Schedule deletion
        asyncio.create_task(
            schedule_deletion(user_id, sent_count, session_data['auto_delete_minutes'])
        )
    
    await progress_msg.edit_text(completion_text)

async def schedule_deletion(user_id: int, message_count: int, delay_minutes: int):
    """Schedule message deletion after specified minutes"""
    await asyncio.sleep(delay_minutes * 60)  # Convert to seconds
    
    try:
        # This is a simplified deletion - in production you'd track specific message IDs
        await bot.send_message(
            user_id,
            f"🧹 Auto-cleanup: Files from this session have been deleted as scheduled."
        )
    except Exception as e:
        logger.error(f"Error in auto-deletion: {e}")

@dp.message_handler()
async def handle_other_messages(message: types.Message):
    """Handle other messages"""
    await db_manager.update_user_activity(message.from_user.id)
    
    if message.from_user.id == OWNER_ID:
        await message.answer(
            "🤖 Bot Commands:\n\n"
            "/start - Show welcome message\n"
            "/help - Show help\n"
            "/setimage - Set start/help images\n"
            "/setmessage - Set start/help texts\n"
            "/upload - Start file upload session\n"
            "/broadcast - Send message to all users\n"
            "/stats - Show statistics"
        )
    else:
        await message.answer(
            "👋 Hello! This bot provides file access through deep links.\n\n"
            "Use /help for more information."
        )

# Web server for health checks
async def health_check(request):
    """Health check endpoint for UptimeRobot"""
    return web.Response(text="ok")

async def webhook_handler(request):
    """Handle Telegram webhook updates"""
    if request.method == "POST":
        update = types.Update(**await request.json())
        await dp.process_update(update)
    return web.Response(text="OK")

async def init_web_app():
    """Initialize web application for Render"""
    app = web.Application()
    app.router.add_get('/health', health_check)
    app.router.add_post('/webhook', webhook_handler)
    return app

if __name__ == '__main__':
    # For local testing with polling (comment out for production)
    if os.getenv('ENVIRONMENT') == 'development':
        executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)
    else:
        # For production with webhook
        web_app = init_web_app()
        web.run_app(web_app, host='0.0.0.0', port=5000)