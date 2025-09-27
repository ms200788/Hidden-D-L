#!/usr/bin/env python3
"""
Advanced Telegram File Sharing Bot
Built with aiogram v2, PostgreSQL, and designed for Render deployment
"""

import os
import asyncio
import logging
import asyncpg
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import json
import aiohttp
from urllib.parse import urlencode

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.middleware import BaseMiddleware
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto, InputMediaDocument,
    ReplyKeyboardMarkup, ReplyKeyboardRemove
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
BOT_TOKEN = os.getenv('BOT_TOKEN')
OWNER_ID = int(os.getenv('OWNER_ID', 0))
DATABASE_URL = os.getenv('DATABASE_URL')
RENDER_URL = os.getenv('RENDER_URL')
UPLOAD_CHANNEL = os.getenv('UPLOAD_CHANNEL')

# Validate required environment variables
if not all([BOT_TOKEN, DATABASE_URL, RENDER_URL, UPLOAD_CHANNEL]):
    missing = [var for var in ['BOT_TOKEN', 'DATABASE_URL', 'RENDER_URL', 'UPLOAD_CHANNEL'] 
               if not os.getenv(var)]
    raise Exception(f"Missing environment variables: {', '.join(missing)}")

if OWNER_ID == 0:
    logger.warning("OWNER_ID not set, some commands will be disabled")

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Database connection pool
db_pool = None

class UserActivityMiddleware(BaseMiddleware):
    """Middleware to update user activity"""
    async def on_pre_process_message(self, message: types.Message, data: dict):
        if message.from_user:
            await Database.update_user_activity(message.from_user.id)

class Database:
    """Database operations for the bot"""
    
    @staticmethod
    async def init_db():
        """Initialize database tables"""
        global db_pool
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        
        async with db_pool.acquire() as conn:
            # Users table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    join_date TIMESTAMP DEFAULT NOW(),
                    last_active TIMESTAMP DEFAULT NOW(),
                    is_banned BOOLEAN DEFAULT FALSE
                )
            ''')
            
            # Messages table for start/help messages
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    message_type TEXT NOT NULL UNIQUE,
                    text_content TEXT,
                    image_file_id TEXT,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            # Upload sessions table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS upload_sessions (
                    session_id TEXT PRIMARY KEY,
                    owner_id BIGINT NOT NULL,
                    file_data JSONB NOT NULL,
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
            
            # Initialize messages if not exists
            await conn.execute('''
                INSERT INTO messages (message_type, text_content) 
                VALUES 
                    ('start', 'Welcome to the file sharing bot! Use /help for instructions.'),
                    ('help', 'This bot allows you to share files securely. Contact the owner for access.')
                ON CONFLICT (message_type) DO NOTHING
            ''')
            
            # Initialize statistics
            await conn.execute('''
                INSERT INTO statistics (total_users, active_users, files_uploaded, sessions_completed) 
                VALUES (0, 0, 0, 0)
            ''')
    
    @staticmethod
    async def get_user(user_id: int) -> Optional[asyncpg.Record]:
        """Get user from database"""
        async with db_pool.acquire() as conn:
            return await conn.fetchrow(
                'SELECT * FROM users WHERE id = $1', user_id
            )
    
    @staticmethod
    async def create_user(user: types.User):
        """Create new user in database"""
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO users (id, username, first_name, last_name, join_date, last_active)
                VALUES ($1, $2, $3, $4, NOW(), NOW())
                ON CONFLICT (id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    last_active = NOW()
            ''', user.id, user.username, user.first_name, user.last_name)
    
    @staticmethod
    async def update_user_activity(user_id: int):
        """Update user's last activity timestamp"""
        async with db_pool.acquire() as conn:
            await conn.execute(
                'UPDATE users SET last_active = NOW() WHERE id = $1',
                user_id
            )
    
    @staticmethod
    async def get_message(message_type: str) -> Optional[asyncpg.Record]:
        """Get message content by type"""
        async with db_pool.acquire() as conn:
            return await conn.fetchrow(
                'SELECT * FROM messages WHERE message_type = $1',
                message_type
            )
    
    @staticmethod
    async def update_message(message_type: str, text_content: str = None, image_file_id: str = None):
        """Update message content"""
        async with db_pool.acquire() as conn:
            if text_content and image_file_id:
                await conn.execute(
                    'UPDATE messages SET text_content = $1, image_file_id = $2, updated_at = NOW() WHERE message_type = $3',
                    text_content, image_file_id, message_type
                )
            elif text_content:
                await conn.execute(
                    'UPDATE messages SET text_content = $1, updated_at = NOW() WHERE message_type = $2',
                    text_content, message_type
                )
            elif image_file_id:
                await conn.execute(
                    'UPDATE messages SET image_file_id = $1, updated_at = NOW() WHERE message_type = $2',
                    image_file_id, message_type
                )
    
    @staticmethod
    async def create_upload_session(session_id: str, owner_id: int, file_data: dict, 
                                  protect_content: bool = True, auto_delete_minutes: int = 0):
        """Create new upload session"""
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO upload_sessions (session_id, owner_id, file_data, protect_content, auto_delete_minutes)
                VALUES ($1, $2, $3, $4, $5)
            ''', session_id, owner_id, json.dumps(file_data), protect_content, auto_delete_minutes)
    
    @staticmethod
    async def get_upload_session(session_id: str) -> Optional[asyncpg.Record]:
        """Get upload session by ID"""
        async with db_pool.acquire() as conn:
            session = await conn.fetchrow(
                'SELECT * FROM upload_sessions WHERE session_id = $1',
                session_id
            )
            if session:
                # Increment access count
                await conn.execute(
                    'UPDATE upload_sessions SET access_count = access_count + 1 WHERE session_id = $1',
                    session_id
                )
            return session
    
    @staticmethod
    async def get_statistics() -> asyncpg.Record:
        """Get current statistics"""
        async with db_pool.acquire() as conn:
            # Update statistics first
            total_users = await conn.fetchval('SELECT COUNT(*) FROM users')
            active_users = await conn.fetchval(
                'SELECT COUNT(*) FROM users WHERE last_active > NOW() - INTERVAL \'48 hours\''
            )
            files_uploaded = await conn.fetchval(
                'SELECT COALESCE(SUM(jsonb_array_length(file_data)), 0) FROM upload_sessions'
            )
            sessions_completed = await conn.fetchval('SELECT COUNT(*) FROM upload_sessions')
            
            await conn.execute('''
                UPDATE statistics SET 
                    total_users = $1,
                    active_users = $2,
                    files_uploaded = $3,
                    sessions_completed = $4,
                    last_updated = NOW()
                WHERE id = 1
            ''', total_users, active_users, files_uploaded, sessions_completed)
            
            return await conn.fetchrow('SELECT * FROM statistics WHERE id = 1')

class UploadStates(StatesGroup):
    """State machine for upload process"""
    waiting_for_files = State()
    waiting_for_protect_content = State()
    waiting_for_auto_delete = State()

class MessageStates(StatesGroup):
    """State machine for message setting"""
    waiting_for_message_type = State()
    waiting_for_message_text = State()

class BroadcastStates(StatesGroup):
    """State machine for broadcast"""
    waiting_for_broadcast = State()

# Global variables for temporary storage
upload_sessions: Dict[int, List[types.Message]] = {}

# Utility functions
def generate_session_id() -> str:
    """Generate unique session ID"""
    import secrets
    return secrets.token_hex(8)

def is_owner(user_id: int) -> bool:
    """Check if user is owner"""
    return user_id == OWNER_ID

async def send_to_upload_channel(file_data: dict) -> str:
    """
    Send file to upload channel and return file_id
    This is a simplified version - in production you'd handle different media types
    """
    try:
        # For demo purposes, we'll use a placeholder
        # In real implementation, you'd use bot.send_message to your channel
        # and extract the file_id from the sent message
        return f"channel_file_id_{datetime.now().timestamp()}"
    except Exception as e:
        logger.error(f"Error sending to upload channel: {e}")
        raise

async def setup_bot_commands():
    """Setup bot commands menu"""
    commands = [
        types.BotCommand("start", "Start the bot"),
        types.BotCommand("help", "Get help"),
    ]
    
    await bot.set_my_commands(commands)

# =============================================================================
# HANDLERS - START & HELP
# =============================================================================

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    """Handle /start command with deep link support"""
    user_id = message.from_user.id
    
    # Update user in database
    await Database.create_user(message.from_user)
    await Database.update_user_activity(user_id)
    
    # Check for deep link
    args = message.get_args()
    if args and args.startswith('start='):
        session_id = args.replace('start=', '')
        await handle_deep_link_access(message, session_id)
        return
    
    # Regular start command
    message_data = await Database.get_message('start')
    
    if message_data and message_data['image_file_id']:
        # Send with image
        await message.answer_photo(
            photo=message_data['image_file_id'],
            caption=message_data['text_content'],
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("Help", callback_data="help_button")
            )
        )
    else:
        # Send text only
        text_content = message_data['text_content'] if message_data else "Welcome to the file sharing bot!"
        await message.answer(
            text_content,
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("Help", callback_data="help_button")
            )
        )

@dp.message_handler(commands=['help'])
async def cmd_help(message: types.Message):
    """Handle /help command"""
    user_id = message.from_user.id
    await Database.update_user_activity(user_id)
    
    message_data = await Database.get_message('help')
    
    if message_data and message_data['image_file_id']:
        await message.answer_photo(
            photo=message_data['image_file_id'],
            caption=message_data['text_content']
        )
    else:
        text_content = message_data['text_content'] if message_data else "Help information will be available soon."
        await message.answer(text_content)

@dp.callback_query_handler(lambda c: c.data == 'help_button')
async def help_button_callback(callback_query: types.CallbackQuery):
    """Handle help button callback"""
    await callback_query.answer()
    message_data = await Database.get_message('help')
    
    if message_data and message_data['image_file_id']:
        await callback_query.message.answer_photo(
            photo=message_data['image_file_id'],
            caption=message_data['text_content']
        )
    else:
        text_content = message_data['text_content'] if message_data else "Help information will be available soon."
        await callback_query.message.answer(text_content)

# =============================================================================
# HANDLERS - OWNER ONLY COMMANDS
# =============================================================================

def owner_only(handler):
    """Decorator to restrict commands to owner only"""
    async def wrapped_handler(message: types.Message, *args, **kwargs):
        if not is_owner(message.from_user.id):
            await message.answer("❌ This command is only available for the bot owner.")
            return
        return await handler(message, *args, **kwargs)
    return wrapped_handler

@dp.message_handler(commands=['setimage'])
@owner_only
async def cmd_setimage(message: types.Message):
    """Handle /setimage command - owner only"""
    if message.reply_to_message and message.reply_to_message.photo:
        # Store the image temporarily and ask for type
        upload_sessions[message.from_user.id] = [message.reply_to_message]
        
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add("Start Image", "Help Image")
        keyboard.add("Cancel")
        
        await MessageStates.waiting_for_message_type.set()
        await message.answer(
            "Which message should this image be set for?",
            reply_markup=keyboard
        )
    else:
        await message.answer(
            "Please reply to an image message with /setimage command"
        )

@dp.message_handler(state=MessageStates.waiting_for_message_type)
@owner_only
async def process_message_type(message: types.Message, state: FSMContext):
    """Process message type selection for image setting"""
    if message.text not in ["Start Image", "Help Image"]:
        await message.answer("Please choose 'Start Image' or 'Help Image'")
        return
    
    if message.from_user.id not in upload_sessions or not upload_sessions[message.from_user.id]:
        await message.answer("No image found. Please start over.")
        await state.finish()
        return
    
    image_message = upload_sessions[message.from_user.id][0]
    message_type = "start" if message.text == "Start Image" else "help"
    
    # Get the highest resolution photo
    photo = image_message.photo[-1]
    file_id = photo.file_id
    
    # Update database
    await Database.update_message(message_type, image_file_id=file_id)
    
    # Cleanup
    if message.from_user.id in upload_sessions:
        del upload_sessions[message.from_user.id]
    
    await state.finish()
    await message.answer(
        f"{message.text} has been updated successfully!",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message_handler(commands=['setmessage'])
@owner_only
async def cmd_setmessage(message: types.Message, state: FSMContext):
    """Handle /setmessage command - owner only"""
    args = message.get_args()
    
    if args:
        # Direct command usage: /setmessage start New welcome message
        parts = args.split(' ', 1)
        if len(parts) == 2:
            message_type, new_text = parts
            if message_type in ['start', 'help']:
                await Database.update_message(message_type, text_content=new_text)
                await message.answer(f"{message_type.capitalize()} message updated!")
                return
    
    # Interactive mode
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add("Start Message", "Help Message")
    keyboard.add("Cancel")
    
    await MessageStates.waiting_for_message_type.set()
    await message.answer(
        "Which message do you want to update?",
        reply_markup=keyboard
    )

@dp.message_handler(state=MessageStates.waiting_for_message_type)
@owner_only
async def process_message_text_type(message: types.Message, state: FSMContext):
    """Process message type selection for text setting"""
    if message.text not in ["Start Message", "Help Message"]:
        await message.answer("Please choose 'Start Message' or 'Help Message'")
        return
    
    message_type = "start" if message.text == "Start Message" else "help"
    
    async with state.proxy() as data:
        data['message_type'] = message_type
    
    await MessageStates.waiting_for_message_text.set()
    await message.answer(
        f"Please send the new text for the {message_type} message:",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message_handler(state=MessageStates.waiting_for_message_text)
@owner_only
async def process_message_text(message: types.Message, state: FSMContext):
    """Process new message text"""
    async with state.proxy() as data:
        message_type = data['message_type']
    
    await Database.update_message(message_type, text_content=message.text)
    await state.finish()
    await message.answer(f"{message_type.capitalize()} message updated successfully!")

@dp.message_handler(commands=['stats'])
@owner_only
async def cmd_stats(message: types.Message):
    """Handle /stats command - owner only"""
    stats = await Database.get_statistics()
    
    stats_text = f"""
📊 **Bot Statistics**

👥 **Users:**
• Total Users: {stats['total_users']}
• Active (48h): {stats['active_users']}

📁 **Files:**
• Files Uploaded: {stats['files_uploaded']}
• Sessions Completed: {stats['sessions_completed']}

🕒 Last Updated: {stats['last_updated'].strftime('%Y-%m-%d %H:%M:%S')}
    """
    
    await message.answer(stats_text, parse_mode='Markdown')

@dp.message_handler(commands=['broadcast'])
@owner_only
async def cmd_broadcast(message: types.Message, state: FSMContext):
    """Handle /broadcast command - owner only"""
    await BroadcastStates.waiting_for_broadcast.set()
    await message.answer(
        "Please send the message you want to broadcast to all users.\n\n"
        "You can include text, photos, documents, or any other media.",
        reply_markup=ReplyKeyboardMarkup().add("Cancel")
    )

@dp.message_handler(state=BroadcastStates.waiting_for_broadcast, content_types=types.ContentType.ANY)
@owner_only
async def process_broadcast(message: types.Message, state: FSMContext):
    """Process broadcast message"""
    if message.text == "Cancel":
        await state.finish()
        await message.answer("Broadcast cancelled.", reply_markup=ReplyKeyboardRemove())
        return
    
    await message.answer("Starting broadcast... This may take a while.")
    
    # Get all users
    async with db_pool.acquire() as conn:
        users = await conn.fetch('SELECT id FROM users WHERE NOT is_banned')
    
    success_count = 0
    fail_count = 0
    
    for user_record in users:
        try:
            # Copy the message to user
            if message.content_type == 'text':
                await bot.send_message(user_record['id'], message.text)
            elif message.content_type == 'photo':
                await bot.send_photo(
                    user_record['id'],
                    message.photo[-1].file_id,
                    caption=message.caption
                )
            elif message.content_type == 'document':
                await bot.send_document(
                    user_record['id'],
                    message.document.file_id,
                    caption=message.caption
                )
            # Add more content types as needed
            
            success_count += 1
            await asyncio.sleep(0.1)  # Rate limiting
            
        except Exception as e:
            logger.error(f"Failed to send broadcast to {user_record['id']}: {e}")
            fail_count += 1
    
    await state.finish()
    await message.answer(
        f"📢 Broadcast completed!\n\n"
        f"✅ Success: {success_count}\n"
        f"❌ Failed: {fail_count}",
        reply_markup=ReplyKeyboardRemove()
    )

# =============================================================================
# UPLOAD SESSION HANDLERS
# =============================================================================

@dp.message_handler(commands=['upload'])
@owner_only
async def cmd_upload(message: types.Message):
    """Start upload session - owner only"""
    user_id = message.from_user.id
    upload_sessions[user_id] = []
    
    await UploadStates.waiting_for_files.set()
    await message.answer(
        "📤 **Upload Session Started**\n\n"
        "Send me the files you want to upload (photos, documents, videos).\n"
        "When finished, use:\n"
        "• /d - Done, proceed to options\n"
        "• /c - Cancel upload session\n\n"
        "You can send multiple files at once.",
        parse_mode='Markdown'
    )

@dp.message_handler(state=UploadStates.waiting_for_files, content_types=types.ContentType.ANY)
@owner_only
async def process_upload_files(message: types.Message, state: FSMContext):
    """Process files during upload session"""
    user_id = message.from_user.id
    
    if message.text and message.text.startswith('/'):
        if message.text == '/d':  # Done
            if user_id not in upload_sessions or not upload_sessions[user_id]:
                await message.answer("No files added. Please send files first or use /c to cancel.")
                return
            
            await UploadStates.waiting_for_protect_content.set()
            
            keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
            keyboard.row("YES", "NO")
            
            await message.answer(
                f"📁 **Upload Options**\n\n"
                f"Files collected: {len(upload_sessions[user_id])}\n\n"
                f"**Protect Content?**\n"
                f"Prevents forwarding/saving for users\n",
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            
        elif message.text == '/c':  # Cancel
            if user_id in upload_sessions:
                del upload_sessions[user_id]
            await state.finish()
            await message.answer("Upload session cancelled.", reply_markup=ReplyKeyboardRemove())
        
        return
    
    # Handle media files
    supported_types = ['photo', 'document', 'video', 'audio']
    content_type = message.content_type
    
    if content_type not in supported_types:
        await message.answer("Unsupported file type. Please send photos, documents, videos, or audio.")
        return
    
    if user_id not in upload_sessions:
        upload_sessions[user_id] = []
    
    upload_sessions[user_id].append(message)
    
    file_count = len(upload_sessions[user_id])
    await message.answer(f"✅ File added! Total files: {file_count}\nUse /d when done or /c to cancel.")

@dp.message_handler(state=UploadStates.waiting_for_protect_content)
@owner_only
async def process_protect_content(message: types.Message, state: FSMContext):
    """Process content protection option"""
    if message.text not in ["YES", "NO"]:
        await message.answer("Please choose YES or NO for content protection.")
        return
    
    protect_content = message.text == "YES"
    
    # Ask for auto-delete timer
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.row("0", "60", "1440")  # 0, 1h, 24h
    keyboard.row("10080", "Custom")  # 1 week
    
    async with state.proxy() as data:
        data['protect_content'] = protect_content
    
    await UploadStates.waiting_for_auto_delete.set()
    await message.answer(
        "⏰ **Auto-delete Timer**\n\n"
        "How many minutes until files auto-delete from user's chat?\n"
        "• 0 = Never delete\n"
        "• 60 = 1 hour\n"
        "• 1440 = 24 hours\n"
        "• 10080 = 1 week\n\n"
        "Choose or send a custom number (0-10080):",
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

@dp.message_handler(state=UploadStates.waiting_for_auto_delete)
@owner_only
async def process_auto_delete(message: types.Message, state: FSMContext):
    """Process auto-delete timer and complete upload session"""
    user_id = message.from_user.id
    
    if user_id not in upload_sessions or not upload_sessions[user_id]:
        await message.answer("Error: No files found. Please start over.")
        await state.finish()
        return
    
    try:
        if message.text == "Custom":
            await message.answer("Please send the number of minutes (0-10080):")
            return
        
        auto_delete_minutes = int(message.text)
        if auto_delete_minutes < 0 or auto_delete_minutes > 10080:
            raise ValueError("Out of range")
            
    except ValueError:
        await message.answer("Please enter a valid number between 0 and 10080.")
        return
    
    async with state.proxy() as data:
        protect_content = data['protect_content']
    
    # Prepare file data
    file_data = []
    for file_message in upload_sessions[user_id]:
        file_info = {
            'content_type': file_message.content_type,
            'caption': file_message.caption,
        }
        
        if file_message.caption_entities:
            file_info['caption_entities'] = [entity.to_python() for entity in file_message.caption_entities]
        
        if file_message.content_type == 'photo':
            file_info['file_id'] = file_message.photo[-1].file_id
        elif file_message.content_type == 'document':
            file_info['file_id'] = file_message.document.file_id
            file_info['file_name'] = file_message.document.file_name
        elif file_message.content_type == 'video':
            file_info['file_id'] = file_message.video.file_id
        elif file_message.content_type == 'audio':
            file_info['file_id'] = file_message.audio.file_id
        
        file_data.append(file_info)
    
    # Generate session ID and save to database
    session_id = generate_session_id()
    await Database.create_upload_session(
        session_id, user_id, file_data, protect_content, auto_delete_minutes
    )
    
    # Create deep link
    bot_username = (await bot.me).username
    deep_link = f"https://t.me/{bot_username}?start={session_id}"
    
    # Cleanup
    del upload_sessions[user_id]
    await state.finish()
    
    # Send success message
    await message.answer(
        f"✅ **Upload Session Created!**\n\n"
        f"📁 Files: {len(file_data)}\n"
        f"🔒 Protect Content: {'Yes' if protect_content else 'No'}\n"
        f"⏰ Auto-delete: {auto_delete_minutes} minutes\n\n"
        f"🔗 **Share this link:**\n`{deep_link}`\n\n"
        f"Anyone with this link can access the files.",
        parse_mode='Markdown',
        reply_markup=ReplyKeyboardRemove()
    )

# =============================================================================
# DEEP LINK ACCESS HANDLER
# =============================================================================

async def handle_deep_link_access(message: types.Message, session_id: str):
    """Handle deep link file access"""
    user_id = message.from_user.id
    
    # Update user activity
    await Database.create_user(message.from_user)
    await Database.update_user_activity(user_id)
    
    # Get session data
    session = await Database.get_upload_session(session_id)
    if not session:
        await message.answer("❌ Invalid or expired session link.")
        return
    
    file_data = json.loads(session['file_data'])
    
    # Check if user is owner (bypass restrictions)
    is_owner_user = is_owner(user_id)
    
    # Send files with appropriate settings
    protect_content = session['protect_content'] if not is_owner_user else False
    auto_delete_minutes = session['auto_delete_minutes'] if not is_owner_user else 0
    
    sent_messages = []
    
    for i, file_info in enumerate(file_data):
        try:
            caption = file_info.get('caption', '')
            caption_entities = file_info.get('caption_entities')
            
            # Convert caption entities back to Telegram objects if they exist
            entities = None
            if caption_entities:
                entities = [types.MessageEntity(**entity) for entity in caption_entities]
            
            if file_info['content_type'] == 'photo':
                msg = await message.answer_photo(
                    file_info['file_id'],
                    caption=caption,
                    caption_entities=entities,
                    protect_content=protect_content
                )
            elif file_info['content_type'] == 'document':
                msg = await message.answer_document(
                    file_info['file_id'],
                    caption=caption,
                    caption_entities=entities,
                    protect_content=protect_content
                )
            elif file_info['content_type'] == 'video':
                msg = await message.answer_video(
                    file_info['file_id'],
                    caption=caption,
                    caption_entities=entities,
                    protect_content=protect_content
                )
            elif file_info['content_type'] == 'audio':
                msg = await message.answer_audio(
                    file_info['file_id'],
                    caption=caption,
                    caption_entities=entities,
                    protect_content=protect_content
                )
            else:
                continue
            
            sent_messages.append(msg)
            
            # Small delay between files
            if i < len(file_data) - 1:
                await asyncio.sleep(0.5)
                
        except Exception as e:
            logger.error(f"Error sending file {i}: {e}")
            await message.answer(f"Error sending file {i+1}: {str(e)}")
    
    # Auto-delete logic
    if auto_delete_minutes > 0 and not is_owner_user:
        delete_time = auto_delete_minutes * 60  # Convert to seconds
        
        # Inform user about auto-delete
        info_msg = await message.answer(
            f"⚠️ These files will be automatically deleted in {auto_delete_minutes} minutes."
        )
        sent_messages.append(info_msg)
        
        # Schedule deletion
        asyncio.create_task(delete_messages_after_delay(sent_messages, delete_time))
    
    elif not is_owner_user:
        await message.answer("✅ All files have been sent. They will remain in this chat.")

async def delete_messages_after_delay(messages: List[types.Message], delay: int):
    """Delete messages after specified delay"""
    await asyncio.sleep(delay)
    
    for msg in messages:
        try:
            await msg.delete()
        except Exception as e:
            logger.error(f"Error deleting message: {e}")

# =============================================================================
# ERROR HANDLER
# =============================================================================

@dp.errors_handler()
async def errors_handler(update: types.Update, exception: Exception):
    """Handle errors"""
    logger.error(f"Update {update} caused error {exception}")
    return True

# =============================================================================
# WEBHOOK SETUP AND HEALTH CHECK
# =============================================================================

async def on_startup(dp):
    """Bot startup actions"""
    await Database.init_db()
    await setup_bot_commands()
    
    # Register middleware
    dp.middleware.setup(UserActivityMiddleware())
    
    # Set webhook for Render
    webhook_url = f"{RENDER_URL}/webhook"
    await bot.set_webhook(webhook_url)
    
    logger.info(f"Bot started with webhook: {webhook_url}")

async def on_shutdown(dp):
    """Bot shutdown actions"""
    await bot.delete_webhook()
    await dp.storage.close()
    await dp.storage.wait_closed()
    await bot.session.close()
    
    if db_pool:
        await db_pool.close()
    
    logger.info("Bot shutdown complete")

# Web application for Render
from aiohttp import web

async def health_check(request):
    """Health check endpoint for Render"""
    return web.Response(text="ok")

async def webhook_handler(request):
    """Webhook handler for aiogram"""
    url = str(request.url)
    index = url.find("/webhook")
    if index != -1:
        url = url[:index]
    
    if request.method == "POST":
        try:
            update_data = await request.json()
            update = types.Update(**update_data)
            await dp.process_update(update)
            return web.Response()
        except Exception as e:
            logger.error(f"Webhook error: {e}")
            return web.Response(status=500)
    else:
        return web.Response(status=405)

def main():
    """Main function"""
    if os.getenv('RENDER'):
        # Webhook mode for Render
        app = web.Application()
        app.router.add_get('/health', health_check)
        app.router.add_post('/webhook', webhook_handler)
        
        port = int(os.getenv('PORT', 8080))
        web.run_app(app, host='0.0.0.0', port=port)
    else:
        # Polling mode for development
        executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown, skip_updates=True)

if __name__ == '__main__':
    main()