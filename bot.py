#!/usr/bin/env python3
"""
Telegram File Sharing Bot - COMPLETELY FIXED VERSION
A robust file sharing bot with deep links, auto-delete, and admin features.
Built with aiogram v2.25.1 for maximum stability.
"""

import os
import asyncio
import logging
import uuid
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.utils import executor
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, 
    InlineKeyboardButton, ContentType
)
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup

# Configuration
class Config:
    BOT_TOKEN = os.getenv('BOT_TOKEN', '8381804391:AAHKa0v35m6caF-N4mszqQe2DN3aMiQGMRY')
    OWNER_ID = int(os.getenv('OWNER_ID', '6169237879'))
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://username:password@host/database')
    UPLOAD_CHANNEL = os.getenv('UPLOAD_CHANNEL', '')
    RENDER_EXTERNAL_URL = os.getenv('RENDER_EXTERNAL_URL', 'https://hidden-0g40.onrender.com')
    PORT = int(os.getenv('PORT', 5000))

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize bot and dispatcher with current context
bot = Bot(token=Config.BOT_TOKEN, parse_mode='HTML')
Bot.set_current(bot)  # Fix the context issue
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

# Database connection pool
db_pool = None

# States for conversation handlers
class UploadStates(StatesGroup):
    waiting_for_files = State()
    waiting_for_options = State()
    waiting_for_broadcast = State()

class MessageStates(StatesGroup):
    waiting_for_message_type = State()
    waiting_for_text = State()

# Enhanced Database class with proper schema handling
class Database:
    @staticmethod
    async def init_db():
        """Initialize database tables with proper error handling"""
        try:
            async with db_pool.acquire() as conn:
                # Drop and recreate all tables to ensure clean schema
                await conn.execute('DROP TABLE IF EXISTS statistics, upload_sessions, messages, users CASCADE')
                
                # Create users table with correct schema
                await conn.execute('''
                    CREATE TABLE users (
                        id BIGSERIAL PRIMARY KEY,
                        user_id BIGINT UNIQUE NOT NULL,
                        username VARCHAR(255),
                        first_name VARCHAR(255),
                        last_name VARCHAR(255),
                        join_date TIMESTAMP DEFAULT NOW(),
                        last_active TIMESTAMP DEFAULT NOW(),
                        is_banned BOOLEAN DEFAULT FALSE
                    )
                ''')
                
                # Create messages table
                await conn.execute('''
                    CREATE TABLE messages (
                        id SERIAL PRIMARY KEY,
                        message_type VARCHAR(50) UNIQUE NOT NULL,
                        text TEXT,
                        image_file_id VARCHAR(500),
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
                # Create upload sessions table
                await conn.execute('''
                    CREATE TABLE upload_sessions (
                        id SERIAL PRIMARY KEY,
                        session_id VARCHAR(100) UNIQUE NOT NULL,
                        owner_id BIGINT NOT NULL,
                        file_ids JSONB NOT NULL,
                        captions JSONB,
                        protect_content BOOLEAN DEFAULT TRUE,
                        auto_delete_minutes INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT NOW(),
                        access_count INTEGER DEFAULT 0,
                        is_active BOOLEAN DEFAULT TRUE
                    )
                ''')
                
                # Create statistics table
                await conn.execute('''
                    CREATE TABLE statistics (
                        id SERIAL PRIMARY KEY,
                        total_users INTEGER DEFAULT 0,
                        active_users INTEGER DEFAULT 0,
                        files_uploaded INTEGER DEFAULT 0,
                        sessions_completed INTEGER DEFAULT 0,
                        last_updated TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
                # Insert default messages
                await conn.execute('''
                    INSERT INTO messages (message_type, text) 
                    VALUES 
                        ('start', '👋 Welcome to File Sharing Bot!\\n\\nUse deep links to access shared files.'),
                        ('help', '📖 Help Guide:\\n\\n• Use /start to begin\\n• Contact owner for file access')
                ''')
                
                # Insert initial statistics
                await conn.execute('''
                    INSERT INTO statistics (total_users, active_users, files_uploaded, sessions_completed) 
                    VALUES (0, 0, 0, 0)
                ''')
                
                logger.info("Database initialized successfully")
                return True
                
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            return False

    @staticmethod
    async def safe_execute(query, *args):
        """Safely execute database query with error handling"""
        try:
            async with db_pool.acquire() as conn:
                result = await conn.execute(query, *args)
                logger.debug(f"Query executed: {query}")
                return result
        except Exception as e:
            logger.error(f"Database query error: {e}, Query: {query}")
            return None

    @staticmethod
    async def safe_fetchrow(query, *args):
        """Safely fetch single database row with error handling"""
        try:
            async with db_pool.acquire() as conn:
                result = await conn.fetchrow(query, *args)
                return result
        except Exception as e:
            logger.error(f"Database fetchrow error: {e}, Query: {query}")
            return None

    @staticmethod
    async def safe_fetchval(query, *args):
        """Safely fetch single value from database with error handling"""
        try:
            async with db_pool.acquire() as conn:
                result = await conn.fetchval(query, *args)
                return result
        except Exception as e:
            logger.error(f"Database fetchval error: {e}, Query: {query}")
            return None

    @staticmethod
    async def get_user(user_id: int):
        """Get user from database"""
        return await Database.safe_fetchrow(
            'SELECT * FROM users WHERE user_id = $1', user_id
        )

    @staticmethod
    async def create_user(user: types.User):
        """Create new user in database"""
        await Database.safe_execute('''
            INSERT INTO users (user_id, username, first_name, last_name) 
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id) DO UPDATE SET
            last_active = NOW()
        ''', user.id, user.username, user.first_name, user.last_name)

    @staticmethod
    async def update_user_activity(user_id: int):
        """Update user's last activity timestamp"""
        await Database.safe_execute(
            'UPDATE users SET last_active = NOW() WHERE user_id = $1',
            user_id
        )

    @staticmethod
    async def get_message(message_type: str):
        """Get message content from database"""
        return await Database.safe_fetchrow(
            'SELECT * FROM messages WHERE message_type = $1', message_type
        )

    @staticmethod
    async def update_message(message_type: str, text: str = None, image_file_id: str = None):
        """Update message content in database"""
        if text and image_file_id:
            await Database.safe_execute(
                'UPDATE messages SET text = $1, image_file_id = $2, updated_at = NOW() WHERE message_type = $3',
                text, image_file_id, message_type
            )
        elif text:
            await Database.safe_execute(
                'UPDATE messages SET text = $1, updated_at = NOW() WHERE message_type = $2',
                text, message_type
            )
        elif image_file_id:
            await Database.safe_execute(
                'UPDATE messages SET image_file_id = $1, updated_at = NOW() WHERE message_type = $2',
                image_file_id, message_type
            )

    @staticmethod
    async def create_upload_session(owner_id: int, file_ids: List[str], captions: List[str], 
                                  protect_content: bool, auto_delete_minutes: int) -> str:
        """Create new upload session and return session ID"""
        try:
            session_id = str(uuid.uuid4())[:8]
            success = await Database.safe_execute('''
                INSERT INTO upload_sessions 
                (session_id, owner_id, file_ids, captions, protect_content, auto_delete_minutes)
                VALUES ($1, $2, $3, $4, $5, $6)
            ''', session_id, owner_id, json.dumps(file_ids), 
               json.dumps(captions), protect_content, auto_delete_minutes)
            
            if success:
                # Update statistics
                await Database.safe_execute('''
                    UPDATE statistics SET 
                    files_uploaded = files_uploaded + $1,
                    sessions_completed = sessions_completed + 1,
                    last_updated = NOW()
                ''', len(file_ids))
                
                return session_id
            return None
        except Exception as e:
            logger.error(f"Error creating upload session: {e}")
            return None

    @staticmethod
    async def get_upload_session(session_id: str):
        """Get upload session by ID"""
        session = await Database.safe_fetchrow(
            'SELECT * FROM upload_sessions WHERE session_id = $1 AND is_active = TRUE',
            session_id
        )
        if session:
            # Update access count
            await Database.safe_execute(
                'UPDATE upload_sessions SET access_count = access_count + 1 WHERE id = $1',
                session['id']
            )
        return session

    @staticmethod
    async def get_statistics() -> Dict:
        """Get bot statistics"""
        try:
            total_users = await Database.safe_fetchval('SELECT COUNT(*) FROM users') or 0
            
            active_users = await Database.safe_fetchval('''
                SELECT COUNT(*) FROM users 
                WHERE last_active > NOW() - INTERVAL '48 hours'
            ''') or 0
            
            stats = await Database.safe_fetchrow('''
                SELECT files_uploaded, sessions_completed FROM statistics 
                ORDER BY id DESC LIMIT 1
            ''')
            
            return {
                'total_users': total_users,
                'active_users': active_users,
                'files_uploaded': stats['files_uploaded'] if stats else 0,
                'sessions_completed': stats['sessions_completed'] if stats else 0
            }
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {'total_users': 0, 'active_users': 0, 'files_uploaded': 0, 'sessions_completed': 0}

    @staticmethod
    async def get_all_users() -> List[int]:
        """Get all user IDs for broadcasting"""
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch('SELECT user_id FROM users WHERE is_banned = FALSE')
                return [row['user_id'] for row in rows]
        except Exception as e:
            logger.error(f"Error getting users: {e}")
            return []

# Utility functions
class Utilities:
    @staticmethod
    def is_owner(user_id: int) -> bool:
        """Check if user is owner"""
        return user_id == Config.OWNER_ID

    @staticmethod
    async def send_message_safe(chat_id: int, text: str, **kwargs):
        """Safely send message with error handling"""
        try:
            await bot.send_message(chat_id, text, **kwargs)
            return True
        except Exception as e:
            logger.error(f"Error sending message to {chat_id}: {e}")
            return False

    @staticmethod
    def create_session_keyboard() -> InlineKeyboardMarkup:
        """Create keyboard for session options"""
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("✅ Protect Content", callback_data="protect_yes"),
            InlineKeyboardButton("❌ No Protection", callback_data="protect_no"),
            InlineKeyboardButton("0 min (No delete)", callback_data="delete_0"),
            InlineKeyboardButton("60 min", callback_data="delete_60"),
            InlineKeyboardButton("1440 min (1 day)", callback_data="delete_1440"),
            InlineKeyboardButton("10080 min (1 week)", callback_data="delete_10080"),
            InlineKeyboardButton("🚀 Create Session", callback_data="create_session")
        )
        return keyboard

    @staticmethod
    def create_message_type_keyboard() -> InlineKeyboardMarkup:
        """Create keyboard for message type selection"""
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("📝 Start Message", callback_data="msg_start"),
            InlineKeyboardButton("ℹ️ Help Message", callback_data="msg_help")
        )
        return keyboard

# Message handlers
@dp.message_handler(commands=['start'])
async def cmd_start(message: Message):
    """Handle /start command with deep link support"""
    try:
        user_id = message.from_user.id
        
        # Update user in database
        await Database.create_user(message.from_user)
        await Database.update_user_activity(user_id)
        
        # Check for deep link
        args = message.get_args()
        if args:
            await handle_deep_link(user_id, args)
            return
        
        # Send welcome message
        msg_data = await Database.get_message('start')
        text = msg_data['text'] if msg_data else "Welcome to File Sharing Bot!"
        
        if msg_data and msg_data.get('image_file_id'):
            try:
                await message.answer_photo(
                    msg_data['image_file_id'],
                    text,
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("Help", callback_data="help_button")
                    )
                )
                return
            except Exception as e:
                logger.error(f"Error sending start image: {e}")
        
        # Fallback to text message
        await message.answer(
            text,
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("Help", callback_data="help_button")
            )
        )
    except Exception as e:
        logger.error(f"Error in /start: {e}")
        await message.answer("An error occurred. Please try again.")

@dp.message_handler(commands=['help'])
async def cmd_help(message: Message):
    """Handle /help command"""
    try:
        user_id = message.from_user.id
        await Database.update_user_activity(user_id)
        
        msg_data = await Database.get_message('help')
        text = msg_data['text'] if msg_data else "Help information"
        
        if msg_data and msg_data.get('image_file_id'):
            try:
                await message.answer_photo(
                    msg_data['image_file_id'],
                    text,
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("Back to Start", callback_data="back_start")
                    )
                )
                return
            except Exception as e:
                logger.error(f"Error sending help image: {e}")
        
        # Fallback to text message
        await message.answer(
            text,
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("Back to Start", callback_data="back_start")
            )
        )
    except Exception as e:
        logger.error(f"Error in /help: {e}")
        await message.answer("An error occurred. Please try again.")

# Owner-only commands
@dp.message_handler(commands=['setmessage'], user_id=Config.OWNER_ID)
async def cmd_setmessage(message: Message):
    """Set start/help messages"""
    try:
        await MessageStates.waiting_for_message_type.set()
        await message.answer(
            "Select which message you want to set:",
            reply_markup=Utilities.create_message_type_keyboard()
        )
    except Exception as e:
        logger.error(f"Error in /setmessage: {e}")
        await message.answer("An error occurred. Please try again.")

@dp.message_handler(commands=['setimage'], user_id=Config.OWNER_ID)
async def cmd_setimage(message: Message):
    """Set start/help images"""
    try:
        if not message.reply_to_message or not message.reply_to_message.photo:
            await message.answer("Please reply to an image with this command.")
            return
        
        file_id = message.reply_to_message.photo[-1].file_id
        
        await MessageStates.waiting_for_message_type.set()
        await message.answer(
            "Select which image you want to set:",
            reply_markup=Utilities.create_message_type_keyboard()
        )
    except Exception as e:
        logger.error(f"Error in /setimage: {e}")
        await message.answer("An error occurred. Please try again.")

@dp.message_handler(commands=['stats'], user_id=Config.OWNER_ID)
async def cmd_stats(message: Message):
    """Show bot statistics"""
    try:
        stats = await Database.get_statistics()
        
        text = f"""
📊 <b>Bot Statistics</b>

👥 Total Users: <code>{stats['total_users']}</code>
🟢 Active Users (48h): <code>{stats['active_users']}</code>
📁 Files Uploaded: <code>{stats['files_uploaded']}</code>
🔄 Sessions Completed: <code>{stats['sessions_completed']}</code>

Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        
        await message.answer(text)
    except Exception as e:
        logger.error(f"Error in /stats: {e}")
        await message.answer("An error occurred. Please try again.")

@dp.message_handler(commands=['broadcast'], user_id=Config.OWNER_ID)
async def cmd_broadcast(message: Message):
    """Start broadcast process"""
    try:
        await UploadStates.waiting_for_broadcast.set()
        await message.answer(
            "Please send the message you want to broadcast to all users. "
            "You can include text, photos, videos, or documents."
        )
    except Exception as e:
        logger.error(f"Error in /broadcast: {e}")
        await message.answer("An error occurred. Please try again.")

@dp.message_handler(commands=['upload'], user_id=Config.OWNER_ID)
async def cmd_upload(message: Message):
    """Start upload process"""
    try:
        await UploadStates.waiting_for_files.set()
        await message.answer(
            "📤 <b>Upload Session Started</b>\n\n"
            "Please send files one by one (photos, videos, documents).\n"
            "When finished, send <code>/d</code> to complete or <code>/c</code> to cancel.\n\n"
            "Supported formats:\n"
            "• Images (JPEG, PNG, etc.)\n"
            "• Videos (MP4, etc.)\n"
            "• Documents (PDF, ZIP, etc.)"
        )
    except Exception as e:
        logger.error(f"Error in /upload: {e}")
        await message.answer("An error occurred. Please try again.")

@dp.message_handler(commands=['d', 'done'], state=UploadStates.waiting_for_files, user_id=Config.OWNER_ID)
async def cmd_done(message: Message, state: FSMContext):
    """Finish file upload and proceed to options"""
    try:
        data = await state.get_data()
        files = data.get('files', [])
        
        if not files:
            await message.answer("No files received. Session cancelled.")
            await state.finish()
            return
        
        await UploadStates.waiting_for_options.set()
        await message.answer(
            f"📁 <b>Files Received:</b> {len(files)}\n\n"
            "Configure session options:\n\n"
            "🛡️ <b>Protect Content:</b> Prevents forwarding/saving\n"
            "⏰ <b>Auto-delete:</b> Files auto-delete after specified time\n\n"
            "Select options below:",
            reply_markup=Utilities.create_session_keyboard()
        )
    except Exception as e:
        logger.error(f"Error in /done: {e}")
        await state.finish()
        await message.answer("An error occurred. Session cancelled.")

@dp.message_handler(commands=['c', 'cancel'], state='*', user_id=Config.OWNER_ID)
async def cmd_cancel(message: Message, state: FSMContext):
    """Cancel current operation"""
    try:
        await state.finish()
        await message.answer("Operation cancelled.")
    except Exception as e:
        logger.error(f"Error in /cancel: {e}")

# File handling
@dp.message_handler(content_types=[ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT], 
                   state=UploadStates.waiting_for_files, user_id=Config.OWNER_ID)
async def handle_files(message: Message, state: FSMContext):
    """Handle file uploads during upload session"""
    try:
        data = await state.get_data()
        files = data.get('files', [])
        captions = data.get('captions', [])
        
        file_id = None
        file_type = None
        
        if message.photo:
            file_id = message.photo[-1].file_id
            file_type = 'photo'
        elif message.video:
            file_id = message.video.file_id
            file_type = 'video'
        elif message.document:
            file_id = message.document.file_id
            file_type = 'document'
        
        if file_id:
            files.append(file_id)
            captions.append(message.caption or "")
            
            await state.update_data(files=files, captions=captions)
            
            await message.answer(f"✅ File {len(files)} received and stored.")
    except Exception as e:
        logger.error(f"Error handling file: {e}")
        await message.answer("Error storing file. Please try again.")

# Callback query handlers
@dp.callback_query_handler(lambda c: c.data in ['msg_start', 'msg_help'], state=MessageStates.waiting_for_message_type)
async def process_message_type(callback_query: CallbackQuery, state: FSMContext):
    """Process message type selection"""
    try:
        message_type = 'start' if callback_query.data == 'msg_start' else 'help'
        
        # Check if we're setting text or image
        message = callback_query.message
        if message.reply_to_message and message.reply_to_message.photo:
            # Setting image
            file_id = message.reply_to_message.photo[-1].file_id
            await Database.update_message(message_type, image_file_id=file_id)
            await callback_query.message.answer(f"✅ {message_type.capitalize()} image updated successfully!")
        else:
            # Setting text
            await state.update_data(message_type=message_type)
            await MessageStates.waiting_for_text.set()
            await callback_query.message.answer(
                f"Please send the new text for the {message_type} message:"
            )
        
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error processing message type: {e}")
        await callback_query.answer("Error updating message.")

@dp.callback_query_handler(lambda c: c.data.startswith('protect_') or c.data.startswith('delete_'), 
                          state=UploadStates.waiting_for_options)
async def process_session_options(callback_query: CallbackQuery, state: FSMContext):
    """Process session configuration options"""
    try:
        data = await state.get_data()
        
        if callback_query.data.startswith('protect_'):
            protect_content = callback_query.data == 'protect_yes'
            await state.update_data(protect_content=protect_content)
            status = "✅ Enabled" if protect_content else "❌ Disabled"
            await callback_query.answer(f"Protect Content: {status}")
        
        elif callback_query.data.startswith('delete_'):
            delete_minutes = int(callback_query.data.split('_')[1])
            await state.update_data(auto_delete_minutes=delete_minutes)
            
            if delete_minutes == 0:
                status = "Disabled"
            else:
                hours = delete_minutes // 60
                days = hours // 24
                if days > 0:
                    status = f"{days} day(s)"
                elif hours > 0:
                    status = f"{hours} hour(s)"
                else:
                    status = f"{delete_minutes} minute(s)"
            
            await callback_query.answer(f"Auto-delete: {status}")
    except Exception as e:
        logger.error(f"Error processing session options: {e}")
        await callback_query.answer("Error updating options.")

@dp.callback_query_handler(lambda c: c.data == 'create_session', state=UploadStates.waiting_for_options)
async def create_session_final(callback_query: CallbackQuery, state: FSMContext):
    """Create the final upload session"""
    try:
        data = await state.get_data()
        
        session_id = await Database.create_upload_session(
            owner_id=callback_query.from_user.id,
            file_ids=data['files'],
            captions=data['captions'],
            protect_content=data.get('protect_content', True),
            auto_delete_minutes=data.get('auto_delete_minutes', 0)
        )
        
        if session_id:
            deep_link = f"https://t.me/{callback_query.message.bot.username}?start={session_id}"
            
            text = f"""
✅ <b>Upload Session Created Successfully!</b>

📁 Files: <code>{len(data['files'])}</code>
🛡️ Protect Content: <code>{'Yes' if data.get('protect_content', True) else 'No'}</code>
⏰ Auto-delete: <code>{data.get('auto_delete_minutes', 0)} minutes</code>

🔗 <b>Deep Link:</b>
<code>{deep_link}</code>

Share this link with users to access the files.
            """
            
            await callback_query.message.answer(text)
        else:
            await callback_query.message.answer("❌ Error creating session. Please try again.")
        
        await state.finish()
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error creating session: {e}")
        await callback_query.message.answer("Error creating session. Please try again.")
        await state.finish()

@dp.callback_query_handler(lambda c: c.data == 'help_button')
async def help_button(callback_query: CallbackQuery):
    """Handle help button click"""
    try:
        await cmd_help(callback_query.message)
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error in help button: {e}")
        await callback_query.answer("Error loading help.")

@dp.callback_query_handler(lambda c: c.data == 'back_start')
async def back_start_button(callback_query: CallbackQuery):
    """Handle back to start button click"""
    try:
        await cmd_start(callback_query.message)
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error in back button: {e}")
        await callback_query.answer("Error going back.")

# Text message handler for message setting
@dp.message_handler(state=MessageStates.waiting_for_text)
async def process_message_text(message: Message, state: FSMContext):
    """Process new message text"""
    try:
        data = await state.get_data()
        message_type = data.get('message_type')
        
        if message_type:
            await Database.update_message(message_type, text=message.text)
            await message.answer(f"✅ {message_type.capitalize()} message updated successfully!")
        
        await state.finish()
    except Exception as e:
        logger.error(f"Error updating message text: {e}")
        await message.answer("Error updating message.")
        await state.finish()

# Deep link handling
async def handle_deep_link(user_id: int, session_id: str):
    """Handle deep link access to upload sessions"""
    try:
        session = await Database.get_upload_session(session_id)
        
        if not session:
            await Utilities.send_message_safe(user_id, "❌ Session not found or expired.")
            return
        
        file_ids = json.loads(session['file_ids'])
        captions = json.loads(session['captions'])
        protect_content = session['protect_content']
        auto_delete = session['auto_delete_minutes']
        
        # Owner bypass for protect content
        if user_id == Config.OWNER_ID:
            protect_content = False
        
        # Send files to user
        for i, file_id in enumerate(file_ids):
            caption = captions[i] if i < len(captions) else ""
            
            try:
                # Determine file type and send appropriately
                if file_id.startswith('AgAC'):  # Photo file_id pattern
                    msg = await bot.send_photo(
                        user_id, file_id, caption=caption, 
                        protect_content=protect_content
                    )
                elif file_id.startswith('BAAC'):  # Video file_id pattern
                    msg = await bot.send_video(
                        user_id, file_id, caption=caption,
                        protect_content=protect_content
                    )
                else:  # Document
                    msg = await bot.send_document(
                        user_id, file_id, caption=caption,
                        protect_content=protect_content
                    )
                
                # Schedule auto-delete if enabled and user is not owner
                if auto_delete > 0 and user_id != Config.OWNER_ID:
                    asyncio.create_task(
                        delete_message_after_delay(msg.chat.id, msg.message_id, auto_delete * 60)
                    )
                    
            except Exception as e:
                logger.error(f"Error sending file {i}: {e}")
                continue
        
        # Notify user about auto-delete
        if auto_delete > 0 and user_id != Config.OWNER_ID:
            if auto_delete >= 1440:
                time_str = f"{auto_delete // 1440} day(s)"
            elif auto_delete >= 60:
                time_str = f"{auto_delete // 60} hour(s)"
            else:
                time_str = f"{auto_delete} minute(s)"
            
            notice_msg = await bot.send_message(
                user_id,
                f"⚠️ These files will be automatically deleted in {time_str}."
            )
            
            # Schedule deletion of notice
            asyncio.create_task(
                delete_message_after_delay(notice_msg.chat.id, notice_msg.message_id, auto_delete * 60)
            )
    except Exception as e:
        logger.error(f"Error handling deep link: {e}")
        await Utilities.send_message_safe(user_id, "Error accessing files. Please try again.")

async def delete_message_after_delay(chat_id: int, message_id: int, delay: int):
    """Delete message after specified delay in seconds"""
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception as e:
        logger.error(f"Error deleting message: {e}")

# Broadcast handling
@dp.message_handler(state=UploadStates.waiting_for_broadcast, user_id=Config.OWNER_ID)
async def handle_broadcast(message: Message, state: FSMContext):
    """Handle broadcast message"""
    try:
        users = await Database.get_all_users()
        success_count = 0
        
        await message.answer(f"📢 Broadcasting to {len(users)} users...")
        
        for user_id in users:
            try:
                if message.photo:
                    await bot.send_photo(user_id, message.photo[-1].file_id, 
                                       caption=message.caption, parse_mode='HTML')
                elif message.video:
                    await bot.send_video(user_id, message.video.file_id,
                                       caption=message.caption, parse_mode='HTML')
                elif message.document:
                    await bot.send_document(user_id, message.document.file_id,
                                          caption=message.caption, parse_mode='HTML')
                else:
                    await bot.send_message(user_id, message.text, parse_mode='HTML')
                
                success_count += 1
                await asyncio.sleep(0.1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error broadcasting to {user_id}: {e}")
                continue
        
        await state.finish()
        await message.answer(f"✅ Broadcast completed!\nSuccessfully sent to: {success_count}/{len(users)} users")
    except Exception as e:
        logger.error(f"Error in broadcast: {e}")
        await state.finish()
        await message.answer("Error during broadcast.")

# Error handler
@dp.errors_handler()
async def errors_handler(update: types.Update, exception: Exception):
    """Global error handler"""
    logger.error(f"Update {update} caused error: {exception}")
    return True

# Health check endpoint for Render
from aiohttp import web

async def health_check(request):
    """Health check endpoint for Render"""
    return web.Response(text="OK")

async def webhook_handler(request):
    """Handle Telegram webhook requests"""
    try:
        url = str(request.url)
        token = url.split('/')[-1]
        
        if token == Config.BOT_TOKEN:
            update_data = await request.json()
            update = types.Update(**update_data)
            await dp.process_update(update)
            return web.Response()
        else:
            return web.Response(status=403)
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return web.Response(status=500)

async def on_startup(app):
    """Initialize bot on startup"""
    global db_pool
    
    try:
        # Initialize database connection
        db_pool = await asyncpg.create_pool(
            Config.DATABASE_URL,
            min_size=1,
            max_size=10,
            timeout=30
        )
        
        # Initialize database
        success = await Database.init_db()
        if not success:
            logger.error("Failed to initialize database")
            # Continue anyway - the bot might work with limited functionality
        
        # Set webhook for Render
        if Config.RENDER_EXTERNAL_URL:
            webhook_url = f"{Config.RENDER_EXTERNAL_URL}/webhook/{Config.BOT_TOKEN}"
            await bot.set_webhook(webhook_url)
            logger.info(f"Webhook set to: {webhook_url}")
        else:
            logger.warning("RENDER_EXTERNAL_URL not set, webhook not configured")
        
        logger.info("Bot started successfully")
        
    except Exception as e:
        logger.error(f"Startup error: {e}")

async def on_shutdown(app):
    """Cleanup on shutdown"""
    if db_pool:
        await db_pool.close()
    await bot.session.close()
    logger.info("Bot shutdown complete")

def main():
    """Main application entry point"""
    # Validate configuration
    if not Config.BOT_TOKEN:
        logger.error("BOT_TOKEN environment variable is required")
        return
    
    if not Config.OWNER_ID:
        logger.error("OWNER_ID environment variable is required")
        return
    
    # Create aiohttp application
    app = web.Application()
    app.router.add_get('/health', health_check)
    app.router.add_post(f'/webhook/{Config.BOT_TOKEN}', webhook_handler)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    
    # Start server
    logger.info(f"Starting server on port {Config.PORT}")
    web.run_app(app, host='0.0.0.0', port=Config.PORT)

if __name__ == '__main__':
    main()