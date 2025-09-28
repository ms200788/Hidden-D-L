#!/usr/bin/env python3
"""
Telegram File Sharing Bot - FINAL WORKING VERSION
A robust file sharing bot with deep links, auto-delete, and admin features.
"""

import os
import asyncio
import logging
import uuid
import json
from datetime import datetime
from typing import Dict, List, Optional

import aiohttp
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, 
    InlineKeyboardButton, ContentType
)
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
from aiogram.utils.exceptions import BotBlocked, ChatNotFound, RetryAfter

# Configuration
class Config:
    BOT_TOKEN = os.getenv('BOT_TOKEN', '8381804391:AAHKa0v35m6caF-N4mszqQe2DN3aMiQGMRY')
    OWNER_ID = int(os.getenv('OWNER_ID', '6169237879'))
    DATABASE_URL = os.getenv('DATABASE_URL')
    RENDER_EXTERNAL_URL = os.getenv('RENDER_EXTERNAL_URL', 'https://hidden-0g40.onrender.com')
    PORT = int(os.getenv('PORT', 5000))

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize bot and dispatcher
bot = Bot(token=Config.BOT_TOKEN, parse_mode='HTML')
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

# Set bot instance for context
Bot.set_current(bot)

# Database connection
db_pool = None

# States
class UploadStates(StatesGroup):
    waiting_for_files = State()
    waiting_for_options = State()

class MessageStates(StatesGroup):
    waiting_for_message_type = State()
    waiting_for_text = State()

# Database class
class Database:
    @staticmethod
    async def init_db():
        """Initialize database tables"""
        try:
            async with db_pool.acquire() as conn:
                # Drop and recreate tables
                await conn.execute('DROP TABLE IF EXISTS statistics, upload_sessions, messages, users CASCADE')
                
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
                
                await conn.execute('''
                    CREATE TABLE messages (
                        id SERIAL PRIMARY KEY,
                        message_type VARCHAR(50) UNIQUE NOT NULL,
                        text TEXT,
                        image_file_id VARCHAR(500),
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
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
                
                # Insert defaults
                await conn.execute('''
                    INSERT INTO messages (message_type, text) 
                    VALUES 
                        ('start', '👋 Welcome to File Sharing Bot!\\n\\nUse deep links to access shared files.'),
                        ('help', '📖 Help Guide:\\n\\n• Use /upload to share files\\n• Contact owner for file access')
                ''')
                
                await conn.execute('''
                    INSERT INTO statistics (total_users, active_users, files_uploaded, sessions_completed) 
                    VALUES (0, 0, 0, 0)
                ''')
                
                logger.info("Database initialized successfully")
                return True
        except Exception as e:
            logger.error(f"Database init error: {e}")
            return False

    @staticmethod
    async def execute(query, *args):
        """Execute database query"""
        try:
            async with db_pool.acquire() as conn:
                return await conn.execute(query, *args)
        except Exception as e:
            logger.error(f"Database error: {e}")
            return None

    @staticmethod
    async def fetchrow(query, *args):
        """Fetch single row"""
        try:
            async with db_pool.acquire() as conn:
                return await conn.fetchrow(query, *args)
        except Exception as e:
            logger.error(f"Database error: {e}")
            return None

    @staticmethod
    async def fetchval(query, *args):
        """Fetch single value"""
        try:
            async with db_pool.acquire() as conn:
                return await conn.fetchval(query, *args)
        except Exception as e:
            logger.error(f"Database error: {e}")
            return None

    @staticmethod
    async def create_user(user: types.User):
        """Create/update user"""
        await Database.execute('''
            INSERT INTO users (user_id, username, first_name, last_name) 
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id) DO UPDATE SET
            last_active = NOW()
        ''', user.id, user.username, user.first_name, user.last_name)

    @staticmethod
    async def update_activity(user_id: int):
        """Update user activity"""
        await Database.execute(
            'UPDATE users SET last_active = NOW() WHERE user_id = $1',
            user_id
        )

    @staticmethod
    async def get_message(message_type: str):
        """Get message content"""
        return await Database.fetchrow(
            'SELECT * FROM messages WHERE message_type = $1', message_type
        )

    @staticmethod
    async def update_message(message_type: str, text: str = None, image_file_id: str = None):
        """Update message"""
        if text:
            await Database.execute(
                'UPDATE messages SET text = $1, updated_at = NOW() WHERE message_type = $2',
                text, message_type
            )
        if image_file_id:
            await Database.execute(
                'UPDATE messages SET image_file_id = $1, updated_at = NOW() WHERE message_type = $2',
                image_file_id, message_type
            )

    @staticmethod
    async def create_session(owner_id: int, file_ids: List[str], captions: List[str], 
                           protect_content: bool, auto_delete: int) -> str:
        """Create upload session"""
        session_id = str(uuid.uuid4())[:8]
        success = await Database.execute('''
            INSERT INTO upload_sessions 
            (session_id, owner_id, file_ids, captions, protect_content, auto_delete_minutes)
            VALUES ($1, $2, $3, $4, $5, $6)
        ''', session_id, owner_id, json.dumps(file_ids), 
           json.dumps(captions), protect_content, auto_delete)
        
        if success:
            await Database.execute('''
                UPDATE statistics SET 
                files_uploaded = files_uploaded + $1,
                sessions_completed = sessions_completed + 1,
                last_updated = NOW()
            ''', len(file_ids))
            return session_id
        return None

    @staticmethod
    async def get_session(session_id: str):
        """Get upload session"""
        session = await Database.fetchrow(
            'SELECT * FROM upload_sessions WHERE session_id = $1 AND is_active = TRUE',
            session_id
        )
        if session:
            await Database.execute(
                'UPDATE upload_sessions SET access_count = access_count + 1 WHERE id = $1',
                session['id']
            )
        return session

    @staticmethod
    async def get_stats() -> Dict:
        """Get statistics"""
        try:
            total_users = await Database.fetchval('SELECT COUNT(*) FROM users') or 0
            active_users = await Database.fetchval('''
                SELECT COUNT(*) FROM users 
                WHERE last_active > NOW() - INTERVAL '48 hours'
            ''') or 0
            
            stats = await Database.fetchrow('''
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
            logger.error(f"Stats error: {e}")
            return {'total_users': 0, 'active_users': 0, 'files_uploaded': 0, 'sessions_completed': 0}

# Utility functions
def is_owner(user_id: int) -> bool:
    return user_id == Config.OWNER_ID

def create_session_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Protect", callback_data="protect_yes"),
        InlineKeyboardButton("❌ No Protect", callback_data="protect_no"),
        InlineKeyboardButton("0 min", callback_data="delete_0"),
        InlineKeyboardButton("60 min", callback_data="delete_60"),
        InlineKeyboardButton("1 day", callback_data="delete_1440"),
        InlineKeyboardButton("1 week", callback_data="delete_10080"),
        InlineKeyboardButton("🚀 Create", callback_data="create_session")
    )
    return keyboard

async def safe_send_message(chat_id: int, text: str, **kwargs):
    """Safely send message with error handling"""
    try:
        await bot.send_message(chat_id, text, **kwargs)
    except BotBlocked:
        logger.warning(f"Bot blocked by user {chat_id}")
    except ChatNotFound:
        logger.warning(f"Chat not found: {chat_id}")
    except RetryAfter as e:
        logger.warning(f"Rate limit. Retry after {e.timeout} seconds")
        await asyncio.sleep(e.timeout)
        return await safe_send_message(chat_id, text, **kwargs)
    except Exception as e:
        logger.error(f"Send message error: {e}")

# Basic command handlers
@dp.message_handler(commands=['start'])
async def cmd_start(message: Message):
    """Handle /start command"""
    try:
        user = message.from_user
        await Database.create_user(user)
        await Database.update_activity(user.id)
        
        # Check for deep link
        args = message.get_args()
        if args:
            await handle_deep_link(user.id, args, message.bot)
            return
        
        # Send welcome
        msg_data = await Database.get_message('start')
        text = msg_data['text'] if msg_data else "Welcome to File Sharing Bot!"
        
        # Remove inline buttons as requested - send plain message
        await message.answer(text)
        
    except Exception as e:
        logger.error(f"Start error: {e}")
        await message.answer("Error occurred. Try again.")

@dp.message_handler(commands=['help'])
async def cmd_help(message: Message):
    """Handle /help command"""
    try:
        user = message.from_user
        await Database.update_activity(user.id)
        
        msg_data = await Database.get_message('help')
        text = msg_data['text'] if msg_data else "Help information"
        
        # Remove inline buttons as requested - send plain message
        await message.answer(text)
        
    except Exception as e:
        logger.error(f"Help error: {e}")
        await message.answer("Error occurred.")

# Callback query handlers for session creation
@dp.callback_query_handler(lambda c: c.data.startswith('protect_') or c.data.startswith('delete_'), state=UploadStates.waiting_for_options)
async def options_callback(callback_query: CallbackQuery, state: FSMContext):
    """Handle session options"""
    try:
        data = await state.get_data()
        
        if callback_query.data.startswith('protect_'):
            protect = callback_query.data == 'protect_yes'
            await state.update_data(protect_content=protect)
            status = "Enabled" if protect else "Disabled"
            await callback_query.answer(f"Protect: {status}")
        
        elif callback_query.data.startswith('delete_'):
            minutes = int(callback_query.data.split('_')[1])
            await state.update_data(auto_delete=minutes)
            
            if minutes == 0:
                status = "No delete"
            elif minutes >= 1440:
                status = f"{minutes//1440} day(s)"
            elif minutes >= 60:
                status = f"{minutes//60} hour(s)"
            else:
                status = f"{minutes} min"
            
            await callback_query.answer(f"Delete: {status}")
    except Exception as e:
        logger.error(f"Options callback error: {e}")
        await callback_query.answer("Error")

@dp.callback_query_handler(lambda c: c.data == 'create_session', state=UploadStates.waiting_for_options)
async def create_session_callback(callback_query: CallbackQuery, state: FSMContext):
    """Create session callback"""
    try:
        data = await state.get_data()
        user = callback_query.from_user
        
        session_id = await Database.create_session(
            user.id, data['files'], data['captions'],
            data.get('protect_content', True), data.get('auto_delete', 0)
        )
        
        if session_id:
            link = f"https://t.me/{callback_query.message.bot.username}?start={session_id}"
            text = f"""
✅ Session Created!

• Files: {len(data['files'])}
• Protect: {'Yes' if data.get('protect_content', True) else 'No'}
• Auto-delete: {data.get('auto_delete', 0)} min

🔗 Share this link:
{link}
            """
            await callback_query.message.answer(text)
        else:
            await callback_query.message.answer("❌ Error creating session")
        
        await state.finish()
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Create session callback error: {e}")
        await callback_query.answer("Error")
        await state.finish()

# Owner commands
@dp.message_handler(commands=['stats'], user_id=Config.OWNER_ID)
async def stats_cmd(message: Message):
    """Stats command"""
    try:
        stats = await Database.get_stats()
        text = f"""
📊 Bot Statistics:

👥 Users: {stats['total_users']}
🟢 Active: {stats['active_users']}
📁 Files: {stats['files_uploaded']}
🔗 Sessions: {stats['sessions_completed']}
        """
        await message.answer(text)
    except Exception as e:
        logger.error(f"Stats error: {e}")
        await message.answer("Error getting statistics")

@dp.message_handler(commands=['upload'], user_id=Config.OWNER_ID)
async def upload_cmd(message: Message):
    """Upload command - start file upload process"""
    try:
        # Clear any existing state
        current_state = dp.current_state(chat=message.chat.id, user=message.from_user.id)
        if await current_state.get_state():
            await current_state.finish()
        
        await UploadStates.waiting_for_files.set()
        await current_state.update_data(files=[], captions=[])
        await message.answer(
            "📤 Upload started! Send files one by one.\n"
            "Use /done when finished or /cancel to abort."
        )
    except Exception as e:
        logger.error(f"Upload error: {e}")
        await message.answer("Error starting upload")

@dp.message_handler(commands=['done'], state=UploadStates.waiting_for_files, user_id=Config.OWNER_ID)
async def done_cmd(message: Message, state: FSMContext):
    """Done command"""
    try:
        data = await state.get_data()
        files = data.get('files', [])
        
        if not files:
            await message.answer("No files received. Cancelled.")
            await state.finish()
            return
        
        await UploadStates.waiting_for_options.set()
        await message.answer(
            f"📁 Files received: {len(files)}\nConfigure sharing options:",
            reply_markup=create_session_keyboard()
        )
    except Exception as e:
        logger.error(f"Done error: {e}")
        await state.finish()
        await message.answer("Error")

@dp.message_handler(commands=['cancel'], state='*', user_id=Config.OWNER_ID)
async def cancel_cmd(message: Message, state: FSMContext):
    """Cancel command"""
    try:
        current_state = await state.get_state()
        if current_state:
            await state.finish()
        await message.answer("✅ Operation cancelled.")
    except Exception as e:
        logger.error(f"Cancel error: {e}")
        await message.answer("Error")

# File handler
@dp.message_handler(content_types=[ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT], 
                   state=UploadStates.waiting_for_files, user_id=Config.OWNER_ID)
async def file_handler(message: Message, state: FSMContext):
    """Handle file uploads"""
    try:
        data = await state.get_data()
        files = data.get('files', [])
        captions = data.get('captions', [])
        
        file_id = None
        if message.photo:
            file_id = message.photo[-1].file_id
        elif message.video:
            file_id = message.video.file_id
        elif message.document:
            file_id = message.document.file_id
        
        if file_id:
            files.append(file_id)
            captions.append(message.caption or "")
            await state.update_data(files=files, captions=captions)
            await message.answer(f"✅ File {len(files)} stored. Send more or use /done when finished.")
    except Exception as e:
        logger.error(f"File handler error: {e}")
        await message.answer("Error storing file.")

# Message update handlers (removed setmessage/setimage commands as requested)
# Users will set these via BotFather

# Deep link handler
async def handle_deep_link(user_id: int, session_id: str, bot_instance: Bot):
    """Handle deep link access"""
    try:
        session = await Database.get_session(session_id)
        if not session:
            await safe_send_message(user_id, "❌ Session not found or expired")
            return
        
        file_ids = json.loads(session['file_ids'])
        captions = json.loads(session['captions'])
        protect = session['protect_content']
        auto_delete = session['auto_delete_minutes']
        
        # Owner bypass
        if user_id == Config.OWNER_ID:
            protect = False
        
        # Send files
        for i, file_id in enumerate(file_ids):
            caption = captions[i] if i < len(captions) else ""
            
            try:
                # Try to detect file type and send appropriately
                if file_id.startswith('AgAC'):  # Photo file ID pattern
                    msg = await bot_instance.send_photo(user_id, file_id, caption=caption, protect_content=protect)
                elif file_id.startswith('BAAC'):  # Video file ID pattern  
                    msg = await bot_instance.send_video(user_id, file_id, caption=caption, protect_content=protect)
                else:
                    msg = await bot_instance.send_document(user_id, file_id, caption=caption, protect_content=protect)
                
                # Schedule deletion if needed
                if auto_delete > 0 and user_id != Config.OWNER_ID:
                    asyncio.create_task(delete_after(msg.chat.id, msg.message_id, auto_delete * 60))
                    
            except Exception as e:
                logger.error(f"Send file error: {e}")
                continue
        
        # Notify about auto-delete
        if auto_delete > 0 and user_id != Config.OWNER_ID:
            if auto_delete >= 1440:
                time_str = f"{auto_delete//1440} day(s)"
            elif auto_delete >= 60:
                time_str = f"{auto_delete//60} hour(s)"
            else:
                time_str = f"{auto_delete} minute(s)"
            
            notice = await bot_instance.send_message(user_id, f"⚠️ Files will auto-delete in {time_str}")
            if auto_delete > 0:
                asyncio.create_task(delete_after(notice.chat.id, notice.message_id, auto_delete * 60))
    except Exception as e:
        logger.error(f"Deep link error: {e}")
        await safe_send_message(user_id, "Error accessing files.")

async def delete_after(chat_id: int, message_id: int, delay: int):
    """Delete message after delay"""
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception as e:
        logger.error(f"Delete error: {e}")

# Error handler
@dp.errors_handler()
async def error_handler(update, exception):
    """Global error handler"""
    logger.error(f"Update: {update} caused error: {exception}")
    return True

# Web server for Render
from aiohttp import web

async def health_check(request):
    return web.Response(text="OK")

async def webhook_handler(request):
    try:
        token = request.path.split('/')[-1]
        if token == Config.BOT_TOKEN:
            data = await request.json()
            update = types.Update(**data)
            await dp.process_update(update)
            return web.Response()
        return web.Response(status=403)
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return web.Response(status=500)

async def on_startup(app):
    """Initialize on startup"""
    global db_pool
    try:
        # Database connection with retry
        max_retries = 5
        for i in range(max_retries):
            try:
                db_pool = await asyncpg.create_pool(Config.DATABASE_URL, min_size=1, max_size=10)
                if await Database.init_db():
                    logger.info("Database initialized successfully")
                    break
                else:
                    raise Exception("Database initialization failed")
            except Exception as e:
                if i < max_retries - 1:
                    logger.warning(f"Database connection attempt {i+1} failed, retrying...")
                    await asyncio.sleep(2)
                else:
                    raise e
        
        # Webhook setup
        if Config.RENDER_EXTERNAL_URL:
            webhook_url = f"{Config.RENDER_EXTERNAL_URL}/webhook/{Config.BOT_TOKEN}"
            await bot.set_webhook(webhook_url)
            logger.info(f"Webhook set: {webhook_url}")
        
        logger.info("Bot started successfully")
    except Exception as e:
        logger.error(f"Startup error: {e}")

async def on_shutdown(app):
    """Cleanup on shutdown"""
    if db_pool:
        await db_pool.close()
    await bot.session.close()
    logger.info("Bot stopped")

def main():
    """Main function"""
    # Validate required environment variables
    if not Config.BOT_TOKEN:
        logger.error("Missing BOT_TOKEN environment variable")
        return
    if not Config.OWNER_ID:
        logger.error("Missing OWNER_ID environment variable")
        return
    if not Config.DATABASE_URL:
        logger.error("Missing DATABASE_URL environment variable")
        return
    
    # Create web app
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    app.router.add_post(f'/webhook/{Config.BOT_TOKEN}', webhook_handler)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    
    # Start server
    web.run_app(app, host='0.0.0.0', port=Config.PORT)

if __name__ == '__main__':
    main()