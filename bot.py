import logging, os, asyncio, asyncpg, aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.utils.executor import set_webhook
from aiogram.dispatcher.webhook import WebhookRequestHandler
from aiohttp import web

# ----------------------
# Logging setup
# ----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------
# Config
# ----------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")

if not BOT_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_URL:
    raise RuntimeError("Missing required environment variables.")

# ----------------------
# Bot & Dispatcher
# ----------------------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# ----------------------
# Database
# ----------------------
async def create_pool():
    return await asyncpg.create_pool(DATABASE_URL)

async def init_db(pool):
    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            joined TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """)
        logger.info("DB initialized")

# ----------------------
# Webhook startup/shutdown
# ----------------------
async def on_startup(app: web.Application):
    pool = await create_pool()
    app["db"] = pool
    await init_db(pool)

    webhook_url = f"{RENDER_EXTERNAL_URL}/webhook/{BOT_TOKEN}"
    await bot.delete_webhook()
    await bot.set_webhook(webhook_url)
    logger.info(f"Webhook set to {webhook_url}")

async def on_shutdown(app: web.Application):
    await bot.session.close()
    await app["db"].close()

# ----------------------
# Aiohttp app
# ----------------------
app = web.Application()
app.on_startup.append(on_startup)
app.on_shutdown.append(on_shutdown)

# webhook route
app.router.add_post(f"/webhook/{BOT_TOKEN}", WebhookRequestHandler(dp))
# healthcheck
app.router.add_get("/health", lambda r: web.Response(text="OK"))
# ----------------------
# Utils
# ----------------------
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

async def add_user(pool, user: types.User):
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO users(user_id, username)
        VALUES($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username;
        """, user.id, user.username or "")

# ----------------------
# /start
# ----------------------
@dp.message_handler(commands=["start"])
async def start_cmd(message: types.Message):
    pool = message.bot.get("db_pool")
    if pool:
        await add_user(pool, message.from_user)
    await message.answer("✅ Bot is alive and ready!")

# ----------------------
# /debug (for owner)
# ----------------------
@dp.message_handler(commands=["debug"])
async def debug_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.reply("❌ You are not the owner.")
    info = await bot.get_webhook_info()
    await message.reply(f"🔍 Debug:\n"
                        f"URL: {info.url}\n"
                        f"Pending: {info.pending_update_count}\n"
                        f"Error: {info.last_error_message or 'None'}")

# ----------------------
# /setmessage (owner only)
# ----------------------
@dp.message_handler(commands=["setmessage"])
async def setmessage_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.reply("❌ Only owner can set message.")
    text = message.get_args()
    if not text:
        return await message.reply("Usage: /setmessage <text>")
    pool = message.bot.get("db_pool")
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO settings(key, value) VALUES('broadcast_text',$1) "
                           "ON CONFLICT(key) DO UPDATE SET value=$1;", text)
    await message.reply("✅ Broadcast message saved.")

# ----------------------
# /setimage (owner only)
# ----------------------
@dp.message_handler(commands=["setimage"], content_types=["photo"])
async def setimage_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.reply("❌ Only owner can set image.")
    file_id = message.photo[-1].file_id
    pool = message.bot.get("db_pool")
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO settings(key,value) VALUES('broadcast_image',$1) "
                           "ON CONFLICT(key) DO UPDATE SET value=$1;", file_id)
    await message.reply("✅ Broadcast image saved.")

# ----------------------
# /broadcast (owner only)
# ----------------------
@dp.message_handler(commands=["broadcast"])
async def broadcast_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.reply("❌ Only owner can broadcast.")
    pool = message.bot.get("db_pool")
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM settings WHERE key='broadcast_text'")
        text = row["value"] if row else "No message set."
        img_row = await conn.fetchrow("SELECT value FROM settings WHERE key='broadcast_image'")
        img = img_row["value"] if img_row else None
        users = await conn.fetch("SELECT user_id FROM users")

    sent = 0
    for u in users:
        try:
            if img:
                await bot.send_photo(u["user_id"], img, caption=text)
            else:
                await bot.send_message(u["user_id"], text)
            sent += 1
        except Exception as e:
            logger.warning(f"Broadcast fail to {u['user_id']}: {e}")
    await message.reply(f"📢 Broadcast sent to {sent} users.")

# ----------------------
# /stats (owner only)
# ----------------------
@dp.message_handler(commands=["stats"])
async def stats_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.reply("❌ Not allowed.")
    pool = message.bot.get("db_pool")
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM users")
    await message.reply(f"📊 Total users: {count}")

# ----------------------
# /restore_db (owner only)
# ----------------------
@dp.message_handler(commands=["restore_db"])
async def restore_db_cmd(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.reply("❌ Not allowed.")
    pool = message.bot.get("db_pool")
    await init_db(pool)
    await message.reply("🗄️ Database schema restored.")
# ----------------------
# Entrypoint
# ----------------------
async def init_db(pool):
    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id BIGINT PRIMARY KEY,
            username TEXT
        );
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """)

async def on_startup(app):
    # database
    pool = await asyncpg.create_pool(DATABASE_URL)
    await init_db(pool)
    app["db_pool"] = pool
    bot["db_pool"] = pool
    dp["db_pool"] = pool
    logger.info("Database initialized.")

    # webhook
    webhook_url = f"{RENDER_EXTERNAL_URL}/webhook/{BOT_TOKEN}"
    await bot.set_webhook(webhook_url, drop_pending_updates=True)
    logger.info(f"Webhook set to {webhook_url}")

async def on_shutdown(app):
    await bot.session.close()
    logger.info("Bot shutdown complete.")

# ----------------------
# Main
# ----------------------
def main():
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    # health
    async def health(request):
        return web.Response(text="OK")
    app.router.add_get("/health", health)

    # webhook
    app.router.add_post(f"/webhook/{BOT_TOKEN}", WebhookRequestHandler(dp))

    # optional root debug
    async def index(request):
        return web.Response(text="Bot is running.")
    app.router.add_get("/", index)

    web.run_app(app, port=PORT)

if __name__ == "__main__":
    main()