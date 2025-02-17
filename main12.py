import asyncio
import random
import logging
import redis
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError
from aiogram.filters import Command
from hashids import Hashids
import json
from redis.asyncio import Redis
from contextlib import asynccontextmanager
import time
from dataclasses import dataclass
from typing import List, Optional, Union, Dict, Any
import os
import redis
import asyncio
from redis.asyncio import Redis
from contextlib import asynccontextmanager
import redis.asyncio as aioredis
from contextlib import asynccontextmanager

# Redis Configuration from Railway Environment Variables
REDIS_URL = "redis://default:ZEWvatsColwbVZEOYlrWpFFDIMhfAyFW@switchback.proxy.rlwy.net:25159"

# Sync Redis client
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# Async Redis Client
async_redis = aioredis.from_url(REDIS_URL, decode_responses=True, max_connections=10)



async_redis = redis.asyncio.Redis.from_url(REDIS_URL, decode_responses=True, max_connections=10)

@asynccontextmanager
async def get_async_redis():
    """Yield the async Redis client without re-creating instances."""
    try:
        yield async_redis
    finally:
        pass  # No need to close here




# Advanced Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Bot Configuration
BOT_TOKEN = "8164630657:AAGcf35y3u6SbHDegxZCVKtKSNsL4B7OS0g"
STORAGE_CHANNEL = -1002463367628
REQUIRED_CHANNELS = ["@zapas_kcrang", "@kcrang"]

# Redis Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None  # Set if needed
REDIS_POOL_SIZE = 10
REDIS_QUEUE_KEY = "video_queue"
REDIS_USER_CACHE_PREFIX = "user_subscriptions:"
REDIS_USER_CACHE_TTL = 3600  # Cache subscription status for 1 hour

# Rate Limiting Config
MAX_MESSAGES_PER_SECOND = 20
MAX_MESSAGES_PER_MINUTE = 300  # Adjust based on Telegram's limits
SUBSCRIPTION_CHECK_COOLDOWN = 120  # Seconds between subscription checks

# Message deletion configuration
MESSAGE_DELETE_DELAY = 120  # 2 minutes in seconds

# Persian (Farsi) Message Templates
MESSAGES = {
    "welcome": "✅ خوش آمدید! برای دریافت مدیا، از لینک‌های محتوا استفاده کنید.",
    "media_preparing": "📩 {count} فایل مدیای شما در حال آماده‌سازی است و به زودی ارسال می‌شود...",
    "join_channels": "⚠️ برای دسترسی به محتوا، لطفاً ابتدا در کانال‌های زیر عضو شوید:",
    "pending_media": "شما {count} مدیا در انتظار دارید که پس از عضویت در کانال‌ها ارسال خواهد شد.",
    "membership_verified": "✅ عضویت شما تأیید شد! اکنون می‌توانید به مدیا دسترسی داشته باشید.",
    "still_not_member": "⚠️ شما هنوز عضو همه کانال‌های مورد نیاز نیستید. لطفاً ابتدا عضو شوید:",
    "error_occurred": "⚠️ خطایی رخ داد. لطفاً دوباره تلاش کنید یا با پشتیبانی تماس بگیرید.",
    "wait_before_check": "لطفاً {seconds} ثانیه صبر کنید تا دوباره بررسی کنید",
    "pending_media_sending": "📩 {count} فایل مدیای در انتظار شما به زودی ارسال خواهد شد.",
    "help_message": "🔍 **نحوه استفاده از این ربات**\n\n"
                   "۱. برای دسترسی به محتوا، در کانال‌های مورد نیاز عضو شوید\n"
                   "۲. از لینک‌های محتوا با فرمت `/start TOKEN` استفاده کنید\n"
                   "۳. ربات مدیای درخواستی را برای شما ارسال می‌کند\n\n"
                   "اگر به کمک نیاز دارید، با @admin تماس بگیرید",
    "status_message": "📊 اندازه صف فعلی: {queue_size} مورد",
    "status_error": "در حال حاضر امکان بررسی وضعیت وجود ندارد.",
    "join_button": "✅ عضویت در {channel}",
    "check_again_button": "🔄 بررسی مجدد"
}







# Bot initialization
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
hashids = Hashids(salt="Admiral23", min_length=6)

# Rate limiter implementation
class RateLimiter:
    def __init__(self):
        self.messages_sent = 0
        self.minute_start = time.time()
        self.second_start = time.time()
        self.lock = asyncio.Lock()
        
    async def can_send(self) -> bool:
        async with self.lock:
            current_time = time.time()
            
            # Reset counters if intervals passed
            if current_time - self.second_start >= 1:
                self.messages_sent = 0
                self.second_start = current_time
            
            if current_time - self.minute_start >= 60:
                self.minute_start = current_time
            
            # Check if we're under limits
            if self.messages_sent >= MAX_MESSAGES_PER_SECOND:
                return False
                
            # We're good to send
            self.messages_sent += 1
            return True
            
    async def wait_for_slot(self):
        while not await self.can_send():
            await asyncio.sleep(0.1)

rate_limiter = RateLimiter()

# Data models for better type safety and serialization
@dataclass
class QueueItem:
    user_id: int
    message_id: int
    timestamp: float = None
    forwarded_message_id: Optional[int] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()
    
    def to_json(self) -> str:
        return json.dumps(self.__dict__)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'QueueItem':
        data = json.loads(json_str)
        return cls(**data)

# ========================= Utility Functions =========================

def decode_movie_token(token: str) -> list:
    """Decode token into a list of message IDs."""
    decoded = hashids.decode(token)
    return list(decoded) if decoded else []

async def get_subscription_cache_key(user_id: int) -> str:
    """Generate Redis key for user subscription cache."""
    return f"{REDIS_USER_CACHE_PREFIX}{user_id}"

async def get_cached_subscription_status(user_id: int) -> Optional[List[str]]:
    """Get cached subscription status for user."""
    async with get_async_redis() as redis:
        cached = await redis.get(await get_subscription_cache_key(user_id))
        if cached:
            return json.loads(cached)
    return None

async def set_cached_subscription_status(user_id: int, unjoined_channels: List[str]):
    """Cache user's subscription status."""
    async with get_async_redis() as redis:
        key = await get_subscription_cache_key(user_id)
        await redis.setex(key, REDIS_USER_CACHE_TTL, json.dumps(unjoined_channels))

async def get_unjoined_channels(user_id: int, force_check: bool = False) -> list:
    """Check if user is subscribed to required channels, with caching."""
    # Check cache first if not forced
    if not force_check:
        cached = await get_cached_subscription_status(user_id)
        if cached is not None:
            return cached
    
    unjoined_channels = []
    for channel in REQUIRED_CHANNELS:
        try:
            member = await bot.get_chat_member(chat_id=channel, user_id=user_id)
            if member.status not in ["member", "administrator", "creator"]:
                unjoined_channels.append(channel)
        except TelegramForbiddenError:
            logger.warning(f"Bot is not an admin in {channel}, skipping check.")
        except Exception as e:
            logger.error(f"Error checking {channel} for user {user_id}: {e}")
            unjoined_channels.append(channel)
    
    # Cache the result
    await set_cached_subscription_status(user_id, unjoined_channels)
    return unjoined_channels

def get_verification_menu(unjoined_channels: list) -> InlineKeyboardMarkup:
    """Generate a verification menu with channel join buttons in Persian."""
    keyboard = [[InlineKeyboardButton(text=MESSAGES["join_button"].format(channel=ch), url=f"https://t.me/{ch[1:]}")] for ch in unjoined_channels]
    keyboard.append([InlineKeyboardButton(text=MESSAGES["check_again_button"], callback_data="verify")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def handle_media_requests(user_id: int, content_codes: List[str]) -> int:
    """Add media requests to queue and return number of queued items."""
    async with get_async_redis() as redis:
        pipeline = redis.pipeline()
        count = 0
        
        for content_id in content_codes:
            item = QueueItem(user_id=user_id, message_id=int(content_id))
            await pipeline.lpush(REDIS_QUEUE_KEY, item.to_json())
            count += 1
        
        await pipeline.execute()
        return count

async def schedule_message_deletion(user_id: int, message_id: int):
    """Schedule a message for deletion after the specified delay."""
    await asyncio.sleep(MESSAGE_DELETE_DELAY)
    try:
        await bot.delete_message(chat_id=user_id, message_id=message_id)
        logger.info(f"Deleted message {message_id} for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to delete message {message_id} for user {user_id}: {e}")

async def rate_limited_forward(user_id: int, message_id: int):
    """Forward a message with intelligent rate limiting and schedule deletion."""
    retry_attempts = 5

    for attempt in range(retry_attempts):
        try:
            # Wait until we can send according to rate limiter
            await rate_limiter.wait_for_slot()
            
            # Forward the message and get the forwarded message object
            forwarded_msg = await bot.forward_message(
                chat_id=user_id,
                from_chat_id=STORAGE_CHANNEL,
                message_id=message_id
            )
            
            # Schedule deletion of the forwarded message
            asyncio.create_task(schedule_message_deletion(user_id, forwarded_msg.message_id))
            
            logger.info(f"Successfully forwarded message {message_id} to user {user_id}")
            return True
            
        except TelegramRetryAfter as e:
            wait_time = e.retry_after + random.uniform(0.1, 1.0)  # Add jitter
            logger.warning(f"Flood control: waiting {wait_time}s before retrying message {message_id} to {user_id}")
            await asyncio.sleep(wait_time)
            
        except Exception as e:
            if "blocked" in str(e).lower() or "deactivated" in str(e).lower():
                logger.info(f"User {user_id} blocked the bot or deactivated: {e}")
                return False  # Don't retry for blocked users
                
            logger.error(f"Error forwarding message {message_id} to {user_id}: {e}")
            if attempt < retry_attempts - 1:
                await asyncio.sleep(random.uniform(1, 3) * (attempt + 1))  # Exponential backoff
            else:
                return False  # Give up after max retries
                
    return False

async def process_queue_worker():
    """Process items from the queue with improved error handling."""
    worker_id = random.randint(1000, 9999)
    logger.info(f"Starting queue worker {worker_id}")
    
    while True:
        try:
            async with get_async_redis() as redis:
                # Get item from queue
                raw_item = await redis.rpop(REDIS_QUEUE_KEY)
                if not raw_item:
                    await asyncio.sleep(0.5)  # Wait briefly when queue is empty
                    continue
                
                # Process item
                try:
                    item = QueueItem.from_json(raw_item)
                    success = await rate_limited_forward(item.user_id, item.message_id)
                    
                    # If forward failed (but not due to user blocking), requeue with backoff
                    if not success:
                        # Limit requeues to avoid infinite loops
                        if time.time() - item.timestamp < 3600:  # Only retry for up to 1 hour
                            await asyncio.sleep(5)  # Wait before requeueing
                            await redis.lpush(REDIS_QUEUE_KEY, raw_item)
                            logger.warning(f"Requeued failed message {item.message_id} for user {item.user_id}")
                        
                except (json.JSONDecodeError, ValueError) as e:
                    logger.error(f"Worker {worker_id}: Invalid queue item format: {e}, raw: {raw_item}")
                    
        except redis.RedisError as e:
            logger.error(f"Worker {worker_id}: Redis error: {e}")
            await asyncio.sleep(1)  # Wait before retrying on Redis errors
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Unexpected error: {e}", exc_info=True)
            await asyncio.sleep(1)  # Wait before continuing on unexpected errors

# ========================= Bot Handlers =========================

@dp.message(Command("start"))
async def start(message: types.Message):
    """Handle /start command, check subscription, and forward videos."""
    user_id = message.from_user.id
    args = message.text.split()[1:] if len(message.text.split()) > 1 else []
    content_codes = []

    # Track user command for analytics
    logger.info(f"User {user_id} sent /start command with args: {args}")

    try:
        # Parse content tokens
        if args:
            tokens = args[0].split('_')
            for token in tokens:
                content_codes.extend(str(id) for id in decode_movie_token(token))

        # Check subscriptions
        unjoined_channels = await get_unjoined_channels(user_id)
        
        if not unjoined_channels:
            if content_codes:
                request_count = await handle_media_requests(user_id, content_codes)
                await message.answer(MESSAGES["media_preparing"].format(count=request_count))
            else:
                await message.answer(MESSAGES["welcome"])
        else:
            keyboard = get_verification_menu(unjoined_channels)
            await message.answer(
                MESSAGES["join_channels"],
                reply_markup=keyboard
            )
            
            # If user has pending content, inform them
            if content_codes:
                await message.answer(MESSAGES["pending_media"].format(count=len(content_codes)))
    except Exception as e:
        logger.error(f"Error in /start for user {user_id}: {e}", exc_info=True)
        await message.answer(MESSAGES["error_occurred"])

@dp.callback_query(lambda query: query.data == "verify")
async def verify_membership(query: types.CallbackQuery):
    """Verify membership when the user clicks the check button."""
    user_id = query.from_user.id
    
    try:
        # Get last verification time
        async with get_async_redis() as redis:
            last_check_key = f"last_verify:{user_id}"
            last_check = await redis.get(last_check_key)
            
            # If checked recently, inform user
            if last_check and (time.time() - float(last_check)) < SUBSCRIPTION_CHECK_COOLDOWN:
                remain_time = int(SUBSCRIPTION_CHECK_COOLDOWN - (time.time() - float(last_check)))
                await query.answer(MESSAGES["wait_before_check"].format(seconds=remain_time))
                return
                
            # Set current check time
            await redis.set(last_check_key, str(time.time()), ex=SUBSCRIPTION_CHECK_COOLDOWN)
        
        # Force fresh subscription check
        unjoined_channels = await get_unjoined_channels(user_id, force_check=True)
        
        if not unjoined_channels:
            # Successful verification
            await query.message.edit_text(MESSAGES["membership_verified"])
            
            # Check if user has pending media in queue
            pending_media = await check_pending_media_for_user(user_id)
            if pending_media > 0:
                await query.message.answer(MESSAGES["pending_media_sending"].format(count=pending_media))
        else:
            # Still needs to join channels
            await query.message.edit_text(
                MESSAGES["still_not_member"],
                reply_markup=get_verification_menu(unjoined_channels)
            )
    except Exception as e:
        logger.error(f"Error verifying membership for user {user_id}: {e}", exc_info=True)
        await query.answer(MESSAGES["error_occurred"])

async def check_pending_media_for_user(user_id: int) -> int:
    """Check and activate any pending media for newly verified users."""
    count = 0
    # This would check any pending media requests that were saved when user wasn't subscribed
    # For simplicity, not implementing full details
    return count

# ========================= Additional Handlers =========================

@dp.message(Command("help"))
async def send_help(message: types.Message):
    """Send help information to the user."""
    await message.answer(MESSAGES["help_message"])

@dp.message(Command("status"))
async def status(message: types.Message):
    """Check queue status (admin only)."""
    user_id = message.from_user.id
    # Admin check would go here
    
    try:
        async with get_async_redis() as redis:
            queue_size = await redis.llen(REDIS_QUEUE_KEY)
            await message.answer(MESSAGES["status_message"].format(queue_size=queue_size))
    except Exception as e:
        logger.error(f"Error in /status command: {e}")
        await message.answer(MESSAGES["status_error"])

# ========================= Start Bot With Multiple Workers =========================

async def main():
    """Start the bot with multiple queue workers for scalability."""
    # Create multiple queue workers
    workers = []
    worker_count = 3  # Adjust based on load and resources
    
    for _ in range(worker_count):
        workers.append(asyncio.create_task(process_queue_worker()))
    
    logger.info(f"Started {worker_count} queue workers")
    
    try:
        # Start the bot
        await dp.start_polling(bot)
    finally:
        # Properly cleanup on shutdown
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        
        # Close Redis pools
        redis_pool.disconnect()
        await async_redis_pool.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.critical(f"Unexpected fatal error: {e}", exc_info=True)
