from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
import logging
import asyncio
import random
import os
from hashids import Hashids
from telegram.error import TelegramError

# تنظیمات لاگ‌گیری
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# اطلاعات بات و کانال‌ها
BOT_TOKEN = '8164630657:AAGcf35y3u6SbHDegxZCVKtKSNsL4B7OS0g'  # استفاده از متغیر محیطی برای امنیت بیشتر
STORAGE_CHANNEL = -1002463367628  # جایگزین با آیدی عددی کانال خصوصی
REQUIRED_CHANNELS = ["@zapas_kcrang", "@kcrang"]

# Use the same salt and configuration as Bot 1
hashids = Hashids(salt="Admiral23", min_length=6)

def decode_movie_token(token: str) -> list:
    """Decode token into list of message IDs"""
    decoded = hashids.decode(token)
    return list(decoded) if decoded else []

# تابع بررسی عضویت کاربر
async def get_unjoined_channels(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    unjoined_channels = []
    for channel in REQUIRED_CHANNELS:
        try:
            member = await context.bot.get_chat_member(chat_id=channel, user_id=user_id)
            if member.status not in ["member", "administrator", "creator"]:
                unjoined_channels.append(channel)
        except TelegramError as e:
            logger.error(f"خطا در بررسی عضویت کاربر {user_id} در {channel}: {e}")
            unjoined_channels.append(channel)
    return unjoined_channels

# تابع ایجاد دکمه‌های عضویت و تأیید
def get_verification_menu(unjoined_channels):
    keyboard = [[InlineKeyboardButton(f"✅ عضویت در {ch}", url=f"https://t.me/{ch[1:]}")] for ch in unjoined_channels]
    keyboard.append([InlineKeyboardButton("🔄 بررسی مجدد عضویت", callback_data="verify")])
    return InlineKeyboardMarkup(keyboard)

# ارسال محتوا به کاربر با تأخیر و کنترل محدودیت نرخ
async def send_timed_messages(user_id: int, context: ContextTypes.DEFAULT_TYPE, content_codes: list):
    try:
        sent_messages = []
        for content_code in content_codes:
            retry_attempts = 3  # حداکثر ۳ تلاش مجدد
            delay = 2  # تأخیر اولیه

            for attempt in range(retry_attempts):
                try:
                    await asyncio.sleep(random.uniform(1.5, 3))  # ایجاد تأخیر تصادفی برای جلوگیری از محدودیت
                    sent_message = await context.bot.forward_message(
                        chat_id=user_id,
                        from_chat_id=STORAGE_CHANNEL,
                        message_id=int(content_code)
                    )
                    sent_messages.append(sent_message)
                    break  # موفقیت، خروج از حلقه تکرار

                except TelegramError as e:
                    if "Too Many Requests" in str(e):
                        wait_time = int(str(e).split("Retry in ")[-1].split()[0])  # استخراج زمان تأخیر
                        logger.warning(f"محدودیت نرخ فعال شد! تلاش مجدد در {wait_time} ثانیه برای کاربر {user_id}")
                        await asyncio.sleep(wait_time + random.uniform(1, 3))  # اضافه کردن تأخیر اضافی
                    else:
                        logger.error(f"خطا در ارسال پیام {content_code} به کاربر {user_id}: {e}")
                        break  # عدم تلاش مجدد برای سایر خطاها

        countdown_message = await context.bot.send_message(
            chat_id=user_id,
            text="⏳ این محتوا پس از ۲ دقیقه حذف خواهد شد!"
        )
        await asyncio.sleep(120)

        for sent_message in sent_messages:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=sent_message.message_id)
            except TelegramError as e:
                logger.warning(f"خطا در حذف پیام {sent_message.message_id} برای کاربر {user_id}: {e}")

        await context.bot.delete_message(chat_id=user_id, message_id=countdown_message.message_id)

    except Exception as e:
        logger.error(f"خطای کلی در ارسال محتوا به کاربر {user_id}: {e}")
        await context.bot.send_message(chat_id=user_id, text="⚠️ مشکلی در ارسال محتوا رخ داده است. لطفاً مجدداً تلاش کنید.")

# دستور /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    args = context.args if context.args else []
    content_codes = []
    if args:
        tokens = args[0].split('_')
        for token in tokens:
            decoded_ids = decode_movie_token(token)
            content_codes.extend(str(id) for id in decoded_ids)
    
    try:
        unjoined_channels = await get_unjoined_channels(user.id, context)
        if not unjoined_channels:
            if content_codes:
                asyncio.create_task(send_timed_messages(user.id, context, content_codes))
                await update.message.reply_text("📩 محتوای شما در حال ارسال است...")
            else:
                await update.message.reply_text("✅ خوش آمدید! برای دریافت رسانه، از لینک‌های محتوایی استفاده کنید.")
        else:
            await update.message.reply_text(
                "⚠️ برای دسترسی به محتوا، ابتدا در کانال‌های زیر عضو شوید:",
                reply_markup=get_verification_menu(unjoined_channels)
            )
    except Exception as e:
        logger.error(f"خطا در پردازش دستور /start برای کاربر {user.id}: {e}")
        await update.message.reply_text("⚠️ مشکلی رخ داده است. لطفاً دوباره تلاش کنید.")

# بررسی عضویت از طریق دکمه تأیید
async def verify_membership(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    try:
        unjoined_channels = await get_unjoined_channels(query.from_user.id, context)
        if not unjoined_channels:
            await query.edit_message_text("✅ عضویت شما تأیید شد! اکنون می‌توانید به محتوای رسانه‌ای دسترسی داشته باشید.")
        else:
            await query.edit_message_text(
                "⚠️ شما هنوز در تمام کانال‌ها عضو نشده‌اید. لطفاً ابتدا عضو شوید:",
                reply_markup=get_verification_menu(unjoined_channels)
            )
    except Exception as e:
        logger.error(f"خطا در بررسی عضویت کاربر {query.from_user.id}: {e}")
        await query.edit_message_text("⚠️ مشکلی در بررسی عضویت رخ داده است. لطفاً مجدداً تلاش کنید.")

# راه‌اندازی ربات
def main():
    application = Application.builder().token(BOT_TOKEN).concurrent_updates(2).build()  # محدود کردن پردازش هم‌زمان
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(verify_membership, pattern="verify"))
    application.run_polling()

if __name__ == '__main__':
    main()
