import logging
from telegram import Update, MessageEntity
from telegram.ext import Application, MessageHandler, filters, CallbackContext
import os


# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Configuration
TOKEN = "7733604493:AAEuzdRdSv0l0xnAb1GDyaSVnzFWXbXN1c4"  # Replace with your bot token
CHANNEL_ID = -1002463367628  # Replace with your channel username or ID
CONTENT_ID_FORMAT = "\nhttps://telegram.me/toop_toop_bot?start={}"  # Customizable format
CAPTION_MAX_LENGTH = 1024  # Telegram caption limit

async def append_content_id(update: Update, context: CallbackContext) -> None:
    message = update.channel_post
    if not message:
        return

    content_id = message.message_id
    content_id_text = CONTENT_ID_FORMAT.format(content_id)
    
    try:
        await message.reply_text(content_id_text)
    except Exception as e:
        logger.error(f"Failed to send reply: {e}")

async def error_handler(update: object, context: CallbackContext) -> None:
    logger.error(msg="Exception while handling an update:", exc_info=context.error)

if __name__ == "__main__":
    app = Application.builder().token(TOKEN).build()
    app.add_handler(MessageHandler(filters.Chat(CHANNEL_ID) & filters.ALL, append_content_id))
    app.add_error_handler(error_handler)
    
    logger.info("Bot is running...")
    app.run_polling()


