import logging
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
from hashids import Hashids

# Define the same secret salt and configuration in both bots
hashids = Hashids(salt="Admiral23", min_length=6)

# Store messages with the same caption
related_messages = {}

# Enable logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
TOKEN = "7733604493:AAEuzdRdSv0l0xnAb1GDyaSVnzFWXbXN1c4"
CHANNEL_ID = -1002463367628
CONTENT_ID_FORMAT = "\nhttps://telegram.me/toop_toop_bot?start={}"
CAPTION_MAX_LENGTH = 1024

def encode_multiple_ids(message_ids: list) -> str:
    """Encodes multiple message IDs into a single token."""
    return hashids.encode(*message_ids)

async def append_content_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles new and edited messages, appends encoded content ID."""
    global related_messages
    message = update.channel_post or update.edited_channel_post
    if not message:
        return

    caption = message.caption or ""
    message_id = message.message_id

    # Handle message edits
    if update.edited_channel_post:
        for caption_group in related_messages.values():
            if message_id in caption_group:
                caption_group.remove(message_id)
        related_messages = {k: v for k, v in related_messages.items() if v}

    # Organize messages by caption
    if caption:
        related_messages.setdefault(caption, []).append(message_id)
        related_ids = sorted(related_messages[caption])
    else:
        related_ids = [message_id]

    # Generate encoded token
    encoded_token = encode_multiple_ids(related_ids)
    content_id_text = CONTENT_ID_FORMAT.format(encoded_token)

    try:
        await message.reply_text(content_id_text)
    except Exception as e:
        logger.error(f"Error sending content ID message: {e}")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles unexpected errors gracefully."""
    logger.error("Exception while handling an update:", exc_info=context.error)

if __name__ == "__main__":
    app = Application.builder().token(TOKEN).build()
    app.add_handler(MessageHandler(filters.Chat(CHANNEL_ID) & filters.ALL, append_content_id))
    app.add_error_handler(error_handler)

    logger.info("Bot is running...")
    app.run_polling()
