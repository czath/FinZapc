import httpx
import logging
from typing import Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

async def send_telegram_message(token: str, chat_id: str, message_text: str) -> bool:
    """
    Sends a message to a specified Telegram chat using a bot token.

    Args:
        token: The Telegram Bot API token.
        chat_id: The chat ID to send the message to.
        message_text: The text of the message to send.

    Returns:
        True if the message was sent successfully, False otherwise.
    """
    if not token or not chat_id:
        logger.error("[Telegram] Bot token or chat_id is missing.")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message_text,
        'parse_mode': 'Markdown'  # Optional: for Markdown formatting
    }
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client: # Added timeout
            response = await client.post(url, json=payload)
            response.raise_for_status()  # Raises an HTTPStatusError for 4xx/5xx responses
            
            result_json = response.json()
            if result_json.get("ok"):
                logger.info(f"[Telegram] Message sent successfully to chat_id {chat_id}.")
                return True
            else:
                logger.error(f"[Telegram] API Error: {result_json.get('description')}")
                return False

    except httpx.HTTPStatusError as e:
        logger.error(f"[Telegram] HTTP error sending message: {e.response.status_code} - {e.response.text}")
        return False
    except httpx.RequestError as e:
        logger.error(f"[Telegram] Request error sending message: {e}")
        return False
    except Exception as e:
        logger.error(f"[Telegram] Unexpected error sending message: {e}", exc_info=True)
        return False

async def send_test_telegram_notification(db_repo, message_prefix="Test Notification") -> bool:
    """
    Retrieves Telegram settings from DB and sends a test message.
    Requires db_repo to have get_notification_settings method.
    """
    try:
        telegram_config = await db_repo.get_notification_settings("telegram")

        if not telegram_config:
            logger.info("[Telegram Test] Telegram notifications are not configured in the database.")
            return False
        
        if not telegram_config.get("is_active"):
            logger.info("[Telegram Test] Telegram notifications are configured but not active.")
            return False

        settings = telegram_config.get("settings")
        if not settings:
            logger.error("[Telegram Test] Telegram service is active, but settings are missing or empty in DB.")
            return False

        bot_token = settings.get("bot_token")
        chat_id = settings.get("chat_id")

        if not bot_token or not chat_id:
            logger.error("[Telegram Test] Telegram service is active, but bot_token or chat_id is missing in DB settings.")
            return False

        timestamp_str = ""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get('http://worldtimeapi.org/api/ip')
                response.raise_for_status()
                timestamp_str = response.json()['utc_datetime']
                logger.info(f"[Telegram Test] Fetched timestamp from worldtimeapi: {timestamp_str}")
        except Exception as e:
            logger.warning(f"[Telegram Test] Failed to fetch timestamp from worldtimeapi: {e}. Using local UTC time.")
            timestamp_str = datetime.now(timezone.utc).isoformat()

        test_message = f"{message_prefix}: This is a test message from the Financial App V3. Timestamp: {timestamp_str}"
        
        return await send_telegram_message(token=bot_token, chat_id=chat_id, message_text=test_message)
    except Exception as e:
        logger.error(f"[Telegram Test] Unexpected error during send_test_telegram_notification: {e}", exc_info=True)
        return False 