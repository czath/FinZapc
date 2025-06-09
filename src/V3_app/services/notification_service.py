import httpx
import logging
from typing import Optional
from datetime import datetime, timezone
from ..V3_database import SQLiteRepository

logger = logging.getLogger(__name__)

async def dispatch_notification(db_repo: SQLiteRepository, task_id: str, message: str) -> bool:
    """
    Central dispatcher for sending notifications.
    Checks if the task and the notification service are active before sending.
    """
    try:
        # 1. Check if the specific task is enabled for notifications
        task_is_active = await db_repo.get_task_notification_setting(task_id)
        if not task_is_active:
            logger.info(f"[Dispatcher] Notifications for task '{task_id}' are disabled. Skipping.")
            return False

        # 2. Check for active notification services (starting with Telegram)
        # --- Telegram Check ---
        telegram_config = await db_repo.get_notification_settings("telegram")
        if telegram_config and telegram_config.get("is_active"):
            settings = telegram_config.get("settings")
            if settings:
                bot_token = settings.get("bot_token")
                chat_id = settings.get("chat_id")
                if bot_token and chat_id:
                    logger.info(f"[Dispatcher] Task '{task_id}' is active and Telegram is configured. Sending notification.")
                    # Prepend task_id to the message for clarity
                    full_message = f"*{task_id}*\n\n{message}"
                    await send_telegram_message(token=bot_token, chat_id=chat_id, message_text=full_message)
                    return True # Sent successfully
                else:
                    logger.warning("[Dispatcher] Telegram service is active, but bot_token or chat_id is missing.")
            else:
                logger.warning("[Dispatcher] Telegram service is active, but settings are missing.")
        
        # --- (Future) Email Check would go here ---

        logger.info(f"[Dispatcher] Task '{task_id}' is active, but no active and configured notification services found.")
        return False

    except Exception as e:
        logger.error(f"[Dispatcher] Unexpected error processing notification for task '{task_id}': {e}", exc_info=True)
        return False

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

async def send_task_completion_telegram_notification(db_repo, message: str) -> bool:
    """
    Retrieves Telegram settings from DB and sends a notification message for a task.
    """
    try:
        telegram_config = await db_repo.get_notification_settings("telegram")

        if not telegram_config or not telegram_config.get("is_active"):
            logger.info("[Telegram Task Notify] Service not configured or inactive. Skipping notification.")
            return False

        settings = telegram_config.get("settings")
        if not settings:
            logger.error("[Telegram Task Notify] Service is active, but settings are missing.")
            return False

        bot_token = settings.get("bot_token")
        chat_id = settings.get("chat_id")

        if not bot_token or not chat_id:
            logger.error("[Telegram Task Notify] Bot token or chat_id is missing.")
            return False

        # The message is passed in directly.
        return await send_telegram_message(token=bot_token, chat_id=chat_id, message_text=message)
    except Exception as e:
        logger.error(f"[Telegram Task Notify] Unexpected error: {e}", exc_info=True)
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