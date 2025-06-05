from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
import logging

# Assuming your repository and services are structured to be importable like this:
# You might need to adjust imports based on your project structure.
from ..V3_database import SQLiteRepository # If V3_database.py is one level up
from ..services.notification_service import send_test_telegram_notification # If services is one level up
# If they are in the same directory or accessible via PYTHONPATH, imports might be simpler.

# Import the actual repository dependency provider
from ..dependencies import get_repository

# Placeholder for getting repository dependency - adapt to your app's dependency injection
# This is a common pattern in FastAPI but your setup might differ.
def get_db_repo():
    # This function needs to provide an instance of SQLiteRepository
    # For example, it might be initialized globally or per request.
    # Replace with your actual dependency injection logic for SQLiteRepository.
    # Example: from ..main import repo # if repo is a global instance
    # yield repo 
    # For now, this will cause an error if not properly set up in the main app.
    # You'll need to ensure this dependency is correctly resolved in your FastAPI app.
    raise NotImplementedError("get_db_repo dependency not implemented in notification_routes.py")

router = APIRouter(
    tags=["notifications"],
)

logger = logging.getLogger(__name__)

# --- Pydantic Models --- (Can also be in a separate schemas.py)
class TelegramSettingsPayload(BaseModel):
    bot_token: str = Field(..., description="Telegram Bot Token")
    chat_id: str = Field(..., description="Telegram Chat ID")
    is_active: bool

class NotificationStatusPayload(BaseModel):
    is_active: bool

class TaskNotificationPayload(BaseModel):
    task_id: str
    is_active: bool

class TelegramSettingsResponse(BaseModel):
    service_name: str = "telegram"
    settings: Optional[Dict[str, str]] = None
    is_active: bool = False

class GeneralResponse(BaseModel):
    status: str
    message: str
# --- End Pydantic Models ---

@router.post("/telegram/settings", response_model=GeneralResponse)
async def save_telegram_settings(
    payload: TelegramSettingsPayload,
    db: SQLiteRepository = Depends(get_repository)
):
    """Save Telegram notification settings (token and chat_id) and active status."""
    try:
        settings_data = {"bot_token": payload.bot_token, "chat_id": payload.chat_id}
        await db.save_notification_settings(
            service_name="telegram", 
            settings=settings_data, 
            is_active=payload.is_active
        )
        return {"status": "success", "message": "Telegram settings saved successfully."}
    except Exception as e:
        logger.error(f"Error saving Telegram settings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save Telegram settings: {str(e)}")

@router.get("/telegram/settings", response_model=Optional[TelegramSettingsResponse])
async def get_telegram_settings(db: SQLiteRepository = Depends(get_repository)):
    """Retrieve Telegram notification settings."""
    try:
        config = await db.get_notification_settings("telegram")
        if config:
            return TelegramSettingsResponse(settings=config.get("settings"), is_active=config.get("is_active", False))
        return TelegramSettingsResponse(settings=None, is_active=False)
    except Exception as e:
        logger.error(f"Error retrieving Telegram settings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve Telegram settings.")

@router.post("/telegram/status", response_model=GeneralResponse)
async def update_telegram_status(
    payload: NotificationStatusPayload,
    db: SQLiteRepository = Depends(get_repository)
):
    """Update the active status of Telegram notifications."""
    try:
        logger.debug(f"Updating telegram status. Payload: {payload.is_active}")
        current_settings = await db.get_notification_settings("telegram")
        logger.debug(f"Current telegram settings from DB: {current_settings}")

        if not current_settings or not current_settings.get("settings"):
            logger.info("Telegram settings not found in DB.")
            if payload.is_active:
                logger.info("Activating: Saving default empty settings as active.")
                # Ensure settings_json is not null, provide empty dict
                await db.save_notification_settings(
                    service_name="telegram",
                    settings={}, # Provide empty JSON object for settings
                    is_active=payload.is_active
                )
                return {"status": "success", "message": f"Telegram notifications initialized and set to {'active' if payload.is_active else 'inactive'}."}
            else:
                logger.warning("Attempting to deactivate non-existent telegram settings.")
                # If trying to deactivate non-existent settings, this could be a 404 or handled as already inactive.
                # Raising 404 as per previous logic.
                raise HTTPException(status_code=404, detail="Telegram configuration not found. Cannot update status for non-existent settings.")

        # If settings exist, just update the status
        logger.info(f"Updating existing telegram service status to: {payload.is_active}")
        success = await db.update_notification_service_status("telegram", payload.is_active)
        if success:
            logger.info("Telegram status updated successfully in DB.")
            return {"status": "success", "message": f"Telegram notifications {'activated' if payload.is_active else 'deactivated'}."}
        else:
            logger.error(f"db.update_notification_service_status returned False for telegram. Active: {payload.is_active}")
            raise HTTPException(status_code=500, detail="Failed to update Telegram status. Update operation in DB failed.")
    except HTTPException as http_exc:
        logger.warning(f"HTTPException in update_telegram_status: {http_exc.detail}")
        raise http_exc # Re-raise HTTPExceptions directly
    except Exception as e:
        logger.error(f"Unexpected error in update_telegram_status: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update Telegram status: {str(e)}")

@router.post("/telegram/test", response_model=GeneralResponse)
async def trigger_test_telegram_notification(db: SQLiteRepository = Depends(get_repository)):
    """Send a test Telegram notification using saved settings."""
    try:
        # The service function now has more robust internal checks
        success = await send_test_telegram_notification(db_repo=db)
        if success:
            return {"status": "success", "message": "Test Telegram notification sent successfully!"}
        else:
            # The service function returned False. This could be due to various reasons
            # already logged by the service (inactive, missing token/chat_id, API error).
            # Provide a user-facing message that suggests checking logs or settings.
            logger.warning("[API Telegram Test] send_test_telegram_notification service returned False.")
            raise HTTPException(status_code=400, # Bad Request or a specific 5xx error like 502 Bad Gateway if it's an external issue
                                detail="Failed to send test Telegram notification. Please check application logs and ensure Telegram settings (Bot Token, Chat ID) are correct and the service is active.")
    except HTTPException as http_exc: # Re-raise HTTPExceptions from deeper calls if any
        raise http_exc
    except Exception as e:
        logger.error(f"[API Telegram Test] Unexpected error in trigger_test_telegram_notification route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred while trying to send a test Telegram notification: {str(e)}")

# --- Task-Specific Notification Settings ---

@router.get("/task_settings", response_model=Dict[str, bool])
async def get_task_notification_settings(db: SQLiteRepository = Depends(get_repository)):
    """Retrieve task-specific notification settings."""
    try:
        config = await db.get_notification_settings("task_notifications")
        if config and config.get("settings"):
            return config["settings"]
        # Return empty dict if not found, which is a valid state (all off)
        return {}
    except Exception as e:
        logger.error(f"Error retrieving task_notification_settings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve task notification settings.")

@router.post("/task_settings", response_model=GeneralResponse)
async def save_task_notification_setting(
    payload: TaskNotificationPayload,
    db: SQLiteRepository = Depends(get_repository)
):
    """Save a single task-specific notification setting."""
    try:
        # Get the current settings object, or an empty one if it doesn't exist
        current_config = await db.get_notification_settings("task_notifications")
        
        current_settings = {}
        if current_config and current_config.get("settings"):
            current_settings = current_config["settings"]

        # Update the specific task's setting
        current_settings[payload.task_id] = payload.is_active

        # Save the entire settings object back. The service is considered "active" as a container.
        await db.save_notification_settings(
            service_name="task_notifications",
            settings=current_settings,
            is_active=True
        )
        return {"status": "success", "message": f"Setting for '{payload.task_id}' updated."}
    except Exception as e:
        logger.error(f"Error saving task_notification_setting for {payload.task_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to save task notification setting.")

# Remember to include this router in your main FastAPI app:
# from .routers import notification_routes
# app.include_router(notification_routes.router)

# You will also need to ensure that the dependency `get_db_repo` is correctly implemented
# in your FastAPI application setup to provide an instance of `SQLiteRepository`.
# For example, in your main.py or app setup:
# from .V3_database import SQLiteRepository, DATABASE_URL
# repo_instance = SQLiteRepository(DATABASE_URL) 
# async def get_repository_instance():
#    return repo_instance 
#
# # To override the get_db_repo in notification_routes.py for the entire app:
# # app.dependency_overrides[get_db_repo] = get_repository_instance
#
# # Or, if your SQLiteRepository is designed as an async context manager:
# # async def get_repository_dependency():
# #     async with SQLiteRepository(DATABASE_URL) as repo:
# #         yield repo
# # app.dependency_overrides[get_db_repo] = get_repository_dependency