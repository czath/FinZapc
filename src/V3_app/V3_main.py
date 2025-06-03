"""
Main entry point for V3 of the financial application.
"""

# --- REMOVE path manipulation comments ---

import logging
import uvicorn
import inspect
import traceback
# Use relative imports when running as a module
from .V3_web import create_app, add_websocket_route
from fastapi import FastAPI, Request, Form
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
# Use relative import
# from .V3_database import SQLiteRepository # <-- Comment out specific class import
from .V3_database import update_exchange_rate
# --- ADD Import for IBKR Monitor --- 
from .V3_ibkr_monitor import register_ibkr_monitor
# --- END Import ---

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Explicitly set log level for edgar_router to ensure its INFO messages are shown
logging.getLogger("src.V3_app.routers.edgar_router").setLevel(logging.INFO)

def main():
    """Main entry point."""
    try:
        # Create the FastAPI application with direct call for debugging
        app = create_app()
        print(f"Type of app: {type(app)}")
        print(f"App attributes: {dir(app) if app is not None else 'None'}")
        if app is None:
            logger.critical("create_app() returned None! Likely due to an exception in the function.")
            
            # Try to examine the create_app function
            print("\n---- create_app function details: ----")
            print(inspect.getsource(create_app))
            print("\n---- end create_app details ----")

        # Run the server using regular call instead of string syntax for now
        if app is not None:
            # --- Register IBKR Status Monitor --- 
            register_ibkr_monitor(app)
            # --- End IBKR Monitor Register --- 
            
            # --- Register original WebSocket route (if still needed) --- 
            # If the add_websocket_route (adding /ws/status) is still required 
            # for other purposes, uncomment the lines below. 
            # Otherwise, keep them commented or remove if /ws/status is unused.
            # add_websocket_route(app)
            # logger.info("Original WebSocket route (/ws/status) added.")
            # --- End Original WebSocket --- 

            uvicorn.run(
                app,
                host="0.0.0.0",
                port=8000,
                log_level="info"
            )
        else:
            logger.critical("Not starting uvicorn because app is None")
    except Exception as e:
        logger.error(f"Application failed to start: {str(e)}")
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main() 