"""
Main entry point for V3 of the financial application.
"""

# --- REMOVE path manipulation comments ---

import logging
import uvicorn
import inspect
import traceback
# Use simple imports as V3_main.py is run directly
from V3_web import create_app, add_websocket_route
from fastapi import FastAPI, Request, Form
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
# Use simple import
from V3_database import update_exchange_rate

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
            # --- Register WebSocket route AFTER app creation --- 
            add_websocket_route(app)
            logger.info("WebSocket route added.")
            # --- End Register ---
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