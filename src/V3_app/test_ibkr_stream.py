import asyncio
import logging
import os
import signal
import requests
import json
from typing import List
import sqlite3 # ADDED for direct DB access

# Assuming the script is run from the directory containing V3_app
try:
    # Import the SYNC service
    from V3_ibkr_api import SyncIBKRService
    # Repository no longer needed directly for this test script
    # from V3_database import SQLiteRepository 
except ImportError:
    # Fallback if run directly from V3_app folder
    from V3_ibkr_api import SyncIBKRService
    # from V3_database import SQLiteRepository


# --- Configuration ---
# Keep TEST_TICKERS for potential manual testing, but main flow uses DB
TEST_TICKERS = ["AAPL"] 
DB_FILENAME = "V3_database.db" # Relative path to the database
# --- End Configuration ---

# --- Logging Setup ---
logging.basicConfig(
    level=logging.DEBUG, # Changed from INFO to DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("IBKR_Stream_Test")
# --- End Logging Setup ---

# --- Global variable to signal shutdown ---
# shutdown_requested = False # COMMENTED OUT - WS specific
# ibkr_service_instance = None # To access disconnect in handler - COMMENTED OUT - WS specific

# def handle_shutdown_signal(signum, frame): # COMMENTED OUT - WS specific
#     """Sets the shutdown flag when SIGINT (Ctrl+C) is received.""" # COMMENTED OUT - WS specific
#     global shutdown_requested, ibkr_service_instance # COMMENTED OUT - WS specific
#     logger.warning(f"Received signal {signum}. Requesting shutdown...") # COMMENTED OUT - WS specific
#     shutdown_requested = True # COMMENTED OUT - WS specific
#     # Attempt graceful disconnect if service exists # COMMENTED OUT - WS specific
#     # if ibkr_service_instance: # COMMENTED OUT - WS specific
#     #      logger.info("Attempting to close WebSocket and disconnect service...") # COMMENTED OUT - WS specific
#     #      # We can't await here, so we schedule it or rely on finally block # COMMENTED OUT - WS specific
#     #      # For simplicity, rely on the finally block in main_test # COMMENTED OUT - WS specific

# --- Copy Sync DB Helper --- 
def get_tickers_from_db_sync(db_path: str) -> List[str]:
    """Fetches unique tickers directly from the positions table using sqlite3."""
    tickers = set()
    try:
        logger.info(f"[Sync Test] Connecting to DB sync: {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Ensure this matches the actual table and column names
        cursor.execute("SELECT DISTINCT ticker FROM positions WHERE ticker IS NOT NULL AND ticker != ''")
        rows = cursor.fetchall()
        for row in rows:
            tickers.add(row[0])
        conn.close()
        logger.info(f"[Sync Test] Fetched {len(tickers)} unique tickers from DB: {list(tickers)}")
        return sorted(list(tickers))
    except sqlite3.Error as e:
        logger.error(f"[Sync Test] DB error fetching tickers: {e}")
        return []
    except Exception as e:
        logger.error(f"[Sync Test] Unexpected error fetching tickers: {e}")
        return []
# --- End Sync DB Helper ---

# --- UPDATED main_test to be Synchronous using SyncIBKRService ---
def main_test(): 
    """Synchronous test: Gets tickers from DB, gets conids & snapshot via SyncIBKRService."""

    # Construct absolute path to the database
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, DB_FILENAME)
    
    # 1. Get Tickers Synchronously from DB
    tickers = get_tickers_from_db_sync(db_path)
    if not tickers:
        logger.error("No tickers found in database. Exiting.")
        return
            
    # 2. Instantiate the SYNCHRONOUS service
    logger.info("Instantiating SyncIBKRService...")
    sync_service = SyncIBKRService() # Uses default base URL
    
    # 3. Get ConIDs using Sync Service
    logger.info(f"Attempting to find conids for tickers: {tickers}")
    conids = sync_service.get_conids_sync(tickers)
                
    if not conids:
        logger.warning("No valid conids found for any tickers. Exiting before snapshot.")
        return
            
    # 4. Call the Synchronous Snapshot Method
    try:
        fields_to_fetch = ['31', '55', '84', '86'] # Use desired fields
        logger.info(f"Attempting SYNC snapshot via fetch_snapshot_sync for conids {conids}")
        snapshot_result = sync_service.fetch_snapshot_sync(conids, fields_to_fetch)
        
        # Sync method already logs and prints
        if snapshot_result is not None:
             logger.info("Successfully received potential snapshot data via SYNC method.")
        else:
             logger.warning("SYNC snapshot method returned None (indicating failure or no data).")

    except Exception as e:
        logger.error(f"An error occurred during the synchronous snapshot test: {e}", exc_info=True)
    finally:
        logger.info("Synchronous test finished.")

# --- Use direct call for main execution --- 
if __name__ == "__main__":
    # signal.signal(signal.SIGINT, handle_shutdown_signal) # COMMENTED OUT - WS specific
    # signal.signal(signal.SIGTERM, handle_shutdown_signal) # COMMENTED OUT - WS specific

    try:
        main_test() # Call the synchronous function directly
    except KeyboardInterrupt:
        logger.info("Test script interrupted by user (KeyboardInterrupt).")
    except Exception as e:
         logger.error(f"Unhandled exception in main execution: {e}", exc_info=True)
    finally:
         logger.info("Test script finished.") 