"""
Web application implementation for V3 of the financial application.
Handles HTTP endpoints and web interface.

Key features:
- FastAPI web server
- Account data endpoints
- Position data endpoints
- Order data endpoints
- Error handling and logging
"""

import os
import logging
import importlib # <-- ADD THIS LINE
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request, Form, WebSocket, WebSocketDisconnect, Body, Depends, status, BackgroundTasks # ADDED BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.base import JobLookupError # <-- ADD THIS LINE
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response, StreamingResponse # ADDED for setting cookies indirectly via response and StreamingResponse
import json
from pydantic import BaseModel, Field, ValidationError
from collections import Counter
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession # <-- ADD THIS IMPORT
from starlette.middleware.base import BaseHTTPMiddleware
import re
import math # Import math for ceiling division
import asyncio # Import asyncio for Lock AND to_thread
import sqlite3 # ADDED for sync DB access
import requests # ADDED for sync service
import urllib3 # ADDED for sync service
from asyncio import AbstractEventLoop # Specific import for type hint
import threading # Import threading for current_thread()
import uuid # <<< ADD THIS IMPORT
from datetime import datetime # <<< ADD THIS IMPORT
# --- Add CORS Middleware Import --- 
from fastapi.middleware.cors import CORSMiddleware
# --- End Import --- 

# --- Standard Library Imports ---
# ... (other imports)

# --- Local Application Imports ---
# Import necessary functions and classes from other modules
from .V3_ibkr_api import IBKRService, IBKRError, SyncIBKRService # ADD SyncIBKRService
from .V3_finviz_fetch import fetch_and_store_finviz_data, update_screener_from_finviz # Corrected import
from .V3_finviz_fetch import fetch_and_store_analytics_finviz
# from .V3_yahoo_api import YahooFinanceAPI, YahooError # Assuming Yahoo API integration
from .V3_investingcom_fetch import fetch_and_store_investingcom_data, update_screener_from_investingcom # Corrected import
from .V3_database import SQLiteRepository, get_exchange_rates, update_exchange_rate, add_or_update_exchange_rate_conid, update_screener_multi_fields_sync, get_screener_tickers_and_conids_sync, get_exchange_rates_and_conids_sync # Import new DB function AND SQLiteRepository
# --- End Local Application Imports ---

# --- Import V3_ibkr_monitor (Keep existing imports) ---
# ... existing imports ...
from .V3_database import SQLiteRepository # Ensure get_db is imported if not already
# --- ADD Import for IBKR Monitor and potentially analytics --- 
from .V3_ibkr_monitor import register_ibkr_monitor # Use alias to avoid name clash
# ADD Analytics import <<<<< ADD THIS LINE
from .V3_analytics import preprocess_raw_analytics_data # CORRECTED Import

# Configure logging
logging.basicConfig(
    level=logging.INFO, # Ensure level is INFO
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Global Job Status Tracking ---
job_statuses: Dict[str, Dict[str, Any]] = {} # Stores {job_id: {"status": "running/completed/failed", "message": "...", "timestamp": ...}}
job_status_lock = asyncio.Lock()
# --- End Global Job Status Tracking ---

# --- Global variable to store processed finviz data (replace with proper caching/DB later) ---
# global_processed_finviz_data: List[Dict[str, Any]] = [] # OLD
global_processed_analytics_data: List[Dict[str, Any]] = [] # NEW: Renamed global variable

# --- Dependency to provide IBKRService instance ---
def get_db_path() -> str:
    # Determine the absolute path to the database file relative to V3_web.py
    # Assuming V3_web.py is inside src/V3_app and the DB is in src/V3_app
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, 'V3_database.db')
    return db_path

# Create scheduler instance
scheduler = AsyncIOScheduler()

# --- ADD In-Memory Cache for Processed Finviz Data ---
processed_finviz_data_cache: List[Dict[str, Any]] = []
# --- END In-Memory Cache ---

# --- NEW Sync DB Helper --- 
def get_tickers_from_db_sync(db_path: str) -> List[str]:
    """Fetches unique tickers directly from the positions table using sqlite3."""
    tickers = set()
    try:
        logger.info(f"[Sync Job] Connecting to DB sync: {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Ensure this matches the actual table and column names
        cursor.execute("SELECT DISTINCT ticker FROM positions WHERE ticker IS NOT NULL AND ticker != ''")
        rows = cursor.fetchall()
        for row in rows:
            tickers.add(row[0])
        conn.close()
        logger.info(f"[Sync Job] Fetched {len(tickers)} unique tickers from DB: {list(tickers)}")
        return sorted(list(tickers))
    except sqlite3.Error as e:
        logger.error(f"[Sync Job] DB error fetching tickers: {e}")
        return []
    except Exception as e:
        logger.error(f"[Sync Job] Unexpected error fetching tickers: {e}")
        return []
# --- End Sync DB Helper ---

# --- NEW Sync DB Helper for Screener Data ---
def get_full_screener_data_sync(db_path: str) -> Dict[str, Dict[str, Any]]:
    """Fetches relevant fields (ticker, conid, company, beta, sector, industry)
       for all entries in the screener table using sqlite3.
       Returns a dictionary keyed by ticker.
    """
    results_dict = {}
    try:
        logger.info(f"[Sync Job] Connecting to DB sync for full screener data: {db_path}")
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row # Return rows as dict-like objects
            cursor = conn.cursor()
            # Select relevant fields from the screener table
            cursor.execute("SELECT ticker, conid, company, beta, sector, industry FROM screener")
            rows = cursor.fetchall()
            for row in rows:
                row_dict = dict(row)
                ticker = row_dict.get('ticker')
                if ticker:
                    results_dict[ticker] = row_dict
        logger.info(f"[Sync Job] Fetched full data for {len(results_dict)} tickers from screener.")
        return results_dict
    except sqlite3.Error as e:
        logger.error(f"[Sync Job] DB error fetching full screener data: {e}")
        return {} # Return empty dict on error
    except Exception as e:
        logger.error(f"[Sync Job] Unexpected error fetching full screener data: {e}")
        return {}
# --- End Sync Screener Helper ---

# --- NEW Sync DB Helper for Single Ticker Screener Data ---
def get_screener_data_for_ticker_sync(db_path: str, ticker: str) -> Optional[Dict[str, Any]]:
    """Fetches relevant fields for a single ticker from the screener table using sqlite3.
       Returns a dictionary for the ticker or None if not found or error.
    """
    result_dict = None
    try:
        # logger.debug(f"[Sync Job] Connecting to DB sync for single ticker screener data: {db_path}") # Keep commented
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row # Return rows as dict-like objects
            cursor = conn.cursor()
            # Select relevant fields from the screener table
            cursor.execute("SELECT ticker, conid, company, beta, sector, industry FROM screener WHERE ticker = ?", (ticker,))
            row = cursor.fetchone()
            if row:
                result_dict = dict(row)
                # logger.debug(f"[Sync Job] Fetched data for ticker {ticker}: {result_dict}") # Keep commented
            # else: # Keep commented
                # logger.debug(f"[Sync Job] Ticker {ticker} not found in screener.")
        return result_dict
    except sqlite3.Error as e:
        logger.error(f"[Sync Job] DB error fetching single screener data for {ticker}: {e}")
        return None # Return None on error
    except Exception as e:
        logger.error(f"[Sync Job] Unexpected error fetching single screener data for {ticker}: {e}")
        return None
# --- End Sync Single Ticker Helper ---

# --- Sync DB Helper for Updating Single Screener Field ---
# Renamed from update_screener_field_sync to _update_screener_single_field_sync
def _update_screener_single_field_sync(db_path: str, ticker: str, field_name: str, value: Any) -> bool:
    """Updates a specific single field (conid or sector for mismatch) for a ticker
       in the screener table using sqlite3. Also updates the updated_at timestamp.
    """
    # Allow only conid or sector (for mismatch case)
    allowed_fields = {'conid', 'sector'}
    if field_name not in allowed_fields:
        logger.error(f"[Sync Update Screener Single] Invalid field name specified: {field_name}")
        return False
        
    success = False
    now = datetime.now() # Get current timestamp
    try:
        # logger.debug(f"[Sync Update Screener Single] Connecting to DB sync to update {field_name} for {ticker}: {db_path}") # Keep commented
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            sql = f"UPDATE screener SET {field_name} = ?, updated_at = ? WHERE ticker = ?"
            cursor.execute(sql, (value, now, ticker))
            conn.commit()
            if cursor.rowcount > 0:
                # logger.debug(f"[Sync Update Screener Single] Successfully updated {field_name} to '{value}' and timestamp for ticker {ticker}") # Keep commented
                success = True
            else:
                logger.warning(f"[Sync Update Screener Single] Ticker {ticker} not found for updating {field_name}.")
                success = False
    except sqlite3.Error as e:
        logger.error(f"[Sync Update Screener Single] DB error updating {field_name} for {ticker}: {e}")
        success = False
    except Exception as e:
        logger.error(f"[Sync Update Screener Single] Unexpected error updating {field_name} for {ticker}: {e}")
        success = False
    return success
# --- End Sync Update Screener Single Field Helper ---

# --- NEW Sync Snapshot Job Function --- 
def run_sync_ibkr_snapshot_job(repository: SQLiteRepository, loop: AbstractEventLoop, manager: 'ConnectionManager', ibkr_base_url: str):
    # --- ADD ENTRY LOG --- 
    logger.info("[Sync Snapshot Job] Starting synchronous IBKR snapshot job...")
    db_path = repository.database_url.split("///", 1)[1] if repository.database_url.startswith("sqlite") else None
    if not db_path:
        logger.error("[Sync Snapshot Job] Could not extract database path from repository URL.")
        return # Cannot proceed without DB path

    # Define fields to fetch (map internal name to IBKR field code)
    # Price = 31, Percentage Change = 83 (often), Volume = 77 (often) - VERIFY THESE
    # Using placeholder _ prefixes for now as per snapshot response examples
    snapshot_fields_map = {
        "price": "31",
        "daychange": "83", 
        "ticker_symbol": "55", # Needed to confirm ticker, though conid is primary key
        "company_name": "7051",
        "sector_from_industry": "7280", 
        "industry_from_category": "7281",
        "beta": "7718",
        "sec_type": "6070", # Added SecType field
        "contract_desc": "7219", # Added Contract Description
        "contract_expiry": "7220" # Added Contract Expiry
        # Add others if needed, e.g., volume? "volume": "77"
    }
    ibkr_field_codes = list(snapshot_fields_map.values()) # Get all the numeric codes to request

    try:
        logger.info("[Sync Snapshot Job] Initializing SyncIBKRService.")
        sync_service = SyncIBKRService(base_url=ibkr_base_url)

        # --- Fetch Tickers/Currencies and ConIDs directly from DB --- 
        logger.info(f"[Sync Snapshot Job] Fetching stock tickers/conids from DB: {db_path}")
        stock_data = get_screener_tickers_and_conids_sync(db_path)
        logger.info(f"[Sync Snapshot Job] Fetched {len(stock_data)} stock items.")
        
        logger.info(f"[Sync Snapshot Job] Fetching FX currencies/conids from DB: {db_path}")
        fx_data = get_exchange_rates_and_conids_sync(db_path)
        logger.info(f"[Sync Snapshot Job] Fetched {len(fx_data)} FX items.")
        # --- End DB Fetch --- 

        # --- Build combined conid list and conid->identifier map --- 
        all_conids = []
        conid_map = {}
        processed_tickers = set() # Avoid duplicates if a ticker somehow maps to multiple conids
        processed_currencies = set()

        for item in stock_data:
            ticker = item.get('ticker')
            conid = item.get('conid')
            if ticker and conid and ticker not in processed_tickers:
                all_conids.append(conid)
                conid_map[conid] = {'identifier': ticker, 'type': 'stock'}
                processed_tickers.add(ticker)
            elif ticker in processed_tickers:
                logger.warning(f"[Sync Snapshot Job] Duplicate ticker {ticker} found with conid {conid} during map build. Using first encountered.")

        for item in fx_data:
            currency = item.get('currency')
            conid = item.get('conid')
            if currency and conid and currency not in processed_currencies:
                all_conids.append(conid)
                conid_map[conid] = {'identifier': currency, 'type': 'fx'}
                processed_currencies.add(currency)
            elif currency in processed_currencies:
                 logger.warning(f"[Sync Snapshot Job] Duplicate currency {currency} found with conid {conid} during map build. Using first encountered.")
        # --- End Build Map --- 

        # Log the map for debugging (optional)
        # logger.debug(f"[Sync Snapshot Job] Built conid_map: {conid_map}")

        if not all_conids:
            logger.warning("[Sync Snapshot Job] No valid conids found in the database (screener or exchange_rates). Skipping snapshot fetch.")
            return
            
        logger.info(f"[Sync Snapshot Job] Requesting snapshot for {len(all_conids)} conids.")
        # Fetch snapshot data using the combined list of conids
        snapshot_data = sync_service.fetch_snapshot_sync(all_conids, ibkr_field_codes)

        if snapshot_data:
            logger.info(f"[Sync Snapshot Job] Received {len(snapshot_data)} snapshot results. Processing...")
            fx_updates_dict = {} # Collect FX updates

            for item in snapshot_data:
                item_conid_str = item.get('conid') # ConID might be string in response
                if not item_conid_str:
                    logger.warning(f"[Sync Snapshot Job] Snapshot item missing 'conid': {item}")
                    continue
                
                try:
                    item_conid = int(item_conid_str)
                except (ValueError, TypeError):
                    logger.warning(f"[Sync Snapshot Job] Could not convert snapshot item conid '{item_conid_str}' to int. Item: {item}")
                    continue

                if item_conid in conid_map:
                    map_entry = conid_map[item_conid]
                    identifier = map_entry['identifier']
                    item_type = map_entry['type']
                    
                    # Extract price (always try field 31 for both stock and FX)
                    price_str = item.get(snapshot_fields_map["price"]) # Field 31
                    price = None
                    if price_str is not None:
                        try:
                            # Attempt to strip known prefixes like 'C' before converting
                            cleaned_price_str = price_str.lstrip('Cc') # Strip leading 'C' or 'c'
                            price = float(cleaned_price_str)
                        except (ValueError, TypeError):
                            logger.warning(f"[Sync Snapshot Job] Could not convert price '{price_str}' to float for {identifier} (conid: {item_conid}).")
                    
                    if item_type == 'stock':
                        ticker = identifier
                        
                        # --- Check for Allowed SecTypes (STK or IND) --- 
                        actual_sec_type = item.get(snapshot_fields_map.get("sec_type")) # Field 6070
                        if actual_sec_type not in ['STK', 'IND']:
                            logger.debug(f"[Sync Snapshot Job] Skipping screener update for {ticker} (conid: {item_conid}). Actual secType from snapshot: '{actual_sec_type}' (Expected STK or IND).")
                            continue # Skip to the next item in snapshot_data
                        # --- END Check --- 
                        
                        # --- Fetch Current Screener Data ---
                        current_screener_data = get_screener_data_for_ticker_sync(db_path, ticker)
                        if not current_screener_data:
                            logger.warning(f"[Sync Snapshot Job] Could not fetch current screener data for {ticker}. Skipping conditional updates, but will attempt price/change.")
                            # Set to empty dict to avoid errors in checks below, allows price/change update
                            current_screener_data = {}
                        # --- End Fetch Current Data ---

                        # --- Prepare updates - Common fields first (Unconditional) --- 
                        updates_for_ticker = {}
                        if price is not None: # Price is already extracted above
                            updates_for_ticker['price'] = price
                            
                        daychange_str = item.get(snapshot_fields_map["daychange"]) # Field 83
                        daychange = None
                        if daychange_str is not None:
                             try:
                                 daychange = float(daychange_str)
                                 updates_for_ticker['daychange'] = daychange # Add if valid
                             except (ValueError, TypeError):
                                 logger.warning(f"[Sync Snapshot Job] Could not convert daychange '{daychange_str}' to float for {ticker} (conid: {item_conid}).")
                        # --- End Common fields --- 
                        
                        # --- Extract and Add Other Fields (Conditional Update) --- 
                        company_name_snap = item.get(snapshot_fields_map["company_name"]) # 7051
                        sector_snap = item.get(snapshot_fields_map["sector_from_industry"]) # 7280
                        industry_snap = item.get(snapshot_fields_map["industry_from_category"]) # 7281
                        beta_str_snap = item.get(snapshot_fields_map["beta"]) # 7718
                        
                        beta_snap = None
                        if beta_str_snap is not None:
                            try:
                                beta_snap = float(beta_str_snap)
                            except (ValueError, TypeError):
                                logger.warning(f"[Sync Snapshot Job] Could not convert snapshot beta '{beta_str_snap}' to float for {ticker} (conid: {item_conid}).")

                        # Helper function to check if current DB value is considered empty/invalid
                        def is_db_value_empty(value):
                           return value is None or value == ''

                        # Add Company conditionally
                        current_company = current_screener_data.get('company')
                        if company_name_snap is not None and is_db_value_empty(current_company):
                            updates_for_ticker['Company'] = str(company_name_snap) # Ensure string
                            logger.debug(f"[Sync Snapshot Job] Adding Company '{company_name_snap}' for {ticker} (DB was empty/None).")
                        elif company_name_snap is not None:
                             logger.debug(f"[Sync Snapshot Job] Skipping Company update for {ticker} (DB already has value: '{current_company}'). Snapshot value: '{company_name_snap}'.")

                        # Add Sector conditionally
                        current_sector = current_screener_data.get('sector')
                        if sector_snap is not None and is_db_value_empty(current_sector):
                             updates_for_ticker['sector'] = str(sector_snap)
                             logger.debug(f"[Sync Snapshot Job] Adding Sector '{sector_snap}' for {ticker} (DB was empty/None).")
                        elif sector_snap is not None:
                              logger.debug(f"[Sync Snapshot Job] Skipping Sector update for {ticker} (DB already has value: '{current_sector}'). Snapshot value: '{sector_snap}'.")

                        # Add Industry conditionally
                        current_industry = current_screener_data.get('industry')
                        if industry_snap is not None and is_db_value_empty(current_industry):
                             updates_for_ticker['industry'] = str(industry_snap)
                             logger.debug(f"[Sync Snapshot Job] Adding Industry '{industry_snap}' for {ticker} (DB was empty/None).")
                        elif industry_snap is not None:
                             logger.debug(f"[Sync Snapshot Job] Skipping Industry update for {ticker} (DB already has value: '{current_industry}'). Snapshot value: '{industry_snap}'.")

                        # Add Beta unconditionally (if valid snapshot value exists)
                        # current_beta = current_screener_data.get('beta') # No longer need to fetch current beta for the check
                        if beta_snap is not None:
                            updates_for_ticker['beta'] = beta_snap
                            # Adjust log message to reflect unconditional update
                            logger.debug(f"[Sync Snapshot Job] Adding Beta '{beta_snap}' for {ticker} from snapshot.") 
                        # --- End Conditional/Unconditional Other Fields ---
                                
                        # Proceed with update if any fields were collected
                        if updates_for_ticker:
                            logger.debug(f"[Sync Snapshot Job] Updating screener for {ticker} ({actual_sec_type}) with fields: {list(updates_for_ticker.keys())}")
                            # logger.debug(f"Full updates dict: {updates_for_ticker}") # Optional verbose logging
                            success = update_screener_multi_fields_sync(db_path, ticker, updates_for_ticker)
                            if not success:
                                logger.error(f"[Sync Snapshot Job] Failed to update screener for {ticker}.")
                        else:
                            logger.debug(f"[Sync Snapshot Job] No applicable updates found for stock {ticker} (conid: {item_conid}) after checking current DB values.")
                    
                    elif item_type == 'fx':
                        currency = identifier
                        if price is not None: # Price field (_31) is used for FX rate
                            logger.debug(f"[Sync Snapshot Job] Received price {price} for FX {currency} (conid: {item_conid}). Calculating inverse.")
                            # --- ADD INVERSION LOGIC --- 
                            if price > 0:
                                inverse_rate = 1.0 / price
                                fx_updates_dict[currency] = inverse_rate
                                logger.debug(f"[Sync Snapshot Job] Queued inverse rate update for {currency}: {inverse_rate:.6f}")
                            else:
                                 logger.warning(f"[Sync Snapshot Job] Skipping FX rate update for {currency} (conid: {item_conid}): Non-positive price {price} received.")
                            # --- END INVERSION LOGIC ---
                        else:
                             logger.debug(f"[Sync Snapshot Job] No valid rate (field {snapshot_fields_map['price']}) extracted from snapshot for FX {currency} (conid: {item_conid}).")
                
                else:
                     logger.warning(f"[Sync Snapshot Job] Received snapshot data for unexpected conid: {item_conid}. Data: {item}")

            # Update all collected FX rates in one go
            if fx_updates_dict:
                logger.info(f"[Sync Snapshot Job] Updating {len(fx_updates_dict)} exchange rates in DB...")
                repository.update_exchange_rates_sync(fx_updates_dict) # Use the existing sync method
            else:
                 logger.info("[Sync Snapshot Job] No FX rates to update.")

        else:
            logger.warning("[Sync Snapshot Job] No snapshot data received from IBKR.")
            # Optionally: Send error status via websocket
            # message = json.dumps({"type": "status", "service": "ibkr_snapshot", "status": "error", "message": "Failed to fetch snapshot data"})
            # loop.call_soon_threadsafe(asyncio.create_task, manager.broadcast(message))

        # --- REMOVED: Original ticker/conid fetching and loop --- 

        logger.info("[Sync Snapshot Job] Synchronous IBKR snapshot job finished.")
        # Optionally: Send success status via websocket
        # message = json.dumps({"type": "status", "service": "ibkr_snapshot", "status": "success", "message": "Snapshot data updated"})
        # loop.call_soon_threadsafe(asyncio.create_task, manager.broadcast(message))

    except Exception as e:
        logger.error(f"[Sync Snapshot Job] Error during execution: {e}", exc_info=True)
        # Optionally: Send error status via websocket
        # message = json.dumps({"type": "status", "service": "ibkr_snapshot", "status": "error", "message": str(e)})
        # loop.call_soon_threadsafe(asyncio.create_task, manager.broadcast(message))

# --- END SYNC SNAPSHOT JOB ---

# --- WebSocket Connection Manager --- 
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected: {websocket.client}. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket disconnected: {websocket.client}. Total connections: {len(self.active_connections)}")
        else:
             logger.warning(f"WebSocket {websocket.client} already removed or not found for disconnect.")

    async def broadcast(self, message: str):
        disconnected_clients = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e: # Catch broader exceptions including WebSocket closed errors
                logger.warning(f"Failed to send message to WebSocket {connection.client}: {e}. Marking for removal.")
                disconnected_clients.append(connection)
        
        # Remove clients that failed to send (likely disconnected)
        for client in disconnected_clients:
            if client in self.active_connections:
                 self.active_connections.remove(client)
# --- End WebSocket Connection Manager ---

# --- Restore Pydantic Models Here ---
from pydantic import BaseModel, Field # Ensure BaseModel is imported
from typing import Optional, Dict, Any, Union # Add needed types

class TickerUpdate(BaseModel):
   ticker: str
   status: str

class TickerDelete(BaseModel):
   ticker: str

class UpdateTickerDetailsRequest(BaseModel):
   ticker: str
   field: str
   value: Union[str, float, int, None]

class UpdateRowRequest(BaseModel):
   ticker: str
   updates: Dict[str, Any]

class PortfolioRuleBase(BaseModel):
    rule_name: str
    min_value: float = 0.0
    max_value: float = 0.0
    description: Optional[str] = None
    is_active: bool = True

class PortfolioRuleCreate(PortfolioRuleBase):
    pass

class PortfolioRuleUpdate(BaseModel):
    rule_id: int
    rule_name: Optional[str] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None

class PortfolioRuleDelete(BaseModel):
    rule_id: int

class RateUpdatePayload(BaseModel):
    # Use Field alias to match form input names if needed, or keep Pythonic names
    usd_rate: Optional[float] = Field(None, alias='usd_rate') # Alias allows matching form names
    gbp_rate: Optional[float] = Field(None, alias='gbp_rate')

class ScheduleUpdatePayload(BaseModel):
    job_id: str # e.g., 'finviz_data_fetch', 'yahoo_data_fetch', 'macro_data_fetch'
    schedule: str # The raw schedule string from the input field
    # We might add is_active later

# Payload for generic schedule updates
class GenericScheduleUpdatePayload(BaseModel):
    job_id: str 
    schedule: str # Raw input (e.g., "daily", "3 hours", "900")

# Payload for status update
class JobStatusUpdatePayload(BaseModel):
    job_id: str
    is_active: bool

# --- NEW Pydantic Model for Ticker List Payload ---
class TickerListPayload(BaseModel):
    tickers: List[str] = Field(..., min_items=1) # Require at least one ticker
# --- END NEW Model ---

class AccountPayload(BaseModel):
    account_id: str = Field(..., min_length=1)
    net_liquidation: float = 0.0
    total_cash: float = 0.0
    gross_position_value: float = 0.0
    currency: str = 'EUR' # Default to EUR, can be overridden

# --- End Pydantic Models ---

# --- Helper Function for Interval Formatting ---
def interval_to_human(seconds: Optional[int]) -> str:
    """Converts seconds into a human-readable string (e.g., 1 day, 6 hours, 900 secs)."""
    if seconds is None or seconds <= 0:
        return "Not Set"
    
    if seconds % 86400 == 0:
        days = seconds // 86400
        return f"{days} day{'s' if days > 1 else ''}"
    if seconds % 3600 == 0:
        hours = seconds // 3600
        return f"{hours} hour{'s' if hours > 1 else ''}"
    if seconds % 60 == 0:
         minutes = seconds // 60
         return f"{minutes} minute{'s' if minutes > 1 else ''}"
    
    return f"{seconds} second{'s' if seconds > 1 else ''}" # Default to seconds
# --- End Helper Function ---

# --- Database Session Middleware --- 
from .V3_database import SQLiteRepository # Change to relative import

class DBSessionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        repository: SQLiteRepository = request.app.state.repository
        async with repository.get_session() as session: # Use repository's session factory
            request.state.db_session = session
            response = await call_next(request)
        return response
# --- End Middleware ---

# --- NEW Dependency Function --- 
def get_repository(request: Request) -> SQLiteRepository:
    """Dependency function to get the repository instance from app state."""
    # --- Restore original implementation ---
    # Ensure the repository exists in state
    if not hasattr(request.app.state, 'repository') or request.app.state.repository is None:
        logger.error("CRITICAL: Repository not found in application state!")
        # Raise an internal server error because this is a configuration issue
        raise HTTPException(status_code=500, detail="Internal server error: Repository not initialized.")
    return request.app.state.repository
    # --- End original implementation ---
# --- End Dependency Function ---

# --- NEW Dependency Function for IBKR Service ---
async def get_ibkr_service(request: Request) -> IBKRService:
    """Dependency function to get the IBKRService instance from app state."""
    if not hasattr(request.app.state, 'ibkr_service') or request.app.state.ibkr_service is None:
        logger.error("CRITICAL: IBKRService not found in application state!")
        raise HTTPException(status_code=500, detail="Internal server error: IBKRService not initialized.")
    # Optional: Add a check here to ensure the service is connected/authenticated if needed
    return request.app.state.ibkr_service
# --- End Dependency Function ---

# --- JSON Serializer Helper ---
def json_datetime_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))
# --- End Helper ---

# --- Dependency for Database Session ---
async def get_db(request: Request) -> AsyncSession:
    """Dependency function to get the database session from request state."""
    # Ensure the session exists in state (added by middleware)
    if not hasattr(request.state, 'db_session') or request.state.db_session is None:
        logger.error("CRITICAL: Database session not found in request state! Middleware might not be running.")
        raise HTTPException(status_code=500, detail="Internal server error: Database session not available.")
    return request.state.db_session
# --- End Dependency ---

# --- Pydantic Models for API ---

class InstrumentSearchRequest(BaseModel):
    identifier: str
    status: str # Get the combined status/type field value
    name: bool = False # NEW: Flag for name vs symbol search, defaults to False

class InstrumentSearchResponse(BaseModel):
    contracts: List[Dict[str, Any]]

# --- Pydantic Model for Add Instrument Form Data ---
class AddInstrumentRequest(BaseModel):
    identifier: str
    status: str
    conid: Optional[str] = None # Make conid optional initially, validate in endpoint
    atr: Optional[str] = None        # Accept as string
    atr_mult: Optional[str] = None   # Accept as string
    risk: Optional[str] = None       # Accept as string
    beta: Optional[str] = None       # Accept as string
    sector: Optional[str] = None
    industry: Optional[str] = None
    comments: Optional[str] = None
    # Add any other fields from the form if needed

# --- End API Models ---

def create_app():
    try: # <-- Add try block here
        # --- Correct Indentation Starts Here ---
        app = FastAPI(title="Financial App V3") # Revert: Keep title

        # Revert: Put back static files setup?
        # Assuming it was correct before
        current_dir = os.path.dirname(os.path.abspath(__file__))
        static_dir = os.path.join(current_dir, "static")
        os.makedirs(static_dir, exist_ok=True)
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

        # Revert: Database path and initialization?
        # Need to use the global DATABASE_URL here if we keep it global
        # This part seems problematic, let's stick to fixing the NameError for now
        # Let's assume repository init is done correctly later in startup
        # db_path = os.path.join(current_dir, 'V3_database.db')
        # db_url = f"sqlite+aiosqlite:///{db_path}"
        # repository = SQLiteRepository(db_url)
        # asyncio.run(repository.create_tables())
        # ibkr_service = IBKRService(repository)
        # app.state.repository = repository
        # app.state.ibkr_service = ibkr_service

        # Revert: Templates setup?
        templates = Jinja2Templates(directory=os.path.join(current_dir, "templates"))
        app.state.templates = templates # Store templates

        # --- Define Theme Context Processor Function ---
        # (No decorator needed here)
        async def theme_processor(request: Request) -> Dict[str, str]:
            """Reads theme cookie and returns it for template context."""
            current_theme = request.cookies.get("theme_preference", "light") # Default to 'light'
            if current_theme not in ['light', 'dark']:
                current_theme = 'light'
            return {"current_theme": current_theme}
        
        # --- Add Processor to Template Globals --- 
        templates.env.globals['theme_processor'] = theme_processor 
        # Note: Jinja calls this function when rendering. We pass the function itself.
        logger.info("Theme processor function added to template globals.")
        # --- End Adding Processor ---

        # Revert: Helper function
        templates.env.globals['interval_to_human'] = interval_to_human

        # Revert: Manager setup
        manager = ConnectionManager()
        app.state.manager = manager

        # Revert: Scheduler setup
        app.state.scheduler = scheduler

        # Revert: Lock setup
        app.state.job_execution_lock = asyncio.Lock()

        

        # --- Define constants INSIDE create_app ---
        # --- Correct the default DATABASE_URL --- 
        correct_db_path = "src/V3_app/V3_database.db" # Relative path from project root
        default_db_url = f"sqlite+aiosqlite:///{correct_db_path}"
        DATABASE_URL = os.environ.get("DATABASE_URL", default_db_url) 
        logger.info(f"Using effective DATABASE_URL: {DATABASE_URL}") # Log the URL being used
        # --- End correction --- 

        IBKR_BASE_URL = os.environ.get("IBKR_BASE_URL", "https://localhost:5000/v1/api/")
        # Ensure the DB path part exists for file-based DBs
        db_path_part = DATABASE_URL.split("///")[-1]
        if ":memory:" not in db_path_part: # Avoid trying to create dirs for in-memory DB
            db_dir = os.path.dirname(db_path_part)
            if db_dir: # Ensure there is a directory part
                os.makedirs(db_dir, exist_ok=True)
                logger.info(f"Ensured database directory exists: {db_dir}")
        # --- End constants definition & dir check ---

        # --- Repository Initialization ---
        # --- Wrap repository init in its own try/except ---
        try:
            repository = SQLiteRepository(database_url=DATABASE_URL)
            # --- Add Logging for DB URL --- 
            logger.info(f"Initialized SQLiteRepository with DB URL: {repository.database_url}")
            # --- End Logging ---
            # Store repository in app state AFTER successful initialization
            app.state.repository = repository
            # Initialize IBKRService AFTER repository is successfully created and stored
            # Pass the repository instance to the service
            # --- FIX: Remove base_url argument --- 
            ibkr_service = IBKRService(repository=repository)
            # --- END FIX ---
            app.state.ibkr_service = ibkr_service
            logger.info("Repository and IBKRService initialized successfully.")
        except Exception as repo_init_error:
            logger.error(f"CRITICAL: Failed to initialize Repository or IBKRService: {repo_init_error}", exc_info=True)
            # Decide how to handle this - maybe return None or re-raise? Re-raising seems appropriate.
            raise repo_init_error # Re-raise to be caught by the outer try/except
        # --- End wrap repository init ---

        # --- Add helper function to Jinja globals ---
        app.state.templates.env.globals['interval_to_human'] = interval_to_human
        # --- End Add ---

        # --- Store manager in app state ---
        manager = ConnectionManager() # ADDED: Instantiate the manager
        app.state.manager = manager
        # --- End Store ---

        # --- Store scheduler in app state ---
        app.state.scheduler = scheduler
        # --- End Store ---

        # --- Initialize Job Execution Lock ---
        app.state.job_execution_lock = asyncio.Lock()
        # --- End Lock Initialization ---

        # --- Add CORS Middleware --- 
        # Define allowed origins for development (adjust if your frontend runs on a different port)
        origins = [
            "http://localhost",
            "http://localhost:8000", # Common development port
            "http://127.0.0.1",
            "http://127.0.0.1:8000",
            # Add other origins if necessary
        ]

        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True, # Allow cookies if needed
            allow_methods=["*"],    # Allow all methods (GET, POST, etc.)
            allow_headers=["*"],    # Allow all headers
        )
        logger.info(f"CORS middleware added, allowing origins: {origins}")
        # --- End Add CORS --- 

        # --- Add Middleware --- 
        app.add_middleware(DBSessionMiddleware)
        logger.info("Database session middleware added.")
        # --- End Add --- 

        async def run_initial_fetch():
            """Background task to fetch data on startup."""
            await asyncio.sleep(5) 
            logger.info("Starting initial data fetch on application startup...")
            try:
                repository: SQLiteRepository = app.state.repository
                # Check if the job is active before running initial fetch
                is_active = await repository.get_job_is_active('ibkr_fetch', default_active=False)

                if is_active:
                    logger.info("Job 'ibkr_fetch' is active. Proceeding with initial data fetch.")
                    ibkr_service: IBKRService = app.state.ibkr_service
                    # --- Temporarily disable data clearing to avoid startup lock issues ---
                    # logger.info("Skipping data clearing during initial fetch.")
                    # await ibkr_service.repository.clear_all_data() 
                    # --- End disable --- 
                    await ibkr_service.fetch_data() # This method handles auth, clearing, and fetching all data
                    logger.info("Initial data fetch completed successfully.")
                else:
                    logger.info("Job 'ibkr_fetch' is inactive. Skipping initial data fetch.")

            except Exception as e:
                logger.error(f"Error during initial data fetch: {str(e)}")
                logger.exception("Initial data fetch failed:")
        
        # --- NEW: Initial Finviz Fetch Function ---
        async def run_initial_finviz_fetch():
            """Background task to fetch Finviz data on startup."""
            await asyncio.sleep(6) # Slightly offset from IBKR fetch
            logger.info("Starting initial Finviz data fetch on application startup...")
            finviz_job_id = 'finviz_data_fetch' # CORRECTED job ID to match DB/Scheduler
            try:
                # Need to import these at the top of V3_web.py if not already done
                from .V3_finviz_fetch import fetch_and_store_finviz_data, update_screener_from_finviz
                
                repository: SQLiteRepository = app.state.repository
                # Check if the finviz job is active before running
                is_active = await repository.get_job_is_active(finviz_job_id, default_active=False) # Default to False if not found

                if is_active:
                    logger.info(f"Job '{finviz_job_id}' is active. Proceeding with initial Finviz data fetch and update.")
                    # Acquire lock? Consider if necessary/shared with other fetches
                    # For now, assume it runs independently or locking is handled within functions
                    
                    logger.info(f"Running fetch_and_store_finviz_data for '{finviz_job_id}'...")
                    await fetch_and_store_finviz_data(repository)
                    logger.info(f"fetch_and_store_finviz_data completed for '{finviz_job_id}'.")
                    
                    logger.info(f"Running update_screener_from_finviz for '{finviz_job_id}'...")
                    await update_screener_from_finviz(repository)
                    logger.info(f"update_screener_from_finviz completed for '{finviz_job_id}'.")

                    logger.info(f"Initial Finviz data fetch and update completed successfully for '{finviz_job_id}'.")
                else:
                    logger.info(f"Job '{finviz_job_id}' is inactive. Skipping initial Finviz data fetch.")

            except ImportError as imp_err:
                 logger.error(f"CRITICAL: Failed to import Finviz functions for initial fetch: {imp_err}. Ensure V3_finviz_fetch.py exists and is accessible.", exc_info=True)
            except Exception as e:
                logger.error(f"Error during initial Finviz data fetch for '{finviz_job_id}': {str(e)}")
                logger.exception(f"Initial Finviz data fetch failed for '{finviz_job_id}':")
        # --- END NEW Finviz Fetch Function ---

        async def scheduled_fetch_job():
            """Job function that the scheduler will call and notify clients."""
            job_id = "ibkr_fetch" # Define job ID for checks
            logger.info(f"Scheduler executing {job_id}...")

            # --- ADD Check is_active status --- 
            try:
                repository: SQLiteRepository = app.state.repository
                is_active = await repository.get_job_is_active(job_id)
                if not is_active:
                    logger.info(f"Job '{job_id}' is inactive in DB. Skipping execution.")
                    return
            except Exception as check_err:
                logger.error(f"Error checking active status for {job_id}: {check_err}. Skipping execution.", exc_info=True)
                return
            # --- END Check ---

            fetch_successful = False
            job_lock: asyncio.Lock = app.state.job_execution_lock
            
            if job_lock.locked():
                logger.warning("Skipping scheduled_fetch_job: Another job is already running.")
                return
            
            await job_lock.acquire() # Acquire lock
            all_account_ids = [] # Store account IDs fetched
            try:
                repository: SQLiteRepository = app.state.repository
                is_active = await repository.get_job_is_active('ibkr_fetch', default_active=False)
                
                if is_active:
                    logger.info("Job 'ibkr_fetch' is active. Proceeding with full data fetch.")
                    ibkr_service: IBKRService = app.state.ibkr_service 
                    repository: SQLiteRepository = app.state.repository
                    
                    fetch_successful = False # Reset fetch successful flag
                    async with ibkr_service:
                        logger.info("Main IBKR Service connected for scheduled job.")
                        
                        target_account = "U15495849" 
                        logger.info(f"Scheduled job: Attempting to switch main session context to {target_account}...")
                        switched = await ibkr_service.switch_account(target_account)
                        if not switched:
                            logger.error(f"Scheduled job: FAILED to switch context to {target_account}. Aborting data fetch.")
                        else: 
                            logger.info(f"Scheduled job: Successfully switched main context to {target_account}.")
                            all_account_ids = []
                            try:
                                logger.info("Scheduled job: Calling /iserver/accounts prerequisite...")
                                await ibkr_service.get_server_accounts()
                                accounts = await ibkr_service.fetch_accounts()
                                if accounts:
                                    all_account_ids = [acc['account_id'] for acc in accounts if acc.get('account_id')]
                                    logger.info(f"Scheduled job: Found {len(all_account_ids)} accounts listed: {all_account_ids}")
                                else:
                                     logger.warning("Scheduled job: No accounts found via fetch_accounts.")
                                
                                for account_id in all_account_ids:
                                    try:
                                        logger.info(f"Scheduled job: Fetching pos/ord for account {account_id}...")
                                        await ibkr_service.fetch_positions(account_id)
                                        await ibkr_service.fetch_orders(account_id)
                                    except Exception as acc_e:
                                        logger.error(f"Scheduled job: Error fetching pos/ord for {account_id}: {acc_e}")
                                
                                fetch_successful = True
                            
                            except Exception as fetch_err:
                                 logger.error(f"Scheduled job: Error during standard async data fetches: {fetch_err}", exc_info=True)
                                 fetch_successful = False
                                
                        logger.info("Scheduled job (account/pos/order fetch) finished.")
                        
                    logger.info("Main IBKR Service disconnected after scheduled job.")
                        
                else:
                    logger.info("Job 'ibkr_fetch' is inactive. Skipping data fetch.")
                    
            except Exception as e:
                logger.error(f"Error during scheduled data fetch execution: {str(e)}")
                logger.exception("Scheduled data fetch failed:")
            finally:
                job_lock.release() # Release lock in finally block
            
            # --- Temporarily comment out the problematic block for diagnostics ---
            # if fetch_successful:
            #     logger.info("Broadcasting data update notification to WebSocket clients.")
            #     try:
            #         now = datetime.now()
            #         logger.info(f"Updating last_run for job 'ibkr_fetch' to {now}")
            #         # await repository.update_job_config('ibkr_fetch', {'last_run': now}) # Line 206
            #     except Exception as db_update_err:
            #          logger.error(f"Failed to update last_run time for ibkr_fetch: {db_update_err}")
            # --- End temporary comment ---

        # --- NEW Finviz Scheduled Job --- 
        async def scheduled_finviz_job():
            """Job function for fetching Finviz data and updating screener."""
            job_id = "finviz_data_fetch"
            logger.info(f"Scheduler executing {job_id}...")

            # --- ADD Check is_active status --- 
            try:
                repository: SQLiteRepository = app.state.repository
                is_active = await repository.get_job_is_active(job_id)
                if not is_active:
                    logger.info(f"Job '{job_id}' is inactive in DB. Skipping execution.")
                    return
            except Exception as check_err:
                logger.error(f"Error checking active status for {job_id}: {check_err}. Skipping execution.", exc_info=True)
                return
            # --- END Check ---

            fetch_successful = False
            update_successful = False
            job_lock: asyncio.Lock = app.state.job_execution_lock
            
            if job_lock.locked():
                logger.warning(f"Skipping {job_id}: Another job is already running.")
                return
            
            await job_lock.acquire() # Acquire lock
            try:
                repository: SQLiteRepository = app.state.repository
                is_active = await repository.get_job_is_active(job_id, default_active=False)
                
                if is_active:
                    logger.info(f"Job '{job_id}' is active. Proceeding with fetch and update.")
                    await fetch_and_store_finviz_data(repository)
                    fetch_successful = True # Mark fetch as attempted/done
                    
                    await update_screener_from_finviz(repository)
                    update_successful = True # Mark update as attempted/done
                    
                    logger.info(f"Scheduled Finviz fetch and screener update completed.")
                    
                    # --- Broadcast Update --- 
                    try:
                        logger.info(f"[{job_id}] Broadcasting data update notification.")
                        await app.state.manager.broadcast(json.dumps({"event": "data_updated"}))
                    except Exception as broadcast_err:
                        logger.error(f"[{job_id}] Error during broadcast: {broadcast_err}")
                    # --- End Broadcast --- 
                else:
                    logger.info(f"Job '{job_id}' is inactive. Skipping execution.")
                    
            except Exception as e:
                logger.error(f"Error during scheduled {job_id} execution: {str(e)}")
                logger.exception(f"Scheduled {job_id} failed:")
            finally:
                 job_lock.release() # Release lock in finally block
                 logger.debug(f"Job lock released by {job_id}")
            
            # Update last_run time in DB if the job was active (regardless of internal success)
            if is_active: 
                try:
                    now = datetime.now()
                    logger.info(f"Updating last_run for job '{job_id}' to {now}")
                    await repository.update_job_config(job_id, {'last_run': now})
                except Exception as db_update_err:
                     logger.error(f"Failed to update last_run time for {job_id}: {db_update_err}")

        # --- NEW Investing.com Scheduled Job --- 
        async def scheduled_investingcom_job():
            """Job function for fetching Investing.com data and updating screener."""
            job_id = "investingcom_data_fetch"
            logger.info(f"Scheduler executing {job_id}...")

            # --- ADD Check is_active status --- 
            try:
                repository: SQLiteRepository = app.state.repository
                is_active = await repository.get_job_is_active(job_id)
                if not is_active:
                    logger.info(f"Job '{job_id}' is inactive in DB. Skipping execution.")
                    return
            except Exception as check_err:
                logger.error(f"Error checking active status for {job_id}: {check_err}. Skipping execution.", exc_info=True)
                return
            # --- END Check ---

            fetch_successful = False
            update_successful = False
            job_lock: asyncio.Lock = app.state.job_execution_lock
            
            if job_lock.locked():
                logger.warning(f"Skipping {job_id}: Another job is already running.")
                return
            
            await job_lock.acquire() # Acquire lock
            try:
                repository: SQLiteRepository = app.state.repository
                is_active = await repository.get_job_is_active(job_id, default_active=False)
                
                if is_active:
                    logger.info(f"Job '{job_id}' is active. Proceeding with fetch and update.")
                    await fetch_and_store_investingcom_data(repository)
                    fetch_successful = True # Mark fetch as attempted/done
                    
                    await update_screener_from_investingcom(repository)
                    update_successful = True # Mark update as attempted/done
                    
                    logger.info(f"Scheduled Investing.com fetch and screener update completed.")
                    
                    # --- Broadcast Update --- 
                    try:
                        logger.info(f"[{job_id}] Broadcasting data update notification.")
                        await app.state.manager.broadcast(json.dumps({"event": "data_updated"}))
                    except Exception as broadcast_err:
                        logger.error(f"[{job_id}] Error during broadcast: {broadcast_err}")
                    # --- End Broadcast --- 
                else:
                    logger.info(f"Job '{job_id}' is inactive. Skipping execution.")
                    
            except Exception as e:
                logger.error(f"Error during scheduled {job_id} execution: {str(e)}")
                logger.exception(f"Scheduled {job_id} failed:")
            finally:
                 job_lock.release() # Release lock in finally block
                 logger.debug(f"Job lock released by {job_id}")

            # Update last_run time in DB if the job was active (regardless of internal success)
            if is_active: 
                try:
                    now = datetime.now()
                    logger.info(f"Updating last_run for job '{job_id}' to {now}")
                    await repository.update_job_config(job_id, {'last_run': now})
                except Exception as db_update_err:
                     logger.error(f"Failed to update last_run time for {job_id}: {db_update_err}")

        # --- NEW Yahoo Scheduled Job --- 
        async def scheduled_yahoo_job():
            """Job function for fetching Yahoo data and updating screener."""
            job_id = "yahoo_data_fetch"
            logger.info(f"Scheduler executing {job_id}...")

            # --- ADD Check is_active status --- 
            try:
                repository: SQLiteRepository = app.state.repository
                is_active = await repository.get_job_is_active(job_id)
                if not is_active:
                    logger.info(f"Job '{job_id}' is inactive in DB. Skipping execution.")
                    return
            except Exception as check_err:
                logger.error(f"Error checking active status for {job_id}: {check_err}. Skipping execution.", exc_info=True)
                return
            # --- END Check ---

            fetch_successful = False
            update_successful = False
            job_lock: asyncio.Lock = app.state.job_execution_lock
            
            if job_lock.locked():
                logger.warning(f"Skipping {job_id}: Another job is already running.")
                return
            
            await job_lock.acquire() # Acquire lock
            try:
                repository: SQLiteRepository = app.state.repository
                is_active = await repository.get_job_is_active(job_id, default_active=False)
                
                if is_active:
                    logger.info(f"Job '{job_id}' is active. Proceeding with fetch and update.")
                    await fetch_and_store_yahoo_data(repository)
                    fetch_successful = True # Mark fetch as attempted/done
                    
                    await update_screener_from_yahoo(repository)
                    update_successful = True # Mark update as attempted/done
                    
                    logger.info(f"Scheduled Yahoo fetch and screener update completed.")
                    
                    # --- Broadcast Update --- 
                    try:
                        logger.info(f"[{job_id}] Broadcasting data update notification.")
                        await app.state.manager.broadcast(json.dumps({"event": "data_updated"}))
                    except Exception as broadcast_err:
                        logger.error(f"[{job_id}] Error during broadcast: {broadcast_err}")
                    # --- End Broadcast --- 
                else:
                    logger.info(f"Job '{job_id}' is inactive. Skipping execution.")
                    
            except Exception as e:
                logger.error(f"Error during scheduled {job_id} execution: {str(e)}")
                logger.exception(f"Scheduled {job_id} failed:")
            finally:
                 job_lock.release() # Release lock in finally block
                 logger.debug(f"Job lock released by {job_id}")

            # Update last_run time in DB if the job was active (regardless of internal success)
            if is_active: 
                try:
                    now = datetime.now()
                    logger.info(f"Updating last_run for job '{job_id}' to {now}")
                    await repository.update_job_config(job_id, {'last_run': now})
                except Exception as db_update_err:
                     logger.error(f"Failed to update last_run time for {job_id}: {db_update_err}")

        @app.on_event("startup")
        async def startup_event():
            """Run initial fetch and start the scheduler."""
            logger.info("Application startup: Initializing scheduler and jobs...")
            try:
                repository: SQLiteRepository = app.state.repository
                
                # --- Add table creation step --- 
                logger.info("Ensuring all database tables exist...")
                await repository.create_tables() # Call method to create tables if they don't exist
                logger.info("Database tables checked/created.")
                # --- End table creation --- 
                
                # --- Determine DB Path for Sync Job --- 
                # Assuming db_path is consistent and accessible here
                # --- Construct DB URL instead of just path --- 
                db_url_for_sync_job = app.state.repository.database_url # Get the URL from the main repository
                logger.info(f"Using DB URL for sync job: {db_url_for_sync_job}")
                # --- End DB URL --- 
                
                global scheduler # Ensure we are using the global scheduler instance
                if not scheduler:
                    scheduler = AsyncIOScheduler()
                    # --- Store scheduler in app state --- 
                    # Store it right after creation if it wasn't already running
                    app.state.scheduler = scheduler 
                    # --- End Store --- 
                
                # --- Get Event Loop and Manager for Sync Job --- 
                loop = asyncio.get_running_loop()
                manager: ConnectionManager = app.state.manager # Get manager from state
                # --- End Get --- 
                 
                # --- Schedule IBKR Job --- 
                ibkr_db_job_id = 'ibkr_fetch' # ID used in DB config
                current_interval = await repository.get_fetch_interval_seconds(default_interval=3600)
                ibkr_is_active = await repository.get_job_is_active(ibkr_db_job_id, default_active=True) # Check DB

                # Always add the job
                if current_interval > 0:
                    try:
                        scheduler.add_job(
                            scheduled_fetch_job, 
                            'interval', 
                            seconds=current_interval, 
                            id=ibkr_db_job_id, 
                            replace_existing=True
                        )
                        logger.info(f"Added job '{ibkr_db_job_id}' with interval: {current_interval} seconds.")
                        # Pause it immediately if it's not active
                        if not ibkr_is_active:
                            scheduler.pause_job(ibkr_db_job_id)
                            logger.info(f"Job '{ibkr_db_job_id}' is inactive in DB. Paused in scheduler.")
                    except Exception as e:
                         logger.error(f"Failed to add or pause job '{ibkr_db_job_id}': {e}", exc_info=True)
                else:
                    logger.warning(f"Job '{ibkr_db_job_id}' has invalid interval ({current_interval}s). Not scheduling.")
                # --- End Schedule IBKR Job --- 

                # --- Schedule Finviz Job --- 
                finviz_job_id = "finviz_data_fetch"
                finviz_default_seconds = 86400 # Default to 1 day
                finviz_interval_seconds = await repository.get_job_schedule_seconds(finviz_job_id, default_seconds=finviz_default_seconds)
                finviz_is_active = await repository.get_job_is_active(finviz_job_id, default_active=True) # Check DB

                # Always add the job if interval is valid
                if finviz_interval_seconds > 0:
                    try:
                        scheduler.add_job(
                            scheduled_finviz_job, 
                            trigger='interval', 
                            seconds=finviz_interval_seconds, 
                            id=finviz_job_id, 
                            replace_existing=True
                        )
                        logger.info(f"Added job '{finviz_job_id}' with interval: {finviz_interval_seconds} seconds.")
                        # Pause it immediately if it's not active
                        if not finviz_is_active:
                            scheduler.pause_job(finviz_job_id)
                            logger.info(f"Job '{finviz_job_id}' is inactive in DB. Paused in scheduler.")
                    except Exception as e:
                         logger.error(f"Failed to add or pause job '{finviz_job_id}': {e}", exc_info=True)
                else:
                    logger.warning(f"Job '{finviz_job_id}' has invalid interval ({finviz_interval_seconds}s). Not scheduling.")
                # --- End Schedule Finviz Job ---

                # --- Schedule Investing.com Job --- 
                investingcom_job_id = "investingcom_data_fetch"
                investingcom_default_seconds = 86400 # Default to 1 day
                investingcom_interval_seconds = await repository.get_job_schedule_seconds(investingcom_job_id, default_seconds=investingcom_default_seconds)
                investingcom_is_active = await repository.get_job_is_active(investingcom_job_id, default_active=True) # Check DB
                
                # Always add the job if interval is valid
                if investingcom_interval_seconds > 0:
                    try:
                        scheduler.add_job(
                            scheduled_investingcom_job, 
                            trigger='interval',
                            seconds=investingcom_interval_seconds,
                            id=investingcom_job_id, # Ensure unique ID
                            replace_existing=True
                        )
                        logger.info(f"Added job '{investingcom_job_id}' with interval: {investingcom_interval_seconds} seconds.")
                        # Pause it immediately if it's not active
                        if not investingcom_is_active:
                            scheduler.pause_job(investingcom_job_id)
                            logger.info(f"Job '{investingcom_job_id}' is inactive in DB. Paused in scheduler.")
                    except Exception as e:
                         logger.error(f"Failed to add or pause job '{investingcom_job_id}': {e}", exc_info=True)
                else:
                     logger.warning(f"Job '{investingcom_job_id}' has invalid interval ({investingcom_interval_seconds}s). Not scheduling.")
                # --- End Schedule Investing.com Job --- 
                
                # --- Schedule Yahoo Job --- 
                yahoo_job_id = "yahoo_data_fetch"
                yahoo_default_seconds = 86400 # Default to 1 day
                yahoo_interval_seconds = await repository.get_job_schedule_seconds(yahoo_job_id, default_seconds=yahoo_default_seconds)
                yahoo_is_active = await repository.get_job_is_active(yahoo_job_id, default_active=True) # Check DB

                # Always add the job if interval is valid
                if yahoo_interval_seconds > 0:
                    try:
                        scheduler.add_job(
                            scheduled_yahoo_job, # Use the placeholder function
                            trigger='interval',
                            seconds=yahoo_interval_seconds,
                            id=yahoo_job_id,
                            replace_existing=True
                        )
                        logger.info(f"Added job '{yahoo_job_id}' with interval: {yahoo_interval_seconds} seconds.")
                        # Pause it immediately if it's not active
                        if not yahoo_is_active:
                            scheduler.pause_job(yahoo_job_id)
                            logger.info(f"Job '{yahoo_job_id}' is inactive in DB. Paused in scheduler.")
                    except Exception as e:
                         logger.error(f"Failed to add or pause job '{yahoo_job_id}': {e}", exc_info=True)
                else:
                    logger.warning(f"Job '{yahoo_job_id}' has invalid interval ({yahoo_interval_seconds}s). Not scheduling.")
                # --- End Schedule Yahoo Job --- 

                # --- Schedule NEW Sync IBKR Snapshot Job --- 
                sync_snapshot_job_id = "ibkr_sync_snapshot"
                
                # --- Ensure default config exists --- 
                existing_sync_config = await repository.get_job_config(sync_snapshot_job_id)
                if existing_sync_config is None:
                    logger.warning(f"Job config for '{sync_snapshot_job_id}' not found. Creating default (60s interval, active).")
                    default_sync_schedule = json.dumps({"trigger": "interval", "seconds": 60}) # Default 60s
                    default_sync_job_data = {
                        'job_id': sync_snapshot_job_id,
                        'schedule': default_sync_schedule,
                        'is_active': 1, # Default to active
                        'job_type': 'data_fetch' # Assuming this type is appropriate
                    }
                    try:
                        await repository.save_job_config(default_sync_job_data)
                        logger.info(f"Successfully created default job config for '{sync_snapshot_job_id}'.")
                        sync_interval_seconds = 60 # Use default since we just created it
                        sync_is_active = True     # Use default since we just created it
                    except Exception as create_err:
                         logger.error(f"Failed to create default job config for '{sync_snapshot_job_id}': {create_err}")
                         sync_interval_seconds = 0 # Prevent scheduling on error
                         sync_is_active = False    # Prevent scheduling on error
                else:
                    # Config exists, read interval and status
                    sync_default_seconds = 60 # Default to 60 seconds if not configured
                    sync_interval_seconds = await repository.get_job_schedule_seconds(sync_snapshot_job_id, default_seconds=sync_default_seconds)
                    sync_is_active = await repository.get_job_is_active(sync_snapshot_job_id, default_active=True) 
                # --- End ensure config --- 
                
                # Always add the job if interval is valid
                if sync_interval_seconds > 0:
                    try:
                        ibkr_base_url_for_job = os.environ.get("IBKR_BASE_URL", "https://localhost:5000/v1/api/")
                        scheduler.add_job(
                            run_sync_ibkr_snapshot_job, 
                            trigger='interval',
                            seconds=sync_interval_seconds,
                            id=sync_snapshot_job_id,
                            replace_existing=True,
                            args=[app.state.repository, loop, manager, ibkr_base_url_for_job]
                        )
                        logger.info(f"Added job '{sync_snapshot_job_id}' with interval: {sync_interval_seconds} seconds.")
                        # Pause it immediately if it's not active
                        if not sync_is_active:
                            scheduler.pause_job(sync_snapshot_job_id)
                            logger.info(f"Job '{sync_snapshot_job_id}' is inactive in DB. Paused in scheduler.")
                    except Exception as e:
                         logger.error(f"Failed to add or pause job '{sync_snapshot_job_id}': {e}", exc_info=True)
                else:
                    logger.warning(f"Job '{sync_snapshot_job_id}' has invalid interval ({sync_interval_seconds}s). Not scheduling.")
                # --- End Schedule NEW Sync Job --- 

                # Start the scheduler if not already running
                if not scheduler.running:
                    scheduler.start()
                    logger.info("Scheduler started.")
                else:
                    logger.info("Scheduler already running.")

            except Exception as e:
                logger.error(f"Failed to setup or start scheduler: {e}", exc_info=True)

            # --- Re-enable Initial Fetch Scheduling ---
            logger.info("Application startup: Scheduling initial data fetch.")
            asyncio.create_task(run_initial_fetch()) 
            # --- End Initial Fetch Scheduling ---

            # --- NEW: Schedule Initial Finviz Fetch ---
            # logger.info("Application startup: Scheduling initial Finviz data fetch.") # REMOVE/COMMENT THIS
            # asyncio.create_task(run_initial_finviz_fetch()) # REMOVE/COMMENT THIS
            # --- END NEW Finviz Fetch ---

        @app.on_event("shutdown")
        async def shutdown_event():
            """Shutdown the scheduler gracefully."""
            logger.info("Application shutdown: Stopping scheduler.")
            if scheduler.running:
                scheduler.shutdown()
            logger.info("Scheduler stopped.")

        @app.get("/")
        async def read_root(request: Request):
            repository: SQLiteRepository = request.app.state.repository
            session: Session = request.state.db_session # Get session from middleware
            try: # Correct indentation
                accounts_raw = await repository.get_all_accounts()
                positions_raw = await repository.get_all_positions()
                orders_raw = await repository.get_all_orders()
                logger.info(f"[UI Read Root] Fetched raw data counts - Accounts: {len(accounts_raw)}, Positions: {len(positions_raw)}, Orders: {len(orders_raw)}")
                screener_tickers_raw = await repository.get_all_screened_tickers()
                portfolio_rules_raw = await repository.get_all_portfolio_rules()
                
                # --- Fetch Exchange Rates --- 
                exchange_rates = await get_exchange_rates(session)
                logger.info(f"[UI Read Root] Fetched exchange rates: {exchange_rates}")
                # --- End Fetch --- 

                # --- Currency Symbol Mapping --- 
                currency_symbols = {
                    'USD': '$',
                    'EUR': '€',
                    'GBP': '£',
                    # Add others if needed
                }
                # --- End Mapping --- 

                # --- Restructure Data Processing --- 
                processed_accounts = []
                for acc_raw in accounts_raw:
                    account_id = acc_raw.get('account_id')
                    if not account_id: continue # Skip if no ID

                    # 1. Format Account Summary
                    symbol = '€' 
                    # Get raw values for percentage calculation
                    net_liq_val = acc_raw.get('net_liquidation', 0.0) or 0.0 # Ensure not None
                    total_cash_val = acc_raw.get('total_cash', 0.0) or 0.0
                    gross_pos_val = acc_raw.get('gross_position_value', 0.0) or 0.0
                    
                    # Calculate percentages
                    cash_percentage = (total_cash_val / net_liq_val * 100) if net_liq_val != 0 else 0.0
                    position_percentage = (gross_pos_val / net_liq_val * 100) if net_liq_val != 0 else 0.0
                        
                    formatted_account = {
                        'account_id': account_id,
                        'net_liquidation': f"{symbol}{net_liq_val:,.2f}",
                        'total_cash': f"{symbol}{total_cash_val:,.2f}",
                        'gross_position_value': f"{symbol}{gross_pos_val:,.2f}",
                        'total_cash_percentage': f"({cash_percentage:.2f}%)", # Add percentage string
                        'gross_position_value_percentage': f"({position_percentage:.2f}%)", # Add percentage string
                        'upd_mode': acc_raw.get('upd_mode'),
                        'last_update': acc_raw.get('last_update').strftime('%Y-%m-%d %H:%M:%S') if acc_raw.get('last_update') else 'N/A',
                        'positions': [], # Initialize empty lists
                        'orders': []
                    }
                        
                    # 2. Filter and Format Positions for this Account
                    account_positions_raw = [p for p in positions_raw if p.get('account_id') == account_id]
                    formatted_positions = []
                    raw_net_liq = acc_raw.get('net_liquidation', 0) # Get raw net liq for percentage calc
                    for pos in account_positions_raw:
                        mkt_value = pos.get('mkt_value', 0)
                        pos_currency = pos.get('currency', 'USD') # Default to USD if missing
                        pos_symbol = currency_symbols.get(pos_currency, pos_currency) # Get symbol or use code
                        
                        # --- Re-calculate Value (EUR) --- 
                        raw_value_eur = 0.0
                        usd_rate = exchange_rates.get('USD', 1.0) 
                        gbp_rate = exchange_rates.get('GBP', 1.0) 
                        if pos_currency == 'USD':
                            raw_value_eur = mkt_value * usd_rate
                        elif pos_currency == 'GBP':
                            raw_value_eur = mkt_value * gbp_rate
                        elif pos_currency == 'EUR':
                            raw_value_eur = mkt_value
                        else:
                            raw_value_eur = mkt_value # Assume 1:1 if rate unknown
                        # --- End Value (EUR) --- 
                        
                        # Convert datetime to string for JSON serialization
                        last_update_str = pos.get('last_update').isoformat() if pos.get('last_update') else None
                        
                        formatted_pos = {
                            **{k: v for k, v in pos.items() if k != 'last_update'}, # Copy other fields
                            'last_update': last_update_str, # Use string version
                            'value_percentage': f"{(raw_value_eur / raw_net_liq * 100):.2f}%" if raw_net_liq else "0.00%", # USE EUR VALUE
                            'value_eur': f"€{raw_value_eur:,.2f}", # Add formatted EUR value
                            'mkt_price_display': f"{pos_symbol}{pos.get('mkt_price', 0.0):,.2f}",
                            'mkt_value_display': f"{pos_symbol}{mkt_value:,.2f}",
                            'avg_cost_display': f"{pos_symbol}{pos.get('avg_cost', 0.0):,.2f}", # Format Avg Cost
                            'avg_price_display': f"{pos_symbol}{pos.get('avg_price', 0.0):,.2f}",
                            'unrealized_pnl_display': f"{pos_symbol}{pos.get('unrealized_pnl', 0.0):,.2f}",
                            'pnl_percentage_display': f"{pos.get('pnl_percentage', 0.0):.2f}%", 
                            # Add P/L classes (ensure this uses raw pnl value)
                            'unrealized_pnl_class': 'text-success' if pos.get('unrealized_pnl', 0) > 0 else 'text-danger' if pos.get('unrealized_pnl', 0) < 0 else '',
                            'pnl_percentage_class': 'text-success' if pos.get('pnl_percentage', 0) > 0 else 'text-danger' if pos.get('pnl_percentage', 0) < 0 else ''
                        }
                        formatted_positions.append(formatted_pos)
                    formatted_account['positions'] = formatted_positions

                    # 3. Filter and Format Orders for this Account
                    account_orders_raw = [o for o in orders_raw if o.get('account_id') == account_id]
                    formatted_orders = []
                    for order in account_orders_raw:
                         order_currency = order.get('currency', 'USD') # Default if missing
                         order_symbol = currency_symbols.get(order_currency, order_currency) # Get symbol or use code
                         
                         # Convert datetime to string for JSON serialization
                         last_update_str_order = order.get('last_update').isoformat() if order.get('last_update') else None
                         
                         formatted_order = {
                             **{k: v for k, v in order.items() if k != 'last_update'}, 
                             'last_update': last_update_str_order, 
                             'limit_price_display': f"{order_symbol}{order.get('limit_price', 0.0):,.2f}" if order.get('limit_price') is not None else 'N/A',
                             'stop_price_display': f"{order_symbol}{order.get('stop_price', 0.0):,.2f}" if order.get('stop_price') is not None else 'N/A',
                             'avg_price_display': f"{order_symbol}{order.get('avg_price', 0.0):,.2f}" if order.get('avg_price') is not None else 'N/A',
                             'total_size_display': f"{order.get('total_size', 0):,.0f}",
                             'filled_qty_display': f"{order.get('filled_qty', 0):,.0f}",
                             'remaining_qty_display': f"{order.get('remaining_qty', 0):,.0f}",
                             'status_class': f"order-status-{(order.get('status') or '').lower()}" 
                         }
                         formatted_orders.append(formatted_order)
                    formatted_account['orders'] = formatted_orders
                    
                    # 4. Append the fully processed account to the list
                    processed_accounts.append(formatted_account)
                    
                # --- End Restructure --- 

                # Format screener data (optional)
                screener_tickers = [dict(t) for t in screener_tickers_raw] # Ensure dicts

                # Format portfolio rules data (optional)
                portfolio_rules = [dict(r) for r in portfolio_rules_raw] # Ensure dicts

                logger.info(f"[UI Read Root] Processed accounts including positions/orders: {len(processed_accounts)}") 

                # --- Fetch Interval Info for Footer (ONLY needed for index.html) --- 
                current_interval = await repository.get_fetch_interval_seconds(default_interval=3600)
                ibkr_is_active = await repository.get_job_is_active('ibkr_fetch')
                logger.info(f"[UI Read Root] Fetch interval for footer: {current_interval}s, Active: {ibkr_is_active}")
                # --- End Fetch Interval Info --- 

                # --- Fetch Exchange Rates (needed for index.html) ---
                exchange_rates = await get_exchange_rates(request.state.db_session) 
                # --- End Fetch --- 

                # --- Context Dictionary for index.html --- 
                context = {
                    "request": request,
                    "accounts": processed_accounts, # Main data
                    "screener_tickers": screener_tickers, # For tracker integration potentially
                    "portfolio_rules": portfolio_rules, # For potential display/checks
                    "current_interval": current_interval, # For footer
                    "ibkr_is_active": ibkr_is_active,   # For footer
                    "exchange_rates": exchange_rates, # For currency calcs
                    "get_symbol": lambda currency: currency_symbols.get(currency, currency) # Helper
                    # REMOVED config-specific variables like yahoo_seconds, finviz_active etc.
                }
                logger.info(f"[UI Read Root] Context keys being passed to template: {list(context.keys())}")
                return templates.TemplateResponse("index.html", context)
            # --- FIX: Correct except/finally indentation --- 
            except Exception as e: # Align with try
                logger.error(f"Error loading root page data: {e}", exc_info=True) # Indent under except
                # CORRECTED defaults for index.html error page
                context = { # Indent under except
                    "request": request,
                    "error": "Failed to load application data.",
                    "accounts": [],
                    "screener_tickers": [],
                    "portfolio_rules": [],
                    "current_interval": 3600, # Default interval for footer
                    "ibkr_is_active": True, # Default status for footer
                    "exchange_rates": {},
                    "get_symbol": lambda currency: currency # Basic fallback
                 }
                return templates.TemplateResponse("error.html", context, status_code=500) # Indent under except
            finally: # Align with try
                await session.close() # Indent under finally
            # --- END FIX ---

        @app.get("/api/accounts")
        async def get_accounts() -> Dict[str, List[Dict[str, Any]]]:
            """API endpoint to get all accounts."""
            try:
                accounts = await app.state.repository.get_all_accounts()
                return {"accounts": accounts}
            except Exception as e:
                logger.error(f"Error getting accounts: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @app.put("/api/accounts/update/{account_id}", response_model=Dict[str, Any])
        async def api_update_account(request: Request, account_id: str, payload: AccountPayload):
            """API endpoint to update an existing account."""
            repository: SQLiteRepository = request.app.state.repository
            logger.info(f"Received API request to update account: {account_id}")
            try:
                # Prepare update data, excluding account_id if it's part of the payload model
                update_data = payload.dict(exclude_unset=True) 
                if 'account_id' in update_data and update_data['account_id'] != account_id:
                   logger.warning(f"Payload account_id ({update_data['account_id']}) mismatches path account_id ({account_id}). Using path ID.")
                   # Decide if you want to reject or just use path ID. Using path ID here.
                   del update_data['account_id'] # Remove account_id from payload if present
                
                updated = await repository.update_account(account_id, update_data) # Assumes update_account exists
                
                if updated:
                    logger.info(f"Successfully updated account {account_id}")
                    # Optionally fetch the updated account data to return
                    updated_account_data = await repository.get_account_by_id(account_id) # Assumes get_account_by_id exists
                    if updated_account_data:
                        return {"message": f"Account {account_id} updated successfully.", "account": updated_account_data}
                    else: # Should not happen if update succeeded, but handle defensively
                        return {"message": f"Account {account_id} updated, but could not retrieve updated data."}
                else:
                    logger.warning(f"Account {account_id} not found for update.")
                    raise HTTPException(status_code=404, detail=f"Account '{account_id}' not found.")
            except HTTPException as http_exc: # Re-raise HTTP exceptions
                raise http_exc
            except Exception as e:
                logger.error(f"Error updating account {account_id}: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Error updating account: {str(e)}")

        @app.get("/api/positions")
        async def get_positions() -> Dict[str, List[Dict[str, Any]]]:
            """API endpoint to get all positions."""
            try:
                positions = await app.state.repository.get_all_positions()
                return {"positions": positions}
            except Exception as e:
                logger.error(f"Error getting positions: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @app.get("/api/orders")
        async def get_orders() -> Dict[str, List[Dict[str, Any]]]:
            """API endpoint to get all orders."""
            try:
                orders = await app.state.repository.get_all_orders()
                return {"orders": orders}
            except Exception as e:
                logger.error(f"Error getting orders: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @app.get("/config", response_class=HTMLResponse)
        async def get_config_page(request: Request):
            """Display the configuration page."""
            message = request.query_params.get('message')
            message_type = request.query_params.get('message_type', 'info')
            repository: SQLiteRepository = request.app.state.repository
            try:
                # Fetch ALL interval seconds
                current_interval = await repository.get_fetch_interval_seconds(default_interval=3600) # IBKR Fetch
                ibkr_snapshot_seconds = await repository.get_job_schedule_seconds('ibkr_sync_snapshot')
                finviz_seconds = await repository.get_job_schedule_seconds('finviz_data_fetch')
                yahoo_seconds = await repository.get_job_schedule_seconds('yahoo_data_fetch')
                investingcom_seconds = await repository.get_job_schedule_seconds('investingcom_data_fetch')
                
                # Fetch ALL active statuses
                ibkr_active = await repository.get_job_is_active('ibkr_fetch')
                ibkr_snapshot_active = await repository.get_job_is_active('ibkr_sync_snapshot')
                finviz_active = await repository.get_job_is_active('finviz_data_fetch')
                yahoo_active = await repository.get_job_is_active('yahoo_data_fetch')
                investingcom_active = await repository.get_job_is_active('investingcom_data_fetch')
                
                # Fetch other data
                exchange_rates = await get_exchange_rates(request.state.db_session) 
                portfolio_rules = await repository.get_all_portfolio_rules()
                portfolio_rules_json = json.dumps(portfolio_rules, default=str) # Keep JSON for JS

                # CORRECTED context dictionary
                context = {
                    "request": request,
                    "message": message,
                    "message_type": message_type,
                    # Scheduler Intervals
                    "current_interval": current_interval,
                    "ibkr_snapshot_schedule_seconds": ibkr_snapshot_seconds,
                    "finviz_schedule_seconds": finviz_seconds,
                    "yahoo_schedule_seconds": yahoo_seconds,
                    "investingcom_schedule_seconds": investingcom_seconds,
                    # Scheduler Statuses
                    "ibkr_is_active": ibkr_active,
                    "ibkr_snapshot_is_active": ibkr_snapshot_active,
                    "finviz_is_active": finviz_active,
                    "yahoo_is_active": yahoo_active,
                    "investingcom_is_active": investingcom_active,
                    # Other Data
                    "exchange_rates": exchange_rates,
                    "portfolio_rules": portfolio_rules, # Pass the list for Jinja
                    "portfolio_rules_json": portfolio_rules_json # Pass JSON string for JS
                }
                return request.app.state.templates.TemplateResponse("config.html", context)
            except Exception as e:
                logger.error(f"Error loading config page: {e}", exc_info=True)
                # CORRECTED defaults in the except block
                return request.app.state.templates.TemplateResponse("config.html", {
                    "request": request, 
                    "message": "Error loading configuration data.", # Display error message
                    "message_type": "danger",
                    # Default Intervals
                    "current_interval": 3600, 
                    "ibkr_snapshot_schedule_seconds": 0, 
                    "finviz_schedule_seconds": 0,
                    "yahoo_schedule_seconds": 0,
                    "investingcom_schedule_seconds": 0,
                    # Default Statuses (assume active on error?)
                    "ibkr_is_active": True, 
                    "ibkr_snapshot_is_active": True,
                    "finviz_is_active": True,
                    "yahoo_is_active": True,
                    "investingcom_is_active": True,
                     # Default Other Data
                    "exchange_rates": {},
                    "portfolio_rules": [], 
                    "portfolio_rules_json": "[]", 
                    "error": "Failed to load configuration." # Keep original error for logging? 
                 })
        
        @app.post("/config/schedule")
        async def update_schedule_config(request: Request, interval_seconds: int = Form(...)):
            """Update the IBKR data fetch schedule interval AND reschedule the running job."""
            repository: SQLiteRepository = request.app.state.repository
            message = ""
            message_type = "danger"
            try:
                if interval_seconds <= 0:
                     raise ValueError("Interval must be a positive integer.")
                
                await repository.set_fetch_interval_seconds(interval_seconds)
                logger.info(f"Fetch interval updated in DB to {interval_seconds} seconds")
                
                # Access scheduler directly (assuming global scope)
                global scheduler 
                if scheduler and scheduler.running:
                    try:
                        scheduler.reschedule_job('ibkr_fetch', trigger='interval', seconds=interval_seconds)
                        logger.info(f"Scheduler job 'ibkr_fetch' successfully rescheduled to {interval_seconds} seconds.")
                        message = f"Fetch interval updated and job rescheduled to {interval_seconds} seconds successfully."
                        message_type = "success"
                    except JobLookupError:
                         logger.warning(f"Job 'ibkr_fetch' not found in scheduler. Cannot reschedule. DB updated.")
                         message = f"Fetch interval updated to {interval_seconds}s in DB (job 'ibkr_fetch' not found in scheduler)."
                         message_type = "warning" # Changed from danger as DB update was successful
                    except Exception as schedule_err:
                        logger.error(f"Failed to reschedule job 'ibkr_fetch': {schedule_err}", exc_info=True)
                        message = f"Fetch interval updated in DB, but failed to reschedule running job 'ibkr_fetch': {schedule_err}"
                        message_type = "warning"
                else:
                    logger.warning("Scheduler not found or not running. Job not rescheduled.")
                    message = f"Fetch interval updated to {interval_seconds} seconds, but scheduler not running."
                    message_type = "warning" 

                return RedirectResponse(url=f"/config?message={message}&message_type={message_type}", status_code=303)
            except ValueError as ve:
                logger.error(f"Invalid interval value provided: {interval_seconds} - {ve}")
                message = f"Invalid interval value: {interval_seconds}. Please enter a positive integer."
                message_type = "danger"
                return RedirectResponse(url=f"/config?message={message}&message_type={message_type}", status_code=303)
            except Exception as e:
                logger.error(f"Error updating fetch interval: {e}")
                message = f"Error updating fetch interval: {e}"
                message_type = "danger"
                return RedirectResponse(url=f"/config?message={message}&message_type={message_type}", status_code=303)

        @app.post("/config/schedule/generic_update", response_class=JSONResponse)
        async def update_generic_schedule(request: Request, payload: GenericScheduleUpdatePayload):
            """Update schedule for FinViz/Yahoo (converts input to seconds JSON)."""
            repository: SQLiteRepository = request.app.state.repository
            # --- Add investing.com to valid jobs --- 
            # ADD 'ibkr_fetch' to the list of valid job IDs
            valid_job_ids = ['yahoo_data_fetch', 'finviz_data_fetch', 'investingcom_data_fetch', 'ibkr_fetch', 'ibkr_sync_snapshot'] 
            # --- End Add --- 
            job_id = payload.job_id
            raw_schedule_input = payload.schedule.strip().lower()
            seconds = 0

            logger.info(f"Received generic schedule update for {job_id}: '{raw_schedule_input}'")

            if job_id not in valid_job_ids:
                raise HTTPException(status_code=400, detail=f"Invalid job_id for generic update: {job_id}")

            # --- Input Parsing Logic (Convert to Seconds) --- 
            if not raw_schedule_input:
                raise HTTPException(status_code=400, detail="Schedule cannot be empty.")
                
            # UPDATED regex to handle singular/plural units and optional unit
            time_pattern = re.compile(r"^(\d+)\s*(days?|d|hours?|h|minutes?|min|m|seconds?|sec|s)?$")
            match = time_pattern.match(raw_schedule_input)

            if raw_schedule_input == 'daily':
                seconds = 86400
            elif raw_schedule_input == 'hourly':
                seconds = 3600
            elif match:
                value = int(match.group(1))
                unit = match.group(2)
                if unit in ['day', 'd']:
                    seconds = value * 86400
                elif unit in ['hour', 'h', 'hours']: # Adjusted unit check
                    seconds = value * 3600
                elif unit in ['minute', 'min', 'm', 'minutes']: # Adjusted unit check
                    seconds = value * 60
                else: # Includes second, sec, s, seconds or no unit
                    seconds = value
            else:
                raise HTTPException(status_code=400, detail="Invalid format. Use N (days|d|hours|h|minutes|min|m|seconds|sec|s) or daily/hourly.") # Updated error message

            if seconds <= 0:
                raise HTTPException(status_code=400, detail="Calculated interval must be positive.")
            # --- End Parsing Logic --- 
                 
            schedule_json_str = json.dumps({"trigger": "interval", "seconds": seconds})

            try:
                job_config_data = {
                    'job_id': job_id,
                    'schedule': schedule_json_str, # Save standard JSON
                }
                # save_job_config handles job_type, is_active, timestamps, and upsert
                await repository.save_job_config(job_config_data)
                logger.info(f"Generic job config for '{job_id}' updated with JSON: '{schedule_json_str}'")
                
                # --- Reschedule the running job --- 
                try:
                    # Check if job exists before rescheduling
                    existing_job = scheduler.get_job(job_id)
                    if existing_job:
                        scheduler.reschedule_job(
                            job_id,
                            trigger='interval',
                            seconds=seconds
                        )
                        logger.info(f"Successfully rescheduled job '{job_id}' to run every {seconds} seconds.")
                        message = f"Schedule for {job_id} updated to {seconds} seconds and rescheduled."
                    else:
                        # Job might not be running if it was inactive or failed to schedule initially
                        logger.warning(f"Job '{job_id}' not found in running scheduler. Database updated, but job not rescheduled.")
                        message = f"Schedule for {job_id} updated to {seconds} seconds in DB (job not running)."
                    
                except Exception as schedule_err:
                    logger.error(f"Error rescheduling job '{job_id}': {schedule_err}", exc_info=True)
                    # Return success but mention rescheduling issue
                    message = f"Schedule updated to {seconds}s in DB, but failed to reschedule running job: {schedule_err}"
                    # Keep status 200, but maybe adjust message type?
                # --- End Reschedule --- 
                
                return {"message": message, "schedule_seconds": seconds}
            except Exception as e:
                logger.error(f"Error saving generic schedule for '{job_id}': {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Error saving schedule for {job_id}")

        @app.post("/config/schedule/update_status", response_class=JSONResponse)
        async def update_job_status(request: Request, payload: JobStatusUpdatePayload):
            """Update the is_active status for a specific job_id in DB and pause/resume scheduler."""
            repository: SQLiteRepository = request.app.state.repository
            # --- Access scheduler from app state --- 
            scheduler_instance: AsyncIOScheduler = request.app.state.scheduler 
            # --- End Access --- 
            job_id = payload.job_id
            is_active = payload.is_active
            # Add job_id validation if necessary
                
            logger.info(f"Received status update for {job_id} to {is_active}")
            try:
                # 1. Update the database first
                db_success = await repository.update_job_active_status(job_id, is_active)
                
                if not db_success:
                    # Moved the exception raise here
                    raise HTTPException(status_code=404, detail=f"Job ID '{job_id}' not found in database.")

                # Removed duplicate try and db update
                # 2. Attempt to pause/resume the job in the scheduler
                action_message = ""
                if scheduler_instance and scheduler_instance.running:
                    try:
                        if is_active:
                            scheduler_instance.resume_job(job_id)
                            logger.info(f"Successfully resumed job '{job_id}' in scheduler.")
                            action_message = "and resumed in scheduler"
                        else:
                            scheduler_instance.pause_job(job_id)
                            logger.info(f"Successfully paused job '{job_id}' in scheduler.")
                            action_message = "and paused in scheduler"
                    except JobLookupError:
                        logger.warning(f"Job '{job_id}' not found in scheduler. Cannot pause/resume. DB status updated.")
                        action_message = f"(job not found in running scheduler)"
                    except Exception as sched_err: # Catch other potential scheduler errors
                        logger.error(f"Error pausing/resuming job '{job_id}' in scheduler: {sched_err}", exc_info=True)
                        action_message = f"(scheduler action failed: {sched_err})"
                else:
                    logger.warning("Scheduler not running or not available. Cannot pause/resume jobs.")
                    action_message = "(scheduler not running)"

                # 3. Return success response
                status_text = "activated" if is_active else "deactivated"
                return JSONResponse(content={
                    "message": f"Job '{job_id}' {status_text} in database {action_message}.",
                    "job_id": job_id,
                    "is_active": is_active
                }, status_code=200)
            # Moved except blocks to align with outer try
            except HTTPException as http_exc: # Re-raise HTTP exceptions
                raise http_exc
            except Exception as e:
                logger.error(f"Error processing status update for '{job_id}': {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Error updating status for {job_id}")

        # --- NEW Portfolio Rule API Endpoints --- 

        @app.post("/api/rules/add", response_model=Dict[str, Any])
        async def api_add_portfolio_rule(request: Request, rule_data: PortfolioRuleCreate):
            """API endpoint to add a new portfolio rule."""
            repository: SQLiteRepository = request.app.state.repository
            try:
                added_rule = await repository.add_portfolio_rule(rule_data.dict())
                if not added_rule:
                    raise HTTPException(status_code=500, detail="Failed to add portfolio rule.")
                
                # Fetch all rules to return to the client for table update
                all_rules = await repository.get_all_portfolio_rules()
                return {"message": "Portfolio rule added successfully!", "rule": added_rule, "all_rules": all_rules}
            except Exception as e:
                logger.error(f"API Error adding portfolio rule: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Error adding rule: {str(e)}")

        @app.post("/api/rules/update", response_model=Dict[str, Any])
        async def api_update_portfolio_rule(request: Request, rule_data: PortfolioRuleUpdate):
            """API endpoint to update an existing portfolio rule."""
            repository: SQLiteRepository = request.app.state.repository
            try:
                # Exclude unset fields to allow partial updates (like just is_active)
                update_payload = rule_data.dict(exclude_unset=True)
                if 'rule_id' not in update_payload: # Should always be present based on model
                     raise HTTPException(status_code=400, detail="Rule ID is required for update.")
                
                rule_id = update_payload.pop('rule_id') # Get ID and remove from payload
                
                updated_rule = await repository.update_portfolio_rule(rule_id, update_payload)
                if not updated_rule:
                    # Check if it was not found or just no changes
                    existing = await repository.get_portfolio_rule_by_id(rule_id) # Need this method
                    if existing:
                         # Rule exists, but maybe no effective change or update failed internally
                         logger.warning(f"Update called for rule {rule_id}, but repository returned None. Maybe no change?")
                         # Return current state? 
                         all_rules = await repository.get_all_portfolio_rules()
                         return {"message": f"Rule {rule_id} status checked.", "rule": existing, "all_rules": all_rules}
                    else:
                         raise HTTPException(status_code=404, detail=f"Portfolio rule ID {rule_id} not found.")

                # Fetch all rules to return to the client for table update
                all_rules = await repository.get_all_portfolio_rules()
                return {"message": f"Portfolio rule {rule_id} updated successfully!", "rule": updated_rule, "all_rules": all_rules}
            except Exception as e:
                logger.error(f"API Error updating portfolio rule {rule_data.rule_id}: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Error updating rule: {str(e)}")

        @app.post("/api/rules/delete", response_model=Dict[str, Any])
        async def api_delete_portfolio_rule(request: Request, rule_data: PortfolioRuleDelete):
            """API endpoint to delete a portfolio rule."""
            repository: SQLiteRepository = request.app.state.repository
            try:
                deleted = await repository.delete_portfolio_rule(rule_data.rule_id)
                if not deleted:
                    raise HTTPException(status_code=404, detail=f"Portfolio rule ID {rule_data.rule_id} not found.")
                
                # Fetch remaining rules to return to the client for table update
                all_rules = await repository.get_all_portfolio_rules()
                return {"message": f"Portfolio rule {rule_data.rule_id} deleted successfully!", "all_rules": all_rules}
            except Exception as e:
                logger.error(f"API Error deleting portfolio rule {rule_data.rule_id}: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Error deleting rule: {str(e)}")

        # --- End Portfolio Rule API Endpoints ---

        # --- Add Placeholder Tracker Endpoint --- 
        @app.get("/tracker", response_class=HTMLResponse, name="get_tracker_page")
        async def get_tracker_page(request: Request):
            logger.info("Accessing /tracker page.")
            try:
                repository: SQLiteRepository = request.app.state.repository
                session: Session = request.state.db_session # Get session via middleware

                # 1. Fetch Screener Data
                screener_tickers_raw = await repository.get_all_screened_tickers()
                # Create a dictionary keyed by ticker for easier lookup/update
                screener_dict = {dict(t)['ticker']: dict(t) for t in screener_tickers_raw}
                logger.info(f"[Tracker] Fetched {len(screener_dict)} tickers from screener.")

                # 2. Fetch Live Data (Positions and Orders)
                positions_raw = await repository.get_all_positions()
                orders_raw = await repository.get_all_orders()
                logger.info(f"[Tracker] Fetched {len(positions_raw)} live positions and {len(orders_raw)} live orders.")
                
                # Create lookups for live data
                live_positions = { (p['account_id'], p['ticker']): p for p in positions_raw }
                # Find relevant orders (e.g., active non-filled stop/trail orders?)
                # For simplicity now, just store the latest order per ticker/account?
                # Or store the most relevant (e.g., first active stop/trail)?
                live_orders_lookup = {}
                for o in orders_raw:
                    # Example: Prioritize active stop/trail orders
                    key = (o['account_id'], o['ticker'])
                    # Basic logic: store the first relevant order found (could be improved)
                    if o.get('status') in ['Submitted', 'PendingSubmit', 'PreSubmitted'] and o.get('order_type') in ['STP', 'TRAIL', 'STP LMT']:
                         if key not in live_orders_lookup: 
                             live_orders_lookup[key] = o

                # 3. Merge Live Data into Screener Data
                for ticker, item in screener_dict.items():
                    item['in_portfolio'] = False # Default
                    item['acc_from_live'] = False
                    item['open_pos_from_live'] = False
                    item['cost_base_from_live'] = False
                    item['currency_from_live'] = False
                    item['stop_from_order'] = False
                    item['trail_from_order'] = False
                    item['lmt_ofst_from_order'] = False
                    # Find matching live position (need account context - how to determine which account?)
                    # --- Simplified Approach: If ticker appears in *any* live position --- 
                    matching_pos = next((p for p in positions_raw if p.get('ticker') == ticker), None)
                    if matching_pos:
                         item['in_portfolio'] = True
                         item['acc'] = matching_pos.get('account_id') # Overwrite screener acc
                         item['acc_from_live'] = True
                         item['open_pos'] = matching_pos.get('position') # Overwrite screener pos
                         item['open_pos_from_live'] = True
                         item['cost_base'] = matching_pos.get('avg_cost') # Use avg_cost as cost_base
                         item['cost_base_from_live'] = True
                         item['currency'] = matching_pos.get('currency') # Overwrite screener currency
                         item['currency_from_live'] = True
                         
                         # Try to find a matching order for this specific account/ticker
                         matching_order = live_orders_lookup.get((item['acc'], ticker))
                         if matching_order:
                             if matching_order.get('stop_price') is not None:
                                 item['order_stop_price'] = matching_order['stop_price']
                                 item['stop_from_order'] = True
                             if matching_order.get('trailing_amount') is not None:
                                 item['order_trailing_amount'] = matching_order['trailing_amount']
                                 item['trail_from_order'] = True
                             if matching_order.get('limit_offset') is not None:
                                 item['order_limit_offset'] = matching_order['limit_offset']
                                 item['lmt_ofst_from_order'] = True
                    # --- End Simplified Approach --- 
                
                # Convert back to list for template
                enriched_tickers = list(screener_dict.values())

                # 4. Calculate Status Counts (use enriched data)
                status_counts = Counter(t.get('status', 'unknown') for t in enriched_tickers)
                logger.info(f"[Tracker] Calculated status counts: {status_counts}")

                # 5. Fetch other necessary data
                accounts_data = await repository.get_all_accounts()
                available_accounts = [acc['account_id'] for acc in accounts_data if acc.get('account_id')]
                exchange_rates_data = await get_exchange_rates(session)
                currency_symbols = {"EUR": "€", "USD": "$", "GBP": "£"} # Define currency symbols

                # --- START: Add formatting for live position data for popovers --- 
                formatted_positions_for_tracker = []
                # Need account net liquidation for percentage calculation. Create a lookup.
                account_nlvs = {acc.get('account_id'): acc.get('net_liquidation', 0) for acc in accounts_data}
                
                for pos in positions_raw:
                    formatted_pos = dict(pos) # Use a copy
                    
                    mkt_value = formatted_pos.get('mkt_value', 0.0) or 0.0
                    pos_currency = formatted_pos.get('currency', 'USD')
                    pos_symbol = currency_symbols.get(pos_currency, pos_currency)
                    account_id = formatted_pos.get('account_id')
                    raw_net_liq = account_nlvs.get(account_id, 0) or 0.0

                    # Calculate Value (EUR) - needed for % NAV
                    raw_value_eur = 0.0
                    usd_rate = exchange_rates_data.get('USD', 1.0)
                    gbp_rate = exchange_rates_data.get('GBP', 1.0)
                    if pos_currency == 'USD':
                        raw_value_eur = mkt_value * usd_rate
                    elif pos_currency == 'GBP':
                        raw_value_eur = mkt_value * gbp_rate
                    elif pos_currency == 'EUR':
                        raw_value_eur = mkt_value
                    else:
                        raw_value_eur = mkt_value # Assume 1:1 if rate unknown

                    # Apply formatting for fields needed by the popover
                    formatted_pos['value_percentage'] = f"{(raw_value_eur / raw_net_liq * 100):.2f}%" if raw_net_liq != 0 else "0.00%"
                    formatted_pos['mkt_value_display'] = f"{pos_symbol}{mkt_value:,.2f}"
                    formatted_pos['pnl_percentage_display'] = f"{formatted_pos.get('pnl_percentage', 0.0):.2f}%"
                    
                    # Ensure ticker exists for JS lookup key
                    if 'ticker' in formatted_pos:
                        formatted_positions_for_tracker.append(formatted_pos)
                    else:
                        logger.warning(f"[Tracker] Position found without ticker, skipping for popover data: {formatted_pos}")
                        
                logger.info(f"[Tracker] Formatted {len(formatted_positions_for_tracker)} positions for popover data.")
                # --- END: Add formatting --- 

                # 6. Prepare Context
                context = {
                    "request": request, 
                    "tickers": enriched_tickers, # Pass enriched data
                    "status_counts": status_counts,
                    "available_accounts": available_accounts,
                    "all_accounts_data": json.dumps(accounts_data, default=json_datetime_serializer),
                    "exchange_rates_data": json.dumps(exchange_rates_data, default=json_datetime_serializer),
                    # --- ADD formatted live positions data --- 
                    "live_positions_data": json.dumps(formatted_positions_for_tracker, default=json_datetime_serializer) 
                }
                
                logger.info(f"[Tracker] Rendering tracker.html with {len(enriched_tickers)} tickers.")
                return request.app.state.templates.TemplateResponse("tracker.html", context)
            except Exception as e:
                logger.error(f"Error loading tracker page: {e}", exc_info=True)
                # Pass minimal context for error display
                # --- ADD default for live_positions_data in error case --- 
                context = {"request": request, "error": "Failed to load tracker data.", "tickers": [], "status_counts": Counter(), "available_accounts": [], "all_accounts_data": "[]", "exchange_rates_data": "{}", "live_positions_data": "[]"} 
                return request.app.state.templates.TemplateResponse("tracker.html", context, status_code=500)
        # --- End Placeholder --- 

        # --- Add Ticker Endpoint ---
        @app.post("/tracker/add", name="add_ticker")
        async def add_ticker(
            request: Request,
            ticker: str = Form(...),
            ticker_status: str = Form(..., alias="status"), # Use alias to match form name
            atr: Optional[float] = Form(None),
            atr_mult: Optional[int] = Form(None),
            risk: Optional[float] = Form(None),
            beta: Optional[float] = Form(None),
            sector: Optional[str] = Form(None),
            industry: Optional[str] = Form(None),
            comments: Optional[str] = Form(None),
            repository: SQLiteRepository = Depends(get_repository) # Reuse dependency
        ):
            """Adds a new ticker from the tracker page form."""
            logger.info(f"Received request to add ticker: {ticker}")
            try:
                # Prepare data for insertion (handle optional fields)
                # Match the columns expected by add_screened_ticker_manual
                ticker_data = {
                    "ticker": ticker.upper(), # Standardize ticker to uppercase
                    "status": ticker_status,
                    "atr": atr,
                    "atr_mult": atr_mult,
                    "risk": risk,
                    "beta": beta,
                    "sector": sector,
                    "industry": industry,
                    "comments": comments,
                    # "updated_at": datetime.now() # Removed as repo method handles it
                }

                # Check if ticker already exists? Or let the repo handle it?
                # Assuming repo handles potential duplicates or updates appropriately

                # Call the correct repository method and unpack data
                await repository.add_or_update_screener_ticker(**ticker_data)
                logger.info(f"Successfully added/updated ticker: {ticker}")

                # Redirect back to the tracker page with a success message
                # Use URL generation for safety
                tracker_url = request.url_for('get_tracker_page')
                # How to pass flash messages? FastAPI doesn't have built-in flash.
                # One way is query parameters, another is session state if configured.
                # Using query parameters for simplicity here.
                redirect_url = f"{tracker_url}?message=Ticker%20{ticker}%20added%20successfully.&message_type=success"
                # --- FIX: Use status module for HTTP status code --- 
                return RedirectResponse(url=redirect_url, status_code=status.HTTP_303_SEE_OTHER)
                # --- END FIX ---

            except ValidationError as e: # Catch potential validation errors if using Pydantic models later
                 logger.error(f"Validation error adding ticker {ticker}: {e}", exc_info=True)
                 tracker_url = request.url_for('get_tracker_page')
                 redirect_url = f"{tracker_url}?message=Error:%20Invalid%20data%20provided%20for%20{ticker}.&message_type=danger"
                 # --- FIX: Use status module for HTTP status code --- 
                 return RedirectResponse(url=redirect_url, status_code=status.HTTP_303_SEE_OTHER) # Redirect even on validation error
                 # --- END FIX ---
            except Exception as e:
                logger.error(f"Error adding ticker {ticker}: {e}", exc_info=True)
                # Redirect back to the tracker page with an error message
                tracker_url = request.url_for('get_tracker_page')
                redirect_url = f"{tracker_url}?message=Error%20adding%20ticker%20{ticker}.&message_type=danger"
                # --- FIX: Use status module for HTTP status code --- 
                # Use 303 See Other for POST-Redirect-Get pattern
                return RedirectResponse(url=redirect_url, status_code=status.HTTP_303_SEE_OTHER)
                # --- END FIX ---
        # --- End Add Ticker Endpoint ---

        # --- Add Update Row Endpoint --- 
        @app.post("/tracker/update-row", response_model=Dict[str, Any], name="update_ticker_row")
        async def update_ticker_row(
            request: Request,
            payload: UpdateRowRequest, # Use the existing Pydantic model
            repository: SQLiteRepository = Depends(get_repository)
        ):
            """Handles updating multiple fields for a specific ticker row."""
            ticker = payload.ticker
            updates = payload.updates
            logger.info(f"Received request to update row for ticker: {ticker} with updates: {updates}")
            
            update_success_count = 0
            update_error_count = 0
            errors = []

            # --- Option 1: Update fields one by one --- 
            # (Simpler, uses existing method, might be less efficient for many updates)
            for field, value in updates.items():
                try:
                    # Validate field name if necessary (update_screener_ticker_details does this)
                    await repository.update_screener_ticker_details(ticker, field, value)
                    logger.debug(f"Successfully updated field '{field}' for ticker {ticker}.")
                    update_success_count += 1
                except ValueError as ve:
                    logger.error(f"Validation error updating field '{field}' for ticker {ticker}: {ve}")
                    update_error_count += 1
                    errors.append(f"Invalid value for {field}: {str(ve)}")
                except Exception as e:
                    logger.error(f"Error updating field '{field}' for ticker {ticker}: {e}", exc_info=True)
                    update_error_count += 1
                    errors.append(f"Error updating {field}: {str(e)}")
            # --- End Option 1 --- 

            # --- Option 2: Use a potential multi-update method (if it exists and is suitable) ---
            # try:
            #     # Example: Assuming a method like this exists
            #     # Note: This method might need adjustments based on its exact implementation
            #     # (e.g., handling allowed fields, type conversions)
            #     success = await repository.update_screener_multi_fields(ticker, updates)
            #     if success:
            #         update_success_count = len(updates) # Assume all updated if method succeeds
            #     else:
            #         update_error_count = len(updates) # Assume all failed if method returns False
            #         errors.append("Failed to update multiple fields (ticker not found or other error).")
            # except AttributeError:
            #     logger.error("update_screener_multi_fields method not found in repository. Falling back to single updates.")
            #     # Implement fallback to Option 1 here if needed
            # except Exception as e:
            #     logger.error(f"Error using multi-update for ticker {ticker}: {e}", exc_info=True)
            #     update_error_count = len(updates)
            #     errors.append(f"Error during multi-update: {str(e)}")
            # --- End Option 2 --- 

            if update_error_count > 0:
                error_detail = "; ".join(errors)
                # Return a 400 Bad Request or 500 Internal Server Error depending on error type
                # Using 400 for validation errors, 500 for others might be appropriate
                # For simplicity, returning 400 if any error occurred
                raise HTTPException(status_code=400, detail=f"Errors updating ticker {ticker}: {error_detail}")
            else:
                return {"message": f"Ticker {ticker} updated successfully ({update_success_count} fields)."}
        # --- End Update Row Endpoint ---

        # --- Add Delete Ticker Endpoint --- 
        @app.post("/tracker/delete", response_model=Dict[str, Any], name="delete_ticker")
        async def delete_ticker_endpoint(
            request: Request,
            payload: TickerDelete, # Use the Pydantic model for the payload
            repository: SQLiteRepository = Depends(get_repository)
        ):
            """Handles deleting a specific ticker from the screener table."""
            ticker = payload.ticker
            logger.info(f"Received request to delete ticker: {ticker}")
            
            try:
                # Call the repository method which returns True if deleted, False if not found
                deleted = await repository.delete_screener_ticker(ticker)
                
                if deleted:
                    logger.info(f"Successfully deleted ticker: {ticker}")
                    # Return success message
                    return {"message": f"Ticker {ticker} deleted successfully.", "status": "success"} # Add status field for JS
                else:
                    logger.warning(f"Ticker {ticker} not found for deletion.")
                    # Return a different message/status if not found, but still HTTP 200
                    return {"message": f"Ticker {ticker} not found.", "status": "warning"} # Add status field for JS

            except ValueError as ve:
                logger.error(f"Validation error deleting ticker {ticker}: {ve}")
                raise HTTPException(status_code=400, detail=str(ve))
            except Exception as e:
                logger.error(f"Error deleting ticker {ticker}: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Error deleting ticker {ticker}")
        # --- End Delete Ticker Endpoint ---

        # --- NEW Endpoint to Find Instruments (for Add Ticker Page) ---
        @app.post("/find_instruments", response_model=InstrumentSearchResponse)
        async def find_instruments_for_conid_selection(
            request: InstrumentSearchRequest, # Now uses model from outer scope
            ibkr_service: IBKRService = Depends(get_ibkr_service) # Inject IBKRService
        ):
            """Receives identifier and status, determines secType, searches IBKR, returns contracts."""
            identifier = request.identifier
            status_type = request.status # e.g., 'currency', 'indicator', 'monitored'
            search_by_name = request.name # NEW: Get the name flag
            logger.info(f"Received request to find instruments for identifier: {identifier}, status/type: {status_type}, search_by_name: {search_by_name}")

            # Determine secType based on status_type
            sec_type: str
            if status_type == 'currency':
                sec_type = 'CASH'
            elif status_type == 'indicator':
                sec_type = 'IND'
            else: # Default to Stock for candidate, monitored, portfolio
                sec_type = 'STK'
            
            logger.info(f"Determined secType: {sec_type} for status/type: {status_type}")

            try:
                # Ensure service is connected (might need a connect/check method)
                # For now, assume the service is managed by the app lifecycle or connect on demand
                # --- Need to ensure the service is actually connected --- 
                # Simple check (can be improved with explicit connect/disconnect in service)
                if not ibkr_service.session or ibkr_service.session.closed:
                    logger.info("IBKRService session not active, attempting to connect...")
                    await ibkr_service.connect() # Assumes connect handles auth
                    if not ibkr_service.session or ibkr_service.session.closed:
                        raise HTTPException(status_code=503, detail="Failed to connect to IBKR service.")
                # --- End connection check --- 
                
                found_contracts = await ibkr_service.search_contracts(
                    symbol=identifier,
                    sec_type=sec_type,
                    name=search_by_name # NEW: Pass the name flag
                )
                logger.info(f"IBKR search returned {len(found_contracts)} contracts for {identifier} ({sec_type}, name={search_by_name})")
                return InstrumentSearchResponse(contracts=found_contracts)

            except HTTPException as http_exc:
                # Re-raise HTTP exceptions (like connection failure)
                raise http_exc 
            except Exception as e:
                logger.error(f"Error searching contracts via IBKRService for {identifier}: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Error searching for instrument: {str(e)}")
        # --- End Find Instruments Endpoint ---

        # --- NEW: Endpoint to Add Instrument --- 
        @app.post("/add_instrument") # Removed default status_code to set it based on DB result
        async def add_instrument(
            request_model: AddInstrumentRequest, # Rename to avoid conflict with Request object
            request: Request, # Keep original Request object for dependency calls
            db: AsyncSession = Depends(get_db),
            repository: SQLiteRepository = Depends(get_repository) # Add repository dependency here
        ):
            """Receives instrument data from the form and saves it to the appropriate table."""
            logger.info(f"[/add_instrument] Received request: Status={request_model.status}, Identifier={request_model.identifier}, ConID={request_model.conid}")
            
            if request_model.status == "currency":
                # --- Handle Currency Case ---
                if not request_model.conid:
                    logger.warning(f"[/add_instrument] ConID is required for currency type, but not provided for {request_model.identifier}. Aborting.")
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ConID must be provided for currency types.")
                if not request_model.identifier:
                     logger.warning(f"[/add_instrument] Identifier (currency code) is required. Aborting.")
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Identifier (currency code) is required.")

                try:
                    # Use the db session injected directly
                    result = await add_or_update_exchange_rate_conid(
                        session=db,
                        currency=request_model.identifier,
                        conid=request_model.conid
                    )

                    status_code = status.HTTP_200_OK # Default OK
                    db_status = result.get("status")
                    if db_status == "inserted":
                        status_code = status.HTTP_201_CREATED
                    elif db_status == "error":
                        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                    elif db_status == "conflict":
                         status_code = status.HTTP_409_CONFLICT # Use 409 for conflict

                    return JSONResponse(
                        content=result, 
                        status_code=status_code
                    )

                except Exception as e:
                    logger.error(f"[/add_instrument] Error processing currency {request_model.identifier}: {e}", exc_info=True)
                    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Internal server error processing currency.")

            elif request_model.status in ["candidate", "monitored", "portfolio", "indicator"]:
                # --- Handle Screener Case --- 
                logger.info(f"Attempting to add/update screener entry for {request_model.identifier} ({request_model.status})")
                
                # --- DIAGNOSTIC: Try importing and instantiating repository inside --- 
                try:
                    from .V3_database import SQLiteRepository # Use relative import
                    # Assuming DATABASE_URL is accessible or defined globally/in config
                    # You might need to fetch DATABASE_URL from app state or config if not global
                    temp_repo = SQLiteRepository(database_url=request.app.state.repository.database_url) # Use existing URL
                    logger.info(f"Diagnostic: temp_repo has method? {hasattr(temp_repo, 'add_or_update_screener_entry')}")
                    # Use the app state repository for the actual call for now, but log check
                    repository_to_use = repository # Keep using the injected one for the actual call
                except Exception as import_err:
                    logger.error(f"Diagnostic: Failed to import/instantiate repository inside endpoint: {import_err}")
                    repository_to_use = repository # Fallback to injected one
                # --- END DIAGNOSTIC --- 

                # Check if conid is provided for non-currency types
                if not request_model.conid:
                    logger.warning(f"[/add_instrument] ConID missing for screener type {request_model.status}, identifier {request_model.identifier}. Aborting.")
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ConID must be provided.")
                 
                # --- Prepare data dictionary and convert numeric strings --- 
                data_to_save = request_model.dict()
                numeric_fields_to_convert = {
                    'atr': float, 'atr_mult': float, # Treat atr_mult as float initially for flexible conversion
                    'risk': float, 'beta': float
                }
                for field, target_type in numeric_fields_to_convert.items():
                    str_value = data_to_save.get(field)
                    if str_value is None or str_value == '':
                        data_to_save[field] = None # Set to None if empty string or None
                    else:
                        try:
                            converted_value = target_type(str_value)
                            if field == 'atr_mult':
                                data_to_save[field] = int(converted_value) if converted_value.is_integer() else converted_value
                            else:
                                data_to_save[field] = converted_value
                        except (ValueError, TypeError):
                            logger.warning(f"[/add_instrument] Could not convert {field}='{str_value}' to {target_type.__name__}. Setting to None.")
                            data_to_save[field] = None
                # --- End data preparation --- 

                # --- Prepare data for database: Map 'identifier' to 'ticker' and remove 'identifier' ---
                if 'identifier' in data_to_save:
                    data_to_save['ticker'] = data_to_save.pop('identifier') # Use pop to get value and remove key

                # Ensure 'ticker' key exists if somehow missed (e.g., if input was 'ticker' directly)
                if 'ticker' not in data_to_save:
                     # Handle case where 'ticker' might be missing entirely? Should not happen with validation.
                     logger.error("CRITICAL: 'ticker' key is missing from data_to_save before DB call.")
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing ticker identifier.")
                # --- End data preparation for DB ---

                try:
                    # --- Add Diagnostics ---
                    logger.info(f"DIAGNOSTIC: Type of repository object: {type(repository)}")
                    # --- ADD: Print the file path of the class --- 
                    import inspect
                    try:
                        repo_file = inspect.getfile(repository.__class__)
                        logger.info(f"DIAGNOSTIC: Repository class defined in: {repo_file}")
                    except TypeError:
                        logger.warning("DIAGNOSTIC: Could not determine file for repository class (likely built-in or dynamically generated).")
                    # --- END ADD --- 
                    logger.info(f"DIAGNOSTIC: Attributes of repository object: {dir(repository)}")
                    # --- End Diagnostics ---
                    
                    # --- ADD FINAL CHECK --- 
                    logger.info(f"FINAL CHECK before call: dir(repository) = {dir(repository)}")
                    # --- END FINAL CHECK ---

                    # Use the repository instance injected via the function signature (or temp_repo for testing)
                    # Sticking to the injected `repository` for the actual call for now
                    
                    # --- Call the updated DB method and capture the result --- 
                    db_result = await repository.add_or_update_screener_ticker(**data_to_save)
                    logger.info(f"Screener DB operation result for ticker '{data_to_save.get('ticker')}': {db_result}")

                    # --- Determine HTTP status based on DB result --- 
                    response_status_code = status.HTTP_200_OK # Default to OK
                    if db_result.get("status") == "inserted":
                        response_status_code = status.HTTP_201_CREATED
                    elif db_result.get("status") == "error":
                        # Keep 500 for internal DB errors
                        response_status_code = status.HTTP_500_INTERNAL_SERVER_ERROR 
                    # Add other mappings if needed (e.g., 409 for skipped/conflict?)
                    elif db_result.get("status") == "skipped":
                         response_status_code = status.HTTP_200_OK # Or maybe 409 Conflict?
                         
                    # Return the result dict from the DB method directly
                    return JSONResponse(content=db_result, status_code=response_status_code)

                except Exception as e:
                    # This catches errors *before* or *during* the call to the DB method
                    # (like the diagnostic checks, or unexpected errors)
                    # The DB method itself now returns error status, so this block
                    # handles broader endpoint errors.
                    identifier_for_log = request_model.identifier if hasattr(request_model, 'identifier') else data_to_save.get('ticker', 'unknown')
                    logger.error(f"Error in /add_instrument endpoint before/during DB call for {identifier_for_log}: {e}", exc_info=True)
                    # Return a generic 500 error
                    return JSONResponse(
                        content={"status": "error", "message": f"Internal server error processing request for {identifier_for_log}"},
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
                    # Removed: raise HTTPException(...)

            else:
                # --- Handle Unknown Status ---
                logger.warning(f"[/add_instrument] Received unknown status type: {request_model.status}")
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid status type: {request_model.status}")

        # --- NEW Route for Add Ticker Page ---
        @app.get("/add_ticker", name="get_add_ticker_page", response_class=HTMLResponse)
        async def get_add_ticker_page(request: Request):
            """Serves the page for adding new tickers/instruments."""
            logger.info("Rendering add_ticker page")
            # Basic context, can add more if needed (e.g., initial form values)
            context = {"request": request}
            return request.app.state.templates.TemplateResponse(
                "add_ticker.html", context
            )
        # --- End Add Ticker Page Route ---

        @app.get("/config", name="get_config_page", response_class=HTMLResponse)
        async def get_config_page(request: Request):
            """Display the configuration page."""
            message = request.query_params.get('message')
            message_type = request.query_params.get('message_type', 'info')
            repository: SQLiteRepository = request.app.state.repository
            try:
                # Fetch ALL interval seconds
                current_interval = await repository.get_fetch_interval_seconds(default_interval=3600) # IBKR Fetch
                ibkr_snapshot_seconds = await repository.get_job_schedule_seconds('ibkr_sync_snapshot')
                finviz_seconds = await repository.get_job_schedule_seconds('finviz_data_fetch')
                yahoo_seconds = await repository.get_job_schedule_seconds('yahoo_data_fetch')
                investingcom_seconds = await repository.get_job_schedule_seconds('investingcom_data_fetch')
                
                # Fetch ALL active statuses
                ibkr_active = await repository.get_job_is_active('ibkr_fetch')
                ibkr_snapshot_active = await repository.get_job_is_active('ibkr_sync_snapshot')
                finviz_active = await repository.get_job_is_active('finviz_data_fetch')
                yahoo_active = await repository.get_job_is_active('yahoo_data_fetch')
                investingcom_active = await repository.get_job_is_active('investingcom_data_fetch')
                
                # Fetch other data
                exchange_rates = await get_exchange_rates(request.state.db_session) 
                portfolio_rules = await repository.get_all_portfolio_rules()
                portfolio_rules_json = json.dumps(portfolio_rules, default=str) # Keep JSON for JS

                # CORRECTED context dictionary
                context = {
                    "request": request,
                    "message": message,
                    "message_type": message_type,
                    # Scheduler Intervals
                    "current_interval": current_interval,
                    "ibkr_snapshot_schedule_seconds": ibkr_snapshot_seconds,
                    "finviz_schedule_seconds": finviz_seconds,
                    "yahoo_schedule_seconds": yahoo_seconds,
                    "investingcom_schedule_seconds": investingcom_seconds,
                    # Scheduler Statuses
                    "ibkr_is_active": ibkr_active,
                    "ibkr_snapshot_is_active": ibkr_snapshot_active,
                    "finviz_is_active": finviz_active,
                    "yahoo_is_active": yahoo_active,
                    "investingcom_is_active": investingcom_active,
                    # Other Data
                    "exchange_rates": exchange_rates,
                    "portfolio_rules": portfolio_rules, # Pass the list for Jinja
                    "portfolio_rules_json": portfolio_rules_json # Pass JSON string for JS
                }
                return request.app.state.templates.TemplateResponse("config.html", context)
            except Exception as e:
                logger.error(f"Error loading config page: {e}", exc_info=True)
                # CORRECTED defaults in the except block
                return request.app.state.templates.TemplateResponse("config.html", {
                    "request": request, 
                    "message": "Error loading configuration data.", # Display error message
                    "message_type": "danger",
                    # Default Intervals
                    "current_interval": 3600, 
                    "ibkr_snapshot_schedule_seconds": 0, 
                    "finviz_schedule_seconds": 0,
                    "yahoo_schedule_seconds": 0,
                    "investingcom_schedule_seconds": 0,
                    # Default Statuses (assume active on error?)
                    "ibkr_is_active": True, 
                    "ibkr_snapshot_is_active": True,
                    "finviz_is_active": True,
                    "yahoo_is_active": True,
                    "investingcom_is_active": True,
                     # Default Other Data
                    "exchange_rates": {},
                    "portfolio_rules": [], 
                    "portfolio_rules_json": "[]", 
                    "error": "Failed to load configuration." # Keep original error for logging? 
                 })

        # --- NEW Analytics Page Route ---
        @app.get("/analytics", response_class=HTMLResponse, name="get_analytics_page")
        async def get_analytics_page(request: Request):
            logger.info("Serving analytics page.")
            try:
                # In the future, fetch any data needed for the analytics page here
                context = {"request": request}
                return request.app.state.templates.TemplateResponse("analytics.html", context)
            except Exception as e:
                logger.exception("Error rendering analytics page", exc_info=True)
                # Handle error appropriately, maybe redirect or show an error page
                raise HTTPException(status_code=500, detail="Internal Server Error rendering analytics page.")
        # --- END Analytics Page Route ---

        # --- Existing route additions (like add_websocket_route) ---
        # ... (potentially add_websocket_route call here) ...

        # --- NEW Analytics Routes --- 
        
        @app.post("/api/analytics/process-raw-data", # NEW: Renamed URL
                  status_code=status.HTTP_200_OK, 
                  summary="Process Raw Analytics Data", # NEW: Renamed Summary
                  tags=["Analytics"])
        async def process_analytics_data_endpoint(repository: SQLiteRepository = Depends(get_repository)): # NEW: Renamed Function
            """
            Processes all raw data stored in the analytics_raw table and stores the result in memory.
            """
            logger.info("Received request to process raw analytics data.") # NEW: Renamed Log
            try:
                # Step 1: Get the raw data from the DB
                logger.info("Fetching all raw analytics data from DB...")
                all_raw_data = await repository.get_all_analytics_raw_data()

                if not all_raw_data:
                    logger.warning("No raw analytics data found in the database.")
                    global_processed_analytics_data.clear() # NEW: Use renamed global
                    return {"message": "No raw analytics data found to process."}

                logger.info(f"Fetched {len(all_raw_data)} raw records. Processing...")

                # Step 2: Preprocess the data using the function from V3_analytics
                processed_data = preprocess_raw_analytics_data(all_raw_data) # NEW: Call renamed function
                logger.info(f"Successfully processed {len(processed_data)} records.")

                # Step 3: Store the processed data in the global variable
                global_processed_analytics_data.clear() # NEW: Use renamed global
                global_processed_analytics_data.extend(processed_data) # NEW: Use renamed global
                
                logger.info("Processed analytics data stored in memory.") # NEW: Renamed Log
                
                return {"message": f"Successfully processed {len(processed_data)} analytics entries and stored in memory."} # NEW: Renamed Message

            except Exception as e:
                logger.error(f"Error processing raw analytics data: {e}", exc_info=True)
                global_processed_analytics_data.clear() # NEW: Use renamed global
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                                    detail=f"An error occurred during data processing: {e}")

        @app.get("/api/analytics/get-processed-data", # NEW: Renamed URL
                 summary="Get Processed Analytics Data from Memory", # NEW: Renamed Summary
                 tags=["Analytics"])
        async def get_processed_analytics_data_endpoint(): # NEW: Renamed Function
            # Return the data stored in memory
            logger.info(f"Returning {len(global_processed_analytics_data)} processed analytics records from memory.") # NEW: Renamed Log & Global
            return global_processed_analytics_data # NEW: Return renamed global

        @app.post("/api/analytics/start-finviz-fetch-screener", 
                  status_code=status.HTTP_202_ACCEPTED, # Use 202 Accepted for background tasks
                  summary="Trigger Finviz Fetch for Analytics (Screener Tickers)",
                  response_model=Dict[str, str], # <<< Define response model
                  tags=["Analytics", "Data Import"])
        async def trigger_finviz_fetch_screener_endpoint(
            background_tasks: BackgroundTasks, 
            repository: SQLiteRepository = Depends(get_repository)
        ):
            """Triggers a background task to fetch Finviz data for all tickers in the screener."""
            logger.info("[API Analytics] Received request to trigger Finviz fetch for screener tickers.")
            
            # Generate unique Job ID
            job_id = str(uuid.uuid4())
            message = "Finviz fetch job for screener tickers triggered successfully."
            
            # Set initial status
            async with job_status_lock:
                job_statuses[job_id] = {
                    "status": "running", 
                    "message": "Job started...", 
                    "timestamp": datetime.now()
                }
            
            # Schedule the background task (will pass job_id in Step 3)
            # For now, it schedules the existing function, but the wrapper will be added later
            background_tasks.add_task(run_analytics_finviz_fetch, repository, job_id) # Pass job_id
            
            logger.info(f"[API Analytics] Scheduled background Finviz fetch (Screener). Job ID: {job_id}")
            # Return the job ID along with the message
            return {"message": message, "job_id": job_id}

        # --- Helper function to run the analytics fetch in the background --- 
        async def run_analytics_finviz_fetch(repository: SQLiteRepository, job_id: str): # <<< Add job_id
             """Fetches screener tickers and runs the Finviz analytics fetch."""
             logger.info(f"[Background Task - {job_id}] Starting analytics Finviz fetch for screener tickers.")
             start_time = datetime.now()
             status = "failed" # Default to failed
             final_message = "An unexpected error occurred."
             try:
                 # 1. Get tickers from the screener table
                 screened_tickers_data = await repository.get_all_screened_tickers()
                 tickers = [item['ticker'] for item in screened_tickers_data if item.get('ticker')]
                 
                 if not tickers:
                     logger.warning(f"[Background Task - {job_id}] No tickers found in screener for analytics fetch.")
                     final_message = "No tickers found in screener table."
                     status = "completed" # Consider this a success (no work to do)
                 else:
                     logger.info(f"[Background Task - {job_id}] Found {len(tickers)} tickers. Fetching Finviz data...")
                     # 2. Call the fetch function
                     # Modify fetch_and_store_analytics_finviz in Step 4 to return status dict
                     result = await fetch_and_store_analytics_finviz(repository, tickers)
                     # Update status based on result (from Step 4)
                     status = result.get("status", "failed")
                     final_message = result.get("message", "Fetch completed with unknown status.")
                     logger.info(f"[Background Task - {job_id}] Finviz fetch completed. Status: {status}")
                     
             except Exception as e:
                 logger.error(f"[Background Task - {job_id}] Error during analytics Finviz fetch: {e}", exc_info=True)
                 final_message = f"Error during fetch: {e}"
                 status = "failed"
             finally:
                 # Update the final job status using the lock
                 end_time = datetime.now()
                 duration = end_time - start_time
                 logger.info(f"[Background Task - {job_id}] Finished in {duration}. Final Status: {status}")
                 async with job_status_lock:
                     job_statuses[job_id] = {
                         "status": status,
                         "message": final_message,
                         "timestamp": end_time
                     }
                     logger.debug(f"[Background Task - {job_id}] Updated global job status.")
        # --- End Helper --- 

        @app.post("/api/analytics/start-finviz-fetch-upload",
                  status_code=status.HTTP_202_ACCEPTED,
                  summary="Trigger Finviz Fetch for Analytics (Uploaded Tickers)",
                  response_model=Dict[str, str], # <<< Define response model
                  tags=["Analytics", "Data Import"])
        async def trigger_finviz_fetch_upload_endpoint(
            payload: TickerListPayload, # Use the Pydantic model for the request body
            background_tasks: BackgroundTasks,
            repository: SQLiteRepository = Depends(get_repository)
        ):
            """Triggers a background task to fetch Finviz data for a provided list of tickers."""
            tickers = payload.tickers
            logger.info(f"[API Analytics] Received request to trigger Finviz fetch for {len(tickers)} uploaded tickers.")

            # Generate unique Job ID
            job_id = str(uuid.uuid4())
            message = f"Finviz fetch job for {len(tickers)} uploaded tickers triggered successfully."

            # Set initial status
            async with job_status_lock:
                job_statuses[job_id] = {
                    "status": "running", 
                    "message": "Job started...", 
                    "timestamp": datetime.now()
                }

            # Schedule the background task (will pass job_id in Step 3)
            background_tasks.add_task(run_analytics_finviz_fetch_list, repository, tickers, job_id) # Pass job_id

            logger.info(f"[API Analytics] Scheduled background Finviz fetch (Upload). Job ID: {job_id}")
            # Return the job ID along with the message
            return {"message": message, "job_id": job_id}
        
        # --- Helper function to run the list fetch in the background --- 
        async def run_analytics_finviz_fetch_list(repository: SQLiteRepository, tickers: List[str], job_id: str): # <<< Add job_id
            """Runs the Finviz analytics fetch for a specific list of tickers."""
            logger.info(f"[Background Task - {job_id}] Starting analytics Finviz fetch for {len(tickers)} uploaded tickers.")
            start_time = datetime.now()
            status = "failed"
            final_message = "An unexpected error occurred."
            try:
                if not tickers:
                    logger.warning(f"[Background Task - {job_id}] No tickers provided in the list.")
                    final_message = "No tickers provided."
                    status = "completed"
                else:
                    # Call the fetch function
                    # Modify fetch_and_store_analytics_finviz in Step 4 to return status dict
                    result = await fetch_and_store_analytics_finviz(repository, tickers)
                    # Update status based on result (from Step 4)
                    status = result.get("status", "failed")
                    final_message = result.get("message", "Fetch completed with unknown status.")
                    logger.info(f"[Background Task - {job_id}] Finviz fetch for list completed. Status: {status}")
                    
            except Exception as e:
                logger.error(f"[Background Task - {job_id}] Error during analytics Finviz fetch for list: {e}", exc_info=True)
                final_message = f"Error during fetch: {e}"
                status = "failed"
            finally:
                # Update the final job status using the lock
                end_time = datetime.now()
                duration = end_time - start_time
                logger.info(f"[Background Task - {job_id}] Finished list fetch in {duration}. Final Status: {status}")
                async with job_status_lock:
                    job_statuses[job_id] = {
                        "status": status,
                        "message": final_message,
                        "timestamp": end_time
                    }
                    logger.debug(f"[Background Task - {job_id}] Updated global job status for list fetch.")
        # --- End Helper --- 

        @app.get("/api/analytics/stream-job-status/{job_id}",
                 summary="Stream Job Status Updates (SSE)",
                 tags=["Analytics", "SSE"])
        async def stream_job_status(request: Request, job_id: str):
            """Server-Sent Events endpoint to stream status updates for a background job."""
            logger.info(f"[SSE] Client connected for job_id: {job_id}")

            async def event_generator():
                try:
                    last_status = None
                    while True:
                        # Check if client is still connected
                        if await request.is_disconnected():
                            logger.info(f"[SSE] Client disconnected for job_id: {job_id}")
                            break

                        # Check job status using the lock
                        async with job_status_lock:
                            current_job_info = job_statuses.get(job_id)
                        
                        if not current_job_info:
                            logger.warning(f"[SSE] Job ID {job_id} not found in status tracking. Closing stream.")
                            # Optionally send an error event before closing
                            yield f"event: error\ndata: {json.dumps({'status': 'error', 'message': 'Job ID not found'})}\n\n"
                            break
                            
                        current_status = current_job_info.get("status")

                        # Only proceed if status exists
                        if current_status:
                            # Send update only if status is final (completed/failed)
                            if current_status in ["completed", "failed", "partial_failure"]:
                                logger.info(f"[SSE] Job {job_id} finished with status: {current_status}. Sending final update.")
                                # Send the final status and message
                                data_to_send = {
                                    "status": current_status,
                                    "message": current_job_info.get("message", "Job finished.")
                                }
                                yield f"data: {json.dumps(data_to_send)}\n\n"
                                logger.debug(f"[SSE] Sent final data for {job_id}. Breaking loop.")
                                break # Exit loop after sending final status
                            # else: # Optional: send 'running' or other intermediate statuses if needed
                            #     if current_status != last_status: # Send only on change
                            #         logger.debug(f"[SSE] Job {job_id} status is {current_status}. Waiting...")
                            #         # yield f"event: progress\ndata: {json.dumps({'status': current_status, 'message': 'In progress...'})}\n\n"
                            #         last_status = current_status
                        else:
                            # Handle case where status key might be missing (shouldn't happen with current logic)
                            logger.warning(f"[SSE] Status key missing for job_id {job_id}. Waiting...")

                        # Wait before checking again
                        await asyncio.sleep(1) # Poll every 1 second
                        
                except asyncio.CancelledError:
                    logger.info(f"[SSE] Task cancelled for job_id: {job_id}")
                    raise
                except Exception as e:
                    logger.error(f"[SSE] Error during event generation for job_id {job_id}: {e}", exc_info=True)
                    # Optionally send an error event to the client
                    try:
                        yield f"event: error\ndata: {json.dumps({'status': 'error', 'message': f'SSE Error: {e}'})}\n\n"
                    except Exception as send_err:
                        logger.error(f"[SSE] Failed to send error event to client for job_id {job_id}: {send_err}")
                finally:
                     logger.info(f"[SSE] Closing event stream for job_id: {job_id}")

            # Return the streaming response using the generator
            return StreamingResponse(event_generator(), media_type="text/event-stream")
        # --- End SSE Endpoint ---
        
        # ------ Helper for Sync Service Calls -------
        # (Add this if needed for other sync tasks)

        logger.info("FastAPI app instance created and configured successfully.")
        return app 
        # --- End Correct Indentation / End of try block ---

    except Exception as e: # <-- This except block is for the main create_app try
        logger.critical(f"CRITICAL ERROR DURING APP CREATION in create_app(): {e}", exc_info=True)
        raise # Re-raise the exception so the main script knows it failed
        
# --- Add WebSocket Endpoint Definition --- 
# Note: This needs to be defined *outside* create_app if app is global,
# or ideally, create_app returns the app and this is defined after calling create_app.
# Assuming app is created and then routes are added:
# app = create_app() # If create_app is called first

# We need access to the `app` instance created in `create_app`.
# Since `create_app` returns the app, we should define this endpoint
# *after* the `create_app` function definition, assuming the main script
# calls `create_app` and then uses the returned app object.
# However, to keep it within this file for now, we need a way to access
# the app or its manager. Let's define it assuming `app` is accessible 
# globally *after* `create_app` is called, or adjust if needed.

# A placeholder function assuming we will get the app instance later
def add_websocket_route(app_instance: FastAPI):
    manager: ConnectionManager = app_instance.state.manager # Get manager from app state
    @app_instance.websocket("/ws/status")
    async def websocket_endpoint(websocket: WebSocket):
        await manager.connect(websocket)
        try:
            while True:
                # Keep connection alive, wait for disconnect
                data = await websocket.receive_text() 
                # Optionally handle messages from client here if needed
                logger.debug(f"WebSocket received message: {data}") 
        except WebSocketDisconnect:
            manager.disconnect(websocket)
            logger.info(f"WebSocket client disconnected: {websocket.client}")
        except Exception as e:
            logger.error(f"WebSocket error: {e}", exc_info=True)
            manager.disconnect(websocket) # Ensure disconnect on error

# The main script (e.g., V3_main.py) should do:
# from V3_web import create_app, add_websocket_route
# app = create_app()
# add_websocket_route(app) 
        