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
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Request, Form, WebSocket, WebSocketDisconnect, Body, Depends, status, BackgroundTasks, APIRouter, Query # MODIFIED: Added APIRouter and Query
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.base import JobLookupError # <-- ADD THIS LINE
from apscheduler.triggers.cron import CronTrigger # <-- ADD THIS LINE
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
import httpx # ADDED: For making HTTP requests in scheduled jobs
# --- Add CORS Middleware Import --- 
from fastapi.middleware.cors import CORSMiddleware
# --- End Import --- 
# --- Add StaticFiles Import ---
from fastapi.staticfiles import StaticFiles # <<< ADD THIS IMPORT
# --- End StaticFiles Import ---

# --- Standard Library Imports ---
# ... (other imports)

# --- Local Application Imports ---
# Import necessary functions and classes from other modules
from .V3_ibkr_api import IBKRService, IBKRError, SyncIBKRService # ADD SyncIBKRService
from .V3_finviz_fetch import fetch_and_store_finviz_data, update_screener_from_finviz # Corrected import
from .V3_finviz_fetch import fetch_and_store_analytics_finviz
# from .V3_yahoo_api import YahooFinanceAPI, YahooError # Assuming Yahoo API integration
from .V3_database import SQLiteRepository, get_exchange_rates, update_exchange_rate, add_or_update_exchange_rate_conid, update_screener_multi_fields_sync, get_screener_tickers_and_conids_sync, get_exchange_rates_and_conids_sync # Import new DB function AND SQLiteRepository
from .services.notification_service import dispatch_notification
# --- End Local Application Imports ---

# --- Import V3_ibkr_monitor (Keep existing imports) ---
# ... existing imports ...
from .V3_database import SQLiteRepository # Ensure get_db is imported if not already
# --- ADD Import for IBKR Monitor and potentially analytics --- 
from .V3_ibkr_monitor import register_ibkr_monitor # Use alias to avoid name clash
# ADD Analytics import <<<<< ADD THIS LINE
from .V3_analytics import preprocess_raw_analytics_data # CORRECTED Import

# --- Import Utilities Router --- 
from .routers import utilities_router
# --- End Import Utilities Router ---
from .routers import edgar_router # <<< ADD THIS IMPORT
from .routers import notification_routes # <<< ADD THIS IMPORT
from .routers.yahoo_job_router import router as yahoo_job_api_router
from . import yahoo_job_manager

# --- NEW: Import Finviz Job Manager ---
from . import finviz_job_manager
from .finviz_job_manager import (
    FINVIZ_MASS_FETCH_JOB_ID,
    trigger_finviz_mass_fetch_job,
    get_job_details_internal as get_finviz_job_details_internal, # Alias to avoid conflicts
    finviz_sse_update_queue
)
# --- END: Import Finviz Job Manager ---

# --- NEW: Pydantic models for Job System (if not already globally defined) ---
# These might already exist or be imported from a common models file.
# For clarity, defining what JobDetailsResponse and TickerListPayload are expected to be.
# from .V3_models import TickerListPayload, JobDetailsResponse # Assuming they are in V3_models.py
# If not, they would be defined like this:
# class TickerListPayload(BaseModel):
#     tickers: List[str]
# class JobDetailsResponse(BaseModel):
#     job_id: str
#     status: str
#     message: Optional[str] = None
#     job_type: Optional[str] = None 
#     progress_percent: Optional[int] = None
#     current_count: Optional[int] = None
#     total_count: Optional[int] = None
#     last_run_summary: Optional[str] = None
#     # ... other relevant fields
# Ensure these models are correctly imported or defined globally if needed.
from .V3_models import TickerListPayload, JobDetailsResponse # This should already be there for Yahoo jobs
from .V3_models import JobDetailedLogResponse # <<< ADD IMPORT FOR NEW MODEL

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
# --- NEW: Finviz Job API Router ---
finviz_job_api_router = APIRouter() # MODIFIED: Changed from FastAPI() to APIRouter()

# --- NEW: Generic Jobs API Router ---
jobs_api_router = APIRouter(prefix="/api", tags=["Jobs"]) # Using /api prefix for consistency

@jobs_api_router.get("/jobs/{job_id}/detailed_log", 
                     response_model=JobDetailedLogResponse, 
                     summary="Get Detailed Run Log for a Job")
async def get_job_detailed_run_log(
    job_id: str,
    repository: SQLiteRepository = Depends(get_repository)
):
    try:
        logger.debug(f"[API GET JOB DETAILED LOG] Request for job_id: {job_id}")
        job_state = await repository.get_persistent_job_state(job_id)
        if not job_state:
            logger.warning(f"[API GET JOB DETAILED LOG] Job state not found for job_id: {job_id}")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job state not found.")
        
        summary = job_state.get("last_run_summary")
        if summary is None:
            logger.info(f"[API GET JOB DETAILED LOG] No detailed summary available for job_id: {job_id}")
            # Return a message indicating no summary, rather than an error, if the key exists but is None
            return JobDetailedLogResponse(job_id=job_id, detailed_log="No detailed summary available for this job yet.")

        logger.debug(f"[API GET JOB DETAILED LOG] Found summary for job_id: {job_id}: {summary[:100]}...") # Log a snippet
        return JobDetailedLogResponse(job_id=job_id, detailed_log=summary)
    
    except HTTPException as http_exc: # Re-raise HTTPExceptions directly
        raise http_exc
    except Exception as e:
        logger.error(f"[API GET JOB DETAILED LOG] Error getting detailed log for job_id {job_id}: {e}", exc_info=True)
        # Return a consistent error response model for unexpected errors
        # If we want to send an error in the response body with 500, 
        # we can return JobDetailedLogResponse(job_id=job_id, error=f"Internal server error: {str(e)}") 
        # but FastAPI typically handles this with a default JSON error response for HTTP 500.
        # For now, let client handle generic 500 or specific 404 from above.
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get detailed job log: {str(e)}")

@finviz_job_api_router.post("/finviz/trigger", response_model=JobDetailsResponse, summary="Trigger Finviz Mass Fetch Job")
async def trigger_finviz_job(
    payload: TickerListPayload, # For uploaded tickers or dummy for screener
    source_type: Optional[str] = Query(None, description="Source of tickers. Expected: 'finviz_screener' or 'upload_finviz_txt'"),
    repository: SQLiteRepository = Depends(get_repository) 
):
    try:
        effective_source = source_type
        if not effective_source:
            # If source_type is None, and payload.tickers is guaranteed by TickerListPayload to be non-empty,
            # it implies an upload from a client not yet sending source_type.
            # Defaulting to upload_finviz_txt if not specified by query param.
            effective_source = "upload_finviz_txt"
            logger.info(f"[API FINVIZ TRIGGER] 'source_type' query parameter not provided, defaulting to '{effective_source}' based on payload presence.")
        
        logger.info(f"[API FINVIZ TRIGGER] Effective source: '{effective_source}', Payload tickers count: {len(payload.tickers)}.")
        
        job_details = await finviz_job_manager.trigger_finviz_mass_fetch_job(
            request_payload=payload, 
            source_identifier=effective_source, 
            repository=repository
        )
        return job_details
    except HTTPException as http_exc: # Re-raise HTTPExceptions directly
        raise http_exc
    except Exception as e:
        logger.error(f"[API FINVIZ TRIGGER] Error triggering Finviz job: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to trigger Finviz job: {str(e)}")

@finviz_job_api_router.get("/finviz/details", response_model=JobDetailsResponse, summary="Get Finviz Mass Fetch Job Details")
async def get_finviz_job_status_details(
    repository: SQLiteRepository = Depends(get_repository)
):
    try:
        logger.debug(f"[API FINVIZ DETAILS] Received request for Finviz job details.")
        job_details = await finviz_job_manager.get_job_details_internal(FINVIZ_MASS_FETCH_JOB_ID, repository)
        return job_details
    except Exception as e:
        logger.error(f"[API FINVIZ DETAILS] Error getting Finviz job details: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get Finviz job details: {str(e)}")

@finviz_job_api_router.get("/finviz/sse", summary="Finviz Job Status SSE Stream")
async def finviz_job_sse(
    request: Request, 
    repository: SQLiteRepository = Depends(get_repository) # Added repository dependency
): 
    logger.info(f"[API FINVIZ SSE] Client connected for Finviz job status stream. Using queue (id: {id(finviz_job_manager.finviz_sse_update_queue)}).") # MODIFIED LOG

    async def event_generator():
        initial_status_sent = False
        try:
            logger.debug(f"[API FINVIZ SSE] INITIAL_STATUS_BLOCK: Attempting to get initial details for {FINVIZ_MASS_FETCH_JOB_ID}.") # ADDED LOG
            initial_status = await get_finviz_job_details_internal(FINVIZ_MASS_FETCH_JOB_ID, repository)
            logger.debug(f"[API FINVIZ SSE] INITIAL_STATUS_BLOCK: Got initial_status object: {initial_status is not None}, Status: {initial_status.get('status') if initial_status else 'N/A'}.") # ADDED LOG
            
            if initial_status:
                logger.debug(f"[API FINVIZ SSE] INITIAL_STATUS_BLOCK: Preparing to yield initial status: {initial_status.get('status')}.") # ADDED LOG
                # REVERTED: Send the full initial_status object
                yield f"data: {json.dumps(initial_status)}\n\n"
                initial_status_sent = True
                logger.debug(f"[API FINVIZ SSE] INITIAL_STATUS_BLOCK: Successfully yielded full initial status: {initial_status.get('status')}.") # MODIFIED LOG
            else:
                logger.debug(f"[API FINVIZ SSE] INITIAL_STATUS_BLOCK: No initial_status found for {FINVIZ_MASS_FETCH_JOB_ID}. Not sending initial data.") # ADDED LOG
            
            logger.debug(f"[API FINVIZ SSE] INITIAL_STATUS_BLOCK: Finished try block. initial_status_sent = {initial_status_sent}")

        except asyncio.CancelledError as e_cancel_initial:
            logger.error(f"[API FINVIZ SSE] INITIAL_STATUS_BLOCK: CancelledError during initial status fetch/send: {e_cancel_initial}", exc_info=True)
            # Do not yield here if cancelled, just log and let it proceed to main loop or exit
            logger.debug("[API FINVIZ SSE] INITIAL_STATUS_BLOCK: Finished CancelledError block. Generator will return.") # MODIFIED LOG
            return # MODIFIED: from raise to return
        except Exception as e_initial:
            logger.error(f"[API FINVIZ SSE] INITIAL_STATUS_BLOCK: Exception during initial status fetch/send: {e_initial}", exc_info=True)
            error_event = {"job_id": FINVIZ_MASS_FETCH_JOB_ID, "status": "error", "message": f"Error fetching initial state: {str(e_initial)}"}
            try:
                yield f"data: {json.dumps(error_event)}\n\n"
                logger.debug(f"[API FINVIZ SSE] INITIAL_STATUS_BLOCK: Yielded error event due to exception.")
            except Exception as e_yield_err:
                logger.error(f"[API FINVIZ SSE] INITIAL_STATUS_BLOCK: Exception while yielding error event: {e_yield_err}", exc_info=True)
            logger.debug("[API FINVIZ SSE] INITIAL_STATUS_BLOCK: Finished Exception block.")
        finally:
            logger.debug(f"[API FINVIZ SSE] INITIAL_STATUS_BLOCK: Reached finally block. initial_status_sent = {initial_status_sent}")

        logger.debug(f"[API FINVIZ SSE] Proceeding to main event loop for queue after initial status block. Initial status was sent: {initial_status_sent}")
        
        # REMOVE THE ENTIRE "EARLY_QUEUE_DRAIN" try/except block.

        try: # Outer try for the main loop
            first_queue_attempt = True # ADDED
            while True:
                logger.debug(f"[API FINVIZ SSE] Top of main event loop. First attempt: {first_queue_attempt}") # MODIFIED LOG
                try:
                    if await request.is_disconnected():
                        logger.info("[API FINVIZ SSE] Client disconnected (checked before queue.get()).")
                        break 

                    current_timeout = 0.1 if first_queue_attempt else 15.0 # ADDED short timeout for first attempt
                    logger.debug(f"[API FINVIZ SSE] Attempting finviz_sse_update_queue.get() (id: {id(finviz_job_manager.finviz_sse_update_queue)}) with timeout {current_timeout}s. Queue size: {finviz_job_manager.finviz_sse_update_queue.qsize()}") # MODIFIED LOG
                    
                    status_update = await asyncio.wait_for(finviz_job_manager.finviz_sse_update_queue.get(), timeout=current_timeout)
                    
                    logger.debug(f"[API FINVIZ SSE] GOT FROM QUEUE: Status {status_update.get('status')}, JobID {status_update.get('job_id')}, JobType {status_update.get('job_type')}")
                    first_queue_attempt = False # ADDED: Reset after first successful get or even timeout

                    if status_update.get("job_id") == FINVIZ_MASS_FETCH_JOB_ID and status_update.get("job_type") == "finviz_mass_fetch":
                        logger.debug(f"[API FINVIZ SSE] YIELDING data for job {FINVIZ_MASS_FETCH_JOB_ID}: Status {status_update.get('status')}")
                        # REVERTED: Send the full status_update object
                        yield f"data: {json.dumps(status_update)}\n\n"
                        log_msg = status_update.get('progress_message', status_update.get('message', 'No message'))
                        if len(log_msg) > 70 : log_msg = log_msg[:67] + "..."
                        logger.debug(f"[API FINVIZ SSE] Sent update: {status_update.get('status')}, Msg: {log_msg}")
                
                except asyncio.TimeoutError:
                    logger.debug(f"[API FINVIZ SSE] Timeout (duration: {current_timeout}s) occurred waiting for queue item. First attempt was: {first_queue_attempt}") # MODIFIED LOG
                    first_queue_attempt = False # ADDED: Ensure it's false after first timeout
                    if await request.is_disconnected():
                        logger.info("[API FINVIZ SSE] Client disconnected (checked after queue.get() timeout).")
                        break 
                    yield ": finviz heartbeat\\n\\n" 
                    continue
                except asyncio.CancelledError:
                    logger.info("[API FINVIZ SSE] Main loop: Stream cancelled by client disconnect or server shutdown (CancelledError). Loop will break.") # MODIFIED LOG
                    break # MODIFIED: Changed from raise to break
                except Exception as e:
                    logger.error(f"[API FINVIZ SSE] Error in Finviz SSE event generator main loop: {e}", exc_info=True)
                    error_event = {"job_id": FINVIZ_MASS_FETCH_JOB_ID, "job_type": "finviz_mass_fetch", "status": "error", "message": "SSE stream error occurred on server"}
                    try:
                        yield f"data: {json.dumps(error_event)}\n\n"
                    except Exception as send_err:
                        logger.error(f"[API FINVIZ SSE] Failed to send error event to client (Finviz): {send_err}")
                    await asyncio.sleep(2)
        finally:
            logger.info(f"[API FINVIZ SSE] Exiting event_generator for Finviz. Request is_disconnected: {await request.is_disconnected() if request else 'N/A - Request object not available'}")
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")

# --- END: Finviz Job API Router ---

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
    job_successful = False # Initialize success flag
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
        job_successful = True # Set success flag
    except Exception as e:
        logger.error(f"[Sync Snapshot Job] Error during execution: {e}", exc_info=True)
        # Optionally: Send error status via websocket
        # message = json.dumps({"type": "status", "service": "ibkr_snapshot", "status": "error", "message": str(e)})
        # loop.call_soon_threadsafe(asyncio.create_task, manager.broadcast(message))
    finally:
        summary_log = "IBKR snapshot job completed successfully." if job_successful else "IBKR snapshot job failed. Check logs."
        coro = dispatch_notification(db_repo=repository, task_id='scheduled_ibkr_snapshot', message=summary_log)
        loop.call_soon_threadsafe(asyncio.create_task, coro)
        logger.info("[Sync Snapshot Job] Finished and notification dispatched.")

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
    schedule: str # Raw input: CRON expression string (e.g., "0 */2 * * *")

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
        from .V3_backend_api import router as backend_router
        @app.get("/api/analytics/fields", summary="Get unified analytics field list", tags=["Analytics"])
        async def get_analytics_fields_endpoint(
            repository: SQLiteRepository = Depends(get_repository)
        ):
            """
            Returns a unified list of all fields available for analytics configuration (Finviz + Yahoo).
            """
            from .V3_analytics import get_finviz_fields_for_analytics
            finviz_fields = get_finviz_fields_for_analytics(global_processed_analytics_data)
            yahoo_db_url = repository.database_url if hasattr(repository, 'database_url') else None
            if not yahoo_db_url:
                return {"error": "Yahoo DB URL not found"}
            from .yahoo_repository import YahooDataRepository
            yahoo_repo = YahooDataRepository(yahoo_db_url)
            yahoo_fields = await yahoo_repo.get_all_yahoo_fields_for_analytics()
            return finviz_fields + yahoo_fields
        app.include_router(backend_router)

        # --- Include Routers (Consolidated) ---
        app.include_router(utilities_router.router, prefix="/api/v3/utilities", tags=["Utilities"])
        app.include_router(edgar_router.router, prefix="/api/v3", tags=["EDGAR"]) # MODIFIED THIS LINE
        app.include_router(yahoo_job_api_router, prefix="/api/v3/jobs", tags=["Jobs - Yahoo"])
        app.include_router(finviz_job_api_router, prefix="/api/v3/jobs", tags=["Jobs - Finviz"]) # NEW: Include Finviz job router
        app.include_router(jobs_api_router) # <<< ADD THE NEW GENERIC JOBS ROUTER
        # --- End Include Routers ---

        # Revert: Put back static files setup?
        # Assuming it was correct before
        current_dir = os.path.dirname(os.path.abspath(__file__))
        static_dir = os.path.join(current_dir, "static")
        # --- Mount Static Directory ---
        # Ensure the directory exists (optional but good practice)
        os.makedirs(static_dir, exist_ok=True) 
        logger.info(f"Mounting static directory: {static_dir} at /static")
        app.mount("/static", StaticFiles(directory=static_dir), name="static")
        logger.info("Static files mounted successfully.")
        # --- End Mount Static Directory ---

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
                if fetch_successful:
                    summary_log = f"IBKR scheduled job completed successfully for accounts: {all_account_ids}."
                else:
                    summary_log = "IBKR scheduled job failed. Check logs for details."
                
                await dispatch_notification(db_repo=app.state.repository, task_id='scheduled_ibkr_data', message=summary_log)
                
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

            # Initialize summary variables to prevent NameError if fetch fails
            updated_tickers, added_tickers, error_tickers = [], [], []

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

            summary_log = f"Finviz scheduled job completed. Updated: {len(updated_tickers)}, Added: {len(added_tickers)}, Errors: {len(error_tickers)}. Check logs for details."
            logger.info(summary_log)
            
            await dispatch_notification(db_repo=app.state.repository, task_id='scheduled_finviz_data', message=summary_log)
            
            return


        # --- NEW Yahoo Scheduled Job --- 
        async def scheduled_yahoo_job():
            """Job function for fetching Yahoo data and updating screener."""
            job_id = "yahoo_data_fetch"
            logger.info(f"Scheduler executing {job_id}...")

            repository: SQLiteRepository = app.state.repository # Define repository once here

            # --- ADD Check is_active status --- 
            try:
                # repository: SQLiteRepository = app.state.repository # Removed re-definition
                is_active = await repository.get_job_is_active(job_id)
                if not is_active:
                    logger.info(f"Job '{job_id}' is inactive in DB. Skipping execution.")
                    return
            except Exception as check_err:
                logger.error(f"Error checking active status for {job_id}: {check_err}. Skipping execution.", exc_info=True)
                return
            # --- END Check ---

            fetch_successful = False
            # update_successful = False # This flag was not used for last_run logic, can be kept or removed based on other needs
            job_lock: asyncio.Lock = app.state.job_execution_lock
            
            if job_lock.locked():
                logger.warning(f"Skipping {job_id}: Another job is already running.")
                return
            
            await job_lock.acquire() # Acquire lock
            try:
                # The is_active check has already passed if we are here.
                logger.info(f"Job '{job_id}' is active. Proceeding with fetch and update.")
                from .V3_yahoo_fetch import mass_load_yahoo_data_from_file
                from .yahoo_repository import YahooDataRepository
            
                yahoo_repo = YahooDataRepository(repository.database_url)
                tickers = await yahoo_repo.yahoo_incremental_refresh()
                
                if not tickers:
                    logger.warning("No tickers found in the database for the scheduled Yahoo job. Skipping.")
                    # fetch_successful remains False, so last_run won't be updated.
                    return
            
                await mass_load_yahoo_data_from_file(tickers, yahoo_repo)
                fetch_successful = True # Mark fetch as successful
                
                # await update_screener_from_yahoo(repository) # Assuming this is intended
                # update_successful = True 
                
                logger.info(f"Scheduled Yahoo fetch and screener update completed.")
                
                # --- Broadcast Update --- 
                try:
                    logger.info(f"[{job_id}] Broadcasting data update notification.")
                    await app.state.manager.broadcast(json.dumps({"event": "data_updated"}))
                except Exception as broadcast_err:
                    logger.error(f"[{job_id}] Error during broadcast: {broadcast_err}")
                # --- End Broadcast --- 
                    
                summary_log = f"Yahoo scheduled job completed. Processed {len(tickers)} tickers. See logs for details."
                await dispatch_notification(db_repo=repository, task_id='scheduled_yahoo_data', message=summary_log)
                    
            except Exception as e:
                logger.error(f"Error during scheduled {job_id} execution: {str(e)}")
                logger.exception(f"Scheduled {job_id} failed:")
                fetch_successful = False # Ensure this is false on error
            finally:
                if fetch_successful: # Only update last_run if the core task was successful
                    try:
                        now = datetime.now()
                        logger.info(f"Updating last_run for successful job '{job_id}' to {now}")
                        await repository.update_job_config(job_id, {'last_run': now})
                    except Exception as db_update_err:
                        logger.error(f"Failed to update last_run time for successful {job_id}: {db_update_err}")
                
                job_lock.release() # Release lock
                logger.debug(f"Job lock released by {job_id}")

            # Remove the old last_run update block from here
            # if is_active: 
            #     try:
            #         now = datetime.now()
            #         logger.info(f"Updating last_run for job '{job_id}' to {now}")
            #         await repository.update_job_config(job_id, {'last_run': now})
            #     except Exception as db_update_err:
            #          logger.error(f"Failed to update last_run time for {job_id}: {db_update_err}")

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
                db_url_for_sync_job = app.state.repository.database_url
                logger.info(f"Using DB URL for sync job: {db_url_for_sync_job}")
                
                global scheduler
                if not scheduler:
                    scheduler = AsyncIOScheduler()
                    app.state.scheduler = scheduler 
                
                loop = asyncio.get_running_loop()
                manager: ConnectionManager = app.state.manager
                 
                job_ids_to_configure = ["ibkr_fetch", "finviz_data_fetch", "yahoo_data_fetch", "ibkr_sync_snapshot", "analytics_data_cache_refresh", "analytics_metadata_cache_refresh"]
                default_cron_schedules = {
                    "ibkr_fetch": "0 * * * *",  # Default: every hour
                    "finviz_data_fetch": "0 */2 * * *", # Default: every 2 hours
                    "yahoo_data_fetch": "*/15 * * * *", # Default: every 15 minutes (example, adjust as needed)
                    "ibkr_sync_snapshot": "*/10 * * * *", # Default: every 10 minutes
                    "analytics_data_cache_refresh": "0 */6 * * *", # Default: every 6 hours
                    "analytics_metadata_cache_refresh": "0 */6 * * *" # Default: every 6 hours
                }
                job_functions = {
                    "ibkr_fetch": scheduled_fetch_job,
                    "finviz_data_fetch": scheduled_finviz_job,
                    "yahoo_data_fetch": scheduled_yahoo_job,
                    "ibkr_sync_snapshot": run_sync_ibkr_snapshot_job,
                    "analytics_data_cache_refresh": scheduled_analytics_data_cache_refresh_job,
                    "analytics_metadata_cache_refresh": scheduled_analytics_metadata_cache_refresh_job
                }
                job_args = {
                    "ibkr_sync_snapshot": lambda: [app.state.repository, loop, manager, os.environ.get("IBKR_BASE_URL", "https://localhost:5000/v1/api/")],
                    "analytics_data_cache_refresh": lambda: [app, app.state.repository, os.environ.get("APP_BASE_URL", "http://localhost:8000")],
                    "analytics_metadata_cache_refresh": lambda: [app, app.state.repository, os.environ.get("APP_BASE_URL", "http://localhost:8000")]
                }

                for job_id in job_ids_to_configure:
                    job_config_str = await repository.get_job_config_str(job_id) # Assumes this method exists
                    job_config = None
                    cron_expression = None
                    is_active_from_db = True # Default to active

                    if job_config_str:
                        try:
                            job_config = json.loads(job_config_str)
                            if isinstance(job_config, dict) and "cron" in job_config:
                                cron_expression = job_config["cron"]
                                # Also fetch is_active status which should be stored alongside or fetched separately
                                # For now, assuming get_job_is_active works independently or job_config contains it
                                is_active_from_db = await repository.get_job_is_active(job_id, default_active=True)
                            else:
                                logger.warning(f"Job config for '{job_id}' is malformed or missing 'cron' key: {job_config_str}. Using default.")
                        except json.JSONDecodeError:
                                logger.warning(f"Failed to parse job config JSON for '{job_id}': {job_config_str}. Using default.")
                    
                    if not cron_expression: # If config missing, malformed, or cron key not found
                        logger.info(f"No valid cron config found for '{job_id}'. Ensuring default config exists.")
                        cron_expression = default_cron_schedules.get(job_id, "0 0 * * *") # Fallback default
                        default_config_json = json.dumps({"cron": cron_expression})
                        # is_active_from_db is already True by default here.
                        # If creating for the first time, save it with active status.
                        try:
                            await repository.save_job_config({
                                'job_id': job_id,
                                'schedule': default_config_json,
                                # 'is_active' should be handled by save_job_config or a separate update
                                # For now, ensure repository.update_job_active_status is called if creating.
                            })
                            # Explicitly set active status if we are creating the default
                            await repository.update_job_active_status(job_id, True) 
                            is_active_from_db = True # Reflect that it's now active
                            logger.info(f"Saved default cron config for '{job_id}': {default_config_json}")
                        except Exception as e_save:
                            logger.error(f"Failed to save default config for '{job_id}': {e_save}")
                            continue # Skip scheduling this job if default save fails

                    # Validate cron_expression format before adding
                    cron_trigger_instance = None # Initialize
                    try:
                        # CronTrigger.from_crontab(cron_expression) # Basic validation # OLD
                        cron_trigger_instance = CronTrigger.from_crontab(cron_expression) # Store the instance
                    except ValueError as cron_val_err:
                        logger.error(f"Invalid cron expression '{cron_expression}' for job '{job_id}': {cron_val_err}. Using emergency fallback: {default_cron_schedules[job_id]}")
                        cron_expression = default_cron_schedules[job_id] # Fallback to a known good default
                        cron_trigger_instance = CronTrigger.from_crontab(cron_expression) # Create instance with fallback

                    current_job_function = job_functions.get(job_id)
                    if not current_job_function:
                        logger.error(f"No function mapped for job_id '{job_id}'. Skipping.")
                        continue
                    
                    current_job_args_func = job_args.get(job_id)
                    args_to_pass = current_job_args_func() if current_job_args_func else []

                    try:
                        scheduler.add_job(
                            current_job_function,
                            # trigger='cron', # OLD
                            # cron_expression=cron_expression, # OLD
                            trigger=cron_trigger_instance, # NEW: Pass the instance directly
                            id=job_id,
                            replace_existing=True,
                            args=args_to_pass
                        )
                        logger.info(f"Scheduled job '{job_id}' with cron: '{cron_expression}'.")
                        if not is_active_from_db:
                            scheduler.pause_job(job_id)
                            logger.info(f"Job '{job_id}' is inactive in DB. Paused in scheduler.")
                    except Exception as e_add:
                        logger.error(f"Failed to add or pause job '{job_id}' with cron '{cron_expression}': {e_add}", exc_info=True)

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

            # Add at the top with other imports
            import pytz

            # Add this async function near other helpers
            async def catch_up_yahoo_job(repository, scheduled_yahoo_job):
                job_id = 'yahoo_data_fetch'
                job_config = await repository.get_job_config(job_id)
                if not job_config or 'schedule' not in job_config:
                    logger.warning(f"No schedule found for {job_id}, skipping catch-up check.")
                    return
                try:
                    schedule_json = job_config['schedule']
                    if isinstance(schedule_json, str):
                        schedule_json = json.loads(schedule_json)
                    cron_expr = schedule_json.get('cron')
                    if not cron_expr:
                        logger.warning(f"No cron expression found in schedule for {job_id}.")
                        return
                except Exception as e:
                    logger.error(f"Error parsing cron schedule for {job_id}: {e}")
                    return
                last_run = job_config.get('last_run')
                if last_run and isinstance(last_run, str):
                    try:
                        last_run = datetime.fromisoformat(last_run)
                    except Exception:
                        last_run = None
                if last_run is not None and last_run.tzinfo is None:
                    last_run = pytz.timezone('Europe/Athens').localize(last_run)
                cron_fields = cron_expr.split()
                if len(cron_fields) != 5:
                    logger.warning(f"Invalid cron expression for {job_id}: {cron_expr}")
                    return
                cron_kwargs = dict(
                    minute=cron_fields[0],
                    hour=cron_fields[1],
                    day=cron_fields[2],
                    month=cron_fields[3],
                    day_of_week=cron_fields[4],
                    timezone=pytz.timezone('Europe/Athens')
                )
                trigger = CronTrigger(**cron_kwargs)
                now = datetime.now(pytz.timezone('Europe/Athens'))
                prev_run_time = get_prev_fire_time(trigger, now)
                if last_run is None or (prev_run_time and last_run < prev_run_time):
                    logger.info(f"Missed scheduled Yahoo job ({job_id}) at {prev_run_time}. Running catch-up now.")
                    await scheduled_yahoo_job()
                    await repository.update_job_config(job_id, {'last_run': now})
                else:
                    logger.info(f"No catch-up needed for {job_id}. Last run: {last_run}, last scheduled: {prev_run_time}")

            # In startup_event, after scheduler/repository are ready, add:
            # await catch_up_yahoo_job(repository, scheduled_yahoo_job) # OLD WAY
            # asyncio.create_task(catch_up_yahoo_job(repository, scheduled_yahoo_job)) # NEW WAY: Run in background - COMMENTED OUT TO PREVENT STARTUP RUN

            logger.info("Application startup: Initializing Yahoo Mass Fetch job status...")
            try:
                if hasattr(app.state, 'repository') and app.state.repository:
                    repo_instance = app.state.repository
                    await yahoo_job_manager.load_initial_job_state_from_db(repo_instance)
                    logger.info("Initial Yahoo job state loading attempted.")
                    # NEW: Load Finviz job state
                    await finviz_job_manager.load_initial_job_state_from_db(repo_instance)
                    logger.info("Initial Finviz job state loading attempted.")
                else:
                    logger.error("Repository not available in app.state during startup for Yahoo/Finviz job init.")
            except Exception as e_job_load:
                logger.error(f"Error loading initial job states (Yahoo/Finviz): {e_job_load}", exc_info=True)
            # --- End Job State Loading ---
        
            logger.info("Application startup complete.") # Ensure this is logged after all startup tasks attempt

        @app.on_event("shutdown")
        async def shutdown_event():
            """Shutdown the scheduler gracefully."""
            logger.info("Application shutdown: Stopping scheduler.")
            if scheduler.running:
                scheduler.shutdown(wait=False) # MODIFIED: Added wait=False
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
                
                # --- Create a set of screener tickers with status 'portfolio' for efficient lookup ---
                screener_portfolio_tickers_set = {t.get('ticker') for t in screener_tickers_raw if t.get('ticker') and t.get('status') == 'portfolio'}
                logger.info(f"[UI Read Root] Created screener_portfolio_tickers_set with {len(screener_portfolio_tickers_set)} unique tickers (status='portfolio').")
                # --- End Create Set ---

                # --- Create a map of screener.ticker to screener.beta for beta calculation ---
                screener_beta_map = {t.get('ticker'): t.get('beta') for t in screener_tickers_raw if t.get('ticker') and t.get('beta') is not None}
                logger.info(f"[UI Read Root] Created screener_beta_map with {len(screener_beta_map)} entries.")
                # --- End Beta Map ---

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
                        
                        # --- Check if position's name is in the set of screener tickers with status 'portfolio' ---
                        position_name = pos.get('name') # Get the name of the position
                        name_in_tracked_screener_tickers = position_name in screener_portfolio_tickers_set if position_name else False
                        # --- End Check ---
                        
                        # --- Re-calculate Value (EUR) --- 
                        raw_value_eur = 0.0
                        
                        # Determine the correct key for the exchange rate lookup, e.g., "EUR.USD"
                        # pos_currency is 'USD', 'GBP', etc.
                        
                        if pos_currency == 'EUR':
                            raw_value_eur = mkt_value
                        else:
                            rate_key = f"EUR.{pos_currency}"
                            exchange_rate_for_pos = exchange_rates.get(rate_key)
                            
                            if exchange_rate_for_pos is not None:
                                raw_value_eur = mkt_value * exchange_rate_for_pos
                            else:
                                raw_value_eur = mkt_value # Fallback to 1:1 if specific EUR.XXX key not found
                                logger.warning(f"[UI Read Root] Exchange rate for {rate_key} not found in {exchange_rates.keys()}. Using 1:1 for mkt_value {mkt_value} {pos_currency}.")
                        # --- End Value (EUR) --- 
                        
                        # --- Get Beta for calculation ---
                        key_for_beta_lookup = pos.get('name') # positions.name is equivalent to screener.ticker for this lookup
                        beta_from_screener = screener_beta_map.get(key_for_beta_lookup) # This might be None or not a number
                        beta_for_calc = 1.0 # Default beta
                        if isinstance(beta_from_screener, (int, float)):
                            beta_for_calc = float(beta_from_screener)
                        elif beta_from_screener is not None:
                            try:
                                beta_for_calc = float(beta_from_screener) # Try to convert if it's a string representation of a number
                            except ValueError:
                                logger.warning(f"[UI Read Root] Could not convert beta '{beta_from_screener}' to float for '{key_for_beta_lookup}'. Using default 1.0.")
                                beta_for_calc = 1.0 # Fallback to default if conversion fails
                        # --- End Get Beta ---

                        # Convert datetime to string for JSON serialization
                        last_update_str = pos.get('last_update').isoformat() if pos.get('last_update') else None
                        
                        formatted_pos = {
                            **{k: v for k, v in pos.items() if k != 'last_update'}, # Copy other fields
                            'last_update': last_update_str, # Use string version
                            'is_untracked_portfolio_item': not name_in_tracked_screener_tickers, # Update flag based on position.name vs screener.ticker (status portfolio)
                            'mkt_value_eur_raw': raw_value_eur, # Add raw numeric EUR value for JS calculations
                            'beta_for_calc': beta_for_calc, # Add beta for JS calculations
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
                
                # <<< START FIX: Explicitly remove 'currency' if present >>>
                if 'currency' in update_data:
                    logger.debug(f"Removing 'currency' key from update data for account {account_id} before DB update.")
                    del update_data['currency']
                # <<< END FIX >>>

                update_data['upd_mode'] = "user" # Set upd_mode to user

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

        @app.post("/api/accounts/add", response_model=Dict[str, Any], status_code=status.HTTP_201_CREATED)
        async def api_add_account(request: Request, payload: AccountPayload):
            """API endpoint to add a new account."""
            repository: SQLiteRepository = request.app.state.repository
            account_id = payload.account_id
            logger.info(f"Received API request to add account: {account_id}")

            try:
                # Check if account already exists (optional, save_account handles upsert but good for specific error)
                existing_account = await repository.get_account_by_id(account_id)
                if existing_account:
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Account '{account_id}' already exists.")

                account_data = payload.dict()
                if 'currency' in account_data: # Keep the previous fix
                    logger.debug(f"Removing 'currency' key from account_data for account {account_id} before saving.")
                    del account_data['currency']
                
                account_data['upd_mode'] = "user" # Set upd_mode to user

                saved_account = await repository.save_account(account_data)
                
                if saved_account:
                    logger.info(f"Successfully added account {account_id}")
                    return {"message": f"Account {account_id} added successfully.", "account": saved_account}
                else:
                    logger.error(f"Failed to save account {account_id} to database.")
                    raise HTTPException(status_code=500, detail="Error saving account to database.")
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                logger.error(f"Error adding account {account_id}: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Error adding account: {str(e)}")

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
            
            default_crons = {
                "ibkr_fetch": "0 * * * *",
                "ibkr_sync_snapshot": "*/10 * * * *",
                "finviz_data_fetch": "0 3 * * *",
                "yahoo_data_fetch": "0 1 * * *"
            }

            async def _get_cron_from_db_config(job_id: str) -> str:
                job_config_data = await repository.get_job_config(job_id)
                cron_expression = default_crons.get(job_id, "0 0 * * *") # Fallback default
                if job_config_data and 'schedule' in job_config_data and job_config_data['schedule']:
                    try:
                        schedule_details_str = job_config_data['schedule']
                        schedule_details = json.loads(schedule_details_str)
                        if isinstance(schedule_details, dict) and "cron" in schedule_details:
                            cron_expression = schedule_details["cron"]
                        else:
                            logger.warning(f"Cron key not found or schedule is not a dict for {job_id} in DB. Using default: {cron_expression}")
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse schedule JSON for {job_id} from DB: '{job_config_data['schedule']}'. Using default: {cron_expression}")
                    except Exception as e:
                        logger.error(f"Unexpected error parsing schedule for {job_id} from DB: {e}. Using default: {cron_expression}")
                else:
                    logger.info(f"No schedule config found for {job_id} in DB or schedule field is empty. Using default: {cron_expression}")
                return cron_expression

            try:
                # Fetch CRON schedules for all jobs
                ibkr_fetch_schedule_cron = await _get_cron_from_db_config('ibkr_fetch')
                ibkr_snapshot_schedule_cron = await _get_cron_from_db_config('ibkr_sync_snapshot')
                finviz_schedule_cron = await _get_cron_from_db_config('finviz_data_fetch')
                yahoo_schedule_cron = await _get_cron_from_db_config('yahoo_data_fetch')
                analytics_data_cache_schedule_cron = await _get_cron_from_db_config('analytics_data_cache_refresh') # ADDED
                analytics_metadata_cache_schedule_cron = await _get_cron_from_db_config('analytics_metadata_cache_refresh') # ADDED
                                
                # Fetch is_active statuses for all jobs
                ibkr_fetch_is_active = await repository.get_job_is_active('ibkr_fetch')
                ibkr_snapshot_is_active = await repository.get_job_is_active('ibkr_sync_snapshot')
                finviz_is_active = await repository.get_job_is_active('finviz_data_fetch')
                yahoo_is_active = await repository.get_job_is_active('yahoo_data_fetch')
                analytics_data_cache_is_active = await repository.get_job_is_active('analytics_data_cache_refresh') # ADDED
                analytics_metadata_cache_is_active = await repository.get_job_is_active('analytics_metadata_cache_refresh') # ADDED
                                
                # Fetch other necessary data
                exchange_rates = await get_exchange_rates(request.state.db_session) 
                portfolio_rules = await repository.get_all_portfolio_rules()
                portfolio_rules_json = json.dumps(portfolio_rules, default=str)

                # Context strictly for CRON-based UI
                context = {
                    "request": request,
                    "message": message,
                    "message_type": message_type,
                    
                    # CRON Schedules for the template
                    "ibkr_fetch_schedule_cron": ibkr_fetch_schedule_cron,
                    "ibkr_snapshot_schedule_cron": ibkr_snapshot_schedule_cron,
                    "finviz_schedule_cron": finviz_schedule_cron,
                    "yahoo_schedule_cron": yahoo_schedule_cron,
                    "analytics_data_cache_refresh_schedule_cron": analytics_data_cache_schedule_cron, # ADDED
                    "analytics_metadata_cache_refresh_schedule_cron": analytics_metadata_cache_schedule_cron, # ADDED
                    
                    # Active Statuses for the template
                    "ibkr_fetch_is_active": ibkr_fetch_is_active,
                    "ibkr_snapshot_is_active": ibkr_snapshot_is_active,
                    "finviz_is_active": finviz_is_active,
                    "yahoo_is_active": yahoo_is_active,
                    "analytics_data_cache_refresh_is_active": analytics_data_cache_is_active, # ADDED
                    "analytics_metadata_cache_refresh_is_active": analytics_metadata_cache_is_active, # ADDED
                    
                    # Other data (unrelated to scheduler intervals)
                    "exchange_rates": exchange_rates,
                    "portfolio_rules": portfolio_rules,
                    "portfolio_rules_json": portfolio_rules_json
                }
                return request.app.state.templates.TemplateResponse("config.html", context)
            
            except Exception as e:
                logger.error(f"Error loading config page: {e}", exc_info=True)
                # Fallback context in case of errors, using default CRON strings
                return request.app.state.templates.TemplateResponse("config.html", {
                    "request": request, 
                    "message": "Error loading configuration data. Displaying default values.",
                    "message_type": "danger",
                    
                    "ibkr_fetch_schedule_cron": default_crons["ibkr_fetch"],
                    "ibkr_snapshot_schedule_cron": default_crons["ibkr_sync_snapshot"],
                    "finviz_schedule_cron": default_crons["finviz_data_fetch"],
                    "yahoo_schedule_cron": default_crons["yahoo_data_fetch"],
                    "analytics_data_cache_refresh_schedule_cron": default_crons.get("analytics_data_cache_refresh", "0 */6 * * *"), # ADDED
                    "analytics_metadata_cache_refresh_schedule_cron": default_crons.get("analytics_metadata_cache_refresh", "0 */6 * * *"), # ADDED
                    
                    "ibkr_fetch_is_active": True, 
                    "ibkr_snapshot_is_active": True,
                    "finviz_is_active": True,
                    "yahoo_is_active": True,
                    "analytics_data_cache_refresh_is_active": True, # ADDED
                    "analytics_metadata_cache_refresh_is_active": True, # ADDED
                    
                    "exchange_rates": {},
                    "portfolio_rules": [], 
                    "portfolio_rules_json": "[]", 
                    "error": "Failed to load configuration data from the backend."
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
            """Update schedule for a given job_id using a CRON expression."""
            repository: SQLiteRepository = request.app.state.repository
            # Consider making valid_job_ids dynamically fetched or managed elsewhere if it grows
            valid_job_ids = ['yahoo_data_fetch', 'finviz_data_fetch', 'ibkr_fetch', 'ibkr_sync_snapshot', 'analytics_data_cache_refresh', 'analytics_metadata_cache_refresh'] 
            job_id = payload.job_id
            cron_expression = payload.schedule.strip()
            logger.info(f"Received generic schedule update for {job_id}: CRON '{cron_expression}'")

            if job_id not in valid_job_ids:
                raise HTTPException(status_code=400, detail=f"Invalid job_id for generic update: {job_id}")

            if not cron_expression:
                raise HTTPException(status_code=400, detail="Cron schedule string cannot be empty.")

            # Validate cron syntax using APScheduler's CronTrigger for robustness
            try:
                CronTrigger.from_crontab(cron_expression)
                logger.info(f"Cron expression '{cron_expression}' for job '{job_id}' is valid.")
            except ValueError as cron_val_err:
                logger.error(f"Invalid cron expression '{cron_expression}' for job '{job_id}': {cron_val_err}")
                raise HTTPException(status_code=400, detail=f"Invalid cron syntax: {cron_val_err}. Example: '0 */2 * * *' for every 2 hours.")
            
            schedule_json_str = json.dumps({"cron": cron_expression})

            try:
                job_config_data = {
                    'job_id': job_id,
                    'schedule': schedule_json_str,
                    # 'is_active' status is managed by a separate endpoint/logic
                }
                await repository.save_job_config(job_config_data)
                logger.info(f"Generic job config for '{job_id}' updated in DB with JSON: '{schedule_json_str}'")
                
                message = f"Schedule for {job_id} updated to CRON '{cron_expression}' in DB."
                
                # Reschedule the running job
                try:
                    # Access scheduler from app state as it might be more reliable
                    scheduler_instance: AsyncIOScheduler = request.app.state.scheduler
                    if scheduler_instance and scheduler_instance.get_job(job_id):
                        cron_trigger_instance = CronTrigger.from_crontab(cron_expression) # Create instance
                        scheduler_instance.reschedule_job(
                                job_id,
                            # trigger='cron', # OLD
                            # cron_expression=cron_expression # OLD
                            trigger=cron_trigger_instance # NEW: Pass the instance
                        )
                        logger.info(f"Successfully rescheduled job '{job_id}' with new cron: '{cron_expression}'")
                        message += " Job has been rescheduled."
                    elif scheduler_instance:
                        logger.warning(f"Job '{job_id}' not found in running scheduler. Database updated, but job not rescheduled (was not running or was removed).")
                        message += " Job not found in scheduler, so not rescheduled (was not running or was removed)."
                    else:
                        logger.warning("Scheduler instance not found. Database updated, but job not rescheduled.")
                        message += " Scheduler instance not available; job not rescheduled."
                    
                except Exception as schedule_err:
                    logger.error(f"Error rescheduling job '{job_id}': {schedule_err}", exc_info=True)
                    # Keep the original DB update message, but add reschedule error
                    message += f" Failed to reschedule running job: {schedule_err}"
                    # Optionally, re-raise or return a different status if reschedule failure is critical
                
                return {"message": message, "job_id": job_id, "cron_schedule": cron_expression}
            except Exception as e:
                logger.error(f"Error saving generic schedule for '{job_id}': {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Error saving schedule for {job_id}: {str(e)}")

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
                # --- Create a set of unique names from live positions for efficient lookup ---
                live_position_names = {p.get('name') for p in positions_raw if p.get('name')}
                logger.info(f"[Tracker] Created set of {len(live_position_names)} unique names from live positions.")
                # --- End set creation ---
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
                    # --- Check if the screener item's ticker is in live position names ---
                    screener_item_ticker = item.get('ticker')
                    if screener_item_ticker and screener_item_ticker in live_position_names:
                        item['in_portfolio'] = True
                        # --- If screener.ticker is in positions.name, set screener.status to portfolio for display AND DB ---
                        current_db_status = item.get('status') # Get status as it was from DB
                        if current_db_status != 'portfolio':
                            try:
                                await repository.update_screener_ticker_details(screener_item_ticker, "status", "portfolio")
                                logger.info(f"[Tracker] DB UPDATE: Status for screener ticker '{screener_item_ticker}' updated to 'portfolio'.")
                            except Exception as db_update_err:
                                logger.error(f"[Tracker] DB UPDATE FAILED: Could not update status for screener ticker '{screener_item_ticker}' to 'portfolio'. Error: {db_update_err}")
                        # Always update the item for display, even if DB update was just attempted or status was already correct
                        item['status'] = 'portfolio' 
                        logger.debug(f"[Tracker] Display status set to 'portfolio' for screener ticker {screener_item_ticker} because it was found in live position names.")
                    # --- End name check ---

                    item['acc_from_live'] = False
                    item['open_pos_from_live'] = False
                    item['cost_base_from_live'] = False
                    item['currency_from_live'] = False
                    item['stop_from_order'] = False
                    item['trail_from_order'] = False
                    item['lmt_ofst_from_order'] = False
                    # Find matching live position to enrich screener data (Acc, Open Pos, Cost Base, Currency)
                    # Match screener.ticker with position.name
                    screener_item_ticker_for_enrichment = item.get('ticker') 
                    matching_pos = next((p for p in positions_raw if screener_item_ticker_for_enrichment and p.get('name') == screener_item_ticker_for_enrichment), None)
                    
                    if matching_pos:
                         # item['in_portfolio'] = True # This line is now handled by the name check above
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
                    # usd_rate = exchange_rates_data.get('USD', 1.0) # OLD - REMOVE THIS LINE
                    # gbp_rate = exchange_rates_data.get('GBP', 1.0) # OLD - REMOVE THIS LINE
                    
                    if pos_currency == 'EUR':
                        raw_value_eur = mkt_value
                    else:
                        rate_key = f"EUR.{pos_currency}" # NEW: Construct the correct key
                        exchange_rate_for_pos = exchange_rates_data.get(rate_key) # NEW: Use the key
                        
                        if exchange_rate_for_pos is not None:
                            raw_value_eur = mkt_value * exchange_rate_for_pos
                        else:
                            raw_value_eur = mkt_value # Fallback to 1:1 if specific EUR.XXX key not found
                            logger.warning(f"[Tracker] Exchange rate for {rate_key} not found in {exchange_rates_data.keys()}. Using 1:1 for mkt_value {mkt_value} {pos_currency}.")
                    # --- End Value (EUR) calculation logic ---

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
            identifier_from_query = request.query_params.get("identifier")
            from_pill_flag = request.query_params.get("from_positions_pill", "false").lower() == "true"
            context = {
                "request": request, 
                "identifier_from_query": identifier_from_query,
                "set_status_to_portfolio": from_pill_flag
            }
            return request.app.state.templates.TemplateResponse(
                "add_ticker.html", context
            )
        # --- End Add Ticker Page Route ---

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
                last_status = None # Initialize last_status here
                try:
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
        
        # --- NEW Utilities Page Route ---
        @app.get("/utilities", response_class=HTMLResponse, name="get_utilities_page")
        async def get_utilities_page(request: Request):
            """
            Serves the utilities page.
            """
            logger.info("Rendering utilities page")
            # Basic context, can add more if needed
            context = {"request": request}
            return request.app.state.templates.TemplateResponse(
                "utilities.html", context
            )
        # --- END Utilities Page Route ---

        # ------ Helper for Sync Service Calls -------
        # (Add this if needed for other sync tasks)

        # --- Include Routers ---
        app.include_router(backend_router) # Existing router
        app.include_router(utilities_router.router) # Add utilities router
        app.include_router(edgar_router.router) # <<< ADD THIS LINE
        logger.info("Included backend_router and utilities_router.")
        # --- End Include Routers ---

        # Include the new Yahoo Job API router
        app.include_router(yahoo_job_api_router)
        logger.info("Included Yahoo Job API router.")

        # --- Mount Static Files (Corrected Placement) ---
        # Ensure static files are mounted correctly relative to the execution path or package structure.
        # If V3_web.py is in src/V3_app, and static is src/V3_app/static:
        static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
        app.mount("/static", StaticFiles(directory=static_dir), name="static")
        # --- End Mount Static Files ---

        # --- Include Routers ---
        app.include_router(utilities_router.router) # Include the utilities router
        app.include_router(edgar_router.router) # <<< ADD THIS LINE
        app.include_router(yahoo_job_api_router, prefix="/api/v3/jobs", tags=["Jobs - Yahoo"]) # Existing Yahoo job router
        app.include_router(finviz_job_api_router, prefix="/api/v3/jobs", tags=["Jobs - Finviz"]) # NEW: Include Finviz job router
        app.include_router(notification_routes.router, prefix="/api/notifications", tags=["Notifications"]) # Added notification router
        # --- End Include Routers ---

        logger.info("FastAPI app instance created and configured successfully with Yahoo Job module.")
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

# Add this helper function near the catch_up_yahoo_job definition
from datetime import timedelta

def get_prev_fire_time(trigger, now, lookback_days=14):
    # Start from now - lookback_days, walk forward to now
    start = now - timedelta(days=lookback_days)
    fire_time = trigger.get_next_fire_time(None, start)
    prev = None
    while fire_time and fire_time < now:
        prev = fire_time
        fire_time = trigger.get_next_fire_time(fire_time, now)
    return prev

# In catch_up_yahoo_job, replace:
# prev_run_time = trigger.get_prev_fire_time(None, now)
# with:
# prev_run_time = get_prev_fire_time(trigger, now)

# --- NEW Analytics Data Cache Refresh Scheduled Job ---
async def scheduled_analytics_data_cache_refresh_job(app: FastAPI, repository: SQLiteRepository, app_base_url: str):
    job_id = "analytics_data_cache_refresh"
    logger.info(f"Scheduler executing {job_id}...")

    try:
        is_active = await repository.get_job_is_active(job_id)
        if not is_active:
            logger.info(f"Job '{job_id}' is inactive in DB. Skipping execution.")
            return
    except Exception as check_err:
        logger.error(f"Error checking active status for {job_id}: {check_err}. Skipping execution.", exc_info=True)
        return

    job_lock: asyncio.Lock = app.state.job_execution_lock # Assuming a global lock for simplicity
    if job_lock.locked():
        logger.warning(f"Skipping {job_id}: Another critical job might be running or lock improperly held.")
        return

    await job_lock.acquire()
    try:
        # Ensure app_base_url is sensible, e.g., from app.state if available and correctly set during startup
        # For now, assuming app_base_url is passed correctly by the scheduler setup.
        api_url = f"{app_base_url}/api/analytics/cache/refresh_data"
        logger.info(f"Job '{job_id}' is active. Triggering POST request to {api_url}")
        async with httpx.AsyncClient(timeout=300.0) as client: # 5 min timeout for the request
            response = await client.post(api_url)
        
        if response.status_code == status.HTTP_202_ACCEPTED:
            logger.info(f"Successfully triggered {job_id}. API Response: {response.status_code}")
        else:
            logger.error(f"Failed to trigger {job_id}. API Response: {response.status_code} - {response.text}")
        
        # Update last_run time in DB
        now = datetime.now()
        await repository.update_job_config(job_id, {'last_run': now})
        logger.info(f"Updated last_run for job '{job_id}' to {now}")

    except httpx.RequestError as req_err:
        logger.error(f"HTTP RequestError during {job_id} execution: {req_err}", exc_info=True)
    except Exception as e:
        logger.error(f"Error during scheduled {job_id} execution: {e}", exc_info=True)
    finally:
        if job_lock.locked(): # Check if this instance of the job actually acquired the lock
            job_lock.release()
        logger.debug(f"Job lock released by {job_id}")

# --- NEW Analytics Metadata Cache Refresh Scheduled Job ---
async def scheduled_analytics_metadata_cache_refresh_job(app: FastAPI, repository: SQLiteRepository, app_base_url: str):
    job_id = "analytics_metadata_cache_refresh"
    logger.info(f"Scheduler executing {job_id}...")

    try:
        is_active = await repository.get_job_is_active(job_id)
        if not is_active:
            logger.info(f"Job '{job_id}' is inactive in DB. Skipping execution.")
            return
    except Exception as check_err:
        logger.error(f"Error checking active status for {job_id}: {check_err}. Skipping execution.", exc_info=True)
        return

    job_lock: asyncio.Lock = app.state.job_execution_lock # Assuming a global lock
    if job_lock.locked():
        logger.warning(f"Skipping {job_id}: Another critical job might be running or lock improperly held.")
        return
    
    await job_lock.acquire()
    try:
        api_url = f"{app_base_url}/api/analytics/cache/refresh_metadata"
        logger.info(f"Job '{job_id}' is active. Triggering POST request to {api_url}")
        async with httpx.AsyncClient(timeout=300.0) as client: # 5 min timeout
            response = await client.post(api_url)

        if response.status_code == status.HTTP_202_ACCEPTED:
            logger.info(f"Successfully triggered {job_id}. API Response: {response.status_code}")
        else:
            logger.error(f"Failed to trigger {job_id}. API Response: {response.status_code} - {response.text}")

        # Update last_run time in DB
        now = datetime.now()
        await repository.update_job_config(job_id, {'last_run': now})
        logger.info(f"Updated last_run for job '{job_id}' to {now}")

    except httpx.RequestError as req_err:
        logger.error(f"HTTP RequestError during {job_id} execution: {req_err}", exc_info=True)
    except Exception as e:
        logger.error(f"Error during scheduled {job_id} execution: {e}", exc_info=True)
    finally:
        if job_lock.locked():
            job_lock.release()
        logger.debug(f"Job lock released by {job_id}")


        
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

# Add this helper function near the catch_up_yahoo_job definition
from datetime import timedelta

def get_prev_fire_time(trigger, now, lookback_days=14):
    # Start from now - lookback_days, walk forward to now
    start = now - timedelta(days=lookback_days)
    fire_time = trigger.get_next_fire_time(None, start)
    prev = None
    while fire_time and fire_time < now:
        prev = fire_time
        fire_time = trigger.get_next_fire_time(fire_time, now)
    return prev

# In catch_up_yahoo_job, replace:
# prev_run_time = trigger.get_prev_fire_time(None, now)
# with:
# prev_run_time = get_prev_fire_time(trigger, now)

