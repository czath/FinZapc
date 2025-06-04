from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from .V3_database import ScreenerModel, PositionModel, SQLiteRepository
from .V3_web import get_db
from pydantic import BaseModel, Field
import uuid
import asyncio
from typing import List, Dict, Any, Optional, Union
import logging
from datetime import datetime
import json

# Import for ProcessPoolExecutor
from concurrent.futures import ProcessPoolExecutor

# Import your Yahoo fetch logic (adjust import as needed)
from .V3_yahoo_fetch import mass_load_yahoo_data_from_file, YahooDataRepository, fetch_daily_historical_data
from .yahoo_data_query_srv import YahooDataQueryService
from .analytics_data_processor import AnalyticsDataProcessor
from .yahoo_data_query_adv import YahooDataQueryAdvService
from .yahoo_data_query_pro import YahooDataQueryProService

router = APIRouter()

# --- Progress tracking ---
yahoo_job_progress: Dict[str, Dict[str, Any]] = {}
yahoo_job_lock = asyncio.Lock()

# --- Logger ---
logger = logging.getLogger(__name__)

# --- NEW: Pydantic model for Fundamentals History Request ---
class FundamentalsHistoryRequest(BaseModel):
    tickers: List[str]
    field_identifiers: List[str] # e.g., ["yf_item_balance_sheet_annual_Total Assets", "yf_item_income_statement_annual_Total Revenue"]
    start_date: Optional[str] = None # YYYY-MM-DD
    end_date: Optional[str] = None   # YYYY-MM-DD
    include_projections: Optional[bool] = False # NEW: Flag to include projections, defaults to False

# --- NEW: Allowed values for period and interval ---
VALID_PERIODS = ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"]
VALID_INTERVALS = ["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"]

class YahooTickerListPayload(BaseModel):
    tickers: List[str]

@router.post('/api/yahoo/mass_fetch', summary='Start Yahoo Mass Fetch for tickers')
async def start_yahoo_mass_fetch(
    payload: YahooTickerListPayload,
    background_tasks: BackgroundTasks,
    request: Request
):
    tickers = payload.tickers
    if not tickers:
        raise HTTPException(status_code=400, detail='No tickers provided')
    job_id = str(uuid.uuid4())
    async with yahoo_job_lock:
        yahoo_job_progress[job_id] = {
            'current': 0,
            'total': len(tickers),
            'status': 'running',
            'last_ticker': None,
            'errors': []
        }
    # Get YahooDataRepository instance from app state or create new
    db_repo = YahooDataRepository(request.app.state.repository.database_url)
    background_tasks.add_task(run_yahoo_mass_fetch, db_repo, tickers, job_id)
    return {'job_id': job_id}

async def run_yahoo_mass_fetch(db_repo, tickers, job_id):
    async def progress_callback(current, total, last_ticker, error=None):
        async with yahoo_job_lock:
            yahoo_job_progress[job_id]['current'] = current
            yahoo_job_progress[job_id]['total'] = total
            yahoo_job_progress[job_id]['last_ticker'] = last_ticker
            if error:
                yahoo_job_progress[job_id]['errors'].append(error)
    try:
        result = await mass_load_yahoo_data_from_file(tickers, db_repo, progress_callback=progress_callback)
        errors = result.get('errors', [])
        success_count = result.get('success_count', 0)
        error_count = result.get('error_count', 0)
        async with yahoo_job_lock:
            yahoo_job_progress[job_id]['success_count'] = success_count
            yahoo_job_progress[job_id]['error_count'] = error_count
            yahoo_job_progress[job_id]['errors'] = errors
            if error_count > 0 and success_count == 0:
                yahoo_job_progress[job_id]['status'] = 'failed'
            elif error_count > 0:
                yahoo_job_progress[job_id]['status'] = 'partial_failure'
            else:
                yahoo_job_progress[job_id]['status'] = 'completed'
    except Exception as e:
        async with yahoo_job_lock:
            yahoo_job_progress[job_id]['status'] = 'failed'
            yahoo_job_progress[job_id]['errors'].append({'error': str(e)})

@router.get('/api/yahoo/mass_fetch/status/{job_id}', summary='Get Yahoo Mass Fetch progress')
async def get_yahoo_mass_fetch_status(job_id: str):
    async with yahoo_job_lock:
        progress = yahoo_job_progress.get(job_id)
        if not progress:
            raise HTTPException(status_code=404, detail='Job not found')
        return progress

@router.get('/api/screener/tickers', summary='Get all unique tickers from the screener table')
async def get_screener_tickers(db: AsyncSession = Depends(get_db)):
    """Return a list of unique tickers from the screener table."""
    try:
        result = await db.execute(select(ScreenerModel.ticker).distinct())
        tickers = [row[0] for row in result.fetchall() if row[0]]
        return JSONResponse(content=tickers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching screener tickers: {e}")

@router.get('/api/portfolio/tickers', summary='Get all unique tickers from the positions table')
async def get_portfolio_tickers(db: AsyncSession = Depends(get_db)):
    """Return a list of unique tickers from the positions table."""
    try:
        result = await db.execute(select(PositionModel.ticker).distinct())
        tickers = [row[0] for row in result.fetchall() if row[0]]
        return JSONResponse(content=tickers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching portfolio tickers: {e}")

@router.get('/api/yahoo/master_tickers', summary='Get all tickers from Yahoo master ticker table')
async def get_yahoo_master_tickers(request: Request):
    repo = YahooDataRepository(request.app.state.repository.database_url)
    tickers = await repo.get_all_master_tickers()
    return JSONResponse(content=tickers)

# --- Dependency Provider (Example - Adapt to your setup) ---
# You need a way to provide instances of your repository and service.
# This is a placeholder. Replace with your actual dependency setup.
async def get_yahoo_repository(request: Request) -> YahooDataRepository:
    # If repository needs the raw db_url string:
    if not hasattr(request.app.state, 'repository') or not hasattr(request.app.state.repository, 'database_url'):
        logger.error("CRITICAL: SQLiteRepository or its database_url not found in application state!")
        raise HTTPException(status_code=500, detail="Internal server error: YahooDataRepository cannot be initialized.")
    db_url = request.app.state.repository.database_url
    # Or if it just needs the session factory:
    # return YahooDataRepository(db.bind) # Adjust based on YahooDataRepository constructor
    return YahooDataRepository(database_url=db_url)

async def get_sqlite_repository(request: Request) -> SQLiteRepository: # <-- NEW Dependency Provider
    """Provides an instance of SQLiteRepository."""
    if not hasattr(request.app.state, 'repository') or not hasattr(request.app.state.repository, 'database_url'):
        logger.error("CRITICAL: SQLiteRepository or its database_url not found in application state!")
        raise HTTPException(status_code=500, detail="Internal server error: SQLiteRepository cannot be initialized.")
    db_url = request.app.state.repository.database_url
    return SQLiteRepository(database_url=db_url)

async def get_yahoo_query_service(
    db_repo: YahooDataRepository = Depends(get_yahoo_repository)
) -> YahooDataQueryService:
    return YahooDataQueryService(db_repo=db_repo)

async def get_yahoo_data_query_pro_service(
    db_repo: YahooDataRepository = Depends(get_yahoo_repository),
    base_query_srv: YahooDataQueryService = Depends(get_yahoo_query_service)
) -> YahooDataQueryProService:
    """FastAPI dependency provider for YahooDataQueryProService.""" 
    logger.debug("Dependency: YahooDataQueryProService requested.")
    service_instance = YahooDataQueryProService(db_repo=db_repo, base_query_srv=base_query_srv)
    logger.debug(f"YahooDataQueryProService instance created: {service_instance}")
    return service_instance

# --- Target Item Types ---
TARGET_ITEM_TYPES = [
    # Type (query string, lowercase), Coverage (actual in DB), Key in output dict
    ('analyst_price_targets', "CUMULATIVE_SNAPSHOT", 'analyst_price_targets'),
    ('forecast_summary', "CUMULATIVE", 'forecast_summary'),
    ('balance_sheet', "FYEAR", 'balance_sheet_annual'),
    ('income_statement', "FYEAR", 'income_statement_annual'),
    ('cash_flow_statement', "FYEAR", 'cash_flow_annual'), # Changed 'cash_flow' to 'cash_flow_statement'
    ('balance_sheet', "QUARTER", 'balance_sheet_quarterly'),
    ('income_statement', "QUARTER", 'income_statement_quarterly'),
    ('cash_flow_statement', "QUARTER", 'cash_flow_quarterly'), # Changed 'cash_flow' to 'cash_flow_statement'
    ('income_statement', "TTM", 'income_statement_ttm'),
    ('cash_flow_statement', "TTM", 'cash_flow_ttm'), # Changed 'cash_flow' to 'cash_flow_statement'
]

# --- New API Route ---
@router.get('/api/analytics/data/yahoo_combined',
            summary='Get Combined Yahoo Master and Item Data for Analytics',
            response_model=List[Dict[str, Any]],
            tags=["Analytics Data"])
async def get_analytics_yahoo_combined_data(
    db_repo: YahooDataRepository = Depends(get_yahoo_repository),
    query_service: YahooDataQueryService = Depends(get_yahoo_query_service)
):
    """
    Fetches combined data from Yahoo master table and latest relevant item payloads
    for all tickers, optimized for the analytics page.
    """
    combined_data_list = []
    try:
        # 1. Get all tickers from master table first
        master_tickers = await db_repo.get_all_master_tickers()
        if not master_tickers:
            logger.warning("No tickers found in Yahoo master table.")
            return []
            
        logger.info(f"Found {len(master_tickers)} tickers in master table. Fetching combined data...")

        # 2. Get all relevant master data in one go
        all_master_data_list = await db_repo.get_master_data_for_analytics()
        all_master_data_map = {item['ticker']: item for item in all_master_data_list}

        # 3. Fetch item data for each ticker (can be parallelized)
        async def fetch_ticker_combined_data(ticker):
            master_data = all_master_data_map.get(ticker, {"ticker": ticker}) # Use fetched data or default
            financial_items = {}
            
            item_fetch_tasks = []
            # Create tasks for fetching each target item type payload
            for item_type, item_coverage, output_key in TARGET_ITEM_TYPES:
                 item_fetch_tasks.append(
                     asyncio.create_task(
                         query_service.get_latest_data_item_payload(ticker, item_type, item_coverage),
                         name=f"{ticker}-{output_key}" # Add name for easier debugging
                     )
                 )
            
            # Await all item fetch tasks for this ticker
            item_results = await asyncio.gather(*item_fetch_tasks, return_exceptions=True)

            # Process results
            for i, result in enumerate(item_results):
                item_type, item_coverage, output_key = TARGET_ITEM_TYPES[i]
                if isinstance(result, Exception):
                     logger.warning(f"Failed to fetch item {output_key} for ticker {ticker}: {result}")
                elif result is not None:
                    financial_items[output_key] = result
                # else: result is None (no data found), do nothing

            return {
                "ticker": ticker,
                "master_data": master_data,
                "financial_items": financial_items
            }

        # 4. Run fetches for all tickers concurrently
        fetch_all_tickers_tasks = [fetch_ticker_combined_data(ticker) for ticker in master_tickers]
        combined_data_list = await asyncio.gather(*fetch_all_tickers_tasks, return_exceptions=True)

        # Filter out potential exceptions from gather results (though fetch_ticker_combined_data handles internal ones)
        successful_results = [res for res in combined_data_list if not isinstance(res, Exception)]
        
        # Log errors if any top-level task failed
        for i, res in enumerate(combined_data_list):
            if isinstance(res, Exception):
                 logger.error(f"Error fetching combined data for ticker {master_tickers[i]}: {res}", exc_info=res)


        logger.info(f"Successfully fetched combined data for {len(successful_results)} tickers.")
        return successful_results

    except Exception as e:
        logger.error(f"Error in get_analytics_yahoo_combined_data endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error fetching combined Yahoo data")

# --- NEW API Route for AnalyticsDataProcessor ---
@router.get("/api/v3/analytics/processed_data",
            summary="Process and retrieve analytics data (Finviz, Yahoo, or Both) - NOW SERVES FROM CACHE",
            response_model=Dict[str, Any], 
            tags=["Analytics Data V3"])
async def get_processed_analytics_data(
    data_source_selection: str, # Query param: "finviz_only", "yahoo_only", "both" - NOW IGNORED by new cache logic
    request: Request, 
    sqlite_repo: SQLiteRepository = Depends(get_sqlite_repository) 
):
    logger.info(f"API: Request for /api/v3/analytics/processed_data. CACHE IMPLEMENTATION. data_source_selection '{data_source_selection}' is now ignored.")

    try:
        data_json_tuple = await sqlite_repo.get_cached_analytics_data()
        metadata_json_tuple = await sqlite_repo.get_cached_analytics_metadata()

        if data_json_tuple and metadata_json_tuple:
            data_json, data_generated_at = data_json_tuple
            metadata_json, metadata_generated_at = metadata_json_tuple

            logger.info(f"API: Serving analytics data from cache. Data generated at: {data_generated_at}, Metadata generated at: {metadata_generated_at}")
            
            try:
                cached_data = json.loads(data_json)
                cached_metadata = json.loads(metadata_json)
            except json.JSONDecodeError as e_json:
                logger.error(f"API: JSONDecodeError when loading analytics from cache: {e_json}", exc_info=True)
                raise HTTPException(status_code=500, detail="Error decoding cached analytics data.")

            return {
                "originalData": cached_data, 
                "metaData": {"field_metadata": cached_metadata},
                "message": "Served from cache.",
                "data_cached_at": data_generated_at.isoformat() if data_generated_at else None,
                "metadata_cached_at": metadata_generated_at.isoformat() if metadata_generated_at else None
            }
        else:
            missing_parts = []
            if not data_json_tuple: missing_parts.append("data")
            if not metadata_json_tuple: missing_parts.append("metadata")
            logger.warning(f"API: Analytics cache miss for: {', '.join(missing_parts)}. Please trigger a refresh if this persists.")
            return {
                "originalData": [], 
                "metaData": {},
                "message": f"Analytics cache ({', '.join(missing_parts)}) is not yet available or is being generated. Please try again later or trigger a manual refresh via config page.",
                "cache_status": "unavailable"
            }

    except Exception as e:
        logger.error(f"API: Error in get_processed_analytics_data while attempting to serve from cache: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error accessing analytics cache: {str(e)}")

    # === OLD ON-THE-FLY GENERATION LOGIC (COMMENTED OUT - Superseded by Caching Mechanism) ===
    # logger.info(f"Received request for /api/v3/analytics/processed_data with selection: {data_source_selection}")
    #
    # valid_selections = ["finviz_only", "yahoo_only", "both"]
    # if data_source_selection not in valid_selections:
    #     raise HTTPException(
    #         status_code=400, 
    #         detail=f"Invalid data_source_selection. Must be one of {valid_selections}"
    #     )
    #
    # processor = None  # Initialize processor to None for the finally block
    # try:
    #     # Initialize the AnalyticsDataProcessor with the repository
    #     processor = AnalyticsDataProcessor(db_repository=sqlite_repo) 
    #
    #     # For debugging:
    #     raw_result = await processor.process_data_for_analytics(data_source_selection)
    #     logger.info(f"Raw result from ADP: type={type(raw_result)}, value={str(raw_result)[:1000]}") # Log type and part of value, increased length
    #     if isinstance(raw_result, tuple) or isinstance(raw_result, list):
    #         logger.info(f"Length of raw_result: {len(raw_result)}")
    #     else:
    #         logger.warning(f"Raw result from ADP is not a tuple or list, it's a {type(raw_result)}")
    #
    #     data, metadata = raw_result # Unpack after logging
    #     
    #     logger.info(f"Successfully processed data for selection '{data_source_selection}'. Returning {len(data)} records.")
    #     return {"originalData": data, "metaData": metadata}
    #
    # except HTTPException as http_exc: # Re-raise HTTPExceptions directly
    #     logger.warning(f"HTTPException during analytics processing for '{data_source_selection}': {http_exc.detail}")
    #     raise http_exc
    # except Exception as e:
    #     logger.error(f"Error in get_processed_analytics_data endpoint for selection '{data_source_selection}': {e}", exc_info=True)
    #     raise HTTPException(status_code=500, detail=f"Internal server error processing analytics data: {str(e)}")
    # finally:
    #     if processor and hasattr(processor, 'close_http_client'):
    #         try:
    #             await processor.close_http_client()
    #             logger.info("ADP HTTP client closed successfully after analytics processing.")
    #         except Exception as e_close:
    #             logger.error(f"Error closing ADP HTTP client: {e_close}", exc_info=True)
    # === END OF OLD ON-THE-FLY GENERATION LOGIC ===

# --- NEW: Analytics Cache Refresh Endpoints ---

# --- Top-level worker function for ProcessPoolExecutor (Analytics Data Cache) ---
def process_analytics_data_cache_in_process(db_url: str) -> bool:
    # This function runs in a SEPARATE PROCESS
    try:
        # Re-initialize logger for the new process if necessary, or configure root logger
        # For simplicity, assuming top-level logger config applies or is inherited.
        process_logger = logging.getLogger(__name__ + ".ProcessPoolWorker_Data")
        process_logger.info(f"ProcessPoolWorker (DataCache): Initializing repository with DB URL: {db_url}")
        
        # Ensure SQLiteRepository can be initialized just with db_url
        temp_sqlite_repo = SQLiteRepository(database_url=db_url)
        
        process_logger.info("ProcessPoolWorker (DataCache): Initializing AnalyticsDataProcessor.")
        processor = AnalyticsDataProcessor(db_repository=temp_sqlite_repo)
        
        process_logger.info("ProcessPoolWorker (DataCache): Calling force_refresh_data_cache.")
        
        async def do_refresh():
            # No progress callback passed from here as it's hard to share across processes directly
            await processor.force_refresh_data_cache(progress_callback=None) 
            await processor.close_http_client()

        asyncio.run(do_refresh()) # Runs the async function in a new event loop in this process
        process_logger.info("ProcessPoolWorker (DataCache): force_refresh_data_cache completed.")
        return True
    except Exception as e:
        # Use a logger specific to this worker context if defined, or the main one
        logger_to_use = logging.getLogger(__name__ + ".ProcessPoolWorker_Data") if logging.getLogger(__name__ + ".ProcessPoolWorker_Data").hasHandlers() else logger
        logger_to_use.error(f"ProcessPoolWorker (DataCache): Error during data cache refresh: {e}", exc_info=True)
        return False

# --- Top-level worker function for ProcessPoolExecutor (Analytics Metadata Cache) ---
def process_analytics_metadata_cache_in_process(db_url: str) -> bool:
    # This function runs in a SEPARATE PROCESS
    try:
        process_logger = logging.getLogger(__name__ + ".ProcessPoolWorker_Metadata")
        process_logger.info(f"ProcessPoolWorker (MetadataCache): Initializing repository with DB URL: {db_url}")
        
        temp_sqlite_repo = SQLiteRepository(database_url=db_url)
        
        process_logger.info("ProcessPoolWorker (MetadataCache): Initializing AnalyticsDataProcessor.")
        processor = AnalyticsDataProcessor(db_repository=temp_sqlite_repo)
        
        process_logger.info("ProcessPoolWorker (MetadataCache): Calling force_refresh_metadata_cache.")
        
        async def do_refresh():
            await processor.force_refresh_metadata_cache(progress_callback=None)
            await processor.close_http_client()

        asyncio.run(do_refresh())
        process_logger.info("ProcessPoolWorker (MetadataCache): force_refresh_metadata_cache completed.")
        return True
    except Exception as e:
        logger_to_use = logging.getLogger(__name__ + ".ProcessPoolWorker_Metadata") if logging.getLogger(__name__ + ".ProcessPoolWorker_Metadata").hasHandlers() else logger
        logger_to_use.error(f"ProcessPoolWorker (MetadataCache): Error during metadata cache refresh: {e}", exc_info=True)
        return False


# Global ProcessPoolExecutor (initialize once, or manage its lifecycle with app startup/shutdown)
# For simplicity here, we'll create it on demand, but this is not ideal for production.
# A better approach is to have one executor instance managed by the app's lifespan.
# process_pool_executor = ProcessPoolExecutor(max_workers=1) # Example: Max 1 worker to serialize these heavy tasks

async def submit_to_process_pool(worker_func, db_url: str, loop: asyncio.AbstractEventLoop):
    logger.info(f"Submitting task ({worker_func.__name__}) to process pool with DB URL: {db_url}")
    try:
        # Using a context manager for the executor ensures it's shutdown if created on demand.
        # If using a global executor, don't use 'with' here.
        with ProcessPoolExecutor(max_workers=1) as executor:
            future = executor.submit(worker_func, db_url)
            # Run the blocking future.result() in a thread to not block the main asyncio event loop
            success = await loop.run_in_executor(None, future.result) 
        
        if success:
            logger.info(f"Process pool task ({worker_func.__name__}) completed successfully.")
        else:
            logger.warning(f"Process pool task ({worker_func.__name__}) encountered an error or returned False.")
    except Exception as e:
        logger.error(f"Error submitting/executing task in process pool ({worker_func.__name__}): {e}", exc_info=True)

@router.post("/api/analytics/cache/refresh_data",
             summary="Manually trigger a refresh of the analytics data cache (via Process Pool)",
             status_code=status.HTTP_202_ACCEPTED,
             tags=["Analytics Data V3", "Cache Management"])
async def trigger_analytics_data_cache_refresh(
    # background_tasks: BackgroundTasks, # No longer using FastAPI's BackgroundTasks for this
    request: Request, 
    sqlite_repo: SQLiteRepository = Depends(get_sqlite_repository) # Keep for getting db_url
):
    logger.info("API: Received request to manually refresh analytics data cache (using Process Pool).")
    try:
        db_url = sqlite_repo.database_url
        app_loop = asyncio.get_running_loop()

        # Create an asyncio task that will manage the ProcessPoolExecutor interaction
        asyncio.create_task(submit_to_process_pool(process_analytics_data_cache_in_process, db_url, app_loop))
        
        return {"message": "Analytics data cache refresh submitted to background process. Check logs for completion."}
    except Exception as e:
        logger.error(f"API: Error initiating data cache refresh (Process Pool): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to initiate analytics data cache refresh.")

# OLD _run_metadata_cache_refresh (REMOVE or COMMENT OUT)
# async def _run_metadata_cache_refresh(processor: AnalyticsDataProcessor):
#     """Helper async function to run metadata cache refresh and ensure client is closed."""
#     try:
#         logger.info("Background task started: Force refreshing analytics metadata cache.")
#         await processor.force_refresh_metadata_cache()
#         logger.info("Background task completed: Analytics metadata cache refresh finished.")
#     except Exception as e:
#         logger.error(f"Background task error during metadata cache refresh: {e}", exc_info=True)
#     finally:
#         if hasattr(processor, 'close_http_client'):
#             try:
#                 await processor.close_http_client()
#                 logger.info("ADP HTTP client closed successfully after metadata cache refresh task.")
#             except Exception as e_close:
#                 logger.error(f"Error closing ADP HTTP client after metadata cache refresh task: {e_close}", exc_info=True)

@router.post("/api/analytics/cache/refresh_metadata",
             summary="Manually trigger a refresh of the analytics metadata cache (via Process Pool)",
             status_code=status.HTTP_202_ACCEPTED,
             tags=["Analytics Data V3", "Cache Management"])
async def trigger_analytics_metadata_cache_refresh(
    # background_tasks: BackgroundTasks, # No longer using FastAPI's BackgroundTasks for this
    request: Request,
    sqlite_repo: SQLiteRepository = Depends(get_sqlite_repository) # Keep for getting db_url
):
    logger.info("API: Received request to manually refresh analytics metadata cache (using Process Pool).")
    try:
        db_url = sqlite_repo.database_url
        app_loop = asyncio.get_running_loop()

        asyncio.create_task(submit_to_process_pool(process_analytics_metadata_cache_in_process, db_url, app_loop))
        
        return {"message": "Analytics metadata cache refresh submitted to background process. Check logs for completion."}
    except Exception as e:
        logger.error(f"API: Error initiating metadata cache refresh (Process Pool): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to initiate analytics metadata cache refresh.")

@router.get("/api/yahoo/ticker_currencies/{ticker_symbol}", 
            summary="Get trade and financial currencies for a ticker",
            response_model=Optional[Dict[str, Optional[str]]],
            tags=["Yahoo Finance Data"])
async def get_ticker_currencies_endpoint(
    ticker_symbol: str,
    query_service: YahooDataQueryService = Depends(get_yahoo_query_service)
):
    """Endpoint to retrieve trade and financial currencies for a given stock ticker."""
    currencies = await query_service.get_ticker_currencies(ticker_symbol)
    if currencies is None:
        raise HTTPException(status_code=404, detail=f"Ticker {ticker_symbol} not found or currencies not available.")
    return currencies

# --- NEW: Timeseries Price History API Endpoint ---
@router.get(
    "/api/v3/timeseries/price_history",
    summary="Fetch historical price data for a ticker",
    tags=["Timeseries Data"],
    response_model=List[Dict[str, Any]] # Expecting a list of OHLCV records
)
async def get_price_history(
    ticker: str,
    interval: str,
    start_date: Optional[str] = None, # YYYY-MM-DD
    end_date: Optional[str] = None,   # YYYY-MM-DD
    period: Optional[str] = None,
    query_service: YahooDataQueryService = Depends(get_yahoo_query_service)  # Add dependency injection
):
    """
    Fetch historical price data for a ticker.
    
    Args:
        ticker: The ticker symbol to fetch data for
        interval: The price interval (e.g. '1d' for daily)
        start_date: Optional start date in YYYY-MM-DD format
        end_date: Optional end date in YYYY-MM-DD format
        period: Optional period type (e.g. '1y', 'max')
        query_service: Injected YahooDataQueryService instance
    
    Returns:
        List of price data points with OHLCV data
    """
    logger.info(f"Price history request for {ticker}: interval={interval}, period={period}, start_date={start_date}, end_date={end_date}")
    
    try:
        # Use the cached service to get price data
        price_data = await query_service.get_price_history(
            ticker=ticker,
            interval=interval,
            period=period,
            start_date=start_date,
            end_date=end_date
        )
        
        if not price_data:
            logger.info(f"No price data found for {ticker} with the given parameters")
            return []
            
        return price_data
        
    except Exception as e:
        logger.error(f"Error fetching price data for {ticker}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching price data: {str(e)}"
        )

# --- NEW: Fundamentals History API Endpoint ---
@router.post("/api/v3/timeseries/fundamentals_history", summary="Fetch historical data for multiple fundamental fields for multiple tickers")
async def get_fundamentals_history_data(
    request_payload: FundamentalsHistoryRequest, # Renamed for clarity
    yahoo_data_query_service: YahooDataQueryService = Depends(get_yahoo_query_service)
):
    """
    Fetches historical data for specified fundamental fields for multiple tickers.
    The output structure depends on the 'include_projections' flag in the request.
    If True: { "ticker1": { "field_id1": {"points": [...], "projectionStartDate": "..."} } }
    If False: { "ticker1": { "field_id1": [ /* list of data points */ ] } }
    """
    try:
        logger.info(f"API: Received fundamentals history request: {request_payload.model_dump_json(exclude_none=True)}")
    except Exception as log_exc:
        logger.warning(f"API: Could not serialize request_payload for logging: {log_exc}")


    results_by_ticker: Dict[str, Dict[str, Any]] = {ticker: {} for ticker in request_payload.tickers}

    # Helper to derive the payload key - this was part of original code, keeping for context if needed, but not directly used in the core logic change.
    # def get_output_payload_key(identifier: str) -> Optional[str]:
    #     parts = identifier.split('_')
    #     if len(parts) >= 4 and parts[0] == 'yf' and parts[1] == 'item':
    #         return identifier 
    #     return None

    for ticker in request_payload.tickers:
        results_by_ticker[ticker] = {} # Ensure ticker key exists
        for field_id in request_payload.field_identifiers:
            try:
                logger.debug(f"API: Fetching data for ticker: {ticker}, field_id: {field_id}, include_projections: {request_payload.include_projections}")
                
                # Call get_specific_field_timeseries with the include_projections flag from the request.
                # The service function returns: Dict[str, Union[List[Dict], Dict[str, Any]]]
                # e.g., { "TICKER": data_for_ticker }
                # where data_for_ticker is List[Dict] if include_projections is False,
                # or Dict[str, Any] (with "points" and "projectionStartDate") if include_projections is True.
                service_response_for_field = await yahoo_data_query_service.get_specific_field_timeseries(
                    field_identifier=field_id,
                    tickers=[ticker], # Pass single ticker as a list
                    start_date_str=request_payload.start_date,
                    end_date_str=request_payload.end_date,
                    include_projection_metadata=request_payload.include_projections # Pass the flag
                )
                
                if ticker in service_response_for_field:
                    # The data for the ticker (service_response_for_field[ticker]) is already in the correct format
                    # (either List or Dict) based on the include_projection_metadata flag passed to the service.
                    results_by_ticker[ticker][field_id] = service_response_for_field[ticker]
                    logger.debug(f"API: Successfully processed {field_id} for {ticker}. Data type: {type(service_response_for_field[ticker])}. Projections included: {request_payload.include_projections}")
                else:
                    # If ticker data is unexpectedly missing from service response
                    logger.warning(f"API: No data returned from service for field {field_id}, ticker {ticker}. service_response_for_field: {service_response_for_field}")
                    # Provide a default empty structure based on the request flag
                    if request_payload.include_projections:
                        results_by_ticker[ticker][field_id] = {"points": [], "projectionStartDate": None, "error": f"Data not found for {field_id}"}
                    else:
                        # For non-projection requests, other studies might expect a list or handle errors differently.
                        # Returning an empty list for missing data, or an object with an error if preferred.
                        # Let's use an object with error for consistency in signaling issues.
                        results_by_ticker[ticker][field_id] = {"error": f"Data not found for {field_id}", "points": []}


            except Exception as e:
                logger.error(f"API: Error fetching/processing {field_id} for ticker {ticker}: {e}", exc_info=True)
                # Ensure the field_id key exists for the ticker with an error structure.
                # Default error structure can be the projection-inclusive one for consistency.
                results_by_ticker[ticker][field_id] = {
                    "points": [], 
                    "projectionStartDate": None,
                    "error": str(e)
                }
    
    logger.info(f"API: Fundamentals history response processing complete for tickers: {list(results_by_ticker.keys())}")
    try:
        response_str_for_log = str(results_by_ticker)
        max_log_len = 2000 # Max characters to log for the response object
        if len(response_str_for_log) > max_log_len:
            response_str_for_log = response_str_for_log[:max_log_len] + f"... (truncated, total tickers: {len(request_payload.tickers)}, fields per ticker approx: {len(request_payload.field_identifiers)})"
        logger.debug(f"API: Sending fundamentals history response: {response_str_for_log}")
    except Exception as log_exc:
        logger.warning(f"API: Could not serialize response for logging: {log_exc}")
        
    return results_by_ticker

# --- END NEW: Timeseries Price History API Endpoint --- 

class SyntheticFundamentalRequest(BaseModel):
    tickers: List[str]
    start_date: Optional[str] = None # YYYY-MM-DD
    end_date: Optional[str] = None   # YYYY-MM-DD

@router.post("/api/v3/timeseries/synthetic_fundamental/{fundamental_name}", 
            summary="Calculate and fetch timeseries for a synthetic fundamental metric",
            response_model=Dict[str, List[Dict[str, Any]]],
            tags=["Timeseries Data", "Fundamentals"])
async def get_synthetic_fundamental_timeseries(
    fundamental_name: str,
    request_payload: SyntheticFundamentalRequest,
    query_service: YahooDataQueryService = Depends(get_yahoo_query_service),
    pro_query_service: YahooDataQueryProService = Depends(get_yahoo_data_query_pro_service)
):
    """
    Calculates and retrieves a timeseries for a specified synthetic fundamental metric.

    - **fundamental_name**: The name of the synthetic fundamental to calculate (e.g., "EPS_TTM").
    - **tickers**: A list of ticker symbols.
    - **start_date**: Optional start date for the timeseries (YYYY-MM-DD). Defaults to YTD if not provided.
    - **end_date**: Optional end date for the timeseries (YYYY-MM-DD). Defaults to today if not provided.
    """
    try:
        # NEW: Logic to use YahooDataQueryAdvService for specific fundamentals
        if fundamental_name.upper() == "FCF_MARGIN_TTM":
            logger.info(f"Routing FCF_MARGIN_TTM to YahooDataQueryAdvService for tickers: {request_payload.tickers}")
            # Assuming db_repo is accessible or can be passed if AdvService needs it directly.
            # For now, AdvService constructor takes db_repo and base_query_srv.
            # The base_query_srv already has db_repo.
            adv_query_service = YahooDataQueryAdvService(
                db_repo=query_service.db_repo, # Pass the db_repo from the base service
                base_query_srv=query_service   # Pass the base service instance
            )
            result = await adv_query_service.calculate_fcf_margin_ttm(
                tickers=request_payload.tickers,
                start_date_str=request_payload.start_date,
                end_date_str=request_payload.end_date
            )
        # END NEW
        # NEW: Add routing for GROSS_MARGIN_TTM
        elif fundamental_name.upper() == "GROSS_MARGIN_TTM":
            logger.info(f"Routing GROSS_MARGIN_TTM to YahooDataQueryAdvService for tickers: {request_payload.tickers}")
            adv_query_service = YahooDataQueryAdvService(
                db_repo=query_service.db_repo, 
                base_query_srv=query_service
            )
            result = await adv_query_service.calculate_gross_margin_ttm(
                tickers=request_payload.tickers,
                start_date_str=request_payload.start_date,
                end_date_str=request_payload.end_date
            )
        # END NEW
        # NEW: Add routing for OPERATING_MARGIN_TTM
        elif fundamental_name.upper() == "OPERATING_MARGIN_TTM":
            logger.info(f"Routing OPERATING_MARGIN_TTM to YahooDataQueryAdvService for tickers: {request_payload.tickers}")
            adv_query_service = YahooDataQueryAdvService(
                db_repo=query_service.db_repo, 
                base_query_srv=query_service
            )
            result = await adv_query_service.calculate_operating_margin_ttm(
                tickers=request_payload.tickers,
                start_date_str=request_payload.start_date,
                end_date_str=request_payload.end_date
            )
        # END NEW
        # NEW: Add routing for NET_PROFIT_MARGIN_TTM
        elif fundamental_name.upper() == "NET_PROFIT_MARGIN_TTM":
            logger.info(f"Routing NET_PROFIT_MARGIN_TTM to YahooDataQueryAdvService for tickers: {request_payload.tickers}")
            adv_query_service = YahooDataQueryAdvService(
                db_repo=query_service.db_repo, 
                base_query_srv=query_service
            )
            result = await adv_query_service.calculate_net_profit_margin_ttm(
                tickers=request_payload.tickers,
                start_date_str=request_payload.start_date,
                end_date_str=request_payload.end_date
            )
        # END NEW
        # NEW: Add routing for PRICE_TO_SALES_TTM
        elif fundamental_name.upper() == "PRICE_TO_SALES_TTM":
            logger.info(f"Routing PRICE_TO_SALES_TTM to YahooDataQueryAdvService for tickers: {request_payload.tickers}")
            adv_query_service = YahooDataQueryAdvService(
                db_repo=query_service.db_repo, 
                base_query_srv=query_service
            )
            result = await adv_query_service.calculate_price_to_sales_ttm(
                tickers=request_payload.tickers,
                start_date_str=request_payload.start_date,
                end_date_str=request_payload.end_date
            )
        # NEW: Add routing for DEBT_TO_EQUITY
        elif fundamental_name.upper() == "DEBT_TO_EQUITY":
            logger.info(f"Routing DEBT_TO_EQUITY to YahooDataQueryAdvService for tickers: {request_payload.tickers}")
            adv_query_service = YahooDataQueryAdvService(
                db_repo=query_service.db_repo, 
                base_query_srv=query_service
            )
            result = await adv_query_service.calculate_debt_to_equity_for_tickers(
                tickers=request_payload.tickers,
                start_date_str=request_payload.start_date,
                end_date_str=request_payload.end_date
            )
        # NEW: Add routing for TOTAL_LIABILITIES_TO_EQUITY
        elif fundamental_name.upper() == "TOTAL_LIABILITIES_TO_EQUITY":
            logger.info(f"Routing TOTAL_LIABILITIES_TO_EQUITY to YahooDataQueryAdvService for tickers: {request_payload.tickers}")
            adv_query_service = YahooDataQueryAdvService(
                db_repo=query_service.db_repo, 
                base_query_srv=query_service
            )
            result = await adv_query_service.calculate_total_liabilities_to_equity_for_tickers(
                tickers=request_payload.tickers,
                start_date_str=request_payload.start_date,
                end_date_str=request_payload.end_date
            )
        # NEW: Add routing for TOTAL_LIABILITIES_TO_ASSETS
        elif fundamental_name.upper() == "TOTAL_LIABILITIES_TO_ASSETS":
            logger.info(f"Routing TOTAL_LIABILITIES_TO_ASSETS to YahooDataQueryAdvService for tickers: {request_payload.tickers}")
            adv_query_service = YahooDataQueryAdvService(
                db_repo=query_service.db_repo, 
                base_query_srv=query_service
            )
            result = await adv_query_service.calculate_total_liabilities_to_assets_for_tickers(
                tickers=request_payload.tickers,
                start_date_str=request_payload.start_date,
                end_date_str=request_payload.end_date
            )
        # NEW: Add routing for DEBT_TO_ASSETS
        elif fundamental_name.upper() == "DEBT_TO_ASSETS":
            logger.info(f"Routing DEBT_TO_ASSETS to YahooDataQueryAdvService for tickers: {request_payload.tickers}")
            adv_query_service = YahooDataQueryAdvService(
                db_repo=query_service.db_repo, 
                base_query_srv=query_service
            )
            result = await adv_query_service.calculate_debt_to_assets_for_tickers(
                tickers=request_payload.tickers,
                start_date_str=request_payload.start_date,
                end_date_str=request_payload.end_date
            )
        # NEW: Add routing for ROA_TTM
        elif fundamental_name.upper() == "ROA_TTM":
            logger.info(f"Routing ROA_TTM to YahooDataQueryAdvService for tickers: {request_payload.tickers}")
            adv_query_service = YahooDataQueryAdvService(
                db_repo=query_service.db_repo, 
                base_query_srv=query_service
            )
            result = await adv_query_service.calculate_roa_ttm(
                tickers=request_payload.tickers,
                start_date_str=request_payload.start_date,
                end_date_str=request_payload.end_date
            )
        # NEW: Add routing for ROE_TTM
        elif fundamental_name.upper() == "ROE_TTM":
            logger.info(f"Routing ROE_TTM to YahooDataQueryAdvService for tickers: {request_payload.tickers}")
            adv_query_service = YahooDataQueryAdvService(
                db_repo=query_service.db_repo, 
                base_query_srv=query_service
            )
            result = await adv_query_service.calculate_roe_ttm(
                tickers=request_payload.tickers,
                start_date_str=request_payload.start_date,
                end_date_str=request_payload.end_date
            )
        # NEW: Add routing for ROIC_TTM
        elif fundamental_name.upper() == "ROIC_TTM":
            logger.info(f"Routing ROIC_TTM to YahooDataQueryAdvService for tickers: {request_payload.tickers}")
            adv_query_service = YahooDataQueryAdvService(
                db_repo=query_service.db_repo, 
                base_query_srv=query_service
            )
            result = await adv_query_service.calculate_roic_ttm(
                tickers=request_payload.tickers,
                start_date_str=request_payload.start_date,
                end_date_str=request_payload.end_date
            )
        # NEW: Add routing for ASSET_TURNOVER_TTM
        elif fundamental_name.upper() == "ASSET_TURNOVER_TTM":
            logger.info(f"Routing ASSET_TURNOVER_TTM to YahooDataQueryAdvService for tickers: {request_payload.tickers}")
            adv_query_service = YahooDataQueryAdvService(
                db_repo=query_service.db_repo, 
                base_query_srv=query_service
            )
            result = await adv_query_service.calculate_asset_turnover_ttm(
                tickers=request_payload.tickers,
                start_date_str=request_payload.start_date,
                end_date_str=request_payload.end_date
            )
        # NEW: Add routing for EV_TO_FCF_TTM
        elif fundamental_name.upper() == "EV_TO_FCF_TTM":
            logger.info(f"Routing EV_TO_FCF_TTM to YahooDataQueryProService for tickers: {request_payload.tickers}")
            result = await pro_query_service.get_ev_to_fcf_ttm_timeseries(
                tickers=request_payload.tickers,
                start_date=request_payload.start_date,
                end_date=request_payload.end_date
            )
        # NEW: Add routing for EV_TO_SALES_TTM
        elif fundamental_name.upper() == "EV_TO_SALES_TTM":
            logger.info(f"Routing EV_TO_SALES_TTM to YahooDataQueryProService for tickers: {request_payload.tickers}")
            result = await pro_query_service.get_ev_to_sales_ttm_timeseries(
                tickers=request_payload.tickers,
                start_date=request_payload.start_date,
                end_date=request_payload.end_date
            )
        # NEW: Add routing for EV_TO_EBITDA_TTM
        elif fundamental_name.upper() == "EV_TO_EBITDA_TTM":
            logger.info(f"Routing EV_TO_EBITDA_TTM to YahooDataQueryProService for tickers: {request_payload.tickers}")
            result = await pro_query_service.get_ev_to_ebitda_ttm_timeseries(
                tickers=request_payload.tickers,
                start_date=request_payload.start_date,
                end_date=request_payload.end_date
            )
        # END NEW
        else:
            result = await query_service.calculate_synthetic_fundamental_timeseries(
                fundamental_name=fundamental_name,
                tickers=request_payload.tickers,
                start_date_str=request_payload.start_date,
                end_date_str=request_payload.end_date
            )
        return result
    except Exception as e:
        logger.error(f"Error in get_synthetic_fundamental_timeseries endpoint for {fundamental_name}: {e}", exc_info=True)
        # Consider returning a more specific HTTP error, e.g., 500 or 400 if input is bad.
        # For now, letting FastAPI handle the generic 500 for unhandled exceptions from the service.
        # If the service returns empty for unsupported, that will be handled by client or be an empty dict.
        raise HTTPException(status_code=500, detail=f"An error occurred while calculating synthetic fundamental {fundamental_name}: {str(e)}")

# --- ADDITION FOR NEW FEATURE ---
@router.get("/api/yahoo/analyst_price_targets/{ticker_symbol}",
            summary="Get latest analyst price targets for a ticker (New Feature)",
            response_model=Optional[Dict[str, float]],
            tags=["Yahoo Finance Data", "Analytics Additions"]) # New tag to distinguish
async def get_analyst_price_targets_for_ticker_new( # New function name
    ticker_symbol: str,
    query_service: YahooDataQueryService = Depends(get_yahoo_query_service)
):
    """
    (New Feature Endpoint)
    Retrieves the latest analyst price target data (high, low, mean, median)
    for a given ticker symbol.
    Data is fetched from the 'ticker_data_items' table where item_type
    is 'ANALYST_PRICE_TARGETS'. This endpoint is part of a new feature.
    """
    # logger.info(f"API (New Feature): Fetching analyst price targets for {ticker_symbol}") # Optional
    analyst_targets = await query_service.get_latest_analyst_price_targets(ticker_symbol)
    if analyst_targets:
        return analyst_targets
    return None
# --- END ADDITION FOR NEW FEATURE ---

# Ensure router is included in the main app if this is a separate file, e.g., app.include_router(router)
# Or if V3_backend_api.py defines `router = APIRouter()`, ensure this router is used. 