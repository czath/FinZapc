from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from .V3_database import ScreenerModel, PositionModel, SQLiteRepository
from .V3_web import get_db
from pydantic import BaseModel
import uuid
import asyncio
from typing import List, Dict, Any, Optional
import logging

# Import your Yahoo fetch logic (adjust import as needed)
from .V3_yahoo_fetch import mass_load_yahoo_data_from_file, YahooDataRepository
from .dependencies import get_db # Assuming you have a get_db dependency provider
from .yahoo_data_query_srv import YahooDataQueryService
from .analytics_data_processor import AnalyticsDataProcessor # <-- IMPORT AnalyticsDataProcessor

router = APIRouter()

# --- Progress tracking ---
yahoo_job_progress: Dict[str, Dict[str, Any]] = {}
yahoo_job_lock = asyncio.Lock()

# --- Logger ---
logger = logging.getLogger(__name__)

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

# --- New API Route for AnalyticsDataProcessor ---
@router.get("/api/v3/analytics/processed_data",
            summary="Process and retrieve analytics data (Finviz, Yahoo, or Both)",
            response_model=Dict[str, Any], # Expecting {"originalData": [], "metaData": {}}
            tags=["Analytics Data V3"])
async def get_processed_analytics_data(
    data_source_selection: str, # Query param: "finviz_only", "yahoo_only", "both"
    request: Request, # To be used by dependency injection for repository
    # Inject SQLiteRepository for ADP
    # Option 1: Directly use the new provider
    sqlite_repo: SQLiteRepository = Depends(get_sqlite_repository) 
    # Option 2: If YahooDataRepository is a SQLiteRepository, could use get_yahoo_repository
    # and cast, but explicit SQLiteRepository is cleaner if ADP only needs that.
):
    """
    Orchestrates data loading and processing via AnalyticsDataProcessor.
    - **data_source_selection**: Specifies the data to load:
        - "finviz_only": Loads only Finviz data.
        - "yahoo_only": Loads only Yahoo data.
        - "both": Loads both Finviz and Yahoo data, then merges them.
    """
    logger.info(f"Received request for /api/v3/analytics/processed_data with selection: {data_source_selection}")

    valid_selections = ["finviz_only", "yahoo_only", "both"]
    if data_source_selection not in valid_selections:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid data_source_selection. Must be one of {valid_selections}"
        )

    adp = None
    try:
        # Instantiate AnalyticsDataProcessor with the SQLiteRepository
        adp = AnalyticsDataProcessor(db_repository=sqlite_repo)
        
        # Define a simple async progress callback if needed for logging ADP's internal steps
        # This is optional for now, ADP has its own logging.
        # async def _progress_logger(update: Dict[str, Any]):
        #     logger.debug(f"ADP Progress: {update}")

        processed_result = await adp.process_data_for_analytics(
            data_source_selection=data_source_selection
            # progress_callback=_progress_logger # Can add if detailed progress needed here
        )
        
        logger.info(f"Successfully processed data for selection '{data_source_selection}'. Returning {len(processed_result.get('originalData', []))} records.")
        return processed_result

    except HTTPException as http_exc: # Re-raise HTTPExceptions directly
        raise http_exc
    except Exception as e:
        logger.error(f"Error in get_processed_analytics_data endpoint for selection '{data_source_selection}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error processing analytics data: {str(e)}")
    finally:
        if adp and hasattr(adp, 'close_http_client'):
            try:
                await adp.close_http_client()
                logger.info("ADP HTTP client closed successfully.")
            except Exception as e_close:
                logger.error(f"Error closing ADP HTTP client: {e_close}", exc_info=True) 