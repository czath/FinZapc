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
from datetime import datetime

# Import your Yahoo fetch logic (adjust import as needed)
from .V3_yahoo_fetch import mass_load_yahoo_data_from_file, YahooDataRepository, fetch_daily_historical_data
from .dependencies import get_db # Assuming you have a get_db dependency provider
from .yahoo_data_query_srv import YahooDataQueryService
from .analytics_data_processor import AnalyticsDataProcessor # <-- IMPORT AnalyticsDataProcessor

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
    # No db_repo or query_service needed here as fetch_daily_historical_data is standalone
):
    """
    Fetches historical price data for a given ticker.

    - **ticker**: The stock symbol (e.g., AAPL).
    - **interval**: Data interval (e.g., "1d", "1wk", "1mo").
      Valid intervals: "1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo".
    - **start_date**: Start date in YYYY-MM-DD format (inclusive). Used if 'period' is not provided.
    - **end_date**: End date in YYYY-MM-DD format (exclusive). Used if 'period' is not provided.
    - **period**: Predefined period (e.g., "1y", "max"). If provided, start_date and end_date are ignored.
      Valid periods: "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max".
    """
    logger.info(f"Price history request for {ticker=}, {interval=}, {start_date=}, {end_date=}, {period=}")

    if interval not in VALID_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid interval. Must be one of: {', '.join(VALID_INTERVALS)}"
        )

    parsed_start_date: Optional[datetime] = None
    parsed_end_date: Optional[datetime] = None

    if period:
        if period not in VALID_PERIODS:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid period. Must be one of: {', '.join(VALID_PERIODS)}"
            )
        if start_date or end_date:
            logger.warning(f"Both period ('{period}') and start_date/end_date provided. Period will take precedence.")
            start_date, end_date = None, None # Ensure start/end are not used if period is set
    elif start_date and end_date:
        try:
            parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d")
            parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d")
            if parsed_start_date >= parsed_end_date:
                raise HTTPException(status_code=400, detail="start_date must be before end_date.")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
    else:
        raise HTTPException(status_code=400, detail="Either 'period' or both 'start_date' and 'end_date' must be provided.")

    try:
        # Import here to avoid circular dependencies if V3_yahoo_fetch imports things from V3_backend_api
        # (though currently it doesn't seem to be the case)
        from .V3_yahoo_fetch import fetch_daily_historical_data 
        import pandas as pd

        history_df = await fetch_daily_historical_data(
            ticker_symbol=ticker,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
            interval=interval,
            period=period
        )

        if history_df is None:
            logger.info(f"No historical data found for {ticker} with the given parameters.")
            # Return 200 with empty list, as this is not strictly an error, just no data.
            # Frontend can then display "No data available".
            return [] 
        
        if history_df.empty:
            logger.info(f"Historical data DataFrame is empty for {ticker} with the given parameters.")
            return []

        # Prepare DataFrame for JSON response
        # Reset index to make the DatetimeIndex a column
        history_df.reset_index(inplace=True)
        
        # Ensure the date column is named consistently, e.g., 'Datetime' or 'Date'
        # yfinance usually names it 'Datetime' or 'Date' depending on interval
        date_col_name = None
        if 'Datetime' in history_df.columns:
            date_col_name = 'Datetime'
        elif 'Date' in history_df.columns:
            date_col_name = 'Date'
        
        if date_col_name:
            # Convert datetime objects to ISO format strings
            history_df[date_col_name] = history_df[date_col_name].dt.strftime('%Y-%m-%dT%H:%M:%S')
        else:
            logger.warning("Date/Datetime column not found in yfinance history_df after reset_index(). Dates may not be formatted correctly for JSON.")

        # Convert NaN to None for JSON compatibility
        history_df = history_df.replace({pd.NaT: None, float('nan'): None})
        
        # Convert DataFrame to list of dictionaries
        data_to_return = history_df.to_dict(orient='records')
        
        logger.info(f"Successfully fetched and processed {len(data_to_return)} historical records for {ticker}.")
        return data_to_return

    except HTTPException as http_exc: # Re-raise HTTPException
        raise http_exc
    except Exception as e:
        logger.error(f"Error fetching price history for {ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# --- NEW: Fundamentals History API Endpoint ---
@router.post("/api/v3/timeseries/fundamentals_history", summary="Fetch historical data for multiple fundamental fields for multiple tickers")
async def get_fundamentals_history_data(
    request: FundamentalsHistoryRequest,
    yahoo_data_query_service: YahooDataQueryService = Depends(get_yahoo_query_service)
):
    """
    Fetches time series data for specified fundamental fields (via full field_identifiers) 
    for a list of tickers over a given period.
    The `field_identifiers` should be the complete internal names like 
    'yf_item_balance_sheet_annual_Total Assets'.
    """
    logger.info(f"Received request for fundamentals history: {request.tickers}, {len(request.field_identifiers)} fields")
    
    all_results: Dict[str, Dict[str, List[Dict[str, Any]]]] = {ticker: {} for ticker in request.tickers}
    
    # Helper to parse payload_key from field_identifier
    def get_payload_key(identifier: str) -> Optional[str]:
        parts = identifier.split('_')
        if len(parts) > 4 and parts[0] == 'yf' and parts[1] == 'item':
            return ' '.join(parts[4:]) # e.g., "Total Assets"
        logger.warning(f"Could not parse payload_key from identifier: {identifier}")
        return None

    for ticker in request.tickers:
        for field_id in request.field_identifiers:
            try:
                logger.debug(f"Fetching data for ticker: {ticker}, field_id: {field_id}")
                # get_specific_field_timeseries expects a single ticker and a single field_identifier
                # It returns Dict[str, List[Dict[str, Any]]] -> {actual_ticker_from_data: [{'date': date, 'value': value}, ...]}
                # or an empty dict if no data
                
                # The get_specific_field_timeseries function expects a list of tickers,
                # but we are calling it per ticker here to fit the desired output structure per ticker.
                # Let's call it with a single ticker in a list.
                timeseries_data_for_field = await yahoo_data_query_service.get_specific_field_timeseries(
                    field_identifier=field_id,
                    tickers=[ticker], # Pass single ticker as a list
                    start_date_str=request.start_date,
                    end_date_str=request.end_date
                )
                
                payload_key = get_payload_key(field_id)
                if not payload_key:
                    logger.warning(f"Skipping field_id {field_id} for ticker {ticker} due to unparsable payload_key.")
                    continue

                # timeseries_data_for_field will be like: { 'TICKER_SYMBOL': [ {'date': ..., 'value': ...}, ... ] }
                # or {} if no data for that ticker/field combination.
                if ticker in timeseries_data_for_field and timeseries_data_for_field[ticker]:
                    # Ensure the payload_key sub-dictionary exists for the current ticker
                    if payload_key not in all_results[ticker]:
                        all_results[ticker][payload_key] = []
                    all_results[ticker][payload_key].extend(timeseries_data_for_field[ticker])
                    logger.debug(f"Successfully fetched {len(timeseries_data_for_field[ticker])} points for {ticker} - {payload_key}")
                else:
                    # Ensure the payload_key entry exists even if no data, to indicate it was queried
                    if payload_key not in all_results[ticker]:
                         all_results[ticker][payload_key] = []
                    logger.info(f"No data found for ticker: {ticker}, field_id: {field_id} (parsed as {payload_key})")

            except Exception as e:
                logger.error(f"Error processing field {field_id} for ticker {ticker}: {e}", exc_info=True)
                # Optionally, you could add error information to the response here for this specific field/ticker
                payload_key = get_payload_key(field_id)
                if payload_key and ticker in all_results:
                     if payload_key not in all_results[ticker]:
                        all_results[ticker][payload_key] = [] # Represent as no data on error
                     # You might want to add an error marker: all_results[ticker][payload_key].append({"error": str(e)})

    # Clean up tickers that might have no data for any requested field by removing them if their field dict is empty.
    # However, the current structure ensures tickers are present, and fields are present (possibly with empty lists).
    # This is probably fine, as the frontend will see which fields have data.

    logger.info(f"Completed fundamentals history processing. Returning data for {len(all_results)} tickers.")
    return all_results

# --- END NEW: Timeseries Price History API Endpoint --- 