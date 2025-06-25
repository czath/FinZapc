from fastapi import APIRouter, Depends, HTTPException, Body, File, UploadFile, Form
from fastapi.responses import Response
from pydantic import BaseModel, Field
import logging
import math
from typing import List, Dict, Any, Optional

# Assuming get_latest_atr is in V3_yahoo_fetch.py and accessible
# Adjust the import path as per your project structure
from ..V3_yahoo_fetch import get_latest_atr 
from ..yahoo_data_query_srv import YahooDataQueryService
from ..dependencies import get_repository
from ..yahoo_repository import YahooDataRepository
from ..V3_database import SQLiteRepository

# --- LLM Analytics Imports ---
from ..services.llm_service import LLMService
from ..llm_data_fetcher import get_yahoo_data_for_tickers
from .. import db_maintenance

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["Utilities"]
)

# --- LOCAL DEPENDENCIES FOR THIS ROUTER ---
async def get_yahoo_repository(repo: SQLiteRepository = Depends(get_repository)) -> YahooDataRepository:
    return YahooDataRepository(database_url=repo.database_url)

async def get_yahoo_query_service(db_repo: YahooDataRepository = Depends(get_yahoo_repository)) -> YahooDataQueryService:
    return YahooDataQueryService(db_repo=db_repo)
# --- END LOCAL DEPENDENCIES ---

# --- JSON SERIALIZATION HELPERS ---
def clean_float_value(value):
    """Clean float values for JSON serialization."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    return value

def clean_dict_for_json(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean a dictionary by removing or fixing invalid float values for JSON serialization."""
    if not isinstance(data, dict):
        return data
    
    cleaned = {}
    for key, value in data.items():
        if isinstance(value, dict):
            cleaned[key] = clean_dict_for_json(value)
        elif isinstance(value, list):
            cleaned[key] = [clean_dict_for_json(item) if isinstance(item, dict) else clean_float_value(item) for item in value]
        else:
            cleaned[key] = clean_float_value(value)
    return cleaned

def clean_list_for_json(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Clean a list of dictionaries for JSON serialization."""
    return [clean_dict_for_json(item) for item in data]
# --- END JSON SERIALIZATION HELPERS ---

class ATRRequest(BaseModel):
    ticker: str = Field(..., title="Ticker Symbol", description="The stock ticker symbol (e.g., AAPL)")
    period: int = Field(14, title="ATR Period", description="The period for ATR calculation", gt=0)

class ATRResponse(BaseModel):
    ticker: str
    period: int
    atr_value: float | None = None
    error: str | None = None

# --- LLM Analytics Models ---
class LLMReportRequest(BaseModel):
    tickers: List[str] = Field(..., min_items=1, description="A list of ticker symbols for analysis.")
    prompt: Optional[str] = Field(None, description="Optional user prompt for the financial analysis. If empty or None, the default prompt from configuration will be used.")

class LLMReportResponse(BaseModel):
    report_markdown: str
    history: List[Dict[str, Any]]
    model_name: str | None = None

class LLMChatRequest(BaseModel):
    history: List[Dict[str, Any]] = Field(..., description="The conversation history.")

class LLMChatResponse(BaseModel):
    response_markdown: str

class ScanInvalidRecordsRequest(BaseModel):
    threshold: float = Field(0.7, ge=0.0, le=1.0, description="Minimum emptiness threshold (0.0 to 1.0).")
    start_date: Optional[str] = Field(None, description="Start date for item_key_date (YYYY-MM-DD).")
    end_date: Optional[str] = Field(None, description="End date for item_key_date (YYYY-MM-DD).")

class InvalidRecordsDeleteRequest(BaseModel):
    ticker_master: List[str] = Field(default_factory=list, description="List of ticker symbols to delete from the master table.")
    ticker_data_items: List[int] = Field(default_factory=list, description="List of data item IDs to delete from the items table.")

@router.post("/calculate_atr", response_model=ATRResponse)
async def calculate_atr_endpoint(request_data: ATRRequest):
    """
    Calculates the Average True Range (ATR) for a given ticker symbol and period.
    """
    ticker = request_data.ticker.strip().upper()
    period = request_data.period

    logger.info(f"Received ATR calculation request for ticker: {ticker}, period: {period}")

    if not ticker:
        logger.warning("ATR calculation request with empty ticker.")
        # Pydantic validation should catch this, but as a safeguard:
        raise HTTPException(status_code=400, detail="Ticker symbol cannot be empty.")
    
    try:
        atr_value = await get_latest_atr(ticker_symbol=ticker, atr_period=period)

        if atr_value is not None:
            logger.info(f"Successfully calculated ATR for {ticker} (period {period}): {atr_value}")
            return ATRResponse(ticker=ticker, period=period, atr_value=atr_value)
        else:
            logger.warning(f"ATR calculation returned None for {ticker} (period {period}). Insufficient data or other issue.")
            return ATRResponse(ticker=ticker, period=period, error="Could not calculate ATR. Insufficient data or ticker not found.")

    except Exception as e:
        logger.error(f"Error calculating ATR for {ticker} (period {period}): {e}", exc_info=True)
        # Return a more generic error to the client for security/simplicity
        # Specific error is logged internally
        raise HTTPException(status_code=500, detail=f"An internal error occurred while calculating ATR for {ticker}.")

# --- LLM Analytics Endpoints ---

@router.post("/generate_llm_report", response_model=LLMReportResponse, summary="Generate Financial Report with LLM")
async def generate_llm_report(
    tickers: str = Form(..., description="Comma-separated list of ticker symbols"),
    prompt: Optional[str] = Form(None, description="Optional user prompt for analysis"),
    sample_reports: List[UploadFile] = File(default=[], description="Optional sample report files to use as examples"),
    query_service: YahooDataQueryService = Depends(get_yahoo_query_service),
):
    """
    Takes a list of tickers, an optional prompt, and optional sample report files,
    fetches comprehensive Yahoo data, and uses the Gemini LLM to generate a financial analysis report.
    Sample reports will be used as examples to improve output quality.
    """
    try:
        # 1. Parse tickers from form data
        ticker_list = [ticker.strip().upper() for ticker in tickers.split(',') if ticker.strip()]
        if not ticker_list:
            raise HTTPException(status_code=400, detail="At least one ticker symbol is required.")

        # 2. Fetch data using the isolated data fetcher
        logger.info(f"Endpoint: Fetching data for tickers: {ticker_list}")
        tickers_data = await get_yahoo_data_for_tickers(ticker_list, query_service)
        if not tickers_data:
            raise HTTPException(status_code=404, detail="Could not retrieve any data for the specified tickers.")

        # 3. Process sample report files if provided
        examples_content = ""
        if sample_reports:
            logger.info(f"Endpoint: Processing {len(sample_reports)} sample report files.")
            for i, file in enumerate(sample_reports):
                if file.filename:
                    try:
                        # Read file content
                        content = await file.read()
                        # Decode content (assuming text files)
                        decoded_content = content.decode('utf-8')
                        examples_content += f"\n--- Sample Report {i+1} ({file.filename}) ---\n"
                        examples_content += decoded_content + "\n"
                    except Exception as e:
                        logger.warning(f"Failed to read sample report file {file.filename}: {e}")
                        continue

        # 4. Log which prompt type will be used
        if prompt and prompt.strip():
            logger.info(f"Endpoint: Using user-provided prompt for tickers: {ticker_list}")
        else:
            logger.info(f"Endpoint: Using default configuration prompt for tickers: {ticker_list}")

        # 5. Generate report using the LLM service
        logger.info(f"Endpoint: Generating report for tickers: {ticker_list}")
        llm_service = LLMService()
        report_data = await llm_service.generate_report(tickers_data, prompt, examples_content if examples_content.strip() else None)
        
        # Add the model name to the response
        response_payload = {
            **report_data,
            "model_name": llm_service.model_name
        }
        
        return LLMReportResponse(**response_payload)

    except Exception as e:
        logger.error(f"Error in generate_llm_report endpoint: {e}", exc_info=True)
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.post("/llm_chat", response_model=LLMChatResponse, summary="Continue a conversation with the LLM")
async def llm_chat(request: LLMChatRequest):
    """
    Takes an existing conversation history and continues the chat with the LLM.
    """
    try:
        logger.info(f"Endpoint: Continuing chat. History has {len(request.history)} turns.")
        llm_service = LLMService()
        response_text = await llm_service.continue_chat(request.history)
        return LLMChatResponse(response_markdown=response_text)
    except Exception as e:
        logger.error(f"Error in llm_chat endpoint: {e}", exc_info=True)
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"An unexpected chat error occurred: {e}")

@router.get("/all-tickers", response_model=List[str], summary="Get All Ticker Symbols")
async def get_all_tickers(
    repo: YahooDataRepository = Depends(get_yahoo_repository),
):
    """
    Retrieves a list of all available ticker symbols from the 'ticker_master' table.
    """
    try:
        logger.info("Fetching all ticker symbols.")
        tickers = await repo.get_all_master_tickers()
        if not tickers:
            logger.warning("No tickers found in the database.")
            return []
        logger.info(f"Successfully fetched {len(tickers)} tickers.")
        return tickers
    except Exception as e:
        logger.error(f"Error fetching all tickers: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch ticker list.")

@router.get(
    "/all-tickers-with-names",
    summary="Get all available tickers with their company names",
    response_model=List[Dict[str, str]],
    tags=["Utilities"]
)
async def get_all_tickers_with_names(
    repo: YahooDataRepository = Depends(get_yahoo_repository)
):
    """
    Retrieves a list of all available tickers from the 'ticker_master' table,
    including their company names.
    """
    try:
        logger.info("Fetching all ticker symbols with company names.")
        tickers_with_names = await repo.get_all_master_tickers_with_names()
        if not tickers_with_names:
            logger.warning("No tickers with names found in the database.")
            return []
        logger.info(f"Successfully fetched {len(tickers_with_names)} tickers with names.")
        return tickers_with_names
    except Exception as e:
        logger.error(f"Error fetching all tickers with names: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch ticker list with names.")

@router.get("/ticker-profile/{ticker}", response_model=Dict[str, Any], summary="Get profile for a single ticker")
async def get_ticker_profile(
    ticker: str,
    query_service: YahooDataQueryService = Depends(get_yahoo_query_service),
):
    """
    Retrieves the master profile for a single ticker symbol.
    """
    try:
        logger.info(f"Fetching profile for ticker: {ticker}")
        profile = await query_service.get_ticker_profile(ticker)
        if not profile:
            logger.warning(f"Profile not found for ticker: {ticker}")
            raise HTTPException(status_code=404, detail="Ticker profile not found.")
        logger.info(f"Successfully fetched profile for {ticker}")
        return clean_dict_for_json(profile)
    except Exception as e:
        logger.error(f"Error fetching profile for {ticker}: {e}", exc_info=True)
        if isinstance(e, HTTPException):
            raise e # Re-raise if it's already an HTTPException
        raise HTTPException(status_code=500, detail=f"Failed to fetch profile for ticker: {ticker}")

@router.get(
    "/tickers-by-sector/{sector}",
    summary="Get all tickers in a specific sector",
    response_model=List[Dict[str, Any]],
    tags=["Utilities"]
)
async def get_tickers_by_sector(
    sector: str,
    query_service: YahooDataQueryService = Depends(get_yahoo_query_service)
):
    """
    Retrieves a list of all tickers within a specific sector.
    """
    try:
        logger.info(f"Fetching tickers for sector: {sector}")
        # URL encoding might replace spaces with %20, FastAPI handles this decoding.
        logger.debug(f"Received sector for lookup: '{sector}'")
        tickers = await query_service.get_tickers_by_sector(sector)
        if not tickers:
            logger.warning(f"No tickers found for sector: {sector}")
            return []
        logger.info(f"Successfully fetched {len(tickers)} tickers for sector: {sector}")
        return clean_list_for_json(tickers)
    except Exception as e:
        logger.error(f"Error fetching tickers for sector {sector}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch ticker list for sector: {sector}")

@router.get(
    "/tickers-by-industry/{industry}",
    summary="Get all tickers in a specific industry",
    response_model=List[Dict[str, Any]],
    tags=["Utilities"]
)
async def get_tickers_by_industry(
    industry: str,
    query_service: YahooDataQueryService = Depends(get_yahoo_query_service)
):
    """
    Retrieves a list of all tickers within a specific industry.
    """
    try:
        logger.info(f"Fetching tickers for industry: {industry}")
        logger.debug(f"Received industry for lookup: '{industry}'")
        tickers = await query_service.get_tickers_by_industry(industry)
        if not tickers:
            logger.warning(f"No tickers found for industry: {industry}")
            return []
        logger.info(f"Successfully fetched {len(tickers)} tickers for industry: {industry}")
        return clean_list_for_json(tickers)
    except Exception as e:
        logger.error(f"Error fetching tickers for industry {industry}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch ticker list for industry: {industry}")

@router.post("/scan-invalid-records",
             summary="Scan DB for invalid records with filters",
             response_model=Dict[str, List[Dict[str, Any]]],
             tags=["Database Utilities"])
async def scan_for_invalid_records(
    request_data: ScanInvalidRecordsRequest,
    repo: YahooDataRepository = Depends(get_yahoo_repository),
):
    """
    Scans the Yahoo ticker_master and ticker_data_items tables for records
    that meet the specified emptiness and date criteria.
    """
    try:
        invalid_records = await db_maintenance.find_invalid_records(
            repo=repo,
            threshold=request_data.threshold,
            start_date_str=request_data.start_date,
            end_date_str=request_data.end_date
        )
        return invalid_records
    except Exception as e:
        logger.error(f"Error during invalid record scan: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during the scan: {e}")

@router.post("/delete-invalid-records",
             summary="Delete specified invalid records",
             response_model=Dict[str, Any],
             tags=["Database Utilities"])
async def delete_invalid_records_endpoint(
    request_data: InvalidRecordsDeleteRequest,
    repo: YahooDataRepository = Depends(get_yahoo_repository),
):
    """
    Deletes a specified list of invalid records from the database.
    """
    try:
        records_to_delete = {
            "ticker_master": request_data.ticker_master,
            "ticker_data_items": request_data.ticker_data_items
        }
        summary = await db_maintenance.delete_invalid_records(repo, records_to_delete)
        return summary
    except Exception as e:
        logger.error(f"Error during invalid record deletion: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during deletion: {e}")

# Example of how to include this router in your main.py:
# from V3_app.routers import utilities_router
# app.include_router(utilities_router.router) 