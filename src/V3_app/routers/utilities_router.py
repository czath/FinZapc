from fastapi import APIRouter, Depends, HTTPException, Body
from fastapi.responses import Response
from pydantic import BaseModel, Field
import logging
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
    prompt: str = Field(..., min_length=10, description="The user's prompt for the financial analysis.")

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
    request: LLMReportRequest,
    query_service: YahooDataQueryService = Depends(get_yahoo_query_service),
):
    """
    Takes a list of tickers and a prompt, fetches comprehensive Yahoo data,
    and uses the Gemini LLM to generate a financial analysis report.
    """
    try:
        # 1. Fetch data using the isolated data fetcher
        logger.info(f"Endpoint: Fetching data for tickers: {request.tickers}")
        tickers_data = await get_yahoo_data_for_tickers(request.tickers, query_service)
        if not tickers_data:
            raise HTTPException(status_code=404, detail="Could not retrieve any data for the specified tickers.")

        # 2. Generate report using the LLM service
        logger.info(f"Endpoint: Generating report for tickers: {request.tickers}")
        llm_service = LLMService()
        report_data = await llm_service.generate_report(tickers_data, request.prompt)
        
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