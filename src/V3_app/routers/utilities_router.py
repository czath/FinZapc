from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel, Field
import logging

# Assuming get_latest_atr is in V3_yahoo_fetch.py and accessible
# Adjust the import path as per your project structure
from ..V3_yahoo_fetch import get_latest_atr 

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/utilities",
    tags=["Utilities"],
)

class ATRRequest(BaseModel):
    ticker: str = Field(..., title="Ticker Symbol", description="The stock ticker symbol (e.g., AAPL)")
    period: int = Field(14, title="ATR Period", description="The period for ATR calculation", gt=0)

class ATRResponse(BaseModel):
    ticker: str
    period: int
    atr_value: float | None = None
    error: str | None = None

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

# Example of how to include this router in your main.py:
# from V3_app.routers import utilities_router
# app.include_router(utilities_router.router) 