from fastapi import APIRouter, HTTPException, Query, status
from typing import Dict, Any, Optional, List
import logging
from pydantic import BaseModel

from ..services.edgar_service import (
    get_company_tickers_data,
    get_company_facts_data,
    EdgarServiceError
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/edgar",
    tags=["EDGAR"],
)

class CompanyTickerInfo(BaseModel):
    cik_str: int
    ticker: str
    title: str

class CompanyTickersResponse(BaseModel):
    data: List[CompanyTickerInfo]

@router.get("/company-tickers", response_model=Optional[CompanyTickerInfo])
async def get_company_ticker_info_endpoint(ticker: str = Query(..., description="Ticker symbol to search for")):
    """
    Searches for a specific company's CIK and title using its ticker symbol.
    """
    try:
        logger.info(f"Fetching all company tickers data to search for: {ticker}")
        all_tickers_data = get_company_tickers_data()
        if not all_tickers_data:
            # This case should ideally be handled by EdgarServiceError if fetch failed
            logger.error("Failed to retrieve any company ticker data from the service.")
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Could not retrieve company ticker list from SEC.")

        # The data is a dictionary where keys are indices and values are company info dicts.
        # We need to iterate through the values.
        for company_info in all_tickers_data.values():
            if company_info.get('ticker') == ticker.upper():
                logger.info(f"Found ticker {ticker}: CIK {company_info.get('cik_str')}, Title: {company_info.get('title')}")
                return CompanyTickerInfo(
                    cik_str=company_info.get('cik_str'),
                    ticker=company_info.get('ticker'),
                    title=company_info.get('title')
                )
        
        logger.warning(f"Ticker {ticker} not found in SEC data.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Ticker {ticker} not found.")

    except EdgarServiceError as e:
        logger.error(f"EdgarServiceError in /company-tickers for ticker {ticker}: {e}", exc_info=True)
        # Distinguish between general fetch failure and specific not found if possible
        # For now, mapping general service errors to 503 seems reasonable if it's about fetching the list itself
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e))
    except HTTPException:
        raise # Re-raise HTTPExceptions directly (like 404 from above)
    except Exception as e:
        logger.error(f"Unexpected error in /company-tickers for ticker {ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")

@router.get("/company-facts/{cik_str}", response_model=Dict[str, Any])
async def get_company_facts_endpoint(cik_str: str):
    """
    Fetches company facts (XBRL data) for a given CIK.
    The CIK should be the string representation of the CIK number.
    """
    try:
        logger.info(f"Fetching company facts for CIK: {cik_str}")
        facts_data = get_company_facts_data(cik_str)
        if not facts_data: # Should be caught by EdgarServiceError with 404 if that's the case
            logger.warning(f"No facts data returned by service for CIK {cik_str}, but no EdgarServiceError was raised.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No company facts found for CIK {cik_str}.")
        return facts_data
    except EdgarServiceError as e:
        logger.error(f"EdgarServiceError for CIK {cik_str}: {e}", exc_info=True)
        if "not found" in str(e).lower(): # Check if error message indicates CIK not found
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        else:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error fetching company facts for CIK {cik_str}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.") 