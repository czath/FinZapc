from fastapi import APIRouter, HTTPException, Query, status, Path
from typing import Dict, Any, Optional, List
import logging
from pydantic import BaseModel
import httpx
import json

from ..services.edgar_service import (
    get_company_tickers_data,
    get_company_facts_data,
    EdgarServiceError,
    REQUEST_TIMEOUT,
    EDGAR_HEADERS
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

@router.get("/company-concept/{cik}/{taxonomy}/{concept_name}", 
            summary="Fetch specific concept data for a company from SEC EDGAR", 
            tags=["EDGAR"], 
            response_model=Dict[str, Any])
async def get_company_concept_data(
    cik: str = Path(..., description="Company CIK (Central Index Key), 10 digits zero-padded if needed"),
    taxonomy: str = Path(..., description="Taxonomy, e.g., us-gaap, ifrs-full, dei"),
    concept_name: str = Path(..., description="Concept name, e.g., Assets, RevenueFromContractWithCustomerExcludingAssessedTax")
):
    """
    Fetches data for a specific XBRL concept for a given company CIK from the SEC EDGAR API.
    Example: /company-concept/0000320193/us-gaap/Assets
    """
    # SEC API expects CIK without leading zeros for this specific endpoint, but tests show it works with them too.
    # However, data.sec.gov documentation for companyfacts implies CIK needs to be 10 digits. 
    # Let's ensure CIK is handled as the SEC expects based on their API structure.
    # The companyconcept API seems flexible, so we might not need to strip leading zeros from CIK.

    # Ensure CIK is zero-padded to 10 digits
    cik_padded = cik.zfill(10)

    # Clean the concept_name, it might contain characters that need to be URL-encoded,
    # although path parameters are typically handled by FastAPI/Starlette.
    # For safety, ensure it's a valid part of a URL path, though usually this is not an issue here.

    api_url = f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik_padded}/{taxonomy}/{concept_name}.json"
    logger.info(f"Fetching specific concept data from SEC EDGAR: {api_url}")

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        try:
            response = await client.get(api_url, headers=EDGAR_HEADERS)
            response.raise_for_status() # Raises an exception for 4XX/5XX responses
            concept_data = response.json()
            logger.info(f"Successfully fetched concept data for CIK {cik}, Taxonomy {taxonomy}, Concept {concept_name}")
            return concept_data
        except httpx.RequestError as e:
            logger.error(f"RequestError fetching concept data for CIK {cik}, Tax {taxonomy}, Concept {concept_name}: {e}")
            raise HTTPException(status_code=503, detail=f"Error connecting to SEC EDGAR for concept: {str(e)}")
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTPStatusError for CIK {cik}, Tax {taxonomy}, Concept {concept_name}: Code {e.response.status_code} - {e.response.text}")
            if e.response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"Concept data not found for CIK {cik}, Taxonomy {taxonomy}, Concept {concept_name}.")
            if e.response.status_code == 400: # Bad request, often due to invalid CIK/taxonomy/concept combination
                 raise HTTPException(status_code=400, detail=f"Invalid request for CIK {cik}, Taxonomy {taxonomy}, Concept {concept_name}. Check parameters. SEC: {e.response.text}")
            raise HTTPException(status_code=e.response.status_code, detail=f"SEC EDGAR API error for concept: {e.response.text}")
        except json.JSONDecodeError as e:
            logger.error(f"JSONDecodeError parsing concept data for CIK {cik}, Tax {taxonomy}, Concept {concept_name}: {e}. Response text: {response.text[:500]}")
            raise HTTPException(status_code=500, detail=f"Failed to parse concept data from SEC EDGAR. Response was not valid JSON.")
        except Exception as e:
            logger.error(f"Unexpected error fetching concept data for CIK {cik}, Tax {taxonomy}, Concept {concept_name}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Internal server error processing EDGAR concept data.")

# Ensure V3_web.py includes this router correctly.
# If this router is already included in V3_web.py, no changes needed there for this new endpoint. 