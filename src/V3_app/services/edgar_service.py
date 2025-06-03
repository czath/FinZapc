import requests
import logging
from typing import Dict, Any, Optional
import json

logger = logging.getLogger(__name__)

# IMPORTANT: Replace with your actual application name and contact email.
# SEC requires a descriptive User-Agent.
USER_AGENT = "YourAppName/1.0 (your.email@example.com)"

# Define constants for router usage
EDGAR_HEADERS = {"User-Agent": USER_AGENT}
REQUEST_TIMEOUT = 15  # Default timeout in seconds


class EdgarServiceError(Exception):
    """Custom exception for EDGAR service errors."""
    pass


def get_company_tickers_data() -> Optional[Dict[str, Any]]:
    """
    Fetches the complete company tickers and CIKs mapping from the SEC.

    Returns:
        Optional[Dict[str, Any]]: Parsed JSON data from SEC, or None if an error occurs.
                                    The JSON is a dictionary where keys are indices (as strings)
                                    and values are dictionaries containing 'cik_str', 'ticker', 'title'.
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    try:
        response = requests.get(url, headers=EDGAR_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        logger.info(f"Successfully fetched company_tickers.json from SEC. Status: {response.status_code}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching company_tickers.json from SEC: {e}", exc_info=True)
        raise EdgarServiceError(f"Failed to fetch company tickers from SEC: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from company_tickers.json: {e}", exc_info=True)
        raise EdgarServiceError(f"Failed to decode company tickers JSON from SEC: {e}")


def get_company_facts_data(cik: str) -> Optional[Dict[str, Any]]:
    """
    Fetches company facts (XBRL data) for a given CIK from the SEC.

    Args:
        cik (str): The CIK of the company. Should be the numeric string.

    Returns:
        Optional[Dict[str, Any]]: Parsed JSON data from SEC, or None if an error occurs.
    """
    # The API expects a 10-digit CIK, zero-padded.
    # The CIKs from company_tickers.json are usually integers.
    try:
        formatted_cik = str(cik).zfill(10)
    except Exception: # Handle cases where cik might not be easily convertible to string/int
        logger.error(f"Invalid CIK format received: {cik}. Cannot format to 10-digit string.")
        raise EdgarServiceError(f"Invalid CIK format: {cik}")

    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{formatted_cik}.json"
    try:
        response = requests.get(url, headers=EDGAR_HEADERS, timeout=REQUEST_TIMEOUT) 
        response.raise_for_status()
        logger.info(f"Successfully fetched company facts for CIK {formatted_cik}. Status: {response.status_code}")
        return response.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"Company facts not found for CIK {formatted_cik} (404). URL: {url}")
            raise EdgarServiceError(f"Company facts not found for CIK {formatted_cik}.")
        logger.error(f"HTTP error fetching company facts for CIK {formatted_cik}: {e}", exc_info=True)
        raise EdgarServiceError(f"Failed to fetch company facts for CIK {formatted_cik}: {e}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching company facts for CIK {formatted_cik}: {e}", exc_info=True)
        raise EdgarServiceError(f"Failed to fetch company facts for CIK {formatted_cik}: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON for company facts CIK {formatted_cik}: {e}", exc_info=True)
        raise EdgarServiceError(f"Failed to decode company facts JSON for CIK {formatted_cik}: {e}") 