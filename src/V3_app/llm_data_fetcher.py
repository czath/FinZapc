import logging
from typing import List, Dict, Any, Optional
import asyncio

# Assuming YahooDataQueryService is in a file that can be imported
# The exact import path might need adjustment based on your final structure.
from .yahoo_data_query_srv import YahooDataQueryService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def get_yahoo_data_for_tickers(tickers: List[str], query_service: YahooDataQueryService) -> List[Dict[str, Any]]:
    """
    Fetches a comprehensive set of data for a list of tickers using the YahooDataQueryService.
    This function is designed to be called by the LLM analytics endpoint.

    Args:
        tickers: A list of ticker symbols (e.g., ["AAPL", "MSFT"]).
        query_service: An instance of the YahooDataQueryService.

    Returns:
        A list of dictionaries, where each dictionary contains the profile,
        financials (income, balance, cash flow), and news for a ticker.
        Returns an empty list if no data can be fetched.
    """
    if not tickers:
        return []

    logger.info(f"Starting data fetch for LLM prompt. Tickers: {tickers}")
    all_tickers_data = []

    async def fetch_data_for_one_ticker(ticker: str, query_service: YahooDataQueryService) -> Optional[Dict[str, Any]]:
        """Fetches all necessary data points for a single ticker."""
        logger.info(f"Fetching all data for ticker: {ticker}")
        try:
            # Using asyncio.gather to run all data fetching concurrently.
            profile_task = query_service.get_ticker_profile(ticker)
            
            # Fetch latest TTM income statement and cash flow
            income_task = query_service.get_data_items(ticker, "INCOME_STATEMENT", "TTM", limit=1)
            cash_flow_task = query_service.get_data_items(ticker, "CASH_FLOW_STATEMENT", "TTM", limit=1)
            
            # Fetch both annual and quarterly balance sheets to find the latest one.
            balance_sheet_annual_task = query_service.get_data_items(ticker, "BALANCE_SHEET", "FYEAR", limit=1)
            balance_sheet_quarterly_task = query_service.get_data_items(ticker, "BALANCE_SHEET", "QUARTER", limit=1)

            # Fetch other supporting data
            analyst_targets_task = query_service.get_data_items(ticker, "ANALYST_PRICE_TARGETS", "CUMULATIVE_SNAPSHOT", limit=1)
            forecast_summary_task = query_service.get_data_items(ticker, "FORECAST_SUMMARY", "CUMULATIVE", limit=1)

            results = await asyncio.gather(
                profile_task,
                income_task,
                cash_flow_task,
                balance_sheet_annual_task,
                balance_sheet_quarterly_task,
                analyst_targets_task,
                forecast_summary_task,
                return_exceptions=True  # Prevent one failure from stopping others
            )
            
            # Unpack results
            (
                profile,
                income_list,
                cash_flow_list,
                balance_sheet_annual_list,
                balance_sheet_quarterly_list,
                analyst_targets_list,
                forecast_summary_list,
            ) = results

            # Helper to safely extract the first item from a list result
            def get_first_item(result_list):
                if isinstance(result_list, list) and result_list:
                    return result_list[0]
                if isinstance(result_list, Exception):
                     logger.error(f"Error fetching data item: {result_list}")
                return None

            income_statement = get_first_item(income_list)
            cash_flow = get_first_item(cash_flow_list)
            analyst_targets = get_first_item(analyst_targets_list)
            forecast_summary = get_first_item(forecast_summary_list)

            # Determine the latest balance sheet to use
            balance_sheet_annual = get_first_item(balance_sheet_annual_list)
            balance_sheet_quarterly = get_first_item(balance_sheet_quarterly_list)
            
            latest_balance_sheet = None
            if balance_sheet_annual and balance_sheet_quarterly:
                # Both are available, compare their key dates
                try:
                    date_annual = balance_sheet_annual.get('item_key_date')
                    date_quarterly = balance_sheet_quarterly.get('item_key_date')
                    if date_annual and date_quarterly:
                        if date_quarterly > date_annual:
                            latest_balance_sheet = balance_sheet_quarterly
                            logger.info(f"For {ticker}, using quarterly balance sheet dated {date_quarterly} (newer).")
                        else:
                            latest_balance_sheet = balance_sheet_annual
                            logger.info(f"For {ticker}, using annual balance sheet dated {date_annual} (newer or same).")
                    else:
                        # Fallback if a date is missing, prefer quarterly as it's often more recent
                        latest_balance_sheet = balance_sheet_quarterly or balance_sheet_annual
                except Exception as e:
                        logger.error(f"Error comparing balance sheet dates for {ticker}: {e}")
                        latest_balance_sheet = balance_sheet_quarterly or balance_sheet_annual # Fallback
            elif balance_sheet_quarterly:
                latest_balance_sheet = balance_sheet_quarterly
                logger.info(f"For {ticker}, using quarterly balance sheet (annual not found).")
            elif balance_sheet_annual:
                latest_balance_sheet = balance_sheet_annual
                logger.info(f"For {ticker}, using annual balance sheet (quarterly not found).")

            # Check for and log any errors
            if isinstance(profile, Exception):
                logger.error(f"Error fetching profile for {ticker}: {profile}")
                profile = None

            if not profile:
                logger.warning(f"Could not retrieve mandatory profile for {ticker}. Skipping ticker.")
                return None

            return {
                "profile": profile,
                "income_statement_ttm": income_statement,
                "balance_sheet_latest": latest_balance_sheet,
                "cash_flow_ttm": cash_flow,
                "analyst_price_targets": analyst_targets,
                "forecast_summary": forecast_summary
            }

        except Exception as e:
            logger.error(f"An unexpected error occurred while fetching data for {ticker}: {e}", exc_info=True)
            return None

    # Run all ticker fetches concurrently
    fetch_tasks = [fetch_data_for_one_ticker(ticker, query_service) for ticker in tickers]
    results = await asyncio.gather(*fetch_tasks)

    # Filter out any None results (from tickers with no data)
    all_tickers_data = [res for res in results if res is not None]

    logger.info(f"Finished data fetch. Found data for {len(all_tickers_data)} out of {len(tickers)} requested tickers.")
    return all_tickers_data 