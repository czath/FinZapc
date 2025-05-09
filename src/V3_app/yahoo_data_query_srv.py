# src/V3_app/yahoo_data_query_srv.py
from datetime import datetime
from typing import Dict, Any, List, Optional

from .yahoo_repository import YahooDataRepository
# from .yahoo_models import YahooTickerMasterModel # For type hinting if returning raw models

import logging
logger = logging.getLogger(__name__)

class YahooDataQueryService:
    def __init__(self, db_repo: YahooDataRepository):
        self.db_repo = db_repo
        logger.info("YahooDataQueryService initialized.")

    async def get_ticker_profile(self, ticker_symbol: str) -> Optional[Dict[str, Any]]:
        logger.debug(f"Query Service: Requesting profile for {ticker_symbol}")
        try:
            profile_data = await self.db_repo.get_ticker_master_by_ticker(ticker_symbol)
            if profile_data:
                logger.info(f"Query Service: Profile found for {ticker_symbol}.")
            else:
                logger.info(f"Query Service: No profile found for {ticker_symbol}.")
            return profile_data
        except Exception as e:
            logger.error(f"Query Service: Error getting profile for {ticker_symbol}: {e}", exc_info=True)
            return None 

    async def get_multiple_ticker_profiles(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Retrieves a list of ticker master profiles based on specified filters.
        'filters' is a dictionary where keys are column names (e.g., 'sector', 'country') 
        and values are the desired filter values.
        Example: filters={'sector': 'Technology', 'country': 'USA'}
        Returns a list of dictionaries, each representing a ticker_master record.
        """
        logger.debug(f"Query Service: Requesting multiple profiles with filters: {filters}")
        try:
            profiles = await self.db_repo.get_ticker_masters_by_criteria(filters)
            logger.info(f"Query Service: Found {len(profiles)} profiles matching criteria.")
            return profiles
        except Exception as e:
            logger.error(f"Query Service: Error getting multiple profiles: {e}", exc_info=True)
            return [] # Return empty list on error

    async def get_data_items(
        self, 
        ticker: str, 
        item_type: str, 
        item_time_coverage: Optional[str] = None,
        key_date: Optional[datetime] = None,
        start_date: Optional[datetime] = None, 
        end_date: Optional[datetime] = None,
        order_by_key_date_desc: bool = True,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieves a list of data items (full records including parsed payload) 
        based on various criteria.
        """
        logger.debug(f"Query Service: Requesting data items for {ticker}, type: {item_type}, coverage: {item_time_coverage}, etc.")
        try:
            items = await self.db_repo.get_data_items_by_criteria(
                ticker=ticker,
                item_type=item_type,
                item_time_coverage=item_time_coverage,
                key_date=key_date,
                start_date=start_date,
                end_date=end_date,
                order_by_key_date_desc=order_by_key_date_desc,
                limit=limit
            )
            logger.info(f"Query Service: Found {len(items)} data items matching criteria for {ticker}/{item_type}.")
            return items
        except Exception as e:
            logger.error(f"Query Service: Error getting data items for {ticker}/{item_type}: {e}", exc_info=True)
            return [] # Return empty list on error

    # --- Convenience wrappers for specific latest item types will follow ---
    async def get_latest_data_item_payload(
        self, 
        ticker: str, 
        item_type: str, 
        item_time_coverage: str
    ) -> Optional[Dict[str, Any]]:
        """
        Convenience wrapper to get the payload of the most recent data item.
        """
        logger.debug(f"Query Service: Requesting latest payload for {ticker}/{item_type}/{item_time_coverage}")
        try:
            # This directly uses the existing repository method
            payload = await self.db_repo.get_latest_item_payload(ticker, item_type, item_time_coverage)
            if payload:
                logger.info(f"Query Service: Latest payload found for {ticker}/{item_type}/{item_time_coverage}.")
            else:
                logger.info(f"Query Service: No latest payload found for {ticker}/{item_type}/{item_time_coverage}.")
            return payload
        except Exception as e:
            logger.error(f"Query Service: Error getting latest payload for {ticker}/{item_type}/{item_time_coverage}: {e}", exc_info=True)
            return None 

    async def get_latest_analyst_price_targets(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetches the latest analyst price targets summary payload."""
        # Assuming "CUMULATIVE_SNAPSHOT" or the correct coverage type used during storage
        return await self.get_latest_data_item_payload(ticker, "ANALYST_PRICE_TARGETS", "CUMULATIVE_SNAPSHOT")

    async def get_latest_dividend_history(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetches the latest dividend history payload."""
        return await self.get_latest_data_item_payload(ticker, "DIVIDEND_HISTORY", "CUMULATIVE")

    async def get_latest_earnings_estimate_history(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetches the latest earnings estimate history payload."""
        return await self.get_latest_data_item_payload(ticker, "EARNINGS_ESTIMATE_HISTORY", "CUMULATIVE")

    async def get_latest_forecast_summary(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetches the latest forecast summary payload."""
        return await self.get_latest_data_item_payload(ticker, "FORECAST_SUMMARY", "CUMULATIVE")

    # You can add more specific wrappers here for other TTM data or single-record items
    # For example:
    # async def get_latest_ttm_income_statement(self, ticker: str) -> Optional[Dict[str, Any]]:
    #     return await self.get_latest_data_item_payload(ticker, "INCOME_STATEMENT", "TTM")
    # 
    # async def get_latest_ttm_cash_flow_statement(self, ticker: str) -> Optional[Dict[str, Any]]:
    #     return await self.get_latest_data_item_payload(ticker, "CASH_FLOW_STATEMENT", "TTM") 