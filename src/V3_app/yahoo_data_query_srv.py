# src/V3_app/yahoo_data_query_srv.py
from datetime import datetime
from typing import Dict, Any, List, Optional, Union, Tuple
import json

from .yahoo_repository import YahooDataRepository
# from .yahoo_models import YahooTickerMasterModel # For type hinting if returning raw models

import logging
logger = logging.getLogger(__name__)

# This new mapping is based on:
# 1. The `output_key` (3rd element) from `TARGET_ITEM_TYPES` in `V3_backend_api.py`, which forms
#    the middle part of the `field_identifier` (e.g., 'balance_sheet_annual').
# 2. The actual `item_type` and `item_time_coverage` strings used when data is stored
#    in the database by functions in `V3_yahoo_fetch.py`.
# Format: 'output_key_from_field_identifier': ('DB_ITEM_TYPE', 'DB_ITEM_TIME_COVERAGE')
OUTPUT_KEY_TO_DB_MAPPING = {
    'analyst_price_targets': ('ANALYST_PRICE_TARGETS', 'CUMULATIVE_SNAPSHOT'),
    'forecast_summary': ('FORECAST_SUMMARY', 'CUMULATIVE'),
    'balance_sheet_annual': ('BALANCE_SHEET', 'FYEAR'),
    'income_statement_annual': ('INCOME_STATEMENT', 'FYEAR'),
    'cash_flow_annual': ('CASH_FLOW_STATEMENT', 'FYEAR'),
    'balance_sheet_quarterly': ('BALANCE_SHEET', 'QUARTER'),
    'income_statement_quarterly': ('INCOME_STATEMENT', 'QUARTER'),
    'cash_flow_quarterly': ('CASH_FLOW_STATEMENT', 'QUARTER'),
    'income_statement_ttm': ('INCOME_STATEMENT', 'TTM'),
    'cash_flow_ttm': ('CASH_FLOW_STATEMENT', 'TTM'),
    # Add other mappings here if the "Fundamentals History" field selector can generate
    # field_identifiers for other data types not explicitly in TARGET_ITEM_TYPES
    # but following the same naming pattern. For example:
    # 'dividend_history_cumulative': ('DIVIDEND_HISTORY', 'CUMULATIVE'),
    # 'earnings_estimates_cumulative': ('EARNINGS_ESTIMATE_HISTORY', 'CUMULATIVE'), # Correct DB type
}
# Removed old KNOWN_COVERAGES_IN_IDENTIFIER, COVERAGE_LOGICAL_TO_DB, LOGICAL_ITEM_TYPE_TO_DB
# as they are superseded by the direct OUTPUT_KEY_TO_DB_MAPPING.

class YahooDataQueryService:
    def __init__(self, db_repo: YahooDataRepository):
        self.db_repo = db_repo

    async def get_ticker_profile(self, ticker_symbol: str) -> Optional[Dict[str, Any]]:
        # ... existing code ...
        return await self.db_repo.get_yahoo_ticker_master_by_ticker(ticker_symbol)

    async def get_multiple_ticker_profiles(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        # ... existing code ...
        return await self.db_repo.get_filtered_yahoo_ticker_profiles(filters)

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
        # ... existing code ...
        return await self.db_repo.get_data_items_by_criteria(
            ticker=ticker,
            item_type=item_type,
            item_time_coverage=item_time_coverage,
            key_date=key_date,
            start_date=start_date,
            end_date=end_date,
            order_by_key_date_desc=order_by_key_date_desc,
            limit=limit
        )

    async def get_latest_data_item_payload(
        self, 
        ticker: str, 
        item_type: str, # This is expected to be lowercase like 'balance_sheet'
        item_time_coverage: str # This is expected to be UPPERCASE like 'FYEAR'
    ) -> Optional[Dict[str, Any]]:
        # The item_type for query should be uppercase as stored in DB
        db_item_type = item_type.upper()
        
        logger.debug(f"Querying latest item for {ticker}, type: {db_item_type}, coverage: {item_time_coverage}")
        
        items = await self.db_repo.get_data_items_by_criteria(
            ticker=ticker,
            item_type=db_item_type, # Use uppercase for DB query
            item_time_coverage=item_time_coverage,
            order_by_key_date_desc=True,
            limit=1
        )
        if items:
            payload = items[0].get('item_data_payload')
            if isinstance(payload, str): # Assuming payload from DB is JSON string
                try:
                    return json.loads(payload)
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON payload for {ticker}, {db_item_type}, {item_time_coverage}")
                    return None
            elif isinstance(payload, dict): # If already a dict (e.g., if ORM handles it)
                return payload
            else:
                logger.warning(f"Payload for {ticker}, {db_item_type}, {item_time_coverage} is not a string or dict. Type: {type(payload)}")
                return None
        return None

    async def get_latest_analyst_price_targets(self, ticker: str) -> Optional[Dict[str, Any]]:
        return await self.get_latest_data_item_payload(ticker, "ANALYST_PRICE_TARGETS", "CUMULATIVE_SNAPSHOT")

    async def get_latest_dividend_history(self, ticker: str) -> Optional[Dict[str, Any]]:
        return await self.get_latest_data_item_payload(ticker, "DIVIDEND_HISTORY", "CUMULATIVE")

    async def get_latest_earnings_estimate_history(self, ticker: str) -> Optional[Dict[str, Any]]:
        # Use the correct DB item_type string
        return await self.get_latest_data_item_payload(ticker, "EARNINGS_ESTIMATE_HISTORY", "CUMULATIVE")

    async def get_latest_forecast_summary(self, ticker: str) -> Optional[Dict[str, Any]]:
        return await self.get_latest_data_item_payload(ticker, "FORECAST_SUMMARY", "CUMULATIVE")

    async def get_specific_field_timeseries(
        self,
        field_identifier: str, # e.g., "yf_item_balance_sheet_annual_Total Assets"
        tickers: Union[str, List[str]],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        tickers_list = [tickers] if isinstance(tickers, str) else tickers

        # --- Start of New Parsing Logic ---
        db_item_type: Optional[str] = None
        db_item_coverage: Optional[str] = None
        payload_key_for_json: Optional[str] = None

        if not field_identifier.startswith("yf_item_"):
            logger.error(f"Invalid field_identifier format (must start with 'yf_item_'): {field_identifier}")
            return results_by_ticker # Return empty if format is wrong

        # Remove prefix "yf_item_"
        identifier_core = field_identifier[len("yf_item_"):] # e.g., "balance_sheet_annual_Total Assets"
        
        matched_output_key_from_map = None
        for output_key_candidate in OUTPUT_KEY_TO_DB_MAPPING.keys():
            if identifier_core.startswith(output_key_candidate):
                # Ensure it's not a partial match for a shorter key
                # e.g. 'cash_flow' shouldn't match 'cash_flow_annual_...' if 'cash_flow_annual' is also a key
                # This is handled by finding the longest match if multiple keys are prefixes of each other.
                # For now, assume keys in OUTPUT_KEY_TO_DB_MAPPING are distinct enough or this implies choosing the first/longest.
                # To be robust, find the longest matching key:
                if matched_output_key_from_map is None or len(output_key_candidate) > len(matched_output_key_from_map):
                    # Check if the character immediately after the candidate match is '_' or end of string
                    if len(identifier_core) == len(output_key_candidate) or identifier_core[len(output_key_candidate)] == '_':
                        matched_output_key_from_map = output_key_candidate
        
        if matched_output_key_from_map:
            db_item_type, db_item_coverage = OUTPUT_KEY_TO_DB_MAPPING[matched_output_key_from_map]
            
            # The payload key starts right after the matched output_key and the following underscore
            # If identifier_core is "balance_sheet_annual_Total Assets" and matched_output_key_from_map is "balance_sheet_annual"
            # payload_key_start_index is len("balance_sheet_annual") + 1
            if len(identifier_core) > len(matched_output_key_from_map) and identifier_core[len(matched_output_key_from_map)] == '_':
                payload_key_for_json = identifier_core[len(matched_output_key_from_map) + 1:]
            elif len(identifier_core) == len(matched_output_key_from_map): # Case where there's no payload key (e.g. if a field_identifier was just yf_item_forecast_summary)
                payload_key_for_json = None # Or handle as an error if payload key is always expected
                logger.warning(f"Field identifier {field_identifier} seems to match an output key '{matched_output_key_from_map}' perfectly, implying no specific payload sub-key. This might not be supported for timeseries queries that expect a sub-key.")
            else: # Should not happen if startsWith and character check was done correctly
                 logger.error(f"Mismatch after finding output key '{matched_output_key_from_map}' in '{identifier_core}' for {field_identifier}. This indicates a parsing logic error.")
                 return results_by_ticker


            logger.info(f"Parsed field_identifier: {field_identifier}")
            logger.info(f"  -> Matched output_key: {matched_output_key_from_map}")
            logger.info(f"  -> DB item_type: {db_item_type}")
            logger.info(f"  -> DB item_coverage: {db_item_coverage}")
            logger.info(f"  -> Payload key for JSON: {payload_key_for_json}")
        else:
            logger.error(f"Could not parse field_identifier: {field_identifier} using OUTPUT_KEY_TO_DB_MAPPING. No matching output_key found.")
            return results_by_ticker # Return empty if parsing failed

        if not db_item_type or not db_item_coverage or payload_key_for_json is None:
            logger.error(f"Parsing resulted in missing critical info for {field_identifier}: db_item_type='{db_item_type}', db_item_coverage='{db_item_coverage}', payload_key_for_json='{payload_key_for_json}'. Cannot proceed.")
            if payload_key_for_json is None and matched_output_key_from_map and len(identifier_core) == len(matched_output_key_from_map) :
                 logger.error(f"This typically means the field '{field_identifier}' refers to a whole data structure, not a specific timeseries value within it.")
            return results_by_ticker
        # --- End of New Parsing Logic ---

        start_date_obj: Optional[datetime] = None
        end_date_obj: Optional[datetime] = None
        if start_date_str:
            try:
                start_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d")
            except ValueError:
                logger.warning(f"Invalid start_date format: {start_date_str}. Proceeding without start_date filter.")
        if end_date_str:
            try:
                end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d")
            except ValueError:
                logger.warning(f"Invalid end_date format: {end_date_str}. Proceeding without end_date filter.")
        
        for ticker_symbol in tickers_list:
            current_ticker_series: List[Dict[str, Any]] = []
            try:
                logger.debug(f"Calling db_repo.get_data_items_by_criteria for {ticker_symbol} with:")
                logger.debug(f"  item_type: {db_item_type}")
                logger.debug(f"  item_time_coverage: {db_item_coverage}")
                logger.debug(f"  start_date: {start_date_obj}")
                logger.debug(f"  end_date: {end_date_obj}")
                logger.debug(f"  order_by_key_date_desc: False")

                data_items = await self.db_repo.get_data_items_by_criteria(
                    ticker=ticker_symbol,
                    item_type=db_item_type, # Use parsed DB item type
                    item_time_coverage=db_item_coverage, # Use parsed DB coverage
                    start_date=start_date_obj,
                    end_date=end_date_obj,
                    order_by_key_date_desc=False # Fetch in ascending order for timeseries
                )
                logger.debug(f"Found {len(data_items)} data items from DB for {ticker_symbol}, type {db_item_type}, coverage {db_item_coverage}.")
                # Log raw items for debugging ONE ticker and ONE field_id
                if tickers_list.index(ticker_symbol) == 0 : # Log only for the first ticker in list
                    logger.debug(f"Raw data_items for {ticker_symbol}, {field_identifier}: {str(data_items)[:1000]}...")


                for item in data_items:
                    payload_data = item.get('item_data_payload') # This is already a dict if ORM/repo handles JSON loading
                    item_key_date_from_db = item.get('item_key_date') 

                    if not payload_data or not item_key_date_from_db:
                        logger.warning(f"Skipping item for {ticker_symbol} due to missing payload or key_date: item_id={item.get('id')}")
                        continue
                    
                    item_key_date_dt: Optional[datetime] = None
                    if isinstance(item_key_date_from_db, str):
                        try: # ISO format with T and microseconds
                            item_key_date_dt = datetime.strptime(item_key_date_from_db, "%Y-%m-%dT%H:%M:%S.%f")
                        except ValueError:
                            try: # ISO format with T, no microseconds
                                item_key_date_dt = datetime.strptime(item_key_date_from_db, "%Y-%m-%dT%H:%M:%S")
                            except ValueError:
                                try: # Space separator with microseconds
                                    item_key_date_dt = datetime.strptime(item_key_date_from_db, "%Y-%m-%d %H:%M:%S.%f")
                                except ValueError:
                                    try: # Space separator, no microseconds
                                        item_key_date_dt = datetime.strptime(item_key_date_from_db, "%Y-%m-%d %H:%M:%S")
                                    except ValueError:
                                        try: # Date only
                                            item_key_date_dt = datetime.strptime(item_key_date_from_db, "%Y-%m-%d")
                                        except ValueError:
                                            logger.warning(f"Could not parse date string '{item_key_date_from_db}' for item_id={item.get('id')}: unconverted data remains. Skipping.")
                                            continue
                    elif isinstance(item_key_date_from_db, datetime):
                        item_key_date_dt = item_key_date_from_db
                    else:
                        logger.warning(f"item_key_date is of unexpected type: {type(item_key_date_from_db)}. Skipping item_id={item.get('id')}")
                        continue
                    
                    item_key_date_iso_str = item_key_date_dt.strftime("%Y-%m-%d")

                    if not isinstance(payload_data, dict):
                        # This might happen if the payload from DB is a string and needs json.loads
                        # However, YahooDataRepository.get_data_items_by_criteria is expected
                        # to handle the item_data_payload conversion from JSON string to dict.
                        # If it's still a string here, it means that conversion might have failed or was skipped.
                        if isinstance(payload_data, str):
                            logger.warning(f"Payload for {ticker_symbol}, item_id={item.get('id')} on {item_key_date_iso_str} is a string. Attempting to parse as JSON.")
                            try:
                                payload_data = json.loads(payload_data)
                            except json.JSONDecodeError:
                                logger.error(f"Failed to parse JSON string payload for item_id={item.get('id')}. Payload: {str(payload_data)[:200]}. Skipping.")
                                continue
                        else:
                            logger.warning(f"Payload for {ticker_symbol}, item_id={item.get('id')} on {item_key_date_iso_str} is not a dict or string. Type: {type(payload_data)}. Data: {str(payload_data)[:200]}. Skipping.")
                            continue
                    
                    # Now payload_data should be a dictionary
                    value = payload_data.get(payload_key_for_json) # Use parsed payload_key_for_json
                    if value is not None:
                        current_ticker_series.append({'date': item_key_date_iso_str, 'value': value})
                        logger.debug(f"Found key '{payload_key_for_json}' in dict payload for {ticker_symbol} on {item_key_date_iso_str} with value: {value}")
                    else:
                        logger.debug(f"Key '{payload_key_for_json}' not found in payload for {ticker_symbol} on {item_key_date_iso_str}. Payload keys: {list(payload_data.keys())}")
                                
            except Exception as e:
                logger.error(f"Error processing data for ticker {ticker_symbol}, field {field_identifier}: {e}", exc_info=True)
            
            results_by_ticker[ticker_symbol] = current_ticker_series
            logger.info(f"Collected {len(current_ticker_series)} data points for {ticker_symbol} and field {field_identifier} (parsed as type='{db_item_type}', coverage='{db_item_coverage}', key='{payload_key_for_json}').")

        return results_by_ticker

    # You can add more specific wrappers here for other TTM data or single-record items
    # For example:
    # async def get_latest_ttm_income_statement(self, ticker: str) -> Optional[Dict[str, Any]]:
    #     return await self.get_latest_data_item_payload(ticker, "INCOME_STATEMENT", "TTM")
    # 
    # async def get_latest_ttm_cash_flow_statement(self, ticker: str) -> Optional[Dict[str, Any]]:
    #     return await self.get_latest_data_item_payload(ticker, "CASH_FLOW_STATEMENT", "TTM") 