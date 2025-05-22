# src/V3_app/yahoo_data_query_srv.py
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union, Tuple
import json
import httpx
import asyncio
from concurrent.futures import ThreadPoolExecutor
import pandas as pd  # Add pandas import

from .yahoo_repository import YahooDataRepository
from .currency_utils import get_current_exchange_rate
from .price_cache import price_cache  # Add this import at the top with other imports
import yfinance as yf  # Add this import at the top

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set to DEBUG level for this module

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
        """Initialize the service with a repository instance."""
        self.db_repo = db_repo
        # Initialize the advanced service with self as the base service
        from .yahoo_data_query_adv import YahooDataQueryAdvService
        self.adv_service = YahooDataQueryAdvService(db_repo=db_repo, base_query_srv=self)
        logger.info("YahooDataQueryService initialized")

    async def _get_conversion_info_for_ticker(
        self, 
        ticker_symbol: str, 
        ticker_profiles_cache: Dict[str, Dict[str, Any]]
    ) -> Optional[Tuple[str, str, float]]:
        """
        Retrieves currency information and exchange rate if conversion is needed.
        Returns (trade_currency, financial_currency, rate_to_convert_to_trade_currency)
        or None if no conversion is needed/possible.
        Uses/populates ticker_profiles_cache to avoid redundant DB calls for master data.
        """
        profile = ticker_profiles_cache.get(ticker_symbol)
        if not profile:
            profile = await self.db_repo.get_ticker_master_by_ticker(ticker_symbol)
            if profile:
                ticker_profiles_cache[ticker_symbol] = profile
            else:
                logger.warning(f"[QuerySrv._get_conversion_info] Profile not found for {ticker_symbol}. Cannot determine currencies.")
                return None

        trade_currency = profile.get("trade_currency")
        financial_currency = profile.get("financial_currency")

        if not trade_currency or not financial_currency:
            logger.warning(f"[QuerySrv._get_conversion_info] Missing trade or financial currency for {ticker_symbol}. Trade: {trade_currency}, Financial: {financial_currency}")
            return None

        if trade_currency.upper() == financial_currency.upper():
            logger.debug(f"[QuerySrv._get_conversion_info] {ticker_symbol}: Trade ({trade_currency}) and Financial ({financial_currency}) currencies are the same. No conversion needed.")
            return None # No conversion needed

        # Currencies differ, fetch exchange rate
        # Rate to multiply by financial_currency_value to get trade_currency_value
        exchange_rate = await get_current_exchange_rate(financial_currency, trade_currency)

        if exchange_rate is None:
            logger.error(f"[QuerySrv._get_conversion_info] {ticker_symbol}: Failed to get exchange rate from {financial_currency} to {trade_currency}.")
            return None # Conversion needed but rate not available
        
        logger.info(f"[QuerySrv._get_conversion_info] {ticker_symbol}: Conversion needed. From {financial_currency} to {trade_currency}. Rate: {exchange_rate}")
        return trade_currency, financial_currency, exchange_rate

    @staticmethod
    def _convert_camel_to_spaced_human(name: str) -> str:
        if not name: return ""
        result = [name[0]]
        for i in range(1, len(name)):
            char = name[i]
            prev_char = name[i-1]
            if char.isupper():
                if prev_char.islower():
                    result.append(' ')
                # Handles cases like "ABCWord" -> "ABC Word" or "NetPPE" -> "Net PPE"
                # If prev_char is Upper, and current char (char) is Upper,
                # but next char is Lower, then char starts a new word.
                elif prev_char.isupper():
                    if (i + 1 < len(name)) and name[i+1].islower():
                        result.append(' ')
            result.append(char)
        return "".join(result)

    async def _apply_currency_conversion_to_payload(
        self, 
        data_payload: Dict[str, Any], 
        exchange_rate: float,
        original_financial_currency: str, # For logging/metadata
        target_trade_currency: str,       # For logging/metadata
        item_type: str                    # NEW: To determine if conversion is applicable
    ) -> Dict[str, Any]:
        """
        Converts numeric values in the data_payload using the exchange_rate,
        subject to item_type and keyword ("shares") exceptions.
        """
        if not isinstance(data_payload, dict):
            logger.warning("[QuerySrv._apply_conversion] Payload is not a dict, cannot convert.")
            return data_payload

        CONVERTIBLE_ITEM_TYPES = {"BALANCE_SHEET", "INCOME_STATEMENT", "CASH_FLOW_STATEMENT"}
        # logger.debug(f"[QuerySrv._apply_conversion] Received item_type: {item_type.upper()} for payload: {list(data_payload.keys())[:5]}...")

        if item_type.upper() not in CONVERTIBLE_ITEM_TYPES:
            logger.debug(f"[QuerySrv._apply_conversion] Item type '{item_type.upper()}' does not require currency conversion. Skipping payload.")
            return data_payload

        converted_payload = {}
        converted_fields_count = 0
        for key, value in data_payload.items():
            if "shares" in key.lower(): # Check for 'shares' keyword (case-insensitive)
                converted_payload[key] = value # Do not convert if 'shares' is in the key
                # logger.debug(f"[QuerySrv._apply_conversion] Skipping conversion for key '{key}' due to 'shares' keyword.")
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                converted_value = value * exchange_rate
                converted_payload[key] = converted_value
                # logger.debug(f"[QuerySrv._apply_conversion] Converted '{key}': {value} ({original_financial_currency}) -> {converted_value} ({target_trade_currency})")
                converted_fields_count +=1
            else:
                converted_payload[key] = value # Keep non-numeric, non-convertible, or boolean values as is
        
        if converted_fields_count > 0:
            logger.info(f"[QuerySrv._apply_conversion] Applied conversion to {converted_fields_count} numeric fields in payload for item_type '{item_type.upper()}'. From {original_financial_currency} to {target_trade_currency} using rate {exchange_rate}.")
        elif item_type.upper() in CONVERTIBLE_ITEM_TYPES: # Log if it was a convertible type but nothing changed (e.g. all shares or no numerics)
            logger.info(f"[QuerySrv._apply_conversion] No fields were converted for item_type '{item_type.upper()}' (e.g., all fields contained 'shares', were non-numeric, or payload was empty).")

        return converted_payload

    async def get_ticker_profile(self, ticker_symbol: str) -> Optional[Dict[str, Any]]:
        # ... existing code ...
        # As per instructions, ticker_master fields are NOT converted.
        # This method primarily serves to provide currency info if needed by callers,
        # or for direct display where original values are expected.
        return await self.db_repo.get_ticker_master_by_ticker(ticker_symbol)

    async def get_multiple_ticker_profiles(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        # ... existing code ...
        # As per instructions, ticker_master fields are NOT converted.
        return await self.db_repo.get_ticker_masters_by_criteria(filters) # Corrected method name

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
        data_items_list = await self.db_repo.get_data_items_by_criteria(
            ticker=ticker,
            item_type=item_type,
            item_time_coverage=item_time_coverage,
            key_date=key_date,
            start_date=start_date,
            end_date=end_date,
            order_by_key_date_desc=order_by_key_date_desc,
            limit=limit
        )

        if not data_items_list:
            return []

        # Attempt to get conversion info for the ticker
        # For this method, ticker_profiles_cache can be an empty dict as it's single-ticker context
        conversion_info = await self._get_conversion_info_for_ticker(ticker, {})

        if conversion_info:
            trade_curr, fin_curr, rate = conversion_info
            processed_items = []
            for item in data_items_list:
                payload = item.get('item_data_payload')
                if isinstance(payload, dict):
                    # Create a copy to avoid modifying the original item dict directly if it's cached elsewhere
                    item_copy = item.copy()
                    item_copy['item_data_payload'] = await self._apply_currency_conversion_to_payload(
                        payload, rate, fin_curr, trade_curr, item_type
                    )
                    processed_items.append(item_copy)
                else:
                    processed_items.append(item) # Append as is if payload not a dict
            return processed_items
        
        return data_items_list # Return original if no conversion needed/possible

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
            # Payload should already be a dict due to get_data_items_by_criteria parsing JSON string
            # However, the conversion logic needs to be applied here too.

            conversion_info = await self._get_conversion_info_for_ticker(ticker, {})
            if conversion_info and isinstance(payload, dict):
                trade_curr, fin_curr, rate = conversion_info
                payload = await self._apply_currency_conversion_to_payload(
                    payload, rate, fin_curr, trade_curr, db_item_type
                )
            
            # Original JSON parsing logic (might be redundant if repo ensures dict, but safe)
            if isinstance(payload, str): 
                try:
                    return json.loads(payload)
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON payload for {ticker}, {db_item_type}, {item_time_coverage} AFTER potential conversion attempt")
                    return None
            elif isinstance(payload, dict): 
                return payload
            else:
                logger.warning(f"Payload for {ticker}, {db_item_type}, {item_time_coverage} is not a string or dict after potential conversion. Type: {type(payload)}")
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

    async def get_ticker_currencies(self, ticker_symbol: str) -> Optional[Dict[str, Optional[str]]]:
        """Retrieves trade_currency and financial_currency for a given ticker."""
        logger.debug(f"Fetching currencies for ticker: {ticker_symbol}")
        profile = await self.db_repo.get_ticker_master_by_ticker(ticker_symbol)
        if profile:
            return {
                "trade_currency": profile.get("trade_currency"),
                "financial_currency": profile.get("financial_currency")
            }
        logger.warning(f"No profile found for ticker {ticker_symbol} when fetching currencies.")
        return None

    async def get_specific_field_timeseries(
        self,
        field_identifier: str, # e.g., "yf_item_balance_sheet_annual_Total Assets"
        tickers: Union[str, List[str]],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        tickers_list = [tickers] if isinstance(tickers, str) else tickers

        # Cache for ticker master profiles to avoid re-fetching for currency info within this request
        ticker_profiles_cache: Dict[str, Dict[str, Any]] = {}

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
            logger.info(f"  -> Raw payload key from identifier: {payload_key_for_json}") # Log raw key
        else:
            logger.error(f"Could not parse field_identifier: {field_identifier} using OUTPUT_KEY_TO_DB_MAPPING. No matching output_key found.")
            return results_by_ticker # Return empty if parsing failed

        if not db_item_type or not db_item_coverage or payload_key_for_json is None:
            logger.error(f"Parsing resulted in missing critical info for {field_identifier}: db_item_type='{db_item_type}', db_item_coverage='{db_item_coverage}', raw_payload_key_for_json='{payload_key_for_json}'. Cannot proceed.")
            if payload_key_for_json is None and matched_output_key_from_map and len(identifier_core) == len(matched_output_key_from_map) :
                 logger.error(f"This typically means the field '{field_identifier}' refers to a whole data structure, not a specific timeseries value within it.")
            return results_by_ticker
        # --- End of New Parsing Logic ---

        # Convert the raw payload key (e.g., "TotalAssets") to the spaced key (e.g., "Total Assets") for lookup
        actual_payload_lookup_key = YahooDataQueryService._convert_camel_to_spaced_human(payload_key_for_json)
        logger.info(f"  -> Attempting lookup with spaced key: '{actual_payload_lookup_key}'")

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

                # Get conversion info for the current ticker_symbol once
                conversion_info = await self._get_conversion_info_for_ticker(ticker_symbol, ticker_profiles_cache)
                rate_to_apply = None
                original_fin_curr_for_log = None
                target_trade_curr_for_log = None

                if conversion_info:
                    target_trade_curr_for_log, original_fin_curr_for_log, rate_to_apply = conversion_info
                    logger.info(f"Conversion info for {ticker_symbol}: Rate {rate_to_apply} from {original_fin_curr_for_log} to {target_trade_curr_for_log} for INCOME_STATEMENT items.")
                else:
                    logger.info(f"No currency conversion needed or possible for {ticker_symbol} for INCOME_STATEMENT items.")

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
                    value = payload_data.get(actual_payload_lookup_key) # Use the converted spaced key for lookup
                    
                    # --- NEW: Attempt to match key with spaces if direct match fails ---
                    # This block is now removed as per user instruction to use the converted key directly.
                    # The _convert_camel_to_spaced_human handles the transformation.
                    # if value is None:
                        # ... old logic for trying spaced_key / title_cased_key ...
                    # --- END: Attempt to match key with spaces ---
                    
                    if value is not None:
                        # Apply conversion if needed and possible, and if the value is numeric
                        if rate_to_apply is not None and isinstance(value, (int, float)) and not isinstance(value, bool):
                            # Check item_type and 'shares' keyword before converting
                            CONVERTIBLE_ITEM_TYPES = {"BALANCE_SHEET", "INCOME_STATEMENT", "CASH_FLOW_STATEMENT"}
                            # Use actual_payload_lookup_key for 'shares' check as it's the key present in payload
                            if db_item_type.upper() in CONVERTIBLE_ITEM_TYPES and "shares" not in actual_payload_lookup_key.lower():
                                original_value = value
                                value = value * rate_to_apply
                                logger.debug(f"[QuerySrv.get_specific_field_timeseries] Converted value for {ticker_symbol}, field '{actual_payload_lookup_key}', date {item_key_date_iso_str}: {original_value} -> {value} using rate {rate_to_apply}")
                            # else: // Value not converted due to item_type or 'shares' keyword
                                # logger.debug(f"[QuerySrv.get_specific_field_timeseries] SKIPPED conversion for {ticker_symbol}, field '{actual_payload_lookup_key}', date {item_key_date_iso_str}: item_type={db_item_type}, rate={rate_to_apply}")
                        
                        current_ticker_series.append({'date': item_key_date_iso_str, 'value': value})
                        # logger.debug(f"Found key '{actual_payload_lookup_key}' in dict payload for {ticker_symbol} on {item_key_date_iso_str} with value: {value}") # Redundant if conversion log is active
                    else:
                        logger.warning(f"Key '{actual_payload_lookup_key}' (derived from '{payload_key_for_json}') not found or value is None in payload for {ticker_symbol} on {item_key_date_iso_str}. Payload keys: {list(payload_data.keys()) if isinstance(payload_data, dict) else 'Payload not a dict'}")
                                
            except Exception as e:
                logger.error(f"Error processing data for ticker {ticker_symbol}, field {field_identifier}: {e}", exc_info=True)
            
            results_by_ticker[ticker_symbol] = current_ticker_series
            logger.info(f"Collected {len(current_ticker_series)} data points for {ticker_symbol} and field {field_identifier} (parsed as type='{db_item_type}', coverage='{db_item_coverage}', key='{actual_payload_lookup_key}').")

        return results_by_ticker

    async def calculate_synthetic_fundamental_timeseries(
        self,
        fundamental_name: str,
        tickers: List[str],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        logger.info(f"[QuerySrv.calculate_synthetic_fundamental_timeseries] Request for {fundamental_name}, Tickers: {tickers}, Start: {start_date_str}, End: {end_date_str}")
        
        # Map fundamental_name to the appropriate calculation method
        supported_fundamentals = {
            "EPS_TTM": self._calculate_eps_ttm_for_tickers,
            "CASH_PER_SHARE": self._calculate_cash_per_share_for_tickers,
            "CASH_PLUS_ST_INV_PER_SHARE": self._calculate_cash_plus_st_inv_per_share_for_tickers,
            "BOOK_VALUE_PER_SHARE": self._calculate_book_value_per_share_for_tickers,
            "PRICE_TO_BOOK_VALUE": self._calculate_price_to_book_value_for_tickers,
            "PRICE_TO_CASH_PLUS_ST_INV": self._calculate_price_to_cash_plus_st_inv_for_tickers,
            "DEBT_TO_EQUITY": self.adv_service.calculate_debt_to_equity_for_tickers,
            "TOTAL_LIABILITIES_TO_EQUITY": self.adv_service.calculate_total_liabilities_to_equity_for_tickers,
            "TOTAL_LIABILITIES_TO_ASSETS": self.adv_service.calculate_total_liabilities_to_assets_for_tickers,
            "DEBT_TO_ASSETS": self.adv_service.calculate_debt_to_assets_for_tickers,
            "ASSET_TURNOVER_TTM": self.adv_service.calculate_asset_turnover_ttm,
            "INVENTORY_TURNOVER_TTM": self.adv_service.calculate_inventory_turnover_ttm,
            "INTEREST_TO_INCOME_TTM": self.adv_service.calculate_interest_to_income_ttm,
            "ROA_TTM": self.adv_service.calculate_roa_ttm,
            "ROE_TTM": self.adv_service.calculate_roe_ttm,
        }

        if fundamental_name.upper() in supported_fundamentals:
            return await supported_fundamentals[fundamental_name.upper()](
                tickers,
                start_date_str,
                end_date_str,
                {}
            )
        else:
            logger.warning(f"calculate_synthetic_fundamental_timeseries: Unsupported fundamental_name '{fundamental_name}'")
            return {}

    async def _calculate_eps_ttm_for_tickers(
        self, 
        tickers: List[str], 
        start_date_str: Optional[str], 
        end_date_str: Optional[str],
        ticker_profiles_cache: Dict[str, Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        
        user_start_date_obj: Optional[datetime] = None
        user_end_date_obj: Optional[datetime] = None
        if start_date_str:
            try: user_start_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d")
            except ValueError: 
                logger.warning(f"EPS_TTM: Invalid start_date_str '{start_date_str}'. Defaulting to YTD start.")
                user_start_date_obj = datetime.today().replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else: user_start_date_obj = datetime.today().replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

        if end_date_str:
            try: user_end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d")
            except ValueError: 
                logger.warning(f"EPS_TTM: Invalid end_date_str '{end_date_str}'. Defaulting to today.")
                user_end_date_obj = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        else: user_end_date_obj = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.debug(f"EPS_TTM: User date range for calculation: {user_start_date_obj.strftime('%Y-%m-%d')} to {user_end_date_obj.strftime('%Y-%m-%d')}")

        target_net_income_key = "Diluted NI Availto Com Stockholders"
        target_shares_key = "Diluted Average Shares"

        for ticker_symbol in tickers:
            try:
                logger.info(f"EPS_TTM: Processing ticker: {ticker_symbol}")

                # 1. Data Fetching and Preparation (Quarterly)
                quarterly_lookback_start_obj = user_start_date_obj - timedelta(days=(365 * 1 + 30 * 9)) # Approx 1 year 9 months before user start
                logger.debug(f"EPS_TTM: Querying QUARTERLY income statements for {ticker_symbol} from {quarterly_lookback_start_obj.strftime('%Y-%m-%d')} to {user_end_date_obj.strftime('%Y-%m-%d')}")
                
                quarterly_income_statements = await self.db_repo.get_data_items_by_criteria(
                    ticker=ticker_symbol,
                    item_type="INCOME_STATEMENT",
                    item_time_coverage="QUARTER",
                    start_date=quarterly_lookback_start_obj,
                    end_date=user_end_date_obj,
                    order_by_key_date_desc=False # Fetch ascending for easier processing
                )
                quarterly_eps_points: List[Dict[str, Any]] = []
                
                # Fetch shares data using the helper for quarterly calculations
                quarterly_shares_data_from_helper: List[Dict[str, Any]] = []
                if quarterly_income_statements:
                    min_q_date = min(self._parse_date_flex(qis.get('item_key_date')) for qis in quarterly_income_statements if self._parse_date_flex(qis.get('item_key_date'))) if quarterly_income_statements else quarterly_lookback_start_obj
                    max_q_date = max(self._parse_date_flex(qis.get('item_key_date')) for qis in quarterly_income_statements if self._parse_date_flex(qis.get('item_key_date'))) if quarterly_income_statements else user_end_date_obj
                    
                    if min_q_date and max_q_date:
                        logger.debug(f"EPS_TTM [{ticker_symbol}]: Fetching quarterly shares series via helper from {min_q_date.strftime('%Y-%m-%d')} to {max_q_date.strftime('%Y-%m-%d')}")
                        quarterly_shares_data_from_helper = await self._get_quarterly_shares_series(
                            ticker_symbol,
                            min_q_date.strftime('%Y-%m-%d'),
                            max_q_date.strftime('%Y-%m-%d')
                        )
                        logger.debug(f"EPS_TTM [{ticker_symbol}]: Helper returned {len(quarterly_shares_data_from_helper)} shares points.")

                if quarterly_income_statements:
                    logger.info(f"EPS_TTM: Found {len(quarterly_income_statements)} QUARTERLY statements for {ticker_symbol}.")
                    conversion_info = await self._get_conversion_info_for_ticker(ticker_symbol, ticker_profiles_cache)
                    rate_to_apply, original_fin_curr, target_trade_curr = (conversion_info[2], conversion_info[1], conversion_info[0]) if conversion_info else (None, None, None)

                    for q_item in quarterly_income_statements:
                        payload = q_item.get('item_data_payload')
                        key_date_from_db = q_item.get('item_key_date')
                        if not isinstance(payload, dict) or not key_date_from_db: continue

                        current_payload = payload
                        if rate_to_apply:
                            current_payload = await self._apply_currency_conversion_to_payload(payload, rate_to_apply, original_fin_curr, target_trade_curr, "INCOME_STATEMENT")
                        
                        q_date_obj = self._parse_date_flex(key_date_from_db)
                        if not q_date_obj: continue

                        # Add fallback logic for net income
                        ni_value = current_payload.get(target_net_income_key)
                        if ni_value is None:  # Fallback to "Net Income" if primary field not found
                            ni_value = current_payload.get("Net Income")
                            if ni_value is None:  # Second fallback to "NetIncome" without space
                                ni_value = current_payload.get("NetIncome")
                        
                        shares_value_for_q_eps = None
                        if quarterly_shares_data_from_helper:
                            for shares_point in reversed(quarterly_shares_data_from_helper):
                                if shares_point['date_obj'] <= q_date_obj:
                                    shares_value_for_q_eps = shares_point['value']
                                    break 
                        else:
                            logger.warning(f"EPS_TTM [{ticker_symbol}]: No quarterly shares data from helper available for NI date {q_date_obj.strftime('%Y-%m-%d')}")

                        if ni_value is not None and shares_value_for_q_eps is not None and shares_value_for_q_eps != 0:
                            try: 
                                q_eps = float(ni_value) / float(shares_value_for_q_eps)
                                quarterly_eps_points.append({
                                    'date_obj': q_date_obj, 
                                    'value': q_eps,  # Changed from 'q_eps' to 'value' for generic TTM
                                    'type': 'quarterly'
                                })
                            except (ValueError, TypeError) as e:
                                logger.warning(f"EPS_TTM [{ticker_symbol}]: Could not calculate quarterly EPS for date {q_date_obj.strftime('%Y-%m-%d')}. NI: {ni_value}, Shares: {shares_value_for_q_eps}. Error: {e}")

                    if quarterly_eps_points:
                        quarterly_eps_points.sort(key=lambda x: x['date_obj']) # Ensure sorted by date
                        logger.debug(f"EPS_TTM [{ticker_symbol}]: Prepared {len(quarterly_eps_points)} quarterly EPS points.")
                else:
                    logger.info(f"EPS_TTM: No QUARTERLY income statements found for {ticker_symbol} in lookback period.")

                # 2. Data Fetching and Preparation (Annual)
                annual_lookback_start_obj = user_start_date_obj - timedelta(days=(365 * 2 + 30 * 3))
                logger.debug(f"EPS_TTM: Querying ANNUAL income statements for {ticker_symbol} from {annual_lookback_start_obj.strftime('%Y-%m-%d')} to {user_end_date_obj.strftime('%Y-%m-%d')}")
                annual_income_statements = await self.db_repo.get_data_items_by_criteria(
                    ticker=ticker_symbol,
                    item_type="INCOME_STATEMENT",
                    item_time_coverage="FYEAR",
                    start_date=annual_lookback_start_obj,
                    end_date=user_end_date_obj,
                    order_by_key_date_desc=False # Fetch ascending
                )
                annual_eps_points: List[Dict[str, Any]] = []
                if annual_income_statements:
                    logger.info(f"EPS_TTM: Found {len(annual_income_statements)} ANNUAL statements for {ticker_symbol}.")
                    conversion_info_annual = await self._get_conversion_info_for_ticker(ticker_symbol, ticker_profiles_cache)
                    rate_annual, orig_curr_annual, target_curr_annual = (conversion_info_annual[2], conversion_info_annual[1], conversion_info_annual[0]) if conversion_info_annual else (None, None, None)

                    for an_item in annual_income_statements:
                        payload = an_item.get('item_data_payload')
                        key_date_from_db = an_item.get('item_key_date')
                        if not isinstance(payload, dict) or not key_date_from_db: continue
                        
                        current_payload = payload
                        if rate_annual:
                            current_payload = await self._apply_currency_conversion_to_payload(payload, rate_annual, orig_curr_annual, target_curr_annual, "INCOME_STATEMENT")
                        
                        fy_date_obj = self._parse_date_flex(key_date_from_db)
                        if not fy_date_obj: continue

                        # Add fallback logic for net income in annual data too
                        ni_value = current_payload.get(target_net_income_key)
                        if ni_value is None:  # Fallback to "Net Income" if primary field not found
                            ni_value = current_payload.get("Net Income")
                            if ni_value is None:  # Second fallback to "NetIncome" without space
                                ni_value = current_payload.get("NetIncome")
                        shares_value = current_payload.get(target_shares_key)
                        if ni_value is not None and shares_value is not None and shares_value != 0:
                            try: 
                                annual_eps = float(ni_value) / float(shares_value)
                                annual_eps_points.append({
                                    'date_obj': fy_date_obj, 
                                    'value': annual_eps,  # Changed from 'annual_eps' to 'value' for generic TTM
                                    'type': 'annual'
                                })
                            except (ValueError, TypeError) as e:
                                logger.warning(f"EPS_TTM [{ticker_symbol}]: Could not calculate annual EPS for date {fy_date_obj.strftime('%Y-%m-%d')}. NI: {ni_value}, Shares: {shares_value}. Error: {e}")
                    if annual_eps_points:
                        annual_eps_points.sort(key=lambda x: x['date_obj']) # Ensure sorted
                        logger.debug(f"EPS_TTM [{ticker_symbol}]: Prepared {len(annual_eps_points)} annual EPS points.")
                else:
                    logger.info(f"EPS_TTM: No ANNUAL income statements found for {ticker_symbol} in lookback period.")

                # 3. Generate Daily TTM Series using Generic TTM Function
                final_eps_series_for_ticker: List[Dict[str, Any]] = []
                current_iter_date = user_start_date_obj
                debug_count = 0

                while current_iter_date <= user_end_date_obj:
                    ttm_value = self._calculate_ttm_value_generic(
                        current_iter_date,
                        quarterly_eps_points,
                        annual_eps_points,
                        "value",  # Using 'value' as the key for both quarterly and annual points
                        debug_identifier=f"EPS_TTM_{ticker_symbol}"
                    )

                    # Debug logging for first few points
                    if debug_count < 5:
                        logger.debug(
                            f"EPS_TTM [{ticker_symbol}] Date {current_iter_date.strftime('%Y-%m-%d')}: "
                            f"TTM Value={ttm_value}"
                        )
                        debug_count += 1

                    final_eps_series_for_ticker.append({
                        'date': current_iter_date.strftime("%Y-%m-%d"), 
                        'value': ttm_value
                    })
                    current_iter_date += timedelta(days=1)
                
                results_by_ticker[ticker_symbol] = final_eps_series_for_ticker
                logger.info(f"EPS_TTM [{ticker_symbol}]: Generated {len(final_eps_series_for_ticker)} total EPS points.")

            except Exception as e:
                logger.error(f"EPS_TTM: Critical error for {ticker_symbol}: {e}", exc_info=True)
                results_by_ticker[ticker_symbol] = [] # Ensure ticker entry exists even on error
        return results_by_ticker

    # --- MODIFIED: Cash/Share Calculation (Removed TTM) ---

    async def _get_quarterly_shares_series(
        self, 
        ticker_symbol: str, 
        fundamental_query_start_date_str: str, 
        fundamental_query_end_date_str: str
    ) -> List[Dict[str, Any]]:
        """
        Fetches, processes, and combines quarterly shares data from primary and fallback sources.
        Returns a chronologically sorted list of shares data points.
        """
        logger.debug(f"SHARES_HELPER [{ticker_symbol}]: Fetching shares between {fundamental_query_start_date_str} and {fundamental_query_end_date_str}")
        
        primary_shares_field_id = "yf_item_income_statement_quarterly_DilutedAverageShares"
        fallback_shares_field_id = "yf_item_balance_sheet_quarterly_ShareIssued"
        
        quarterly_shares_points: List[Dict[str, Any]] = []

        # 1. Fetch and Process Primary Shares Data
        logger.debug(f"SHARES_HELPER [{ticker_symbol}]: Fetching primary shares data ({primary_shares_field_id})")
        shares_data_primary_raw = await self.get_specific_field_timeseries(
            field_identifier=primary_shares_field_id,
            tickers=[ticker_symbol],
            start_date_str=fundamental_query_start_date_str,
            end_date_str=fundamental_query_end_date_str
        )
        logger.debug(f"SHARES_HELPER [{ticker_symbol}]: Raw primary shares data: {shares_data_primary_raw.get(ticker_symbol)}")
        
        if shares_data_primary_raw.get(ticker_symbol):
            for item in shares_data_primary_raw[ticker_symbol]:
                date_obj = self._parse_date_flex(item.get('date'))
                value = item.get('value')
                if date_obj and value is not None:
                    try:
                        float_value = float(value)
                        if float_value != 0:
                            quarterly_shares_points.append({'date_obj': date_obj, 'value': float_value, 'source': 'primary'})
                    except (ValueError, TypeError):
                        logger.warning(f"SHARES_HELPER [{ticker_symbol}]: Could not parse primary shares value '{value}' for date '{item.get('date')}'")
            logger.info(f"SHARES_HELPER [{ticker_symbol}]: Parsed {len(quarterly_shares_points)} primary quarterly shares points.")
            logger.debug(f"SHARES_HELPER [{ticker_symbol}]: Parsed primary shares points: {quarterly_shares_points[:5]}...")

        # 2. Fetch and Process Fallback Shares Data
        logger.debug(f"SHARES_HELPER [{ticker_symbol}]: Fetching fallback shares data ({fallback_shares_field_id})")
        shares_data_fallback_raw = await self.get_specific_field_timeseries(
            field_identifier=fallback_shares_field_id,
            tickers=[ticker_symbol],
            start_date_str=fundamental_query_start_date_str,
            end_date_str=fundamental_query_end_date_str
        )
        logger.debug(f"SHARES_HELPER [{ticker_symbol}]: Raw fallback shares data: {shares_data_fallback_raw.get(ticker_symbol)}")
        
        fallback_shares_added_count = 0
        if shares_data_fallback_raw.get(ticker_symbol):
            for item in shares_data_fallback_raw[ticker_symbol]:
                date_obj = self._parse_date_flex(item.get('date'))
                value = item.get('value')
                if date_obj and value is not None:
                    # Check if a point with the same date already exists from the primary source
                    is_duplicate_date = any(sp['date_obj'] == date_obj for sp in quarterly_shares_points)
                    if not is_duplicate_date:
                        try:
                            float_value = float(value)
                            if float_value != 0:
                                quarterly_shares_points.append({'date_obj': date_obj, 'value': float_value, 'source': 'fallback'})
                                fallback_shares_added_count +=1
                        except (ValueError, TypeError):
                            logger.warning(f"SHARES_HELPER [{ticker_symbol}]: Could not parse fallback shares value '{value}' for date '{item.get('date')}'")
            logger.info(f"SHARES_HELPER [{ticker_symbol}]: Added {fallback_shares_added_count} fallback quarterly shares points.")
        
        # 3. Sort Data
        quarterly_shares_points.sort(key=lambda x: x['date_obj'])
        logger.debug(f"SHARES_HELPER [{ticker_symbol}]: Combined and sorted shares points ({len(quarterly_shares_points)} total): {quarterly_shares_points[:5]}...")
        
        return quarterly_shares_points

    async def _calculate_cash_per_share_for_tickers(
        self,
        tickers: List[str],
        start_date_str: Optional[str],
        end_date_str: Optional[str],
        ticker_profiles_cache: Dict[str, Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        logger.info(f"CASH_PER_SHARE: Starting calculation for tickers: {tickers}, start: {start_date_str}, end: {end_date_str}")

        user_start_date_obj = self._parse_date_flex(start_date_str) if start_date_str else datetime.now() - timedelta(days=365*5) # Default 5 years
        user_end_date_obj = self._parse_date_flex(end_date_str) if end_date_str else datetime.now()

        # For point-in-time Cash/Share, fundamental query can align more closely with user dates,
        # but we still need a bit of lookback for shares data alignment.
        fundamental_query_start_date_obj = user_start_date_obj - timedelta(days=180) # Approx 6 months buffer for shares alignment
        fundamental_query_start_date_str = fundamental_query_start_date_obj.strftime('%Y-%m-%d')
        fundamental_query_end_date_str = user_end_date_obj.strftime('%Y-%m-%d')

        cash_field_id = "yf_item_balance_sheet_quarterly_CashAndCashEquivalents"
        # REMOVED: primary_shares_field_id and fallback_shares_field_id definitions here

        for ticker_symbol in tickers:
            try:
                logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: Fetching cash data ({cash_field_id}) between {fundamental_query_start_date_str} and {fundamental_query_end_date_str}")
                cash_data_raw = await self.get_specific_field_timeseries(
                    field_identifier=cash_field_id,
                    tickers=[ticker_symbol],
                    start_date_str=fundamental_query_start_date_str,
                    end_date_str=fundamental_query_end_date_str
                )
                logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: Raw cash data: {cash_data_raw.get(ticker_symbol)}")
                
                quarterly_cash_points: List[Dict[str, Any]] = []
                if cash_data_raw.get(ticker_symbol):
                    for item in cash_data_raw[ticker_symbol]:
                        date_obj = self._parse_date_flex(item.get('date'))
                        value = item.get('value')
                        if date_obj and value is not None:
                            try: quarterly_cash_points.append({'date_obj': date_obj, 'value': float(value)})
                            except (ValueError, TypeError): pass
                    quarterly_cash_points.sort(key=lambda x: x['date_obj'])
                    logger.info(f"CASH_PER_SHARE [{ticker_symbol}]: Parsed {len(quarterly_cash_points)} quarterly cash points.")
                    logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: Parsed cash points: {quarterly_cash_points[:5]}...")
                else:
                    logger.warning(f"CASH_PER_SHARE [{ticker_symbol}]: No quarterly cash data found using {cash_field_id}. Skipping.")
                    results_by_ticker[ticker_symbol] = []
                    continue

                # NEW: Call the helper function to get shares data
                logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: Calling _get_quarterly_shares_series helper.")
                quarterly_shares_points = await self._get_quarterly_shares_series(
                    ticker_symbol,
                    fundamental_query_start_date_str,
                    fundamental_query_end_date_str
                )
                # REMOVED: Old shares fetching and processing logic that was here
                
                if not quarterly_cash_points:
                    logger.warning(f"CASH_PER_SHARE [{ticker_symbol}]: No valid quarterly cash points derived after parsing. No Cash/Share data.")
                    results_by_ticker[ticker_symbol] = []
                    continue
                if not quarterly_shares_points: # Check if helper returned any shares data
                    logger.warning(f"CASH_PER_SHARE [{ticker_symbol}]: No valid quarterly shares points from helper. No Cash/Share data.")
                    results_by_ticker[ticker_symbol] = []
                    continue

                # 4. Calculate point-in-time Cash/Share
                # For each quarterly cash point, find the latest shares figure and calculate.
                point_in_time_cash_per_share_series: List[Dict[str, Any]] = []
                logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: Starting point-in-time Cash/Share calculation.")

                for cash_point in quarterly_cash_points:
                    current_cash_date = cash_point['date_obj']
                    current_cash_value = cash_point['value']

                    latest_shares_value = None
                    latest_shares_date = None
                    for sp in reversed(quarterly_shares_points): # Iterate backwards (latest first)
                        if sp['date_obj'] <= current_cash_date:
                            latest_shares_value = sp['value']
                            latest_shares_date = sp['date_obj']
                            logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: For cash date {current_cash_date.strftime('%Y-%m-%d')} (value: {current_cash_value}), using shares {latest_shares_value} from {latest_shares_date.strftime('%Y-%m-%d')} (source: {sp['source']})")
                            break
                    
                    if latest_shares_value is not None and latest_shares_value != 0:
                        cash_per_share_value = current_cash_value / latest_shares_value
                        logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: Calculated C/S at {current_cash_date.strftime('%Y-%m-%d')}: {current_cash_value} / {latest_shares_value} = {cash_per_share_value}")
                        point_in_time_cash_per_share_series.append({
                            'date_obj': current_cash_date, 
                            'value': cash_per_share_value
                        })
                    else:
                        logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: No valid shares figure or shares were zero for cash date {current_cash_date.strftime('%Y-%m-%d')}. Shares value: {latest_shares_value}")

                if not point_in_time_cash_per_share_series:
                    logger.warning(f"CASH_PER_SHARE [{ticker_symbol}]: No point-in-time Cash/Share points could be calculated.")
                    results_by_ticker[ticker_symbol] = []
                    continue
                
                # point_in_time_cash_per_share_series is already sorted by date due to iterating quarterly_cash_points

                # 5. Generate daily series for the user-requested period by propagating the last known Cash/Share
                daily_series: List[Dict[str, Any]] = []
                current_iter_date = user_start_date_obj
                last_known_val = None

                # Find initial value for start of user period
                for point in reversed(point_in_time_cash_per_share_series):
                    if point['date_obj'] <= current_iter_date:
                        last_known_val = point['value']
                        logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: Initial last_known_val for daily series (at or before {current_iter_date.strftime('%Y-%m-%d')}): {last_known_val} from {point['date_obj'].strftime('%Y-%m-%d')}")
                        break
                
                if last_known_val is None and point_in_time_cash_per_share_series: 
                    if point_in_time_cash_per_share_series[0]['date_obj'] <= user_end_date_obj: 
                       logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: No C/S data at or before user start date {user_start_date_obj.strftime('%Y-%m-%d')}. Daily series will start from first available point: {point_in_time_cash_per_share_series[0]['date_obj'].strftime('%Y-%m-%d') if point_in_time_cash_per_share_series else 'N/A'}")
                       pass

                point_idx = 0
                while current_iter_date <= user_end_date_obj:
                    while point_idx < len(point_in_time_cash_per_share_series) and point_in_time_cash_per_share_series[point_idx]['date_obj'] <= current_iter_date:
                        last_known_val = point_in_time_cash_per_share_series[point_idx]['value']
                        point_idx += 1
                    
                    if last_known_val is not None:
                        if point_in_time_cash_per_share_series and current_iter_date >= point_in_time_cash_per_share_series[0]['date_obj']:
                            daily_series.append({'date': current_iter_date.strftime('%Y-%m-%d'), 'value': last_known_val})

                    current_iter_date += timedelta(days=1)
                
                results_by_ticker[ticker_symbol] = daily_series
                logger.info(f"CASH_PER_SHARE [{ticker_symbol}]: Successfully generated {len(daily_series)} daily Cash/Share data points.")

            except Exception as e:
                logger.error(f"CASH_PER_SHARE [{ticker_symbol}]: Unhandled error during processing: {e}", exc_info=True)
                results_by_ticker[ticker_symbol] = []
            
        return results_by_ticker
    # --- END: Cash/Share Calculation ---

    # --- NEW: Cash + Short Term Investments / Share Calculation ---
    async def _calculate_cash_plus_st_inv_per_share_for_tickers(
        self,
        tickers: List[str],
        start_date_str: Optional[str],
        end_date_str: Optional[str],
        ticker_profiles_cache: Dict[str, Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        logger.info(f"CASH_PLUS_ST_INV_PER_SHARE: Starting calculation for tickers: {tickers}, start: {start_date_str}, end: {end_date_str}")

        user_start_date_obj = self._parse_date_flex(start_date_str) if start_date_str else datetime.now() - timedelta(days=365*5) # Default 5 years
        user_end_date_obj = self._parse_date_flex(end_date_str) if end_date_str else datetime.now()

        fundamental_query_start_date_obj = user_start_date_obj - timedelta(days=180) # Approx 6 months buffer for shares alignment
        fundamental_query_start_date_str = fundamental_query_start_date_obj.strftime('%Y-%m-%d')
        fundamental_query_end_date_str = user_end_date_obj.strftime('%Y-%m-%d')

        # Key difference: Use the field for Cash + Cash Equivalents + Short Term Investments
        cash_field_id = "yf_item_balance_sheet_quarterly_CashCashEquivalentsAndShortTermInvestments"
        # REMOVED: primary_shares_field_id and fallback_shares_field_id definitions here

        for ticker_symbol in tickers:
            try:
                logger.debug(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: Fetching cash+ST data ({cash_field_id}) between {fundamental_query_start_date_str} and {fundamental_query_end_date_str}")
                cash_data_raw = await self.get_specific_field_timeseries(
                    field_identifier=cash_field_id,
                    tickers=[ticker_symbol],
                    start_date_str=fundamental_query_start_date_str,
                    end_date_str=fundamental_query_end_date_str
                )
                logger.debug(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: Raw cash+ST data: {cash_data_raw.get(ticker_symbol)}")
                
                quarterly_cash_points: List[Dict[str, Any]] = []
                if cash_data_raw.get(ticker_symbol):
                    for item in cash_data_raw[ticker_symbol]:
                        date_obj = self._parse_date_flex(item.get('date'))
                        value = item.get('value')
                        if date_obj and value is not None:
                            try: quarterly_cash_points.append({'date_obj': date_obj, 'value': float(value)})
                            except (ValueError, TypeError): pass
                    quarterly_cash_points.sort(key=lambda x: x['date_obj'])
                    logger.info(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: Parsed {len(quarterly_cash_points)} quarterly cash+ST points.")
                else:
                    logger.warning(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: No quarterly cash+ST data found using {cash_field_id}. Skipping.")
                    results_by_ticker[ticker_symbol] = []
                    continue

                # Shares fetching logic (identical to Cash/Share)
                # NEW: Call the helper function to get shares data
                logger.debug(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: Calling _get_quarterly_shares_series helper.")
                quarterly_shares_points = await self._get_quarterly_shares_series(
                    ticker_symbol,
                    fundamental_query_start_date_str,
                    fundamental_query_end_date_str
                )
                # REMOVED: Old shares fetching and processing logic that was here
                
                if not quarterly_cash_points:
                    logger.warning(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: No valid quarterly cash+ST points. No data.")
                    results_by_ticker[ticker_symbol] = []
                    continue
                if not quarterly_shares_points: # Check if helper returned any shares data
                    logger.warning(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: No valid quarterly shares points from helper. No data.")
                    results_by_ticker[ticker_symbol] = []
                    continue

                # Point-in-time calculation (identical to Cash/Share)
                point_in_time_series: List[Dict[str, Any]] = []
                for cash_point in quarterly_cash_points:
                    current_cash_date = cash_point['date_obj']
                    current_cash_value = cash_point['value']
                    latest_shares_value = None
                    for sp in reversed(quarterly_shares_points):
                        if sp['date_obj'] <= current_cash_date:
                            latest_shares_value = sp['value']
                            break
                    if latest_shares_value is not None and latest_shares_value != 0:
                        calculated_value = current_cash_value / latest_shares_value
                        point_in_time_series.append({'date_obj': current_cash_date, 'value': calculated_value})

                if not point_in_time_series:
                    logger.warning(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: No point-in-time values calculated.")
                    results_by_ticker[ticker_symbol] = []
                    continue
                
                # Daily series propagation (identical to Cash/Share)
                daily_series: List[Dict[str, Any]] = []
                current_iter_date = user_start_date_obj
                last_known_val = None
                for point in reversed(point_in_time_series):
                    if point['date_obj'] <= current_iter_date:
                        last_known_val = point['value']
                        break
                if last_known_val is None and point_in_time_series: 
                    if point_in_time_series[0]['date_obj'] <= user_end_date_obj: 
                       pass

                point_idx = 0
                while current_iter_date <= user_end_date_obj:
                    while point_idx < len(point_in_time_series) and point_in_time_series[point_idx]['date_obj'] <= current_iter_date:
                        last_known_val = point_in_time_series[point_idx]['value']
                        point_idx += 1
                    if last_known_val is not None:
                         if point_in_time_series and current_iter_date >= point_in_time_series[0]['date_obj']:
                            daily_series.append({'date': current_iter_date.strftime('%Y-%m-%d'), 'value': last_known_val})
                    current_iter_date += timedelta(days=1)
                
                results_by_ticker[ticker_symbol] = daily_series
                logger.info(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: Generated {len(daily_series)} daily data points.")

            except Exception as e:
                logger.error(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: Unhandled error: {e}", exc_info=True)
                results_by_ticker[ticker_symbol] = []
            
        return results_by_ticker
    # --- END: Cash + Short Term Investments / Share Calculation ---

    # --- NEW: Book Value per Share Calculation ---
    async def _calculate_book_value_per_share_for_tickers(
        self,
        tickers: List[str],
        start_date_str: Optional[str],
        end_date_str: Optional[str],
        ticker_profiles_cache: Dict[str, Dict[str, Any]] # Retained for consistency, though may not be directly used if helper handles currency
    ) -> Dict[str, List[Dict[str, Any]]]:
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        logger.info(f"BOOK_VALUE_PER_SHARE: Starting calculation for tickers: {tickers}, start: {start_date_str}, end: {end_date_str}")

        user_start_date_obj = self._parse_date_flex(start_date_str) if start_date_str else datetime.now() - timedelta(days=365*5) # Default 5 years
        user_end_date_obj = self._parse_date_flex(end_date_str) if end_date_str else datetime.now()

        fundamental_query_start_date_obj = user_start_date_obj - timedelta(days=180) # Approx 6 months buffer
        fundamental_query_start_date_str = fundamental_query_start_date_obj.strftime('%Y-%m-%d')
        fundamental_query_end_date_str = user_end_date_obj.strftime('%Y-%m-%d')

        cse_quarterly_field_id = "yf_item_balance_sheet_quarterly_CommonStockEquity"
        cse_annual_field_id = "yf_item_balance_sheet_annual_CommonStockEquity"

        for ticker_symbol in tickers:
            try:
                # 1. Fetch Common Stock Equity (CSE) Data
                # Fetch Quarterly CSE
                logger.debug(f"BOOK_VALUE_PER_SHARE [{ticker_symbol}]: Fetching Q CSE ({cse_quarterly_field_id})")
                cse_q_raw = await self.get_specific_field_timeseries(
                    field_identifier=cse_quarterly_field_id,
                    tickers=[ticker_symbol],
                    start_date_str=fundamental_query_start_date_str,
                    end_date_str=fundamental_query_end_date_str
                )
                quarterly_cse_points: List[Dict[str, Any]] = []
                if cse_q_raw.get(ticker_symbol):
                    for item in cse_q_raw[ticker_symbol]:
                        date_obj = self._parse_date_flex(item.get('date'))
                        value = item.get('value')
                        if date_obj and value is not None:
                            try: quarterly_cse_points.append({'date_obj': date_obj, 'value': float(value), 'source': 'quarterly'})
                            except (ValueError, TypeError): pass
                logger.info(f"BOOK_VALUE_PER_SHARE [{ticker_symbol}]: Parsed {len(quarterly_cse_points)} quarterly CSE points.")

                # Fetch Annual CSE
                logger.debug(f"BOOK_VALUE_PER_SHARE [{ticker_symbol}]: Fetching A CSE ({cse_annual_field_id})")
                cse_a_raw = await self.get_specific_field_timeseries(
                    field_identifier=cse_annual_field_id,
                    tickers=[ticker_symbol],
                    start_date_str=fundamental_query_start_date_str, # Use same extended range
                    end_date_str=fundamental_query_end_date_str
                )
                annual_cse_points: List[Dict[str, Any]] = []
                if cse_a_raw.get(ticker_symbol):
                    for item in cse_a_raw[ticker_symbol]:
                        date_obj = self._parse_date_flex(item.get('date'))
                        value = item.get('value')
                        if date_obj and value is not None:
                            try: annual_cse_points.append({'date_obj': date_obj, 'value': float(value), 'source': 'annual'})
                            except (ValueError, TypeError): pass
                logger.info(f"BOOK_VALUE_PER_SHARE [{ticker_symbol}]: Parsed {len(annual_cse_points)} annual CSE points.")

                # Combine CSE points, giving preference to quarterly if dates overlap
                combined_cse_map: Dict[datetime, float] = {}
                for point in annual_cse_points: # Annual first
                    combined_cse_map[point['date_obj']] = point['value']
                for point in quarterly_cse_points: # Quarterly overwrites if date matches
                    combined_cse_map[point['date_obj']] = point['value']
                
                processed_cse_points = sorted([{'date_obj': dt, 'value': val} for dt, val in combined_cse_map.items()], key=lambda x: x['date_obj'])
                logger.info(f"BOOK_VALUE_PER_SHARE [{ticker_symbol}]: Combined to {len(processed_cse_points)} unique CSE points.")

                if not processed_cse_points:
                    logger.warning(f"BOOK_VALUE_PER_SHARE [{ticker_symbol}]: No CSE data (quarterly or annual). Skipping.")
                    results_by_ticker[ticker_symbol] = []
                    continue

                # 2. Fetch Shares Data using Helper
                logger.debug(f"BOOK_VALUE_PER_SHARE [{ticker_symbol}]: Calling _get_quarterly_shares_series helper.")
                quarterly_shares_points = await self._get_quarterly_shares_series(
                    ticker_symbol,
                    fundamental_query_start_date_str,
                    fundamental_query_end_date_str
                )
                if not quarterly_shares_points:
                    logger.warning(f"BOOK_VALUE_PER_SHARE [{ticker_symbol}]: No shares data from helper. Skipping.")
                    results_by_ticker[ticker_symbol] = []
                    continue

                # 3. Calculate Point-in-Time Book Value per Share
                point_in_time_bvps_series: List[Dict[str, Any]] = []
                for cse_point in processed_cse_points:
                    current_cse_date = cse_point['date_obj']
                    current_cse_value = cse_point['value']
                    latest_shares_value = None
                    for sp in reversed(quarterly_shares_points): # Iterate backwards (latest first)
                        if sp['date_obj'] <= current_cse_date:
                            latest_shares_value = sp['value']
                            break
                    
                    if latest_shares_value is not None and latest_shares_value != 0:
                        bvps_value = current_cse_value / latest_shares_value
                        point_in_time_bvps_series.append({'date_obj': current_cse_date, 'value': bvps_value})
                    # else: logger.debug(f"BOOK_VALUE_PER_SHARE [{ticker_symbol}]: No shares for CSE date {current_cse_date}, or shares were zero.")

                if not point_in_time_bvps_series:
                    logger.warning(f"BOOK_VALUE_PER_SHARE [{ticker_symbol}]: No point-in-time BVPS points calculated. Skipping.")
                    results_by_ticker[ticker_symbol] = []
                    continue

                # 4. Generate Daily Series
                daily_series: List[Dict[str, Any]] = []
                current_iter_date = user_start_date_obj
                last_known_val = None
                # Find initial value for start of user period
                for point in reversed(point_in_time_bvps_series):
                    if point['date_obj'] <= current_iter_date:
                        last_known_val = point['value']
                        break
                
                # If no data before start, but data exists within period, allow it to be picked up
                if last_known_val is None and point_in_time_bvps_series: 
                    if point_in_time_bvps_series[0]['date_obj'] <= user_end_date_obj: 
                       pass # last_known_val remains None, will be picked up by loop if first data point is after user_start_date_obj

                point_idx = 0
                while current_iter_date <= user_end_date_obj:
                    while point_idx < len(point_in_time_bvps_series) and point_in_time_bvps_series[point_idx]['date_obj'] <= current_iter_date:
                        last_known_val = point_in_time_bvps_series[point_idx]['value']
                        point_idx += 1
                    
                    # Only add to series if last_known_val is set AND current_iter_date is on or after the first actual data point date
                    # This avoids prepending None values if user_start_date_obj is before any available data.
                    if last_known_val is not None and point_in_time_bvps_series and current_iter_date >= point_in_time_bvps_series[0]['date_obj']:
                        daily_series.append({'date': current_iter_date.strftime('%Y-%m-%d'), 'value': last_known_val})
                    elif last_known_val is None and point_in_time_bvps_series and current_iter_date < point_in_time_bvps_series[0]['date_obj']:
                        # If we haven't reached the first data point yet, and user start date is before it, append None
                        daily_series.append({'date': current_iter_date.strftime('%Y-%m-%d'), 'value': None})
                    elif last_known_val is not None and not point_in_time_bvps_series: # Should not happen if point_in_time_bvps_series check is done before
                        daily_series.append({'date': current_iter_date.strftime('%Y-%m-%d'), 'value': last_known_val})
                        
                    current_iter_date += timedelta(days=1)
                
                results_by_ticker[ticker_symbol] = daily_series
                logger.info(f"BOOK_VALUE_PER_SHARE [{ticker_symbol}]: Successfully generated {len(daily_series)} daily BVPS data points.")

            except Exception as e:
                logger.error(f"BOOK_VALUE_PER_SHARE [{ticker_symbol}]: Unhandled error: {e}", exc_info=True)
                results_by_ticker[ticker_symbol] = []
            
        return results_by_ticker
    # --- END: Book Value per Share Calculation ---

    # --- NEW: Price/Book Value Calculation ---
    async def _calculate_price_to_book_value_for_tickers(
        self,
        tickers: List[str],
        start_date_str: Optional[str],
        end_date_str: Optional[str],
        ticker_profiles_cache: Dict[str, Dict[str, Any]] # Retained for consistency
    ) -> Dict[str, List[Dict[str, Any]]]:
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        logger.info(f"PRICE_TO_BOOK_VALUE: Starting calculation for tickers: {tickers}, start: {start_date_str}, end: {end_date_str}")

        # 1. Get Book Value per Share data
        book_value_per_share_data = await self._calculate_book_value_per_share_for_tickers(
            tickers,
            start_date_str,
            end_date_str,
            ticker_profiles_cache
        )

        for ticker_symbol in tickers:
            try:
                bvps_series = book_value_per_share_data.get(ticker_symbol, [])
                if not bvps_series:
                    logger.warning(f"PRICE_TO_BOOK_VALUE [{ticker_symbol}]: No Book Value/Share data available. Skipping ratio calculation.")
                    results_by_ticker[ticker_symbol] = []
                    continue

                # Convert BVPS series to a map for quick lookup
                bvps_map = {item['date']: item['value'] for item in bvps_series if item['value'] is not None}

                # 2. Fetch Price Data
                # Price history already handles default dates if start/end are None, but P/E used explicit defaults.
                # For consistency with how other P/X ratios are handled here, ensure dates.
                query_start_date = start_date_str
                query_end_date = end_date_str
                
                # Use user-defined start_date_obj and end_date_obj from Book Value per Share section (which have defaults)
                # This ensures alignment with the period for which BVPS was generated.
                # Defaulting to 1 year if not provided, to ensure price_data is fetched.
                if not query_start_date:
                    default_start_obj = datetime.now() - timedelta(days=365)
                    query_start_date = default_start_obj.strftime("%Y-%m-%d")
                if not query_end_date:
                    query_end_date = datetime.now().strftime("%Y-%m-%d")
                
                logger.debug(f"PRICE_TO_BOOK_VALUE [{ticker_symbol}]: Fetching price data from {query_start_date} to {query_end_date}")
                price_data = await self.get_price_history(
                    ticker=ticker_symbol,
                    interval="1d",
                    start_date=query_start_date, 
                    end_date=query_end_date
                )

                if not price_data:
                    logger.warning(f"PRICE_TO_BOOK_VALUE [{ticker_symbol}]: No price data returned. Skipping ratio calculation.")
                    results_by_ticker[ticker_symbol] = []
                    continue
                
                logger.debug(f"PRICE_TO_BOOK_VALUE [{ticker_symbol}]: Received {len(price_data)} price points.")

                # 3. Calculate Ratio
                current_ratio_series: List[Dict[str, Any]] = []
                for price_point in price_data:
                    price_date_str_key = 'Date' if 'Date' in price_point else 'Datetime'
                    if price_date_str_key not in price_point:
                        logger.warning(f"PRICE_TO_BOOK_VALUE [{ticker_symbol}]: Price point missing 'Date' or 'Datetime' key: {price_point}")
                        continue
                    
                    price_date_str = price_point[price_date_str_key].split("T")[0]
                    price_value = price_point.get('Close')

                    if price_value is None:
                        current_ratio_series.append({'date': price_date_str, 'value': None})
                        continue

                    bvps_value_for_date = bvps_map.get(price_date_str)

                    if bvps_value_for_date is not None and bvps_value_for_date > 0: # Denominator must be positive
                        ratio_value = float(price_value) / float(bvps_value_for_date)
                        current_ratio_series.append({'date': price_date_str, 'value': ratio_value})
                    else:
                        # logger.debug(f"PRICE_TO_BOOK_VALUE [{ticker_symbol}] on {price_date_str}: BVPS value is {bvps_value_for_date}. Ratio set to None.")
                        current_ratio_series.append({'date': price_date_str, 'value': None})
                
                results_by_ticker[ticker_symbol] = current_ratio_series
                logger.info(f"PRICE_TO_BOOK_VALUE [{ticker_symbol}]: Calculated {len(current_ratio_series)} Price/Book Value points.")

            except Exception as e:
                logger.error(f"PRICE_TO_BOOK_VALUE [{ticker_symbol}]: Unhandled error: {e}", exc_info=True)
                results_by_ticker[ticker_symbol] = []
        
        return results_by_ticker
    # --- END: Price/Book Value Calculation ---

    # --- NEW: Price / (Cash + ST Investments / Share) Calculation ---
    async def _calculate_price_to_cash_plus_st_inv_for_tickers(
        self,
        tickers: List[str],
        start_date_str: Optional[str],
        end_date_str: Optional[str],
        ticker_profiles_cache: Dict[str, Dict[str, Any]] # Keep for consistency, though not directly used by this top-level ratio func
    ) -> Dict[str, List[Dict[str, Any]]]:
        # --- Price / (Cash + ST Investments / Share) Calculation ---
        price_to_cash_results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        
        # First get the cash + ST investments per share data
        cash_st_inv_per_share_data_by_ticker = await self._calculate_cash_plus_st_inv_per_share_for_tickers(
            tickers=tickers,
            start_date_str=start_date_str,
            end_date_str=end_date_str,
            ticker_profiles_cache=ticker_profiles_cache
        )

        for ticker_symbol in tickers:
            try:
                logger.debug(f"PRICE_TO_CASH_PLUS_ST_INV: Processing {ticker_symbol}")
                cash_st_inv_series = cash_st_inv_per_share_data_by_ticker.get(ticker_symbol, [])
                
                # Create a map for quick lookup of cash+ST inv values by date
                cash_st_inv_map = {item['date']: item['value'] for item in cash_st_inv_series if item['value'] is not None}

                # Fetch price data using self.get_price_history
                logger.debug(f"PRICE_TO_CASH_PLUS_ST_INV: Fetching price data for {ticker_symbol} using self.get_price_history from {start_date_str} to {end_date_str}")
                price_data_raw = await self.get_price_history(
                    ticker=ticker_symbol,
                    interval="1d",
                    start_date=start_date_str,
                    end_date=end_date_str
                )

                if not price_data_raw:
                    logger.warning(f"PRICE_TO_CASH_PLUS_ST_INV: No price data returned for {ticker_symbol} from self.get_price_history for range {start_date_str} to {end_date_str}.")
                    price_to_cash_results_by_ticker[ticker_symbol] = []
                    continue
                
                logger.debug(f"PRICE_TO_CASH_PLUS_ST_INV: Received {len(price_data_raw)} price points for {ticker_symbol}.")

                current_ratio_series: List[Dict[str, Any]] = []
                for price_point in price_data_raw:
                    price_date_str_key = 'Date' if 'Date' in price_point else 'Datetime'
                    if price_date_str_key not in price_point:
                        logger.warning(f"PRICE_TO_CASH_PLUS_ST_INV: Price point for {ticker_symbol} missing 'Date' or 'Datetime' key. Point: {price_point}")
                        continue
                    
                    price_date_str = price_point[price_date_str_key].split("T")[0] # YYYY-MM-DD
                    price_value = price_point.get('Close')

                    if price_value is None:
                        current_ratio_series.append({'date': price_date_str, 'value': None})
                        continue

                    # Get the cash+ST inv value for this date
                    cash_st_inv_value = cash_st_inv_map.get(price_date_str)
                    if cash_st_inv_value is None or cash_st_inv_value == 0:
                        current_ratio_series.append({'date': price_date_str, 'value': None})
                        continue

                    # Calculate the ratio
                    ratio = price_value / cash_st_inv_value
                    current_ratio_series.append({'date': price_date_str, 'value': ratio})

                price_to_cash_results_by_ticker[ticker_symbol] = current_ratio_series
                logger.info(f"PRICE_TO_CASH_PLUS_ST_INV: Calculated {len(current_ratio_series)} Price/Cash+ST Inv points for {ticker_symbol}.")

            except httpx.HTTPStatusError as e:
                logger.error(f"PRICE_TO_CASH_PLUS_ST_INV: HTTP error fetching price for {ticker_symbol}: {e.response.status_code} - {e.response.text}", exc_info=False)
                price_to_cash_results_by_ticker[ticker_symbol] = []
            except httpx.RequestError as e:
                logger.error(f"PRICE_TO_CASH_PLUS_ST_INV: Request error fetching price for {ticker_symbol}: {e}", exc_info=False)
                price_to_cash_results_by_ticker[ticker_symbol] = []
            except Exception as e:
                logger.error(f"PRICE_TO_CASH_PLUS_ST_INV: Error processing Price/Cash+ST Inv for {ticker_symbol}: {e}", exc_info=True)
                price_to_cash_results_by_ticker[ticker_symbol] = []

        return price_to_cash_results_by_ticker
    # --- END: Price / (Cash + ST Investments / Share) Calculation ---

    def _parse_date_flex(self, date_input: Union[str, datetime]) -> Optional[datetime]:
        """Helper to parse date string from various common DB formats or handle datetime object."""
        if isinstance(date_input, datetime):
            return date_input.replace(tzinfo=None) if date_input.tzinfo else date_input
        if isinstance(date_input, str):
            formats_to_try = [
                "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", 
                "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d"
            ]
            for fmt in formats_to_try:
                try: return datetime.strptime(date_input, fmt)
                except ValueError: continue
            logger.warning(f"_parse_date_flex: Could not parse date string '{date_input}' with known formats.")
        return None

    async def get_price_history(
        self,
        ticker: str,
        interval: str = '1d',
        period: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get price history for a ticker."""
        logger.info(f"Price history request for {ticker}: interval={interval}, period={period}, start_date={start_date}, end_date={end_date}")
        
        # Use today's date as end_date if none provided
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
            logger.info(f"No end_date provided for {ticker}, using today: {end_date}")

        # Check cache first
        cache_key = f"{ticker}_{interval}_{period}_{start_date}_{end_date}"

        # Try to get from cache first
        cached_data = price_cache.get_price_data(
            ticker=ticker,
            interval=interval,
            period=period,
            start_date=start_date,
            end_date=end_date
        )

        if cached_data is not None:
            logger.info(f"PriceCache HIT for {ticker} {interval} {period} - returning {len(cached_data)} data points")
            return cached_data

        logger.info(f"PriceCache MISS for {ticker} {interval} {period} - fetching from API...")

        # If not in cache, fetch from API
        try:
            # Fetch fresh data from yfinance
            api_data = await self._fetch_price_from_api(ticker, interval, period, start_date, end_date)
            
            if api_data:
                # Store in cache before returning
                price_cache.store_price_data(
                    ticker=ticker,
                    interval=interval,
                    data=api_data,
                    period=period,
                    start_date=start_date,
                    end_date=end_date
                )
                logger.info(f"PriceCache stored {len(api_data)} data points for {ticker} {interval} {period}")
                return api_data
            
            logger.warning(f"No price data returned from API for {ticker}")
            return []
        except Exception as e:
            logger.error(f"Error fetching price data for {ticker}: {str(e)}", exc_info=True)
            raise

    async def _fetch_price_from_api(
        self,
        ticker: str,
        interval: str = '1d',
        period: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch price data from yfinance API.
        """
        try:
            # logger.debug(f"Fetching price data for {ticker} from yfinance: interval={interval}, period={period}, start_date={start_date}, end_date={end_date}")
            
            # Initialize yfinance ticker
            yf_ticker = yf.Ticker(ticker)
            
            # Fetch historical data
            if period:
                # logger.debug(f"Using period={period} for {ticker}")
                hist = yf_ticker.history(period=period, interval=interval)
            else:
                # logger.debug(f"Using start_date={start_date}, end_date={end_date} for {ticker}")
                hist = yf_ticker.history(start=start_date, end=end_date, interval=interval)
            
            if hist.empty:
                logger.warning(f"No price data returned from yfinance for {ticker}")
                return []
            
            # Convert to list of dicts
            price_data = []
            for index, row in hist.iterrows():
                price_point = {
                    'Date': index.strftime('%Y-%m-%d'),
                    'Open': float(row['Open']),
                    'High': float(row['High']),
                    'Low': float(row['Low']),
                    'Close': float(row['Close']),
                    'Volume': int(row['Volume'])
                }
                price_data.append(price_point)
            
            # logger.debug(f"Fetched {len(price_data)} price points for {ticker}")
            return price_data
            
        except Exception as e:
            logger.error(f"Error fetching price data from yfinance for {ticker}: {str(e)}", exc_info=True)
            raise

    # You can add more specific wrappers here for other TTM data or single-record items
    # For example:
    # async def get_latest_ttm_income_statement(self, ticker: str) -> Optional[Dict[str, Any]]:
    #     return await self.get_latest_data_item_payload(ticker, "INCOME_STATEMENT", "TTM")
    # 
    # async def get_latest_ttm_cash_flow_statement(self, ticker: str) -> Optional[Dict[str, Any]]:
    #     return await self.get_latest_data_item_payload(ticker, "CASH_FLOW_STATEMENT", "TTM") 

    # --- NEW: _get_annual_shares_series helper ---
    async def _get_annual_shares_series(
        self, 
        ticker_symbol: str, 
        start_date_str: str, 
        end_date_str: str
    ) -> List[Dict[str, Any]]:
        """
        Fetches and processes annual diluted average shares data from income statements.
        Returns a chronologically sorted list of shares data points.
        """
        logger.debug(f"ANNUAL_SHARES_HELPER [{ticker_symbol}]: Fetching annual shares between {start_date_str} and {end_date_str}")
        
        annual_shares_points: List[Dict[str, Any]] = []
        # yfinance key for diluted average shares from the income statement
        target_shares_key = "Diluted Average Shares" 

        # Parse start and end dates
        parsed_start_date = self._parse_date_flex(start_date_str)
        parsed_end_date = self._parse_date_flex(end_date_str)

        annual_income_statements = await self.db_repo.get_data_items_by_criteria(
            ticker=ticker_symbol,
            item_type="INCOME_STATEMENT", # Shares are from Income Statement
            item_time_coverage="FYEAR",
            start_date=parsed_start_date,
            end_date=parsed_end_date,
            order_by_key_date_desc=False # Fetch ascending for easier processing
        )

        if annual_income_statements:
            # Currency conversion is not applied to shares, but fetching info might be needed if other fields were involved.
            # For shares alone, it's not critical.
            # conversion_info = await self._get_conversion_info_for_ticker(ticker_symbol, {}) 
            
            for item in annual_income_statements:
                payload = item.get('item_data_payload')
                key_date_from_db = item.get('item_key_date')
                
                if not isinstance(payload, dict) or not key_date_from_db:
                    logger.debug(f"ANNUAL_SHARES_HELPER [{ticker_symbol}]: Skipping item due to missing payload or key_date. Item: {item.get('id')}")
                    continue
                
                date_obj = self._parse_date_flex(key_date_from_db)
                if not date_obj:
                    logger.warning(f"ANNUAL_SHARES_HELPER [{ticker_symbol}]: Could not parse date '{key_date_from_db}' for item {item.get('id')}")
                    continue

                shares_value = payload.get(target_shares_key)
                
                if shares_value is not None:
                    try:
                        float_shares = float(shares_value)
                        if float_shares != 0: # Shares count should not be zero for meaningful per-share metrics
                            annual_shares_points.append({'date_obj': date_obj, 'value': float_shares})
                        else:
                            logger.debug(f"ANNUAL_SHARES_HELPER [{ticker_symbol}]: Zero shares value found for date '{date_obj.strftime('%Y-%m-%d')}', value: {shares_value}")
                    except (ValueError, TypeError):
                        logger.warning(f"ANNUAL_SHARES_HELPER [{ticker_symbol}]: Could not parse annual shares value '{shares_value}' for date '{date_obj.strftime('%Y-%m-%d')}'")
                else:
                    logger.debug(f"ANNUAL_SHARES_HELPER [{ticker_symbol}]: '{target_shares_key}' not found in payload for date '{date_obj.strftime('%Y-%m-%d')}'")
        
        annual_shares_points.sort(key=lambda x: x['date_obj']) # Ensure sorted by date
        logger.info(f"ANNUAL_SHARES_HELPER [{ticker_symbol}]: Prepared {len(annual_shares_points)} annual shares points.")
        return annual_shares_points
    # --- END: _get_annual_shares_series helper ---

    # --- NEW: Generic TTM Calculation Helper ---
    def _calculate_ttm_value_generic(
        self,
        current_eval_date: datetime,
        quarterly_points: List[Dict[str, Any]], # Expects [{'date_obj': datetime, 'value_key': float}]
        annual_points: List[Dict[str, Any]],   # Expects [{'date_obj': datetime, 'value_key': float}]
        value_key: str,  # The key in the dicts that holds the numerical value (e.g., 'q_eps', 'annual_cf_per_share')
        debug_identifier: str = "TTM_GENERIC" # Added for identifiable logging
    ) -> Optional[float]:
        """
        Calculates a Trailing Twelve Months (TTM) value for a given metric using the same prioritization
        logic as EPS TTM calculation:

        1. Most recent annual report (if newer than any quarterly report)
        2. 4-quarter TTM pattern (270-380 days span)
        3. 2-quarter semi-annual pattern (170-190 days span)
        4. Most recent annual report as fallback
        5. None if no valid data is available

        Args:
            current_eval_date: The date for which to calculate the TTM value.
            quarterly_points: A list of dictionaries, each containing at least 'date_obj' (datetime)
                              and the specified 'value_key' (float) for quarterly data.
                              Must be sorted chronologically ascending by 'date_obj'.
            annual_points: A list of dictionaries, similar to quarterly_points, for annual data.
                           Must be sorted chronologically ascending by 'date_obj'.
            value_key: The key within the dictionaries in quarterly_points and annual_points
                       that contains the numeric value of the metric.
            debug_identifier: String identifier for logging purposes.

        Returns:
            The calculated TTM value as a float, or None if it cannot be determined.
        """
        # 1. Find most recent records
        most_recent_annual = None
        most_recent_quarterly = None
        
        if annual_points:
            relevant_annual = [p for p in annual_points if p['date_obj'] <= current_eval_date and p.get(value_key) is not None]
            if relevant_annual:
                most_recent_annual = max(relevant_annual, key=lambda x: x['date_obj'])
        
        if quarterly_points:
            relevant_quarterly = [p for p in quarterly_points if p['date_obj'] <= current_eval_date and p.get(value_key) is not None]
            if relevant_quarterly:
                most_recent_quarterly = max(relevant_quarterly, key=lambda x: x['date_obj'])

        # 2. First Priority: Most recent annual if newer than quarterly
        if most_recent_annual and (not most_recent_quarterly or most_recent_annual['date_obj'] >= most_recent_quarterly['date_obj']):
            try:
                annual_value = float(most_recent_annual[value_key])
                logger.debug(
                    f"[{debug_identifier}] EvalDate: {current_eval_date.strftime('%Y-%m-%d')}, "
                    f"Using most recent annual value: {annual_value} from {most_recent_annual['date_obj'].strftime('%Y-%m-%d')}"
                )
                return annual_value
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"[{debug_identifier}] EvalDate: {current_eval_date.strftime('%Y-%m-%d')}, "
                    f"Error converting annual value for key '{value_key}'. Point: {most_recent_annual}. Error: {e}"
                )

        # 3. Second Priority: Quarterly TTM (4Q pattern)
        if most_recent_quarterly:
            # Get all quarters up to current date, sorted by date descending
            relevant_quarters = [p for p in quarterly_points if p['date_obj'] <= current_eval_date and p.get(value_key) is not None]
            relevant_quarters.sort(key=lambda x: x['date_obj'], reverse=True)

            # Check for 4-quarter pattern (270-380 days span)
            if len(relevant_quarters) >= 4:
                for i in range(len(relevant_quarters) - 3):
                    recent_quarters = relevant_quarters[i:i+4]
                    span_days = (recent_quarters[0]['date_obj'] - recent_quarters[3]['date_obj']).days
                    
                    if 270 < span_days < 380:  # Strict bounds for 4-quarter pattern
                        try:
                            quarter_values = [float(p[value_key]) for p in recent_quarters]
                            ttm_value = sum(quarter_values)
                            logger.debug(
                                f"[{debug_identifier}] EvalDate: {current_eval_date.strftime('%Y-%m-%d')}, "
                                f"Using 4Q TTM pattern. Span: {span_days} days, "
                                f"Quarters: {[q['date_obj'].strftime('%Y-%m-%d') for q in recent_quarters]}, "
                                f"Values: {quarter_values}, Sum: {ttm_value}"
                            )
                            return ttm_value
                        except (ValueError, TypeError) as e:
                            logger.warning(
                                f"[{debug_identifier}] EvalDate: {current_eval_date.strftime('%Y-%m-%d')}, "
                                f"Error calculating 4Q TTM sum. Quarters: {recent_quarters}. Error: {e}"
                            )

            # 4. Third Priority: 2-quarter semi-annual pattern
            if len(relevant_quarters) >= 2:
                for i in range(len(relevant_quarters) - 1):
                    recent_quarters = relevant_quarters[i:i+2]
                    days_between = (recent_quarters[0]['date_obj'] - recent_quarters[1]['date_obj']).days
                    
                    if 170 <= days_between <= 190:  # Strict bounds for 2-quarter pattern
                        try:
                            quarter_values = [float(p[value_key]) for p in recent_quarters]
                            ttm_value = sum(quarter_values)
                            logger.debug(
                                f"[{debug_identifier}] EvalDate: {current_eval_date.strftime('%Y-%m-%d')}, "
                                f"Using 2Q semi-annual pattern. Days between: {days_between}, "
                                f"Quarters: {[q['date_obj'].strftime('%Y-%m-%d') for q in recent_quarters]}, "
                                f"Values: {quarter_values}, Sum: {ttm_value}"
                            )
                            return ttm_value
                        except (ValueError, TypeError) as e:
                            logger.warning(
                                f"[{debug_identifier}] EvalDate: {current_eval_date.strftime('%Y-%m-%d')}, "
                                f"Error calculating 2Q semi-annual sum. Quarters: {recent_quarters}. Error: {e}"
                            )

        # 5. Fourth Priority: Fallback to most recent annual
        if most_recent_annual:
            try:
                annual_value = float(most_recent_annual[value_key])
                logger.debug(
                    f"[{debug_identifier}] EvalDate: {current_eval_date.strftime('%Y-%m-%d')}, "
                    f"Using annual fallback value: {annual_value} from {most_recent_annual['date_obj'].strftime('%Y-%m-%d')}"
                )
                return annual_value
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"[{debug_identifier}] EvalDate: {current_eval_date.strftime('%Y-%m-%d')}, "
                    f"Error converting annual fallback value for key '{value_key}'. Point: {most_recent_annual}. Error: {e}"
                )

        # 6. Last Resort: No valid data
        logger.debug(
            f"[{debug_identifier}] EvalDate: {current_eval_date.strftime('%Y-%m-%d')}, "
            f"No valid data found through any method"
        )
        return None
    # --- END: Generic TTM Calculation Helper ---

    async def _get_applicable_shares_for_date(
        self,
        target_date: datetime,
        quarterly_shares_data: List[Dict[str, Any]],
        annual_shares_data: List[Dict[str, Any]],
        ticker_symbol: str
    ) -> Optional[float]:
        """
        Helper method to get the most applicable shares value for a given date.
        Prioritizes quarterly data over annual data.
        
        Args:
            target_date: The date to find shares for
            quarterly_shares_data: List of quarterly shares data points
            annual_shares_data: List of annual shares data points
            ticker_symbol: Ticker symbol for logging
            
        Returns:
            Optional[float]: The most applicable shares value, or None if not found
        """
        try:
            # First try to find the most recent quarterly shares value
            applicable_quarterly = [p for p in quarterly_shares_data if p['date_obj'] <= target_date]
            if applicable_quarterly:
                # Sort by date descending and take the most recent
                most_recent_quarterly = sorted(applicable_quarterly, key=lambda x: x['date_obj'], reverse=True)[0]
                logger.debug(
                    f"SHARES_HELPER [{ticker_symbol}]: Using quarterly shares {most_recent_quarterly['value']} "
                    f"from {most_recent_quarterly['date_obj'].strftime('%Y-%m-%d')} for target date {target_date.strftime('%Y-%m-%d')}"
                )
                return most_recent_quarterly['value']
            
            # If no quarterly data, try annual data
            applicable_annual = [p for p in annual_shares_data if p['date_obj'] <= target_date]
            if applicable_annual:
                # Sort by date descending and take the most recent
                most_recent_annual = sorted(applicable_annual, key=lambda x: x['date_obj'], reverse=True)[0]
                logger.debug(
                    f"SHARES_HELPER [{ticker_symbol}]: Using annual shares {most_recent_annual['value']} "
                    f"from {most_recent_annual['date_obj'].strftime('%Y-%m-%d')} for target date {target_date.strftime('%Y-%m-%d')}"
                )
                return most_recent_annual['value']
            
            logger.warning(
                f"SHARES_HELPER [{ticker_symbol}]: No applicable shares data found for date {target_date.strftime('%Y-%m-%d')}"
            )
            return None
            
        except Exception as e:
            logger.error(
                f"SHARES_HELPER [{ticker_symbol}]: Error getting applicable shares for date {target_date.strftime('%Y-%m-%d')}: {e}",
                exc_info=True
            )
            return None
