# src/V3_app/yahoo_data_query_srv.py
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union, Tuple
import json
import httpx

from .yahoo_repository import YahooDataRepository
from .currency_utils import get_current_exchange_rate
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
                    value = payload_data.get(payload_key_for_json)
                    
                    # --- NEW: Attempt to match key with spaces if direct match fails ---
                    if value is None:
                        # Construct a key with spaces, e.g., "CashAndCashEquivalents" -> "Cash And Cash Equivalents"
                        # This is a common pattern for yfinance field names.
                        payload_key_with_spaces = ""
                        for i, char in enumerate(payload_key_for_json):
                            if char.isupper() and i > 0: # Add space before uppercase letters, except the first char
                                # Also handle sequences of uppercase letters like "PPE" -> "PPE" not "P P E"
                                if not (payload_key_for_json[i-1].isupper() and ( (i+1 < len(payload_key_for_json) and payload_key_for_json[i+1].islower()) or payload_key_for_json[i-1].islower() ) ):
                                     # More refined check for space insertion
                                    if not (payload_key_for_json[i-1].isupper() and (i + 1 < len(payload_key_for_json) and payload_key_for_json[i+1].islower())):
                                        # Avoid double spaces if previous char was already a space maker for an acronym end
                                        if not (payload_key_for_json[i-1].isupper() and i > 1 and payload_key_for_json[i-2].islower()): # e.g. NetIncome -> Net Income, not Net Income
                                            # General case: lowercase followed by uppercase
                                            if payload_key_for_json[i-1].islower():
                                                payload_key_with_spaces += " "
                                            # Acronym ends, e.g. TotalRevenue -> Total Revenue, CurrentAssets -> Current Assets
                                            # Handle cases like 'NetPPE' -> 'Net PPE' - if prev is upper and current is upper but next is lower
                                            elif payload_key_for_json[i-1].isupper() and \
                                                (i + 1 < len(payload_key_for_json) and payload_key_for_json[i+1].islower()) and \
                                                not (i > 1 and payload_key_for_json[i-2].islower()): # Avoid A BC -> A B C
                                                pass # No space if it's part of an acronym like PPE that should remain together until the end.
                                            # Case for start of new word after an acronym like 'NetWorkingCapital'
                                            elif i > 0 and payload_key_for_json[i-1].isupper() and \
                                                 not(i + 1 < len(payload_key_for_json) and payload_key_for_json[i+1].islower()) and \
                                                 not(i > 1 and payload_key_for_json[i-2].isupper()): # Avoid splitting in middle of acronym
                                                 # This condition is tricky, aiming for "ABCWord" -> "ABC Word"
                                                 # Check if previous was an acronym by looking at char before previous
                                                 if i > 1 and payload_key_for_json[i-2].islower(): # e.g. YFIN -> YFIN
                                                      payload_key_with_spaces += " "


                            payload_key_with_spaces += char
                        
                        # A simpler heuristic for now, as the above is getting complex:
                        # Just try common yfinance patterns: split by uppercase, then try title case.
                        # Example: "CashAndCashEquivalents"
                        # 1. "Cash And Cash Equivalents" (add space before capital)
                        # 2. "Cash and Cash Equivalents" (title case variant, though less common for exact match)

                        # Heuristic 1: Add space before uppercase letters (camel case to spaced title case)
                        spaced_key = ""
                        for i, char in enumerate(payload_key_for_json):
                            if char.isupper() and i > 0 and payload_key_for_json[i-1].islower():
                                spaced_key += " "
                            spaced_key += char
                        
                        if spaced_key != payload_key_for_json: # Check if a transformation actually happened
                            logger.debug(f"[QuerySrv.get_specific_field_timeseries] Direct key '{payload_key_for_json}' not found. Trying with spaces: '{spaced_key}'")
                            value = payload_data.get(spaced_key)
                            if value is not None:
                                logger.info(f"[QuerySrv.get_specific_field_timeseries] Found value using spaced key '{spaced_key}' for original '{payload_key_for_json}'.")
                                payload_key_for_json = spaced_key # Update to the key that worked
                            else:
                                # Heuristic 2: Try title case if spaced_key didn't work (e.g. some yfinance keys might be "Net income" vs "NetIncome")
                                # This is less likely for Balance Sheet items which tend to be TitleCaseWithSpaces
                                title_cased_key = payload_key_for_json.title() # Converts "NetIncome" to "Netincome" - unlikely match.
                                                                              # "CashAndCashEquivalents" -> "Cashandcashequivalents" - also unlikely.
                                # A better title case might be to space it first, then title() each word, but yfinance is usually consistent.
                                # Let's stick to the spaced_key as the primary alternative for now.
                                # If `spaced_key` also failed, value remains None.
                                pass
                    # --- END: Attempt to match key with spaces ---
                    
                    if value is not None:
                        # Apply conversion if needed and possible, and if the value is numeric
                        if rate_to_apply is not None and isinstance(value, (int, float)) and not isinstance(value, bool):
                            # Check item_type and 'shares' keyword before converting
                            CONVERTIBLE_ITEM_TYPES = {"BALANCE_SHEET", "INCOME_STATEMENT", "CASH_FLOW_STATEMENT"}
                            if db_item_type.upper() in CONVERTIBLE_ITEM_TYPES and "shares" not in payload_key_for_json.lower():
                                original_value = value
                                value = value * rate_to_apply
                                logger.debug(f"[QuerySrv.get_specific_field_timeseries] Converted value for {ticker_symbol}, field '{payload_key_for_json}', date {item_key_date_iso_str}: {original_value} -> {value} using rate {rate_to_apply}")
                            # else: // Value not converted due to item_type or 'shares' keyword
                                # logger.debug(f"[QuerySrv.get_specific_field_timeseries] SKIPPED conversion for {ticker_symbol}, field '{payload_key_for_json}', date {item_key_date_iso_str}: item_type={db_item_type}, rate={rate_to_apply}")
                        
                        current_ticker_series.append({'date': item_key_date_iso_str, 'value': value})
                        # logger.debug(f"Found key '{payload_key_for_json}' in dict payload for {ticker_symbol} on {item_key_date_iso_str} with value: {value}") # Redundant if conversion log is active
                    else:
                        logger.warning(f"Key '{payload_key_for_json}' not found or value is None in payload for {ticker_symbol} on {item_key_date_iso_str}. Payload keys: {list(payload_data.keys()) if isinstance(payload_data, dict) else 'Payload not a dict'}")
                                
            except Exception as e:
                logger.error(f"Error processing data for ticker {ticker_symbol}, field {field_identifier}: {e}", exc_info=True)
            
            results_by_ticker[ticker_symbol] = current_ticker_series
            logger.info(f"Collected {len(current_ticker_series)} data points for {ticker_symbol} and field {field_identifier} (parsed as type='{db_item_type}', coverage='{db_item_coverage}', key='{payload_key_for_json}').")

        return results_by_ticker

    async def calculate_synthetic_fundamental_timeseries(
        self,
        fundamental_name: str,
        tickers: List[str],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        logger.info(f"[QuerySrv.calculate_synthetic_fundamental_timeseries] Request for {fundamental_name}, Tickers: {tickers}, Start: {start_date_str}, End: {end_date_str}")
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}

        if fundamental_name.upper() == "EPS_TTM":
            return await self._calculate_eps_ttm_for_tickers(
                tickers,
                start_date_str,
                end_date_str,
                {}
            )
        elif fundamental_name.upper() == "PE_TTM":
            logger.info(f"P/E TTM calculation requested for {tickers} from {start_date_str} to {end_date_str}")
            
            eps_data_by_ticker = await self._calculate_eps_ttm_for_tickers(
                tickers,
                start_date_str,
                end_date_str,
                {}
            )

            pe_results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}

            # Determine the base URL for the internal price history API
            # This is a simplification. In a real app, this would come from config or service discovery.
            # Assuming the service runs on http://localhost:8000 or similar.
            # For now, let's make it a placeholder that would need to be configured.
            # THIS IS A CRITICAL PART THAT NEEDS A REAL URL FOR THE PRICE API WHEN DEPLOYED
            # For local testing, if the main app runs on 8000, this might be http://127.0.0.1:8000
            # It's generally better if services can access data stores directly rather than calling other API endpoints
            # of the same application, but following the "replicate PFC/PFR" instruction which uses an API call.
            price_api_base_url = "http://127.0.0.1:8000" # Placeholder: Needs to be the actual app's URL

            async with httpx.AsyncClient(timeout=30.0) as client: # Increased timeout
                for ticker_symbol in tickers:
                    try:
                        logger.debug(f"PE_TTM: Processing {ticker_symbol}")
                        eps_series = eps_data_by_ticker.get(ticker_symbol, [])
                        if not eps_series:
                            logger.warning(f"PE_TTM: No EPS data for {ticker_symbol}, cannot calculate P/E.")
                            pe_results_by_ticker[ticker_symbol] = []
                            continue

                        # Convert EPS series to a dict for quick lookup
                        eps_map = {item['date']: item['value'] for item in eps_series if item['value'] is not None}

                        # Fetch price data
                        price_start_date_str = start_date_str
                        # Yahoo finance API end_date is exclusive, so add 1 day for price fetch
                        price_end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1)
                        price_end_date_str = price_end_date_obj.strftime("%Y-%m-%d")
                        
                        price_api_url = f"{price_api_base_url}/api/v3/timeseries/price_history"
                        params = {
                            "ticker": ticker_symbol,
                            "interval": "1d",
                            "start_date": price_start_date_str,
                            "end_date": price_end_date_str
                        }
                        logger.debug(f"PE_TTM: Fetching price data for {ticker_symbol} from {price_api_url} with params {params}")
                        
                        response = await client.get(price_api_url, params=params)
                        response.raise_for_status() # Raise an exception for HTTP errors 4xx/5xx
                        price_data_raw = response.json() # List of {'Date', 'Close', ...} or {'Datetime', ...}

                        if not price_data_raw:
                            logger.warning(f"PE_TTM: No price data returned for {ticker_symbol} from {price_start_date_str} to {price_end_date_str}.")
                            pe_results_by_ticker[ticker_symbol] = []
                            continue
                        
                        logger.debug(f"PE_TTM: Received {len(price_data_raw)} price points for {ticker_symbol}.")

                        current_pe_series: List[Dict[str, Any]] = []
                        for price_point in price_data_raw:
                            # Adjusting for potential date key variations from price API
                            price_date_str_key = 'Date' if 'Date' in price_point else 'Datetime'
                            if price_date_str_key not in price_point:
                                logger.warning(f"PE_TTM: Price point for {ticker_symbol} missing 'Date' or 'Datetime' key. Point: {price_point}")
                                continue
                            
                            price_date_str = price_point[price_date_str_key].split("T")[0] # Ensure YYYY-MM-DD format
                            price_value = price_point.get('Close')

                            if price_value is None:
                                current_pe_series.append({'date': price_date_str, 'value': None})
                                continue

                            eps_value_for_date = eps_map.get(price_date_str)

                            if eps_value_for_date is not None and eps_value_for_date > 0: # P/E typically not shown for zero/negative EPS
                                pe_value = float(price_value) / float(eps_value_for_date)
                                current_pe_series.append({'date': price_date_str, 'value': pe_value})
                            else:
                                current_pe_series.append({'date': price_date_str, 'value': None})
                        
                        pe_results_by_ticker[ticker_symbol] = current_pe_series
                        logger.info(f"PE_TTM: Calculated {len(current_pe_series)} P/E points for {ticker_symbol}.")

                    except httpx.HTTPStatusError as e:
                        logger.error(f"PE_TTM: HTTP error fetching price for {ticker_symbol}: {e.response.status_code} - {e.response.text}", exc_info=False)
                        pe_results_by_ticker[ticker_symbol] = []
                    except httpx.RequestError as e:
                        logger.error(f"PE_TTM: Request error fetching price for {ticker_symbol}: {e}", exc_info=False)
                        pe_results_by_ticker[ticker_symbol] = []
                    except Exception as e:
                        logger.error(f"PE_TTM: Error processing P/E for {ticker_symbol}: {e}", exc_info=True)
                        pe_results_by_ticker[ticker_symbol] = []
            return pe_results_by_ticker
        elif fundamental_name.upper() == "CASH_PER_SHARE_TTM":
            logger.info(f"SYNTHETIC_FUNDAMENTAL: '{fundamental_name}' requested. Calling _calculate_cash_per_share_ttm_for_tickers.")
            # Currency conversion for cash and shares components is handled within _calculate_cash_per_share_ttm_for_tickers
            # by its calls to get_specific_field_timeseries.
            results_by_ticker = await self._calculate_cash_per_share_ttm_for_tickers(
                tickers, start_date_str, end_date_str, {}
            )
            logger.info(f"SYNTHETIC_FUNDAMENTAL: Finished calculating '{fundamental_name}'. Results for {len(results_by_ticker)} tickers.")
        elif fundamental_name.upper() == "CASH_PER_SHARE":
            logger.info(f"SYNTHETIC_FUNDAMENTAL: '{fundamental_name}' requested. Calling _calculate_cash_per_share_for_tickers.")
            results_by_ticker = await self._calculate_cash_per_share_for_tickers(
                tickers, start_date_str, end_date_str, {}
            )
            logger.info(f"SYNTHETIC_FUNDAMENTAL: Finished calculating '{fundamental_name}'. Results for {len(results_by_ticker)} tickers.")
        elif fundamental_name.upper() == "CASH_PLUS_ST_INV_PER_SHARE":
            logger.info(f"SYNTHETIC_FUNDAMENTAL: '{fundamental_name}' requested. Calling _calculate_cash_plus_st_inv_per_share_for_tickers.")
            results_by_ticker = await self._calculate_cash_plus_st_inv_per_share_for_tickers(
                tickers, start_date_str, end_date_str, {}
            )
            logger.info(f"SYNTHETIC_FUNDAMENTAL: Finished calculating '{fundamental_name}'. Results for {len(results_by_ticker)} tickers.")
        elif fundamental_name.upper() == "PRICE_TO_CASH_PLUS_ST_INV":
            logger.info(f"SYNTHETIC_FUNDAMENTAL: '{fundamental_name}' requested. Calling _calculate_price_to_cash_plus_st_inv_for_tickers.")
            results_by_ticker = await self._calculate_price_to_cash_plus_st_inv_for_tickers(
                tickers, start_date_str, end_date_str, {} # Passing empty cache
            )
            logger.info(f"SYNTHETIC_FUNDAMENTAL: Finished calculating '{fundamental_name}'. Results for {len(results_by_ticker)} tickers.")
        else:
            logger.warning(f"Unsupported synthetic fundamental requested: {fundamental_name}")
            for ticker in tickers:
                results_by_ticker[ticker] = []
        
        # Ensure all requested tickers have an entry in the results, even if empty
        return results_by_ticker

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
                logger.warning(f"EPS_TTM: Invalid start_date_str '{start_date_str}'. Defaulting to YTD.")
                user_start_date_obj = datetime.today().replace(month=1, day=1)
        else: user_start_date_obj = datetime.today().replace(month=1, day=1)

        if end_date_str:
            try: user_end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d")
            except ValueError: 
                logger.warning(f"EPS_TTM: Invalid end_date_str '{end_date_str}'. Defaulting to today.")
                user_end_date_obj = datetime.today()
        else: user_end_date_obj = datetime.today()
        
        logger.debug(f"EPS_TTM: User date range for calculation: {user_start_date_obj.strftime('%Y-%m-%d')} to {user_end_date_obj.strftime('%Y-%m-%d')}")

        target_net_income_key = "Diluted NI Availto Com Stockholders"
        target_shares_key = "Diluted Average Shares"

        for ticker_symbol in tickers:
            try:
                logger.info(f"EPS_TTM: Processing ticker: {ticker_symbol}")
                ttm_eps_series_for_ticker: List[Dict[str, Any]] = []
                calculation_mode = "Quarterly TTM" # Default mode

                # 1. Attempt to fetch QUARTERLY income statements
                quarterly_lookback_start_obj = user_start_date_obj - timedelta(days=(365 * 1 + 30 * 9))
                logger.debug(f"EPS_TTM: Querying QUARTERLY income statements for {ticker_symbol} from {quarterly_lookback_start_obj.strftime('%Y-%m-%d')} to {user_end_date_obj.strftime('%Y-%m-%d')}")
                
                quarterly_income_statements = await self.db_repo.get_data_items_by_criteria(
                    ticker=ticker_symbol,
                    item_type="INCOME_STATEMENT",
                    item_time_coverage="QUARTER",
                    start_date=quarterly_lookback_start_obj,
                    end_date=user_end_date_obj,
                    order_by_key_date_desc=False
                )

                quarterly_eps_points: List[Dict[str, Any]] = []
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

                        ni_value = current_payload.get(target_net_income_key)
                        shares_value = current_payload.get(target_shares_key)
                        if ni_value is not None and shares_value is not None and shares_value != 0:
                            try: quarterly_eps_points.append({'date_obj': q_date_obj, 'q_eps': float(ni_value) / float(shares_value)})
                            except (ValueError, TypeError): pass # Logged implicitly by lack of points later
                    
                    if quarterly_eps_points:
                        quarterly_eps_points.sort(key=lambda x: x['date_obj'])
                        # Check if we have enough quarterly data to proceed with TTM calculation for the *start* of the user period
                        relevant_for_start = [p for p in quarterly_eps_points if p['date_obj'] <= user_start_date_obj]
                        if len(relevant_for_start) >= 4:
                            logger.info(f"EPS_TTM: Sufficient quarterly data for {ticker_symbol}. Proceeding with TTM calculation.")
                            current_iter_date = user_start_date_obj
                            while current_iter_date <= user_end_date_obj:
                                relevant_q_eps = [p for p in quarterly_eps_points if p['date_obj'] <= current_iter_date]
                                ttm_eps_value: Optional[float] = None
                                if len(relevant_q_eps) >= 4:
                                    last_four_q_eps = sorted(relevant_q_eps, key=lambda x: x['date_obj'], reverse=True)[:4]
                                    if len(last_four_q_eps) == 4: ttm_eps_value = sum(p['q_eps'] for p in last_four_q_eps)
                                ttm_eps_series_for_ticker.append({'date': current_iter_date.strftime("%Y-%m-%d"), 'value': ttm_eps_value})
                                current_iter_date += timedelta(days=1)
                        else:
                            logger.warning(f"EPS_TTM: Insufficient distinct quarterly EPS points ({len(relevant_for_start)} found before start date) for {ticker_symbol} to reliably calculate TTM. Will attempt fallback to annual data.")
                            quarterly_eps_points = [] # Clear to trigger fallback
                    else:
                        logger.warning(f"EPS_TTM: No valid quarterly EPS points derived for {ticker_symbol} from {len(quarterly_income_statements)} statements. Attempting fallback to annual.")
                else:
                    logger.info(f"EPS_TTM: No QUARTERLY income statements found for {ticker_symbol}. Attempting fallback to annual data.")

                # 2. Fallback to ANNUAL (FYEAR) data if quarterly TTM calculation was not performed
                if not ttm_eps_series_for_ticker: # This condition means quarterly TTM was not successful or skipped
                    calculation_mode = "Annual Fallback"
                    logger.info(f"EPS_TTM ({calculation_mode}): Attempting to use ANNUAL income statements for {ticker_symbol}.")
                    annual_lookback_start_obj = user_start_date_obj - timedelta(days=(365 * 2 + 30 * 3)) # Approx 2 years 3 months for annual reports
                    logger.debug(f"EPS_TTM ({calculation_mode}): Querying ANNUAL income statements for {ticker_symbol} from {annual_lookback_start_obj.strftime('%Y-%m-%d')} to {user_end_date_obj.strftime('%Y-%m-%d')}")

                    annual_income_statements = await self.db_repo.get_data_items_by_criteria(
                        ticker=ticker_symbol,
                        item_type="INCOME_STATEMENT",
                        item_time_coverage="FYEAR",
                        start_date=annual_lookback_start_obj,
                        end_date=user_end_date_obj,
                        order_by_key_date_desc=False
                    )

                    if not annual_income_statements:
                        logger.warning(f"EPS_TTM ({calculation_mode}): No ANNUAL income statements found for {ticker_symbol}. No EPS data will be available.")
                        results_by_ticker[ticker_symbol] = [] # Explicitly empty
                        continue # Next ticker
                    
                    logger.info(f"EPS_TTM ({calculation_mode}): Found {len(annual_income_statements)} ANNUAL statements for {ticker_symbol}.")
                    annual_eps_points: List[Dict[str, Any]] = []
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

                        ni_value = current_payload.get(target_net_income_key)
                        shares_value = current_payload.get(target_shares_key)
                        if ni_value is not None and shares_value is not None and shares_value != 0:
                            try: annual_eps_points.append({'date_obj': fy_date_obj, 'annual_eps': float(ni_value) / float(shares_value)})
                            except (ValueError, TypeError): pass
                    
                    if not annual_eps_points:
                        logger.warning(f"EPS_TTM ({calculation_mode}): No valid annual EPS points derived for {ticker_symbol}. No EPS data.")
                        results_by_ticker[ticker_symbol] = []
                        continue

                    annual_eps_points.sort(key=lambda x: x['date_obj'])
                    
                    # Generate daily series using last known annual EPS
                    current_iter_date = user_start_date_obj
                    last_known_annual_eps: Optional[float] = None
                    processed_annual_eps_series: List[Dict[str, Any]] = [] # Temp list for this mode

                    # Find initial EPS for the start of the period
                    initial_eps_found = False
                    for point in reversed(annual_eps_points):
                        if point['date_obj'] <= current_iter_date:
                            last_known_annual_eps = point['annual_eps']
                            initial_eps_found = True
                            break
                    if not initial_eps_found and annual_eps_points: # If no point before start, use earliest point if it's after start and user range is short
                         if annual_eps_points[0]['date_obj'] > current_iter_date and (user_end_date_obj - user_start_date_obj).days < 400 : # Heuristic: only for short ranges
                             pass # last_known_annual_eps remains None, it will be picked up in the loop
                    
                    while current_iter_date <= user_end_date_obj:
                        # Update last_known_annual_eps if a new annual report is effective for this date
                        for point in annual_eps_points:
                            if point['date_obj'] <= current_iter_date:
                                last_known_annual_eps = point['annual_eps'] # Will take the latest one due to sorted points
                            elif point['date_obj'] > current_iter_date:
                                break # Optimization: points are sorted by date
                        
                        processed_annual_eps_series.append({
                            'date': current_iter_date.strftime("%Y-%m-%d"), 
                            'value': last_known_annual_eps
                        })
                        current_iter_date += timedelta(days=1)
                    ttm_eps_series_for_ticker = processed_annual_eps_series # Assign to the main series variable

                # Final assignment to results_by_ticker
                if ttm_eps_series_for_ticker:
                    results_by_ticker[ticker_symbol] = ttm_eps_series_for_ticker
                    logger.info(f"EPS_TTM ({calculation_mode}): Generated {len(ttm_eps_series_for_ticker)} EPS points for {ticker_symbol}.")
                else:
                    # This case should ideally be caught earlier (e.g., no annual data found)
                    logger.warning(f"EPS_TTM: No EPS series (Quarterly or Annual Fallback) could be generated for {ticker_symbol}.")
                    results_by_ticker[ticker_symbol] = []

            except Exception as e:
                logger.error(f"EPS_TTM: Critical error for {ticker_symbol}: {e}", exc_info=True)
                results_by_ticker[ticker_symbol] = []
        return results_by_ticker

    # --- MODIFIED: Cash/Share Calculation (Removed TTM) ---
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
        primary_shares_field_id = "yf_item_income_statement_quarterly_DilutedAverageShares" # Or consider a direct shares outstanding from BS if more appropriate for point-in-time
        fallback_shares_field_id = "yf_item_balance_sheet_quarterly_ShareIssued"

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

                logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: Fetching primary shares data ({primary_shares_field_id})")
                shares_data_primary_raw = await self.get_specific_field_timeseries(
                    field_identifier=primary_shares_field_id,
                    tickers=[ticker_symbol],
                    start_date_str=fundamental_query_start_date_str,
                    end_date_str=fundamental_query_end_date_str
                )
                logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: Raw primary shares data: {shares_data_primary_raw.get(ticker_symbol)}")
                quarterly_shares_points: List[Dict[str, Any]] = []
                if shares_data_primary_raw.get(ticker_symbol):
                    for item in shares_data_primary_raw[ticker_symbol]:
                        date_obj = self._parse_date_flex(item.get('date'))
                        value = item.get('value')
                        if date_obj and value is not None and float(value) != 0:
                            try: quarterly_shares_points.append({'date_obj': date_obj, 'value': float(value), 'source': 'primary'})
                            except (ValueError, TypeError): pass
                    logger.info(f"CASH_PER_SHARE [{ticker_symbol}]: Parsed {len(quarterly_shares_points)} primary quarterly shares points.")
                    logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: Parsed primary shares points: {quarterly_shares_points[:5]}...")
                # else: # Note: Removed the skip here, allow fallback to try even if primary fails completely
                #    logger.warning(f"CASH_PER_SHARE [{ticker_symbol}]: No primary shares data found using {primary_shares_field_id}. Will attempt fallback.")


                logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: Fetching fallback shares data ({fallback_shares_field_id})")
                shares_data_fallback_raw = await self.get_specific_field_timeseries(
                    field_identifier=fallback_shares_field_id,
                    tickers=[ticker_symbol],
                    start_date_str=fundamental_query_start_date_str,
                    end_date_str=fundamental_query_end_date_str
                )
                logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: Raw fallback shares data: {shares_data_fallback_raw.get(ticker_symbol)}")
                fallback_shares_added_count = 0
                if shares_data_fallback_raw.get(ticker_symbol):
                    for item in shares_data_fallback_raw[ticker_symbol]:
                        date_obj = self._parse_date_flex(item.get('date'))
                        value = item.get('value')
                        if date_obj and value is not None and float(value) != 0:
                            is_duplicate_date = any(sp['date_obj'] == date_obj for sp in quarterly_shares_points)
                            if not is_duplicate_date:
                                try: 
                                    quarterly_shares_points.append({'date_obj': date_obj, 'value': float(value), 'source': 'fallback'})
                                    fallback_shares_added_count +=1
                                except (ValueError, TypeError): pass
                    logger.info(f"CASH_PER_SHARE [{ticker_symbol}]: Added {fallback_shares_added_count} fallback quarterly shares points.")
                
                quarterly_shares_points.sort(key=lambda x: x['date_obj'])
                logger.debug(f"CASH_PER_SHARE [{ticker_symbol}]: Combined and sorted shares points ({len(quarterly_shares_points)} total): {quarterly_shares_points[:5]}...")
                
                if not quarterly_cash_points:
                    logger.warning(f"CASH_PER_SHARE [{ticker_symbol}]: No valid quarterly cash points derived after parsing. No Cash/Share data.")
                    results_by_ticker[ticker_symbol] = []
                    continue
                if not quarterly_shares_points:
                    logger.warning(f"CASH_PER_SHARE [{ticker_symbol}]: No valid quarterly shares points (primary or fallback) after parsing. No Cash/Share data.")
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
        primary_shares_field_id = "yf_item_income_statement_quarterly_DilutedAverageShares"
        fallback_shares_field_id = "yf_item_balance_sheet_quarterly_ShareIssued"

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
                logger.debug(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: Fetching primary shares data ({primary_shares_field_id})")
                shares_data_primary_raw = await self.get_specific_field_timeseries(
                    field_identifier=primary_shares_field_id,
                    tickers=[ticker_symbol],
                    start_date_str=fundamental_query_start_date_str,
                    end_date_str=fundamental_query_end_date_str
                )
                logger.debug(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: Raw primary shares data: {shares_data_primary_raw.get(ticker_symbol)}")
                quarterly_shares_points: List[Dict[str, Any]] = []
                if shares_data_primary_raw.get(ticker_symbol):
                    for item in shares_data_primary_raw[ticker_symbol]:
                        date_obj = self._parse_date_flex(item.get('date'))
                        value = item.get('value')
                        if date_obj and value is not None and float(value) != 0:
                            try: quarterly_shares_points.append({'date_obj': date_obj, 'value': float(value), 'source': 'primary'})
                            except (ValueError, TypeError): pass
                    logger.info(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: Parsed {len(quarterly_shares_points)} primary quarterly shares points.")

                logger.debug(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: Fetching fallback shares data ({fallback_shares_field_id})")
                shares_data_fallback_raw = await self.get_specific_field_timeseries(
                    field_identifier=fallback_shares_field_id,
                    tickers=[ticker_symbol],
                    start_date_str=fundamental_query_start_date_str,
                    end_date_str=fundamental_query_end_date_str
                )
                logger.debug(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: Raw fallback shares data: {shares_data_fallback_raw.get(ticker_symbol)}")
                fallback_shares_added_count = 0
                if shares_data_fallback_raw.get(ticker_symbol):
                    for item in shares_data_fallback_raw[ticker_symbol]:
                        date_obj = self._parse_date_flex(item.get('date'))
                        value = item.get('value')
                        if date_obj and value is not None and float(value) != 0:
                            is_duplicate_date = any(sp['date_obj'] == date_obj for sp in quarterly_shares_points)
                            if not is_duplicate_date:
                                try: 
                                    quarterly_shares_points.append({'date_obj': date_obj, 'value': float(value), 'source': 'fallback'})
                                    fallback_shares_added_count +=1
                                except (ValueError, TypeError): pass
                    logger.info(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: Added {fallback_shares_added_count} fallback quarterly shares points.")
                
                quarterly_shares_points.sort(key=lambda x: x['date_obj'])
                
                if not quarterly_cash_points:
                    logger.warning(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: No valid quarterly cash+ST points. No data.")
                    results_by_ticker[ticker_symbol] = []
                    continue
                if not quarterly_shares_points:
                    logger.warning(f"CASH_PLUS_ST_INV_PER_SHARE [{ticker_symbol}]: No valid quarterly shares points. No data.")
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

    # --- NEW: Price / (Cash + ST Investments / Share) Calculation ---
    async def _calculate_price_to_cash_plus_st_inv_for_tickers(
        self,
        tickers: List[str],
        start_date_str: Optional[str],
        end_date_str: Optional[str],
        ticker_profiles_cache: Dict[str, Dict[str, Any]] # Keep for consistency, though not directly used by this top-level ratio func
    ) -> Dict[str, List[Dict[str, Any]]]:
        logger.info(f"PRICE_TO_CASH_PLUS_ST_INV: Calculation requested for {tickers} from {start_date_str} to {end_date_str}")

        # 1. Get "Cash + ST Investments / Share" data
        cash_st_inv_per_share_data_by_ticker = await self._calculate_cash_plus_st_inv_per_share_for_tickers(
            tickers,
            start_date_str,
            end_date_str,
            ticker_profiles_cache # Pass cache along
        )

        price_to_cash_results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        price_api_base_url = "http://127.0.0.1:8000" # Placeholder: Needs to be the actual app's URL

        async with httpx.AsyncClient(timeout=30.0) as client:
            for ticker_symbol in tickers:
                try:
                    logger.debug(f"PRICE_TO_CASH_PLUS_ST_INV: Processing {ticker_symbol}")
                    cash_st_inv_series = cash_st_inv_per_share_data_by_ticker.get(ticker_symbol, [])
                    
                    if not cash_st_inv_series:
                        logger.warning(f"PRICE_TO_CASH_PLUS_ST_INV: No 'Cash+ST Inv/Share' data for {ticker_symbol}, cannot calculate Price/Cash+ST Inv.")
                        price_to_cash_results_by_ticker[ticker_symbol] = []
                        continue

                    # Convert Cash+ST Inv/Share series to a dict for quick lookup by date
                    cash_st_inv_map = {item['date']: item['value'] for item in cash_st_inv_series if item['value'] is not None}

                    # 2. Fetch price data
                    # Ensure start_date_str and end_date_str are valid for the price API
                    # The price API requires start_date and end_date.
                    # _calculate_cash_plus_st_inv_per_share_for_tickers has defaults if these are None.
                    # We should ensure we use the same effective dates for price fetching.
                    
                    # Use the provided start_date_str and end_date_str directly.
                    # If they were None for the cash_per_share call, they will be None here too,
                    # and the price API might have its own defaults or error out.
                    # It's better if calculate_synthetic_fundamental_timeseries enforces date provision for such ratios.
                    # For now, assume start_date_str and end_date_str are provided as per P/E TTM logic.
                    
                    query_start_date = start_date_str
                    query_end_date = end_date_str
                    
                    if not query_start_date or not query_end_date:
                        # Fallback to a default range if not provided, e.g., last 1 year.
                        # This should ideally be handled by the calling layer or have consistent defaults.
                        default_end_obj = datetime.now()
                        default_start_obj = default_end_obj - timedelta(days=365)
                        query_start_date = default_start_obj.strftime("%Y-%m-%d")
                        query_end_date = default_end_obj.strftime("%Y-%m-%d")
                        logger.warning(f"PRICE_TO_CASH_PLUS_ST_INV [{ticker_symbol}]: start_date or end_date not provided. Defaulting to 1 year: {query_start_date} to {query_end_date}")

                    # Yahoo finance API end_date for price history is exclusive, so add 1 day for price fetch
                    price_api_end_date_obj = datetime.strptime(query_end_date, "%Y-%m-%d") + timedelta(days=1)
                    price_api_end_date_str = price_api_end_date_obj.strftime("%Y-%m-%d")
                    
                    price_api_url = f"{price_api_base_url}/api/v3/timeseries/price_history"
                    params = {
                        "ticker": ticker_symbol,
                        "interval": "1d",
                        "start_date": query_start_date,
                        "end_date": price_api_end_date_str
                    }
                    logger.debug(f"PRICE_TO_CASH_PLUS_ST_INV: Fetching price data for {ticker_symbol} from {price_api_url} with params {params}")
                    
                    response = await client.get(price_api_url, params=params)
                    response.raise_for_status()
                    price_data_raw = response.json()

                    if not price_data_raw:
                        logger.warning(f"PRICE_TO_CASH_PLUS_ST_INV: No price data returned for {ticker_symbol} from {query_start_date} to {query_end_date}.")
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

                        cash_st_inv_value_for_date = cash_st_inv_map.get(price_date_str)

                        if cash_st_inv_value_for_date is not None and cash_st_inv_value_for_date > 0: # Denominator must be positive
                            ratio_value = float(price_value) / float(cash_st_inv_value_for_date)
                            current_ratio_series.append({'date': price_date_str, 'value': ratio_value})
                        else:
                            # Log if cash_st_inv_value_for_date is missing for a price date, or if it's zero/negative
                            # logger.debug(f"PRICE_TO_CASH_PLUS_ST_INV [{ticker_symbol}] on {price_date_str}: Cash+ST Inv/Share value is {cash_st_inv_value_for_date}. Ratio set to None.")
                            current_ratio_series.append({'date': price_date_str, 'value': None})
                    
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

    # You can add more specific wrappers here for other TTM data or single-record items
    # For example:
    # async def get_latest_ttm_income_statement(self, ticker: str) -> Optional[Dict[str, Any]]:
    #     return await self.get_latest_data_item_payload(ticker, "INCOME_STATEMENT", "TTM")
    # 
    # async def get_latest_ttm_cash_flow_statement(self, ticker: str) -> Optional[Dict[str, Any]]:
    #     return await self.get_latest_data_item_payload(ticker, "CASH_FLOW_STATEMENT", "TTM") 