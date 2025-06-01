"""
Module for the AnalyticsDataProcessor (ADP).
Handles loading, merging, and filtering of analytics data from various sources.
"""
import logging
import asyncio
from typing import List, Dict, Any, Optional, Callable, Union, Tuple
import httpx # <-- ADDED IMPORT
import json # <-- ADDED IMPORT FOR JSON PARSING IN REPOSITORY (though parsing happens there now)

# Assuming SQLiteRepository is defined here or imported correctly
from .V3_database import SQLiteRepository # <-- ADDED IMPORT
from . import V3_finviz_fetch # <-- ADD THIS IMPORT
from .V3_finviz_fetch import parse_raw_data # <-- ADD THIS SPECIFIC IMPORT
from . import V3_analytics # <-- ADD THIS IMPORT (Already seems to be used, but good to ensure it's explicit if not already)

# Define the base URL for the API, can be moved to config later
BASE_API_URL = "http://localhost:8000" # Adjust if your app runs on a different port

logger = logging.getLogger(__name__)

MAX_UNIQUE_TEXT_SAMPLE_SIZE = 10 # Define the constant for text sample size

# +++ NEW MODULE-LEVEL HELPER FUNCTIONS (COPIED AND ADAPTED) +++

def _adp_parse_finviz_value(value_str: Optional[str]) -> Union[float, int, str, None]:
    """
    Copied and adapted from V3_analytics._parse_finviz_value.
    Attempts to parse a Finviz string value into a float, int, or handles
    common suffixes (K, M, B, T, %) and missing values ('-', 'N/A', '').
    If parsing fails, the original string is returned.
    """
    if value_str is None or not isinstance(value_str, str):
        return None

    original_value_str = value_str 
    processed_value_str = value_str.strip()

    if processed_value_str in ('-', 'N/A', ''):
        return None

    try:
        if processed_value_str.endswith('%'):
            return float(processed_value_str[:-1])

        value_to_convert = processed_value_str
        multiplier = 1
        if processed_value_str.endswith('K'):
            multiplier = 1_000
            value_to_convert = processed_value_str[:-1]
        elif processed_value_str.endswith('M'):
            multiplier = 1_000_000
            value_to_convert = processed_value_str[:-1]
        elif processed_value_str.endswith('B'):
            multiplier = 1_000_000_000
            value_to_convert = processed_value_str[:-1]
        elif processed_value_str.endswith('T'):
             multiplier = 1_000_000_000_000
             value_to_convert = processed_value_str[:-1]

        try:
            num_val = float(value_to_convert) * multiplier
            if num_val == int(num_val):
                return int(num_val)
            return num_val
        except ValueError:
            logger.debug(f"ADP: Could not convert '{value_to_convert}' (from '{original_value_str}') to float. Returning original string.")
            return original_value_str

    except Exception as e:
        logger.warning(f"ADP: Could not parse value '{original_value_str}': {e}")
        return original_value_str

def _adp_preprocess_raw_entries(raw_analytics_entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Copied and adapted from V3_analytics.preprocess_raw_analytics_data.
    Preprocesses raw analytics data strings (e.g., from Finviz, potentially others)
    from the database into structured dictionaries.
    Input format assumes: [{'ticker': str, 'source': str, 'raw_data': str}, ...]
    Raw data format assumes: "key1=value1,key2=value2,key3=value3,..."
    """
    processed_list = []
    for entry in raw_analytics_entries:
        ticker = entry.get('ticker')
        source = entry.get('source')
        raw_data_str = entry.get('raw_data') 
        
        processed_entry = {'ticker': ticker, 'source': source, 'processed_data': {}, 'error': None}

        if not ticker:
            logger.warning(f"ADP: Skipping entry with missing ticker (source: {source}).")
            processed_entry['error'] = "Missing ticker"
            processed_list.append(processed_entry)
            continue
        
        if not source:
             logger.warning(f"ADP: Processing entry with missing source (ticker: {ticker}).")

        if not raw_data_str or not isinstance(raw_data_str, str):
            logger.warning(f"ADP: Skipping entry for ticker {ticker} (source: {source}) due to missing or invalid raw_data.")
            processed_entry['error'] = "Missing or invalid raw_data"
            processed_list.append(processed_entry)
            continue

        try:
            processed_fields = {}
            kv_pairs = raw_data_str.split(',')

            for kv_pair_string in kv_pairs:
                kv_pair_string = kv_pair_string.strip()
                if not kv_pair_string:
                    continue

                parts = kv_pair_string.split('=', 1)
                key = parts[0].strip()

                if not key:
                    logger.debug(f"ADP: Empty key found for ticker {ticker} (source: {source}) in pair '{kv_pair_string}'")
                    continue
                
                value_str_inner = parts[1].strip() if len(parts) > 1 else ''
                parsed_value = _adp_parse_finviz_value(value_str_inner) 
                processed_fields[key] = parsed_value

            processed_entry['processed_data'] = processed_fields

        except Exception as e:
            logger.error(f"ADP: Error processing raw data for ticker {ticker} (source: {source}): {e}", exc_info=True)
            processed_entry['error'] = f"Parsing failed: {e}"

        processed_list.append(processed_entry)
    return processed_list

# --- END OF NEW MODULE-LEVEL HELPERS ---

class AnalyticsDataProcessor:
    def __init__(self, db_repository: SQLiteRepository): # <-- MODIFIED: Accept repository
        """
        Initializes the AnalyticsDataProcessor.
        Dependencies like HTTP clients or pointers to other services can be injected here if needed.
        """
        logger.info("AnalyticsDataProcessor initialized.")
        self.http_client = httpx.AsyncClient(base_url=BASE_API_URL, timeout=1800.0) # <-- INCREASED TIMEOUT
        self.db_repository = db_repository # <-- STORE REPOSITORY INSTANCE

    async def close_http_client(self):
        """Gracefully close the HTTP client."""
        await self.http_client.aclose()
        logger.info("AnalyticsDataProcessor: HTTP client closed.")

    async def _load_finviz_data(self, progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """
        Loads Finviz data by querying the 'analytics_raw' table via the SQLiteRepository.
        Filters for source='finviz' and expects the repository to parse the raw_data JSON.
        """
        logger.info("ADP: Loading Finviz data from analytics_raw table...")
        processed_data: List[Dict[str, Any]] = []
        try:
            all_finviz_raw_entries = await self.db_repository.get_analytics_raw_data_by_source('finviz')
            total_entries = len(all_finviz_raw_entries)
            logger.info(f"ADP: Found {total_entries} raw Finviz entries.")

            for i, entry in enumerate(all_finviz_raw_entries):
                ticker = entry.get('ticker')
                raw_data_str = entry.get('raw_data')
                last_fetched_at = entry.get('last_fetched_at') # Get last_fetched_at

                if not ticker or not raw_data_str:
                    logger.warning(f"ADP Finviz: Skipping entry due to missing ticker or raw_data. Entry: {entry}")
                    continue

                logger.debug(f"ADP Finviz Processing Ticker: {ticker}")
                # logger.debug(f"ADP Finviz raw_data_str from DB (first 200 chars): '{str(raw_data_str)[:200]}'") # Keep for debugging if needed

                try:
                    # Step 1: Parse the raw_data string
                    parsed_finviz_fields = parse_raw_data(raw_data_str)
                    # logger.debug(f"ADP Finviz Parsed fields for {ticker}: {parsed_finviz_fields}") # Keep for debugging

                    # Step 2: Construct the processed_item with prefixed fields
                    processed_item: Dict[str, Any] = {
                        'ticker': ticker,
                        'source': 'finviz', # Explicitly set source
                        'last_fetched_at': last_fetched_at # Include last_fetched_at
                    }

                    # Add prefixed Finviz fields
                    for key, value in parsed_finviz_fields.items():
                        processed_item[f"fv_{key.replace('/', '_').replace(' ', '_').replace('-', '_').replace('.', '_')}"] = value
                    
                    # logger.debug(f"ADP Finviz Final processed_item for {ticker}: {processed_item}") # Keep for debugging

                    processed_data.append(processed_item)

                except Exception as e:
                    logger.error(f"ADP Finviz: Error processing entry for ticker {ticker}. Error: {e}", exc_info=True)
                    # Optionally, append a minimal record or skip
                    processed_data.append({
                        'ticker': ticker,
                        'source': 'finviz',
                        'last_fetched_at': last_fetched_at,
                        'error_processing': str(e),
                        'raw_data': raw_data_str # include raw_data if there was an error
                    })

                if progress_callback and callable(progress_callback):
                    # Simulate progress update
                    await asyncio.sleep(0.01) # Simulate async work
                    progress_callback({
                        "current": i + 1,
                        "total": total_entries,
                        "status": f"Processing Finviz: {ticker} ({i+1}/{total_entries})"
                    })
            
            logger.info(f"ADP: Successfully processed {len(processed_data)} Finviz entries into structured format.")

        except Exception as e:
            logger.error(f"ADP: Error loading/processing Finviz data from DB: {e}", exc_info=True)
            # If there's a general error, we might return an empty list or re-raise
            # For now, returning what has been processed so far or an empty list
        
        return processed_data

    async def _load_yahoo_data(self, progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """
        Loads combined Yahoo data by calling the /api/analytics/data/yahoo_combined endpoint.
        """
        logger.info("ADP: Loading Yahoo combined data...")
        yahoo_data: List[Dict[str, Any]] = []
        endpoint_url = "/api/analytics/data/yahoo_combined"

        # Determine if progress_callback is async or sync
        is_async_callback = asyncio.iscoroutinefunction(progress_callback)

        async def do_progress_update_async(task_name, status, progress, message, count=None):
            payload = {"task_name": task_name, "status": status, "progress": progress, "message": message}
            if count is not None: payload["count"] = count
            if progress_callback: await progress_callback(payload)

        def do_progress_update_sync(task_name, status, progress, message, count=None):
            payload = {"task_name": task_name, "status": status, "progress": progress, "message": message}
            if count is not None: payload["count"] = count
            if progress_callback: progress_callback(payload)

        do_progress_update = do_progress_update_async if is_async_callback else do_progress_update_sync
        
        try:
            if is_async_callback:
                await do_progress_update("load_yahoo_data", "started", 0, f"Fetching from {endpoint_url}")
            else:
                do_progress_update("load_yahoo_data", "started", 0, f"Fetching from {endpoint_url}")
            logger.debug(f"ADP: Calling Yahoo data endpoint: {self.http_client.base_url}{endpoint_url}")
            response = await self.http_client.get(endpoint_url)
            response.raise_for_status() 

            try:
                yahoo_data = response.json()
                if not isinstance(yahoo_data, list):
                    logger.error(f"ADP: Yahoo combined endpoint did not return a list. Received: {type(yahoo_data)}. Response text: {response.text[:200]}")
                    yahoo_data = [] 
            except json.JSONDecodeError:
                logger.error(f"ADP: Failed to decode JSON from Yahoo combined endpoint. Response text: {response.text[:500]}", exc_info=True)
                yahoo_data = []

            logger.info(f"ADP: Successfully fetched {len(yahoo_data)} records from Yahoo combined endpoint.")
            if is_async_callback:
                await do_progress_update("load_yahoo_data", "parsing", 50, "Parsing Yahoo data")
            else:
                do_progress_update("load_yahoo_data", "parsing", 50, "Parsing Yahoo data")

        except httpx.HTTPStatusError as e:
            logger.error(f"ADP: HTTP error loading Yahoo data from {e.request.url}: {e.response.status_code} - {e.response.text}", exc_info=True)
            if is_async_callback:
                await do_progress_update("load_yahoo_data", "failed", 100, f"HTTP error: {e.response.status_code}")
            else:
                do_progress_update("load_yahoo_data", "failed", 100, f"HTTP error: {e.response.status_code}")
            yahoo_data = []
        except httpx.RequestError as e:
            logger.error(f"ADP: Request error loading Yahoo data from {e.request.url}: {e}", exc_info=True)
            if is_async_callback:
                await do_progress_update("load_yahoo_data", "failed", 100, f"Request error: {e}")
            else:
                do_progress_update("load_yahoo_data", "failed", 100, f"Request error: {e}")
            yahoo_data = []
        except Exception as e:
            logger.error(f"ADP: Unexpected error loading Yahoo data: {e}", exc_info=True)
            if is_async_callback:
                await do_progress_update("load_yahoo_data", "failed", 100, f"Unexpected error: {e}")
            else:
                do_progress_update("load_yahoo_data", "failed", 100, f"Unexpected error: {e}")
            yahoo_data = []
        
        final_progress_message = f"Finished fetching Yahoo data ({len(yahoo_data)} records)."
        if is_async_callback:
            await do_progress_update("load_yahoo_data", "completed", 100, final_progress_message, count=len(yahoo_data))
        else:
            do_progress_update("load_yahoo_data", "completed", 100, final_progress_message, count=len(yahoo_data))
        return yahoo_data

    # +++ NEW METHOD: _transform_raw_yahoo_data +++
    def _transform_raw_yahoo_data(self, raw_yahoo_data_list: List[Dict[str, Any]], progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """
        Transforms the raw, structured data from _load_yahoo_data into a flat list
        of dictionaries, with fields prefixed (e.g., yf_tm_shortName, yf_item_income_statement_ttm_totalRevenue).
        This is the format expected by _merge_data.
        """
        logger.info(f"ADP: Transforming {len(raw_yahoo_data_list)} raw Yahoo records...")
        
        is_async_callback = asyncio.iscoroutinefunction(progress_callback)

        async def do_progress_update_async(task_name, status, progress, message, count=None):
            payload = {"task_name": task_name, "status": status, "progress": progress, "message": message}
            if count is not None: payload["count"] = count
            if progress_callback: await progress_callback(payload)
            await asyncio.sleep(0) # Yield control

        def do_progress_update_sync(task_name, status, progress, message, count=None):
            payload = {"task_name": task_name, "status": status, "progress": progress, "message": message}
            if count is not None: payload["count"] = count
            if progress_callback: progress_callback(payload)

        # Since this method is synchronous, we should use the sync helper or ensure the async one is called appropriately
        # For simplicity in a sync method, we'll just call the sync version. If callback is async, it will be wrapped.
        # However, the progress_callback itself might be async and expect to be awaited.
        # The outer _prepare_analytics_components manages awaiting the overall progress callback.
        # Here, we'll call the appropriate helper based on inspection.
        
        _do_progress_update_for_transform = do_progress_update_async if is_async_callback else do_progress_update_sync
        
        # If the method itself is sync, but the callback is async, we might need to run the callback in an event loop.
        # This gets complex. Let's assume the caller (_prepare_analytics_components) handles the async nature of the callback.
        # For now, we'll make the _do_progress_update_for_transform an async def and await it if the callback is async.
        # This requires making _transform_raw_yahoo_data async if progress_callback is async.
        # Simpler: Assume _transform_raw_yahoo_data remains sync, and progress_callback is called without await if it's sync.
        # If it's async, it will be scheduled by the caller's event loop.

        if progress_callback:
            if is_async_callback:
                asyncio.create_task(progress_callback({"task_name":"transform_yahoo_data", "status":"started", "progress":0, "message":"Starting Yahoo data transformation"}))
            else:
                progress_callback({"task_name":"transform_yahoo_data", "status":"started", "progress":0, "message":"Starting Yahoo data transformation"})


        transformed_data_list: List[Dict[str, Any]] = []
        total_records = len(raw_yahoo_data_list)

        for index, raw_ticker_data in enumerate(raw_yahoo_data_list):
            ticker = raw_ticker_data.get("ticker")
            if not ticker:
                logger.warning("ADP Transform Yahoo: Skipping record due to missing ticker.")
                continue

            flat_ticker_data: Dict[str, Any] = {"ticker": ticker, "source": "yahoo"}

            master_data = raw_ticker_data.get("master_data", {})
            if isinstance(master_data, dict):
                for key, value in master_data.items():
                    if key not in ["ticker", "id", "yahoo_uid", "created_at", "updated_at"]: 
                        flat_ticker_data[f"yf_tm_{key}"] = value
            else:
                logger.warning(f"ADP Transform Yahoo ({ticker}): master_data is not a dict or is missing. Skipping master_data fields.")
            
            financial_items = raw_ticker_data.get("financial_items", {})
            if isinstance(financial_items, dict):
                for item_key, item_payload in financial_items.items():
                    if isinstance(item_payload, dict):
                        for field_name, field_value in item_payload.items():
                            flat_ticker_data[f"yf_item_{item_key}_{field_name}"] = field_value
                    else:
                        logger.warning(f"ADP Transform Yahoo ({ticker}): Payload for financial item '{item_key}' is not a dict. Skipping this item. Payload: {str(item_payload)[:100]}")
            else:
                logger.warning(f"ADP Transform Yahoo ({ticker}): financial_items is not a dict or is missing. Skipping financial_items fields.")
            
            transformed_data_list.append(flat_ticker_data)

            if progress_callback and (index + 1) % 20 == 0: # Update more frequently
                progress = int(((index + 1) / total_records) * 100)
                msg = f"Transforming Yahoo: {ticker} ({index+1}/{total_records})"
                payload_update = {"task_name":"transform_yahoo_data", "status":"processing", "progress":progress, "message":msg}
                if is_async_callback:
                    asyncio.create_task(progress_callback(payload_update))
                else:
                    progress_callback(payload_update)
        
        logger.info(f"ADP: Successfully transformed {len(transformed_data_list)} Yahoo records.")
        if progress_callback:
            final_msg = "Yahoo data transformation complete."
            final_payload = {"task_name":"transform_yahoo_data", "status":"completed", "progress":100, "count":len(transformed_data_list), "message":final_msg}
            if is_async_callback:
                 asyncio.create_task(progress_callback(final_payload))
            else:
                progress_callback(final_payload)
        return transformed_data_list
    # --- END NEW METHOD ---

    def _merge_data(self, finviz_data: List[Dict[str, Any]], yahoo_data: List[Dict[str, Any]], progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        logger.info(f"ADP: Merging {len(finviz_data)} Finviz records (expecting 'fv_' prefix) and {len(yahoo_data)} Yahoo records (expecting 'yf_' prefixes)...")
        if progress_callback:
            # Using asyncio.create_task for potentially async callback
            if asyncio.iscoroutinefunction(progress_callback):
                asyncio.create_task(progress_callback(task_name="merge_data", status="started", progress=0, message="Starting data merge"))
            else:
                progress_callback(task_name="merge_data", status="started", progress=0, message="Starting data merge")

        merged_data_dict: Dict[str, Dict[str, Any]] = {}
        
        # --- TEST LOGGING: Define test tickers ---
        test_tickers_to_log = ["META", "GOOG", "NVDA"]
        # --- END TEST LOGGING ---

        # 1. Process Finviz data: Add all Finviz items to the dictionary first.
        # This ensures that if a ticker is only in Finviz, it's included.
        for fv_item in finviz_data:
            ticker = fv_item.get('ticker')
            if not ticker:
                logger.warning(f"ADP Merge: Finviz item missing ticker, skipping: {fv_item.get('fv_Name', 'N/A')}")
                continue
            
            # --- TEST LOGGING: Log Finviz record if it's a test ticker ---
            if ticker in test_tickers_to_log:
                logger.info(f"ADP Merge - PRE-FINVIZ-PROCESS Test Ticker {ticker}: {fv_item}")
            # --- END TEST LOGGING ---

            # If ticker already exists (e.g. duplicate ticker in finviz_data), current item overwrites previous.
            merged_data_dict[ticker] = {**fv_item} 

        if progress_callback:
            # Using create_task for potentially async callback
            if asyncio.iscoroutinefunction(progress_callback):
                asyncio.create_task(progress_callback(task_name="merge_data", status="processing", progress=33, message="Finviz data processed, starting Yahoo merge."))
            else:
                progress_callback(task_name="merge_data", status="processing", progress=33, message="Finviz data processed, starting Yahoo merge.")

        # 2. Process Yahoo data: Merge with existing items or add new ones
        # Yahoo fields are now expected to be prefixed (e.g., yf_tm_shortName, yf_item_income_statement_ttm_totalRevenue)
        # by the _transform_raw_yahoo_data method BEFORE this _merge_data method is called.
        for y_item in yahoo_data:
            ticker = y_item.get('ticker')
            if not ticker:
                logger.warning(f"ADP Merge: Yahoo item missing ticker, skipping: {y_item.get('yf_tm_shortName', 'N/A')}")
                continue

            if ticker in merged_data_dict:
                # Ticker exists (came from Finviz)
                # Add/overwrite Yahoo fields into the existing Finviz item.
                for key, value in y_item.items():
                    if key != 'ticker': # Don't overwrite the ticker itself
                        merged_data_dict[ticker][key] = value
            else:
                # Ticker is new (only in Yahoo data), add the full Yahoo item
                merged_data_dict[ticker] = {**y_item}
        
        if progress_callback:
            # Using create_task for potentially async callback
            if asyncio.iscoroutinefunction(progress_callback):
                asyncio.create_task(progress_callback(task_name="merge_data", status="processing", progress=66, message="Yahoo data merged.")) # Corrected progress message
            else:
                progress_callback(task_name="merge_data", status="processing", progress=66, message="Yahoo data merged.") # Corrected progress message

        # --- TEST LOGGING: Log merged records for test tickers ---
        for test_ticker in test_tickers_to_log:
            if test_ticker in merged_data_dict:
                logger.info(f"ADP Merge - POST-YAHOO-PROCESS Test Ticker {test_ticker}: {merged_data_dict[test_ticker]}")
            else:
                logger.info(f"ADP Merge - POST-YAHOO-PROCESS Test Ticker {test_ticker}: NOT FOUND in merged_data_dict")
        # --- END TEST LOGGING ---
        
        logger.info(f"ADP Merge: Merged data contains {len(merged_data_dict)} unique records.")
        if progress_callback:
            # Using create_task for potentially async callback
            if asyncio.iscoroutinefunction(progress_callback):
                asyncio.create_task(progress_callback(task_name="merge_data", status="completed", progress=100, count=len(merged_data_dict), message="Merge complete."))
            else:
                progress_callback(task_name="merge_data", status="completed", progress=100, count=len(merged_data_dict), message="Merge complete.")
        
        return list(merged_data_dict.values())

    def _generate_field_metadata(self, data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Generates comprehensive metadata for each field in the dataset.
        Calculates type, count, unique values (sample), min/max/avg/median for numerics,
        and an example value.
        """
        if not data:
            return {}

        field_stats: Dict[str, Dict[str, Any]] = {}
        
        # First pass: discover all fields and initialize stats
        all_field_names = set()
        for record in data:
            if isinstance(record, dict):
                for key in record.keys():
                    all_field_names.add(key)

        # Exclude 'ticker' from the list of fields for which metadata is generated,
        # as it's the primary identifier and usually handled separately in UIs.
        if 'ticker' in all_field_names:
            all_field_names.remove('ticker')

        for field_name in all_field_names:
            field_stats[field_name] = {
                "name": field_name,
                "count": 0,
                "type": "unknown", # Will attempt to infer
                "numeric_values": [], # Temp store for median calculation
                "text_values": set(), # Temp store for unique text values
                "min_value": float('inf'),
                "max_value": float('-inf'),
                "sum_value": 0,
                "boolean_true_count": 0,
                "boolean_false_count": 0,
                "all_null_or_empty": True, # Assume all are null/empty initially
                "example_value": None,
                "has_numeric": False,
                "has_text": False,
                "has_boolean": False
            }

        # Second pass: iterate through data to populate stats
        for record_index, record in enumerate(data):
            if not isinstance(record, dict):
                continue
            for field_name, value in record.items():
                if field_name not in field_stats:
                    # This should not happen if all_field_names was comprehensive
                    logger.warning(f"ADP _generate_field_metadata: Field '{field_name}' found in record but not in initial field_stats. Skipping.")
                    continue

                stats = field_stats[field_name]

                # --- DETAILED LOGGING FOR SPECIFIC YAHOO FIELDS ---
                if field_name.startswith("yf_tm_") or field_name.startswith("yf_item_"):
                    if record_index < 5 or field_name == "yf_tm_short_ratio": # Log first 5 records for all yf fields, and all records for yf_tm_short_ratio
                        logger.debug(f"ADP META_GEN_DETAIL: Record {record_index}, Field '{field_name}', Raw Value: '{value}' (Type: {type(value)})")
                # --- END DETAILED LOGGING ---


                # Update example value (first non-null, non-empty string, non-empty list/dict)
                if stats["example_value"] is None:
                    if value is not None and value != '' and value != [] and value != {}:
                        stats["example_value"] = value
                
                # Check for null or empty string, or specific placeholder like '-' often seen in financial data
                is_truly_empty = value is None or str(value).strip() == '' or str(value).strip() == '-'
                
                if not is_truly_empty:
                    stats["all_null_or_empty"] = False
                    stats["count"] += 1
                    
                    # Attempt to convert to number
                    try:
                        num_value = float(value)
                        if not (isinstance(value, bool)): # Don't treat True/False as 1.0/0.0 for numeric stats here
                            stats["has_numeric"] = True
                            stats["numeric_values"].append(num_value)
                            if num_value < stats["min_value"]:
                                stats["min_value"] = num_value
                            if num_value > stats["max_value"]:
                                stats["max_value"] = num_value
                            stats["sum_value"] += num_value
                        else: # It's a boolean
                            stats["has_boolean"] = True
                            if num_value == 1.0: # True
                                stats["boolean_true_count"] +=1
                            else: # False
                                stats["boolean_false_count"] +=1
                            stats["text_values"].add(str(value)) # Also add booleans as text unique values

                    except (ValueError, TypeError):
                        # Not a number, treat as text (or boolean if already identified)
                        if isinstance(value, bool):
                            stats["has_boolean"] = True
                            if value:
                                stats["boolean_true_count"] +=1
                            else:
                                stats["boolean_false_count"] +=1
                            stats["text_values"].add(str(value))
                        elif isinstance(value, str): # Explicitly check for string
                            stats["has_text"] = True
                            stats["text_values"].add(value)
                        elif isinstance(value, (list, dict)): # If it's a list or dict, treat as complex text
                            stats["has_text"] = True
                            try:
                                stats["text_values"].add(json.dumps(value)) # Store JSON string for complex types
                            except TypeError:
                                stats["text_values"].add(str(value)) # Fallback to simple string
                        else: # Other non-numeric, non-boolean, non-string types
                            stats["has_text"] = True # Default to text
                            stats["text_values"].add(str(value))


        # Third pass: finalize stats (calculate averages, medians, determine type)
        final_metadata = {}
        # Sort field_stats by field_name (the key) before creating final_metadata
        sorted_field_stats_items = sorted(field_stats.items())

        for field_name, stats in sorted_field_stats_items: # Iterate over sorted items
            meta_entry: Dict[str, Any] = {"name": field_name, "count": stats["count"]}

            if stats["all_null_or_empty"]:
                meta_entry["type"] = "empty"
            else:
                # Calculate type based on the presence of numeric, text, or boolean values
                if stats["has_numeric"]:
                    meta_entry["type"] = "numeric"
                elif stats["has_text"]:
                    meta_entry["type"] = "text"
                elif stats["has_boolean"]:
                    meta_entry["type"] = "boolean"
                else:
                    meta_entry["type"] = "unknown" # Default to unknown if no valid data found

                # Calculate min, max, avg, median for numeric values
                if stats["numeric_values"]:
                    num_values = sorted(stats["numeric_values"])
                    meta_entry["min_value"] = num_values[0]
                    meta_entry["max_value"] = num_values[-1]
                    meta_entry["avg_value"] = sum(num_values) / len(num_values)
                    
                    # Median
                    n = len(num_values)
                    mid = n // 2
                    if n % 2 == 0: # Even number of values
                        meta_entry["median_value"] = (num_values[mid - 1] + num_values[mid]) / 2
                    else: # Odd number of values
                        meta_entry["median_value"] = num_values[mid]

                # Convert text_values set to list for JSON serialization if needed by frontend
                # Limit the sample size
                unique_sample_list = list(stats["text_values"])
                if len(unique_sample_list) > MAX_UNIQUE_TEXT_SAMPLE_SIZE:
                    meta_entry["unique_values_sample"] = unique_sample_list[:MAX_UNIQUE_TEXT_SAMPLE_SIZE]
                else:
                    meta_entry["unique_values_sample"] = unique_sample_list

            final_metadata[field_name] = meta_entry

        return final_metadata

    # +++ NEW HELPER METHOD +++
    def _calculate_median(self, numeric_values: List[Union[int, float]]) -> Optional[Union[int, float]]:
        """Helper to calculate median for a list of numeric values."""
        if not numeric_values:
            return None
        sorted_values = sorted(numeric_values)
        n = len(sorted_values)
        mid = n // 2
        if n % 2 == 0: # Even number of values
            # Ensure a float is returned if the average of two integers is not an integer
            median_val = (sorted_values[mid - 1] + sorted_values[mid]) / 2.0
            return median_val
        else: # Odd number of values
            return sorted_values[mid]
    # +++ END OF NEW HELPER METHOD +++

    # +++ NEW CORE ORCHESTRATION METHOD FOR CACHING PIPELINE +++
    async def _prepare_analytics_components(self, 
                                           create_original_data: bool, 
                                           create_metadata: bool,
                                           progress_callback: Optional[Callable] = None
                                           ) -> Tuple[Optional[List[Dict[str, Any]]], Optional[Dict[str, Dict[str, Any]]]]:
        """
        Core internal method to prepare analytics data (originalData) and/or metadata (fieldMetadata).
        This method now correctly sources Finviz data from the DB and Yahoo data via an API call,
        then processes and merges them.
        """
        logger.info(f"ADP _prepare_analytics_components: create_original_data={create_original_data}, create_metadata={create_metadata}")
        
        original_data_output: Optional[List[Dict[str, Any]]] = None
        metadata_output: Optional[Dict[str, Dict[str, Any]]] = None
        
        is_async_outer_callback = asyncio.iscoroutinefunction(progress_callback)

        async def _do_overall_progress_callback(message_content: str):
            if progress_callback:
                current_task_name = "prepare_analytics_components"
                stage_progress = 0 
                if "Finviz data preparation" in message_content: stage_progress = 5
                elif "Finviz data loaded" in message_content: stage_progress = 25 # After _load_finviz_data finishes
                elif "Yahoo data loading" in message_content: stage_progress = 30
                elif "Raw Yahoo data loaded" in message_content: stage_progress = 50 # After _load_yahoo_data finishes
                elif "Yahoo data transformation" in message_content: stage_progress = 55 
                elif "Yahoo data transformed" in message_content: stage_progress = 75 # After _transform_raw_yahoo_data finishes
                elif "Merging" in message_content: stage_progress = 80
                elif "Data merged" in message_content: stage_progress = 90 # After _merge_data finishes
                elif "Metadata generation" in message_content: stage_progress = 95
                elif "Metadata generated" in message_content: stage_progress = 98
                elif "Completed" in message_content: stage_progress = 100
                
                update_payload = {"task_name": current_task_name, "status": "running", "progress": stage_progress, "message": message_content}
                if is_async_outer_callback:
                    await progress_callback(update_payload)
                else:
                    progress_callback(update_payload)
                await asyncio.sleep(0) # Yield control

        all_finviz_processed: List[Dict[str, Any]] = []
        all_yahoo_transformed: List[Dict[str, Any]] = []

        try:
            # --- 1. Fetch and Process Finviz Data ---
            await _do_overall_progress_callback("Starting Finviz data preparation...")
            if create_original_data or create_metadata:
                try:
                    all_finviz_processed = await self._load_finviz_data(progress_callback=progress_callback) # Pass original callback through
                    logger.info(f"ADP: Finviz data loaded. Count: {len(all_finviz_processed)}")
                    await _do_overall_progress_callback(f"Finviz data loaded ({len(all_finviz_processed)} records).")
                except Exception as e_finviz_load:
                    logger.error(f"ADP: Error during Finviz data loading: {e_finviz_load}", exc_info=True)
                    await _do_overall_progress_callback(f"Error loading Finviz data: {e_finviz_load}")
                    all_finviz_processed = [] 

            # --- 2. Fetch and Process Yahoo Data ---
            await _do_overall_progress_callback("Starting Yahoo data loading...")
            if create_original_data or create_metadata: 
                try:
                    raw_yahoo_data = await self._load_yahoo_data(progress_callback=progress_callback) # Pass original callback
                    logger.info(f"ADP: Raw Yahoo data loaded. Count: {len(raw_yahoo_data)}")
                    await _do_overall_progress_callback(f"Raw Yahoo data loaded ({len(raw_yahoo_data)} records).")

                    await _do_overall_progress_callback("Starting Yahoo data transformation...")
                    if raw_yahoo_data:
                        # _transform_raw_yahoo_data is SYNC but can take an ASYNC callback that it schedules with create_task
                        all_yahoo_transformed = self._transform_raw_yahoo_data(raw_yahoo_data, progress_callback=progress_callback) # Pass original callback
                        logger.info(f"ADP: Yahoo data transformed. Count: {len(all_yahoo_transformed)}")
                        await _do_overall_progress_callback(f"Yahoo data transformed ({len(all_yahoo_transformed)} records).")
                    else:
                        logger.info("ADP: No raw Yahoo data to transform.")
                        await _do_overall_progress_callback("No raw Yahoo data to transform.")
                        all_yahoo_transformed = []
                except Exception as e_yahoo_prep:
                    logger.error(f"ADP: Error during Yahoo data preparation: {e_yahoo_prep}", exc_info=True)
                    await _do_overall_progress_callback(f"Error preparing Yahoo data: {e_yahoo_prep}")
                    all_yahoo_transformed = []

            # --- 3. Merge Data (if creating originalData) ---
            merged_data: List[Dict[str, Any]] = []
            if create_original_data:
                await _do_overall_progress_callback("Starting data merging (Finviz & Yahoo)...")
                try:
                    # _merge_data is SYNC but can take an ASYNC callback
                    merged_data = self._merge_data(all_finviz_processed, all_yahoo_transformed, progress_callback=progress_callback) # Pass original callback
                    logger.info(f"ADP: Data merged. Total records: {len(merged_data)}")
                    await _do_overall_progress_callback(f"Data merged ({len(merged_data)} records).")
                    original_data_output = merged_data
                except Exception as e_merge:
                    logger.error(f"ADP: Error during data merging: {e_merge}", exc_info=True)
                    await _do_overall_progress_callback(f"Error merging data: {e_merge}")
                    original_data_output = [] 
            
            # --- 4. Generate Metadata (if creating metadata) ---
            if create_metadata:
                await _do_overall_progress_callback("Starting metadata generation...")
                try:
                    data_for_metadata_generation: List[Dict[str, Any]] = []
                    if original_data_output is not None: 
                        data_for_metadata_generation = original_data_output
                        logger.info(f"ADP: Generating metadata from recently merged original_data_output ({len(data_for_metadata_generation)} records).")
                    elif create_original_data is False and (all_finviz_processed or all_yahoo_transformed):
                        logger.info("ADP: Metadata requested without prior originalData creation. Merging Finviz/Yahoo for metadata generation.")
                        data_for_metadata_generation = self._merge_data(all_finviz_processed, all_yahoo_transformed, progress_callback=progress_callback)
                        await _do_overall_progress_callback(f"Data re-merged for metadata ({len(data_for_metadata_generation)} records).")
                    else:
                         logger.warning("ADP: Metadata generation requested, but no data available.")
                         await _do_overall_progress_callback("No data available for metadata generation.")

                    if data_for_metadata_generation:
                        # _generate_field_metadata is SYNC
                        metadata_output = self._generate_field_metadata(data_for_metadata_generation) 
                        logger.info(f"ADP: Metadata generated. Number of fields: {len(metadata_output)}")
                        await _do_overall_progress_callback(f"Metadata generated ({len(metadata_output)} fields).")
                    else:
                        logger.warning("ADP: No data for _generate_field_metadata. Metadata will be empty.")
                        metadata_output = {}
                        await _do_overall_progress_callback("Metadata generation skipped (no data).")     
                except Exception as e_meta:
                    logger.error(f"ADP: Error during metadata generation: {e_meta}", exc_info=True)
                    await _do_overall_progress_callback(f"Error generating metadata: {e_meta}")
                    metadata_output = {} 

            await _do_overall_progress_callback("Completed analytics components preparation.")
            return original_data_output, metadata_output

        except Exception as e_main:
            logger.error(f"ADP: Critical error in _prepare_analytics_components: {e_main}", exc_info=True)
            await _do_overall_progress_callback(f"Critical error in preparation: {e_main}")
            return None if original_data_output is None else original_data_output, \
                   None if metadata_output is None else metadata_output

    async def process_data_for_analytics(self, data_source_selection: str, progress_callback=None):
        # Simulate progress updates if a callback is provided
        def _update_progress(message):
            if progress_callback:
                progress_callback(message)

        _update_progress(f"Processing started for: {data_source_selection}")
        original_data = []
        field_metadata_dict: Dict[str, Dict[str, Any]] = {} # NEW: Will hold rich metadata
        message = "No data processed."

        if data_source_selection == "finviz_only":
            if progress_callback: await progress_callback(task_name="load_finviz_data", status="started", progress=0)
            finviz_data_raw = await self._load_finviz_data()
            if progress_callback: await progress_callback(task_name="load_finviz_data", status="completed", progress=100, count=len(finviz_data_raw))
            
            if finviz_data_raw:
                original_data = finviz_data_raw
                field_metadata_dict = self._generate_field_metadata(original_data) # NEW
                message = f"Successfully loaded {len(original_data)} records from Finviz."
            else:
                message = "No data loaded from Finviz."

        elif data_source_selection == "yahoo_only":
            if progress_callback: await progress_callback(task_name="load_yahoo_data", status="started", progress=0)
            yahoo_data_raw = await self._load_yahoo_data(progress_callback=progress_callback) # Pass callback
            # Progress for flattening is now inside _load_yahoo_data
            
            if yahoo_data_raw:
                original_data = yahoo_data_raw
                field_metadata_dict = self._generate_field_metadata(original_data) # NEW
                message = f"Successfully loaded and processed {len(original_data)} records from Yahoo."

            else:
                message = "No data loaded from Yahoo."

        elif data_source_selection == "both":
            # --- Load Finviz ---
            if progress_callback: await progress_callback(task_name="load_finviz_data", status="started", progress=0)
            finviz_data = await self._load_finviz_data()
            if progress_callback: await progress_callback(task_name="load_finviz_data", status="completed", progress=100, count=len(finviz_data))

            # --- Load Yahoo ---
            if progress_callback: await progress_callback(task_name="load_yahoo_data", status="started", progress=0)
            yahoo_data = await self._load_yahoo_data(progress_callback=progress_callback)
            # Progress for flattening is now inside _load_yahoo_data

            if not finviz_data and not yahoo_data:
                message = "No data loaded from either Finviz or Yahoo."
            elif not finviz_data:
                original_data = yahoo_data
                message = f"Only Yahoo data loaded ({len(yahoo_data)} records)."
            elif not yahoo_data:
                original_data = finviz_data
                message = f"Only Finviz data loaded ({len(finviz_data)} records)."
            else:
                # --- Merge Data ---
                if progress_callback: await progress_callback(task_name="merge_data", status="started", progress=0)
                original_data = self._merge_data(finviz_data, yahoo_data, progress_callback=progress_callback)
                if progress_callback: await progress_callback(task_name="merge_data", status="completed", progress=100, count=len(original_data))
                message = f"Successfully merged {len(finviz_data)} Finviz records and {len(yahoo_data)} Yahoo records into {len(original_data)} records."

            if original_data: # If any data resulted from the selection
                field_metadata_dict = self._generate_field_metadata(original_data)

        else: # Should be caught by API validation, but as a fallback
            message = f"Invalid data_source_selection: {data_source_selection}"
            if progress_callback: await progress_callback(task_name="processing", status="failed", message=message)
            # Consider raising an error here or returning a specific error structure
            return {"originalData": [], "metaData": {"field_metadata": {}, "error": message, "source_selection": data_source_selection}, "message": message}

        _update_progress(f"Processing completed for {data_source_selection}. {message}")
        if progress_callback: await progress_callback(task_name="processing", status="completed", message=message)
        
        # Construct the final metaData object
        final_meta_data = {
            "source_selection": data_source_selection,
            "field_metadata": field_metadata_dict, # Use the new rich metadata
            # "fields": sorted(list(field_metadata_dict.keys())) # If frontend still needs a simple list of names
        }

        # Return a tuple (data, metadata) as expected by the calling endpoint
        logger.info(f"ADP returning {len(original_data)} records and metadata for {data_source_selection}. Message: {message}")
        return original_data, final_meta_data

    # +++ NEW PUBLIC CACHE REFRESH METHODS +++
    async def force_refresh_data_cache(self, progress_callback: Optional[Callable] = None) -> None:
        """
        Forces a refresh of the analytics data cache.
        It generates the original data and stores it in the dedicated cache table.
        Metadata might be generated internally by _prepare_analytics_components if its create_metadata is True,
        but only data is explicitly saved here.
        """
        logger.info("ADP: Forcing refresh of DATA cache...")
        if progress_callback:
            progress_callback({"type":"status", "message": "Starting data cache refresh..."})
        
        try:
            # Call _prepare_analytics_components to get the original_data.
            # Metadata generation is not strictly needed for data-only cache, but if the underlying
            # _prepare_analytics_components generates it as a byproduct when create_original_data=True,
            # we simply ignore the metadata part for this specific cache update.
            # To be more efficient for data-only, _prepare_analytics_components is called with create_metadata=False.
            original_data, _ = await self._prepare_analytics_components(
                create_original_data=True, 
                create_metadata=False, # We don't need to force metadata generation for data cache
                progress_callback=progress_callback
            )

            if original_data is not None:
                logger.info(f"ADP: Data generated for cache ({len(original_data)} records). Saving to DB...")
                try:
                    # Ensure datetime objects are handled if any; typically they should be strings or numbers by now.
                    # Using default=str for json.dumps is a common fallback for non-serializable types.
                    data_json = json.dumps(original_data, default=str) 
                except TypeError as te_json:
                    logger.error(f"ADP: JSON TypeError during data cache serialization: {te_json}", exc_info=True)
                    if progress_callback: progress_callback({"type":"error", "message": f"Data cache: JSON serialization error: {te_json}"})
                    return # Stop if serialization fails
                
                await self.db_repository.update_cached_analytics_data(data_json=data_json)
                logger.info("ADP: Data cache updated successfully.")
                if progress_callback: progress_callback({"type":"status", "message": "Data cache refresh completed successfully."})
            else:
                logger.warning("ADP: No data was generated by _prepare_analytics_components for data cache refresh.")
                if progress_callback: progress_callback({"type":"warning", "message": "Data cache: No data generated to refresh."})
        
        except Exception as e:
            logger.error(f"ADP: Error during data cache refresh: {e}", exc_info=True)
            if progress_callback: progress_callback({"type":"error", "message": f"Data cache refresh failed: {e}"})

    async def force_refresh_metadata_cache(self, progress_callback: Optional[Callable] = None) -> None:
        """
        Forces a refresh of the analytics metadata cache.
        It attempts to use existing cached data if available and valid.
        Otherwise, it generates fresh original data and then metadata,
        then stores the metadata in its dedicated cache table.
        """
        logger.info("ADP: Forcing refresh of METADATA cache...")
        if progress_callback:
            asyncio.create_task(progress_callback({"type":"status", "message": "Starting metadata cache refresh..."})) # Assuming callback can handle being called like this

        original_data_for_metadata: Optional[List[Dict[str, Any]]] = None
        metadata_output: Optional[Dict[str, Any]] = None
        source_of_data = "unknown" # To log whether data came from cache or was freshly generated

        try:
            # Step 1: Try to load originalData from the data cache
            logger.info("ADP Metadata Refresh: Attempting to load data from existing data cache...")
            if progress_callback:
                asyncio.create_task(progress_callback({"type":"status", "message": "Checking data cache..."}))

            cached_data_tuple = await self.db_repository.get_cached_analytics_data()

            if cached_data_tuple:
                data_json_from_cache, generated_at = cached_data_tuple
                logger.info(f"ADP Metadata Refresh: Data cache found (generated at {generated_at}). Deserializing...")
                if progress_callback:
                    asyncio.create_task(progress_callback({"type":"status", "message": f"Data cache found (generated {generated_at}), deserializing..."}))
                try:
                    original_data_for_metadata = json.loads(data_json_from_cache)
                    if not isinstance(original_data_for_metadata, list):
                        logger.warning("ADP Metadata Refresh: Cached data is not a list after deserialization. Fallback to fresh generation.")
                        original_data_for_metadata = None # Force fallback
                        if progress_callback:
                            asyncio.create_task(progress_callback({"type":"warning", "message": "Cached data format error. Will generate fresh data."}))
                    else:
                        logger.info(f"ADP Metadata Refresh: Successfully deserialized {len(original_data_for_metadata)} records from data cache.")
                        source_of_data = f"data_cache (generated_at: {generated_at})"
                except json.JSONDecodeError as e_json:
                    logger.warning(f"ADP Metadata Refresh: JSONDecodeError for cached data: {e_json}. Fallback to fresh generation.")
                    original_data_for_metadata = None # Force fallback
                    if progress_callback:
                         asyncio.create_task(progress_callback({"type":"warning", "message": f"Data cache JSON error: {e_json}. Will generate fresh data."}))
            else:
                logger.info("ADP Metadata Refresh: Data cache is empty. Will generate fresh data.")
                if progress_callback:
                    asyncio.create_task(progress_callback({"type":"status", "message": "Data cache empty. Generating fresh data for metadata."}))

            # Step 2: If data wasn't loaded from cache, generate it freshly
            if original_data_for_metadata is None:
                logger.info("ADP Metadata Refresh: Proceeding to generate fresh data and metadata via _prepare_analytics_components...")
                # Metadata generation requires original_data, so both must be true for _prepare_analytics_components.
                # The 'original_data' part of the tuple will be discarded if we only needed metadata from fresh pull,
                # but it's necessary for the _generate_field_metadata call.
                fresh_original_data, fresh_metadata = await self._prepare_analytics_components(
                    create_original_data=True,
                    create_metadata=True, # Ensure metadata is also generated by _prepare_analytics_components
                    progress_callback=progress_callback
                )
                # Use the metadata directly from _prepare_analytics_components if it was generated fresh
                metadata_output = fresh_metadata
                # We also need original_data if metadata was generated this way to ensure consistency if _generate_field_metadata is called again
                original_data_for_metadata = fresh_original_data 
                source_of_data = "freshly_generated"
                logger.info(f"ADP Metadata Refresh: Fresh data and metadata generated. Original data records: {len(fresh_original_data if fresh_original_data else [])}, Metadata fields: {len(fresh_metadata if fresh_metadata else [])}")

            # Step 3: Generate metadata if not already generated by _prepare_analytics_components (i.e., if data came from cache)
            if metadata_output is None and original_data_for_metadata is not None:
                logger.info(f"ADP Metadata Refresh: Generating metadata from {source_of_data} ({len(original_data_for_metadata)} records)...")
                if progress_callback:
                    asyncio.create_task(progress_callback({"type":"status", "message": f"Generating metadata from {len(original_data_for_metadata)} records from {source_of_data}..."}))
                
                metadata_output = self._generate_field_metadata(original_data_for_metadata)
                logger.info(f"ADP Metadata Refresh: Metadata generated from {source_of_data}. Number of fields: {len(metadata_output if metadata_output else [])}")
                if progress_callback:
                    asyncio.create_task(progress_callback({"type":"status", "message": f"Metadata generated ({len(metadata_output if metadata_output else [])} fields)."}))
            elif metadata_output is not None:
                 logger.info(f"ADP Metadata Refresh: Using metadata directly generated by _prepare_analytics_components (source: {source_of_data}).")


            # Step 4: Save the metadata to cache
            if metadata_output is not None:
                logger.info(f"ADP Metadata Refresh: Metadata (from {source_of_data}) contains {len(metadata_output)} fields. Saving to DB...")
                if progress_callback:
                    asyncio.create_task(progress_callback({"type":"status", "message": f"Saving {len(metadata_output)} metadata fields to cache..."}))
                try:
                    metadata_json = json.dumps(metadata_output, default=str)
                except TypeError as te_json:
                    logger.error(f"ADP: JSON TypeError during metadata cache serialization: {te_json}", exc_info=True)
                    if progress_callback: asyncio.create_task(progress_callback({"type":"error", "message": f"Metadata cache: JSON serialization error: {te_json}"}))
                    return # Stop if serialization fails

                await self.db_repository.update_cached_analytics_metadata(metadata_json=metadata_json)
                logger.info(f"ADP: Metadata cache updated successfully (source of data for generation: {source_of_data}).")
                if progress_callback: asyncio.create_task(progress_callback({"type":"status", "message": "Metadata cache refresh completed successfully."}))
            else:
                logger.warning(f"ADP Metadata Refresh: No metadata was generated (source attempt: {source_of_data}). Metadata cache not updated.")
                if progress_callback: asyncio.create_task(progress_callback({"type":"warning", "message": "Metadata cache: No metadata generated to refresh."}))

        except Exception as e:
            logger.error(f"ADP: Error during metadata cache refresh: {e}", exc_info=True)
            if progress_callback: asyncio.create_task(progress_callback({"type":"error", "message": f"Metadata cache refresh failed: {e}"}))
    # +++ END OF NEW PUBLIC CACHE REFRESH METHODS +++

# Example usage (for testing, would not be here in production)
async def example_progress_reporter(status_update: Dict[str, Any]):
    print(f"Progress Update: {status_update}")

# Remove the old __main__ test block if it instantiates ADP without the repository
# if __name__ == '__main__':
#    # ... old test code ... 