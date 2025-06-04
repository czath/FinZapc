"""
Module for the AnalyticsDataProcessor (ADP).
Handles loading, merging, and filtering of analytics data from various sources.
"""
import logging
import asyncio
from typing import List, Dict, Any, Optional, Callable, Union, Tuple
import json
from fastapi.concurrency import run_in_threadpool

from .V3_database import SQLiteRepository
from . import V3_finviz_fetch
from .V3_finviz_fetch import parse_raw_data
from . import V3_analytics

# --- ADD IMPORTS for direct Yahoo data handling ---
from .V3_yahoo_fetch import YahooDataRepository
from .yahoo_data_query_srv import YahooDataQueryService
# --- END ADD IMPORTS ---

logger = logging.getLogger(__name__)

MAX_UNIQUE_TEXT_SAMPLE_SIZE = 10

# --- COPIED TARGET_ITEM_TYPES from V3_backend_api.py ---
# Ideally, this would be in a shared constants module
TARGET_ITEM_TYPES = [
    # Type (query string, lowercase), Coverage (actual in DB), Key in output dict
    ('analyst_price_targets', "CUMULATIVE_SNAPSHOT", 'analyst_price_targets'),
    ('forecast_summary', "CUMULATIVE", 'forecast_summary'),
    ('balance_sheet', "FYEAR", 'balance_sheet_annual'),
    ('income_statement', "FYEAR", 'income_statement_annual'),
    ('cash_flow_statement', "FYEAR", 'cash_flow_annual'),
    ('balance_sheet', "QUARTER", 'balance_sheet_quarterly'),
    ('income_statement', "QUARTER", 'income_statement_quarterly'),
    ('cash_flow_statement', "QUARTER", 'cash_flow_quarterly'),
    ('income_statement', "TTM", 'income_statement_ttm'),
    ('cash_flow_statement', "TTM", 'cash_flow_ttm'),
]
# --- END COPIED TARGET_ITEM_TYPES ---

# +++ EXISTING MODULE-LEVEL HELPER FUNCTIONS (_adp_parse_finviz_value, _adp_preprocess_raw_entries) +++
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
# --- END OF EXISTING MODULE-LEVEL HELPERS ---

class AnalyticsDataProcessor:
    def __init__(self, db_repository: SQLiteRepository):
        logger.info("AnalyticsDataProcessor initialized.")
        self.db_repository = db_repository
        # --- Initialize Yahoo specific repositories/services ---
        if not hasattr(self.db_repository, 'database_url') or not self.db_repository.database_url:
            err_msg = "ADP Critical: db_repository does not have a valid database_url attribute."
            logger.error(err_msg)
            raise ValueError(err_msg)
        
        try:
            self.yahoo_db_repo = YahooDataRepository(database_url=self.db_repository.database_url)
            self.yahoo_query_service = YahooDataQueryService(db_repo=self.yahoo_db_repo)
            logger.info("ADP: YahooDataRepository and YahooDataQueryService initialized.")
        except Exception as e:
            logger.error(f"ADP: Failed to initialize YahooDataRepository/YahooDataQueryService: {e}", exc_info=True)
            # Depending on how critical these are, you might re-raise or handle appropriately
            raise  # Re-raise for now, as these are essential for _load_yahoo_data

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
                last_fetched_at = entry.get('last_fetched_at') 

                if not ticker or not raw_data_str:
                    logger.warning(f"ADP Finviz: Skipping entry due to missing ticker or raw_data. Entry: {entry}")
                    continue

                try:
                    parsed_finviz_fields = await run_in_threadpool(parse_raw_data, raw_data_str)
                    
                    processed_item: Dict[str, Any] = {
                        'ticker': ticker,
                        'source': 'finviz', 
                        'last_fetched_at': last_fetched_at
                    }
                    for key, value in parsed_finviz_fields.items():
                        processed_item[f"fv_{key.replace('/', '_').replace(' ', '_').replace('-', '_').replace('.', '_')}"] = value
                    
                    processed_data.append(processed_item)

                except Exception as e:
                    logger.error(f"ADP Finviz: Error processing entry for ticker {ticker}. Error: {e}", exc_info=True)
                    processed_data.append({
                        'ticker': ticker,
                        'source': 'finviz',
                        'last_fetched_at': last_fetched_at,
                        'error_processing': str(e),
                        'raw_data': raw_data_str 
                    })

                if progress_callback and callable(progress_callback):
                    await asyncio.sleep(0.001) # Reduced sleep, was 0.01
                    progress_payload = {
                        "current": i + 1,
                        "total": total_entries,
                        "status": f"Processing Finviz: {ticker} ({i+1}/{total_entries})"
                    }
                    if asyncio.iscoroutinefunction(progress_callback):
                        await progress_callback(progress_payload)
                    else:
                        progress_callback(progress_payload)
            
            logger.info(f"ADP: Successfully processed {len(processed_data)} Finviz entries into structured format.")

        except Exception as e:
            logger.error(f"ADP: Error loading/processing Finviz data from DB: {e}", exc_info=True)
        
        return processed_data

    async def _load_yahoo_data(self, progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        logger.info("ADP: Loading Yahoo combined data directly using services...")
        is_async_callback = asyncio.iscoroutinefunction(progress_callback)

        async def do_progress_update(task_name, status, progress_percent, message, count=None, total_count=None):
            payload = {"task_name": task_name, "status": status, "progress": progress_percent, "message": message}
            if count is not None: payload["count"] = count
            if total_count is not None: payload["total_count"] = total_count
            if progress_callback:
                if is_async_callback:
                    await progress_callback(payload)
                else:
                    progress_callback(payload)
                await asyncio.sleep(0)

        await do_progress_update("load_yahoo_data", "started", 0, "Initializing Yahoo data load.")

        combined_data_list: List[Dict[str, Any]] = []
        try:
            await do_progress_update("load_yahoo_data", "running", 5, "Fetching master tickers...")
            master_tickers = await self.yahoo_db_repo.get_all_master_tickers()
            if not master_tickers:
                logger.warning("ADP Yahoo: No tickers found in Yahoo master table.")
                await do_progress_update("load_yahoo_data", "completed", 100, "No master tickers found.", count=0)
                return []
            
            total_master_tickers = len(master_tickers)
            logger.info(f"ADP Yahoo: Found {total_master_tickers} tickers in master table.")
            await do_progress_update("load_yahoo_data", "running", 10, f"Found {total_master_tickers} master tickers. Fetching master data...")

            all_master_data_list = await self.yahoo_db_repo.get_master_data_for_analytics()
            all_master_data_map = {item['ticker']: item for item in all_master_data_list}
            await do_progress_update("load_yahoo_data", "running", 20, "Master data fetched. Preparing item fetches...")

            async def fetch_ticker_combined_data_internal(ticker_idx: int, ticker_symbol: str):
                current_ticker_progress_start = 20 + int((ticker_idx / total_master_tickers) * 70)
                master_data = all_master_data_map.get(ticker_symbol, {"ticker": ticker_symbol})
                financial_items = {}
                
                item_fetch_tasks = []
                for item_type, item_coverage, output_key in TARGET_ITEM_TYPES:
                    item_fetch_tasks.append(
                        asyncio.create_task(
                            self.yahoo_query_service.get_latest_data_item_payload(ticker_symbol, item_type, item_coverage),
                            name=f"ADP-{ticker_symbol}-{output_key}"
                        )
                    )
                
                item_results = await asyncio.gather(*item_fetch_tasks, return_exceptions=True)

                for i, result in enumerate(item_results):
                    _, _, output_key = TARGET_ITEM_TYPES[i]
                    if isinstance(result, Exception):
                        logger.warning(f"ADP Yahoo: Failed to fetch item {output_key} for ticker {ticker_symbol}: {result}")
                    elif result is not None:
                        financial_items[output_key] = result
                
                if progress_callback and (ticker_idx + 1) % (total_master_tickers // 10 or 1) == 0:
                     await do_progress_update("load_yahoo_data", "running", current_ticker_progress_start, f"Processing items for {ticker_symbol} ({ticker_idx+1}/{total_master_tickers})")

                return {
                    "ticker": ticker_symbol,
                    "master_data": master_data,
                    "financial_items": financial_items
                }

            fetch_all_tickers_tasks = [fetch_ticker_combined_data_internal(idx, ticker) for idx, ticker in enumerate(master_tickers)]
            
            raw_combined_data_list = await asyncio.gather(*fetch_all_tickers_tasks, return_exceptions=True)

            successful_results_count = 0
            for i, res in enumerate(raw_combined_data_list):
                if isinstance(res, Exception):
                    logger.error(f"ADP Yahoo: Error fetching combined data for ticker {master_tickers[i]}: {res}", exc_info=res)
                else:
                    combined_data_list.append(res)
                    successful_results_count +=1
            
            logger.info(f"ADP Yahoo: Successfully fetched combined data for {successful_results_count} of {total_master_tickers} tickers.")
            await do_progress_update("load_yahoo_data", "completed", 100, f"Yahoo data load complete ({successful_results_count}/{total_master_tickers} tickers).", count=successful_results_count, total_count=total_master_tickers)
            return combined_data_list

        except Exception as e:
            logger.error(f"ADP: Unexpected error loading Yahoo data directly: {e}", exc_info=True)
            await do_progress_update("load_yahoo_data", "failed", 100, f"Unexpected error: {e}")
            return []

    # ... (rest of the class: _transform_raw_yahoo_data, _merge_data, _generate_field_metadata, etc. remain largely the same,
    # ensure they are compatible with the output of the new _load_yahoo_data)

    # +++ NEW METHOD: _transform_raw_yahoo_data (ensure it expects data from new _load_yahoo_data) +++
    # This method expects a list of dicts, where each dict has 'ticker', 'master_data', 'financial_items'
    def _transform_raw_yahoo_data(self, raw_yahoo_data_list: List[Dict[str, Any]], progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        logger.info(f"ADP: Transforming {len(raw_yahoo_data_list)} raw Yahoo records (from direct load)...")
        
        is_async_callback = asyncio.iscoroutinefunction(progress_callback)

        async def do_transform_progress_update(current, total, ticker_symbol):
            if progress_callback:
                progress_percent = int((current / total) * 100) if total > 0 else 0
                msg = f"Transforming Yahoo: {ticker_symbol} ({current}/{total})"
                payload = {"task_name":"transform_yahoo_data", "status":"processing", "progress":progress_percent, "message":msg}
                if is_async_callback:
                    await progress_callback(payload)
                else:
                    progress_callback(payload)
                await asyncio.sleep(0) # Yield

        transformed_data_list: List[Dict[str, Any]] = []
        total_records = len(raw_yahoo_data_list)

        if progress_callback:
            initial_payload = {"task_name":"transform_yahoo_data", "status":"started", "progress":0, "message":"Starting Yahoo data transformation"}
            if is_async_callback:
                asyncio.create_task(progress_callback(initial_payload)) # Use create_task if callback is async and method is sync
            else:
                progress_callback(initial_payload)

        for index, raw_ticker_data in enumerate(raw_yahoo_data_list):
            ticker = raw_ticker_data.get("ticker")
            if not ticker:
                logger.warning("ADP Transform Yahoo: Skipping record due to missing ticker.")
                continue

            flat_ticker_data: Dict[str, Any] = {"ticker": ticker, "source": "yahoo"}

            # Process 'master_data'
            master_data = raw_ticker_data.get("master_data", {})
            if isinstance(master_data, dict):
                for key, value in master_data.items():
                    # Exclude DB-specific or non-data fields from Yahoo Master Table if necessary
                    if key not in ["ticker", "id", "yahoo_uid", "created_at", "updated_at"]: 
                        flat_ticker_data[f"yf_tm_{key}"] = value
            else:
                logger.warning(f"ADP Transform Yahoo ({ticker}): master_data is not a dict or is missing. Type: {type(master_data)}. Skipping master_data fields.")
            
            # Process 'financial_items'
            financial_items = raw_ticker_data.get("financial_items", {})
            if isinstance(financial_items, dict):
                for item_key, item_payload in financial_items.items(): # item_key is like 'balance_sheet_annual'
                    if isinstance(item_payload, dict): # item_payload is the actual data dict for that item_type/coverage
                        for field_name, field_value in item_payload.items():
                            flat_ticker_data[f"yf_item_{item_key}_{field_name}"] = field_value
                    else:
                        logger.warning(f"ADP Transform Yahoo ({ticker}): Payload for financial item '{item_key}' is not a dict. Skipping this item. Payload: {str(item_payload)[:100]}")
            else:
                logger.warning(f"ADP Transform Yahoo ({ticker}): financial_items is not a dict or is missing. Type: {type(financial_items)}. Skipping financial_items fields.")
            
            transformed_data_list.append(flat_ticker_data)

            if progress_callback and (index + 1) % (total_records // 20 or 1) == 0: # Update ~20 times
                if is_async_callback:
                    asyncio.create_task(do_transform_progress_update(index + 1, total_records, ticker))
                else:
                    do_transform_progress_update(index + 1, total_records, ticker) # Call sync version
        
        logger.info(f"ADP: Successfully transformed {len(transformed_data_list)} Yahoo records (from direct load).")
        if progress_callback:
            final_msg = "Yahoo data transformation complete."
            final_payload = {"task_name":"transform_yahoo_data", "status":"completed", "progress":100, "count":len(transformed_data_list), "message":final_msg}
            if is_async_callback:
                 asyncio.create_task(progress_callback(final_payload))
            else:
                progress_callback(final_payload)
        return transformed_data_list
    # --- END MODIFIED _transform_raw_yahoo_data ---

    def _merge_data(self, finviz_data: List[Dict[str, Any]], yahoo_data: List[Dict[str, Any]], progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        logger.info(f"ADP: Merging {len(finviz_data)} Finviz records and {len(yahoo_data)} Yahoo records...")
        
        is_async_callback = asyncio.iscoroutinefunction(progress_callback)
        async def do_merge_progress_update(status_str, progress_percent, message_str, current_count=None):
            if progress_callback:
                payload = {"task_name":"merge_data", "status":status_str, "progress":progress_percent, "message":message_str}
                if current_count is not None: payload["count"] = current_count
                if is_async_callback: await progress_callback(payload)
                else: progress_callback(payload)
                await asyncio.sleep(0) # yield

        if progress_callback: # Initial call for merge starting
             if is_async_callback: asyncio.create_task(do_merge_progress_update("started", 0, "Starting data merge"))
             else: do_merge_progress_update("started", 0, "Starting data merge")


        merged_data_dict: Dict[str, Dict[str, Any]] = {}
        
        for fv_item in finviz_data:
            ticker = fv_item.get('ticker')
            if not ticker:
                logger.warning(f"ADP Merge: Finviz item missing ticker, skipping.")
                continue
            merged_data_dict[ticker] = {**fv_item} 

        if progress_callback:
             if is_async_callback: asyncio.create_task(do_merge_progress_update("processing", 33, "Finviz data processed, starting Yahoo merge."))
             else: do_merge_progress_update("processing", 33, "Finviz data processed, starting Yahoo merge.")

        for y_item in yahoo_data:
            ticker = y_item.get('ticker')
            if not ticker:
                logger.warning(f"ADP Merge: Yahoo item missing ticker, skipping.")
                continue

            if ticker in merged_data_dict:
                for key, value in y_item.items():
                    if key != 'ticker': 
                        merged_data_dict[ticker][key] = value
            else:
                merged_data_dict[ticker] = {**y_item}
        
        if progress_callback:
             final_count = len(merged_data_dict)
             if is_async_callback: asyncio.create_task(do_merge_progress_update("processing", 66, "Yahoo data merged.", current_count=final_count))
             else: do_merge_progress_update("processing", 66, "Yahoo data merged.", current_count=final_count)
        
        logger.info(f"ADP Merge: Merged data contains {len(merged_data_dict)} unique records.")
        if progress_callback:
            final_count = len(merged_data_dict)
            if is_async_callback: asyncio.create_task(do_merge_progress_update("completed", 100, "Merge complete.", current_count=final_count))
            else: do_merge_progress_update("completed", 100, "Merge complete.", current_count=final_count)
        
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
        
        all_field_names = set()
        for record in data:
            if isinstance(record, dict):
                for key in record.keys():
                    all_field_names.add(key)

        if 'ticker' in all_field_names:
            all_field_names.remove('ticker') # Exclude 'ticker' itself

        for field_name in all_field_names:
            field_stats[field_name] = {
                "name": field_name, "count": 0, "type": "unknown",
                "numeric_values": [], "text_values": set(),
                "min_value": float('inf'), "max_value": float('-inf'), "sum_value": 0,
                "boolean_true_count": 0, "boolean_false_count": 0,
                "all_null_or_empty": True, "example_value": None,
                "has_numeric": False, "has_text": False, "has_boolean": False
            }

        for record_index, record in enumerate(data):
            if not isinstance(record, dict): continue
            for field_name, value in record.items():
                if field_name not in field_stats: continue # Skip 'ticker' or unexpected fields

                stats = field_stats[field_name]
                if stats["example_value"] is None and value is not None and value != '' and value != [] and value != {}:
                    stats["example_value"] = value
                
                is_truly_empty = value is None or str(value).strip() == '' or str(value).strip() == '-'
                
                if not is_truly_empty:
                    stats["all_null_or_empty"] = False
                    stats["count"] += 1
                    try:
                        num_value = float(value)
                        if not (isinstance(value, bool)):
                            stats["has_numeric"] = True
                            stats["numeric_values"].append(num_value)
                            if num_value < stats["min_value"]: stats["min_value"] = num_value
                            if num_value > stats["max_value"]: stats["max_value"] = num_value
                            stats["sum_value"] += num_value
                        else: 
                            stats["has_boolean"] = True
                            if num_value == 1.0: stats["boolean_true_count"] +=1
                            else: stats["boolean_false_count"] +=1
                            stats["text_values"].add(str(value)) 
                    except (ValueError, TypeError):
                        if isinstance(value, bool):
                            stats["has_boolean"] = True
                            if value: stats["boolean_true_count"] +=1
                            else: stats["boolean_false_count"] +=1
                            stats["text_values"].add(str(value))
                        elif isinstance(value, str): 
                            stats["has_text"] = True
                            stats["text_values"].add(value)
                        elif isinstance(value, (list, dict)): 
                            stats["has_text"] = True
                            try: stats["text_values"].add(json.dumps(value)) 
                            except TypeError: stats["text_values"].add(str(value)) 
                        else: 
                            stats["has_text"] = True 
                            stats["text_values"].add(str(value))

        final_metadata = {}
        sorted_field_stats_items = sorted(field_stats.items())

        for field_name, stats in sorted_field_stats_items:
            meta_entry: Dict[str, Any] = {"name": field_name, "count": stats["count"]}
            if stats["all_null_or_empty"]:
                meta_entry["type"] = "empty"
            else:
                if stats["has_numeric"]: meta_entry["type"] = "numeric"
                elif stats["has_text"]: meta_entry["type"] = "text"
                elif stats["has_boolean"]: meta_entry["type"] = "boolean"
                else: meta_entry["type"] = "unknown"

                if stats["numeric_values"]:
                    num_values = sorted(stats["numeric_values"])
                    meta_entry["min_value"] = num_values[0]
                    meta_entry["max_value"] = num_values[-1]
                    meta_entry["avg_value"] = sum(num_values) / len(num_values)
                    meta_entry["median_value"] = self._calculate_median(num_values) # Use helper

                unique_sample_list = list(stats["text_values"])
                if len(unique_sample_list) > MAX_UNIQUE_TEXT_SAMPLE_SIZE:
                    meta_entry["unique_values_sample"] = unique_sample_list[:MAX_UNIQUE_TEXT_SAMPLE_SIZE]
                else:
                    meta_entry["unique_values_sample"] = unique_sample_list
            
            if stats["has_boolean"]: # Add boolean counts if type is boolean or if any booleans were found
                 meta_entry["boolean_counts"] = {"true": stats["boolean_true_count"], "false": stats["boolean_false_count"]}


            final_metadata[field_name] = meta_entry
        return final_metadata

    def _calculate_median(self, numeric_values: List[Union[int, float]]) -> Optional[Union[int, float]]:
        """Helper to calculate median for a list of numeric values."""
        if not numeric_values: return None
        sorted_values = sorted(numeric_values)
        n = len(sorted_values)
        mid = n // 2
        if n % 2 == 0: 
            median_val = (sorted_values[mid - 1] + sorted_values[mid]) / 2.0
            return median_val
        else: 
            return sorted_values[mid]

    async def _prepare_analytics_components(self, 
                                           create_original_data: bool, 
                                           create_metadata: bool,
                                           progress_callback: Optional[Callable] = None
                                           ) -> Tuple[Optional[List[Dict[str, Any]]], Optional[Dict[str, Dict[str, Any]]]]:
        logger.info(f"ADP _prepare_analytics_components: create_original_data={create_original_data}, create_metadata={create_metadata}")
        
        original_data_output: Optional[List[Dict[str, Any]]] = None
        metadata_output: Optional[Dict[str, Dict[str, Any]]] = None
        
        is_async_outer_callback = asyncio.iscoroutinefunction(progress_callback)

        async def _do_overall_progress_callback(message_content: str, task_status: str = "running", current_progress: int = -1): # Added more params
            if progress_callback:
                # Determine progress if not explicitly set
                auto_progress = 0
                if "Finviz data preparation" in message_content: auto_progress = 5
                elif "Finviz data loaded" in message_content: auto_progress = 20 # Adjusted
                elif "Yahoo data loading" in message_content: auto_progress = 25 # Adjusted
                elif "Yahoo data direct load completed" in message_content: auto_progress = 55 # After _load_yahoo_data finishes
                elif "Yahoo data transformation" in message_content: auto_progress = 60 
                elif "Yahoo data transformed" in message_content: auto_progress = 75
                elif "Merging" in message_content: auto_progress = 80
                elif "Data merged" in message_content: auto_progress = 90
                elif "Metadata generation" in message_content: auto_progress = 95
                elif "Metadata generated" in message_content: auto_progress = 98
                elif "Completed" in message_content or task_status == "completed": auto_progress = 100
                
                effective_progress = current_progress if current_progress != -1 else auto_progress

                update_payload = {"task_name": "prepare_analytics_components", "status": task_status, "progress": effective_progress, "message": message_content}
                if is_async_outer_callback:
                    await progress_callback(update_payload)
                else:
                    progress_callback(update_payload) # Sync call
                await asyncio.sleep(0) 

        all_finviz_processed: List[Dict[str, Any]] = []
        all_yahoo_transformed: List[Dict[str, Any]] = []

        try:
            await _do_overall_progress_callback("Starting Finviz data preparation...")
            if create_original_data or create_metadata:
                try:
                    all_finviz_processed = await self._load_finviz_data(progress_callback=progress_callback)
                    logger.info(f"ADP: Finviz data loaded. Count: {len(all_finviz_processed)}")
                    await _do_overall_progress_callback(f"Finviz data loaded ({len(all_finviz_processed)} records).")
                except Exception as e_finviz_load:
                    logger.error(f"ADP: Error during Finviz data loading: {e_finviz_load}", exc_info=True)
                    await _do_overall_progress_callback(f"Error loading Finviz data: {e_finviz_load}", task_status="failed_stage")
                    all_finviz_processed = [] 

            await _do_overall_progress_callback("Starting Yahoo data loading (direct method)...")
            if create_original_data or create_metadata: 
                try:
                    # _load_yahoo_data is now async and uses services directly, and handles its own progress_callback passing internally
                    raw_yahoo_data_from_direct_load = await self._load_yahoo_data(progress_callback=progress_callback)
                    logger.info(f"ADP: Raw Yahoo data from direct load. Count: {len(raw_yahoo_data_from_direct_load)}")
                    await _do_overall_progress_callback(f"Yahoo data direct load completed ({len(raw_yahoo_data_from_direct_load)} records).")

                    await _do_overall_progress_callback("Starting Yahoo data transformation...")
                    if raw_yahoo_data_from_direct_load:
                        # _transform_raw_yahoo_data is SYNC but can take an ASYNC callback
                        all_yahoo_transformed = await run_in_threadpool(self._transform_raw_yahoo_data, raw_yahoo_data_from_direct_load, progress_callback=progress_callback)
                        logger.info(f"ADP: Yahoo data transformed. Count: {len(all_yahoo_transformed)}")
                        await _do_overall_progress_callback(f"Yahoo data transformed ({len(all_yahoo_transformed)} records).")
                    else:
                        logger.info("ADP: No raw Yahoo data (from direct load) to transform.")
                        await _do_overall_progress_callback("No raw Yahoo data to transform.")
                        all_yahoo_transformed = []
                except Exception as e_yahoo_prep:
                    logger.error(f"ADP: Error during Yahoo data preparation (direct load/transform): {e_yahoo_prep}", exc_info=True)
                    await _do_overall_progress_callback(f"Error preparing Yahoo data: {e_yahoo_prep}", task_status="failed_stage")
                    all_yahoo_transformed = []

            merged_data: List[Dict[str, Any]] = []
            if create_original_data:
                await _do_overall_progress_callback("Starting data merging (Finviz & Yahoo)...")
                try:
                    merged_data = await run_in_threadpool(self._merge_data, all_finviz_processed, all_yahoo_transformed, progress_callback=progress_callback)
                    logger.info(f"ADP: Data merged. Total records: {len(merged_data)}")
                    await _do_overall_progress_callback(f"Data merged ({len(merged_data)} records).")
                    original_data_output = merged_data
                except Exception as e_merge:
                    logger.error(f"ADP: Error during data merging: {e_merge}", exc_info=True)
                    await _do_overall_progress_callback(f"Error merging data: {e_merge}", task_status="failed_stage")
                    original_data_output = [] 
            
            if create_metadata:
                await _do_overall_progress_callback("Starting metadata generation...")
                try:
                    data_for_metadata_generation: List[Dict[str, Any]] = []
                    if original_data_output is not None: 
                        data_for_metadata_generation = original_data_output
                    elif create_original_data is False and (all_finviz_processed or all_yahoo_transformed):
                        data_for_metadata_generation = await run_in_threadpool(self._merge_data, all_finviz_processed, all_yahoo_transformed, progress_callback=progress_callback)
                        await _do_overall_progress_callback(f"Data re-merged for metadata ({len(data_for_metadata_generation)} records).")
                    
                    if data_for_metadata_generation:
                        metadata_output = await run_in_threadpool(self._generate_field_metadata, data_for_metadata_generation) 
                        logger.info(f"ADP: Metadata generated. Number of fields: {len(metadata_output if metadata_output else {})}")
                        await _do_overall_progress_callback(f"Metadata generated ({len(metadata_output if metadata_output else {})} fields).")
                    else:
                        logger.warning("ADP: No data for _generate_field_metadata. Metadata will be empty.")
                        metadata_output = {}
                        await _do_overall_progress_callback("Metadata generation skipped (no data).")     
                except Exception as e_meta:
                    logger.error(f"ADP: Error during metadata generation: {e_meta}", exc_info=True)
                    await _do_overall_progress_callback(f"Error generating metadata: {e_meta}", task_status="failed_stage")
                    metadata_output = {} 

            await _do_overall_progress_callback("Completed analytics components preparation.", task_status="completed", current_progress=100)
            return original_data_output, metadata_output

        except Exception as e_main:
            logger.error(f"ADP: Critical error in _prepare_analytics_components: {e_main}", exc_info=True)
            await _do_overall_progress_callback(f"Critical error in preparation: {e_main}", task_status="failed", current_progress=100)
            return None if original_data_output is None else original_data_output, \
                   None if metadata_output is None else metadata_output

    async def process_data_for_analytics(self, data_source_selection: str, progress_callback=None):
        # This method is now less relevant as primary entry point for cache, but kept for potential direct use.
        # It calls _prepare_analytics_components which now has improved progress reporting.
        logger.info(f"ADP process_data_for_analytics called with: {data_source_selection}. This method usually defers to cache now.")
        
        # Translate data_source_selection to what _prepare_analytics_components expects
        # For simplicity, we'll always try to prepare both sources and let _prepare handle it,
        # then filter if necessary, though the cache design makes this less direct.
        # This method might need rethinking if used directly for non-cached scenarios.
        # For now, let's assume it's primarily for generating data that would then be cached.
        
        # If this method is called directly, it should likely call _prepare for "both" and then metadata.
        
        # If this method is called directly, it should likely call _prepare_analytics_components.
        # The data_source_selection logic here was for on-the-fly, which is now mostly deprecated by cache.
        # Let's make it simpler: it just calls _prepare for "both" and then metadata.
        
        if progress_callback: 
            if asyncio.iscoroutinefunction(progress_callback):
                await progress_callback({"task_name":"process_data_for_analytics", "status":"started", "progress":0, "message":f"Processing for {data_source_selection}"})
            else:
                progress_callback({"task_name":"process_data_for_analytics", "status":"started", "progress":0, "message":f"Processing for {data_source_selection}"})

        # Call the core preparation method, requesting both data and metadata
        original_data, field_metadata_dict = await self._prepare_analytics_components(
            create_original_data=True,
            create_metadata=True,
            progress_callback=progress_callback
        )
        
        message = f"Data preparation complete via _prepare_analytics_components. Records: {len(original_data if original_data else [])}"

        if progress_callback:
            final_status = "completed" if original_data is not None else "failed"
            if asyncio.iscoroutinefunction(progress_callback):
                await progress_callback({"task_name":"process_data_for_analytics", "status":final_status, "progress":100, "message":message, "count": len(original_data if original_data else [])})
            else:
                 progress_callback({"task_name":"process_data_for_analytics", "status":final_status, "progress":100, "message":message, "count": len(original_data if original_data else [])})
        
        final_meta_data = {
            "source_selection": "both_processed_internally", # Indicate it used internal full processing
            "field_metadata": field_metadata_dict if field_metadata_dict else {},
        }

        logger.info(f"ADP process_data_for_analytics returning. Message: {message}")
        return original_data if original_data is not None else [], final_meta_data


    async def force_refresh_data_cache(self, progress_callback: Optional[Callable] = None) -> None:
        logger.info("ADP: Forcing refresh of DATA cache...")
        if progress_callback:
            cb_payload = {"type":"status", "task_name":"force_refresh_data_cache", "status":"started", "progress":0, "message": "Starting data cache refresh..."}
            if asyncio.iscoroutinefunction(progress_callback): await progress_callback(cb_payload)
            else: progress_callback(cb_payload)
        
        try:
            original_data, _ = await self._prepare_analytics_components(
                create_original_data=True, 
                create_metadata=False, 
                progress_callback=progress_callback
            )

            if original_data is not None:
                logger.info(f"ADP: Data generated for cache ({len(original_data)} records). Saving to DB...")
                if progress_callback:
                    cb_payload_saving = {"type":"status", "task_name":"force_refresh_data_cache", "status":"saving_data", "progress":90, "message": f"Saving {len(original_data)} records to data cache..."}
                    if asyncio.iscoroutinefunction(progress_callback): await progress_callback(cb_payload_saving)
                    else: progress_callback(cb_payload_saving)

                data_json = await run_in_threadpool(json.dumps, original_data, default=str) 
                await self.db_repository.update_cached_analytics_data(data_json=data_json)
                logger.info("ADP: Data cache updated successfully.")
                if progress_callback:
                    cb_payload_done = {"type":"status", "task_name":"force_refresh_data_cache", "status":"completed", "progress":100, "message": "Data cache refresh completed successfully."}
                    if asyncio.iscoroutinefunction(progress_callback): await progress_callback(cb_payload_done)
                    else: progress_callback(cb_payload_done)
            else:
                logger.warning("ADP: No data was generated by _prepare_analytics_components for data cache refresh.")
                if progress_callback:
                    cb_payload_nodata = {"type":"warning", "task_name":"force_refresh_data_cache", "status":"completed_no_data", "progress":100, "message": "Data cache: No data generated to refresh."}
                    if asyncio.iscoroutinefunction(progress_callback): await progress_callback(cb_payload_nodata)
                    else: progress_callback(cb_payload_nodata)
        
        except Exception as e:
            logger.error(f"ADP: Error during data cache refresh: {e}", exc_info=True)
            if progress_callback:
                cb_payload_err = {"type":"error", "task_name":"force_refresh_data_cache", "status":"failed", "progress":100, "message": f"Data cache refresh failed: {e}"}
                if asyncio.iscoroutinefunction(progress_callback): await progress_callback(cb_payload_err)
                else: progress_callback(cb_payload_err)

    async def force_refresh_metadata_cache(self, progress_callback: Optional[Callable] = None) -> None:
        logger.info("ADP: Forcing refresh of METADATA cache...")
        is_async_cb = asyncio.iscoroutinefunction(progress_callback)
        async def _send_progress(status_str, prog_val, msg_str):
            if progress_callback:
                payload = {"type":"status", "task_name":"force_refresh_metadata_cache", "status":status_str, "progress":prog_val, "message":msg_str}
                if is_async_cb: await progress_callback(payload)
                else: progress_callback(payload)

        await _send_progress("started", 0, "Starting metadata cache refresh...")

        original_data_for_metadata: Optional[List[Dict[str, Any]]] = None
        metadata_output: Optional[Dict[str, Any]] = None
        source_of_data = "unknown"

        try:
            await _send_progress("running", 10, "Checking data cache for existing data...")
            cached_data_tuple = await self.db_repository.get_cached_analytics_data()

            if cached_data_tuple:
                data_json_from_cache, generated_at = cached_data_tuple
                await _send_progress("running", 20, f"Data cache found (generated {generated_at}), deserializing...")
                try:
                    original_data_for_metadata = await run_in_threadpool(json.loads, data_json_from_cache)
                    if not isinstance(original_data_for_metadata, list):
                        logger.warning("ADP Metadata Refresh: Cached data is not a list. Fallback to fresh generation.")
                        original_data_for_metadata = None 
                        await _send_progress("warning", 25, "Cached data format error. Will generate fresh data.")
                    else:
                        logger.info(f"ADP Metadata Refresh: Successfully deserialized {len(original_data_for_metadata)} records from data cache.")
                        source_of_data = f"data_cache (generated_at: {generated_at})"
                        await _send_progress("running", 30, f"Using {len(original_data_for_metadata)} records from data cache.")
                except json.JSONDecodeError as e_json:
                    logger.warning(f"ADP Metadata Refresh: JSONDecodeError for cached data: {e_json}. Fallback to fresh generation.")
                    original_data_for_metadata = None 
                    await _send_progress("warning", 25, f"Data cache JSON error: {e_json}. Will generate fresh data.")
            else:
                logger.info("ADP Metadata Refresh: Data cache is empty. Will generate fresh data.")
                await _send_progress("running", 20, "Data cache empty. Generating fresh data for metadata.")

            if original_data_for_metadata is None: # Fallback to fresh generation
                await _send_progress("running", 30, "Generating fresh data and metadata via _prepare_analytics_components...")
                fresh_original_data, fresh_metadata = await self._prepare_analytics_components(
                    create_original_data=True,
                    create_metadata=True, 
                    progress_callback=progress_callback # Pass through for sub-component progress
                )
                metadata_output = fresh_metadata
                original_data_for_metadata = fresh_original_data 
                source_of_data = "freshly_generated"
                record_count = len(fresh_original_data if fresh_original_data else [])
                metadata_field_count = len(fresh_metadata if fresh_metadata else [])
                await _send_progress("running", 70, f"Fresh data ({record_count}) and metadata ({metadata_field_count} fields) generated.")


            if metadata_output is None and original_data_for_metadata is not None:
                await _send_progress("running", 75, f"Generating metadata from {source_of_data} ({len(original_data_for_metadata)} records)...")
                metadata_output = await run_in_threadpool(self._generate_field_metadata, original_data_for_metadata)
                await _send_progress("running", 85, f"Metadata generated ({len(metadata_output if metadata_output else {})} fields).")
            elif metadata_output is not None:
                 logger.info(f"ADP Metadata Refresh: Using metadata directly generated by _prepare_analytics_components (source: {source_of_data}).")

            if metadata_output is not None:
                await _send_progress("saving_metadata", 90, f"Saving {len(metadata_output)} metadata fields to cache...")
                metadata_json = await run_in_threadpool(json.dumps, metadata_output, default=str)
                await self.db_repository.update_cached_analytics_metadata(metadata_json=metadata_json)
                logger.info(f"ADP: Metadata cache updated successfully (source: {source_of_data}).")
                await _send_progress("completed", 100, "Metadata cache refresh completed successfully.")
            else:
                logger.warning(f"ADP Metadata Refresh: No metadata generated (source attempt: {source_of_data}). Cache not updated.")
                await _send_progress("completed_no_data", 100, "Metadata cache: No metadata generated to refresh.")

        except Exception as e:
            logger.error(f"ADP: Error during metadata cache refresh: {e}", exc_info=True)
            await _send_progress("failed", 100, f"Metadata cache refresh failed: {e}")

# Example usage (for testing, would not be here in production)
# async def example_progress_reporter(status_update: Dict[str, Any]):
#     print(f"Progress Update: {status_update}")

# if __name__ == '__main__':
#    # ... old test code ...