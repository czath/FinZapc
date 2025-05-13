"""
Module for the AnalyticsDataProcessor (ADP).
Handles loading, merging, and filtering of analytics data from various sources.
"""
import logging
import asyncio
from typing import List, Dict, Any, Optional, Callable
import httpx # <-- ADDED IMPORT
import json # <-- ADDED IMPORT FOR JSON PARSING IN REPOSITORY (though parsing happens there now)

# Assuming SQLiteRepository is defined here or imported correctly
from .V3_database import SQLiteRepository # <-- ADDED IMPORT

# Define the base URL for the API, can be moved to config later
BASE_API_URL = "http://localhost:8000" # Adjust if your app runs on a different port

logger = logging.getLogger(__name__)

class AnalyticsDataProcessor:
    def __init__(self, db_repository: SQLiteRepository): # <-- MODIFIED: Accept repository
        """
        Initializes the AnalyticsDataProcessor.
        Dependencies like HTTP clients or pointers to other services can be injected here if needed.
        """
        logger.info("AnalyticsDataProcessor initialized.")
        self.http_client = httpx.AsyncClient(base_url=BASE_API_URL, timeout=30.0) # <-- INITIALIZE HTTPX CLIENT with timeout
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
        finviz_data: List[Dict[str, Any]] = []
        if progress_callback:
            await progress_callback(task_name="load_finviz_data", status="started", progress=0, message="Querying database for Finviz data")
        
        try:
            # Call the repository method to get Finviz data
            # Assumes get_analytics_raw_data_by_source handles JSON parsing
            finviz_data = await self.db_repository.get_analytics_raw_data_by_source(source_filter='finviz')
            logger.info(f"ADP: Successfully loaded {len(finviz_data)} Finviz records from DB.")
            
            # --- TEMPORARY DEBUG LOGGING ---
            if finviz_data:
                logger.debug(f"ADP DEBUG: _load_finviz_data - First 3 records (if available): {finviz_data[:3]}")
            else:
                logger.debug("ADP DEBUG: _load_finviz_data - No records returned from repository.")
            logger.debug(f"ADP DEBUG: _load_finviz_data - Total records being returned: {len(finviz_data)}")
            # --- END TEMPORARY DEBUG LOGGING ---

            if progress_callback:
                # Report completion after successful fetch
                await progress_callback(task_name="load_finviz_data", status="completed", progress=100, count=len(finviz_data), message="Finviz data loaded from DB")

        except Exception as e:
            logger.error(f"ADP: Error loading Finviz data from database: {e}", exc_info=True)
            if progress_callback:
                await progress_callback(task_name="load_finviz_data", status="failed", progress=100, message=f"DB error: {e}")
            finviz_data = [] # Ensure empty list on error
       
        return finviz_data

    async def _load_yahoo_data(self, progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """
        Loads combined Yahoo data by calling the /api/analytics/data/yahoo_combined endpoint.
        """
        logger.info("ADP: Loading Yahoo combined data...")
        yahoo_data: List[Dict[str, Any]] = []
        endpoint_url = "/api/analytics/data/yahoo_combined"

        if progress_callback:
            await progress_callback(task_name="load_yahoo_data", status="started", progress=0, message=f"Fetching from {endpoint_url}")

        try:
            logger.debug(f"ADP: Calling Yahoo data endpoint: {self.http_client.base_url}{endpoint_url}")
            response = await self.http_client.get(endpoint_url)
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
            yahoo_data = response.json()
            logger.info(f"ADP: Successfully fetched {len(yahoo_data)} records from Yahoo combined endpoint.")
            if progress_callback:
                # Simulate some progress during fetching if the call is long, 
                # or just report completion. For now, direct completion.
                await progress_callback(task_name="load_yahoo_data", status="parsing", progress=50, message="Parsing Yahoo data")

        except httpx.HTTPStatusError as e:
            logger.error(f"ADP: HTTP error loading Yahoo data from {e.request.url}: {e.response.status_code} - {e.response.text}", exc_info=True)
            if progress_callback:
                await progress_callback(task_name="load_yahoo_data", status="failed", progress=100, message=f"HTTP error: {e.response.status_code}")
            yahoo_data = []
        except httpx.RequestError as e:
            logger.error(f"ADP: Request error loading Yahoo data from {e.request.url}: {e}", exc_info=True)
            if progress_callback:
                await progress_callback(task_name="load_yahoo_data", status="failed", progress=100, message=f"Request error: {e}")
            yahoo_data = []
        except Exception as e:
            logger.error(f"ADP: Generic error loading Yahoo data: {e}", exc_info=True)
            if progress_callback:
                await progress_callback(task_name="load_yahoo_data", status="failed", progress=100, message=f"Generic error: {e}")
            yahoo_data = []
        
        # --- FLATTEN THE YAHOO DATA ---
        flattened_yahoo_data = []
        if yahoo_data: # Only process if data was successfully fetched
            logger.info(f"ADP: Flattening {len(yahoo_data)} Yahoo records...")
            for item in yahoo_data:
                if not isinstance(item, dict):
                    logger.warning(f"ADP: Skipping non-dict item in yahoo_data: {item}")
                    continue

                flattened_item = {}
                # 1. Copy ticker directly
                if 'ticker' in item:
                    flattened_item['ticker'] = item['ticker']
                else:
                    logger.warning(f"ADP: Skipping item due to missing 'ticker': {item.get('master_data', {}).get('shortName', 'N/A')}")
                    continue # Skip if no ticker, essential for merging and identification

                # 2. Process master_data
                master_data = item.get('master_data')
                if isinstance(master_data, dict):
                    for key, value in master_data.items():
                        if key != 'ticker': # Avoid duplicating/overwriting the main ticker
                            flattened_item[f"yf_tm_{key}"] = value
                
                # 3. Process financial_items
                financial_items_container = item.get('financial_items')
                if isinstance(financial_items_container, dict):
                    for item_type_key, payload_dict in financial_items_container.items():
                        if isinstance(payload_dict, dict):
                            for sub_key, sub_value in payload_dict.items():
                                flattened_item[f"yf_item_{item_type_key}_{sub_key}"] = sub_value
                        elif payload_dict is not None: # Handle cases where a financial item might not be a dict (e.g. a direct value)
                            flattened_item[f"yf_item_{item_type_key}"] = payload_dict


                flattened_yahoo_data.append(flattened_item)
            logger.info(f"ADP: Flattening complete. Produced {len(flattened_yahoo_data)} flattened Yahoo records.")
        # --- END FLATTENING ---

        if progress_callback:
            await progress_callback(task_name="load_yahoo_data", status="completed", progress=100, count=len(flattened_yahoo_data)) # Use count of flattened data
        
        return flattened_yahoo_data # Return the flattened data

    def _merge_data(self, finviz_data: List[Dict[str, Any]], yahoo_data: List[Dict[str, Any]], progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        logger.info(f"ADP: Merging {len(finviz_data)} Finviz records (expecting 'fv_' prefix) and {len(yahoo_data)} Yahoo records (expecting 'yf_' prefixes)...")
        if progress_callback:
            asyncio.create_task(progress_callback(task_name="merge_data", status="started", progress=0, message="Starting data merge"))

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
            asyncio.create_task(progress_callback(task_name="merge_data", status="processing", progress=33, message="Finviz data processed, starting Yahoo merge."))

        # 2. Process Yahoo data: Merge with existing items or add new ones
        # Yahoo fields are already prefixed (e.g., yf_tm_shortName) from _load_yahoo_data
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
             asyncio.create_task(progress_callback(task_name="merge_data", status="processing", progress=66, message="Yahoo data merged."))

        # --- TEST LOGGING: Log merged records for test tickers ---
        for test_ticker in test_tickers_to_log:
            if test_ticker in merged_data_dict:
                logger.info(f"ADP Merge - POST-YAHOO-PROCESS Test Ticker {test_ticker}: {merged_data_dict[test_ticker]}")
            else:
                logger.info(f"ADP Merge - POST-YAHOO-PROCESS Test Ticker {test_ticker}: NOT FOUND in merged_data_dict")
        # --- END TEST LOGGING ---
        
        logger.info(f"ADP Merge: Merged data contains {len(merged_data_dict)} unique records.")
        if progress_callback:
            asyncio.create_task(progress_callback(task_name="merge_data", status="completed", progress=100, count=len(merged_data_dict), message="Merge complete."))
        
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
        for field_name, stats in field_stats.items():
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
                meta_entry["unique_values_sample"] = list(stats["text_values"])

            final_metadata[field_name] = meta_entry

        return final_metadata

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

        return {"originalData": original_data, "metaData": final_meta_data, "message": message}

# Example usage (for testing, would not be here in production)
async def example_progress_reporter(status_update: Dict[str, Any]):
    print(f"Progress Update: {status_update}")

# Remove the old __main__ test block if it instantiates ADP without the repository
# if __name__ == '__main__':
#    # ... old test code ... 