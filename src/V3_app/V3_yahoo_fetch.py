"""
Module for fetching and processing stock data from Yahoo Finance.
"""

import requests
import cloudscraper # Consider using this if standard requests are blocked
from bs4 import BeautifulSoup
import logging
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any, List
import asyncio
import os
import re
import time 
import random 
import argparse # Added
import json     # Added
import yfinance as yf # Added for yfinance library
from concurrent.futures import ThreadPoolExecutor # Added for running sync code
import pandas as pd # Added for DataFrame handling
import pprint # For pretty-printing dicts/lists in text output
# from yfinance.utils import FastInfo # Removed due to ImportError - FastInfo handled by dict() conversion

# --- COMMENT OUT Caching/Rate Limiting Imports ---
# from requests import Session 
# from requests_cache import CacheMixin, SQLiteCache
# from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
# from pyrate_limiter import Duration, RequestRate, Limiter
# --- END Imports ---

# --- Custom Imports for this module ---
# from .V3_database import SQLiteRepository # Removed
from .yahoo_repository import YahooDataRepository # Added
from .yahoo_models import YahooTickerMasterModel, TickerDataItemsModel # Added, though might not be directly used
from .yahoo_data_query_srv import YahooDataQueryService # Import the new query service

# Configure logging - SET TO DEBUG initially for development
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Define CachedLimiterSession Class ---
# class CachedLimiterSession(CacheMixin, LimiterMixin, Session): # UNCOMMENTED
#    pass
# --- End Class Definition ---

# --- Custom JSON Encoder for pandas/numpy types ---
class PandasEncoder(json.JSONEncoder):
    def default(self, obj):
        # Specific type checks first
        if isinstance(obj, (pd.Timestamp, pd.Timedelta, date)):
            return str(obj) # Convert Timestamp, Timedelta, and date to string
        
        if isinstance(obj, pd.Series):
            # Convert Series to a list of records for tabular output
            series_df = obj.reset_index()
            # Ensure column names are strings (usually they are, but good practice)
            series_df.columns = [str(col_name) for col_name in series_df.columns]
            return series_df.to_dict(orient='records')

        if isinstance(obj, pd.DataFrame):
            # Convert DataFrame to a list of records for tabular output
            # Ensure column names are strings
            obj.columns = [str(col_name) for col_name in obj.columns]
            return obj.to_dict(orient='records') 
        
        # Check for scalar NA values (e.g., pd.NA, np.nan)
        if pd.isna(obj): 
            return None
        
        # Handle numpy scalar types 
        if hasattr(obj, 'item') and callable(obj.item):
            try:
                if hasattr(obj, 'size') and obj.size > 1:
                    pass # Let it fall through if it's an array-like numpy object not caught by pd.Series/DataFrame
                else:
                    return obj.item() # Convert numpy scalar to Python native type
            except ValueError: 
                pass # Fall through
        
        return json.JSONEncoder.default(self, obj)

# --- Placeholder functions to be implemented ---

# --- Standalone Testing Block ---
if __name__ == '__main__':
    # import pprint # No longer used directly in __main__ typically
    import asyncio
    import os 
    # import time # Used in functions
    # import random # Used in functions
    # argparse and json are already imported at the top of the file.

    # --- All hardcoded field lists, their definitions, and old analysis/consolidation logic
    # --- including INCOME_STATEMENT_QUARTERLY_FIELDS, BALANCE_SHEET_CASH_FLOW_QUARTERLY_FIELDS,
    # --- ALL_UNIQUE_FIELDS_PREVIOUS_ANALYSIS, FINAL_MASTER_FIELDS, NEWLY_DISCOVERED_FIELDS,
    # --- and the old analyze_field_structures function definition and its calls
    # --- should have been REMOVED by previous edits or are confirmed not present. --- 
    # --- This section is now cleaned up for the new argparse and main call flow. ---

    # --- NEW Main execution logic using argparse will be added in the next step ---
    # (Argparse setup and the call to the new fetch_and_log_field_data function)

    # logger.info("--- Yahoo Finance Data Fetch Script (placeholder for main logic) ---") # Commented out placeholder

async def fetch_and_log_data( # Renamed function for clarity
    ticker: str, 
    timeseries_fields_to_fetch: List[str], # Specific to timeseries
    base_output_filepath: str, # Changed from output_filepath
    timeseries_retries: int = 3, 
    info_retries: int = 3, # Retries for fetching yfinance info
    initial_retry_delay: float = 2.0, 
    max_retry_delay: float = 10.0, 
    ):
    """
    Fetches TimeSeries data (using direct API call) and general ticker info 
    (longName, shortName, sector, industry etc. using yfinance library) for a given ticker.
    Logs raw JSON responses to separate files prefixed with 'ts_' (TimeSeries) 
    and 'pi_' (Profile Information - from yfinance).
    Manages retries for each fetch type individually.
    """
    # --- Derive output filenames ---
    output_dir = os.path.dirname(base_output_filepath)
    base_filename = os.path.basename(base_output_filepath)
    ts_output_filepath = os.path.join(output_dir, f"ts_{base_filename}")
    pi_output_filepath = os.path.join(output_dir, f"pi_{base_filename}") # For combined Profile Info

    logger.info(f"--- Starting Combined Data Fetch for Symbol: {ticker} ---")
    logger.info(f"TimeSeries fields requested: {len(timeseries_fields_to_fetch)}")
    logger.info(f"Profile Info requested: longName, shortName (from v7/quote); sector, industry (from v10/quoteSummary assetProfile).")
    logger.info(f"TimeSeries output will be written to: {ts_output_filepath}")
    logger.info(f"Profile Information output will be written to: {pi_output_filepath}")

    # Estimate time (rough) - 1 API call (TimeSeries) + 1 yfinance call (Info) + retries
    avg_api_time_with_retries = ( (initial_retry_delay + max_retry_delay) / 2 * 0.5 ) + 2.0 # Estimate 2s API time, 0.5 retry chance
    # Assuming info fetch might take slightly longer on average
    estimated_minutes = (avg_api_time_with_retries + (avg_api_time_with_retries + 1.0) + 0.5) / 60 # Add 0.5s delay
    logger.info(f"Estimated processing time for API calls & logging: ~{estimated_minutes:.2f} minutes.")

    # --- Ensure output directory exists ---
    if output_dir and not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f"Created output directory: {output_dir}")
        except OSError as e:
            logger.error(f"Could not create output directory {output_dir}: {e}.")
            ts_output_filepath = f"ts_{base_filename}"
            pi_output_filepath = f"pi_{base_filename}"
            logger.info(f"Fallback TimeSeries output filepath: {ts_output_filepath}")
            logger.info(f"Fallback Profile Information output filepath: {pi_output_filepath}")

    # --- Initialize/Clear the TimeSeries output file ---
    try:
        with open(ts_output_filepath, 'w', encoding='utf-8') as f:
            f.write(f"# Yahoo Finance TimeSeries Data Log for Ticker: {ticker}\n")
            f.write(f"# Fetched on: {datetime.now().isoformat()}\n")
            f.write(f"# Fields requested: {len(timeseries_fields_to_fetch)}\n")
            f.write(f"# Requested field list: {json.dumps(timeseries_fields_to_fetch)}\n")
            f.write(f"# Format: Field name followed by its JSON response or error object from the batch response.\n\n")
        logger.info(f"Initialized TimeSeries output file: {ts_output_filepath}")
    except IOError as e:
        logger.critical(f"FATAL: Error initializing TimeSeries output file {ts_output_filepath}: {e}. Aborting TimeSeries fetch.")
        timeseries_api_response = {"error": f"Failed to initialize output file {ts_output_filepath}", "_fetch_timestamp": datetime.now().isoformat()}
    else:
        # --- Fetch TimeSeries Data ---
        if not timeseries_fields_to_fetch:
            logger.info("No TimeSeries fields specified to fetch. Skipping TimeSeries API call.")
            timeseries_api_response = {"success": True, "result_data": [], "info": "No fields requested."}
            try:
                 with open(ts_output_filepath, 'a', encoding='utf-8') as f:
                    f.write("--- No TimeSeries fields requested in --fields-file. Skipping fetch. ---\n\n")
            except IOError as e_log:
                logger.error(f"Failed to write 'no fields requested' message to TimeSeries log: {e_log}")
        else:
            logger.info(f"--- Starting TimeSeries Fetch for {len(timeseries_fields_to_fetch)} Fields ---")
            timeseries_api_response = None
            for attempt in range(timeseries_retries): # Use timeseries_retries
                logger.info(f"Attempt {attempt + 1}/{timeseries_retries} for TimeSeries batch fetch: {len(timeseries_fields_to_fetch)} fields for ticker '{ticker}'")
                
                timeseries_api_response = get_yahoo_stock_data(symbol=ticker, metrics_to_fetch=timeseries_fields_to_fetch, print_raw_json=False) 
                
                if isinstance(timeseries_api_response, dict) and timeseries_api_response.get("success") == True:
                    logger.info(f"Successfully fetched TimeSeries API response on attempt {attempt + 1}. Items in result_data: {len(timeseries_api_response.get('result_data', []))}")
                    break # Success
                else:
                    error_info = "Unknown error" 
                    status_code_info = 'N/A'
                    if isinstance(timeseries_api_response, dict):
                        error_info = timeseries_api_response.get('error', 'No error message')
                        status_code_info = timeseries_api_response.get('status_code', 'N/A')
                    elif timeseries_api_response is None:
                        error_info = 'get_yahoo_stock_data returned None (unexpected)'
                    
                    logger.warning(f"Attempt {attempt + 1}/{timeseries_retries} FAILED for TimeSeries fetch. Status: {status_code_info}, Info: {error_info}")
                    
                    if attempt < timeseries_retries - 1: 
                        current_retry_delay = random.uniform(initial_retry_delay * (1.5 ** attempt), max_retry_delay * (1.5 ** attempt) / 1.5)
                        current_retry_delay = min(current_retry_delay, 45.0) # Cap delay
                        current_retry_delay = max(current_retry_delay, initial_retry_delay) # Floor delay
                        logger.info(f"Waiting {current_retry_delay:.2f} seconds before retrying TimeSeries call...")
                        await asyncio.sleep(current_retry_delay)
                
                if attempt == timeseries_retries - 1 and (not isinstance(timeseries_api_response, dict) or not timeseries_api_response.get("success")):
                    final_error_info = timeseries_api_response.get('error', 'Undetermined error after all retries') if isinstance(timeseries_api_response, dict) else "Response was not a dictionary or not successful"
                    logger.error(f"All {timeseries_retries} attempts FAILED for TimeSeries fetch. Last error: {final_error_info}")
                    # Log failure to the file
                    try:
                        with open(ts_output_filepath, 'a', encoding='utf-8') as f:
                            f.write(f"--- TimeSeries API Call FAILED for Ticker: {ticker} ---\n")
                            f.write(f"# Fields attempted: {json.dumps(timeseries_fields_to_fetch)}\n")
                            if isinstance(timeseries_api_response, dict):
                                f.write(json.dumps(timeseries_api_response, indent=2, ensure_ascii=False))
                            else:
                                f.write(json.dumps({"error": "TimeSeries API call failed, response not dict", "response_type": str(type(timeseries_api_response))}, indent=2))
                            f.write("\n\n")
                    except Exception as e_log:
                        logger.error(f"Failed to write TimeSeries API failure to log: {e_log}")
                    # Continue to Profile fetch even if Timeseries failed

            # --- Process and log TimeSeries results ---
            if isinstance(timeseries_api_response, dict) and timeseries_api_response.get("success") == True:
                results_list = timeseries_api_response.get("result_data", [])
                
                if not isinstance(results_list, list):
                    logger.error(f"CRITICAL: 'result_data' from successful TimeSeries call was not a list. Type: {type(results_list)}. Response: {timeseries_api_response}")
                    try:
                        with open(ts_output_filepath, 'a', encoding='utf-8') as f:
                            f.write(f"--- CRITICAL ERROR: TimeSeries 'result_data' not a list ---\n")
                            f.write(json.dumps(timeseries_api_response, indent=2, ensure_ascii=False))
                            f.write("\n\n")
                    except Exception as e_log:
                        logger.error(f"Failed to write critical TimeSeries 'result_data' error to log: {e_log}")

                elif not results_list: 
                    logger.warning(f"TimeSeries API call for {ticker} was successful, but 'result_data' list is empty.")
                    try:
                        with open(ts_output_filepath, 'a', encoding='utf-8') as f:
                            f.write(f"--- TimeSeries API Call Successful, but no data returned ---\n")
                            f.write(f"# Ticker: {ticker}\n")
                            f.write(f"# Fields requested: {json.dumps(timeseries_fields_to_fetch)}\n")
                            f.write(json.dumps(timeseries_api_response, indent=2, ensure_ascii=False))
                            f.write("\n\n")
                    except Exception as e_log:
                        logger.error(f"Failed to write empty TimeSeries result_data warning to log: {e_log}")

                else: # Process valid, non-empty results list
                    returned_metrics_map = {}
                    successful_individual_metrics = 0
                    for metric_item_json in results_list:
                        if (isinstance(metric_item_json, dict) and 
                            metric_item_json.get('meta') and 
                            isinstance(metric_item_json.get('meta'), dict) and 
                            isinstance(metric_item_json.get('meta', {}).get('type'), list) and 
                            metric_item_json.get('meta', {}).get('type')): 
                            field_name_from_meta = metric_item_json['meta']['type'][0]
                            returned_metrics_map[field_name_from_meta] = metric_item_json
                            successful_individual_metrics +=1
                        else:
                            logger.warning(f"Skipping malformed item in TimeSeries result_data: {str(metric_item_json)[:200]}")

                    # Log each requested field
                    field_record_counts = {field_name: 0 for field_name in timeseries_fields_to_fetch}
                    for requested_field_name in timeseries_fields_to_fetch:
                        logger.info(f"Logging TimeSeries data for requested field: '{requested_field_name}'")
                        metric_data_to_log = returned_metrics_map.get(requested_field_name)
                        
                        if metric_data_to_log and isinstance(metric_data_to_log, dict):
                            field_name_for_data = metric_data_to_log.get('meta', {}).get('type', [None])[0]
                            if field_name_for_data:
                                actual_data_points = metric_data_to_log.get(field_name_for_data)
                                if isinstance(actual_data_points, list):
                                    non_null_count = sum(1 for record in actual_data_points if isinstance(record, dict))
                                    field_record_counts[requested_field_name] = non_null_count
                                    logger.debug(f"Counted {non_null_count} non-null records for TS '{requested_field_name}'")
                                else: field_record_counts[requested_field_name] = 0
                            else: field_record_counts[requested_field_name] = 0
                        
                        try:
                            with open(ts_output_filepath, 'a', encoding='utf-8') as f:
                                f.write(f"--- Field: {requested_field_name} ---\n")
                                if metric_data_to_log:
                                    f.write(json.dumps(metric_data_to_log, indent=2, ensure_ascii=False))
                                else:
                                    f.write(json.dumps({
                                        "info": "Field was requested but not found in the Timeseries API response.",
                                        "field_requested": requested_field_name,
                                        "ticker": ticker,
                                        "_timestamp": datetime.now().isoformat()
                                    }, indent=2, ensure_ascii=False))
                                f.write("\n\n")
                        except IOError as e: logger.error(f"IOError writing TS data for '{requested_field_name}' to {ts_output_filepath}: {e}")
                        except TypeError as te: logger.error(f"TypeError serializing TS data for '{requested_field_name}': {te}")
                        
                    # Append summary to TS file
                    try:
                        with open(ts_output_filepath, 'a', encoding='utf-8') as f:
                            f.write("\n\n--- TimeSeries Field Data Record Counts Summary ---\n")
                            f.write(f"# Ticker: {ticker}\n")
                            f.write(f"# Fetched on: {datetime.now().isoformat()}\n")
                            if isinstance(timeseries_api_response, dict) and timeseries_api_response.get("success"):
                                f.write("# TimeSeries API call successful.\n")
                                f.write(f"# {successful_individual_metrics} metrics found in response out of {len(timeseries_fields_to_fetch)} requested.\n")
                            else:
                                f.write("# TimeSeries API call FAILED. Counts may be inaccurate.\n")
                            
                            sorted_counts = sorted(field_record_counts.items())
                            for field_name, count in sorted_counts:
                                f.write(f"{field_name} = {count}\n")
                        logger.info(f"Appended TimeSeries record count summary to {ts_output_filepath}")
                    except IOError as e:
                        logger.error(f"Error appending TimeSeries record count summary to {ts_output_filepath}: {e}")

    # --- Initialize Combined Profile Information Data --- 
    combined_profile_info = {
        "symbol": ticker,
        "longName": None,
        "shortName": None,
        "sector": None,
        "industry": None,
        "_errors": [],
        "_yfinance_info_fetch_timestamp": None,
        "_yfinance_info_data_raw": None # Optionally store the raw dict
    }

    # --- Initialize/Clear the Profile Information output file ---
    try:
        with open(pi_output_filepath, 'w', encoding='utf-8') as f:
            f.write(f"# Yahoo Finance Combined Profile Information Log for Ticker: {ticker}\n")
            f.write(f"# Fetched on: {datetime.now().isoformat()}\n")
            f.write(f"# Data source: yfinance library (ticker.info)\n")
            f.write(f"# Data requested: longName, shortName, sector, industry (extracted from ticker.info)\n")
            f.write(f"# Format: JSON object containing combined data or error details.\n\n")
        logger.info(f"Initialized Profile Information output file: {pi_output_filepath}")
    except IOError as e:
        logger.critical(f"FATAL: Error initializing Profile Information output file {pi_output_filepath}: {e}. Aborting Profile Info fetch.")
        # Log this critical failure to the file itself if possible, or just mark in combined_profile_info
        combined_profile_info["_errors"].append(f"Failed to initialize output file {pi_output_filepath}: {e}")
        # We will still attempt to write combined_profile_info at the end.

    # --- Fetch Profile Info using yfinance (via executor) --- 
    logger.info(f"--- Starting yfinance info Fetch for {ticker} (via executor) ---")
    
    # Define the synchronous function to be run in the executor
    def _sync_get_yf_info(ticker_symbol):
        try:
            logger.debug(f"[_sync_get_yf_info] Creating yf.Ticker('{ticker_symbol}')")
            ticker_obj = yf.Ticker(ticker_symbol)
            logger.debug(f"[_sync_get_yf_info] Fetching .info")
            info_data = ticker_obj.info
            logger.debug(f"[_sync_get_yf_info] Fetched .info, type: {type(info_data)}")
            # Return both the info_data and the ticker_obj for potential reuse
            return info_data, ticker_obj 
        except Exception as e:
            logger.error(f"[_sync_get_yf_info] Exception: {e}", exc_info=True)
            return None, None # Return None on error

    loop = asyncio.get_running_loop()
    yf_ticker_info = None
    ticker_obj = None # Define ticker_obj in this scope
    try:
        # Run the synchronous function in the default executor
        with ThreadPoolExecutor() as pool:
            yf_ticker_info, ticker_obj = await loop.run_in_executor(
                pool, _sync_get_yf_info, ticker # Pass ticker symbol
            )
        
        if yf_ticker_info and isinstance(yf_ticker_info, dict):
            logger.info(f"Successfully fetched yfinance info via executor. Keys found: {len(yf_ticker_info)}")
            # Process the successful fetch
            combined_profile_info["_yfinance_info_fetch_timestamp"] = datetime.now().isoformat()
            combined_profile_info["_yfinance_info_data_raw"] = yf_ticker_info
            try:
                combined_profile_info["longName"] = yf_ticker_info.get("longName")
                combined_profile_info["shortName"] = yf_ticker_info.get("shortName")
                combined_profile_info["sector"] = yf_ticker_info.get("sector")
                combined_profile_info["industry"] = yf_ticker_info.get("industry")
                logger.info("Successfully extracted specific keys from yfinance info.")
            except Exception as extract_err:
                err_msg = f"Error extracting fields from yfinance info dict: {extract_err}"
                logger.error(err_msg, exc_info=True)
                combined_profile_info["_errors"].append({"source": "yfinance_info_extraction", "error": err_msg})
        else:
             error_message = "yfinance fetch via executor returned None, empty, or non-dict data."
             logger.error(error_message)
             combined_profile_info["_errors"].append({"source": "yfinance_info_executor", "error": error_message})

    except Exception as exec_err:
        error_message = f"Error running yfinance fetch in executor: {exec_err}"
        logger.error(error_message, exc_info=True)
        combined_profile_info["_errors"].append({"source": "yfinance_info_executor", "error": error_message})

    # <<< Optional Diagnostic: Try fetching analyst price targets using the same method >>>
    if ticker_obj: # Check if ticker_obj was successfully created
        logger.debug("--- Attempting to fetch analyst_price_targets via executor ---")
        def _sync_get_yf_targets(t_obj):
            try:
                logger.debug("[_sync_get_yf_targets] Fetching .analyst_price_targets")
                targets = t_obj.analyst_price_targets
                logger.debug(f"[_sync_get_yf_targets] Fetched targets, type: {type(targets)}")
                return targets
            except Exception as e:
                logger.error(f"[_sync_get_yf_targets] Exception: {e}", exc_info=True)
                return None
        try:
             with ThreadPoolExecutor() as pool:
                analyst_targets = await loop.run_in_executor(
                    pool, _sync_get_yf_targets, ticker_obj
                )
             if analyst_targets is not None:
                 if hasattr(analyst_targets, 'shape'):
                     logger.debug(f"Analyst price targets data shape: {analyst_targets.shape}")
                 else:
                     logger.debug(f"Analyst price targets data (raw): {str(analyst_targets)[:500]}")
             else:
                 logger.debug("Analyst price targets data fetched as None.")
        except Exception as target_err:
            logger.error(f"Error fetching analyst_price_targets via executor: {target_err}", exc_info=True)
        logger.debug("--- Finished fetching analyst_price_targets via executor ---")
    else:
        logger.debug("Skipping analyst targets fetch as ticker_obj was not created.")
    # <<< END TEST >>>

    # --- Log Combined Profile Information --- 
    logger.debug(f"Data to be written to {pi_output_filepath}: {combined_profile_info}") 
    try:
        with open(pi_output_filepath, 'a', encoding='utf-8') as f:
            f.write(json.dumps(combined_profile_info, indent=2, ensure_ascii=False))
            f.write("\n\n")
        logger.info(f"Combined profile information logged to: {pi_output_filepath}")
    except IOError as e:
        logger.error(f"IOError writing combined profile information to {pi_output_filepath}: {e}")
    except TypeError as te:
        try:
            pass
            # Ensure pprint is imported if used
        except Exception:
            pass # Added pass to fix linter error
            logger.error("Could not log problematic dictionary structure.")

    logger.info(f"--- Combined Data Fetch & Logging Run Complete for Ticker '{ticker}' --- ")
    logger.info(f"TimeSeries results logged to: {ts_output_filepath}")
    logger.info(f"Profile Information results logged to: {pi_output_filepath}")

def indent_string(text: str, indent_spaces: int) -> str:
    """Indents each line of a given string."""
    prefix = " " * indent_spaces
    return "\n".join([f"{prefix}{line}" for line in text.splitlines()])

async def fetch_and_write_all_yfinance_data(
    ticker: str, 
    output_filepath: str, 
    retries: int = 3, 
    initial_retry_delay: float = 2.0, 
    max_retry_delay: float = 10.0
    ):
    """
    Fetches all available data from yfinance for a given ticker 
    and writes it to a single consolidated, human-readable TEXT file.
    Uses ThreadPoolExecutor to run synchronous yfinance calls.
    """
    logger.info(f"--- Starting Consolidated yfinance Data Fetch for Symbol: {ticker} ---")
    logger.info(f"Output will be written to: {output_filepath}")

    # all_data structure remains the same as defined in previous steps
    all_data = {
        "symbol": ticker,
        "_fetch_timestamps_utc": {},
        "profile_info": None, 
        "analyst_price_targets": None, 
        "financials": {
            "annual_balance_sheet": None,
            "quarterly_balance_sheet": None,
            "annual_income_statement": None,
            "quarterly_income_statement": None,
            "annual_cash_flow": None,
            "quarterly_cash_flow": None,
        },
        "additional_ticker_data": { 
            "dividends": None,
            "earnings_dates": None,
            "earnings_estimates": None,
            "earnings_history": None,
            "eps_revisions": None,
            "eps_trend": None,
            "growth_estimates": None,
            "revenue_estimates": None,
            "insider_purchases": None,
            "shares_full_history": None, 
            "ttm_income_statement": None,
            "ttm_cash_flow_statement": None,
            "fast_info_summary": None 
        },
        "_errors": []
    }

    loop = asyncio.get_running_loop()
    yf_ticker_obj = None

    async def _execute_yfinance_call(call_name, sync_func, *args):
        # ... (this helper function remains the same)
        last_error = None
        for attempt in range(retries):
            try:
                logger.debug(f"Attempt {attempt + 1}/{retries} for yfinance call: {call_name}")
                with ThreadPoolExecutor(max_workers=1) as pool: 
                    result = await loop.run_in_executor(pool, sync_func, *args)
                all_data["_fetch_timestamps_utc"][call_name] = datetime.utcnow().isoformat()
                logger.debug(f"Successfully fetched {call_name}")
                return result
            except Exception as e:
                last_error = f"Attempt {attempt + 1}/{retries} for {call_name} failed: {e}"
                logger.warning(last_error, exc_info=False) 
                if attempt < retries - 1:
                    delay = random.uniform(initial_retry_delay * (1.5**attempt), max_retry_delay * (1.5**attempt) / 1.5)
                    delay = min(max(delay, initial_retry_delay), 60.0)
                    logger.info(f"Waiting {delay:.2f} seconds before retrying {call_name}...")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"All {retries} attempts FAILED for {call_name}. Last error: {last_error}", exc_info=True)
                    all_data["_errors"].append({"source": call_name, "error_type": type(e).__name__, "error": str(last_error)})
                    all_data["_fetch_timestamps_utc"][call_name] = datetime.utcnow().isoformat() 
        return None

    def _sync_get_ticker(ticker_symbol):
        return yf.Ticker(ticker_symbol)
    
    yf_ticker_obj = await _execute_yfinance_call("get_ticker_object", _sync_get_ticker, ticker)

    if not yf_ticker_obj:
        logger.error(f"Failed to create yfinance.Ticker object for {ticker} after retries. Cannot fetch further yfinance data.")
        all_data["_errors"].append({"source": "yfinance_ticker_creation", "error": "Failed to create Ticker object"})
    else:
        # Fetch Profile Info (remains the same logic, assignment changes later)
        def _sync_get_info(obj): return obj.info
        profile_info_data = await _execute_yfinance_call("profile_info", _sync_get_info, yf_ticker_obj)
        all_data["profile_info"] = {
            "source_attribute": ".info",
            "data": profile_info_data if profile_info_data is not None else None
        }

        # Fetch Analyst Price Targets (remains the same logic)
        def _sync_get_analyst_targets(obj): return obj.analyst_price_targets
        analyst_targets_data = await _execute_yfinance_call("analyst_price_targets", _sync_get_analyst_targets, yf_ticker_obj)
        all_data["analyst_price_targets"] = {
            "source_attribute": ".analyst_price_targets",
            "data": analyst_targets_data
        }

        # Fetch Financial Statements (remains the same logic)
        financial_statements_map = {
            "annual_balance_sheet": {"method_name": "balance_sheet", "func": lambda obj: obj.balance_sheet},
            "quarterly_balance_sheet": {"method_name": "quarterly_balance_sheet", "func": lambda obj: obj.quarterly_balance_sheet},
            "annual_income_statement": {"method_name": "income_stmt", "func": lambda obj: obj.income_stmt},
            "quarterly_income_statement": {"method_name": "quarterly_income_stmt", "func": lambda obj: obj.quarterly_income_stmt},
            "annual_cash_flow": {"method_name": "cashflow", "func": lambda obj: obj.cashflow},
            "quarterly_cash_flow": {"method_name": "quarterly_cashflow", "func": lambda obj: obj.quarterly_cashflow},
        }
        for name, details in financial_statements_map.items():
            statement_data = await _execute_yfinance_call(name, details["func"], yf_ticker_obj)
            all_data["financials"][name] = {
                "source_method": details["method_name"],
                "data": statement_data
            }

        # Fetch Additional Ticker Data (remains the same logic)
        additional_data_map = {
            "dividends": {"source": "get_dividends()", "type": "method", "func": lambda obj: obj.get_dividends()},
            "earnings_dates": {"source": "get_earnings_dates()", "type": "method", "func": lambda obj: obj.get_earnings_dates()},
            "earnings_estimates": {"source": "get_earnings_estimate()", "type": "method", "func": lambda obj: obj.get_earnings_estimate()},
            "earnings_history": {"source": "get_earnings_history()", "type": "method", "func": lambda obj: obj.get_earnings_history()},
            "eps_revisions": {"source": "get_eps_revisions()", "type": "method", "func": lambda obj: obj.get_eps_revisions()},
            "eps_trend": {"source": "get_eps_trend()", "type": "method", "func": lambda obj: obj.get_eps_trend()},
            "growth_estimates": {"source": "get_growth_estimates()", "type": "method", "func": lambda obj: obj.get_growth_estimates()},
            "revenue_estimates": {"source": "get_revenue_estimate()", "type": "method", "func": lambda obj: obj.get_revenue_estimate()},
            "insider_purchases": {"source": "get_insider_purchases()", "type": "method", "func": lambda obj: obj.get_insider_purchases()},
            "shares_full_history": {"source": "get_shares_full()", "type": "method", "func": lambda obj: obj.get_shares_full()},
            "fast_info_summary": {"source": "get_fast_info()", "type": "method", "func": lambda obj: dict(obj.get_fast_info())},
            "ttm_income_statement": {"source": "ttm_income_stmt", "type": "attribute", "func": lambda obj: obj.ttm_income_stmt},
            "ttm_cash_flow_statement": {"source": "ttm_cashflow", "type": "attribute", "func": lambda obj: obj.ttm_cashflow},
        }
        for name, details in additional_data_map.items():
            data_point = await _execute_yfinance_call(f"additional_{name}", details["func"], yf_ticker_obj)
            source_key = "source_method" if details["type"] == "method" else "source_attribute"
            all_data["additional_ticker_data"][name] = {
                source_key: details["source"],
                "data": data_point 
            }

    # --- NEW TEXT FILE WRITING LOGIC --- 
    try:
        with open(output_filepath, 'w', encoding='utf-8') as f:
            f.write(f"# Consolidated yfinance Data for Ticker: {all_data['symbol']}\n")
            f.write(f"# Fetched around: {datetime.utcnow().isoformat()} UTC\n")
            f.write("================================================================================\n")

            # Order of sections to write
            section_order = ["profile_info", "analyst_price_targets", "financials", "additional_ticker_data", "_fetch_timestamps_utc", "_errors"]

            for main_key in section_order:
                main_value = all_data.get(main_key)
                if main_value is None and main_key not in ["_fetch_timestamps_utc", "_errors"]: # Skip if main section data is None (e.g. if Ticker object failed)
                    continue
                
                f.write(f"\n--- {main_key.upper().replace('_', ' ')} ---\n")

                if main_key in ["_fetch_timestamps_utc", "_errors"]:
                    if main_value:
                        f.write(indent_string(pprint.pformat(main_value, indent=2, width=120), 2))
                        f.write("\n")
                    else:
                        f.write("  (No data or errors logged for this section)\n")
                    continue
                
                # For structured sections like profile_info, financials, additional_ticker_data
                if isinstance(main_value, dict):
                    # Handle single item sections like profile_info, analyst_price_targets directly
                    if "source_attribute" in main_value or "source_method" in main_value:
                        source = main_value.get("source_attribute") or main_value.get("source_method")
                        data_content = main_value.get("data")
                        f.write(f"  Source: {source}\n")
                        if data_content is None:
                            f.write("    Data: None\n")
                        elif isinstance(data_content, (pd.DataFrame, pd.Series)):
                            f.write("    Data:\n")
                            f.write(indent_string(data_content.to_string(), 4))
                            f.write("\n")
                        elif isinstance(data_content, (dict, list)):
                            f.write("    Data:\n")
                            f.write(indent_string(pprint.pformat(data_content, indent=2, width=100), 4))
                            f.write("\n")
                        else:
                            f.write(f"    Data: {str(data_content)}\n")
                    else: # For nested dicts like financials, additional_ticker_data
                        for sub_key, item_obj in main_value.items():
                            if item_obj is None: # If a specific fetch like 'actions' failed and is None
                                f.write(f"\n  --- {sub_key.upper().replace('_', ' ')} ---\n")
                                f.write("    Data: Not available or fetch failed\n")
                                continue

                            f.write(f"\n  --- {sub_key.upper().replace('_', ' ')} ---\n")
                            source = item_obj.get("source_method") or item_obj.get("source_attribute")
                            data_content = item_obj.get("data")

                            if source:
                                f.write(f"    Source: {source}\n")
                            
                            if data_content is None:
                                f.write("    Data: None\n")
                            elif isinstance(data_content, (pd.DataFrame, pd.Series)):
                                # Check if DataFrame/Series is empty before calling to_string
                                if not data_content.empty:
                                    f.write("    Data:\n")
                                    f.write(indent_string(data_content.to_string(), 6))
                                    f.write("\n")
                                else:
                                    f.write("    Data: (Empty DataFrame/Series)\n")
                            elif isinstance(data_content, (dict, list)):
                                f.write("    Data:\n")
                                f.write(indent_string(pprint.pformat(data_content, indent=2, width=100), 6))
                                f.write("\n")
                            else:
                                f.write(f"    Data: {str(data_content)}\n")
                f.write("-\n") # Small separator after each sub-item data

            f.write("\n================================================================================\n")
            f.write("# End of Report\n")
        logger.info(f"Consolidated yfinance data text report saved to: {output_filepath}")
    except IOError as e:
        logger.error(f"IOError writing consolidated yfinance text report to {output_filepath}: {e}")
    except Exception as e_general:
        logger.error(f"Unexpected error writing text report: {e_general}", exc_info=True)

    logger.info(f"--- Consolidated yfinance Data Fetch Run Complete for Ticker '{ticker}' ---")

async def fetch_daily_historical_data(
    ticker_symbol: str, 
    start_date: Optional[datetime] = None, # Made optional
    end_date: Optional[datetime] = None,   # Made optional
    interval: str = "1d",
    period: Optional[str] = None        # NEW: Added period parameter
) -> Optional[pd.DataFrame]:
    """
    Fetches historical data (OHLCV) for a given ticker symbol using yfinance.
    Can fetch data between start_date and end_date OR for a specific period.
    Runs synchronous yfinance calls in a ThreadPoolExecutor.

    Args:
        ticker_symbol: The stock ticker symbol.
        start_date: The start date for data fetching (inclusive). Ignored if period is provided.
        end_date: The end date for data fetching (exclusive). Ignored if period is provided.
        interval: Data interval (e.g., "1d", "1wk", "1mo").
        period: Period of data to fetch (e.g., "1y", "max"). If provided, start_date and end_date are ignored.

    Returns:
        A pandas DataFrame with historical data, or None if fetching fails or no data.
    """
    if period:
        logger.info(f"Fetching historical data for {ticker_symbol} for period '{period}' with interval '{interval}'")
    elif start_date and end_date:
        logger.info(f"Fetching historical data for {ticker_symbol} from {start_date.date()} to {end_date.date()} with interval '{interval}'")
    else:
        logger.error(f"Insufficient parameters for historical data fetch for {ticker_symbol}. Provide 'period' or both 'start_date' and 'end_date'.")
        return None
    
    def _sync_fetch_history(symbol: str, start: Optional[datetime], end: Optional[datetime], intrvl: str, prd: Optional[str]) -> Optional[pd.DataFrame]:
        try:
            logger.debug(f"[_sync_fetch_history] Creating yf.Ticker('{symbol}')")
            ticker_obj = yf.Ticker(symbol)
            
            history_params = {"interval": intrvl}
            if prd:
                history_params["period"] = prd
                logger.debug(f"[_sync_fetch_history] Fetching .history(period='{prd}', interval='{intrvl}')")
            elif start and end:
                history_params["start"] = start
                history_params["end"] = end
                logger.debug(f"[_sync_fetch_history] Fetching .history(start={start.date()}, end={end.date()}, interval='{intrvl}')")
            else:
                # This case should be caught by the initial check in the outer function
                logger.error("[_sync_fetch_history] Invalid state: period, start, or end not properly provided.")
                return None

            history_df = ticker_obj.history(**history_params)
            
            if history_df is None or history_df.empty:
                log_msg_period_or_dates = f"for period '{prd}'" if prd else f"between {start.date() if start else 'N/A'} and {end.date() if end else 'N/A'}"
                logger.warning(f"[_sync_fetch_history] No historical data returned for {symbol} {log_msg_period_or_dates} for interval '{intrvl}'.")
                return None
            
            logger.info(f"[_sync_fetch_history] Successfully fetched {len(history_df)} rows of historical data for {symbol}.")
            return history_df
        except Exception as e:
            logger.error(f"[_sync_fetch_history] Error fetching historical data for {symbol}: {e}", exc_info=True)
            return None

    loop = asyncio.get_running_loop()
    try:
        historical_data_df = await loop.run_in_executor(
            None, # Use default executor
            _sync_fetch_history, 
            ticker_symbol, 
            start_date, 
            end_date,
            interval,
            period # Pass period to the sync function
        )
        return historical_data_df
    except Exception as e:
        logger.error(f"Error running _sync_fetch_history in executor for {ticker_symbol}: {e}", exc_info=True)
        return None

async def fetch_and_process_yahoo_info(ticker_symbol: str) -> Optional[Dict[str, Any]]: # REMOVE session
    """Fetches data from yfinance .info, processes/normalizes it, and returns a dictionary ready for DB/file.
       Returns None if fetching or essential processing fails.
    """
    logger.info(f"Fetching and processing Yahoo .info for {ticker_symbol}")
    try:
        # MODIFIED: Wrap yfinance calls in asyncio.to_thread
        def _sync_get_info():
            yf_ticker = yf.Ticker(ticker_symbol)
            return yf_ticker.info
        
        info_data = await asyncio.to_thread(_sync_get_info)
        # END MODIFICATION
        
        if not info_data:
            logger.warning(f"No .info data received from yfinance for ticker: {ticker_symbol}")
            return None

        master_data = {'ticker': ticker_symbol}

        # Helper to safely convert value to datetime from Unix timestamp
        def to_datetime_from_timestamp(value):
            if value is not None:
                try:
                    # Handle potential large negative timestamps or other errors
                    # Ensure it's treated as an integer first
                    ts = int(value)
                    # Basic sanity check for reasonable date range (e.g., > 1900)
                    # This avoids OverflowError on some systems for large negative numbers
                    if ts < -1577923200: # Approx year 1920
                         logger.warning(f"Timestamp {value} seems out of range, skipping conversion for {ticker_symbol}.")
                         return None
                    return datetime.fromtimestamp(ts)
                except (ValueError, TypeError, OSError): 
                    logger.warning(f"Could not convert value '{value}' to datetime for {ticker_symbol}. Invalid timestamp.")
                    return None
            return None

        # Helper to safely convert value to float
        def to_float(value):
            if value is not None:
                try:
                    return float(value)
                except (ValueError, TypeError):
                    # Log warning but don't return None immediately inside lambda
                    # logger.warning(f"Could not convert value '{value}' to float for {ticker_symbol}.") 
                    return None
            return None

        # Helper to safely convert value to int
        def to_int(value):
            if value is not None:
                try:
                    return int(float(value)) # Convert to float first to handle e.g. 3.0
                except (ValueError, TypeError):
                    # logger.warning(f"Could not convert value '{value}' to int for {ticker_symbol}.")
                    return None
            return None

        # Normalizer for percentage fields (dividend yields)
        def normalize_yield(value):
            f_val = to_float(value)
            if f_val is not None:
                 # Heuristic: if value looks like a percentage point (e.g., 0.1 to 100), convert to decimal
                 # Adjust range as needed based on observation
                 if 0.01 < abs(f_val) <= 100: 
                     logger.debug(f"Normalizing yield value {f_val} for {ticker_symbol} by dividing by 100.")
                     return f_val / 100.0
                 else: # Assume it's already a decimal or outside the likely percentage range
                      logger.debug(f"Yield value {f_val} for {ticker_symbol} assumed to be decimal already.")
                      return f_val 
            return None

        # Field mapping: (db_column_name, yahoo_info_key, type_converter_or_normalizer_func)
        field_map = {
            # Static
            'company_name': ('longName', str),
            'country': ('country', str),
            'exchange': ('exchange', str),
            'industry': ('industryKey', str),
            'sector': ('sectorKey', str),
            'trade_currency': ('currency', str),
            'asset_type': ('quoteType', lambda x: str(x).upper() if x else None), # Use quoteType, convert to upper string
            'financial_currency': ('financialCurrency', str), # NEW FIELD

            # Market
            'average_volume': ('averageVolume', to_float),
            'beta': ('beta', to_float),
            'current_price': ('currentPrice', to_float), 
            'dividend_date': ('dividendDate', to_datetime_from_timestamp),
            'dividend_date_last': ('lastDividendDate', to_datetime_from_timestamp),
            'dividend_ex_date': ('exDividendDate', to_datetime_from_timestamp),
            'dividend_value_last': ('lastDividendValue', to_float),
            'dividend_yield': ('dividendYield', normalize_yield), # Use refined normalizer
            'dividend_yield_ttm': ('trailingAnnualDividendYield', to_float), # Assume this is usually decimal
            'earnings_timestamp': ('earningsTimestamp', to_datetime_from_timestamp),
            'eps_forward': ('forwardEps', to_float),
            'fifty_two_week_change': ('52WeekChange', to_float),
            'fifty_two_week_high': ('fiftyTwoWeekHigh', to_float),
            'fifty_two_week_low': ('fiftyTwoWeekLow', to_float),
            'five_year_avg_dividend_yield': ('fiveYearAvgDividendYield', normalize_yield), # Use refined normalizer
            'market_cap': ('marketCap', to_float),
            'overall_risk': ('overallRisk', to_int),
            'pe_forward': ('forwardPE', to_float),
            'price_eps_current_year': ('priceEpsCurrentYear', to_float),
            'price_to_book': ('priceToBook', to_float),
            'price_to_sales_ttm': ('priceToSalesTrailing12Months', to_float),
            'recommendation_key': ('recommendationKey', str),
            'recommendation_mean': ('recommendationMean', to_float),
            'regular_market_change': ('regularMarketChange', to_float),
            'regular_market_day_high': ('regularMarketDayHigh', to_float),
            'regular_market_day_low': ('regularMarketDayLow', to_float),
            'regular_market_open': ('regularMarketOpen', to_float),
            'regular_market_previous_close': ('regularMarketPreviousClose', to_float),
            'shares_percent_insiders': ('heldPercentInsiders', to_float),
            'shares_percent_institutions': ('heldPercentInstitutions', to_float),
            'shares_short': ('sharesShort', to_float),
            'shares_short_prior_month': ('sharesShortPriorMonth', to_float),
            'shares_short_prior_month_date': ('sharesShortPreviousMonthDate', to_datetime_from_timestamp),
            'short_percent_of_float': ('shortPercentOfFloat', to_float),
            'short_ratio': ('shortRatio', to_float),
            'sma_fifty_day': ('fiftyDayAverage', to_float),
            'sma_two_hundred_day': ('twoHundredDayAverage', to_float),
            'target_mean_price': ('targetMeanPrice', to_float),
            'target_median_price': ('targetMedianPrice', to_float),
            'trailing_pe': ('trailingPE', to_float),
            'trailing_peg_ratio': ('trailingPegRatio', to_float),

            # Financial Summary
            'book_value': ('bookValue', to_float),
            'current_ratio': ('currentRatio', to_float),
            'debt_to_equity': ('debtToEquity', to_float),
            'dividend_rate': ('dividendRate', to_float),
            'dividend_rate_ttm': ('trailingAnnualDividendRate', to_float),
            'earnings_growth': ('earningsGrowth', to_float),
            'earnings_quarterly_growth': ('earningsQuarterlyGrowth', to_float),
            'ebitda_margin': ('ebitdaMargins', to_float),
            'enterprise_to_ebitda': ('enterpriseToEbitda', to_float),
            'enterprise_to_revenue': ('enterpriseToRevenue', to_float),
            'enterprise_value': ('enterpriseValue', to_float),
            'eps_current_year': ('epsCurrentYear', to_float),
            'gross_margin': ('grossMargins', to_float),
            'last_fiscal_year_end': ('lastFiscalYearEnd', to_datetime_from_timestamp),
            'operating_margin': ('operatingMargins', to_float),
            'payout_ratio': ('payoutRatio', to_float),
            'profit_margin': ('profitMargins', to_float),
            'quick_ratio': ('quickRatio', to_float),
            'return_on_assets': ('returnOnAssets', to_float),
            'return_on_equity': ('returnOnEquity', to_float),
            'revenue_growth': ('revenueGrowth', to_float),
            'revenue_per_share': ('revenuePerShare', to_float),
            'shares_float': ('floatShares', to_float),
            'shares_outstanding': ('sharesOutstanding', to_float),
            'shares_outstanding_implied': ('impliedSharesOutstanding', to_float),
            'total_cash_per_share': ('totalCashPerShare', to_float),
            'eps_ttm': ('trailingEps', to_float),
        }

        for db_key, (yahoo_key, converter_func) in field_map.items():
            raw_value = info_data.get(yahoo_key)
            master_data[db_key] = None # Default to None
            if raw_value is not None:
                try:
                    if converter_func == str:
                        master_data[db_key] = str(raw_value)
                    else:
                        processed_value = converter_func(raw_value)
                        master_data[db_key] = processed_value
                        # Log conversion warnings from helpers if needed (currently suppressed)
                        if processed_value is None and converter_func in [to_float, to_int]:
                            pass # logger.warning(f"Conversion failed for {db_key} ('{raw_value}'), stored as None.")
                        elif processed_value is None and converter_func == to_datetime_from_timestamp:
                            pass # Warning already logged in helper
                        elif processed_value is None and converter_func == normalize_yield:
                             logger.warning(f"Could not convert/normalize yield {db_key} ('{raw_value}'), stored as None.")

                except Exception as e:
                    logger.error(f"Error converting/assigning Yahoo key '{yahoo_key}' to DB key '{db_key}' for {ticker_symbol}: {e}. Raw value: '{raw_value}'")
                    master_data[db_key] = None # Ensure None on error

        # Timestamps for the record update (set just before returning)
        now = datetime.now()
        master_data['update_last_full'] = now
        master_data['update_marketonly'] = now
        
        logger.info(f"Successfully processed Yahoo .info data for {ticker_symbol}")
        return master_data

    except AttributeError as ae:
        # Handle cases where yfinance might return an error dict
        logger.error(f"AttributeError processing {ticker_symbol} (possibly bad ticker or yfinance error dict): {ae}", exc_info=False) # Log less verbosely
        if hasattr(yf_ticker, 'info') and isinstance(yf_ticker.info, dict):
             logger.error(f"yfinance returned info dict: {yf_ticker.info}") # Log the dict if available
        return None
    except Exception as e:
        logger.error(f"Failed to fetch or process Yahoo .info for {ticker_symbol}: {e}", exc_info=True)
        return None

def write_processed_data_to_file(processed_data: Dict[str, Any]):
    """Writes the processed data dictionary to a text file."""
    ticker = processed_data.get('ticker')
    if not ticker:
        logger.error("Cannot write file: Ticker missing from processed data.")
        return

    try:
        # Generate timestamp
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Define output directory and filename
        output_dir = os.path.join("data", "yahoo_info_dumps")
        filename = f"{ticker}_info_{timestamp_str}.txt"
        output_path = os.path.join(output_dir, filename)

        # Ensure directory exists
        os.makedirs(output_dir, exist_ok=True)

        logger.info(f"Writing processed data for {ticker} to file: {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            for key, value in processed_data.items():
                # Format value for printing (handle None, datetime)
                if isinstance(value, datetime):
                    value_str = value.isoformat()
                elif value is None:
                    value_str = "None"
                else:
                    value_str = str(value)
                f.write(f"{key}: {value_str}\n")
        logger.info(f"Successfully wrote data to {output_path}")

    except IOError as e:
        logger.error(f"Failed to write processed data file for {ticker}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error writing processed data file for {ticker}: {e}", exc_info=True)

# --- NEW: Function to update specific market data from info --- 
async def update_ticker_master_market_data(ticker_symbol: str, db_repo: YahooDataRepository):
    """Fetches data using yfinance .info and updates specific market fields in ticker_master.
    
    Fields updated (if available in .info):
    - sma_fifty_day
    - sma_two_hundred_day
    - regular_market_open
    - regular_market_previous_close
    - current_price
    - regular_market_day_high
    - regular_market_day_low
    - regular_market_change (calculated)
    - update_marketonly (timestamp)
    """
    logger.info(f"Fetching .info to update market data for {ticker_symbol}")
    try:
        # MODIFIED: Wrap yfinance calls in asyncio.to_thread
        def _sync_get_info_for_market_data():
            yf_ticker = yf.Ticker(ticker_symbol)
            return yf_ticker.info

        info_data = await asyncio.to_thread(_sync_get_info_for_market_data)
        # END MODIFICATION

        if not info_data:
            logger.warning(f"No .info data received from yfinance for ticker: {ticker_symbol}")
            return

        market_updates = {}
        fetched_values_log = {} # Log raw values fetched for specific keys

        # --- Helper to safely convert value to float (reuse or redefine locally if needed) ---
        def to_float_fast(value):
            if value is not None:
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None
            return None
        # --- End Helper ---

        # Map DB columns to the desired Yahoo .info keys 
        # db_column_key: yahoo_info_key
        info_key_map = {
            'sma_fifty_day': 'fiftyDayAverage',
            'sma_two_hundred_day': 'twoHundredDayAverage',
            'regular_market_open': 'open', # Mapping from .info 'open'
            'regular_market_previous_close': 'previousClose',
            'current_price': 'currentPrice', # Mapping from .info 'currentPrice'
            'regular_market_day_high': 'dayHigh',
            'regular_market_day_low': 'dayLow',
            'regular_market_change': 'regularMarketChange' # ADD direct mapping
        }

        # Process only the specified keys
        for db_key, info_key in info_key_map.items():
            raw_value = info_data.get(info_key) # Get value from .info dict
            fetched_values_log[info_key] = raw_value # Log the raw value fetched
            processed_value = to_float_fast(raw_value)
            
            # --- ADD DETAILED DEBUG LOGGING INSIDE LOOP ---
            logger.debug(f"Processing info key '{info_key}' for db key '{db_key}': raw='{raw_value}', processed='{processed_value}'")
            # --- END DEBUG LOGGING ---
            
            if processed_value is not None:
                market_updates[db_key] = processed_value

        # Log the specifically fetched raw values
        logger.info(f"Fetched raw values from .info for {ticker_symbol}: {fetched_values_log}")

        # Add the market update timestamp
        market_updates['update_marketonly'] = datetime.now()

        if len(market_updates) > 1: # Check if there's more than just the timestamp to update
            logger.info(f"Updating market data in DB for {ticker_symbol} with: {market_updates}") # Log the dict being sent
            await db_repo.update_ticker_master_fields(ticker_symbol, market_updates)
            # Success/failure message logged by the repo method
        else:
            logger.info(f"No valid market data updates found from .info for {ticker_symbol}. Skipping DB update.")

    except Exception as e:
        logger.error(f"Failed to fetch .info or update market data for {ticker_symbol}: {e}", exc_info=True)

# --- NEW: Function to fetch and upsert Analyst Price Targets Summary ---
async def fetch_and_upsert_analyst_targets_summary(ticker_symbol: str, db_repo: YahooDataRepository):
    """Fetches analyst price target data from yfinance .analyst_price_targets,
       handles if it's a DataFrame or a dictionary, and upserts it into the ticker_data_items table.
    """
    logger.info(f"--- Starting Fetch & Upsert for Analyst Price Targets: {ticker_symbol} (from .analyst_price_targets) ---")
    
    try:
        # MODIFIED: Wrap yfinance calls in asyncio.to_thread
        def _sync_get_analyst_targets():
            yf_ticker = yf.Ticker(ticker_symbol)
            return yf_ticker.analyst_price_targets
        analyst_targets_data = await asyncio.to_thread(_sync_get_analyst_targets)
        # END MODIFICATION
        
        payload_data = None

        if isinstance(analyst_targets_data, pd.DataFrame) and not analyst_targets_data.empty:
            logger.info(f"[Analyst Targets Upsert] Received a DataFrame from .analyst_price_targets for {ticker_symbol}. Using it as payload.")
            payload_data = analyst_targets_data # PandasEncoder will handle this
        elif isinstance(analyst_targets_data, dict) and analyst_targets_data: # Check if it's a non-empty dict
            logger.info(f"[Analyst Targets Upsert] Received a dictionary from .analyst_price_targets for {ticker_symbol}. Original dict: {analyst_targets_data}")
            # Exclude the 'current' key if present, as currentPrice is in ticker_master
            payload_data = {k: v for k, v in analyst_targets_data.items() if k != 'current'}
            if not payload_data: # If dict becomes empty after removing 'current'
                logger.warning(f"[Analyst Targets Upsert] Dictionary became empty after removing 'current' key for {ticker_symbol}. Skipping upsert.")
                return
            logger.info(f"[Analyst Targets Upsert] Using dictionary as payload (with 'current' key removed if it was present): {payload_data}")
        else:
            logger.warning(f"[Analyst Targets Upsert] Data from .analyst_price_targets for {ticker_symbol} is None, empty, or not a recognized type (DataFrame/dict). Skipping upsert.")
            if analyst_targets_data is not None: # Log if it's some other unexpected type
                 logger.info(f"[Analyst Targets Upsert] Unexpected data type received: {type(analyst_targets_data)}, data: {analyst_targets_data}")
            return

        item_data = {
            'ticker': ticker_symbol,
            'item_type': "ANALYST_PRICE_TARGETS", 
            'item_time_coverage': "CUMULATIVE_SNAPSHOT", 
            'item_key_date': datetime.now(),
            'item_source': "yfinance.Ticker.analyst_price_targets",
            'item_data_payload': payload_data 
        }

        logger.info(f"[Analyst Targets Upsert] Prepared item_data for {ticker_symbol}.")

        inserted_id = await db_repo.upsert_ticker_data_item(item_data)
        
        if inserted_id:
            logger.info(f"[Analyst Targets Upsert] Successfully upserted analyst targets for {ticker_symbol}, ID: {inserted_id}.")
        else:
            logger.error(f"[Analyst Targets Upsert] Failed to upsert analyst targets for {ticker_symbol}.")

    except Exception as e:
        logger.error(f"[Analyst Targets Upsert] Error fetching/processing/upserting analyst targets for {ticker_symbol}: {e}", exc_info=True)
    finally:
        logger.info(f"--- Fetch & Upsert FINISHED for Analyst Price Targets: {ticker_symbol} (from .analyst_price_targets) ---")

# --- NEW: Function to fetch and store Annual Balance Sheets ---
async def fetch_and_store_annual_balance_sheets(ticker_symbol: str, db_repo: YahooDataRepository):
    """Fetches annual balance sheets from yfinance (.balance_sheet)
       and stores each annual report as a separate item in ticker_data_items.
    """
    logger.info(f"--- Starting Fetch & Store for Annual Balance Sheets: {ticker_symbol} ---")
    
    try:
        # MODIFIED: Wrap yfinance calls in asyncio.to_thread
        def _sync_get_annual_bs():
            yf_ticker = yf.Ticker(ticker_symbol)
            return yf_ticker.balance_sheet
        annual_bs_df = await asyncio.to_thread(_sync_get_annual_bs)
        # END MODIFICATION
        
        if not isinstance(annual_bs_df, pd.DataFrame) or annual_bs_df.empty:
            logger.warning(f"[Annual BS Store] No DataFrame received from .balance_sheet for {ticker_symbol}, or DataFrame is empty. Skipping.")
            return

        logger.info(f"[Annual BS Store] Received DataFrame with shape {annual_bs_df.shape} for {ticker_symbol}. Iterating through columns (dates).")
        
        items_inserted_count = 0
        for date_col in annual_bs_df.columns:
            try:
                # The column header (date_col) is the key date for this balance sheet
                # yfinance typically returns these as pd.Timestamp objects
                item_key_date_ts = pd.to_datetime(date_col) # Ensure it's a pandas Timestamp
                item_key_date_naive = item_key_date_ts.to_pydatetime().replace(tzinfo=None) # Convert to naive python datetime

                # The data for this column is a Series of financial items for that date
                sheet_data_for_date = annual_bs_df[date_col]
                
                # Convert the Series to a dictionary, then to JSON for the payload
                # Handle potential NaN values by dropping them or converting to None for JSON
                payload_dict = sheet_data_for_date.where(pd.notnull(sheet_data_for_date), None).to_dict()
                
                if not payload_dict: # If all values were NaN
                    logger.warning(f"[Annual BS Store] Balance sheet for {ticker_symbol} on {item_key_date_naive.date()} was empty or all NaN. Skipping.")
                    continue

                item_data = {
                    'ticker': ticker_symbol,
                    'item_type': "BALANCE_SHEET",
                    'item_time_coverage': "FYEAR",
                    'item_key_date': item_key_date_naive,
                    'item_source': "yfinance.Ticker.balance_sheet",
                    'item_data_payload': payload_dict # The repository will handle JSON conversion
                }

                logger.debug(f"[Annual BS Store] Prepared item_data for {ticker_symbol}, date {item_key_date_naive.date()}.")
                
                inserted_id = await db_repo.insert_ticker_data_item(item_data)
                if inserted_id:
                    items_inserted_count += 1
                    logger.info(f"[Annual BS Store] Successfully stored annual balance sheet for {ticker_symbol}, date {item_key_date_naive.date()}, ID: {inserted_id}.")
                else:
                    logger.error(f"[Annual BS Store] Failed to store annual balance sheet for {ticker_symbol}, date {item_key_date_naive.date()}.")
            
            except Exception as col_err:
                logger.error(f"[Annual BS Store] Error processing column '{str(date_col)}' for {ticker_symbol}: {col_err}", exc_info=True)
                # Continue to the next column if one fails

        logger.info(f"[Annual BS Store] Finished processing. Stored {items_inserted_count} annual balance sheets for {ticker_symbol}.")

    except Exception as e:
        logger.error(f"[Annual BS Store] Error fetching/processing annual balance sheets for {ticker_symbol}: {e}", exc_info=True)
    finally:
        logger.info(f"--- Fetch & Store FINISHED for Annual Balance Sheets: {ticker_symbol} ---")
# --- End Annual Balance Sheets ---

# --- NEW: Function to fetch and store Quarterly Balance Sheets ---
async def fetch_and_store_quarterly_balance_sheets(ticker_symbol: str, db_repo: YahooDataRepository):
    """Fetches quarterly balance sheets from yfinance (.quarterly_balance_sheet)
       and stores each quarterly report as a separate item in ticker_data_items.
       item_type will be 'BALANCE_SHEET' and item_time_coverage will be 'QUARTER'.
    """
    logger.info(f"--- Starting Fetch & Store for Quarterly Balance Sheets: {ticker_symbol} ---")
    
    try:
        # MODIFIED: Wrap yfinance calls in asyncio.to_thread
        def _sync_get_quarterly_bs():
            yf_ticker = yf.Ticker(ticker_symbol)
            return yf_ticker.quarterly_balance_sheet
        quarterly_bs_df = await asyncio.to_thread(_sync_get_quarterly_bs)
        # END MODIFICATION
        
        if not isinstance(quarterly_bs_df, pd.DataFrame) or quarterly_bs_df.empty:
            logger.warning(f"[Quarterly BS Store] No DataFrame received from .quarterly_balance_sheet for {ticker_symbol}, or DataFrame is empty. Skipping.")
            return

        logger.info(f"[Quarterly BS Store] Received DataFrame with shape {quarterly_bs_df.shape} for {ticker_symbol}. Iterating through columns (dates).")
        
        items_inserted_count = 0
        for date_col in quarterly_bs_df.columns:
            try:
                item_key_date_ts = pd.to_datetime(date_col) # Ensure pandas Timestamp
                item_key_date_naive = item_key_date_ts.to_pydatetime().replace(tzinfo=None) # Naive python datetime

                sheet_data_for_date = quarterly_bs_df[date_col]
                payload_dict = sheet_data_for_date.where(pd.notnull(sheet_data_for_date), None).to_dict()
                
                if not payload_dict:
                    logger.warning(f"[Quarterly BS Store] Balance sheet for {ticker_symbol} on {item_key_date_naive.date()} was empty or all NaN. Skipping.")
                    continue

                item_data = {
                    'ticker': ticker_symbol,
                    'item_type': "BALANCE_SHEET", # Consistent item_type
                    'item_time_coverage': "QUARTER",   # Differentiator
                    'item_key_date': item_key_date_naive,
                    'item_source': "yfinance.Ticker.quarterly_balance_sheet",
                    'item_data_payload': payload_dict
                }

                logger.debug(f"[Quarterly BS Store] Prepared item_data for {ticker_symbol}, date {item_key_date_naive.date()}.")
                
                inserted_id = await db_repo.insert_ticker_data_item(item_data)
                if inserted_id:
                    items_inserted_count += 1
                    logger.info(f"[Quarterly BS Store] Successfully stored quarterly balance sheet for {ticker_symbol}, date {item_key_date_naive.date()}, ID: {inserted_id}.")
                # else: (Already logged by insert_ticker_data_item if conflict or error)
            
            except Exception as col_err:
                logger.error(f"[Quarterly BS Store] Error processing column '{str(date_col)}' for {ticker_symbol}: {col_err}", exc_info=True)
                # Continue to the next column

        logger.info(f"[Quarterly BS Store] Finished processing. Stored {items_inserted_count} quarterly balance sheets for {ticker_symbol}.")

    except Exception as e:
        logger.error(f"[Quarterly BS Store] Error fetching/processing quarterly balance sheets for {ticker_symbol}: {e}", exc_info=True)
    finally:
        logger.info(f"--- Fetch & Store FINISHED for Quarterly Balance Sheets: {ticker_symbol} ---")
# --- End Quarterly Balance Sheets ---

# --- NEW: Functions to fetch and store Income Statements (Annual, Quarterly, TTM) ---

async def fetch_and_store_annual_income_statements(ticker_symbol: str, db_repo: YahooDataRepository):
    """Fetches annual income statements from yfinance (.income_stmt)
       and stores each annual report as a separate item in ticker_data_items.
       item_type: INCOME_STATEMENT, item_time_coverage: FYEAR.
    """
    logger.info(f"--- Starting Fetch & Store for Annual Income Statements: {ticker_symbol} ---")
    try:
        # MODIFIED: Wrap yfinance calls in asyncio.to_thread
        def _sync_get_annual_is():
            yf_ticker = yf.Ticker(ticker_symbol)
            return yf_ticker.income_stmt
        annual_is_df = await asyncio.to_thread(_sync_get_annual_is)
        # END MODIFICATION
        
        if not isinstance(annual_is_df, pd.DataFrame) or annual_is_df.empty:
            logger.warning(f"[Annual IS Store] No DataFrame from .income_stmt for {ticker_symbol}, or empty. Skipping.")
            return

        logger.info(f"[Annual IS Store] DataFrame shape {annual_is_df.shape} for {ticker_symbol}. Iterating columns.")
        items_inserted_count = 0
        for date_col in annual_is_df.columns:
            try:
                item_key_date_ts = pd.to_datetime(date_col)
                item_key_date_naive = item_key_date_ts.to_pydatetime().replace(tzinfo=None)
                statement_data = annual_is_df[date_col]
                payload_dict = statement_data.where(pd.notnull(statement_data), None).to_dict()
                if not payload_dict: continue

                item_data = {
                    'ticker': ticker_symbol,
                    'item_type': "INCOME_STATEMENT",
                    'item_time_coverage': "FYEAR",
                    'item_key_date': item_key_date_naive,
                    'item_source': "yfinance.Ticker.income_stmt",
                    'item_data_payload': payload_dict
                }
                inserted_id = await db_repo.insert_ticker_data_item(item_data)
                if inserted_id: items_inserted_count += 1
            except Exception as col_err:
                logger.error(f"[Annual IS Store] Error processing column '{str(date_col)}' for {ticker_symbol}: {col_err}", exc_info=True)
        logger.info(f"[Annual IS Store] Stored {items_inserted_count} annual income statements for {ticker_symbol}.")
    except Exception as e:
        logger.error(f"[Annual IS Store] Error fetching/processing for {ticker_symbol}: {e}", exc_info=True)
    finally:
        logger.info(f"--- Fetch & Store FINISHED for Annual Income Statements: {ticker_symbol} ---")

async def fetch_and_store_quarterly_income_statements(ticker_symbol: str, db_repo: YahooDataRepository):
    """Fetches quarterly income statements from yfinance (.quarterly_income_stmt)
       and stores each quarterly report as a separate item in ticker_data_items.
       item_type: INCOME_STATEMENT, item_time_coverage: QUARTER.
    """
    logger.info(f"--- Starting Fetch & Store for Quarterly Income Statements: {ticker_symbol} ---")
    try:
        # MODIFIED: Wrap yfinance calls in asyncio.to_thread
        def _sync_get_quarterly_is():
            yf_ticker = yf.Ticker(ticker_symbol)
            return yf_ticker.quarterly_income_stmt
        quarterly_is_df = await asyncio.to_thread(_sync_get_quarterly_is)
        # END MODIFICATION
        
        if not isinstance(quarterly_is_df, pd.DataFrame) or quarterly_is_df.empty:
            logger.warning(f"[Quarterly IS Store] No DataFrame from .quarterly_income_stmt for {ticker_symbol}, or empty. Skipping.")
            return

        logger.info(f"[Quarterly IS Store] DataFrame shape {quarterly_is_df.shape} for {ticker_symbol}. Iterating columns.")
        items_inserted_count = 0
        for date_col in quarterly_is_df.columns:
            try:
                item_key_date_ts = pd.to_datetime(date_col)
                item_key_date_naive = item_key_date_ts.to_pydatetime().replace(tzinfo=None)
                statement_data = quarterly_is_df[date_col]
                payload_dict = statement_data.where(pd.notnull(statement_data), None).to_dict()
                if not payload_dict: continue

                item_data = {
                    'ticker': ticker_symbol,
                    'item_type': "INCOME_STATEMENT",
                    'item_time_coverage': "QUARTER",
                    'item_key_date': item_key_date_naive,
                    'item_source': "yfinance.Ticker.quarterly_income_stmt",
                    'item_data_payload': payload_dict
                }
                inserted_id = await db_repo.insert_ticker_data_item(item_data)
                if inserted_id: items_inserted_count += 1
            except Exception as col_err:
                logger.error(f"[Quarterly IS Store] Error processing column '{str(date_col)}' for {ticker_symbol}: {col_err}", exc_info=True)
        logger.info(f"[Quarterly IS Store] Stored {items_inserted_count} quarterly income statements for {ticker_symbol}.")
    except Exception as e:
        logger.error(f"[Quarterly IS Store] Error fetching/processing for {ticker_symbol}: {e}", exc_info=True)
    finally:
        logger.info(f"--- Fetch & Store FINISHED for Quarterly Income Statements: {ticker_symbol} ---")

async def fetch_and_store_ttm_income_statement(ticker_symbol: str, db_repo: YahooDataRepository):
    """Fetches the TTM income statement from yfinance (.ttm_income_stmt)
       and stores it as a single item in ticker_data_items.
       item_type: INCOME_STATEMENT, item_time_coverage: TTM.
       item_key_date is derived from the name of the TTM Series.
    """
    logger.info(f"--- Starting Fetch & Store for TTM Income Statement: {ticker_symbol} ---")
    try:
        # MODIFIED: Wrap yfinance calls in asyncio.to_thread
        def _sync_get_ttm_is():
            yf_ticker = yf.Ticker(ticker_symbol)
            return yf_ticker.ttm_income_stmt
        ttm_data_raw = await asyncio.to_thread(_sync_get_ttm_is)
        # END MODIFICATION
        ttm_is_series = None # Initialize

        # --- DIAGNOSTIC LOGGING (keeping it for now) ---
        # logger.info(f"[TTM IS Store - DIAGNOSTIC] Raw ttm_data_raw for {ticker_symbol}: {ttm_data_raw}")
        # logger.info(f"[TTM IS Store - DIAGNOSTIC] Type of ttm_data_raw for {ticker_symbol}: {type(ttm_data_raw)}")
        # --- END DIAGNOSTIC LOGGING ---

        # --- NEW LOGIC to handle DataFrame or Series ---
        if isinstance(ttm_data_raw, pd.DataFrame):
            if not ttm_data_raw.empty and len(ttm_data_raw.columns) > 0:
                # Assume the first column is the TTM data, and its name is the date
                ttm_is_series = ttm_data_raw.iloc[:, 0] 
                # The name of the series (which was the column name) should be the date string for key_date
                # The series itself does not retain the column name as its .name property directly in this case.
                # We get the date from the column name of the original DataFrame.
                series_name_date_str = str(ttm_data_raw.columns[0]) # Explicitly get column name
                logger.info(f"[TTM IS Store] Processed as DataFrame. Extracted Series for column '{series_name_date_str}'.")
            else:
                logger.warning(f"[TTM IS Store] ttm_income_stmt for {ticker_symbol} is an empty DataFrame or has no columns. Skipping.")
                return
        elif isinstance(ttm_data_raw, pd.Series):
            ttm_is_series = ttm_data_raw
            series_name_date_str = str(ttm_is_series.name) # Get from Series name
            logger.info(f"[TTM IS Store] Processed as Series. Series name for date: '{series_name_date_str}'.")
        else:
            logger.warning(f"[TTM IS Store] ttm_income_stmt for {ticker_symbol} is not a Series or DataFrame. Type: {type(ttm_data_raw)}. Skipping.")
            return
        
        if ttm_is_series is None or ttm_is_series.empty:
            logger.warning(f"[TTM IS Store] Resulting TTM series for {ticker_symbol} is None or empty after processing. Skipping.")
            return
        # --- END NEW LOGIC ---

        # The name of the Series (or the first column name of DF) is typically the end date of the TTM period
        # series_name_date_str = ttm_is_series.name # This was original, now handled above
        item_key_date_naive = None
        if series_name_date_str:
            try:
                item_key_date_naive = pd.to_datetime(series_name_date_str).to_pydatetime().replace(tzinfo=None)
            except ValueError as ve:
                logger.error(f"[TTM IS Store] Could not parse date '{series_name_date_str}' from TTM series name for {ticker_symbol}: {ve}. Using current date as fallback.")
                item_key_date_naive = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) # Fallback
        else:
            logger.warning(f"[TTM IS Store] TTM income statement series for {ticker_symbol} has no name. Using current date as fallback for item_key_date.")
            item_key_date_naive = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) # Fallback

        payload_dict = ttm_is_series.where(pd.notnull(ttm_is_series), None).to_dict()
        if not payload_dict:
            logger.warning(f"[TTM IS Store] TTM Income Statement for {ticker_symbol} is empty after to_dict. Skipping.")
            return

        item_data = {
            'ticker': ticker_symbol,
            'item_type': "INCOME_STATEMENT",
            'item_time_coverage': "TTM",
            'item_key_date': item_key_date_naive,
            'item_source': "yfinance.Ticker.ttm_income_stmt",
            'item_data_payload': payload_dict
        }
        
        logger.debug(f"[TTM IS Store] Prepared item_data for {ticker_symbol}, key_date {item_key_date_naive.date()}.")
        inserted_id = await db_repo.upsert_single_ttm_statement(item_data)
        if inserted_id:
            logger.info(f"[TTM IS Store] Successfully processed TTM income statement for {ticker_symbol} (inserted/updated), new/effective ID: {inserted_id}.")
        else:
            logger.info(f"[TTM IS Store] TTM income statement for {ticker_symbol} (key_date {item_key_date_naive.date()}) likely already existed or an error occurred; no new record inserted or no older records to manage.")
        # else: (Already logged by insert_ticker_data_item if conflict or error)

    except Exception as e:
        logger.error(f"[TTM IS Store] Error fetching/processing for {ticker_symbol}: {e}", exc_info=True)
    finally:
        logger.info(f"--- Fetch & Store FINISHED for TTM Income Statement: {ticker_symbol} ---")

# --- End Income Statement Functions ---

# --- NEW: Functions to fetch and store Cash Flow Statements (Annual, Quarterly, TTM) ---

async def fetch_and_store_annual_cash_flow_statements(ticker_symbol: str, db_repo: YahooDataRepository):
    """Fetches annual cash flow statements from yfinance (.cashflow)
       and stores each annual report as a separate item in ticker_data_items.
       item_type: CASH_FLOW_STATEMENT, item_time_coverage: FYEAR.
    """
    logger.info(f"--- Starting Fetch & Store for Annual Cash Flow Statements: {ticker_symbol} ---")
    try:
        # MODIFIED: Wrap yfinance calls in asyncio.to_thread
        def _sync_get_annual_cf():
            yf_ticker = yf.Ticker(ticker_symbol)
            return yf_ticker.cashflow
        annual_cf_df = await asyncio.to_thread(_sync_get_annual_cf)
        # END MODIFICATION
        
        if not isinstance(annual_cf_df, pd.DataFrame) or annual_cf_df.empty:
            logger.warning(f"[Annual CF Store] No DataFrame from .cashflow for {ticker_symbol}, or empty. Skipping.")
            return

        logger.info(f"[Annual CF Store] DataFrame shape {annual_cf_df.shape} for {ticker_symbol}. Iterating columns.")
        items_inserted_count = 0
        for date_col in annual_cf_df.columns:
            try:
                item_key_date_ts = pd.to_datetime(date_col)
                item_key_date_naive = item_key_date_ts.to_pydatetime().replace(tzinfo=None)
                statement_data = annual_cf_df[date_col]
                payload_dict = statement_data.where(pd.notnull(statement_data), None).to_dict()
                if not payload_dict: continue

                item_data = {
                    'ticker': ticker_symbol,
                    'item_type': "CASH_FLOW_STATEMENT",
                    'item_time_coverage': "FYEAR",
                    'item_key_date': item_key_date_naive,
                    'item_source': "yfinance.Ticker.cashflow",
                    'item_data_payload': payload_dict
                }
                inserted_id = await db_repo.insert_ticker_data_item(item_data)
                if inserted_id: items_inserted_count += 1
            except Exception as col_err:
                logger.error(f"[Annual CF Store] Error processing column '{str(date_col)}' for {ticker_symbol}: {col_err}", exc_info=True)
        logger.info(f"[Annual CF Store] Stored {items_inserted_count} annual cash flow statements for {ticker_symbol}.")
    except Exception as e:
        logger.error(f"[Annual CF Store] Error fetching/processing for {ticker_symbol}: {e}", exc_info=True)
    finally:
        logger.info(f"--- Fetch & Store FINISHED for Annual Cash Flow Statements: {ticker_symbol} ---")

async def fetch_and_store_quarterly_cash_flow_statements(ticker_symbol: str, db_repo: YahooDataRepository):
    """Fetches quarterly cash flow statements from yfinance (.quarterly_cashflow)
       and stores each quarterly report as a separate item in ticker_data_items.
       item_type: CASH_FLOW_STATEMENT, item_time_coverage: QUARTER.
    """
    logger.info(f"--- Starting Fetch & Store for Quarterly Cash Flow Statements: {ticker_symbol} ---")
    try:
        # MODIFIED: Wrap yfinance calls in asyncio.to_thread
        def _sync_get_quarterly_cf():
            yf_ticker = yf.Ticker(ticker_symbol)
            return yf_ticker.quarterly_cashflow
        quarterly_cf_df = await asyncio.to_thread(_sync_get_quarterly_cf)
        # END MODIFICATION
        
        if not isinstance(quarterly_cf_df, pd.DataFrame) or quarterly_cf_df.empty:
            logger.warning(f"[Quarterly CF Store] No DataFrame from .quarterly_cashflow for {ticker_symbol}, or empty. Skipping.")
            return

        logger.info(f"[Quarterly CF Store] DataFrame shape {quarterly_cf_df.shape} for {ticker_symbol}. Iterating columns.")
        items_inserted_count = 0
        for date_col in quarterly_cf_df.columns:
            try:
                item_key_date_ts = pd.to_datetime(date_col)
                item_key_date_naive = item_key_date_ts.to_pydatetime().replace(tzinfo=None)
                statement_data = quarterly_cf_df[date_col]
                payload_dict = statement_data.where(pd.notnull(statement_data), None).to_dict()
                if not payload_dict: continue

                item_data = {
                    'ticker': ticker_symbol,
                    'item_type': "CASH_FLOW_STATEMENT",
                    'item_time_coverage': "QUARTER",
                    'item_key_date': item_key_date_naive,
                    'item_source': "yfinance.Ticker.quarterly_cashflow",
                    'item_data_payload': payload_dict
                }
                inserted_id = await db_repo.insert_ticker_data_item(item_data)
                if inserted_id: items_inserted_count += 1
            except Exception as col_err:
                logger.error(f"[Quarterly CF Store] Error processing column '{str(date_col)}' for {ticker_symbol}: {col_err}", exc_info=True)
        logger.info(f"[Quarterly CF Store] Stored {items_inserted_count} quarterly cash flow statements for {ticker_symbol}.")
    except Exception as e:
        logger.error(f"[Quarterly CF Store] Error fetching/processing for {ticker_symbol}: {e}", exc_info=True)
    finally:
        logger.info(f"--- Fetch & Store FINISHED for Quarterly Cash Flow Statements: {ticker_symbol} ---")

async def fetch_and_store_ttm_cash_flow_statement(ticker_symbol: str, db_repo: YahooDataRepository):
    """Fetches the TTM cash flow statement from yfinance (.ttm_cashflow)
       and stores it as a single item in ticker_data_items using the upsert_single_ttm_statement method.
       item_type: CASH_FLOW_STATEMENT, item_time_coverage: TTM.
       item_key_date is derived from the name of the TTM Series/DataFrame column.
    """
    logger.info(f"--- Starting Fetch & Store for TTM Cash Flow Statement: {ticker_symbol} ---")
    try:
        # MODIFIED: Wrap yfinance calls in asyncio.to_thread
        def _sync_get_ttm_cf():
            yf_ticker = yf.Ticker(ticker_symbol)
            return yf_ticker.ttm_cashflow
        ttm_data_raw = await asyncio.to_thread(_sync_get_ttm_cf)
        # END MODIFICATION
        ttm_cf_series = None
        series_name_date_str = None

        if isinstance(ttm_data_raw, pd.DataFrame):
            if not ttm_data_raw.empty and len(ttm_data_raw.columns) > 0:
                ttm_cf_series = ttm_data_raw.iloc[:, 0]
                series_name_date_str = str(ttm_data_raw.columns[0])
                logger.info(f"[TTM CF Store] Processed as DataFrame. Extracted Series for column '{series_name_date_str}'.")
            else:
                logger.warning(f"[TTM CF Store] .ttm_cashflow for {ticker_symbol} is an empty DataFrame. Skipping.")
                return
        elif isinstance(ttm_data_raw, pd.Series):
            ttm_cf_series = ttm_data_raw
            series_name_date_str = str(ttm_cf_series.name)
            logger.info(f"[TTM CF Store] Processed as Series. Series name for date: '{series_name_date_str}'.")
        else:
            logger.warning(f"[TTM CF Store] .ttm_cashflow for {ticker_symbol} is not Series/DataFrame. Type: {type(ttm_data_raw)}. Skipping.")
            return
        
        if ttm_cf_series is None or ttm_cf_series.empty:
            logger.warning(f"[TTM CF Store] Resulting TTM series for {ticker_symbol} is None or empty. Skipping.")
            return

        item_key_date_naive = None
        if series_name_date_str:
            try:
                item_key_date_naive = pd.to_datetime(series_name_date_str).to_pydatetime().replace(tzinfo=None)
            except ValueError as ve:
                logger.error(f"[TTM CF Store] Could not parse date '{series_name_date_str}' for {ticker_symbol}: {ve}. Using current date.")
                item_key_date_naive = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            logger.warning(f"[TTM CF Store] TTM cash flow data for {ticker_symbol} has no date name. Using current date.")
            item_key_date_naive = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        payload_dict = ttm_cf_series.where(pd.notnull(ttm_cf_series), None).to_dict()
        if not payload_dict:
            logger.warning(f"[TTM CF Store] TTM Cash Flow for {ticker_symbol} is empty after to_dict. Skipping.")
            return

        item_data = {
            'ticker': ticker_symbol,
            'item_type': "CASH_FLOW_STATEMENT",
            'item_time_coverage': "TTM",
            'item_key_date': item_key_date_naive,
            'item_source': "yfinance.Ticker.ttm_cashflow",
            'item_data_payload': payload_dict
        }
        
        logger.debug(f"[TTM CF Store] Prepared item_data for {ticker_symbol}, key_date {item_key_date_naive.date()}.")
        inserted_id = await db_repo.upsert_single_ttm_statement(item_data)
        if inserted_id:
            logger.info(f"[TTM CF Store] Processed TTM cash flow for {ticker_symbol}, ID: {inserted_id}.")
        else:
            logger.info(f"[TTM CF Store] TTM cash flow for {ticker_symbol} (key_date {item_key_date_naive.date()}) likely already existed or error; no new record.")

    except Exception as e:
        logger.error(f"[TTM CF Store] Error fetching/processing for {ticker_symbol}: {e}", exc_info=True)
    finally:
        logger.info(f"--- Fetch & Store FINISHED for TTM Cash Flow Statement: {ticker_symbol} ---")

# --- End Cash Flow Statement Functions ---

# --- NEW: Function to fetch and store Dividend History ---
async def fetch_and_store_dividend_history(ticker_symbol: str, db_repo: YahooDataRepository):
    """Fetches dividend history using yfinance (.get_dividends())
       and stores it as a single cumulative record in ticker_data_items.
       item_type: DIVIDEND_HISTORY, item_time_coverage: CUMULATIVE.
       item_key_date is the date of the most recent dividend.
       Uses upsert_single_ttm_statement to manage the single record per ticker.
    """
    logger.info(f"--- Starting Fetch & Store for Dividend History: {ticker_symbol} ---")
    try:
        # MODIFIED: Wrap yfinance calls in asyncio.to_thread
        def _sync_get_dividends():
            yf_ticker = yf.Ticker(ticker_symbol)
            return yf_ticker.get_dividends()
        dividends_series = await asyncio.to_thread(_sync_get_dividends)
        # END MODIFICATION

        if not isinstance(dividends_series, pd.Series) or dividends_series.empty:
            logger.warning(f"[Dividend History Store] No dividend data found for {ticker_symbol}, or data is not a Series. Skipping.")
            # Potentially insert a record indicating no dividends if that's a desired state to capture
            # For now, just skipping.
            return

        # Determine item_key_date from the most recent dividend
        # yfinance Series index is typically DatetimeIndex
        if not isinstance(dividends_series.index, pd.DatetimeIndex):
            logger.error(f"[Dividend History Store] Dividend Series index for {ticker_symbol} is not a DatetimeIndex. Skipping.")
            return
            
        item_key_date_ts = dividends_series.index[-1] # Most recent date
        item_key_date_naive = item_key_date_ts.to_pydatetime().replace(tzinfo=None)

        # Prepare payload: dict of {date_str: amount}
        payload_dict = {}
        for date_val, amount in dividends_series.items():
            if isinstance(date_val, pd.Timestamp):
                payload_dict[date_val.strftime('%Y-%m-%d')] = amount
            else: # Should not happen if index is DatetimeIndex, but as a fallback
                payload_dict[str(date_val)] = amount
        
        if not payload_dict: # Should not happen if series was not empty
            logger.warning(f"[Dividend History Store] Dividend payload dict is empty for {ticker_symbol} after processing. Skipping.")
            return

        item_data = {
            'ticker': ticker_symbol,
            'item_type': "DIVIDEND_HISTORY",
            'item_time_coverage': "CUMULATIVE",
            'item_key_date': item_key_date_naive, # Date of the latest dividend
            'item_source': "yfinance.Ticker.get_dividends()",
            'item_data_payload': payload_dict # The entire history as a dict
        }

        logger.debug(f"[Dividend History Store] Prepared item_data for {ticker_symbol}, latest dividend date {item_key_date_naive.date()}.")
        
        # Use the TTM upsert logic to ensure only one record (the latest) per ticker/type/coverage exists
        inserted_id = await db_repo.upsert_single_ttm_statement(item_data) 
        
        if inserted_id:
            logger.info(f"[Dividend History Store] Successfully processed dividend history for {ticker_symbol} (inserted/updated), new/effective ID: {inserted_id}.")
        else:
            logger.info(f"[Dividend History Store] Dividend history for {ticker_symbol} (latest dividend {item_key_date_naive.date()}) likely already current or an error occurred; no new record inserted.")

    except Exception as e:
        logger.error(f"[Dividend History Store] Error fetching/processing dividend history for {ticker_symbol}: {e}", exc_info=True)
    finally:
        logger.info(f"--- Fetch & Store FINISHED for Dividend History: {ticker_symbol} ---")
# --- End Dividend History Function ---

# --- NEW: Function to fetch and store Earnings Estimate History ---
async def fetch_and_store_earnings_estimate_history(ticker_symbol: str, db_repo: YahooDataRepository):
    """Fetches earnings estimate history from yfinance (.earnings_history),
       merges it with existing stored history, and stores the combined record.
       item_type: EARNINGS_ESTIMATE_HISTORY, item_time_coverage: CUMULATIVE.
       item_key_date is the fetch timestamp. Uses upsert_single_ttm_statement.
    """
    logger.info(f"--- Starting Fetch & Store for Earnings Estimate History: {ticker_symbol} ---")
    current_fetch_time = datetime.now() # Used for item_key_date and to ensure consistency if called by other functions
    item_type_val = "EARNINGS_ESTIMATE_HISTORY"
    item_time_coverage_val = "CUMULATIVE"

    try:
        # MODIFIED: Wrap yfinance calls in asyncio.to_thread
        def _sync_get_earnings_history():
            yf_ticker = yf.Ticker(ticker_symbol)
            new_estimates_df_local = None
            try:
                data = yf_ticker.earnings_history
                if isinstance(data, pd.DataFrame):
                    new_estimates_df_local = data
                elif callable(data): # If it's a method like get_earnings_history()
                    logger.debug("[Earnings Est Store] .earnings_history is callable, attempting to call.")
                    new_estimates_df_local = data() # Call the method
            except Exception as e_fetch_local:
                logger.warning(f"[Earnings Est Store - Sync] Error trying to access/call .earnings_history for {ticker_symbol}: {e_fetch_local}. Trying get_earnings_history().")
                try:
                    new_estimates_df_local = yf_ticker.get_earnings_history()
                except Exception as e_get_fetch_local:
                    logger.error(f"[Earnings Est Store - Sync] Error calling .get_earnings_history() for {ticker_symbol}: {e_get_fetch_local}")
                    new_estimates_df_local = None
            return new_estimates_df_local

        new_estimates_df = await asyncio.to_thread(_sync_get_earnings_history)
        # END MODIFICATION

        if not isinstance(new_estimates_df, pd.DataFrame) or new_estimates_df.empty:
            logger.warning(f"[Earnings Est Store] No earnings estimate data found for {ticker_symbol}. Skipping.")
            return

        # Prepare newly fetched estimates
        newly_fetched_estimates_dict = {}
        for quarter_date, row in new_estimates_df.iterrows():
            if isinstance(quarter_date, pd.Timestamp) and 'epsEstimate' in row and pd.notna(row['epsEstimate']):
                newly_fetched_estimates_dict[quarter_date.strftime('%Y-%m-%d')] = row['epsEstimate']
            # else: Skip if date is not Timestamp or epsEstimate is missing/NaN

        if not newly_fetched_estimates_dict:
            logger.warning(f"[Earnings Est Store] No valid EPS estimates extracted from fetched data for {ticker_symbol}. Skipping.")
            return

        # Retrieve existing history
        existing_history_payload = await db_repo.get_latest_item_payload(
            ticker_symbol, item_type_val, item_time_coverage_val
        )
        
        merged_estimates_dict = existing_history_payload.copy() if existing_history_payload else {}
        # Update with new estimates, new ones overwrite old for the same quarter date
        merged_estimates_dict.update(newly_fetched_estimates_dict)

        # Check if the merged data is different from the existing stored data
        # This avoids a DB write if nothing effectively changed.
        if existing_history_payload == merged_estimates_dict:
            logger.info(f"[Earnings Est Store] Earnings estimate history for {ticker_symbol} is unchanged. No update needed.")
            return

        item_data = {
            'ticker': ticker_symbol,
            'item_type': item_type_val,
            'item_time_coverage': item_time_coverage_val,
            'item_key_date': current_fetch_time, # Current fetch time
            'item_source': "yfinance.Ticker.earnings_history/get_earnings_history()",
            'item_data_payload': merged_estimates_dict
        }

        logger.debug(f"[Earnings Est Store] Prepared merged earnings estimate data for {ticker_symbol}.")
        
        inserted_id = await db_repo.upsert_single_ttm_statement(item_data)
        
        if inserted_id:
            logger.info(f"[Earnings Est Store] Successfully processed earnings estimate history for {ticker_symbol}. Effective ID: {inserted_id}.")
        else:
            logger.info(f"[Earnings Est Store] Earnings estimate history for {ticker_symbol} processed; no new record ID returned (e.g., exact same fetch time record existed or error during upsert).")

    except Exception as e:
        logger.error(f"[Earnings Est Store] Error fetching/processing earnings estimate history for {ticker_symbol}: {e}", exc_info=True)
    finally:
        logger.info(f"--- Fetch & Store FINISHED for Earnings Estimate History: {ticker_symbol} ---")
# --- End Earnings Estimate History Function ---

# --- NEW: Function to Inspect Forecast Data (Temporary for Planning) ---
async def inspect_raw_forecast_data(ticker_symbol: str):
    """Fetches and prints raw data from get_eps_trend() and get_revenue_estimate() for inspection."""
    logger.info(f"--- Inspecting Raw Forecast Data for: {ticker_symbol} ---")
    print(f"\n--- Inspecting Raw Forecast Data for: {ticker_symbol} ---")
    
    try:
        yf_ticker = yf.Ticker(ticker_symbol)

        # --- EPS Trend Data ---
        print("\n\n--- EPS Trend Data (yf_ticker.get_eps_trend()) ---")
        eps_trend_df = None
        try:
            eps_trend_df = yf_ticker.get_eps_trend()
            if eps_trend_df is not None:
                print("\n* DataFrame Info (eps_trend_df.info()):")
                eps_trend_df.info()
                print("\n* DataFrame Head (eps_trend_df.head().to_string()):")
                print(eps_trend_df.head().to_string())
                print(f"\n* Index Name: {eps_trend_df.index.name}")
                print(f"* Index Values: {list(eps_trend_df.index)}")
                print(f"* Column Names: {list(eps_trend_df.columns)}")
            else:
                print("eps_trend_df is None.")
        except Exception as e_eps:
            print(f"Error fetching or processing get_eps_trend(): {e_eps}")
            logger.error(f"Error fetching get_eps_trend() for {ticker_symbol}: {e_eps}", exc_info=True)

        # --- Revenue Estimate Data ---
        print("\n\n--- Revenue Estimate Data (yf_ticker.get_revenue_estimate()) ---")
        revenue_estimate_df = None
        try:
            revenue_estimate_df = yf_ticker.get_revenue_estimate()
            if revenue_estimate_df is not None:
                print("\n* DataFrame Info (revenue_estimate_df.info()):")
                revenue_estimate_df.info()
                print("\n* DataFrame Head (revenue_estimate_df.head().to_string()):")
                print(revenue_estimate_df.head().to_string())
                print(f"\n* Index Name: {revenue_estimate_df.index.name}")
                print(f"* Index Values: {list(revenue_estimate_df.index)}")
                print(f"* Column Names: {list(revenue_estimate_df.columns)}")
            else:
                print("revenue_estimate_df is None.")
        except Exception as e_rev:
            print(f"Error fetching or processing get_revenue_estimate(): {e_rev}")
            logger.error(f"Error fetching get_revenue_estimate() for {ticker_symbol}: {e_rev}", exc_info=True)

    except Exception as e:
        print(f"An error occurred setting up Ticker object for {ticker_symbol}: {e}")
        logger.error(f"Error setting up Ticker for forecast inspection ({ticker_symbol}): {e}", exc_info=True)
    finally:
        print(f"\n--- Finished Inspecting Raw Forecast Data for: {ticker_symbol} ---")
        logger.info(f"--- Finished Inspecting Raw Forecast Data for: {ticker_symbol} ---")
# --- End Inspect Forecast Data Function ---

async def fetch_and_store_forecast_summary(ticker_symbol: str, db_repo: YahooDataRepository):
    """
    Fetches EPS trend and revenue estimate data, combines them into a single
    cumulative record, and stores it using upsert_single_ttm_statement.
    item_type="FORECAST_SUMMARY", item_time_coverage="CUMULATIVE".
    item_key_date is the fetch timestamp.
    Labels used for periods: '0q' -> 'current_qtr', '+1q' -> 'next_qtr',
                             '0y' -> 'current_fyear', '+1y' -> 'next_fyear'.
    """
    logger.info(f"--- Starting Fetch & Store for Forecast Summary: {ticker_symbol} ---")
    current_fetch_time = datetime.now()
    item_type_val = "FORECAST_SUMMARY"
    item_time_coverage_val = "CUMULATIVE"
    item_source_val = "yfinance.Ticker.get_eps_trend() & .get_revenue_estimate()"

    combined_payload = {}
    period_label_map = {
        '0q': 'current_qtr',
        '+1q': 'next_qtr',
        '0y': 'current_fyear',
        '+1y': 'next_fyear'
    }

    try:
        # MODIFIED: Wrap yfinance calls in asyncio.to_thread
        def _sync_get_forecast_data():
            yf_ticker_local = yf.Ticker(ticker_symbol)
            eps_trend_df_local = None
            revenue_estimate_df_local = None
            try:
                eps_trend_df_local = yf_ticker_local.get_eps_trend()
                if not isinstance(eps_trend_df_local, pd.DataFrame) or eps_trend_df_local.empty:
                    logger.warning(f"[Forecast Summary Store - Sync] EPS trend data for {ticker_symbol} is not a non-empty DataFrame. Setting to None.")
                    eps_trend_df_local = None
            except Exception as e_eps_local:
                logger.error(f"[Forecast Summary Store - Sync] Error fetching EPS trend for {ticker_symbol}: {e_eps_local}", exc_info=True)
                eps_trend_df_local = None
            
            try:
                revenue_estimate_df_local = yf_ticker_local.get_revenue_estimate()
                if not isinstance(revenue_estimate_df_local, pd.DataFrame) or revenue_estimate_df_local.empty:
                    logger.warning(f"[Forecast Summary Store - Sync] Revenue estimate data for {ticker_symbol} is not a non-empty DataFrame. Setting to None.")
                    revenue_estimate_df_local = None
            except Exception as e_rev_local:
                logger.error(f"[Forecast Summary Store - Sync] Error fetching revenue estimates for {ticker_symbol}: {e_rev_local}", exc_info=True)
                revenue_estimate_df_local = None
            return eps_trend_df_local, revenue_estimate_df_local

        eps_trend_df, revenue_estimate_df = await asyncio.to_thread(_sync_get_forecast_data)
        # END MODIFICATION

        all_periods = set()
        if eps_trend_df is not None:
            all_periods.update(eps_trend_df.index)
        if revenue_estimate_df is not None:
            all_periods.update(revenue_estimate_df.index)

        if not all_periods:
            logger.warning(f"[Forecast Summary Store] No data fetched for either EPS trend or revenue estimates for {ticker_symbol}. Skipping store.")
            return

        for period_orig_label in sorted(list(all_periods)): # Sort for consistent key order
            descriptive_label = period_label_map.get(period_orig_label, period_orig_label) 

            # Process EPS Trend data for this period
            if eps_trend_df is not None and period_orig_label in eps_trend_df.index:
                eps_row = eps_trend_df.loc[period_orig_label]
                for col_name, value in eps_row.items():
                    if pd.notna(value): 
                        safe_col_name = str(col_name).replace(' ', '_').replace('(', '').replace(')', '')
                        # Explicitly cast to float, as EPS trends are float64
                        combined_payload[f"{descriptive_label}_eps_{safe_col_name}"] = float(value)
            
            # Process Revenue Estimate data for this period (only 'avg' column)
            if revenue_estimate_df is not None and period_orig_label in revenue_estimate_df.index:
                if 'avg' in revenue_estimate_df.columns:
                    avg_revenue = revenue_estimate_df.loc[period_orig_label, 'avg']
                    if pd.notna(avg_revenue):
                        # Explicitly cast to int, as revenue avg is int64
                        combined_payload[f"{descriptive_label}_revenue_avg"] = int(avg_revenue)
                else:
                    logger.warning(f"[Forecast Summary Store] 'avg' column not found in revenue estimates for period {period_orig_label}, ticker {ticker_symbol}.")

        if not combined_payload:
            logger.warning(f"[Forecast Summary Store] Combined payload for {ticker_symbol} is empty. Skipping store.")
            return

        existing_payload = await db_repo.get_latest_item_payload(
            ticker_symbol, item_type_val, item_time_coverage_val
        )
        if existing_payload == combined_payload:
            logger.info(f"[Forecast Summary Store] Forecast summary data for {ticker_symbol} is unchanged. No update needed.")
            return

        item_data = {
            'ticker': ticker_symbol,
            'item_type': item_type_val,
            'item_time_coverage': item_time_coverage_val,
            'item_key_date': current_fetch_time,
            'item_source': item_source_val,
            'item_data_payload': combined_payload
        }

        logger.debug(f"[Forecast Summary Store] Prepared item_data for {ticker_symbol}. Payload keys (first 5): {list(combined_payload.keys())[:5]}...")
        
        inserted_id = await db_repo.upsert_single_ttm_statement(item_data)
        
        if inserted_id:
            logger.info(f"[Forecast Summary Store] Successfully processed Forecast Summary for {ticker_symbol}. Effective ID: {inserted_id}.")
        else:
            logger.info(f"[Forecast Summary Store] Forecast Summary for {ticker_symbol} processed. Check logs for DB operation details.")

    except Exception as e:
        logger.error(f"[Forecast Summary Store] General error fetching/processing forecast summary for {ticker_symbol}: {e}", exc_info=True)
    finally:
        logger.info(f"--- Fetch & Store FINISHED for Forecast Summary: {ticker_symbol} ---")

# --- ATR Calculation Functions ---

def _calculate_true_range_on_df(historical_data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the True Range (TR) and adds it as a 'TR' column to the DataFrame.
    The input DataFrame must have 'High', 'Low', 'Close' columns.
    Note: The first row of 'TR' will be NaN due to the 'Previous Close' shift.
    """
    if not all(col in historical_data.columns for col in ['High', 'Low', 'Close']):
        logger.error("[ATR_CALC] DataFrame missing required columns: High, Low, or Close.")
        # Add an empty TR column or raise error, for now, let it proceed, subsequent ops will fail.
        historical_data['TR'] = pd.NA 
        return historical_data

    df = historical_data.copy()
    df['Previous Close'] = df['Close'].shift(1)
    
    df['H-L'] = df['High'] - df['Low']
    df['H-PC'] = abs(df['High'] - df['Previous Close'])
    df['L-PC'] = abs(df['Low'] - df['Previous Close'])
    
    # Ensure numeric types before max, handle potential NAs from shift
    tr_components = df[['H-L', 'H-PC', 'L-PC']].apply(pd.to_numeric, errors='coerce')
    df['TR'] = tr_components.max(axis=1)
    
    # Clean up temporary columns
    df.drop(columns=['H-L', 'H-PC', 'L-PC', 'Previous Close'], inplace=True, errors='ignore')
    return df

def calculate_atr_smoothed(tr_values: pd.Series, period: int = 14) -> Optional[float]:
    """
    Calculates the Average True Range (ATR) using Wilder's smoothing method.
    """
    if not isinstance(tr_values, pd.Series):
        logger.error(f"[ATR_CALC] Smoothed ATR: tr_values is not a Series. Type: {type(tr_values)}")
        return None
    if tr_values.empty or period <= 0:
        logger.warning(f"[ATR_CALC] Smoothed ATR: TR series is empty or period is invalid (period: {period}).")
        return None
    if len(tr_values) < period:
        logger.warning(f"[ATR_CALC] Smoothed ATR: Not enough TR values ({len(tr_values)}) for ATR period {period}.")
        return None

    atr = pd.Series(index=tr_values.index, dtype='float64', name='ATR_Smoothed')
    # Initial ATR: Simple average of the first 'period' TRs
    atr.iloc[period - 1] = tr_values.iloc[:period].mean()
    
    # Subsequent ATRs
    for i in range(period, len(tr_values)):
        atr.iloc[i] = (atr.iloc[i - 1] * (period - 1) + tr_values.iloc[i]) / period
        
    return atr.iloc[-1] if not atr.empty and pd.notna(atr.iloc[-1]) else None

def calculate_atr_simple_average(tr_values: pd.Series, period: int = 14) -> Optional[float]:
    """
    Calculates the Average True Range (ATR) as a simple moving average of TR values.
    """
    if not isinstance(tr_values, pd.Series):
        logger.error(f"[ATR_CALC] Simple ATR: tr_values is not a Series. Type: {type(tr_values)}")
        return None
    if tr_values.empty or period <= 0:
        logger.warning(f"[ATR_CALC] Simple ATR: TR series is empty or period is invalid (period: {period}).")
        return None
    if len(tr_values) < period:
        logger.warning(f"[ATR_CALC] Simple ATR: Not enough TR values ({len(tr_values)}) for ATR period {period}.")
        return None
        
    last_n_trs = tr_values.iloc[-period:] # Get the last 'period' TR values
    return last_n_trs.mean() if not last_n_trs.empty else None

async def get_latest_atr( # RENAMED and SIMPLIFIED from fetch_and_calculate_atr_for_ticker
    ticker_symbol: str, 
    atr_period: int = 14
) -> Optional[float]:
    """
    Fetches the latest historical data for a ticker, calculates True Range, 
    and then returns the latest smoothed Average True Range (ATR) value.
    Internally fetches a default number of calendar days (e.g., 35) to ensure sufficient data.
    """
    internal_calendar_days_to_fetch = 35 # Default days to fetch, ensures ~22-25 trading days
    if atr_period > 14: # If ATR period is larger, fetch more days
        internal_calendar_days_to_fetch = int(atr_period * 2.5) # Heuristic: 2.5x period in calendar days
        internal_calendar_days_to_fetch = max(internal_calendar_days_to_fetch, 35) # Ensure a minimum fetch

    logger.info(f"[ATR_GET] Calculating ATR({atr_period}) for {ticker_symbol}, fetching last {internal_calendar_days_to_fetch} calendar days.")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=internal_calendar_days_to_fetch)
    
    historical_df = await fetch_daily_historical_data(ticker_symbol, start_date, end_date)

    if historical_df is None or historical_df.empty:
        logger.warning(f"[ATR_GET] Failed to fetch historical data or no data returned for {ticker_symbol} for ATR({atr_period}).")
        return None

    if len(historical_df) < atr_period + 1: # Need at least period + 1 days for first TR calculation
        logger.warning(f"[ATR_GET] Insufficient trading days ({len(historical_df)}) fetched for {ticker_symbol} to calculate ATR({atr_period}). Needs at least {atr_period + 1} days.")
        return None
        
    logger.debug(f"[ATR_GET] Fetched {len(historical_df)} trading days for {ticker_symbol} for ATR({atr_period}). Last date: {historical_df.index[-1].strftime('%Y-%m-%d')}")

    # Calculate TR
    df_with_tr = _calculate_true_range_on_df(historical_df)
    tr_series = df_with_tr['TR'].dropna() # Drop first NaN from shift
    
    if len(tr_series) < atr_period:
        logger.warning(f"[ATR_GET] Not enough TR values ({len(tr_series)}) after calculation for {ticker_symbol} to calculate ATR({atr_period}). Needs at least {atr_period} TR values.")
        return None

    logger.debug(f"[ATR_GET] Calculated {len(tr_series)} TR values for {ticker_symbol} for ATR({atr_period}).")

    # Calculate Smoothed ATR
    atr_value = calculate_atr_smoothed(tr_series, atr_period)
    
    if atr_value is not None:
        logger.info(f"[ATR_GET] Calculated ATR({atr_period}) for {ticker_symbol}: {atr_value:.4f}")
    else:
        logger.warning(f"[ATR_GET] Smoothed ATR calculation returned None for {ticker_symbol}, period {atr_period}.")
        
    return atr_value

# --- End ATR Calculation Functions ---

async def mass_load_yahoo_data_from_file(ticker_source, db_repo, progress_callback=None):
    """Reads tickers from a file or list and processes them for full data loading.
    Optionally calls progress_callback(current, total, last_ticker) after each ticker."""
    logger.info(f"--- Starting Mass Load ---")
    processed_count = 0
    error_count = 0
    tickers_with_errors = []

    # Get tickers from either file or list
    if isinstance(ticker_source, str):
        try:
            with open(ticker_source, 'r', encoding='utf-8') as f:
                tickers = [line.strip() for line in f if line.strip()]
        except Exception as e:
            logger.error(f"[Mass Load] Error reading ticker file {ticker_source}: {e}")
            return
    elif isinstance(ticker_source, list):
        tickers = [t.strip().upper() for t in ticker_source if t and isinstance(t, str)]
    else:
        logger.error(f"[Mass Load] Invalid ticker_source type: {type(ticker_source)}")
        return

    total = len(tickers)
    for idx, ticker_symbol in enumerate(tickers, 1):
        logger.info(f"[Mass Load] >>> Processing ticker: {ticker_symbol} <<<")
        ticker_master_data_found = False # Tracks if the core ticker info was found
        ticker_had_critical_error = False # Tracks if a top-level exception occurred for the ticker

        try:
            # --- Step B: Master Ticker Update ---
            logger.info(f"[Mass Load][{ticker_symbol}] Updating Ticker Master record...")
            master_data = await fetch_and_process_yahoo_info(ticker_symbol)
            
            if master_data:
                ticker_master_data_found = True
                await db_repo.upsert_yahoo_ticker_master(master_data)
                logger.info(f"[Mass Load][{ticker_symbol}] Ticker Master record updated/inserted.")

                # --- Step C: Ticker Data Items Update (only if master_data was found) ---
                logger.info(f"[Mass Load][{ticker_symbol}] Updating Ticker Data Items...")
                data_item_fetch_functions = {
                    "AnalystPriceTargets": fetch_and_upsert_analyst_targets_summary,
                    "AnnualBalanceSheets": fetch_and_store_annual_balance_sheets,
                    "QuarterlyBalanceSheets": fetch_and_store_quarterly_balance_sheets,
                    "AnnualIncomeStatements": fetch_and_store_annual_income_statements,
                    "QuarterlyIncomeStatements": fetch_and_store_quarterly_income_statements,
                    "TTMIncomeStatement": fetch_and_store_ttm_income_statement,
                    "AnnualCashFlow": fetch_and_store_annual_cash_flow_statements,
                    "QuarterlyCashFlow": fetch_and_store_quarterly_cash_flow_statements,
                    "TTMCashFlow": fetch_and_store_ttm_cash_flow_statement,
                    "DividendHistory": fetch_and_store_dividend_history,
                    "EarningsEstimateHistory": fetch_and_store_earnings_estimate_history,
                    "ForecastSummary": fetch_and_store_forecast_summary
                }

                for item_name, fetch_func in data_item_fetch_functions.items():
                    try:
                        logger.info(f"[Mass Load][{ticker_symbol}] Fetching/Storing {item_name}...")
                        await fetch_func(ticker_symbol, db_repo)
                        logger.info(f"[Mass Load][{ticker_symbol}] {item_name} processed.")
                    except Exception as e_item: # Individual item fetch error
                        logger.error(f"[Mass Load][{ticker_symbol}] Error processing {item_name}: {e_item}", exc_info=False)
                        # This does NOT set ticker_master_data_found to False or ticker_had_critical_error to True
            else:
                # master_data is None
                logger.warning(f"[Mass Load][{ticker_symbol}] Failed to fetch master data. Ticker considered not found.")
                # ticker_master_data_found remains False

            # Determine final status for counting and logging for this ticker
            if ticker_master_data_found:
                processed_count += 1
                logger.info(f"[Mass Load] <<< Finished processing for ticker: {ticker_symbol} - MASTER DATA FOUND >>>")
            else:
                # This branch is hit if master_data was None
                error_count += 1
                tickers_with_errors.append(ticker_symbol)
                logger.warning(f"[Mass Load] <<< Finished processing for ticker: {ticker_symbol} - MASTER DATA NOT FOUND OR FAILED >>>")

        except Exception as e_ticker: # This is for truly unexpected critical errors for the whole ticker
            logger.error(f"[Mass Load] UNEXPECTED CRITICAL ERROR processing ticker {ticker_symbol}: {e_ticker}", exc_info=True)
            ticker_had_critical_error = True
            if not ticker_master_data_found: # If master data wasn't found AND a critical error occurred
                if ticker_symbol not in tickers_with_errors: # Add to errors if not already there due to master data fail
                    error_count +=1 # Increment error if it wasn't already from master_data fail path
                    tickers_with_errors.append(ticker_symbol)
            else: # Master data was found, but then a critical error happened
                # This ticker was likely counted in processed_count already if error is late.
                # We need to re-classify it as an error.
                processed_count -=1 # Decrement if it was optimistically counted
                error_count += 1
                if ticker_symbol not in tickers_with_errors:
                    tickers_with_errors.append(ticker_symbol)
        
        # Determine the status to pass to the callback
        # Callback's 'had_error' should be true if master data wasn't found OR a critical error occurred.
        callback_had_error_status = not ticker_master_data_found or ticker_had_critical_error

        # --- Progress callback ---
        if progress_callback:
            await progress_callback(idx, total, ticker_symbol, callback_had_error_status)
            await asyncio.sleep(0.01)  # Explicit additional yield after each ticker
    logger.info(f"--- Mass Load from File FINISHED ---")
    logger.info(f"Tickers processed (master data found): {processed_count}")
    logger.info(f"Tickers with errors (master data not found or critical error): {error_count}")
    if tickers_with_errors:
        logger.warning(f"Tickers that encountered errors: {tickers_with_errors}")
    return {
        'errors': tickers_with_errors,
        'success_count': processed_count,
        'error_count': error_count
    }

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Fetch Yahoo Finance data, update DB, or calculate ATR.", 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--ticker", 
        required=True,
        help="Stock ticker symbol (e.g., AAPL, MSFT)."
    )
    parser.add_argument(
        "--update-mode",
        default="full",
        choices=[
            'full', 'fast', 
            'upsert_analyst_summary', 
            'store_annual_bs', 'store_quarterly_bs', 
            'store_annual_is', 'store_quarterly_is', 'store_ttm_is',
            'store_annual_cf', 'store_quarterly_cf', 'store_ttm_cf',
            'store_dividend_history',
            'store_earnings_estimates',
            'inspect_forecast_data', # ADDED mode for inspection
            'store_forecast_summary', # New mode
            'view_filtered_profiles', # New mode for testing profile filtering
            'view_data_items', # New mode for testing get_data_items
            'mass_load_from_file', # New mode for mass loading
            'test_fetch_historical', 'calculate_atr'
        ], 
        help="Specify the operation: e.g., 'full', 'mass_load_from_file', 'view_data_items', etc." # Simplified help
    )
    parser.add_argument(
        "--ticker-file",
        help="Path to a file containing ticker symbols (one per line) for mass_load_from_file mode."
    )
    parser.add_argument(
        "--start-date",
        help="Start date for historical data fetch (YYYY-MM-DD). Required if --update-mode is 'test_fetch_historical'."
    )
    parser.add_argument(
        "--end-date",
        help="End date for historical data fetch (YYYY-MM-DD). Required if --update-mode is 'test_fetch_historical'."
    )
    # --- Arguments for view_data_items mode ---
    parser.add_argument(
        "--item-type",
        help="Item type for view_data_items mode (e.g., BALANCE_SHEET, INCOME_STATEMENT)."
    )
    parser.add_argument(
        "--item-coverage",
        help="Item time coverage for view_data_items mode (e.g., FYEAR, QUARTER, TTM)."
    )
    parser.add_argument(
        "--start-date-query",
        help="Start key date for view_data_items query (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--end-date-query",
        help="End key date for view_data_items query (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--limit-query",
        type=int,
        help="Limit number of results for view_data_items query."
    )
    # --- End arguments for view_data_items ---

    parser.add_argument(
        "--atr-period",
        type=int,
        default=14,
        help="Period for ATR calculation (e.g., 14). Used with --update-mode calculate_atr."
    )
    parser.add_argument(
        "--log-level", 
        default="INFO", 
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], 
        help="Set the logging level."
    )

    args = parser.parse_args()

    # --- Logging Setup (No change) ---
    try:
        requested_log_level_value = getattr(logging, args.log_level.upper())
        logging.basicConfig(level=requested_log_level_value, 
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
                            force=True) 
        logger.setLevel(requested_log_level_value) 
        logger.info(f"Logging level set to: {args.log_level}")
    except AttributeError:
        logger.error(f"Invalid log level specified: {args.log_level}. Using default INFO.")
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
        logger.setLevel(logging.INFO)
    # --- End Logging Setup ---
    
    # --- Define DB Path --- 
    db_file_path_relative = "src/V3_app/V3_database.db" # Hardcoded relative path
    # Resolve to absolute path based on CWD (assuming script run from workspace root)
    workspace_root = os.getcwd()
    db_file_path_absolute = os.path.join(workspace_root, db_file_path_relative)
    logger.debug(f"Resolved DB path to absolute: {db_file_path_absolute}")
    db_url = f"sqlite+aiosqlite:///{db_file_path_absolute}" # Ensure aiosqlite for async
    logger.info(f"Using database URL: {db_url}") # Log the final URL being used
    # --- End DB Path --- 
    
    # --- Instantiate Repository --- 
    # db_repo = SQLiteRepository(database_url=db_url) # Changed
    db_repo = YahooDataRepository(database_url=db_url) # Changed to YahooDataRepository
    # --- End Instantiate --- 

    # --- COMMENT OUT Session Instantiation --- 
    # # Define cache file path relative to workspace root
    # cache_file_path_relative = "cache/yfinance.cache"
    # cache_file_path_absolute = os.path.join(workspace_root, cache_file_path_relative)
    # cache_dir = os.path.dirname(cache_file_path_absolute)
    # os.makedirs(cache_dir, exist_ok=True) # Ensure cache directory exists
    # logger.info(f"Using cache file at: {cache_file_path_absolute}")
    # 
    # # # REVERT TO CachedLimiterSession --- 
    # # yahoo_session = CachedLimiterSession(
    # #     limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # Restore rate limiter
    # #     bucket_class=MemoryQueueBucket,
    # #     backend=SQLiteCache(cache_file_path_absolute), # Use SQLite backend
    # # )
    # # # Optionally set user agent
    # # # yahoo_session.headers['User-agent'] = 'financial-app-v3/1.0'
    # # # logger.info("Initialized CachedLimiterSession for yfinance.") # UPDATED Log message
    # # # --- End Session Instantiation --- 

    # --- Main Async Task Definition --- 
    async def main_task(cli_args: argparse.Namespace): # MODIFIED to accept cli_args
        logger.info(f"Starting main task for ticker '{cli_args.ticker}' with mode '{cli_args.update_mode}'")
        try:
            # Ensure DB tables exist (common for modes needing DB)
            if cli_args.update_mode in [
                'full', 'fast', 
                'upsert_analyst_summary', 
                'store_annual_bs', 'store_quarterly_bs',
                'store_annual_is', 'store_quarterly_is', 'store_ttm_is',
                'store_annual_cf', 'store_quarterly_cf', 'store_ttm_cf',
                'store_dividend_history',
                'store_earnings_estimates',
                'inspect_forecast_data', # ADDED mode for inspection
                'store_forecast_summary', # New mode
                'view_filtered_profiles', # New mode for testing profile filtering
                'view_data_items', # New mode for testing get_data_items
                'mass_load_from_file' # <<< ADD THIS LINE TO DB CHECK MODES
            ]: 
                logger.info("Ensuring database tables exist...")
                await db_repo.create_tables()
                logger.info("Database tables checked/created.")
            # Removed 'else' simulation mode log to avoid confusion with test_fetch_historical

            if cli_args.update_mode == 'full':
                # Fetch and process data
                processed_data = await fetch_and_process_yahoo_info(cli_args.ticker) # REMOVE session

                if processed_data:
                    # Write to file (only for full update)
                    try:
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, write_processed_data_to_file, processed_data)
                    except Exception as file_write_err:
                        logger.error(f"Error occurred during file writing: {file_write_err}", exc_info=True)
                        # Log error but continue with DB update

                    # Update database (full upsert)
                    await db_repo.upsert_yahoo_ticker_master(processed_data)
                    # Success message logged inside upsert method

                    # REMOVED call to update market data - full mode does full update
                else:
                    logger.error(f"Failed to fetch or process data for {cli_args.ticker} in 'full' mode. No DB update or file write performed.")
            
            elif cli_args.update_mode == 'fast':
                # Only update specific market data
                await update_ticker_master_market_data(cli_args.ticker, db_repo) # REMOVE session
                # Success/failure message logged inside function
            
            # --- UPDATED/NEW MODE ---    
            elif cli_args.update_mode == 'upsert_analyst_summary':
                logger.info(f"Running Analyst Price Targets Summary Upsert for: {cli_args.ticker}")
                await fetch_and_upsert_analyst_targets_summary(cli_args.ticker, db_repo)
            
            elif cli_args.update_mode == 'store_annual_bs': # ADDED new mode block
                logger.info(f"Running Annual Balance Sheet Storage for: {cli_args.ticker}")
                await fetch_and_store_annual_balance_sheets(cli_args.ticker, db_repo)
            
            elif cli_args.update_mode == 'store_quarterly_bs': # ADDED new mode block
                logger.info(f"Running Quarterly Balance Sheet Storage for: {cli_args.ticker}")
                await fetch_and_store_quarterly_balance_sheets(cli_args.ticker, db_repo)
            
            elif cli_args.update_mode == 'store_annual_is':
                logger.info(f"Running Annual Income Statement Storage for: {cli_args.ticker}")
                await fetch_and_store_annual_income_statements(cli_args.ticker, db_repo)
            
            elif cli_args.update_mode == 'store_quarterly_is':
                logger.info(f"Running Quarterly Income Statement Storage for: {cli_args.ticker}")
                await fetch_and_store_quarterly_income_statements(cli_args.ticker, db_repo)

            elif cli_args.update_mode == 'store_ttm_is':
                logger.info(f"Running TTM Income Statement Storage for: {cli_args.ticker}")
                await fetch_and_store_ttm_income_statement(cli_args.ticker, db_repo)
            
            elif cli_args.update_mode == 'store_annual_cf':
                logger.info(f"Running Annual Cash Flow Statement Storage for: {cli_args.ticker}")
                await fetch_and_store_annual_cash_flow_statements(cli_args.ticker, db_repo)
            
            elif cli_args.update_mode == 'store_quarterly_cf':
                logger.info(f"Running Quarterly Cash Flow Statement Storage for: {cli_args.ticker}")
                await fetch_and_store_quarterly_cash_flow_statements(cli_args.ticker, db_repo)

            elif cli_args.update_mode == 'store_ttm_cf':
                logger.info(f"Running TTM Cash Flow Statement Storage for: {cli_args.ticker}")
                await fetch_and_store_ttm_cash_flow_statement(cli_args.ticker, db_repo)
            
            elif cli_args.update_mode == 'store_dividend_history':
                logger.info(f"Running Dividend History Storage for: {cli_args.ticker}")
                await fetch_and_store_dividend_history(cli_args.ticker, db_repo)
            
            elif cli_args.update_mode == 'store_earnings_estimates':
                logger.info(f"Running Earnings Estimate History Storage for: {cli_args.ticker}")
                await fetch_and_store_earnings_estimate_history(cli_args.ticker, db_repo)
            
            elif cli_args.update_mode == 'inspect_forecast_data':
                logger.info(f"Running Raw Forecast Data Inspection for: {cli_args.ticker}")
                await inspect_raw_forecast_data(cli_args.ticker)

            elif cli_args.update_mode == 'store_forecast_summary':
                logger.info(f"Running Forecast Summary Storage for: {cli_args.ticker}")
                await fetch_and_store_forecast_summary(cli_args.ticker, db_repo)

            elif cli_args.update_mode == 'view_filtered_profiles': # <<< START NEW ELIF BLOCK
                logger.info(f"Running View Filtered Profiles (ticker arg '{cli_args.ticker}' is used if no filters are specified or for context, otherwise filters dictate)")
                query_service = YahooDataQueryService(db_repo)
                
                # --- Define your test filters here ---
                # Example 1: Get all profiles (empty filter) - Useful for seeing all data
                # test_filters = {}
                
                # Example 2: Filter by a specific ticker if you want to test that pathway with filters still in place
                # test_filters = {'ticker': cli_args.ticker} 

                # Example 3: Filter by sector (adjust to a sector you have in your DB)
                # test_filters = {'sector': 'Technology'}

                # Example 4: Filter by country and exchange (adjust to data in your DB for this to work)
                test_filters = {'country': 'Greece', 'exchange': 'ATH'} 
                # To get all tickers if the above filter is too restrictive, uncomment Example 1 and comment this out.

                logger.info(f"Applying filters: {test_filters}")
                profiles = await query_service.get_multiple_ticker_profiles(filters=test_filters)
                
                if profiles:
                    print(f"\n--- Found {len(profiles)} Ticker Profiles Matching Filters ---")
                    # Import pprint if not already at the top of V3_yahoo_fetch.py
                    # import pprint 
                    for profile_idx, profile_data in enumerate(profiles):
                        print(f"\n--- Profile {profile_idx + 1} ---")
                        pprint.pprint(profile_data, indent=2) # Assumes pprint is imported
                else:
                    print("\n--- No Ticker Profiles Found Matching Filters ---")
                print("-----------------------------------------------------\n") # <<< END NEW ELIF BLOCK
            
            elif cli_args.update_mode == 'view_data_items':
                logger.info(f"Running View Data Items for ticker: {cli_args.ticker}")
                query_service = YahooDataQueryService(db_repo)

                if not cli_args.item_type:
                    logger.error("For 'view_data_items' mode, --item-type is required.")
                    print("\nERROR: --item-type is required for this mode (e.g., BALANCE_SHEET, INCOME_STATEMENT).\n")
                    return

                start_dt_query = None
                if cli_args.start_date_query:
                    try:
                        start_dt_query = datetime.strptime(cli_args.start_date_query, "%Y-%m-%d")
                    except ValueError:
                        logger.error("Invalid format for --start-date-query. Use YYYY-MM-DD.")
                        print("\nERROR: Invalid format for --start-date-query. Use YYYY-MM-DD.\n")
                        return
                
                end_dt_query = None
                if cli_args.end_date_query:
                    try:
                        end_dt_query = datetime.strptime(cli_args.end_date_query, "%Y-%m-%d")
                    except ValueError:
                        logger.error("Invalid format for --end-date-query. Use YYYY-MM-DD.")
                        print("\nERROR: Invalid format for --end-date-query. Use YYYY-MM-DD.\n")
                        return

                logger.info(f"Querying for item_type: {cli_args.item_type}, coverage: {cli_args.item_coverage}, start: {cli_args.start_date_query}, end: {cli_args.end_date_query}, limit: {cli_args.limit_query}")
                
                data_items = await query_service.get_data_items(
                    ticker=cli_args.ticker,
                    item_type=cli_args.item_type.upper(), # Standardize to upper
                    item_time_coverage=cli_args.item_coverage.upper() if cli_args.item_coverage else None, # Standardize
                    start_date=start_dt_query,
                    end_date=end_dt_query,
                    limit=cli_args.limit_query,
                    order_by_key_date_desc=True # Default to show latest first
                )

                if data_items:
                    print(f"\n--- Found {len(data_items)} Data Items ---")
                    for item_idx, item_data in enumerate(data_items):
                        print(f"\n--- Item {item_idx + 1} (ID: {item_data.get('data_item_id')}, KeyDate: {item_data.get('item_key_date')}) ---")
                        pprint.pprint(item_data, indent=2) 
                else:
                    print("\n--- No Data Items Found Matching Criteria ---")
                print("-----------------------------------------------\n")

            elif cli_args.update_mode == 'mass_load_from_file':
                logger.info(f"Running Mass Load from File.")
                if not cli_args.ticker_file:
                    logger.error("For 'mass_load_from_file' mode, --ticker-file argument is required.")
                    print("\nERROR: --ticker-file <path_to_file> is required for this mode.\n")
                    return
                
                # The ticker argument from parser is not directly used here, 
                # but it's still required by argparse setup. Mass load uses tickers from the file.
                await mass_load_yahoo_data_from_file(cli_args.ticker_file, db_repo)

            elif cli_args.update_mode == 'test_fetch_historical':
                logger.info(f"Running Historical Data Fetch Test for: {cli_args.ticker}")
                if not cli_args.start_date or not cli_args.end_date:
                    logger.error("For 'test_fetch_historical' mode, --start-date and --end-date are required.")
                    print("\nERROR: --start-date and --end-date are required for this mode. Use YYYY-MM-DD format.\n")
                    return

                try:
                    start_dt = datetime.strptime(cli_args.start_date, "%Y-%m-%d")
                    end_dt = datetime.strptime(cli_args.end_date, "%Y-%m-%d")
                except ValueError:
                    logger.error("Invalid date format for --start-date or --end-date. Please use YYYY-MM-DD.")
                    print("\nERROR: Invalid date format. Please use YYYY-MM-DD for --start-date and --end-date.\n")
                    return
                
                # Default interval is "1d" in the function itself
                historical_df = await fetch_daily_historical_data(cli_args.ticker, start_dt, end_dt)

                if historical_df is not None and not historical_df.empty:
                    print("\n--- Fetched Historical Data ---")
                    print(f"Ticker: {cli_args.ticker}, From: {cli_args.start_date}, To: {cli_args.end_date}")
                    print(historical_df.to_string())
                    print("-----------------------------\n")
                elif historical_df is not None and historical_df.empty: # DataFrame exists but is empty
                    print("\n--- Fetched Historical Data ---")
                    print(f"Ticker: {cli_args.ticker}, From: {cli_args.start_date}, To: {cli_args.end_date}")
                    print("No data found for the given ticker and date range (DataFrame is empty).")
                    print("Make sure the dates are correct and the market was open (e.g., avoid fetching for a future end_date without data).")
                    print("-----------------------------\n")
                else: # Function returned None (likely an error during fetch)
                    print("\n--- Fetched Historical Data ---")
                    print(f"Ticker: {cli_args.ticker}, From: {cli_args.start_date}, To: {cli_args.end_date}")
                    print("Failed to fetch historical data or no data available. Check logs for details.")
                    print("-----------------------------\n")
            
            elif cli_args.update_mode == 'calculate_atr':
                logger.info(f"Running ATR Calculation for: {cli_args.ticker}, Period: {cli_args.atr_period}")
                latest_atr_value = await get_latest_atr( # CALLING THE NEW FUNCTION
                    cli_args.ticker, 
                    cli_args.atr_period
                )
                print("\n--- ATR Calculation Result ---")
                print(f"  Ticker: {cli_args.ticker}")
                print(f"  ATR Period: {cli_args.atr_period}")
                if latest_atr_value is not None:
                    print(f"  Latest ATR ({cli_args.atr_period}): {latest_atr_value:.4f}") # Format to 4 decimal places
                else:
                    print(f"  Latest ATR ({cli_args.atr_period}): Not available (calculation failed or insufficient data)")
                print("-----------------------------\n")
            # --- END UPDATED/NEW MODE ---

        except Exception as e:
            logger.error(f"An error occurred in the main task for {cli_args.ticker} (mode: {cli_args.update_mode}): {e}", exc_info=True)
        finally:
            logger.info(f"Main task finished for ticker '{cli_args.ticker}' (mode: {cli_args.update_mode})")
    # --- End Main Async Task --- 

    # --- Run Main Task --- 
    asyncio.run(main_task(args)) # MODIFIED to pass args
    # --- End Run --- 
