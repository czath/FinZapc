"""
Module for fetching and processing stock data from Yahoo Finance.
"""

import requests
import cloudscraper # Consider using this if standard requests are blocked
from bs4 import BeautifulSoup
import logging
from datetime import datetime, date
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

# --- Custom Imports for this module ---
from .V3_database import SQLiteRepository # For DB operations

# Configure logging - SET TO DEBUG initially for development
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

async def fetch_and_process_yahoo_info(ticker_symbol: str) -> Optional[Dict[str, Any]]:
    """Fetches data from yfinance .info, processes/normalizes it, and returns a dictionary ready for DB/file.
       Returns None if fetching or essential processing fails.
    """
    logger.info(f"Fetching and processing Yahoo .info for {ticker_symbol}")
    try:
        yf_ticker = yf.Ticker(ticker_symbol)
        # Add basic retry or timeout logic here if needed, yfinance can hang
        info_data = yf_ticker.info
        
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Fetch Yahoo Finance .info data, update the master DB, and save processed data to a text file.", # Updated description
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--ticker", 
        required=True,
        help="Stock ticker symbol (e.g., AAPL, MSFT)."
    )
    # REMOVED --task argument
    # REMOVED --output-file argument
    # REMOVED --db-path argument (will use hardcoded default)
    parser.add_argument(
        "--log-level", 
        default="INFO", 
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], 
        help="Set the logging level."
    )
    # REMOVED --retries argument
    # REMOVED --initial-retry-delay argument
    # REMOVED --max-retry-delay argument

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
    db_repo = SQLiteRepository(database_url=db_url)
    # --- End Instantiate --- 

    # --- Main Async Task Definition --- 
    async def main_task(ticker: str):
        logger.info(f"Starting main task for ticker '{ticker}'")
        try:
            # Ensure DB tables exist
            logger.info("Ensuring database tables exist...")
            await db_repo.create_tables()
            logger.info("Database tables checked/created.")

            # Fetch and process data
            processed_data = await fetch_and_process_yahoo_info(ticker)

            if processed_data:
                # Write to file (using sync function for simplicity now)
                try:
                    # Run sync write in executor to avoid blocking async loop if it were complex
                    # For simple writes, direct call might be okay, but executor is safer practice
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, write_processed_data_to_file, processed_data)
                except Exception as file_write_err:
                    logger.error(f"Error occurred during file writing: {file_write_err}", exc_info=True)
                    # Decide if we should still try to update DB?
                    # Let's proceed to DB update even if file write fails for now.

                # Update database
                await db_repo.upsert_yahoo_ticker_master(processed_data)
                # Success message logged inside upsert method
            else:
                logger.error(f"Failed to fetch or process data for {ticker}. No DB update or file write performed.")

        except Exception as e:
            logger.error(f"An error occurred in the main task for {ticker}: {e}", exc_info=True)
        finally:
            logger.info(f"Main task finished for ticker '{ticker}'")
    # --- End Main Async Task --- 

    # --- Run Main Task --- 
    asyncio.run(main_task(args.ticker))
    # --- End Run --- 
