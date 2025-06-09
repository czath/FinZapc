import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable
import asyncio
import os
import httpx

from .V3_database import SQLiteRepository
from .V3_models import TickerListPayload
from .services.notification_service import dispatch_notification

# Configure logging - SET TO DEBUG
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def convert_to_numeric(value):
    """Convert string values from Finviz to appropriate numeric types."""
    if not isinstance(value, str):
        return value
        
    # Remove any commas and spaces
    value = value.replace(',', '').strip()
    
    # Handle percentages
    if value.endswith('%'):
        try:
            return float(value.rstrip('%')) / 100
        except ValueError:
            return value
            
    # Handle billions
    if value.endswith('B'):
        try:
            return float(value.rstrip('B')) * 1e9
        except ValueError:
            return value
            
    # Handle millions
    if value.endswith('M'):
        try:
            return float(value.rstrip('M')) * 1e6
        except ValueError:
            return value
            
    # Handle thousands
    if value.endswith('K'):
        try:
            return float(value.rstrip('K')) * 1e3
        except ValueError:
            return value
    
    # Handle '-' as None
    if value == '-':
        return None
        
    # Try converting to float
    try:
        return float(value)
    except ValueError:
        pass
    
    # Try converting to int
    try:
        return int(value)
    except ValueError:
        pass
    
    # Return original value if no conversion possible
    return value

async def get_stock_data(symbol: str) -> Optional[Dict[str, Any]]:
    """Fetches and parses the snapshot table data for a given stock symbol from Finviz."""
    # Create list of symbol variations if dot is present
    symbol_variations = [symbol]
    if '.' in symbol:
        symbol_variations.append(symbol.replace('.', '-'))
        # Finviz often uses dashes for dots (e.g., BRK.B -> BRK-B)
        # symbol_variations.append(symbol.replace('.', '/ ')) # Less common
    
    for sym in symbol_variations:
        # URL for Finviz stock page
        url = f"https://finviz.com/quote.ashx?t={sym}"
        
        # Headers to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        try:
            logger.debug(f"Attempting to fetch data for {symbol} using variation {sym} from {url}")
            # MODIFIED: Wrap requests.get in asyncio.to_thread
            response = await asyncio.to_thread(requests.get, url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the snapshot table (Finviz uses class snapshot-table2)
            snapshot_table = soup.find('table', class_='snapshot-table2')
            if not snapshot_table:
                logger.debug(f"Snapshot table not found for {symbol} using variation {sym}. Trying next variation.")
                continue # Try next symbol variation if table not found
                
            # Initialize dictionary to store all fields
            data = {
                # Add fetch time for reference?
                 '_fetch_timestamp': datetime.now() 
            }
            
            # Extract all fields from the snapshot table
            rows = snapshot_table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                # Process cells in pairs (label, value)
                for i in range(0, len(cells), 2):
                    if i + 1 < len(cells):
                        field_name = cells[i].text.strip()
                        field_value_raw = cells[i+1].text.strip()
                        # Convert numerical values before storing
                        field_value_converted = convert_to_numeric(field_value_raw)
                        data[field_name] = field_value_converted
                        # logger.debug(f"  {field_name}: {field_value_raw} -> {field_value_converted}") # Very verbose
                    else:
                         logger.warning(f"Odd number of cells found in a row for {symbol} ({sym})")

            # --- Add Sector/Industry parsing --- 
            try:
                # Find the div containing the links (adjust selector if needed)
                links_div = soup.find('div', class_='quote-links')
                logger.debug(f"Found links_div: {links_div is not None}") # Back to DEBUG
                if links_div:
                    # Find all relevant anchor tags within the specific structure
                    inner_div = links_div.find('div', class_=lambda x: x and 'flex' in x.split())
                    logger.debug(f"Found inner_div: {inner_div is not None}") # Back to DEBUG
                    if inner_div:
                        links = inner_div.find_all('a', class_='tab-link')
                        logger.debug(f"Found {len(links)} tab-links inside inner_div.") # Back to DEBUG
                        if len(links) >= 2:
                            sector = links[0].text.strip()
                            industry = links[1].text.strip()
                            logger.debug(f"  Extracted Sector: '{sector}', Industry: '{industry}'") # Back to DEBUG
                            data['Sector'] = sector
                            data['Industry'] = industry
                        else:
                             logger.warning(f"Could not find enough links for Sector/Industry for {symbol} ({sym})")
                    else:
                         logger.warning(f"Could not find inner flex div for Sector/Industry links for {symbol} ({sym})")
                else:
                    logger.warning(f"Could not find quote-links div for Sector/Industry for {symbol} ({sym})")
            except Exception as link_err:
                 logger.error(f"Error parsing Sector/Industry links for {symbol} ({sym}): {link_err}", exc_info=False) # Keep log level reasonable
            # --- End Sector/Industry parsing ---

            # --- Add Company Name parsing ---
            try:
                # Find the header div
                header_div = soup.find('div', class_='quote-header_ticker-wrapper')
                if header_div:
                    # Find the h2 within the header div
                    h2_tag = header_div.find('h2', class_='quote-header_ticker-wrapper_company')
                    if h2_tag:
                        # Find the anchor tag within the h2
                        a_tag = h2_tag.find('a', class_='tab-link')
                        if a_tag:
                            company_name = a_tag.text.strip()
                            data['Name'] = company_name
                            logger.debug(f"  Extracted Name: '{company_name}'")
                        else:
                            logger.warning(f"Could not find company name link (a.tab-link) for {symbol} ({sym})")
                    else:
                        logger.warning(f"Could not find company name header (h2) for {symbol} ({sym})")
                else:
                    logger.warning(f"Could not find quote-header div for company name for {symbol} ({sym})")
            except Exception as name_err:
                logger.error(f"Error parsing Company Name for {symbol} ({sym}): {name_err}", exc_info=False)
            # --- End Company Name parsing ---

            logger.info(f"Successfully processed data for {symbol} (using {sym})")
            return data # Return data if found
                
        except requests.exceptions.HTTPError as http_err:
            # Handle specific HTTP errors like 404 Not Found more gracefully
            if response.status_code == 404:
                logger.debug(f"HTTP 404 Not Found for {symbol} using variation {sym}. Trying next variation.")
            else:
                logger.warning(f"HTTP error fetching data for {symbol} ({sym}): {http_err}")
            continue # Try next variation
        except requests.exceptions.RequestException as req_err:
             logger.warning(f"Request error fetching data for {symbol} ({sym}): {req_err}")
             continue # Try next variation
        except Exception as e:
            logger.error(f"General error parsing data for {symbol} ({sym}): {e}", exc_info=True) # Log traceback for unexpected errors
            continue # Try next variation
    
    # If loop completes without returning data
    logger.warning(f"No data found for {symbol} after trying variations: {', '.join(symbol_variations)}")
    return None

# --- Function to Update Screener Table --- 
def parse_raw_data(raw_data_str: str) -> Dict[str, Any]:
    """Parses the comma-delimited raw_data string into a dictionary."""
    data_dict = {}
    if not raw_data_str:
        return data_dict
    try:
        items = raw_data_str.split(',')
        for item in items:
            parts = item.split('=', 1) # Split only on the first '=' 
            if len(parts) == 2:
                key, value = parts
                # Convert numeric values back if possible (using the same helper)
                data_dict[key.strip()] = convert_to_numeric(value.strip())
            else:
                # Handle cases where there might be no '=' or empty values
                key = parts[0].strip()
                if key: # Avoid storing empty keys
                    data_dict[key] = None 
    except Exception as e:
        logger.error(f"Error parsing raw_data string: '{raw_data_str}'. Error: {e}")
    return data_dict

# --- NEW Helper to Serialize Raw Data ---
def serialize_raw_data(data: Dict[str, Any]) -> str:
    """Serializes a dictionary into a comma-delimited key=value string."""
    if not data:
        return ""
    
    # Filter out internal keys like _fetch_timestamp before serializing
    filtered_data = {k: v for k, v in data.items() if not k.startswith('_')}
    
    items = []
    for key, value in filtered_data.items():
        # Ensure both key and value are strings; handle None values gracefully
        key_str = str(key).strip()
        value_str = str(value).strip() if value is not None else '' 
        # Avoid empty keys and potentially problematic characters in keys/values if needed
        if key_str:
            items.append(f"{key_str}={value_str}")
            
    return ','.join(items)
# --- END NEW Helper ---

async def update_screener_from_finviz(repository):
    logger.info("Starting to update screener table from latest Finviz raw data...")
    try:
        # Fetch all raw data and all existing screener data at the beginning
        all_finviz_data = await repository.get_all_finviz_raw_data()
        all_screener_data = await repository.get_all_screened_tickers()
        
        # Create a lookup map for efficient access to screener data
        screener_data_map = {item['ticker']: item for item in all_screener_data}

        updated_count = 0
        skipped_count = 0
        for raw_entry in all_finviz_data:
            ticker = raw_entry.get('ticker')
            raw_data_str = raw_entry.get('raw_data')

            if not ticker or not raw_data_str:
                continue

            parsed_data = parse_raw_data(raw_data_str)

            # --- START: Company Name Sanity Check ---
            # Use the in-memory map instead of a new DB call
            screener_item = screener_data_map.get(ticker)
            if not screener_item:
                logger.warning(f"Finviz update: Ticker '{ticker}' found in finviz_raw but not in screener table. Skipping.")
                continue

            existing_company_name = screener_item.get('Company')
            new_company_name_from_finviz = parsed_data.get('Name')

            if existing_company_name and new_company_name_from_finviz:
                # Normalize by lowercasing and taking the first word. This avoids issues with "Inc." vs "Inc" etc.
                # but is strong enough to catch "Hellenic" vs "H2O".
                existing_key_part = existing_company_name.split(' ')[0].lower()
                new_key_part = new_company_name_from_finviz.split(' ')[0].lower()
                
                if existing_key_part != new_key_part:
                    logger.warning(
                        f"Finviz update SKIPPED for ticker '{ticker}' due to potential company mismatch. "
                        f"DB name starts with '{existing_key_part}', Finviz name with '{new_key_part}'."
                    )
                    skipped_count += 1
                    continue
            # --- END: Company Name Sanity Check ---
            
            updates = {}
            if 'Company' in parsed_data and parsed_data['Company']:
                updates['Company'] = parsed_data['Company']
            if 'Sector' in parsed_data and parsed_data['Sector']:
                updates['sector'] = parsed_data['Sector']
            if 'Industry' in parsed_data and parsed_data['Industry']:
                updates['industry'] = parsed_data['Industry']

            if 'Beta' in parsed_data and parsed_data['Beta']:
                numeric_beta = convert_to_numeric(parsed_data['Beta'])
                if isinstance(numeric_beta, (int, float)):
                    updates['beta'] = numeric_beta

            if 'ATR (14)' in parsed_data and parsed_data['ATR (14)']:
                numeric_atr = convert_to_numeric(parsed_data['ATR (14)'])
                if isinstance(numeric_atr, (int, float)):
                    updates['atr'] = numeric_atr

            if 'Price' in parsed_data and parsed_data['Price']:
                numeric_price = convert_to_numeric(parsed_data['Price'])
                if isinstance(numeric_price, (int, float)):
                    updates['price'] = numeric_price

            if updates:
                for field, value in updates.items():
                    await repository.update_screener_ticker_details(ticker, field, value)
                updated_count += 1
        
        summary_message = f"Finviz update to screener complete. Updated: {updated_count} tickers, Skipped: {skipped_count} due to mismatch."
        logger.info(summary_message)
        await dispatch_notification(db_repo=repository, task_id='scheduled_finviz_update_from_raw', message=summary_message)

    except Exception as e:
        logger.error(f"Error updating screener from Finviz raw data: {e}", exc_info=True)
        await dispatch_notification(db_repo=repository, task_id='scheduled_finviz_update_from_raw', message=f"Finviz update to screener failed: {e}")

# Fetch and store data for a list of tickers (used by analytics pipe)
async def fetch_and_store_finviz_data(repository): # Keep existing for scheduled job compatibility for now
    """Fetches Finviz data for all tickers in the screener and stores it."""
    logger.info("Starting Finviz data fetch for all screener tickers...")
    try:
        screener_tickers_dicts = await repository.get_all_screened_tickers()
        if not screener_tickers_dicts:
            logger.warning("No tickers found in the screener. Skipping Finviz data fetch.")
            return {"success_count": 0, "failed_count": 0, "errors": [], "message": "No tickers in screener."}

        tickers = [item['ticker'] for item in screener_tickers_dicts if item.get('ticker')]
        if not tickers:
            logger.warning("Ticker list extracted from screener is empty.")
            return {"success_count": 0, "failed_count": 0, "errors": [], "message": "Extracted ticker list is empty."}

        logger.info(f"Fetching Finviz data for {len(tickers)} tickers from screener.")
        
        successful_count = 0
        failed_count = 0
        errors_list = []

        for ticker in tickers:
            stock_data = await get_stock_data(ticker)
            if stock_data:
                raw_data_str = serialize_raw_data(stock_data)
                try:
                    await repository.save_or_update_finviz_raw_data(ticker=ticker, raw_data=raw_data_str)
                    successful_count += 1
                    logger.debug(f"Successfully fetched and stored Finviz data for {ticker}")
                except Exception as e_save:
                    logger.error(f"Error saving Finviz data for {ticker} to finviz_raw: {e_save}")
                    failed_count += 1
                    errors_list.append({"ticker": ticker, "error": str(e_save)})
            else:
                logger.warning(f"Failed to fetch Finviz data for {ticker}")
                failed_count += 1
                errors_list.append({"ticker": ticker, "error": "Failed to fetch data from Finviz"})
        
        total_processed = successful_count + failed_count
        summary_msg = f"Finviz data fetch for screener completed. Fetched: {successful_count}/{total_processed}. Errors: {failed_count}."
        logger.info(summary_msg)
        return {
            "success_count": successful_count, 
            "failed_count": failed_count, 
            "errors": errors_list,
            "message": summary_msg
        }
    except Exception as e:
        logger.error(f"Error in fetch_and_store_finviz_data: {e}", exc_info=True)
        return {
            "success_count": 0, 
            "failed_count": 0, # Assume all failed if error here
            "errors": [{"ticker": "GENERAL", "error": str(e)}],
            "message": f"General error in Finviz data fetch: {e}"
        }

async def fetch_and_store_analytics_finviz(
    repository, 
    tickers: List[str], 
    progress_callback: Optional[Callable] = None
):
    """
    Fetches Finviz data for a given list of tickers and stores it in analytics_raw.
    Includes progress callback and detailed return status.
    """
    if not tickers:
        logger.warning("fetch_and_store_analytics_finviz called with an empty ticker list.")
        return {"success_count": 0, "failed_count": 0, "errors": [], "message": "No tickers provided."}

    total_tickers = len(tickers)
    logger.info(f"Starting Finviz analytics data fetch for {total_tickers} tickers.")
    
    successful_count = 0
    failed_count = 0
    errors_list = []

    for idx, ticker in enumerate(tickers):
        ticker_had_errors = False
        error_message_for_ticker = "Unknown error"
        try:
            logger.debug(f"Processing ticker {idx+1}/{total_tickers}: {ticker}")
            stock_data = await get_stock_data(ticker)
            if stock_data:
                raw_data_str = serialize_raw_data(stock_data)
                try:
                    await repository.save_or_update_analytics_raw_data(ticker, 'finviz', raw_data_str)
                    successful_count += 1
                    logger.debug(f"Successfully fetched and stored Finviz data for {ticker} (analytics).")
                except Exception as e_save:
                    logger.error(f"Error saving Finviz data for {ticker} to analytics_raw: {e_save}")
                    failed_count += 1
                    errors_list.append({"ticker": ticker, "error": str(e_save)})
                    ticker_had_errors = True
                    error_message_for_ticker = str(e_save)
            else:
                logger.warning(f"Failed to fetch Finviz data for {ticker} (analytics).")
                failed_count += 1
                errors_list.append({"ticker": ticker, "error": "Failed to fetch data from Finviz"})
                ticker_had_errors = True
                error_message_for_ticker = "Failed to fetch data from Finviz"
        
        except Exception as e_outer:
            logger.error(f"Outer loop exception processing ticker {ticker}: {e_outer}", exc_info=True)
            failed_count += 1
            errors_list.append({"ticker": ticker, "error": str(e_outer)})
            ticker_had_errors = True
            error_message_for_ticker = str(e_outer)

        if progress_callback:
            try:
                await progress_callback(
                    current_idx=idx + 1,
                    total_items=total_tickers,
                    last_ticker_processed=ticker,
                    ticker_had_errors=ticker_had_errors
                )
            except Exception as cb_exc:
                logger.error(f"Error in progress_callback for ticker {ticker}: {cb_exc}", exc_info=True)

    summary_msg = f"Finviz analytics data fetch completed. Processed: {total_tickers}, Successful: {successful_count}, Errors: {failed_count}."
    if errors_list:
        summary_msg += f" First error on: {errors_list[0]['ticker']} - {errors_list[0]['error'][:50]}..."

    logger.info(summary_msg)
    
    return {
        "success_count": successful_count,
        "failed_count": failed_count,
        "errors": errors_list,
        "message": summary_msg
    }

# For testing this module directly
if __name__ == '__main__':
    # This is a basic test setup. You'll need to ensure your DB path is correct
    # and that the SQLiteRepository class is accessible.
    
    # --- Determine the correct path to the database ---
    # Assuming this script is in src/V3_app and the DB is in src/V3_app/database/
    # Adjust if your structure is different.
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    db_relative_path = os.path.join('..', '..', 'database', 'V3_app_main.db') # Path from src/V3_app to database/
    # db_absolute_path = os.path.abspath(os.path.join(current_script_dir, db_relative_path))
    
    # --- MORE ROBUST PATH: Assume DB is in the same directory as this script for simplicity in test ---
    # --- OR, one level up in a 'database' folder if this script is in 'src/V3_app' ---
    # For example, if V3_finviz_fetch.py is in src/V3_app/
    # and V3_app_main.db is in src/database/
    # db_path = os.path.join(os.path.dirname(current_script_dir), 'database', 'V3_app_main.db') # Two levels up then into database
    
    # Let's simplify and assume the DB is directly in a 'database' subdir relative to 'src'
    # and this script is in 'src/V3_app'
    # So, from 'src/V3_app' go up to 'src', then into 'database'
    
    # --- Safest relative path assuming script is in src/V3_app and DB in src/database/V3_app_main.db ---
    db_path_from_src_V3_app = os.path.join(current_script_dir, '..', 'database', 'V3_app_main.db')
    
    # If you run this script directly from the workspace root (e.g. /c%3A/Users/zoumb/financial-app/)
    # then the path might be 'src/database/V3_app_main.db'
    # This needs to be robust. The get_db_path from V3_web.py is more reliable.
    # For this standalone test, we might need to hardcode or use an env var.
    
    # Let's assume we are running from workspace root: financial-app/
    # and the script is financial-app/src/V3_app/V3_finviz_fetch.py
    # and DB is financial-app/src/database/V3_app_main.db
    
    # Simplification for test: Assume DB is where it's expected for the app
    # This will likely fail if run standalone without the app's full context/PYTHONPATH.
    # The test below requires SQLiteRepository to be importable.
    
    # To make this test runnable standalone more easily:
    # 1. Add `src` to PYTHONPATH: `export PYTHONPATH=$PYTHONPATH:/path/to/your/financial-app/src`
    # 2. Or, modify sys.path within the script (less ideal for production code)
    import sys
    # Assuming the script is in financial-app/src/V3_app/
    # We want to add financial-app/src/ to sys.path
    project_src_dir = os.path.abspath(os.path.join(current_script_dir, '..'))
    if project_src_dir not in sys.path:
        sys.path.insert(0, project_src_dir)
        
    # Now try importing (this might still fail depending on your exact structure)
    try:
        from V3_app.V3_database import SQLiteRepository # Assuming SQLiteRepository is in V3_database.py within V3_app
    except ImportError:
        print("Failed to import SQLiteRepository. Ensure PYTHONPATH is set correctly or adjust import paths.")
        print(f"Current sys.path: {sys.path}")
        print(f"Attempted to add project_src_dir: {project_src_dir}")
        sys.exit(1)

    # Construct DB URL for SQLiteRepository
    # Assuming db_path_from_src_V3_app is the correct relative path from script location
    db_file_path = os.path.abspath(db_path_from_src_V3_app)
    DATABASE_URL = f"sqlite+aiosqlite:///{db_file_path}"
    print(f"Using Database URL: {DATABASE_URL}")


    async def main_test():
        # Point to your actual database file
        # repository = SQLiteRepository("sqlite+aiosqlite:///../database/V3_app_main.db") # Example path
        repository = SQLiteRepository(DATABASE_URL) # Use constructed URL

        # Test 1: Fetch data for a single known ticker (if get_stock_data is to be tested standalone)
        # symbol_to_test = "AAPL" 
        # print(f"--- Testing get_stock_data for {symbol_to_test} ---")
        # data = await get_stock_data(symbol_to_test)
        # if data:
        #     print(f"Data for {symbol_to_test}:")
        #     for key, value in data.items():
        #         print(f"  {key}: {value}")
        #     # Test serialization
        #     raw_str = serialize_raw_data(data)
        #     print(f"  Serialized: {raw_str[:100]}...") # Print first 100 chars
        #     # Test parsing
        #     parsed_back = parse_raw_data(raw_str)
        #     print(f"  Parsed back (first 3 items): {list(parsed_back.items())[:3]}")
        # else:
        #     print(f"No data found for {symbol_to_test}")

        # Test 2: Fetch and store data for a list of tickers (analytics pipe)
        # test_tickers = ["MSFT", "GOOGL", "NONEXISTENTTICKER"] 
        test_tickers = ["MSFT", "GOOGL", "TSLA", "NVDA", "AMZN", "BRK-A", "BRK-B", "META", "UNH", "XOM"]
        print(f"\n--- Testing fetch_and_store_analytics_finviz for tickers: {test_tickers} ---")
        
        async def sample_progress_callback(current_idx, total_items, last_ticker_processed, ticker_had_errors):
            print(f"[Progress CB] {current_idx}/{total_items} - Ticker: {last_ticker_processed}, Error: {ticker_had_errors}")

        results = await fetch_and_store_analytics_finviz(repository, test_tickers, progress_callback=sample_progress_callback)
        print("Results of fetch_and_store_analytics_finviz:")
        print(f"  Message: {results.get('message')}")
        print(f"  Success Count: {results.get('success_count')}")
        print(f"  Failed Count: {results.get('failed_count')}")
        if results.get('errors'):
            print("  Errors:")
            for err in results.get('errors')[:3]: # Print first 3 errors
                print(f"    - Ticker: {err['ticker']}, Error: {err['error']}")
            if len(results.get('errors')) > 3:
                print(f"    ... and {len(results.get('errors')) - 3} more errors.")
        
        # Test 3: Update screener from raw data (optional, depends on data being in finviz_raw)
        # print("\n--- Testing update_screener_from_finviz ---")
        # await update_screener_from_finviz(repository) # This requires data in finviz_raw table

        # Test 4: Fetch and store data for all screener tickers (original scheduled job logic)
        # print("\n--- Testing fetch_and_store_finviz_data (for all screener) ---")
        # all_screener_results = await fetch_and_store_finviz_data(repository)
        # print("Results of fetch_and_store_finviz_data (all screener):")
        # print(f"  Message: {all_screener_results.get('message')}")
        # print(f"  Success Count: {all_screener_results.get('success_count')}")
        # print(f"  Failed Count: {all_screener_results.get('failed_count')}")
        # if all_screener_results.get('errors'):
        #     print("  Errors:")
        #     for err in all_screener_results.get('errors')[:3]: # Print first 3 errors
        #         print(f"    - Ticker: {err['ticker']}, Error: {err['error']}")
        #     if len(all_screener_results.get('errors')) > 3:
        #         print(f"    ... and {len(all_screener_results.get('errors')) - 3} more errors.")


    asyncio.run(main_test()) 