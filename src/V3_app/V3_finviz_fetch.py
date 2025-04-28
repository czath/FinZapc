import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
import asyncio
import os

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

def get_stock_data(symbol: str) -> Optional[Dict[str, Any]]:
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
            response = requests.get(url, headers=headers, timeout=10) # Added timeout
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
    """Updates the screener table with data fetched from finviz_raw."""
    logger.info("Starting screener update process from Finviz raw data...")
    try:
        # 1. Get all raw data
        all_raw_data = await repository.get_all_finviz_raw_data()
        if not all_raw_data:
            logger.warning("No data found in finviz_raw table. Skipping screener update.")
            return
        
        update_count = 0
        error_count = 0
        # 2. Loop through each record, parse, and update screener
        for record in all_raw_data:
            ticker = record.get('ticker')
            raw_data_str = record.get('raw_data')
            
            if not ticker or not raw_data_str:
                logger.warning(f"Skipping record due to missing ticker or raw_data: {record}")
                continue
                
            # 3. Parse the raw data string
            parsed_data = parse_raw_data(raw_data_str)
            if not parsed_data:
                logger.warning(f"Could not parse raw data for ticker {ticker}. Skipping update.")
                continue
                
            # 4. Prepare updates for the screener table
            updates_for_screener = {}
            
            # Get values from the parsed Finviz data
            # Ensure keys exist before accessing
            sector = parsed_data.get('Sector')
            industry = parsed_data.get('Industry')
            beta = parsed_data.get('Beta')
            atr = parsed_data.get('ATR (14)')
            company_name = parsed_data.get('Name') 

            # LOGGING: Check the value and type of ATR before the check
            logger.info(f"[Finviz Update Info] Checking ATR for {ticker}: Value='{atr}', Type={type(atr)}")

            # Add to updates dict only if the value is not None
            if sector is not None:
                updates_for_screener['sector'] = sector
            if industry is not None:
                updates_for_screener['industry'] = industry
            if beta is not None:
                # Check if beta is a valid float/int before adding
                if isinstance(beta, (float, int)):
                     updates_for_screener['beta'] = beta
                else:
                    logger.warning(f"[Finviz Update] Invalid Beta '{beta}' (type: {type(beta)}) found for {ticker}. Skipping Beta update.")
            if isinstance(atr, (float, int)) and atr > 0: # Check if it's a positive number
                updates_for_screener['atr'] = atr
                logger.debug(f"[Finviz Update] Adding valid ATR {atr} for {ticker} to updates.")
            elif atr is None: # Explicitly handle None (Finviz displayed '-')
                updates_for_screener['atr'] = None # Add None to potentially clear DB field
                logger.info(f"[Finviz Update] ATR for {ticker} is None (from Finviz ''). Adding None to updates.")
            if company_name is not None:
                updates_for_screener['Company'] = company_name # Use 'Company' key for DB
            
            # Check if there are any updates to apply
            if updates_for_screener:
                logger.info(f"[Finviz Update] Applying updates for {ticker}: {updates_for_screener}")
                # --- Corrected Call: Loop through updates ---
                update_errors = 0
                for field, value in updates_for_screener.items():
                    try:
                        # Call the DB method for each field-value pair
                        await repository.update_screener_ticker_details(ticker, field, value)
                    except ValueError as ve: # Catch errors like "Ticker not found" or "Invalid value"
                        logger.error(f"[Finviz Update] Error updating {field}={value} for {ticker}: {ve}")
                        update_errors += 1
                        # Decide if we should break or continue on error
                        # break # Stop updating this ticker on first error
                        # continue # Continue with next field for this ticker
                    except Exception as e:
                        logger.error(f"[Finviz Update] Unexpected error updating {field}={value} for {ticker}: {e}", exc_info=True)
                        update_errors += 1
                        # break # Stop updating this ticker
                        # continue # Continue with next field
                        
                if update_errors == 0:
                    logger.info(f"[Finviz Update] Successfully applied all updates for {ticker}.")
                    update_count += 1
                else:
                    logger.warning(f"[Finviz Update] Encountered {update_errors} errors while applying updates for {ticker}.")
                    error_count += 1
                # --- End Corrected Call ---
            else:
                logger.info(f"[Finviz Update] No relevant updates found in Finviz data for {ticker}.")

        logger.info("Screener update process finished.")
        logger.info(f"Screener records updated: {update_count}, Errors/Skipped: {error_count}")

    except Exception as e:
        logger.error(f"An error occurred during the screener update process: {e}", exc_info=True)

# --- Finviz Fetch Service (Code before this function needs adjustment) ---
async def fetch_and_store_finviz_data(repository):
    """ 
    Fetches Finviz data for all tickers in the screener table and stores 
    the raw data in the finviz_raw table.
    
    Args:
        repository: An instance of the SQLiteRepository.
    """
    logger.info("Starting Finviz data fetch process...")
    try:
        # 0. Clear the existing finviz_raw table data
        logger.info("Clearing existing Finviz raw data...")
        await repository.clear_finviz_raw_data()
        
        # 1. Get tickers from the screener table
        logger.info("Fetching tickers from screener table...")
        screened_tickers_data = await repository.get_all_screened_tickers() 
        if not screened_tickers_data:
            logger.warning("No tickers found in the screener table. Aborting Finviz fetch.")
            return
        
        # Create a list of tickers and a mapping for t_source1 lookup
        tickers = []
        ticker_to_source1 = {}
        for item in screened_tickers_data:
            ticker = item.get('ticker')
            if ticker:
                tickers.append(ticker)
                ticker_to_source1[ticker] = item.get('t_source1') # Store t_source1, might be None
                
        logger.info(f"Found {len(tickers)} tickers to process.")

        processed_count = 0
        failed_count = 0
        # 2. Loop through tickers, fetch data, and store
        for ticker in tickers:
            logger.info(f"Processing ticker: {ticker} ({processed_count + failed_count + 1}/{len(tickers)})")
            # 3. Fetch data using get_stock_data with the primary ticker
            finviz_data = get_stock_data(ticker)
            
            # 3a. If fetch failed, try using t_source1 as alternative symbol
            if not finviz_data:
                alternative_symbol = ticker_to_source1.get(ticker)
                if alternative_symbol and alternative_symbol.strip():
                    logger.info(f"Primary ticker {ticker} failed, trying alternative symbol: {alternative_symbol}")
                    finviz_data = get_stock_data(alternative_symbol)
                    if finviz_data:
                        logger.info(f"Successfully fetched data for {ticker} using alternative symbol {alternative_symbol}")
                    else:
                        logger.warning(f"Failed to fetch Finviz data for {ticker} using both primary and alternative symbol {alternative_symbol}.")
                # else: # No need for else, just proceeds if finviz_data is still None
                #    logger.warning(f"Failed to fetch Finviz data for {ticker} (no alternative symbol available).")
            
            # 4. Store raw data if fetch was successful (using either symbol)
            if finviz_data:
                try:
                    # Serialize the data first
                    raw_data_str = serialize_raw_data(finviz_data)
                    # IMPORTANT: Always save using the original ticker from the screener table
                    # Call the NEW repository method
                    await repository.save_or_update_finviz_raw_data(ticker, raw_data_str) 
                    processed_count += 1
                except Exception as db_err:
                    logger.error(f"Database error saving Finviz data for {ticker}: {db_err}", exc_info=True)
                    failed_count += 1
            else:
                # This log now correctly reflects failure after trying both primary and alternative
                logger.warning(f"Failed to fetch Finviz data for {ticker}. Skipping database save.") 
                failed_count += 1
            
            # Optional: Add a small delay to avoid hammering Finviz too hard
            # await asyncio.sleep(0.5) # Example: 0.5 second delay

        logger.info(f"Finviz data fetch process completed.")
        logger.info(f"Successfully processed: {processed_count}, Failed: {failed_count}")

    except Exception as e:
        logger.error(f"An error occurred during the Finviz fetch process: {e}", exc_info=True)

# --- NEW Function for Analytics Finviz Fetch --- 
async def fetch_and_store_analytics_finviz(repository, tickers: List[str]):
    """
    Fetches Finviz data for a given list of tickers and stores it 
    in the analytics_raw table.

    Args:
        repository: An instance of the SQLiteRepository.
        tickers: A list of ticker symbols to process.
        
    Returns:
        A dictionary containing the status and a summary message. 
        Example: {"status": "completed", "message": "Processed 10/12 tickers."}
    """
    logger.info(f"[Analytics Fetch] Starting Finviz data fetch for analytics_raw table for {len(tickers)} tickers.")
    if not tickers:
        logger.warning("[Analytics Fetch] No tickers provided for Finviz analytics fetch. Skipping.")
        return {"status": "completed", "message": "No tickers provided."} # Return status dict

    total_tickers = len(tickers)
    processed_count = 0
    failed_count = 0
    source_name = "finviz" # Define source explicitly

    for ticker in tickers:
        logger.info(f"[Analytics Fetch] Processing {ticker} ({processed_count + failed_count + 1}/{total_tickers}) for {source_name} analytics.")
        try:
            # 1. Fetch data using existing function
            # Note: get_stock_data handles ticker variations (e.g., BRK.B vs BRK-B)
            finviz_data_dict = get_stock_data(ticker)

            if finviz_data_dict:
                # 2. Serialize the fetched data dictionary
                raw_data_str = serialize_raw_data(finviz_data_dict)
                
                # 3. Store in the analytics_raw table
                await repository.save_or_update_analytics_raw_data(ticker, source_name, raw_data_str)
                processed_count += 1
            else:
                # Log failure if get_stock_data returned None after trying variations
                logger.warning(f"[Analytics Fetch] Failed to fetch Finviz data for {ticker} (for analytics). Skipping database save.")
                failed_count += 1
                # Optionally: Save a record indicating failure?
                # await repository.save_or_update_analytics_raw_data(ticker, source_name, "FETCH_FAILED")

            # Optional small delay
            # await asyncio.sleep(0.1) 

        except Exception as e:
            logger.error(f"[Analytics Fetch] Error processing ticker {ticker} for {source_name} analytics: {e}", exc_info=True)
            failed_count += 1

    logger.info(f"[Analytics Fetch] {source_name.capitalize()} data fetch for analytics completed.")
    logger.info(f"[Analytics Fetch] Successfully processed: {processed_count}, Failed: {failed_count}")
    
    # Construct final message and status
    final_status = "completed" if failed_count == 0 else "partial_failure"
    if processed_count == 0 and failed_count > 0:
        final_status = "failed"
        
    final_message = f"Processed {processed_count}/{total_tickers} tickers. Failures: {failed_count}."
    
    return {"status": final_status, "message": final_message} # Return status dict
# --- END NEW Function --- 

if __name__ == '__main__':
    # Import the repository (adjust path if necessary based on your structure)
    # Assuming V3_database.py is in the same directory or accessible via PYTHONPATH
    try:
        from V3_database import SQLiteRepository
    except ImportError:
        # Handle case where the script might be run from a different context
        import sys
        # Assuming V3_database is in the same parent directory as the script's dir
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sys.path.append(os.path.dirname(current_dir)) # Add parent directory
        from V3_app.V3_database import SQLiteRepository

    # --- Test get_stock_data (optional, keep if useful) ---
    # test_symbol = 'AAPL' 
    # logger.info(f"Testing get_stock_data for symbol: {test_symbol}")
    # stock_data = get_stock_data(test_symbol)
    # if stock_data: print(f"Data for {test_symbol} found.")
    # else: print(f"Data for {test_symbol} not found.")
    # -----------------------------------------------------

    async def main_test():
        # Point to your actual database file
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'V3_database.db')
        db_url = f"sqlite+aiosqlite:///{db_path}"
        repository = SQLiteRepository(db_url)
        
        # Ensure the finviz_raw table exists (important for first run)
        logger.info("Ensuring database tables exist...")
        await repository.create_tables() 
        
        logger.info("Running Finviz fetch and store process...")
        await fetch_and_store_finviz_data(repository)
        
        logger.info("Running screener update from Finviz raw data process...")
        await update_screener_from_finviz(repository)
        
        # --- ADD TEST CODE FOR analytics_raw ---
        # logger.info("--- Testing analytics_raw table save --- ")
        # test_ticker = "TESTAAPL"
        # test_source = "finviz"
        # test_raw_data = "P/E=25,MarketCap=2T,Test=Value"
        # await repository.save_or_update_analytics_raw_data(test_ticker, test_source, test_raw_data)
        # # Test update
        # test_raw_data_updated = "P/E=26,MarketCap=2.1T,Test=UpdatedValue,NewField=123"
        # await repository.save_or_update_analytics_raw_data(test_ticker, test_source, test_raw_data_updated)
        # # Test different source
        # await repository.save_or_update_analytics_raw_data(test_ticker, "test_source", "Source=Test,Value=ABC")
        # logger.info("--- Finished testing analytics_raw table save --- ")
        # --- END TEST CODE ---

        logger.info("Test finished.")

    # Run the async main function
    asyncio.run(main_test()) 