import requests
import cloudscraper # Added for bypassing Cloudflare
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
import asyncio
import os
import re
import time # Added for retry delays
import random # Added for randomized delays

# Configure logging - SET TO DEBUG
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def convert_to_numeric(value):
    """Convert string values from Investing.com to appropriate numeric types."""
    # TODO: Adjust this conversion logic for Investing.com data format
    if not isinstance(value, str):
        return value
        
    # Remove any commas and spaces
    value = value.replace(',', '').strip()
    
    # Handle potential leading '+' sign before checking for '%'
    if value.startswith('+'):
        value = value[1:] # Remove the '+'
    
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

def get_investingcom_stock_data(symbol: str, max_retries: int = 3, initial_delay: float = 5.0) -> Optional[Dict[str, Any]]:
    """Fetches and parses data for a given stock symbol from Investing.com with retries."""
    # The 'symbol' here should be the specific suffix used by Investing.com
    url = f"https://www.investing.com/equities/{symbol}" # Use the provided symbol directly
    
    # Headers might need adjustment
    headers = {
        # Using a more recent User-Agent
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.investing.com/',
        'DNT': '1', # Do Not Track
        'Upgrade-Insecure-Requests': '1'
    }

    # Create a Cloudscraper instance
    scraper = cloudscraper.create_scraper()

    for attempt in range(max_retries):
        try:
            delay = initial_delay * (2 ** attempt) # Exponential backoff
            logger.debug(f"Attempt {attempt + 1}/{max_retries}: Fetching data for {symbol} from {url} using cloudscraper")
            response = scraper.get(url, headers=headers, timeout=20)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

            # --- Successful Fetch: Proceed with parsing --- 
            logger.debug(f"Attempt {attempt + 1} successful (Status {response.status_code}) for {symbol}. Parsing...")
            soup = BeautifulSoup(response.text, 'html.parser')
            data = {'_fetch_timestamp': datetime.now()}
            
            # --- Updated Parsing Logic for Investing.com ---
            summary_div = soup.find('div', id='quotes_summary_current_data')
            # Fallback if the ID is not present (less likely but possible)
            if not summary_div:
                summary_div = soup.find('div', class_='instrumentDataFlex')

            if not summary_div:
                logger.warning(f"Could not find main summary div for {symbol} after successful fetch. Skipping.")
                return None # Or continue retrying? For now, assume page structure error is fatal.

            # Extract Last Price
            last_price_span = summary_div.find('span', {'data-test': 'instrument-price-last'})
            if not last_price_span: # Fallback 1: Try the pid-xxxx-last class structure (more specific)
                last_price_span = summary_div.find('span', class_=lambda x: x and '-last' in x and 'pid-' in x) # Look for pid-xxxx-last pattern
            if not last_price_span: # Fallback 2: General class ending in -last
                last_price_span = summary_div.find('span', class_=lambda x: x and x.endswith('-last'))
            if not last_price_span: # Fallback 3: Find div with price and get first direct span
                 price_div = summary_div.find('div', {'data-test': 'instrument-price'}) # More general div
                 if price_div:
                     last_price_span = price_div.find('span', recursive=False) # First direct span child

            last_price = last_price_span.text.strip() if last_price_span else None
            if last_price:
                data['Last Price'] = convert_to_numeric(last_price)
                logger.debug(f"  Extracted Raw Last Price: '{last_price}'")
            else:
                 logger.warning(f"Could not extract Last Price for {symbol}")

            # Extract Percentage Change
            percentage_change_span = summary_div.find('span', {'data-test': 'instrument-price-change-percent'})
            if not percentage_change_span: # Fallback 1: Try the pid-xxxx-pcp class structure
                 percentage_change_span = summary_div.find('span', class_=lambda x: x and '-pcp' in x and 'pid-' in x) # Look for pid-xxxx-pcp pattern
            if not percentage_change_span: # Fallback 2: General class ending in -pcp
                percentage_change_span = summary_div.find('span', class_=lambda x: x and x.endswith('-pcp'))

            percentage_change = percentage_change_span.text.strip('() ') if percentage_change_span else None # Remove parentheses
            if percentage_change:
                data['Percentage Change'] = convert_to_numeric(percentage_change)
                logger.debug(f"  Extracted Raw Percentage Change: '{percentage_change}'")
            else:
                 logger.warning(f"Could not extract Percentage Change for {symbol}")

            # Extract Currency
            currency = None
            # Strategy 1: Try finding the bold span within the 'Currency in' text structure first
            currency_info_div = summary_div.find('div', class_='bottom') # Usually within the bottom div
            if currency_info_div:
                # Find the text node containing 'Currency in'
                currency_text_node = currency_info_div.find(string=lambda t: t and 'Currency in' in t)
                if currency_text_node:
                    # Find the immediately following bold span (might not be direct sibling in complex structures)
                    currency_bold_span = currency_text_node.find_next('span', class_='bold')
                    if currency_bold_span:
                        currency = currency_bold_span.text.strip()
                        logger.debug(f"  Extracted Raw Currency (Bottom Div): '{currency}'")

            # Strategy 2 (Fallback): Try meta tag
            if not currency:
                currency_meta = soup.find('meta', itemprop='priceCurrency')
                if currency_meta and currency_meta.get('content'):
                     currency = currency_meta['content'].strip()
                     logger.debug(f"  Extracted Raw Currency (Meta Tag): '{currency}'")

            # Strategy 3 (Fallback): Look for a span with data-test attribute
            if not currency:
                 currency_test_span = summary_div.find('span', {'data-test': 'instrument-price-currency'})
                 if currency_test_span:
                      currency = currency_test_span.text.strip()
                      logger.debug(f"  Extracted Raw Currency (Data Test): '{currency}'")

            # Assign to data if found, otherwise log warning
            if currency:
                data['Currency'] = currency
            else:
                logger.warning(f"Could not extract Currency for {symbol}")


            # Extract Data Status (e.g., "Real-time Data")
            final_data_status = None # The final status to be stored
            data_status_raw = None   # Intermediate storage for full text if not 'Closed'

            # --- Step 1: Prioritize finding "Closed" --- 
            bottom_div = summary_div.find('div', class_='bottom')
            if bottom_div:
                text_nodes = bottom_div.find_all(string=True, recursive=True)
                full_bottom_text = ' '.join(node.strip() for node in text_nodes if node.strip())
                # Use word boundary and ignore case for robust matching
                if re.search(r'\bClosed\b', full_bottom_text, re.IGNORECASE):
                    final_data_status = "Closed"
                    logger.debug(f"  Processed Data Status: '{final_data_status}' (Found explicitly)")

            # --- Step 2: If not Closed, check for "Delayed" or "Real-time" --- 
            if final_data_status is None:
                status_keywords = ["Delayed", "Real-time"] # Check these keywords now
                found_keyword = None

                # Location 1: span.noBold
                data_status_span = summary_div.find('span', class_='noBold')
                if data_status_span:
                    raw_text = data_status_span.text.strip(' - ')
                    for keyword in status_keywords:
                        if keyword in raw_text:
                            data_status_raw = raw_text
                            found_keyword = keyword
                            logger.debug(f"  Found Raw Data Status (noBold span): '{data_status_raw}'")
                            break

                # Location 2: div.lastUpdated span.noBold (only if not found yet)
                if not data_status_raw:
                    last_updated_div = summary_div.find('div', class_='lastUpdated')
                    if last_updated_div:
                        inner_span = last_updated_div.find('span', class_='noBold')
                        if inner_span:
                             raw_text = inner_span.text.strip(' - ')
                             for keyword in status_keywords:
                                 if keyword in raw_text:
                                     data_status_raw = raw_text
                                     found_keyword = keyword
                                     logger.debug(f"  Found Raw Data Status (lastUpdated div): '{data_status_raw}'")
                                     break

                # Location 3: Text nodes in div.bottom (only if not found yet)
                # Re-check bottom_div text, this time for Delayed/Real-time
                if not data_status_raw and bottom_div:
                    # We already have full_bottom_text from the 'Closed' check
                    for keyword in status_keywords:
                         # Use word boundary search again
                         if re.search(rf'\b{keyword}\b', full_bottom_text, re.IGNORECASE):
                              # Find the specific node containing the keyword to get cleaner text
                              for node in text_nodes:
                                  stripped_node = node.strip()
                                  if keyword in stripped_node:
                                      data_status_raw = stripped_node
                                      found_keyword = keyword
                                      logger.debug(f"  Found Raw Data Status (Bottom Div Text): '{data_status_raw}'")
                                      break
                              if data_status_raw: # Stop if found in bottom div
                                   break

                # --- Step 3: Process raw status (Delayed/Real-time) if found --- 
                if data_status_raw:
                    period_index = data_status_raw.find('.')
                    if period_index != -1:
                        final_data_status = data_status_raw[:period_index].strip() # Truncate before period
                    else:
                        final_data_status = data_status_raw.strip() # Use as is, ensure stripped

                    # Clean potential leading/trailing hyphens/spaces
                    final_data_status = final_data_status.strip(' - ')

                    logger.debug(f"  Processed Data Status: '{final_data_status}' (From {found_keyword} check)")

            # --- Step 4: Assign final status if found, otherwise warn --- 
            if final_data_status:
                data['Data Status'] = final_data_status
            else:
                logger.warning(f"Could not extract Data Status for {symbol}")

            # --- End Updated Parsing Logic ---

            # --- Add specific field parsing if needed (e.g., Name, Sector) ---
            # Example: Parsing Company Name might involve finding a specific h1 tag
            try:
                company_name = None
                ticker_symbol = None # Add variable for ticker

                # Strategy 1: Look for H1 inside div.instrumentHead (Most specific)
                instrument_head_div = soup.find('div', class_='instrumentHead')
                if instrument_head_div:
                     h1_tag = instrument_head_div.find('h1')
                     if h1_tag:
                          full_text = h1_tag.text.strip()
                          logger.debug(f"  Found H1 text in instrumentHead: '{full_text}'")
                          # Parse Name and Ticker from "Name (Ticker)" format
                          if '(' in full_text and full_text.endswith(')'):
                               parts = full_text.rsplit('(', 1) # Split on the last '('
                               name_part = parts[0].strip()
                               ticker_part = parts[1].rstrip(')').strip()

                               if name_part and ticker_part:
                                   company_name = name_part
                                   ticker_symbol = ticker_part
                                   logger.debug(f"  Extracted Name (instrumentHead): '{company_name}'")
                                   logger.debug(f"  Extracted Ticker (instrumentHead): '{ticker_symbol}'")
                               else:
                                   # If format doesn't match, store the whole text as name as a fallback
                                   company_name = full_text
                                   logger.debug(f"  Extracted Name (instrumentHead, no ticker found): '{company_name}'")

                # --- Fallback Strategies if instrumentHead method fails ---
                # Strategy 2: Check <title> tag
                if not company_name:
                    title_tag = soup.find('title')
                    if title_tag:
                        title_text = title_tag.text.strip()
                        # Often format is "CompanyName (SYMBOL) Stock Price | Investing.com"
                        # Try splitting by common separators and take the first part
                        separators = [' (', '|', '-']
                        possible_name = title_text
                        for sep in separators:
                            if sep in possible_name:
                                possible_name = possible_name.split(sep, 1)[0].strip()
                        # Basic validation: avoid overly short titles or just the symbol
                        if len(possible_name) > 3 and not possible_name.isupper(): 
                            company_name = possible_name
                            logger.debug(f"  Extracted Name (title tag): '{company_name}'")

                # Strategy 3: Check meta tags (og:title, twitter:title)
                if not company_name:
                    og_title = soup.find('meta', property='og:title')
                    if og_title and og_title.get('content'):
                         meta_text = og_title['content'].strip()
                         # Clean similarly to title tag if needed
                         separators = [' (', '|', '-']
                         possible_name = meta_text
                         for sep in separators:
                             if sep in possible_name:
                                 possible_name = possible_name.split(sep, 1)[0].strip()
                         if len(possible_name) > 3 and not possible_name.isupper():
                              company_name = possible_name
                              logger.debug(f"  Extracted Name (og:title meta): '{company_name}'")

                if not company_name:
                     twitter_title = soup.find('meta', attrs={'name': 'twitter:title'})
                     if twitter_title and twitter_title.get('content'):
                          meta_text = twitter_title['content'].strip()
                          separators = [' (', '|', '-']
                          possible_name = meta_text
                          for sep in separators:
                              if sep in possible_name:
                                  possible_name = possible_name.split(sep, 1)[0].strip()
                          if len(possible_name) > 3 and not possible_name.isupper():
                               company_name = possible_name
                               logger.debug(f"  Extracted Name (twitter:title meta): '{company_name}'")

                # Strategy 4: Look within the main header div for H1 with data-test
                if not company_name:
                    header_div = soup.find('div', class_=lambda x: x and 'instrumentHeader' in x)
                    if header_div:
                         company_name_tag = header_div.find('h1', {'data-test': 'asset-instrument-name-last'})
                         if company_name_tag:
                             company_name = company_name_tag.text.strip()
                             logger.debug(f"  Extracted Name (header div + data-test): '{company_name}'")

                # Strategy 5: data-test attribute directly on H1
                if not company_name:
                    company_name_tag = soup.find('h1', {'data-test': 'asset-instrument-name-last'})
                    if company_name_tag:
                        company_name = company_name_tag.text.strip()
                        logger.debug(f"  Extracted Name (data-test direct): '{company_name}'")

                # Strategy 6: Common class name pattern on H1
                if not company_name:
                    company_name_tag = soup.find('h1', class_=lambda x: x and 'instrument-header_title' in x)
                    if company_name_tag:
                        inner_span = company_name_tag.find('span')
                        if inner_span:
                            company_name = inner_span.text.strip()
                        else:
                            company_name = company_name_tag.text.strip()
                        logger.debug(f"  Extracted Name (class pattern): '{company_name}'")

                # Strategy 7: H1 inside a specific wrapper div
                if not company_name:
                    wrapper_div = soup.find('div', class_=lambda x: x and ('instrument-header' in x or 'instrument-name' in x))
                    if wrapper_div:
                        company_name_tag = wrapper_div.find('h1')
                        if company_name_tag:
                            company_name = company_name_tag.text.strip()
                            logger.debug(f"  Extracted Name (wrapper div): '{company_name}'")

                # Assign Name if found
                if company_name:
                    data['Name'] = company_name
                else:
                    logger.warning(f"Could not extract Company Name for {symbol}")

                # Assign Ticker if found by the specific strategy
                if ticker_symbol:
                     data['Ticker Symbol'] = ticker_symbol
                # No warning if ticker isn't found, as it might not always be present or needed

            except Exception as name_err:
                logger.error(f"Error parsing Company Name/Ticker for {symbol}: {name_err}", exc_info=False)
            # --- End Specific Field Parsing ---

            # --- Final Check & Return on Success --- 
            if not data or len(data) <= 1:
                logger.warning(f"No significant data extracted for {symbol} even after successful fetch. Check selectors.")
                # Decide whether to retry here? Probably not if fetch was ok but parsing failed.
                return None 
            
            logger.info(f"Successfully processed data for {symbol} on attempt {attempt + 1}. Found: {list(data.keys())}")
            return data # Return data on successful fetch and parse

        except (requests.exceptions.HTTPError, requests.exceptions.RequestException, cloudscraper.exceptions.CloudflareException) as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {symbol}: {type(e).__name__} - {e}")
            if attempt < max_retries - 1:
                 # Use exponential backoff for delay, but cap it to avoid excessively long waits
                 current_delay = min(initial_delay * (2 ** attempt), 30.0) # Cap delay at 30s
                 logger.info(f"Retrying in {current_delay:.1f} seconds...")
                 time.sleep(current_delay)
            else:
                logger.error(f"All {max_retries} attempts failed for {symbol}. Last error: {e}")
                return None # Failed all retries
        except Exception as e:
            # Catch any other unexpected errors during parsing or setup
            logger.error(f"Unexpected error during attempt {attempt + 1} for {symbol}: {e}", exc_info=True)
            # Decide if retrying makes sense for unexpected errors. Maybe not.
            return None # Exit on unexpected error

    # This point should theoretically not be reached if logic is correct, but as a safeguard:
    logger.error(f"Exited retry loop unexpectedly for {symbol} after {max_retries} attempts.")
    return None

async def clean_orphaned_investingcom_data(repository):
    """Removes records from investingcom_raw if their slug is no longer in the screener table."""
    logger.info("Starting cleanup of orphaned Investing.com raw data...")
    try:
        logger.debug("Fetching slugs from screener table (t_source2)...")
        # Assumes repository has get_all_screener_slugs returning a set or None
        screener_slugs = await repository.get_all_screener_slugs()
        if screener_slugs is None:
            logger.warning("Could not retrieve slugs from screener table (maybe empty or DB error). Skipping cleanup.")
            return

        logger.debug("Fetching slugs from investingcom_raw table...")
        # Assumes repository has get_all_investingcom_raw_slugs returning a set or None
        raw_data_slugs = await repository.get_all_investingcom_raw_slugs()
        if raw_data_slugs is None:
             logger.warning("Could not retrieve slugs from investingcom_raw table (maybe empty or DB error). Skipping cleanup.")
             return

        # Find slugs in raw_data but not in screener (set difference)
        orphaned_slugs = raw_data_slugs - screener_slugs

        if not orphaned_slugs:
            logger.info("No orphaned Investing.com raw data found. Cleanup not needed.")
            return

        logger.warning(f"Found {len(orphaned_slugs)} orphaned slugs in investingcom_raw to be deleted.")
        # Optional: Log the specific slugs being deleted if needed for debugging, but can be verbose
        # logger.debug(f"Orphaned slugs: {list(orphaned_slugs)}")

        # Assumes repository has delete_investingcom_data_by_slugs(list_of_slugs) returning deleted count or None
        deleted_count = await repository.delete_investingcom_data_by_slugs(list(orphaned_slugs))

        if deleted_count is not None:
            logger.info(f"Successfully deleted {deleted_count} orphaned records from investingcom_raw.")
            if deleted_count != len(orphaned_slugs):
                 logger.warning(f"Mismatch: Expected to delete {len(orphaned_slugs)} but actually deleted {deleted_count}.")
        else:
            logger.error("Failed to delete orphaned records from investingcom_raw (repository method returned None or failed).")

    except AttributeError as ae:
         logger.error(f"Cleanup failed: A required repository method might be missing. Ensure get_all_screener_slugs, get_all_investingcom_raw_slugs, and delete_investingcom_data_by_slugs are implemented. Error: {ae}", exc_info=False)
    except Exception as e:
        logger.error(f"An error occurred during the Investing.com raw data cleanup process: {e}", exc_info=True)


# --- Investing.com Fetch Service ---
async def fetch_and_store_investingcom_data(repository):
    """
    Fetches Investing.com data for tickers found in the screener table
    (using t_source2 for slugs) and stores the raw data. Also cleans up orphaned data first.

    Args:
        repository: An instance of the SQLiteRepository.
    """
    logger.info("Starting Investing.com data fetch process...")

    # --- Call the cleanup function first ---
    await clean_orphaned_investingcom_data(repository)
    # ----------------------------------------

    try:
        # Always fetch from the screener table now
        logger.info("Fetching tickers and slugs (t_source2) from screener table...")
        screened_tickers_data = await repository.get_all_screened_tickers()
        if not screened_tickers_data:
            logger.warning("No tickers found in the screener table. Aborting Investing.com fetch.")
            return

        tickers_to_process = []
        for item in screened_tickers_data:
            ticker = item.get('ticker')
            investingcom_slug = item.get('t_source2') # Assuming t_source2 holds the slug
            if ticker and investingcom_slug: # MUST have both ticker and slug
                tickers_to_process.append({'ticker': ticker, 'slug': investingcom_slug})
            else:
                logger.debug(f"Skipping screener entry for ticker '{ticker or 'N/A'}' due to missing slug (t_source2).")

        if not tickers_to_process:
             logger.warning("No tickers with Investing.com slugs found in screener table. Aborting fetch.")
             return
        
        logger.info(f"Found {len(tickers_to_process)} tickers from screener with Investing.com slugs to process.")

        processed_count = 0
        failed_count = 0
        # 2. Loop through tickers, fetch data, and store
        for ticker_info in tickers_to_process:
            internal_ticker = ticker_info['ticker'] # Keep internal ticker for logging clarity if needed
            investingcom_slug = ticker_info['slug']
            logger.info(f"Processing ticker: {internal_ticker} (slug: {investingcom_slug}) ({processed_count + failed_count + 1}/{len(tickers_to_process)}) --> Fetching with retries...")

            # Fetch data using get_investingcom_stock_data with the slug
            investingcom_data = get_investingcom_stock_data(investingcom_slug)

            # 4. Store raw data if fetch was successful, otherwise update status
            if investingcom_data:
                try:
                    # Use the new repository method, passing the SLUG as the key
                    await repository.save_or_update_investingcom_data(investingcom_slug, investingcom_data)
                    processed_count += 1
                except Exception as db_err:
                    logger.error(f"Database error saving Investing.com data for slug '{investingcom_slug}': {db_err}", exc_info=True)
                    failed_count += 1
            else:
                # If fetch/parse failed after retries, update status in DB using SLUG
                try:
                    await repository.save_or_update_investingcom_data(investingcom_slug, None)
                    logger.info(f"Updated status to Delayed Data in DB for failed fetch of slug '{investingcom_slug}'")
                except Exception as db_err:
                    logger.error(f"Database error updating status for failed fetch slug '{investingcom_slug}': {db_err}", exc_info=True)
                    # Still count as failure overall if DB update fails
                failed_count += 1
            
            # Optional: Add a small RANDOM delay BETWEEN different tickers to be slightly gentler
            random_delay = random.uniform(1.0, 3.0) # Delay between 1 and 3 seconds
            logger.debug(f"Sleeping for {random_delay:.2f} seconds before next ticker...")
            await asyncio.sleep(random_delay)

        logger.info(f"Investing.com data fetch process completed.")
        logger.info(f"Successfully processed: {processed_count}, Failed (fetch/parse/DB): {failed_count}")

    except Exception as e:
        logger.error(f"An error occurred during the Investing.com fetch process: {e}", exc_info=True)

# --- Function to Update Screener Table --- 
def parse_raw_data(raw_data_str: str) -> Dict[str, Any]:
    """Parses the raw_data string (format TBD) into a dictionary."""
    # TODO: Adjust parsing based on how raw data is stored for Investing.com
    data_dict = {}
    if not raw_data_str:
        return data_dict
    try:
        # Example: Assuming JSON string storage
        import json
        data_dict = json.loads(raw_data_str)
        # Apply numeric conversion if needed after parsing
        for key, value in data_dict.items():
             data_dict[key] = convert_to_numeric(value) # Use adjusted converter

    except Exception as e:
        logger.error(f"Error parsing raw_data string: '{raw_data_str}'. Error: {e}")
    return data_dict

async def update_screener_from_investingcom(repository):
    """Updates the screener table with data fetched from investingcom_raw."""
    logger.info("Starting screener update process from Investing.com raw data...")
    try:
        # 1. Get all raw data
        all_raw_data = await repository.get_all_investingcom_raw_data()

        if not all_raw_data:
            logger.warning("No data found in investingcom_raw table. Skipping screener update.")
            return
        
        logger.info(f"Processing {len(all_raw_data)} records from investingcom_raw.")
        update_count = 0
        processed_count = 0
        error_count = 0
        skipped_count = 0

        # 2. Loop through each raw record
        for raw_record in all_raw_data:
            processed_count += 1
            slug = raw_record.get('slug')
            raw_name = raw_record.get('name')
            raw_currency = raw_record.get('currency')
            raw_last_price = raw_record.get('lastprice')

            if not slug:
                logger.warning(f"Skipping raw record due to missing slug: {raw_record}")
                skipped_count += 1
                continue

            # 3. Find corresponding screener entry by slug (t_source2)
            screener_entry = await repository.get_screener_by_slug(slug)

            if not screener_entry:
                logger.warning(f"No screener entry found for slug '{slug}'. Skipping update.")
                skipped_count += 1
                continue

            ticker = screener_entry.get('ticker')
            if not ticker:
                 logger.warning(f"Screener entry found for slug '{slug}' has no ticker. Skipping update: {screener_entry}")
                 skipped_count += 1
                 continue

            updates_made_for_ticker = False
            update_errors_for_ticker = 0

            # --- Logic 1: Update Screener 'Company' if empty --- 
            screener_company = screener_entry.get('Company')
            # Check if screener Company is None or an empty string
            if not screener_company and raw_name:
                formatted_company = f"{raw_name} ({ticker})" # Format as Name (Ticker)
                try:
                    logger.info(f"[Investing.com Update] Updating Company for {ticker} from '{screener_company}' to '{formatted_company}'")
                    await repository.update_screener_ticker_details(ticker, 'Company', formatted_company)
                    updates_made_for_ticker = True
                except Exception as e:
                    logger.error(f"[Investing.com Update] Error updating Company for {ticker}: {e}", exc_info=True)
                    update_errors_for_ticker += 1
            
            # --- Logic 2: Update Screener 'currency' if empty --- 
            screener_currency = screener_entry.get('currency')
            if not screener_currency and raw_currency:
                try:
                    logger.info(f"[Investing.com Update] Updating currency for {ticker} from '{screener_currency}' to '{raw_currency}'")
                    await repository.update_screener_ticker_details(ticker, 'currency', raw_currency)
                    updates_made_for_ticker = True
                except Exception as e:
                    logger.error(f"[Investing.com Update] Error updating currency for {ticker}: {e}", exc_info=True)
                    update_errors_for_ticker += 1
            
            # --- Logic 3: Update Price --- 
            if raw_last_price is not None: # Only update if we have a price
                try:
                    logger.info(f"[Investing.com Update] Updating price for {ticker} to {raw_last_price}")
                    await repository.update_screener_ticker_details(ticker, 'price', raw_last_price)
                    updates_made_for_ticker = True
                except Exception as e:
                    logger.error(f"[Investing.com Update] Error updating price for {ticker}: {e}", exc_info=True)
                    update_errors_for_ticker += 1
            # --- End Logic 3 --- 

            # Update counts based on processing result for this ticker
            if update_errors_for_ticker > 0:
                error_count += 1
            elif updates_made_for_ticker:
                update_count += 1
            # No else needed, handled by skipped_count or successful update

        logger.info("Screener update process from Investing.com finished.")
        logger.info(f"Processed: {processed_count}, Updated: {update_count}, Errors: {error_count}, Skipped: {skipped_count}")

    except Exception as e:
        logger.error(f"An error occurred during the screener update process: {e}", exc_info=True)

# --- Service class (Optional, can be added later if needed) --- 

# Remove the standalone test execution block
# if __name__ == '__main__':
#     # ... (removed code) ... 