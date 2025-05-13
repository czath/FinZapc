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
        
        logger.info(f"ADP: Yahoo data loading complete. Loaded {len(yahoo_data)} records.")
        if progress_callback:
            await progress_callback(task_name="load_yahoo_data", status="completed", progress=100, count=len(yahoo_data))
        return yahoo_data

    def _merge_data(self, finviz_data: List[Dict[str, Any]], yahoo_data: List[Dict[str, Any]], progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """
        Merges Finviz and Yahoo data using an outer join on the 'ticker' field.
        Yahoo data fields will be prefixed with 'yf_' to avoid naming collisions,
        except for the 'ticker' field used for joining.
        """
        logger.info("ADP: Merging Finviz and Yahoo data...")
        if progress_callback:
            # Progress for merging can be tricky to quantify precisely in steps
            # For now, simple start/complete messages
            asyncio.create_task(progress_callback(task_name="merge_data", status="started"))


        # TODO: Implement actual merging logic
        # - Create a dictionary of Yahoo data keyed by ticker.
        # - Iterate through Finviz data, merging with Yahoo data.
        # - Iterate through Yahoo data for tickers not in Finviz, adding them.
        # - Ensure proper prefixing for Yahoo fields (e.g., yf_somefield)
        
        merged_data = [] # Placeholder
        
        # Example simplified merge logic (needs to be robust)
        yahoo_data_map = {item['ticker']: item for item in yahoo_data if 'ticker' in item}
        
        processed_tickers = set()

        # Process Finviz data and merge with Yahoo
        for fv_item in finviz_data:
            ticker = fv_item.get('ticker')
            if not ticker:
                # Handle items without a ticker if necessary, or skip
                merged_data.append(fv_item) 
                continue
            
            processed_tickers.add(ticker)
            y_item_master = yahoo_data_map.get(ticker, {}).get('master_data', {})
            y_item_financials = yahoo_data_map.get(ticker, {}).get('financial_items', {})

            combined_item = {**fv_item} # Start with Finviz item

            # Add Yahoo master data fields with yf_tm_ prefix
            for key, value in y_item_master.items():
                if key != 'ticker': # Avoid duplicating the join key
                    combined_item[f"yf_tm_{key}"] = value
            
            # Add Yahoo financial items (already prefixed in a sense by their keys)
            for key, value in y_item_financials.items():
                 combined_item[f"yf_item_{key}"] = value # e.g. yf_item_analyst_price_targets_latest

            merged_data.append(combined_item)

        # Add Yahoo data for tickers not present in Finviz
        for ticker, y_data_container in yahoo_data_map.items():
            if ticker not in processed_tickers:
                y_item_master = y_data_container.get('master_data', {})
                y_item_financials = y_data_container.get('financial_items', {})
                
                # Create a new item, starting with the ticker
                new_item = {'ticker': ticker}
                
                # Add Yahoo master data fields with yf_tm_ prefix
                for key, value in y_item_master.items():
                    if key != 'ticker':
                        new_item[f"yf_tm_{key}"] = value
                
                # Add Yahoo financial items
                for key, value in y_item_financials.items():
                    new_item[f"yf_item_{key}"] = value
                
                merged_data.append(new_item)
        
        logger.info(f"ADP: Data merging complete. Total {len(merged_data)} records after merge.")
        if progress_callback:
            asyncio.create_task(progress_callback(task_name="merge_data", status="completed", count=len(merged_data)))
        return merged_data

    async def process_data(
        self, 
        data_source_selection: str, # e.g., "finviz_only", "yahoo_only", "both"
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None # For reporting progress
    ) -> Dict[str, Any]:
        """
        Main method to process analytics data.
        1. Loads data based on 'data_source_selection'.
        2. Merges data if 'both' are selected.
        3. Returns the processed data ('originalData') and metadata.
        
        'progress_callback' is an async function that ADP can call to report status updates.
        It might take a dictionary like: 
        {'task_name': 'load_yahoo_data', 'status': 'running'|'completed'|'failed', 'progress': 0-100, 'message': '...', 'count': N}
        """
        logger.info(f"ADP: Starting data processing. Selection: {data_source_selection}")
        if progress_callback:
            await progress_callback(task_name="main_process", status="started", message=f"Processing started for {data_source_selection}")

        finviz_data: List[Dict[str, Any]] = []
        yahoo_data: List[Dict[str, Any]] = []
        processed_data: List[Dict[str, Any]] = []
        
        # Default metadata
        meta_data = {
            "fields": [], # This will be populated with the fields from the processed_data
            "source_selection": data_source_selection,
        }

        if data_source_selection == "finviz_only" or data_source_selection == "both":
            finviz_data = await self._load_finviz_data(progress_callback=progress_callback)
        
        if data_source_selection == "yahoo_only" or data_source_selection == "both":
            yahoo_data = await self._load_yahoo_data(progress_callback=progress_callback)

        if data_source_selection == "both":
            processed_data = self._merge_data(finviz_data, yahoo_data, progress_callback=progress_callback)
        elif data_source_selection == "finviz_only":
            processed_data = finviz_data
        elif data_source_selection == "yahoo_only":
            # If only Yahoo, the data structure is List[{"ticker": ..., "master_data": {...}, "financial_items": {...}}]
            # We need to flatten this into a List[Dict[str, Any]] like the merged or Finviz-only structure.
            logger.info("ADP: Flattening Yahoo-only data structure...")
            flat_yahoo_data = []
            for y_item_container in yahoo_data:
                ticker = y_item_container.get('ticker')
                if not ticker: continue

                flat_item = {'ticker': ticker}
                master_data = y_item_container.get('master_data', {})
                financial_items = y_item_container.get('financial_items', {})

                for key, value in master_data.items():
                    if key != 'ticker':
                        flat_item[f"yf_tm_{key}"] = value
                for key, value in financial_items.items():
                    flat_item[f"yf_item_{key}"] = value
                flat_yahoo_data.append(flat_item)
            processed_data = flat_yahoo_data
            logger.info(f"ADP: Yahoo-only data flattened. {len(processed_data)} records.")

        # Generate metadata fields from the final processed data
        if processed_data:
            sample_item_fields = list(processed_data[0].keys())
            # This is a simplified field definition. Might need types, examples, etc. later.
            meta_data["fields"] = [{"name": field, "type": "unknown"} for field in sample_item_fields]

        logger.info(f"ADP: Data processing complete. Returning {len(processed_data)} records.")
        if progress_callback:
            await progress_callback(task_name="main_process", status="completed", message="Processing complete.", final_count=len(processed_data))
            
        return {
            "originalData": processed_data,
            "metaData": meta_data 
        }

# Example usage (for testing, would not be here in production)
async def example_progress_reporter(status_update: Dict[str, Any]):
    print(f"Progress Update: {status_update}")

# Remove the old __main__ test block if it instantiates ADP without the repository
# if __name__ == '__main__':
#    # ... old test code ... 