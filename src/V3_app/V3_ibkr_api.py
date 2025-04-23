"""
IBKR API service implementation for V3 of the financial application.
Handles interaction with Interactive Brokers API.

Key features:
- Account data retrieval
- Position data retrieval
- Order data retrieval
- Error handling and logging
"""

import logging
import json
import asyncio
import functools
from datetime import datetime
from typing import Dict, Any, List, Optional
import aiohttp
import sqlite3
import websocket # ADDED for websocket-client
import threading # ADDED for WS thread management
import ssl
import requests
import json
import urllib3
import time # Add import for sleep

from asyncio import Queue # Explicit import for clarity

# Use simple import -> CHANGE TO RELATIVE IMPORT
# from .V3_database import SQLiteRepository # Revert this line
from V3_database import SQLiteRepository # Back to simple import

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# RE-ADD: Disable SSL Warnings for sync example
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class IBKRService:
    """Service for interacting with IBKR API."""
    
    def __init__(self, repository: SQLiteRepository):
        """Initialize IBKR service."""
        self.repository = repository
        self.base_url = "https://localhost:5000/v1/portal"  # Base URL from official docs
        self.session: Optional[aiohttp.ClientSession] = None 
        self.authenticated_account = None
        
        # --- WebSocket Attributes COMMENTED OUT --- 
        # self.ws_app: Optional[websocket.WebSocketApp] = None
        # self.ws_thread: Optional[threading.Thread] = None
        # self.ws_loop = None 
        # self.ws_message_queue: Optional[Queue] = None
        # self.ws_connection_established = asyncio.Event() 
        # self._ws_requested_close = False 
        # --- End WebSocket Attributes --- 
        
    async def __aenter__(self):
        """Async context manager entry."""
        # Store the loop when entering the async context
        # self.ws_loop = asyncio.get_running_loop() # COMMENTED OUT
        # self.ws_message_queue = Queue() # COMMENTED OUT
        await self.connect() # Connect REST session
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        # Ensure WebSocket is closed on exit
        # await self.close_websocket() # COMMENTED OUT
        await self.disconnect() # Disconnect REST session
        
    async def connect(self) -> None:
        """Connect to IBKR API."""
        try:
            if not self.session or self.session.closed:
                 # Initialize session with an unsafe (in-memory) cookie jar
                 # This helps manage cookies automatically across requests within the session
                 jar = aiohttp.CookieJar(unsafe=True)
                 self.session = aiohttp.ClientSession(
                     connector=aiohttp.TCPConnector(ssl=False), # For development only
                     cookie_jar=jar
                 )
                 logger.info("aiohttp.ClientSession initialized with CookieJar.")
                 
            # Authenticate first
            if await self.authenticate():
                logger.info("Connected to IBKR API")
                return
            else:
                logger.error("Failed to authenticate with IBKR API")
                raise Exception("Authentication failed")
        except Exception as e:
            logger.error(f"Failed to connect to IBKR API: {str(e)}")
            logger.error("Please ensure IBKR Client Portal API Gateway is running and accessible at https://localhost:5000")
            # Clean up session if connection fails
            if self.session:
                await self.disconnect()
            raise
            
    async def authenticate(self) -> bool:
        """Authenticate with IBKR API following the official authentication flow."""
        try:
            # Create session if it doesn't exist
            if not self.session:
                self.session = aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(ssl=False)  # For development only
                )
            
            # Step 1: Check if already authenticated
            auth_status_url = f"{self.base_url}/iserver/auth/status"
            async with self.session.get(auth_status_url) as response:
                if response.status == 200:
                    status_data = await response.json()
                    logger.info(f"Auth status response: {status_data}")
                    
                    # Handle both single object and list response formats
                    if isinstance(status_data, list):
                        status = status_data[0] if status_data else {}
                    else:
                        status = status_data
                        
                    if status.get('authenticated', False) and status.get('connected', False):
                        logger.info("Already authenticated with IBKR API")
                        return True
                    elif not status.get('connected', False):
                        logger.error("IBKR Gateway is not connected")
                        return False
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to check authentication status: {error_text}")
                    return False

            # Only try to authenticate if not already authenticated
            auth_url = f"{self.base_url}/iserver/authenticate"
            async with self.session.post(auth_url) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Authentication request failed: {error_text}")
                    return False
                
                auth_response = await response.json()
                logger.info(f"Auth response: {auth_response}")
                
                # Re-check authentication status
                async with self.session.get(auth_status_url) as status_response:
                    if status_response.status == 200:
                        status_data = await status_response.json()
                        logger.info(f"Final auth status: {status_data}")
                        
                        # Handle both single object and list response formats
                        if isinstance(status_data, list):
                            status = status_data[0] if status_data else {}
                        else:
                            status = status_data
                            
                        if status.get('authenticated', False) and status.get('connected', False):
                            logger.info("Successfully authenticated with IBKR API")
                            return True
                        else:
                            error_msg = status.get('message', 'Unknown error')
                            logger.error(f"Authentication failed: {error_msg}")
                            if 'competing session' in error_msg.lower():
                                logger.error("Please restart the IBKR Gateway to clear competing sessions")
                            return False
                    else:
                        error_text = await status_response.text()
                        logger.error(f"Failed to verify authentication status: {error_text}")
                        return False

        except Exception as e:
            logger.error(f"Error during authentication: {str(e)}")
            return False
            
    async def disconnect(self) -> None:
        """Disconnect from IBKR API."""
        try:
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
                logger.info("Disconnected from IBKR API and closed aiohttp session.")
        except Exception as e:
            logger.error(f"Error disconnecting from IBKR API: {str(e)}")
            raise
            
    async def fetch_accounts(self) -> List[Dict[str, Any]]:
        """Fetch all accounts."""
        try:
            logger.info("[IBKR] Fetching accounts")
            
            # Create session if it doesn't exist
            if not self.session:
                self.session = aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(ssl=False)  # For development only
                )
            
            # Get accounts
            accounts_url = f"{self.base_url}/portfolio/accounts"
            logger.info(f"[IBKR] Request URL: {accounts_url}")
            
            async with self.session.get(accounts_url) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"[IBKR] Failed to fetch accounts: {error_text}")
                    return []
                
                accounts_data = await response.json()
                logger.info(f"[IBKR] Received accounts data: {accounts_data}")
                
                if not accounts_data or not isinstance(accounts_data, list):
                    logger.error("[IBKR] No accounts data received or invalid format")
                    return []
                
                processed_accounts = []
                for account in accounts_data:
                    try:
                        account_id = account.get('id')
                        if not account_id or account_id.lower() == 'all':
                            continue
                            
                        # Get account details
                        details_url = f"{self.base_url}/portfolio/{account_id}/summary"
                        logger.info(f"[IBKR] Fetching details for account {account_id}")
                        
                        async with self.session.get(details_url) as details_response:
                            if details_response.status != 200:
                                error_text = await details_response.text()
                                logger.error(f"[IBKR] Failed to fetch details for account {account_id}: {error_text}")
                                continue
                            
                            details = await details_response.json()
                            logger.info(f"[IBKR] Account details: {details}")
                            
                            # Extract values from summary data
                            net_liq = details.get('netliquidation', {})
                            total_cash = details.get('totalcashvalue', {})
                            gross_pos = details.get('grosspositionvalue', {})
                            
                            # Ensure we have valid amounts
                            net_liq_amount = float(net_liq.get('amount', 0)) if isinstance(net_liq, dict) else float(net_liq or 0)
                            total_cash_amount = float(total_cash.get('amount', 0)) if isinstance(total_cash, dict) else float(total_cash or 0)
                            gross_pos_amount = float(gross_pos.get('amount', 0)) if isinstance(gross_pos, dict) else float(gross_pos or 0)
                            
                            processed_account = {
                                'account_id': account_id,
                                'net_liquidation': net_liq_amount,
                                'total_cash': total_cash_amount,
                                'gross_position_value': gross_pos_amount,
                                'last_update': datetime.now(),
                                'upd_mode': 'auto'  # Set update mode to auto for fetched data
                            }
                            
                            logger.info(f"[IBKR] Processed account (auto): {processed_account}")
                            
                            # Save account
                            await self.repository.save_account(processed_account)
                            processed_accounts.append(processed_account)
                        
                    except Exception as e:
                        logger.error(f"[IBKR] Error processing account {account.get('id', 'Unknown')}: {str(e)}")
                        continue
                
                logger.info(f"[IBKR] Successfully processed {len(processed_accounts)} accounts")
                return processed_accounts
                
        except Exception as e:
            logger.error(f"[IBKR] Error fetching accounts: {str(e)}")
            raise
            
    async def fetch_positions(self, account_id: str) -> None:
        """Fetch positions for a specific account."""
        try:
            logger.info(f"[IBKR] Fetching positions for account {account_id}")
            
            # Create session if it doesn't exist
            if not self.session:
                self.session = aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(ssl=False)  # For development only
                )
            
            # Construct request URL
            request_url = f"{self.base_url}/portfolio2/{account_id}/positions?direction=a&sort=position"
            logger.info(f"[IBKR] Request URL: {request_url}")
            
            # Make request
            async with self.session.get(request_url) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"[IBKR] Failed to fetch positions: {error_text}")
                    return
                
                positions_data = await response.json()
                logger.info(f"[IBKR] Received positions data: {positions_data}")
                
                # Clear existing positions for this account
                await self.repository.clear_positions(account_id)
                logger.info(f"[IBKR] Cleared existing positions for account {account_id}")
                
                # Process and save each position
                for position in positions_data:
                    try:
                        # Extract position details with proper error handling
                        position_size = float(position.get('position', 0))
                        avg_cost = float(position.get('avgCost', 0))
                        mkt_price = float(position.get('marketPrice', 0))
                        mkt_value = float(position.get('marketValue', 0))
                        unrealized_pnl = float(position.get('unrealizedPnl', 0))
                        realized_pnl = float(position.get('realizedPnl', 0))
                        
                        # Calculate P/L percentage
                        pnl_percentage = 0
                        if position_size != 0 and avg_cost != 0:
                            pnl_percentage = (unrealized_pnl / (abs(position_size) * avg_cost)) * 100
                        
                        # Get description and symbol
                        description = position.get('description', '')
                        symbol = position.get('symbol', '')
                        ticker = symbol if symbol else description.split()[0] if description else ''
                        
                        processed_position = {
                            'account_id': account_id,
                            'ticker': ticker,
                            'name': description,
                            'position': position_size,
                            'mkt_price': mkt_price,
                            'mkt_value': mkt_value,
                            'avg_cost': avg_cost,
                            'avg_price': avg_cost,  # Using avgCost as avgPrice
                            'unrealized_pnl': unrealized_pnl,
                            'realized_pnl': realized_pnl,
                            'pnl_percentage': pnl_percentage,
                            'sector': position.get('sector', ''),
                            'group': position.get('group', ''),
                            'sector_group': position.get('assetClass', ''),
                            'currency': position.get('currency', 'USD'),
                            'sec_type': position.get('assetClass', ''),
                            'contract_desc': description,
                            'last_update': datetime.now()
                        }
                        
                        logger.info(f"[IBKR] Processed position: {processed_position}")
                        
                        # Save position
                        await self.repository.save_position(processed_position)
                        logger.info(f"[IBKR] Saved position for {processed_position['ticker']}")
                        
                    except Exception as e:
                        logger.error(f"[IBKR] Error processing position: {str(e)}")
                        logger.error(f"[IBKR] Position data that caused error: {position}")
                        continue
                
                logger.info(f"[IBKR] Successfully processed all positions for account {account_id}")
                
        except Exception as e:
            logger.error(f"[IBKR] Error fetching positions for account {account_id}: {str(e)}")
            raise
            
    async def fetch_orders(self, account_id: str) -> None:
        """Fetch orders for a given account."""
        try:
            logger.info(f"[IBKR] Fetching orders for account {account_id}")
            
            # Create session if it doesn't exist
            if not self.session:
                self.session = aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(ssl=False)  # For development only
                )
            
            # First switch to the account
            switch_url = f"{self.base_url}/iserver/account"
            switch_data = {"acctId": account_id}
            
            logger.info(f"[IBKR] Switching to account {account_id}")
            logger.info(f"[IBKR] Switch data: {json.dumps(switch_data, indent=2)}")
            
            async with self.session.post(switch_url, json=switch_data) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"[IBKR] Failed to switch account: {error_text}")
                    return
                
                switch_response = await response.json()
                logger.info(f"[IBKR] Switch account response: {switch_response}")
            
            # Get orders with live orders endpoint
            orders_url = f"{self.base_url}/iserver/account/orders"
            logger.info(f"[IBKR] Request URL: {orders_url}")
            
            # Try up to 5 times to get orders when snapshot is ready
            max_retries = 5
            retry_delay = 2
            
            for attempt in range(max_retries):
                async with self.session.get(orders_url) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"[IBKR] Failed to fetch orders: {error_text}")
                        return
                    
                    response_data = await response.json()
                    logger.info(f"[IBKR] Orders response: {response_data}")
                    
                    # Check if snapshot is ready
                    if response_data.get('snapshot', False):
                        orders_data = response_data.get('orders', [])
                        logger.info(f"[IBKR] Found {len(orders_data)} total orders")
                        
                        # Clear existing orders for this account
                        await self.repository.delete_orders_for_account(account_id)
                        logger.info(f"[IBKR] Cleared existing orders for account {account_id}")
                        
                        # Filter orders for this account and get detailed status
                        account_orders = []
                        for order in orders_data:
                            try:
                                if not isinstance(order, dict):
                                    continue
                                    
                                # Only process orders for this account
                                if order.get('account') != account_id:
                                    continue
                                
                                # Get detailed status for the order
                                order_id = order.get('orderId')
                                if order_id:
                                    status_url = f"{self.base_url}/iserver/account/order/status/{order_id}"
                                    async with self.session.get(status_url) as status_response:
                                        if status_response.status == 200:
                                            order_status = await status_response.json()
                                            # Log the detailed status response
                                            logger.info(f"[IBKR] Detailed status for order {order_id}: {json.dumps(order_status, indent=2)}") 
                                            order.update(order_status)
                                        else:
                                            error_text = await status_response.text()
                                            logger.warning(f"[IBKR] Failed to get status for order {order_id}: {error_text}")
                                
                                # Extract order details with proper field mapping
                                # Using keys provided by user based on observed API response
                                order_data = {
                                    'account_id': account_id,
                                    'order_id': str(order.get('orderId', '')),
                                    'ticker': order.get('symbol', order.get('ticker', '')), 
                                    'description': order.get('order_description', order.get('contractDescription', '')), # Use 'order_description' first
                                    'status': order.get('status', ''),
                                    'side': order.get('side', ''),
                                    'order_type': order.get('orderType', ''),
                                    'total_size': float(order.get('size', order.get('totalSize', 0))), 
                                    'filled_qty': float(order.get('filledQuantity', 0)),
                                    'remaining_qty': float(order.get('remainingQuantity', 0)),
                                    'stop_price': float(order['stop_price']) if order.get('stop_price') is not None else None, # Use 'stop_price'
                                    'limit_price': float(order['price']) if order.get('price') is not None else None,
                                    'limit_offset': float(order['limit_price_Offset']) if order.get('limit_price_Offset') is not None else None, # Use 'limit_price_Offset' as specified
                                    'trailing_amount': float(order['trailing_amount']) if order.get('trailing_amount') is not None else None, # Use 'trailing_amount'
                                    'avg_price': float(order['avgPrice']) if order.get('avgPrice') is not None else None,
                                    'currency': order.get('currency', 'USD'),
                                    'last_update': datetime.now()
                                }
                                
                                # Log the final processed order data before saving
                                logger.info(f"[IBKR] Processed order data before save: {order_data}")
                                
                                # Save order
                                await self.repository.save_order(order_data)
                                logger.info(f"[IBKR] Saved order {order_data['order_id']}")
                                account_orders.append(order_data)
                                
                            except Exception as e:
                                logger.error(f"[IBKR] Error processing order: {str(e)}")
                                logger.error(f"[IBKR] Order data that caused error: {order}")
                                continue
                        
                        logger.info(f"[IBKR] Successfully processed {len(account_orders)} orders for account {account_id}")
                        break
                    else:
                        if attempt < max_retries - 1:
                            logger.info(f"[IBKR] Orders snapshot not ready, waiting {retry_delay} seconds (attempt {attempt + 1}/{max_retries})")
                            await asyncio.sleep(retry_delay)
                            retry_delay *= 2  # Exponential backoff
                        else:
                            logger.warning(f"[IBKR] Orders snapshot not ready after {max_retries} attempts")
                
        except Exception as e:
            logger.error(f"[IBKR] Error fetching orders for account {account_id}: {str(e)}")
            raise
            
    async def process_positions(self, account_id: str, positions_data: List[Dict[str, Any]]) -> None:
        """Process and save position data."""
        try:
            # Delete existing positions for this account first
            if self.repository:
                try:
                    await self.repository.delete_positions_for_account(account_id)
                    logger.info(f"Deleted existing positions for account {account_id}")
                except Exception as e:
                    logger.error(f"Error deleting existing positions for {account_id}: {str(e)}")
            
            if not positions_data:
                logger.info(f"No positions found for account {account_id}")
                return
                
            processed_count = 0
            for position in positions_data:
                try:
                    logger.info(f"Processing position: {json.dumps(position, indent=2)}")
                    
                    # Calculate P/L percentage correctly
                    position_size = float(position.get('position', 0))
                    avg_cost = float(position.get('avgCost', 0))
                    unrealized_pnl = float(position.get('unrealizedPnl', 0))
                    total_cost = position_size * avg_cost
                    pnl_percentage = (unrealized_pnl / total_cost * 100) if total_cost != 0 else 0
                    
                    # Get currency from the position data
                    currency = None
                    
                    # Try to get currency from different fields
                    if 'listingCurrency' in position:
                        currency = position['listingCurrency']
                    elif 'currency' in position:
                        currency = position['currency']
                    elif 'contract' in position and isinstance(position['contract'], dict):
                        currency = position['contract'].get('currency')
                    
                    # If no currency found, try to extract from market value
                    if not currency and isinstance(position.get('mktValue'), dict):
                        currency = position['mktValue'].get('currency')
                    
                    # Default to USD if no currency found
                    if not currency:
                        currency = 'USD'
                        logger.warning(f"No currency found for position {position.get('ticker', 'Unknown')}, defaulting to USD")
                    
                    position_data = {
                        'account_id': account_id,
                        'ticker': position.get('ticker', position.get('contractDesc', '')),
                        'name': position.get('name', ''),
                        'position': position_size,
                        'mkt_price': float(position.get('mktPrice', 0)),
                        'mkt_value': float(position.get('mktValue', 0)),
                        'avg_cost': avg_cost,
                        'avg_price': float(position.get('avgPrice', 0)),
                        'pnl': unrealized_pnl,
                        'pnl_percentage': pnl_percentage,
                        'sector': position.get('sector', ''),
                        'group': position.get('group', ''),
                        'currency': currency,
                        'last_update': datetime.now()
                    }
                    
                    logger.info(f"Processed position data: {json.dumps(position_data, indent=2, default=str)}")
                    
                    if self.repository:
                        try:
                            await self.repository.save_position(position_data)
                            processed_count += 1
                            logger.info(f"Saved position for {account_id}: {position_data['ticker']}")
                        except Exception as e:
                            logger.error(f"Error saving position for {account_id}: {str(e)}")
                            continue
                except Exception as e:
                    logger.error(f"Error processing position for {account_id}: {str(e)}")
                    continue
                    
            logger.info(f"Successfully processed {processed_count} positions for account {account_id}")
                    
        except Exception as e:
            logger.error(f"Error processing positions for account {account_id}: {str(e)}")
        
    async def process_orders(self, account_id: str, orders_data: List[Dict[str, Any]]) -> None:
        """Process and save order data."""
        try:
            # Delete existing orders for this account first
            if self.repository:
                try:
                    await self.repository.delete_orders_for_account(account_id)
                    logger.info(f"Deleted existing orders for account {account_id}")
                except Exception as e:
                    logger.error(f"Error deleting existing orders for {account_id}: {str(e)}")
            
            if not orders_data:
                logger.info(f"No orders found for account {account_id}")
                return
                
            logger.info(f"\nProcessing {len(orders_data)} orders for account {account_id}")
            processed_count = 0
            for order in orders_data:
                try:
                    logger.info(f"\nProcessing order: {json.dumps(order, indent=2)}")
                    
                    # Extract order details with proper field mapping
                    order_data = {
                        'account_id': account_id,
                        'order_id': str(order.get('id', '')),
                        'ticker': order.get('symbol', ''),
                        'description': order.get('description', ''),
                        'status': order.get('status', ''),
                        'side': order.get('side', ''),
                        'order_type': order.get('type', ''),
                        'total_size': float(order.get('size', 0)),
                        'filled_qty': float(order.get('filledQuantity', 0)),
                        'remaining_qty': float(order.get('remainingQuantity', 0)),
                        'stop_price': float(order.get('stopPrice', 0)) if order.get('stopPrice') else None,
                        'limit_price': float(order.get('price', 0)) if order.get('price') else None,
                        'limit_offset': float(order.get('limit_price_Offset', 0)) if order.get('limit_price_Offset') else None,
                        'trailing_amount': float(order.get('trailing', 0)) if order.get('trailing') else None,
                        'avg_price': float(order.get('avgPrice', 0)) if order.get('avgPrice') else None,
                        'currency': order.get('currency', 'USD'),
                        'last_update': datetime.now()
                    }
                    
                    logger.info(f"\nProcessed order data:")
                    for key, value in order_data.items():
                        logger.info(f"  {key}: {value}")
                    
                    if self.repository:
                        try:
                            await self.repository.save_order(order_data)
                            processed_count += 1
                            logger.info(f"Successfully saved order {order_data['order_id']} for account {account_id}")
                        except Exception as e:
                            logger.error(f"Error saving order for {account_id}: {str(e)}")
                            continue
                except Exception as e:
                    logger.error(f"Error processing order for {account_id}: {str(e)}")
                    continue
                    
            logger.info(f"\nSummary: Successfully processed {processed_count} out of {len(orders_data)} orders for account {account_id}")
                    
        except Exception as e:
            logger.error(f"Error processing orders for account {account_id}: {str(e)}")
        
    async def fetch_data(self) -> None:
        """Fetch all data from IBKR API."""
        try:
            # Clear all existing data first -- REMOVED to preserve manual accounts
            # await self.repository.clear_all_data()
            # logger.info("Cleared all existing data")
            
            # Connect and authenticate
            await self.connect()
            
            # Fetch accounts first
            accounts = await self.fetch_accounts()
            if not accounts:
                logger.warning("No accounts found in IBKR API")
                return
                
            logger.info(f"Found {len(accounts)} accounts")
            
            # Process each account's data
            for account in accounts:
                account_id = account.get('account_id')
                if not account_id:
                    continue
                    
                try:
                    # Fetch positions and orders for each account
                    await self.fetch_positions(account_id)
                    await self.fetch_orders(account_id)
                    logger.info(f"Successfully fetched data for account {account_id}")
                except Exception as e:
                    logger.error(f"Error fetching data for account {account_id}: {str(e)}")
                    continue
            
            logger.info("Successfully fetched all data from IBKR API")
            
        except Exception as e:
            logger.error(f"Error fetching data from IBKR API: {str(e)}")
            raise
        finally:
            # Only disconnect after all data is fetched
            await self.disconnect() 

    # --- NEW Get ConID by Ticker --- 
    async def get_conid_for_ticker(self, ticker: str) -> Optional[int]:
        """Fetches the primary conid for a given stock ticker symbol."""
        if not self.session or self.session.closed:
            logger.error("Cannot fetch conid, no active session. Please connect first.")
            # Optionally try connecting here? 
            return None

        search_url = f"{self.base_url}/iserver/secdef/search"
        payload = {
            "symbol": ticker.upper(),
            "secType": "STK" # Specify Stock to narrow results
            # Add other parameters like 'exchange' or 'currency' if needed for disambiguation
        }
        logger.info(f"Searching for conid for ticker: {ticker} with payload: {payload}")

        try:
            async with self.session.post(search_url, json=payload) as response:
                if response.status == 200:
                    results = await response.json()
                    logger.debug(f"Secdef search results for {ticker}: {results}")
                    
                    if not results or not isinstance(results, list):
                        logger.warning(f"No conid found for ticker {ticker} or invalid response format.")
                        return None
                        
                    # Select the first result (often the primary listing)
                    # More sophisticated logic could be added here to select based on exchange etc.
                    first_result = results[0]
                    conid_val = first_result.get('conid')
                    
                    # --- Attempt to convert conid string to int --- 
                    conid = None
                    if conid_val:
                        try:
                            conid = int(conid_val)
                        except (ValueError, TypeError):
                            logger.warning(f"Could not convert extracted conid '{conid_val}' to integer for {ticker}.")
                    # --- End Conversion ---
                    
                    # Check if conversion was successful
                    if conid is not None: 
                        logger.info(f"Found conid {conid} for ticker {ticker}")
                        return conid
                    else:
                        logger.warning(f"Could not extract valid conid from first result for {ticker}: {first_result}")
                        return None
                        
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to search secdef for {ticker}: {response.status} - {error_text}")
                    return None

        except aiohttp.ClientError as e:
            logger.error(f"Network error fetching conid for {ticker}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching conid for {ticker}: {e}", exc_info=True)
            return None
    # --- End Get ConID ---

    # --- Market Data Snapshot via REST (Using aiohttp Session) ---
    async def fetch_market_data_snapshot(self, conids: List[int], fields: List[str]) -> Optional[List[Dict[str, Any]]]:
        """Fetches snapshot using the active authenticated aiohttp session."""
        if not self.session or self.session.closed:
            logger.error("Snapshot: Cannot fetch, no active REST session.")
            return None
        if not self.base_url:
             logger.error("Snapshot: Base URL not set.")
             return None
        if not conids:
            logger.warning("Snapshot: No conids provided.")
            return []
        if not fields: # Also check if fields list is empty
            logger.warning("Snapshot: No fields provided to request.")
            return []

        # Use the fields provided directly
        logger.debug(f"Snapshot: Requesting fields: {fields}")

        conids_str = ",".join(map(str, conids))
        fields_str = ",".join(fields) # Use the original fields list
        snapshot_url = f"{self.base_url}/iserver/marketdata/snapshot?conids={conids_str}&fields={fields_str}"
        logger.info(f"Snapshot (Async): Requesting market data snapshot: {snapshot_url}")

        try:
            async with self.session.get(snapshot_url) as response:
                logger.info(f"Snapshot (Async): Response Status: {response.status}")
                if response.status == 200:
                    try:
                        data = await response.json()
                        if isinstance(data, list):
                            # Check against the originally requested fields
                            if data and all(f'_{field}' in data[0] for field in fields):
                                logger.info(f"Snapshot (Async): Successfully received data with fields: {json.dumps(data[0], indent=2)}")
                            elif data:
                                logger.warning(f"Snapshot (Async): Received data but MISSING expected fields. Fields requested: {fields}. Data: {json.dumps(data[0], indent=2)}")
                            else:
                                 logger.info("Snapshot (Async): Received empty list.")
                            return data
                        else:
                            logger.error(f"Snapshot (Async): Unexpected response format (not a list): {data}")

                    except json.JSONDecodeError as e:
                        raw_text = await response.text()
                        logger.error(f"Snapshot (Async): JSON decode error: {e}")
                        logger.error(f"Snapshot (Async): Raw response text: {raw_text}")
                        return None 
                    except aiohttp.ContentTypeError as e:
                        raw_text = await response.text()
                        logger.error(f"Snapshot (Async): Content type error (likely not JSON): {e}")
                        logger.error(f"Snapshot (Async): Raw response text: {raw_text}")
                        return None
                else:
                    error_text = await response.text()
                    logger.error(f"Snapshot (Async): Request failed: {response.status} - {error_text}")
                    return None 

        except aiohttp.ClientError as e:
            logger.error(f"Snapshot (Async): Network error during aiohttp request: {e}")
            return None 
        except Exception as e:
            logger.error(f"Snapshot (Async): Unexpected error during aiohttp request: {e}", exc_info=True)
            return None 
    # --- End Market Data Snapshot --- 

    # --- Switch Account Method ---
    async def switch_account(self, account_id: str) -> bool:
        """Switches the active account context for the session."""
        if not self.session or self.session.closed:
            logger.error("SwitchAccount: Cannot switch, no active REST session.")
            return False
        if not account_id:
            logger.error("SwitchAccount: No account_id provided.")
            return False

        switch_url = f"{self.base_url}/iserver/account"
        payload = {"acctId": account_id}
        logger.info(f"SwitchAccount: Attempting to switch active account to {account_id}")
        logger.debug(f"SwitchAccount: Request URL: {switch_url}, Payload: {json.dumps(payload)}")

        try:
            async with self.session.post(switch_url, json=payload) as response:
                logger.info(f"SwitchAccount: Response Status: {response.status}")
                response_text = await response.text() # Read text regardless of status
                logger.debug(f"SwitchAccount: Response Text: {response_text}")

                if response.status == 200:
                    try:
                        response_data = json.loads(response_text) # Try parsing JSON
                        logger.info(f"SwitchAccount: Successfully switched to account {account_id}. Response: {response_data}")
                        # Optional: Check response content if needed, e.g., response_data.get('set', False)
                        return True
                    except json.JSONDecodeError:
                         # Sometimes success response might not be JSON
                         logger.info(f"SwitchAccount: Successfully switched to account {account_id} (non-JSON response).")
                         return True
                else:
                    logger.error(f"SwitchAccount: Failed to switch to account {account_id}. Status: {response.status}, Text: {response_text}")
                    return False
        except aiohttp.ClientError as e:
            logger.error(f"SwitchAccount: Network error switching account: {e}")
            return False
        except Exception as e:
            logger.error(f"SwitchAccount: Unexpected error switching account: {e}", exc_info=True)
            return False
    # --- End Switch Account Method ---

    # --- Get Server Accounts --- 
    async def get_server_accounts(self) -> Optional[Dict[str, Any]]:
        """Calls the /iserver/accounts endpoint to fulfill snapshot prerequisite."""
        if not self.session or self.session.closed:
            logger.error("IServerAccounts: Cannot fetch, no active REST session.")
            return None

        iserver_accounts_url = f"{self.base_url}/iserver/accounts"
        logger.info(f"IServerAccounts: Calling endpoint: {iserver_accounts_url}")
        
        try:
            async with self.session.get(iserver_accounts_url) as response:
                if response.status == 200:
                    accounts_data = await response.json()
                    logger.info(f"IServerAccounts: Received data: {json.dumps(accounts_data)}")
                    # We don't strictly need the data, just need to have called the endpoint
                    return accounts_data 
                else:
                    error_text = await response.text()
                    logger.error(f"IServerAccounts: Failed to call endpoint: {response.status} - {error_text}")
                    return None
        except Exception as e:
             logger.error(f"IServerAccounts: Unexpected error: {e}", exc_info=True)
             return None
    # --- End Get Server Accounts --- 

    # --- WebSocket Implementation COMMENTED OUT --- 

    # --- Callback Methods --- 
    # def _ws_on_message(self, ws, message):
    #   ...
    # def _ws_on_error(self, ws, error):
    #   ...
    # def _ws_on_close(self, ws, close_status_code, close_msg):
    #   ...
    # def _ws_on_open(self, ws):
    #   ...
    # --- End Callback Methods --- 

    # --- WebSocket Thread Runner --- 
    # def _run_websocket_client(self):
    #    ...
    # --- End WebSocket Thread Runner --- 
                 
    # --- Main WebSocket Control Methods --- 
    # async def stream_market_data(self, conids: List[int]):
    #    ...
    # --- Helper to Send Messages --- 
    # async def _send_ws_message_threadsafe(self, message: str):
    #    ...
    # --- End Helper --- 
    # async def subscribe_market_data(self, conids: List[int]):
    #    ...
    # async def unsubscribe_market_data(self, conids: List[int]):
    #    ...
    # async def close_websocket(self):
    #    ...
    # async def subscribe_account_summary(self, account_id: str):
    #    ...
    # --- End WebSocket --- 

# --- ADD SyncIBKRService Class Definition --- 
class SyncIBKRService:
    """Minimal service for specific synchronous IBKR tasks using requests."""
    def __init__(self, base_url: str = "https://localhost:5000/v1/api/"):
        # Note: Using /v1/api/ path based on successful sync test example
        self.base_url = base_url
        logger.info(f"SyncIBKRService initialized with base URL: {self.base_url}")

    def get_conids_sync(self, identifiers_info: List[Dict[str, str]]) -> List[Optional[int]]:
        """Synchronously fetches conids for a list of identifiers (ticker or pair)
           and their security types.
        Returns a list of the same length as input, with None for failed lookups.
        """
        if not identifiers_info:
            return []

        conid_results: List[Optional[int]] = [] # Initialize list to hold results (int or None)
        search_url = f"{self.base_url}/iserver/secdef/search"
        headers = {'accept': 'application/json'}

        logger.info(f"Sync GetConids: Fetching conids for {len(identifiers_info)} identifiers.")

        # Disable SSL warnings for localhost testing if necessary
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        session = requests.Session()
        session.verify = False # Disable SSL verification for localhost

        for identifier_info in identifiers_info:
            identifier = identifier_info.get('identifier')
            sec_type = identifier_info.get('secType')

            if not identifier or not sec_type:
                logger.warning(f"Sync GetConids: Skipping invalid entry in identifiers_info: {identifier_info}")
                conid_results.append(None)
                continue

            # Use the provided secType in the payload
            payload = {"symbol": identifier.upper(), "secType": sec_type}
            found_conid: Optional[int] = None # Reset for each identifier

            try:
                response = session.post(search_url, headers=headers, json=payload)

                if response.status_code == 200:
                    # Use response.json() directly
                    results = response.json()

                    if results and isinstance(results, list):
                        # Iterate through results to find matching secType
                        match_found = False # RENAME to first_match_found
                        first_match_found = False
                        match_count = 0 # ADD counter
                        for item in results:
                            try:
                                # Check secType within the first section if available
                                response_sec_type = None
                                if isinstance(item, dict) and 'sections' in item and isinstance(item['sections'], list) and item['sections']:
                                    response_sec_type = item['sections'][0].get('secType')
                                
                                if response_sec_type == sec_type: # Compare with requested sec_type
                                    match_count += 1 # Increment count
                                    # Store only the FIRST valid conid found
                                    if not first_match_found:
                                        conid_val = item.get('conid')
                                        if conid_val is not None:
                                            temp_conid = int(conid_val) # Convert to int
                                            if temp_conid <= 0: # Treat 0 or negative as invalid
                                                logger.warning(f"Sync GetConids: Found matching {sec_type} (first) but non-positive conid {temp_conid} for {identifier}. Treating as failure.")
                                                # Keep found_conid as None for this item
                                            else:
                                                # This is the first valid match
                                                found_conid = temp_conid 
                                                first_match_found = True 
                                                logger.info(f"Sync GetConids: Found FIRST matching conid {found_conid} for {identifier} (secType: {sec_type}) in results.")
                                        else:
                                             logger.warning(f"Sync GetConids: Found matching {sec_type} (first) for {identifier} but 'conid' key missing in item: {item}")
                                    # END storing logic
                            except (ValueError, TypeError, IndexError, KeyError) as e:
                                logger.warning(f"Sync GetConids: Error processing item while searching for {sec_type} match for {identifier}: {e}. Item: {item}")
                                continue # Try next item
                        
                        # Update logging based on count
                        if match_count == 0:
                             logger.warning(f"Sync GetConids: No result with matching secType '{sec_type}' found for {identifier} in {len(results)} results.")
                        elif match_count > 1:
                             logger.warning(f"Sync GetConids: Found {match_count} results matching secType '{sec_type}' for {identifier}. Using first conid found: {found_conid}.")
                        # END Update logging
                    else:
                        # Log remains the same: No results or invalid format
                        logger.warning(f"Sync GetConids: No results or invalid format for {identifier} (secType: {sec_type}). Response: {results}")
                else:
                    # Log API error (already exists, keep it)
                    logger.error(f"Sync GetConids: API error for {identifier} (secType: {sec_type}): {response.status_code} - {response.text}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Sync GetConids: Network error for {identifier} (secType: {sec_type}): {e}")
            except Exception as e:
                 logger.error(f"Sync GetConids: Unexpected error processing {identifier} (secType: {sec_type}): {e}", exc_info=True)

            # ALWAYS append the result (found_conid or None)
            conid_results.append(found_conid)
            if found_conid is None:
                 logger.debug(f"Sync GetConids: Appending None for identifier {identifier} (secType: {sec_type})")
            else:
                 logger.debug(f"Sync GetConids: Appending {found_conid} for identifier {identifier} (secType: {sec_type})")

        successful_count = sum(1 for c in conid_results if c is not None)
        logger.info(f"Sync GetConids: Found {successful_count} conids in total. Returning list of length {len(conid_results)}.")
        return conid_results

    def fetch_snapshot_sync(self, conids: List[int], fields: List[str], retries: int = 3, delay: int = 2) -> Optional[List[Dict[str, Any]]]:
        """Synchronous snapshot fetch using requests with retry logic."""
        if not conids or not fields:
            logger.warning("Sync Snapshot: Missing conids or fields.")
            return None

        conids_str = ",".join(map(str, conids))
        fields_str = ",".join(fields)
        params_str = f"conids={conids_str}&fields={fields_str}"
        request_url = f"{self.base_url}iserver/marketdata/snapshot?{params_str}" 
        
        for attempt in range(retries):
            logger.info(f"Sync Snapshot: Requesting URL (Attempt {attempt + 1}/{retries}): {request_url}")
            try:
                response = requests.get(url=request_url, verify=False, timeout=15)
                logger.info(f"Sync Snapshot: Response Status: {response.status_code}")
                print(f"<Response [{response.status_code}]>") # Keep user's print statement

                if response.status_code == 200:
                    try:
                        data = response.json()
                        print(json.dumps(data, indent=2)) # Keep user's print statement

                        if isinstance(data, list) and data:
                            # Check if *any* requested data field exists in the first item
                            # Assumes the presence of one field indicates a successful data fetch
                            first_item = data[0]
                            has_any_data_field = any(f in first_item for f in fields)

                            if has_any_data_field:
                                logger.info(f"Sync Snapshot (Attempt {attempt + 1}): Successfully fetched snapshot data WITH expected fields.")
                                return data # Success!
                            else:
                                logger.warning(f"Sync Snapshot (Attempt {attempt + 1}): Received 200 but MISSING expected data fields. Data: {first_item}")
                        elif isinstance(data, list):
                            logger.info(f"Sync Snapshot (Attempt {attempt + 1}): Received empty list.")
                            # Consider if an empty list is a valid success or should be retried
                            return data # Returning empty list as potentially valid
                        else:
                            logger.warning(f"Sync Snapshot (Attempt {attempt + 1}): Response was not a list. Data: {data}")

                    except json.JSONDecodeError as e:
                        logger.error(f"Sync Snapshot (Attempt {attempt + 1}): JSON decode error: {e}. Raw text: {response.text}")
                        print(f"JSON Decode Error: {e}")
                        print(f"Raw Text: {response.text}")
                    except Exception as e: # Catch other potential errors during JSON processing
                         logger.error(f"Sync Snapshot (Attempt {attempt + 1}): Error processing JSON response: {e}", exc_info=True)

                else: # Non-200 status code
                     logger.error(f"Sync Snapshot (Attempt {attempt + 1}): Request failed: {response.status_code} - {response.text}")
                     print(f"Request Failed ({response.status_code}): {response.text}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Sync Snapshot (Attempt {attempt + 1}): Request exception: {e}")
                print(f"Request Error: {e}")
            except Exception as e:
                logger.error(f"Sync Snapshot (Attempt {attempt + 1}): Unexpected error: {e}", exc_info=True)
                print(f"Unexpected Error: {e}")

            # If we reached here, it means the attempt failed or data was missing
            if attempt < retries - 1:
                logger.info(f"Sync Snapshot: Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                 logger.error(f"Sync Snapshot: Failed after {retries} attempts.")

        return None # Return None after all retries failed
# --- End Sync Service --- 