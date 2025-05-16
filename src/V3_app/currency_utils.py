"""Utilities for currency conversion and exchange rate fetching."""
import asyncio
import logging
from typing import Optional, Dict
import yfinance as yf
from cachetools import TTLCache

logger = logging.getLogger(__name__)

# Cache for exchange rates: 100 max items, 1 hour TTL
# The key will be a tuple: (from_currency, to_currency)
exchange_rate_cache = TTLCache(maxsize=100, ttl=3600) 

# Lock for cache access to prevent race conditions in async environment
# if multiple calls try to fetch and set the same rate simultaneously.
# A more robust solution might involve an async-specific lock if many concurrent
# requests for the *same new* rate are expected. For now, a simple lock suffices
# as yfinance calls are run in executor.
_cache_lock = asyncio.Lock()

async def get_current_exchange_rate(from_currency: str, to_currency: str) -> Optional[float]:
    """
    Fetches the current exchange rate between two currencies using Yahoo Finance.
    Caches results for 1 hour. Returns None if fetching fails or currencies are invalid.
    """
    if not from_currency or not to_currency:
        logger.warning("Cannot fetch exchange rate: from_currency or to_currency is empty.")
        return None
    
    from_currency = from_currency.upper()
    to_currency = to_currency.upper()

    if from_currency == to_currency:
        return 1.0

    cache_key = (from_currency, to_currency)
    
    # Check cache first (non-blocking check)
    if cache_key in exchange_rate_cache:
        cached_rate = exchange_rate_cache.get(cache_key)
        if cached_rate is not None: # Defensive check, TTLCache should handle expired
            logger.debug(f"Exchange rate for {from_currency}->{to_currency} found in cache: {cached_rate}")
            return cached_rate

    async with _cache_lock:
        # Double-check cache after acquiring lock, in case it was populated while waiting
        if cache_key in exchange_rate_cache:
            cached_rate = exchange_rate_cache.get(cache_key)
            if cached_rate is not None:
                return cached_rate

        try:
            currency_pair_symbol = f"{from_currency}{to_currency}=X"
            logger.info(f"Fetching exchange rate for {currency_pair_symbol} (from {from_currency} to {to_currency})")
            
            # yfinance calls are synchronous, run in executor
            loop = asyncio.get_running_loop()
            ticker = await loop.run_in_executor(None, yf.Ticker, currency_pair_symbol)
            
            # Accessing 'regularMarketPrice' or 'currentPrice' from info or fast_info
            # fast_info is generally quicker if available
            info_data = await loop.run_in_executor(None, lambda t: t.fast_info if hasattr(t, 'fast_info') else t.info, ticker)

            rate = None
            if isinstance(info_data, dict): # yf.Ticker().info returns a dict
                 # Common keys for current price in yfinance currency data
                possible_keys = ['regularMarketPrice', 'currentPrice', 'previousClose'] 
                for key in possible_keys:
                    if key in info_data and info_data[key] is not None:
                        rate = float(info_data[key])
                        logger.info(f"Fetched rate {rate} for {currency_pair_symbol} using key '{key}'.")
                        break
                if rate is None:
                    logger.warning(f"Could not find a valid rate key in info data for {currency_pair_symbol}. Data: {info_data}")
            
            elif hasattr(info_data, 'last_price'): # For yfinance versions where fast_info might be a different object
                rate = float(info_data.last_price)
                logger.info(f"Fetched rate {rate} for {currency_pair_symbol} using 'last_price'.")

            if rate is not None:
                exchange_rate_cache[cache_key] = rate
                logger.info(f"Cached exchange rate for {from_currency}->{to_currency}: {rate}")
                return rate
            else:
                logger.error(f"Failed to retrieve exchange rate for {currency_pair_symbol}. Info object was not as expected or rate was None.")
                return None

        except Exception as e:
            logger.error(f"Error fetching exchange rate for {from_currency}{to_currency}=X: {e}", exc_info=True)
            return None

if __name__ == '__main__':
    async def test_rates():
        # Test cases
        print(f"USD to EUR: {await get_current_exchange_rate('USD', 'EUR')}")
        print(f"USD to EUR (cached): {await get_current_exchange_rate('USD', 'EUR')}")
        print(f"EUR to USD: {await get_current_exchange_rate('EUR', 'USD')}")
        print(f"GBP to JPY: {await get_current_exchange_rate('GBP', 'JPY')}")
        print(f"USD to USD: {await get_current_exchange_rate('USD', 'USD')}")
        print(f"Invalid pair (XXX to YYY): {await get_current_exchange_rate('XXX', 'YYY')}")
        print(f"BRL to USD: {await get_current_exchange_rate('BRL', 'USD')}") # Example for plan
        print(f"USD to BRL: {await get_current_exchange_rate('USD', 'BRL')}")


    asyncio.run(test_rates()) 