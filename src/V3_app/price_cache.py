"""
Module for caching price data in the backend, mirroring the frontend AnalyticsPriceCache implementation.
"""
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class PriceCache:
    def __init__(self):
        """Initialize the price cache with an empty dictionary."""
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_hits = 0
        self._cache_misses = 0
        logger.info("PriceCache initialized")

    def _generate_cache_key(self, ticker: str, interval: str, period: Optional[str], start_date: Optional[str], end_date: Optional[str]) -> str:
        """Generate a cache key from the input parameters."""
        # Convert None values to empty strings to avoid TypeError in join
        key_parts = [
            ticker,
            interval,
            period if period is not None else '',
            start_date if start_date is not None else '',
            end_date if end_date is not None else ''
        ]
        return '|'.join(key_parts)

    def get_price_data(self, ticker: str, interval: str, period: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Get price data from cache if available.
        
        Args:
            ticker: The ticker symbol
            interval: The price interval (e.g. '1d')
            period: The period type (e.g. '1y', 'max', 'custom')
            start_date: Optional start date for custom period
            end_date: Optional end date for custom period
            
        Returns:
            Cached price data if available, None otherwise
        """
        cache_key = self._generate_cache_key(ticker, interval, period, start_date, end_date)
        
        if cache_key in self._cache:
            self._cache_hits += 1
            logger.debug(f"PriceCache HIT for {cache_key}. Total hits: {self._cache_hits}")
            return self._cache[cache_key]['data']
        
        self._cache_misses += 1
        logger.debug(f"PriceCache MISS for {cache_key}. Total misses: {self._cache_misses}")
        return None

    def store_price_data(self, ticker: str, interval: str, data: List[Dict[str, Any]], period: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> None:
        """
        Store price data in the cache.
        
        Args:
            ticker: The ticker symbol
            interval: The price interval (e.g. '1d')
            data: The price data to cache
            period: The period type (e.g. '1y', 'max', 'custom')
            start_date: Optional start date for custom period
            end_date: Optional end date for custom period
        """
        cache_key = self._generate_cache_key(ticker, interval, period, start_date, end_date)
        
        # Store with timestamp for potential future cache invalidation
        self._cache[cache_key] = {
            'data': data,
            'timestamp': datetime.now(),
            'period': period,
            'interval': interval
        }
        logger.debug(f"PriceCache stored data for {cache_key}. Cache size: {len(self._cache)}")

    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0
        logger.info("PriceCache cleared")

    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        return {
            'size': len(self._cache),
            'hits': self._cache_hits,
            'misses': self._cache_misses
        }

# Create a singleton instance
price_cache = PriceCache() 