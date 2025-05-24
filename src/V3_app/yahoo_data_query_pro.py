from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union, Tuple
import json
import logging
import numpy as np

# Assuming these will be needed, adjust as necessary
from .yahoo_repository import YahooDataRepository
# from .yahoo_data_query_srv import YahooDataQueryService # Forward declaration, or Any type hint for base_query_srv

logger = logging.getLogger(__name__)

class YahooDataQueryProService:
    def __init__(self, db_repo: YahooDataRepository, base_query_srv: Any): # 'Any' to avoid circular import issues with YahooDataQueryService
        """
        Initialize the professional-tier query service with a repository instance and 
        an instance of the base query service to access its helpers and advanced service.
        """
        self.db_repo = db_repo
        if base_query_srv is None:
            logger.error("[ProQuerySrv.__init__] Base query service (base_query_srv) is required.")
            raise ValueError("Base query service instance is required for YahooDataQueryProService")
        self.base_query_srv = base_query_srv 
        logger.info("YahooDataQueryProService initialized")

    async def get_ev_to_fcf_ttm_timeseries(self, ticker: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Calculate EV/FCF (TTM) timeseries for a ticker over the given date range.
        Follows the same robust data-fetch and evaluation pattern as other TTM ratio functions.
        """
        base = self.base_query_srv
        today = datetime.utcnow().date()
        # 1. Parse user date range
        if not start_date:
            start_date = today.replace(month=1, day=1).isoformat()
        if not end_date:
            end_date = today.isoformat()
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        # 2. Expand lookback window for data fetch (2 years for quarterly, 3 for annual)
        q_lookback_start = (start_dt - timedelta(days=365*2 + 30*9)).strftime("%Y-%m-%d")
        a_lookback_start = (start_dt - timedelta(days=365*3)).strftime("%Y-%m-%d")
        # 3. Fetch price history (daily close) for user range
        price_data = await base.get_price_history(
            ticker=ticker,
            interval="1d",
            start_date=start_date,
            end_date=end_date
        )
        price_map = {d['Date']: d['Close'] for d in price_data if d.get('Close') is not None}
        # 4. Fetch shares series (quarterly, fallback logic inside helper) for expanded window
        shares_series = await base._get_quarterly_shares_series(
            ticker,
            q_lookback_start,
            end_date
        )
        shares_points = sorted(shares_series, key=lambda x: x['date_obj'])
        # 5. Fetch balance sheet fields (quarterly) for expanded window
        async def fetch_field(field):
            field_id = f"yf_item_balance_sheet_quarterly_{field.replace(' ', '')}"
            data = await base.get_specific_field_timeseries(
                field_identifier=field_id,
                tickers=[ticker],
                start_date_str=q_lookback_start,
                end_date_str=end_date
            )
            arr = data.get(ticker, []) if data and ticker in data else []
            return {d['date']: d['value'] for d in arr if d.get('value') is not None}
        debt_map = await fetch_field("Total Debt")
        minint_map = await fetch_field("Minority Interest")
        pref_map = await fetch_field("Preferred Stock")
        cash_map = await fetch_field("Cash And Cash Equivalents")
        # 6. Fetch FCF (quarterly and annual) for TTM, using expanded window
        quarterly_cf_items = await self.db_repo.get_data_items_by_criteria(
            ticker=ticker,
            item_type="CASH_FLOW_STATEMENT",
            item_time_coverage="QUARTER",
            start_date=q_lookback_start,
            end_date=end_date,
            order_by_key_date_desc=False
        )
        annual_cf_items = await self.db_repo.get_data_items_by_criteria(
            ticker=ticker,
            item_type="CASH_FLOW_STATEMENT",
            item_time_coverage="FYEAR",
            start_date=a_lookback_start,
            end_date=end_date,
            order_by_key_date_desc=False
        )
        def to_points(items):
            out = []
            for item in items:
                payload = item.get('item_data_payload')
                key_date_str = item.get('item_key_date')
                if not isinstance(payload, dict) or not key_date_str:
                    continue
                date_obj = self.base_query_srv._parse_date_flex(key_date_str)
                fcf_val = payload.get("Free Cash Flow")
                if fcf_val is not None:
                    try:
                        out.append({'date_obj': date_obj, 'value': float(fcf_val)})
                    except Exception:
                        continue
            return sorted(out, key=lambda x: x['date_obj'])
        fcf_q_points = to_points(quarterly_cf_items)
        fcf_a_points = to_points(annual_cf_items)
        # 7. Prepare daily date list for user range
        date_list = [start_dt + timedelta(days=i) for i in range((end_dt - start_dt).days + 1)]
        # 8. Helper to get latest value as of a date from a date->value map
        def get_latest(map_, date):
            if isinstance(date, datetime):
                date = date.date()
            vals = []
            for k, v in map_.items():
                try:
                    k_date = datetime.strptime(k, "%Y-%m-%d").date()
                    if k_date <= date:
                        vals.append((k_date, v))
                except Exception:
                    continue
            if not vals:
                return 0  # Default to 0 if not found
            return sorted(vals, key=lambda x: x[0])[-1][1]
        def get_latest_shares(date):
            for sp in reversed(shares_points):
                sp_date = sp['date_obj'].date() if hasattr(sp['date_obj'], 'date') else sp['date_obj']
                if sp_date <= date:
                    return sp['value']
            return 0
        # 9. Calculate EV/FCF (TTM) for each date
        results = []
        for date in date_list:
            date_str = date.isoformat()
            try:
                price = price_map.get(date_str)
                shares = get_latest_shares(date)
                debt = get_latest(debt_map, date)
                minint = get_latest(minint_map, date)
                pref = get_latest(pref_map, date)
                cash = get_latest(cash_map, date)
                ttm_date = datetime.combine(date, datetime.min.time())
                fcf_ttm = base._calculate_ttm_value_generic(
                    ttm_date,
                    fcf_q_points,
                    fcf_a_points,
                    "value",
                    debug_identifier=f"EV_FCF_TTM_{ticker}"
                )
                if price is None or shares == 0 or fcf_ttm is None or fcf_ttm == 0:
                    results.append({'date': date_str, 'value': None})
                    continue
                ev = price * shares + debt + minint + pref - cash
                ratio = ev / fcf_ttm if fcf_ttm else None
                results.append({'date': date_str, 'value': ratio})
            except Exception as e:
                results.append({'date': date_str, 'value': None})
        return results
