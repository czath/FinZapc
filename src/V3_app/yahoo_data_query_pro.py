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

    async def get_ev_to_fcf_ttm_timeseries(
        self, 
        tickers: List[str], 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate EV/FCF (TTM) timeseries for a list of tickers over the given date range.
        """
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        base_helpers = self.base_query_srv # Alias for convenience

        today = datetime.utcnow().date()
        # 1. Parse user date range & set defaults for YTD
        if not start_date:
            start_date_obj = today.replace(month=1, day=1)
        else:
            try:
                start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            except ValueError:
                logger.warning(f"[EV_FCF_TTM] Invalid start_date format: {start_date}. Defaulting to YTD start.")
                start_date_obj = today.replace(month=1, day=1)
        
        if not end_date:
            end_date_obj = today
        else:
            try:
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
            except ValueError:
                logger.warning(f"[EV_FCF_TTM] Invalid end_date format: {end_date}. Defaulting to today.")
                end_date_obj = today
        
        user_start_date_iso = start_date_obj.isoformat()
        user_end_date_iso = end_date_obj.isoformat()

        # 2. Expand lookback window for data fetch
        q_lookback_start_date_obj = start_date_obj - timedelta(days=365*2 + 30*9) 
        q_lookback_start_iso = q_lookback_start_date_obj.strftime("%Y-%m-%d")
        a_lookback_start_date_obj = start_date_obj - timedelta(days=365*3)
        a_lookback_start_iso = a_lookback_start_date_obj.strftime("%Y-%m-%d")

        for ticker in tickers:
            logger.info(f"[EV_FCF_TTM] Processing ticker: {ticker} from {user_start_date_iso} to {user_end_date_iso}")
            current_ticker_results: List[Dict[str, Any]] = []
            
            try: # Main try for processing a single ticker
                # 3. Fetch price history (daily close) for user range
                price_data = await base_helpers.get_price_history(
                    ticker=ticker,
                    interval="1d",
                    start_date=user_start_date_iso,
                    end_date=user_end_date_iso
                )
                price_map = {d['Date']: d['Close'] for d in price_data if d.get('Close') is not None}
                if not price_map:
                    logger.warning(f"[EV_FCF_TTM] No price data for {ticker} in range {user_start_date_iso}-{user_end_date_iso}. Skipping ticker.")
                    results_by_ticker[ticker] = []
                    continue # Skip to the next ticker in the outer loop
                
                # 4. Fetch shares series (quarterly, fallback logic inside helper) for expanded window
                shares_series = await base_helpers._get_quarterly_shares_series(
                    ticker,
                    q_lookback_start_iso, 
                    user_end_date_iso
                )
                shares_points = sorted([s for s in shares_series if s.get('date_obj') and s.get('value') is not None], key=lambda x: x['date_obj'])

                # 5. Fetch balance sheet fields (quarterly) for expanded window
                async def fetch_bs_field(field_name_camel_case: str):
                    field_id = f"yf_item_balance_sheet_quarterly_{field_name_camel_case}"
                    data = await base_helpers.get_specific_field_timeseries(
                        field_identifier=field_id,
                        tickers=[ticker],
                        start_date_str=q_lookback_start_iso,
                        end_date_str=user_end_date_iso
                    )
                    arr = data.get(ticker, []) if data and ticker in data else []
                    field_map = {}
                    for d_item in arr:
                        try:
                            point_date = datetime.strptime(d_item['date'], "%Y-%m-%d").date()
                            if d_item.get('value') is not None:
                                field_map[point_date] = d_item['value']
                        except (ValueError, TypeError):
                            logger.warning(f"[EV_FCF_TTM] Could not parse date or value for {field_id}, ticker {ticker}, item: {d_item}")
                            continue
                    return field_map

                debt_map = await fetch_bs_field("TotalDebt")
                minint_map = await fetch_bs_field("MinorityInterest")
                pref_map = await fetch_bs_field("PreferredStock") 
                cash_map = await fetch_bs_field("CashCashEquivalentsAndShortTermInvestments")

                # 6. Fetch FCF (quarterly and annual) for TTM, using expanded window
                quarterly_cf_items_raw = await self.db_repo.get_data_items_by_criteria(
                    ticker=ticker, item_type="CASH_FLOW_STATEMENT", item_time_coverage="QUARTER",
                    start_date=q_lookback_start_date_obj, end_date=end_date_obj, order_by_key_date_desc=False
                )
                annual_cf_items_raw = await self.db_repo.get_data_items_by_criteria(
                    ticker=ticker, item_type="CASH_FLOW_STATEMENT", item_time_coverage="FYEAR",
                    start_date=a_lookback_start_date_obj, end_date=end_date_obj, order_by_key_date_desc=False
                )

                conversion_info_fcf = await base_helpers._get_conversion_info_for_ticker(ticker, {})
                rate_fcf, orig_curr_fcf, target_curr_fcf = (conversion_info_fcf[2], conversion_info_fcf[1], conversion_info_fcf[0]) if conversion_info_fcf else (None, None, None)

                async def to_fcf_points(items_raw):
                    out = []
                    for item in items_raw:
                        payload = item.get('item_data_payload') 
                        key_date_str = item.get('item_key_date') 
                        if not isinstance(payload, dict) or not key_date_str:
                            continue
                        
                        converted_payload = payload
                        if rate_fcf and orig_curr_fcf and target_curr_fcf:
                            converted_payload = await base_helpers._apply_currency_conversion_to_payload(
                                payload, rate_fcf, orig_curr_fcf, target_curr_fcf, "CASH_FLOW_STATEMENT"
                            )
                        
                        fcf_val = converted_payload.get("Free Cash Flow") 
                        date_obj = base_helpers._parse_date_flex(key_date_str) 

                        if fcf_val is not None and date_obj:
                            try:
                                out.append({'date_obj': date_obj, 'value': float(fcf_val)})
                            except (ValueError, TypeError):
                                logger.warning(f"[EV_FCF_TTM] Could not parse FCF value for {ticker} on {key_date_str}: {fcf_val}")
                                continue # continue with the next item in items_raw
                    return sorted(out, key=lambda x: x['date_obj'])

                fcf_q_points = await to_fcf_points(quarterly_cf_items_raw)
                fcf_a_points = await to_fcf_points(annual_cf_items_raw)

                # 7. Prepare daily date list for user range
                date_list = [start_date_obj + timedelta(days=i) for i in range((end_date_obj - start_date_obj).days + 1)]
                
                # 8. Helper to get latest value as of a date from a date->value map
                def get_latest_from_map(data_map: Dict[datetime.date, Any], target_date: datetime.date) -> Any:
                    latest_val = None
                    latest_dt = None
                    for dt, val in data_map.items():
                        if dt <= target_date:
                            if latest_dt is None or dt > latest_dt:
                                latest_dt = dt
                                latest_val = val
                    return latest_val 

                def get_latest_shares_val(target_date: datetime.date) -> Optional[float]:
                    latest_s_val = None
                    latest_s_dt = None
                    for sp in shares_points: 
                        sp_date = sp['date_obj'].date() if isinstance(sp['date_obj'], datetime) else sp['date_obj']
                        if sp_date <= target_date:
                            if latest_s_dt is None or sp_date > latest_s_dt:
                                latest_s_dt = sp_date
                                latest_s_val = sp['value']
                        else: 
                            break
                    return latest_s_val

                # 9. Calculate EV/FCF (TTM) for each date
                for calc_date in date_list:
                    calc_date_iso = calc_date.isoformat()
                    ratio_value = None # Initialize ratio_value for the current day
                    
                    try: # try for a single day's calculation
                        price = price_map.get(calc_date_iso) 
                        if price is None: 
                            logger.debug(f"[EV_FCF_TTM] Price is None for {ticker} on {calc_date_iso}. Skipping day.")
                            continue # Skip to the next calc_date, do not append

                        shares = get_latest_shares_val(calc_date)
                        if shares is None or shares == 0: 
                            logger.debug(f"[EV_FCF_TTM] Shares are None or zero for {ticker} on {calc_date_iso}. Skipping day.")
                            continue # Skip to the next calc_date, do not append

                        debt = get_latest_from_map(debt_map, calc_date)
                        minint = get_latest_from_map(minint_map, calc_date)
                        pref = get_latest_from_map(pref_map, calc_date)
                        cash = get_latest_from_map(cash_map, calc_date)

                        debt = debt if debt is not None else 0.0
                        minint = minint if minint is not None else 0.0
                        pref = pref if pref is not None else 0.0
                        cash = cash if cash is not None else 0.0
                        
                        market_cap = price * shares
                        ev = market_cap + debt + minint + pref - cash
                        
                        current_eval_dt_for_ttm = datetime.combine(calc_date, datetime.min.time())
                        fcf_ttm = base_helpers._calculate_ttm_value_generic(
                            current_eval_date=current_eval_dt_for_ttm,
                            quarterly_points=fcf_q_points,
                            annual_points=fcf_a_points,
                            value_key="value", 
                            debug_identifier=f"EV_FCF_TTM_{ticker}_{calc_date_iso}"
                        )

                        if fcf_ttm is None or fcf_ttm == 0:
                            # No need to set ratio_value to None, it's already initialized to None
                            # The final check will handle this.
                            logger.debug(f"[EV_FCF_TTM] FCF TTM is None or zero for {ticker} on {calc_date_iso}. Ratio will be None.")
                        else:
                            ratio_value = ev / fcf_ttm
                        
                        # Only append if the ratio was successfully calculated and is not None
                        if ratio_value is not None:
                            current_ticker_results.append({'date': calc_date_iso, 'value': ratio_value})
                        else:
                            # This date will be skipped in the output if ratio_value remains None
                            logger.debug(f"[EV/FCF] Final EV/FCF ratio is None for {ticker} on {calc_date_iso}. Skipping data point append.")

                    except Exception as e_day_calc: # catch error for a single day's calculation
                        logger.error(f"[EV_FCF_TTM] Error calculating for {ticker} on {calc_date_iso}: {e_day_calc}", exc_info=False)
                        # Do not append anything if an error occurs for the day
                        continue # Continue to the next day
                
                results_by_ticker[ticker] = current_ticker_results
                logger.info(f"[EV_FCF_TTM] Successfully processed {ticker}. Generated {len(current_ticker_results)} data points.")

            except Exception as e_ticker_proc: # Main except for processing a single ticker
                logger.error(f"[EV_FCF_TTM] Failed to process ticker {ticker} entirely: {e_ticker_proc}", exc_info=True)
                results_by_ticker[ticker] = [] 
        
        return results_by_ticker

    async def get_ev_to_sales_ttm_timeseries(
        self, 
        tickers: List[str], 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate EV/Sales (TTM) timeseries for a list of tickers over the given date range.
        """
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        base_helpers = self.base_query_srv # Alias for convenience

        today = datetime.utcnow().date()
        # 1. Parse user date range & set defaults for YTD
        if not start_date:
            start_date_obj = today.replace(month=1, day=1)
        else:
            try:
                start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            except ValueError:
                logger.warning(f"[EV_SALES_TTM] Invalid start_date format: {start_date}. Defaulting to YTD start.")
                start_date_obj = today.replace(month=1, day=1)
        
        if not end_date:
            end_date_obj = today
        else:
            try:
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
            except ValueError:
                logger.warning(f"[EV_SALES_TTM] Invalid end_date format: {end_date}. Defaulting to today.")
                end_date_obj = today
        
        user_start_date_iso = start_date_obj.isoformat()
        user_end_date_iso = end_date_obj.isoformat()

        # 2. Expand lookback window for data fetch
        # For balance sheet items (quarterly) and shares (quarterly)
        q_lookback_start_date_obj = start_date_obj - timedelta(days=365*2 + 30*9) 
        q_lookback_start_iso = q_lookback_start_date_obj.strftime("%Y-%m-%d")
        # For income statement items (annual/quarterly for TTM)
        a_lookback_start_date_obj = start_date_obj - timedelta(days=365*3) # Adjusted for potentially longer TTM needs
        a_lookback_start_iso = a_lookback_start_date_obj.strftime("%Y-%m-%d")

        for ticker in tickers:
            logger.info(f"[EV_SALES_TTM] Processing ticker: {ticker} from {user_start_date_iso} to {user_end_date_iso}")
            current_ticker_results: List[Dict[str, Any]] = []
            
            try: # Main try for processing a single ticker
                # 3. Fetch price history (daily close) for user range
                price_data = await base_helpers.get_price_history(
                    ticker=ticker,
                    interval="1d",
                    start_date=user_start_date_iso,
                    end_date=user_end_date_iso
                )
                price_map = {d['Date']: d['Close'] for d in price_data if d.get('Close') is not None}
                if not price_map:
                    logger.warning(f"[EV_SALES_TTM] No price data for {ticker} in range {user_start_date_iso}-{user_end_date_iso}. Skipping ticker.")
                    results_by_ticker[ticker] = []
                    continue 

                # 4. Fetch shares series (quarterly, fallback logic inside helper) for expanded window
                shares_series = await base_helpers._get_quarterly_shares_series(
                    ticker,
                    q_lookback_start_iso, 
                    user_end_date_iso
                )
                shares_points = sorted([s for s in shares_series if s.get('date_obj') and s.get('value') is not None], key=lambda x: x['date_obj'])

                # 5. Fetch balance sheet fields (quarterly) for expanded window
                async def fetch_bs_field(field_name_camel_case: str):
                    field_id = f"yf_item_balance_sheet_quarterly_{field_name_camel_case}"
                    data = await base_helpers.get_specific_field_timeseries(
                        field_identifier=field_id,
                        tickers=[ticker],
                        start_date_str=q_lookback_start_iso,
                        end_date_str=user_end_date_iso
                    )
                    arr = data.get(ticker, []) if data and ticker in data else []
                    field_map = {}
                    for d_item in arr:
                        try:
                            point_date = datetime.strptime(d_item['date'], "%Y-%m-%d").date()
                            if d_item.get('value') is not None:
                                field_map[point_date] = d_item['value']
                        except (ValueError, TypeError):
                            logger.warning(f"[EV_SALES_TTM] Could not parse date or value for {field_id}, ticker {ticker}, item: {d_item}")
                            continue
                    return field_map

                debt_map = await fetch_bs_field("TotalDebt")
                minint_map = await fetch_bs_field("MinorityInterest")
                # Corrected field name for preferred stock based on previous discussion
                pref_map = await fetch_bs_field("PreferredStock") 
                cash_map = await fetch_bs_field("CashCashEquivalentsAndShortTermInvestments")

                # 6. Fetch Sales ("Total Revenue") (quarterly and annual) for TTM, using expanded window
                quarterly_is_items_raw = await self.db_repo.get_data_items_by_criteria(
                    ticker=ticker, item_type="INCOME_STATEMENT", item_time_coverage="QUARTER",
                    start_date=q_lookback_start_date_obj, end_date=end_date_obj, order_by_key_date_desc=False
                )
                annual_is_items_raw = await self.db_repo.get_data_items_by_criteria(
                    ticker=ticker, item_type="INCOME_STATEMENT", item_time_coverage="FYEAR",
                    start_date=a_lookback_start_date_obj, end_date=end_date_obj, order_by_key_date_desc=False
                )

                conversion_info_sales = await base_helpers._get_conversion_info_for_ticker(ticker, {})
                rate_sales, orig_curr_sales, target_curr_sales = (conversion_info_sales[2], conversion_info_sales[1], conversion_info_sales[0]) if conversion_info_sales else (None, None, None)

                async def to_sales_points(items_raw):
                    out = []
                    for item in items_raw:
                        payload = item.get('item_data_payload') 
                        key_date_str = item.get('item_key_date') 
                        if not isinstance(payload, dict) or not key_date_str:
                            continue
                        
                        converted_payload = payload
                        if rate_sales and orig_curr_sales and target_curr_sales:
                            converted_payload = await base_helpers._apply_currency_conversion_to_payload(
                                payload, rate_sales, orig_curr_sales, target_curr_sales, "INCOME_STATEMENT"
                            )
                        
                        sales_val = converted_payload.get("Total Revenue") 
                        date_obj = base_helpers._parse_date_flex(key_date_str) 

                        if sales_val is not None and date_obj:
                            try:
                                out.append({'date_obj': date_obj, 'value': float(sales_val)})
                            except (ValueError, TypeError):
                                logger.warning(f"[EV_SALES_TTM] Could not parse Sales value for {ticker} on {key_date_str}: {sales_val}")
                                continue 
                    return sorted(out, key=lambda x: x['date_obj'])

                sales_q_points = await to_sales_points(quarterly_is_items_raw)
                sales_a_points = await to_sales_points(annual_is_items_raw)
                
                # 7. Prepare daily date list for user range
                date_list = [start_date_obj + timedelta(days=i) for i in range((end_date_obj - start_date_obj).days + 1)]
                
                # 8. Helper to get latest value as of a date from a date->value map
                def get_latest_from_map(data_map: Dict[datetime.date, Any], target_date: datetime.date) -> Any:
                    latest_val = None
                    latest_dt = None
                    for dt, val in data_map.items():
                        if dt <= target_date:
                            if latest_dt is None or dt > latest_dt:
                                latest_dt = dt
                                latest_val = val
                    return latest_val 

                def get_latest_shares_val(target_date: datetime.date) -> Optional[float]:
                    latest_s_val = None
                    latest_s_dt = None
                    for sp in shares_points: 
                        sp_date = sp['date_obj'].date() if isinstance(sp['date_obj'], datetime) else sp['date_obj']
                        if sp_date <= target_date:
                            if latest_s_dt is None or sp_date > latest_s_dt:
                                latest_s_dt = sp_date
                                latest_s_val = sp['value']
                        else: 
                            break 
                    return latest_s_val

                # 9. Calculate EV/Sales (TTM) for each date
                for calc_date in date_list:
                    calc_date_iso = calc_date.isoformat()
                    ratio_value = None 
                    
                    try: 
                        price = price_map.get(calc_date_iso) 
                        if price is None: 
                            logger.debug(f"[EV_SALES_TTM] Price is None for {ticker} on {calc_date_iso}. Skipping day's ratio calc.")
                            continue # Skip to the next calc_date, do not append

                        shares = get_latest_shares_val(calc_date)
                        # If price is None, or shares are None/0, EV can't be reliably calculated.
                        if price is not None and (shares is None or shares == 0):
                            logger.debug(f"[EV_SALES_TTM] Shares are None or zero for {ticker} on {calc_date_iso} while price exists. EV cannot be calculated.")
                            # Ratio will remain None

                        ev = None
                        if price is not None and shares is not None and shares != 0:
                            debt = get_latest_from_map(debt_map, calc_date)
                            minint = get_latest_from_map(minint_map, calc_date)
                            pref = get_latest_from_map(pref_map, calc_date)
                            cash = get_latest_from_map(cash_map, calc_date)

                            # Default missing EV components to 0.0 as per EV/FCF structure
                            debt = debt if debt is not None else 0.0
                            minint = minint if minint is not None else 0.0
                            pref = pref if pref is not None else 0.0
                            cash = cash if cash is not None else 0.0
                            
                            market_cap = price * shares
                            ev = market_cap + debt + minint + pref - cash
                        
                        sales_ttm = None
                        if ev is not None: # Only calculate TTM Sales if EV could be calculated
                            current_eval_dt_for_ttm = datetime.combine(calc_date, datetime.min.time())
                            sales_ttm = base_helpers._calculate_ttm_value_generic(
                                current_eval_date=current_eval_dt_for_ttm,
                                quarterly_points=sales_q_points,
                                annual_points=sales_a_points,
                                value_key="value", 
                                debug_identifier=f"EV_SALES_TTM_{ticker}_{calc_date_iso}"
                            )

                        if ev is not None and sales_ttm is not None and sales_ttm != 0:
                            ratio_value = ev / sales_ttm
                        elif ev is not None and (sales_ttm is None or sales_ttm == 0):
                            logger.debug(f"[EV_SALES_TTM] Sales TTM is None or zero for {ticker} on {calc_date_iso}. Ratio is None.")
                            # ratio_value remains None
                        # If EV is None, ratio_value remains None

                        current_ticker_results.append({'date': calc_date_iso, 'value': ratio_value})

                    except Exception as e_day:
                        logger.error(f"[EV_SALES_TTM] Error calculating ratio for {ticker} on {calc_date_iso}: {e_day}", exc_info=True)
                        current_ticker_results.append({'date': calc_date_iso, 'value': None}) # Append None on error for this day
                
                results_by_ticker[ticker] = current_ticker_results
                logger.info(f"[EV_SALES_TTM] Successfully processed ticker: {ticker}. Generated {len(current_ticker_results)} data points.")

            except Exception as e_ticker:
                logger.error(f"[EV_SALES_TTM] Critical error processing ticker {ticker}: {e_ticker}", exc_info=True)
                results_by_ticker[ticker] = [] # Ensure ticker entry exists but is empty on critical error

        logger.info(f"[EV_SALES_TTM] Completed processing all tickers.")
        return results_by_ticker

    async def get_ev_to_ebitda_ttm_timeseries(
        self, 
        tickers: List[str], 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate EV/EBITDA (TTM) timeseries for a list of tickers over the given date range.
        """
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        base_helpers = self.base_query_srv # Alias for convenience

        today = datetime.utcnow().date()
        # 1. Parse user date range & set defaults for YTD
        if not start_date:
            start_date_obj = today.replace(month=1, day=1)
        else:
            try:
                start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            except ValueError:
                logger.warning(f"[EV_EBITDA_TTM] Invalid start_date format: {start_date}. Defaulting to YTD start.")
                start_date_obj = today.replace(month=1, day=1)
        
        if not end_date:
            end_date_obj = today
        else:
            try:
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
            except ValueError:
                logger.warning(f"[EV_EBITDA_TTM] Invalid end_date format: {end_date}. Defaulting to today.")
                end_date_obj = today
        
        user_start_date_iso = start_date_obj.isoformat()
        user_end_date_iso = end_date_obj.isoformat()

        # 2. Expand lookback window for data fetch
        q_lookback_start_date_obj = start_date_obj - timedelta(days=365*2 + 30*9) 
        q_lookback_start_iso = q_lookback_start_date_obj.strftime("%Y-%m-%d")
        a_lookback_start_date_obj = start_date_obj - timedelta(days=365*3) 
        a_lookback_start_iso = a_lookback_start_date_obj.strftime("%Y-%m-%d")

        for ticker in tickers:
            logger.info(f"[EV_EBITDA_TTM] Processing ticker: {ticker} from {user_start_date_iso} to {user_end_date_iso}")
            current_ticker_results: List[Dict[str, Any]] = []
            
            try: # Main try for processing a single ticker
                # 3. Fetch price history (daily close) for user range
                price_data = await base_helpers.get_price_history(
                    ticker=ticker,
                    interval="1d",
                    start_date=user_start_date_iso,
                    end_date=user_end_date_iso
                )
                price_map = {d['Date']: d['Close'] for d in price_data if d.get('Close') is not None}
                if not price_map:
                    logger.warning(f"[EV_EBITDA_TTM] No price data for {ticker} in range {user_start_date_iso}-{user_end_date_iso}. Skipping ticker.")
                    results_by_ticker[ticker] = []
                    continue 

                # 4. Fetch shares series (quarterly, fallback logic inside helper) for expanded window
                shares_series = await base_helpers._get_quarterly_shares_series(
                    ticker,
                    q_lookback_start_iso, 
                    user_end_date_iso
                )
                shares_points = sorted([s for s in shares_series if s.get('date_obj') and s.get('value') is not None], key=lambda x: x['date_obj'])

                # 5. Fetch balance sheet fields (quarterly) for expanded window
                async def fetch_bs_field(field_name_camel_case: str):
                    field_id = f"yf_item_balance_sheet_quarterly_{field_name_camel_case}"
                    data = await base_helpers.get_specific_field_timeseries(
                        field_identifier=field_id,
                        tickers=[ticker],
                        start_date_str=q_lookback_start_iso,
                        end_date_str=user_end_date_iso
                    )
                    arr = data.get(ticker, []) if data and ticker in data else []
                    field_map = {}
                    for d_item in arr:
                        try:
                            point_date = datetime.strptime(d_item['date'], "%Y-%m-%d").date()
                            if d_item.get('value') is not None:
                                field_map[point_date] = d_item['value']
                        except (ValueError, TypeError):
                            logger.warning(f"[EV_EBITDA_TTM] Could not parse date or value for {field_id}, ticker {ticker}, item: {d_item}")
                            continue
                    return field_map

                debt_map = await fetch_bs_field("TotalDebt")
                minint_map = await fetch_bs_field("MinorityInterest")
                pref_map = await fetch_bs_field("PreferredStock") 
                cash_map = await fetch_bs_field("CashCashEquivalentsAndShortTermInvestments")

                # 6. Fetch EBITDA (quarterly and annual) for TTM, using expanded window
                quarterly_is_items_raw = await self.db_repo.get_data_items_by_criteria(
                    ticker=ticker, item_type="INCOME_STATEMENT", item_time_coverage="QUARTER",
                    start_date=q_lookback_start_date_obj, end_date=end_date_obj, order_by_key_date_desc=False
                )
                annual_is_items_raw = await self.db_repo.get_data_items_by_criteria(
                    ticker=ticker, item_type="INCOME_STATEMENT", item_time_coverage="FYEAR",
                    start_date=a_lookback_start_date_obj, end_date=end_date_obj, order_by_key_date_desc=False
                )

                conversion_info_ebitda = await base_helpers._get_conversion_info_for_ticker(ticker, {})
                rate_ebitda, orig_curr_ebitda, target_curr_ebitda = (conversion_info_ebitda[2], conversion_info_ebitda[1], conversion_info_ebitda[0]) if conversion_info_ebitda else (None, None, None)

                async def to_ebitda_points(items_raw):
                    out = []
                    for item in items_raw:
                        payload = item.get('item_data_payload') 
                        key_date_str = item.get('item_key_date') 
                        if not isinstance(payload, dict) or not key_date_str:
                            continue
                        
                        converted_payload = payload
                        if rate_ebitda and orig_curr_ebitda and target_curr_ebitda:
                            converted_payload = await base_helpers._apply_currency_conversion_to_payload(
                                payload, rate_ebitda, orig_curr_ebitda, target_curr_ebitda, "INCOME_STATEMENT"
                            )
                        
                        ebitda_val = converted_payload.get("EBITDA") # Changed from "Total Revenue"
                        date_obj = base_helpers._parse_date_flex(key_date_str) 

                        if ebitda_val is not None and date_obj:
                            try:
                                out.append({'date_obj': date_obj, 'value': float(ebitda_val)})
                            except (ValueError, TypeError):
                                logger.warning(f"[EV_EBITDA_TTM] Could not parse EBITDA value for {ticker} on {key_date_str}: {ebitda_val}")
                                continue 
                    return sorted(out, key=lambda x: x['date_obj'])

                ebitda_q_points = await to_ebitda_points(quarterly_is_items_raw)
                ebitda_a_points = await to_ebitda_points(annual_is_items_raw)
                
                # 7. Prepare daily date list for user range
                date_list = [start_date_obj + timedelta(days=i) for i in range((end_date_obj - start_date_obj).days + 1)]
                
                # 8. Helper to get latest value as of a date from a date->value map
                def get_latest_from_map(data_map: Dict[datetime.date, Any], target_date: datetime.date) -> Any:
                    latest_val = None
                    latest_dt = None
                    for dt, val in data_map.items():
                        if dt <= target_date:
                            if latest_dt is None or dt > latest_dt:
                                latest_dt = dt
                                latest_val = val
                    return latest_val 

                def get_latest_shares_val(target_date: datetime.date) -> Optional[float]:
                    latest_s_val = None
                    latest_s_dt = None
                    for sp in shares_points: 
                        sp_date = sp['date_obj'].date() if isinstance(sp['date_obj'], datetime) else sp['date_obj']
                        if sp_date <= target_date:
                            if latest_s_dt is None or sp_date > latest_s_dt:
                                latest_s_dt = sp_date
                                latest_s_val = sp['value']
                        else: 
                            break 
                    return latest_s_val

                # 9. Calculate EV/EBITDA (TTM) for each date
                for calc_date in date_list:
                    calc_date_iso = calc_date.isoformat()
                    ratio_value = None 
                    
                    try: 
                        price = price_map.get(calc_date_iso) 
                        if price is None: 
                            logger.debug(f"[EV_EBITDA_TTM] Price is None for {ticker} on {calc_date_iso}. Skipping day's ratio calc.")
                            continue 

                        shares = get_latest_shares_val(calc_date)
                        if price is not None and (shares is None or shares == 0):
                            logger.debug(f"[EV_EBITDA_TTM] Shares are None or zero for {ticker} on {calc_date_iso} while price exists. EV cannot be calculated.")
                            # Ratio will remain None, but we append it for this date as price existed.
                            # This matches the previous logic before the continue was added for price=None.
                            # To strictly skip if EV cannot be made, we would 'continue' here too.
                            # For now, let it append None if EV part fails but price exists.

                        ev = None
                        if price is not None and shares is not None and shares != 0:
                            debt = get_latest_from_map(debt_map, calc_date)
                            minint = get_latest_from_map(minint_map, calc_date)
                            pref = get_latest_from_map(pref_map, calc_date)
                            cash = get_latest_from_map(cash_map, calc_date)

                            debt = debt if debt is not None else 0.0
                            minint = minint if minint is not None else 0.0
                            pref = pref if pref is not None else 0.0
                            cash = cash if cash is not None else 0.0
                            
                            market_cap = price * shares
                            ev = market_cap + debt + minint + pref - cash
                        
                        ebitda_ttm = None
                        if ev is not None: 
                            current_eval_dt_for_ttm = datetime.combine(calc_date, datetime.min.time())
                            ebitda_ttm = base_helpers._calculate_ttm_value_generic(
                                current_eval_date=current_eval_dt_for_ttm,
                                quarterly_points=ebitda_q_points,
                                annual_points=ebitda_a_points,
                                value_key="value", 
                                debug_identifier=f"EV_EBITDA_TTM_{ticker}_{calc_date_iso}"
                            )

                        if ev is not None and ebitda_ttm is not None and ebitda_ttm != 0:
                            ratio_value = ev / ebitda_ttm
                        elif ev is not None and (ebitda_ttm is None or ebitda_ttm == 0):
                            logger.debug(f"[EV_EBITDA_TTM] EBITDA TTM is None or zero for {ticker} on {calc_date_iso}. Ratio is None.")
                        
                        current_ticker_results.append({'date': calc_date_iso, 'value': ratio_value})

                    except Exception as e_day:
                        logger.error(f"[EV_EBITDA_TTM] Error calculating ratio for {ticker} on {calc_date_iso}: {e_day}", exc_info=True)
                        current_ticker_results.append({'date': calc_date_iso, 'value': None}) 
                
                results_by_ticker[ticker] = current_ticker_results
                logger.info(f"[EV_EBITDA_TTM] Successfully processed ticker: {ticker}. Generated {len(current_ticker_results)} data points.")

            except Exception as e_ticker:
                logger.error(f"[EV_EBITDA_TTM] Critical error processing ticker {ticker}: {e_ticker}", exc_info=True)
                results_by_ticker[ticker] = [] 

        logger.info(f"[EV_EBITDA_TTM] Completed processing all tickers.")
        return results_by_ticker
