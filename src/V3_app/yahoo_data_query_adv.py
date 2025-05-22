# src/V3_app/yahoo_data_query_adv.py
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union, Tuple
import json
import logging

# Assuming helper functions will be imported from the existing service or refactored into a common place.
# For now, we'll define placeholders or assume they can be accessed.
# from .yahoo_data_query_srv import YahooDataQueryService (or specific helpers)
from .yahoo_repository import YahooDataRepository
# from .currency_utils import get_current_exchange_rate # Removed direct import

logger = logging.getLogger(__name__)

# OUTPUT_KEY_TO_DB_MAPPING # Removed, assuming it's used by helpers in base_query_srv

class YahooDataQueryAdvService:
    def __init__(self, db_repo: YahooDataRepository, base_query_srv: Any): # Changed base_query_srv type to Any for now, ideally YahooDataQueryService
        """
        Initialize the advanced service with a repository instance and 
        an instance of the base query service to access its helpers.
        """
        self.db_repo = db_repo
        if base_query_srv is None:
            # Optionally, handle the case where base_query_srv might not be provided,
            # though for this refactoring, we assume it is.
            logger.error("[AdvQuerySrv.__init__] Base query service (base_query_srv) is required.")
            raise ValueError("Base query service instance is required for YahooDataQueryAdvService")
        self.base_query_srv = base_query_srv 
        logger.info("YahooDataQueryAdvService initialized")

    async def calculate_fcf_margin_ttm(
        self,
        tickers: List[str],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None,
        # ticker_profiles_cache is managed by base_query_srv._get_conversion_info_for_ticker
    ) -> Dict[str, List[Dict[str, Any]]]:
        logger.info(f"[AdvQuerySrv.calculate_fcf_margin_ttm] Request for Tickers: {tickers}, Start: {start_date_str}, End: {end_date_str}")
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        
        # Initialize a shared ticker_profiles_cache for this operation - NO LONGER NEEDED HERE IF CONVERSION IS REMOVED
        # ticker_profiles_cache: Dict[str, Dict[str, Any]] = {} 

        user_start_date_obj: Optional[datetime] = None
        user_end_date_obj: Optional[datetime] = None
        today = datetime.today()

        if start_date_str:
            user_start_date_obj = self.base_query_srv._parse_date_flex(start_date_str)
        else:
            user_start_date_obj = today.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        
        if end_date_str:
            user_end_date_obj = self.base_query_srv._parse_date_flex(end_date_str)
        else:
            user_end_date_obj = today.replace(hour=23, minute=59, second=59, microsecond=999999)

        if not user_start_date_obj or not user_end_date_obj:
            logger.error("[AdvQuerySrv.calculate_fcf_margin_ttm] Failed to parse start or end dates.")
            return results_by_ticker

        fundamental_query_start_date_obj = user_start_date_obj - timedelta(days=5*365)
        fundamental_query_start_date_str = fundamental_query_start_date_obj.strftime("%Y-%m-%d")
        fundamental_query_end_date_str = user_end_date_obj.strftime("%Y-%m-%d")

        for ticker_symbol in tickers:
            try:
                logger.debug(f"[AdvQuerySrv.FCF_MARGIN] Processing Ticker: {ticker_symbol}")

                # Fetch Quarterly FCF components
                quarterly_cf_statements_raw = await self.base_query_srv.db_repo.get_data_items_by_criteria(
                    ticker=ticker_symbol, item_type="CASH_FLOW_STATEMENT", item_time_coverage="QUARTER",
                    start_date=fundamental_query_start_date_obj, end_date=user_end_date_obj, order_by_key_date_desc=False
                )
                quarterly_fcf_points: List[Dict[str, Any]] = []
                for idx, item_data in enumerate(quarterly_cf_statements_raw):
                    payload = item_data.get('item_data_payload')
                    key_date_str = item_data.get('item_key_date')
                    
                    if idx == 0: # Log details for the first item before checks
                        logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_FIRST_Q_CF_PRE_CHECK] Ticker: {ticker_symbol}, Item_Data_Keys: {list(item_data.keys())}, Payload_Type: {type(payload)}, Key_Date_Str: {key_date_str}")

                    if not isinstance(payload, dict) or not key_date_str:
                        if idx == 0: logger.warning(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_FIRST_Q_CF_FAIL_PAYLOAD_OR_DATE_STR] Ticker: {ticker_symbol}, Payload is dict: {isinstance(payload, dict)}, Key_Date_Str exists: {bool(key_date_str)}")
                        continue
                    
                    date_obj = self.base_query_srv._parse_date_flex(key_date_str)
                    if idx == 0: # Log result of date parsing for the first item
                        logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_FIRST_Q_CF_DATE_PARSE] Ticker: {ticker_symbol}, Key_Date_Str: {key_date_str}, Parsed_Date_Obj: {date_obj}")

                    if not date_obj:
                        if idx == 0: logger.warning(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_FIRST_Q_CF_FAIL_DATE_PARSE] Ticker: {ticker_symbol}, Key_Date_Str: {key_date_str} resulted in None date_obj")
                        continue
                    
                    original_payload_fcf_value = payload.get("Free Cash Flow")
                    if idx == 0: # Log for the very first item
                        logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_FIRST_Q_CF_DIRECT_GET] Ticker: {ticker_symbol}, Date: {date_obj.strftime('%Y-%m-%d')}, Direct get 'Free Cash Flow': {original_payload_fcf_value}")

                    fcf_val_after_conversion = payload.get("Free Cash Flow")
                    if idx == 0: # and (rate_to_apply and original_fin_curr and target_trade_curr): # Log if conversion happened (condition modified)
                        logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_FIRST_Q_CF_CONVERTED_GET] Ticker: {ticker_symbol}, Date: {date_obj.strftime('%Y-%m-%d')}, Get 'Free Cash Flow' after (intended no) conversion: {fcf_val_after_conversion}")


                    # Use fcf_val_after_conversion for appending points
                    if fcf_val_after_conversion is not None:
                        try:
                            quarterly_fcf_points.append({"date_obj": date_obj, "value": float(fcf_val_after_conversion)})
                        except (ValueError, TypeError): 
                            if idx < 2 : logger.warning(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_Q_FCF_FLOAT_FAIL] Ticker: {ticker_symbol}, Date: {date_obj.strftime('%Y-%m-%d')}, Value '{fcf_val_after_conversion}' could not be converted to float.")
                            pass # Silently skip if conversion to float fails
                quarterly_fcf_points.sort(key=lambda x: x['date_obj'])
                # DEBUG: Log populated quarterly_fcf_points
                if quarterly_fcf_points:
                    logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_POP_Q_FCF] Ticker: {ticker_symbol}, First 2 Q_FCF points: {quarterly_fcf_points[:2]}")
                else:
                    logger.warning(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_POP_Q_FCF] Ticker: {ticker_symbol}, No quarterly FCF points populated.")

                # Fetch Annual FCF components (for fallback)
                annual_cf_statements_raw = await self.base_query_srv.db_repo.get_data_items_by_criteria(
                    ticker=ticker_symbol, item_type="CASH_FLOW_STATEMENT", item_time_coverage="FYEAR",
                    start_date=fundamental_query_start_date_obj, end_date=user_end_date_obj, order_by_key_date_desc=False
                )
                annual_fcf_points: List[Dict[str, Any]] = []
                for item_data in annual_cf_statements_raw:
                    payload = item_data.get('item_data_payload')
                    key_date_str = item_data.get('item_key_date')
                    if not isinstance(payload, dict) or not key_date_str: continue
                    date_obj = self.base_query_srv._parse_date_flex(key_date_str)
                    if not date_obj: continue

                    # DEBUG: Log raw FCF before conversion for annual
                    raw_fcf_a = payload.get("Free Cash Flow")
                    if raw_fcf_a is not None and len(annual_fcf_points) < 1: # Log first one
                        logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_RAW_A_FCF] Ticker: {ticker_symbol}, Date: {date_obj.strftime('%Y-%m-%d')}, Raw Annual FCF: {raw_fcf_a}")

                    fcf_val = payload.get("Free Cash Flow")
                    # DEBUG: Log FCF after conversion for annual
                    if fcf_val is not None and len(annual_fcf_points) < 1:
                        logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_CONV_A_FCF] Ticker: {ticker_symbol}, Date: {date_obj.strftime('%Y-%m-%d')}, Converted Annual FCF: {fcf_val}")
                    
                    if fcf_val is not None:
                        try:
                            annual_fcf_points.append({"date_obj": date_obj, "value": float(fcf_val)})
                        except (ValueError, TypeError): pass
                annual_fcf_points.sort(key=lambda x: x['date_obj'])
                # DEBUG: Log populated annual_fcf_points
                if annual_fcf_points:
                    logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_POP_A_FCF] Ticker: {ticker_symbol}, First A_FCF point: {annual_fcf_points[:1]}")
                else:
                    logger.warning(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_POP_A_FCF] Ticker: {ticker_symbol}, No annual FCF points populated.")
                
                # Fetch Quarterly Revenue
                quarterly_is_statements_raw = await self.base_query_srv.db_repo.get_data_items_by_criteria(
                    ticker=ticker_symbol, item_type="INCOME_STATEMENT", item_time_coverage="QUARTER",
                    start_date=fundamental_query_start_date_obj, end_date=user_end_date_obj, order_by_key_date_desc=False
                )
                quarterly_revenue_points: List[Dict[str, Any]] = []
                for idx, item_data in enumerate(quarterly_is_statements_raw):
                    payload = item_data.get('item_data_payload')
                    key_date_str = item_data.get('item_key_date')

                    if idx == 0: # Log details for the first item before checks
                        logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_FIRST_Q_IS_PRE_CHECK] Ticker: {ticker_symbol}, Item_Data_Keys: {list(item_data.keys())}, Payload_Type: {type(payload)}, Key_Date_Str: {key_date_str}")

                    if not isinstance(payload, dict) or not key_date_str:
                        if idx == 0: logger.warning(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_FIRST_Q_IS_FAIL_PAYLOAD_OR_DATE_STR] Ticker: {ticker_symbol}, Payload is dict: {isinstance(payload, dict)}, Key_Date_Str exists: {bool(key_date_str)}")
                        continue
                    
                    date_obj = self.base_query_srv._parse_date_flex(key_date_str)
                    if idx == 0: # Log result of date parsing for the first item
                        logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_FIRST_Q_IS_DATE_PARSE] Ticker: {ticker_symbol}, Key_Date_Str: {key_date_str}, Parsed_Date_Obj: {date_obj}")

                    if not date_obj:
                        if idx == 0: logger.warning(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_FIRST_Q_IS_FAIL_DATE_PARSE] Ticker: {ticker_symbol}, Key_Date_Str: {key_date_str} resulted in None date_obj")
                        continue

                    original_payload_revenue_value = payload.get("Total Revenue")
                    if idx == 0: # Log for the very first item
                        logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_FIRST_Q_IS_DIRECT_GET] Ticker: {ticker_symbol}, Date: {date_obj.strftime('%Y-%m-%d')}, Direct get 'Total Revenue': {original_payload_revenue_value}")
                    
                    revenue_after_conversion = payload.get("Total Revenue")
                    if idx == 0: # and (rate_to_apply and original_fin_curr and target_trade_curr): # Log if conversion happened (condition modified)
                        logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_FIRST_Q_IS_CONVERTED_GET] Ticker: {ticker_symbol}, Date: {date_obj.strftime('%Y-%m-%d')}, Get 'Total Revenue' after (intended no) conversion: {revenue_after_conversion}")
                    
                    if revenue_after_conversion is not None:
                        try: quarterly_revenue_points.append({"date_obj": date_obj, "value": float(revenue_after_conversion)})
                        except (ValueError, TypeError):
                            if idx < 2 : logger.warning(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_Q_REV_FLOAT_FAIL] Ticker: {ticker_symbol}, Date: {date_obj.strftime('%Y-%m-%d')}, Value '{revenue_after_conversion}' could not be converted to float.")
                            pass # Silently skip
                quarterly_revenue_points.sort(key=lambda x: x['date_obj'])
                # DEBUG: Log populated quarterly_revenue_points
                if quarterly_revenue_points:
                    logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_POP_Q_REV] Ticker: {ticker_symbol}, First 2 Q_REV points: {quarterly_revenue_points[:2]}")
                else:
                    logger.warning(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_POP_Q_REV] Ticker: {ticker_symbol}, No quarterly Revenue points populated.")

                # Fetch Annual Revenue (for fallback)
                annual_is_statements_raw = await self.base_query_srv.db_repo.get_data_items_by_criteria(
                    ticker=ticker_symbol, item_type="INCOME_STATEMENT", item_time_coverage="FYEAR",
                    start_date=fundamental_query_start_date_obj, end_date=user_end_date_obj, order_by_key_date_desc=False
                )
                annual_revenue_points: List[Dict[str, Any]] = []
                for item_data in annual_is_statements_raw:
                    payload = item_data.get('item_data_payload')
                    key_date_str = item_data.get('item_key_date')
                    if not isinstance(payload, dict) or not key_date_str: continue
                    date_obj = self.base_query_srv._parse_date_flex(key_date_str)
                    if not date_obj: continue

                    # DEBUG: Log raw Revenue before conversion for annual
                    raw_rev_a = payload.get("Total Revenue")
                    if raw_rev_a is not None and len(annual_revenue_points) < 1:
                        logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_RAW_A_REV] Ticker: {ticker_symbol}, Date: {date_obj.strftime('%Y-%m-%d')}, Raw Annual Revenue: {raw_rev_a}")

                    revenue = payload.get("Total Revenue")
                     # DEBUG: Log Revenue after conversion for annual
                    if revenue is not None and len(annual_revenue_points) < 1:
                        logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_CONV_A_REV] Ticker: {ticker_symbol}, Date: {date_obj.strftime('%Y-%m-%d')}, Converted Annual Revenue: {revenue}")

                    if revenue is not None:
                        try: annual_revenue_points.append({"date_obj": date_obj, "value": float(revenue)})
                        except (ValueError, TypeError): pass
                annual_revenue_points.sort(key=lambda x: x['date_obj'])
                # DEBUG: Log populated annual_revenue_points
                if annual_revenue_points:
                    logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_POP_A_REV] Ticker: {ticker_symbol}, First A_REV point: {annual_revenue_points[:1]}")
                else:
                    logger.warning(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_POP_A_REV] Ticker: {ticker_symbol}, No annual Revenue points populated.")

                # Generate daily TTM series and Calculate FCF Margin
                final_margin_series: List[Dict[str, Any]] = []
                if not user_start_date_obj or not user_end_date_obj : # Should not happen due to earlier checks
                    results_by_ticker[ticker_symbol] = []
                    continue

                current_eval_date = user_start_date_obj
                log_count = 0 # DEBUG: Counter for TTM value logs
                while current_eval_date <= user_end_date_obj:
                    # TTM FCF calculation
                    ttm_fcf_value = self.base_query_srv._calculate_ttm_value_generic(
                        current_eval_date, 
                        quarterly_fcf_points, 
                        annual_fcf_points,
                        "value",
                        debug_identifier=f"TTM_FCF_FOR_FCF_MARGIN_{ticker_symbol}"
                    )
                    # TTM Revenue calculation
                    ttm_revenue_value = self.base_query_srv._calculate_ttm_value_generic(
                        current_eval_date, 
                        quarterly_revenue_points, 
                        annual_revenue_points,
                        "value",
                        debug_identifier=f"TTM_REVENUE_FOR_FCF_MARGIN_{ticker_symbol}"
                    )

                    # DEBUG: Log TTM values
                    if log_count < 5: # Log first 5 TTM calculations
                        logger.debug(f"[AdvQuerySrv.FCF_MARGIN_DEBUG_TTM_VALS] Ticker: {ticker_symbol}, EvalDate: {current_eval_date.strftime('%Y-%m-%d')}, TTM_FCF: {ttm_fcf_value}, TTM_Revenue: {ttm_revenue_value}")
                        log_count +=1

                    fcf_margin: Optional[float] = None
                    if ttm_fcf_value is not None and ttm_revenue_value is not None and ttm_revenue_value != 0:
                        fcf_margin = (ttm_fcf_value / ttm_revenue_value) * 100
                    
                    final_margin_series.append({
                        "date": current_eval_date.strftime("%Y-%m-%d"),
                        "value": fcf_margin
                    })
                    current_eval_date += timedelta(days=1)
                
                results_by_ticker[ticker_symbol] = final_margin_series
                logger.info(f"[AdvQuerySrv.FCF_MARGIN] Ticker {ticker_symbol}: Generated {len(final_margin_series)} margin points.")

            except Exception as e:
                logger.error(f"[AdvQuerySrv.FCF_MARGIN] Error processing ticker {ticker_symbol} for FCF Margin: {e}", exc_info=True)
                results_by_ticker[ticker_symbol] = []
        return results_by_ticker

    # Removed duplicated helper methods, assuming they are accessible via self.base_query_srv
    # e.g., self.base_query_srv._parse_date_flex(...)
    # e.g., self.base_query_srv._get_conversion_info_for_ticker(...)
    # e.g., self.base_query_srv._apply_currency_conversion_to_payload(...)
    # e.g., self.base_query_srv.get_specific_field_timeseries(...)
    # e.g., self.base_query_srv._get_quarterly_shares_series(...)
    # e.g., self.base_query_srv._get_annual_shares_series(...)

    async def calculate_gross_margin_ttm(
        self,
        tickers: List[str],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        logger.info(f"[AdvQuerySrv.calculate_gross_margin_ttm] Request for Tickers: {tickers}, Start: {start_date_str}, End: {end_date_str}")
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        
        ticker_profiles_cache: Dict[str, Dict[str, Any]] = {}

        user_start_date_obj: Optional[datetime] = None
        user_end_date_obj: Optional[datetime] = None
        today = datetime.today()

        if start_date_str:
            user_start_date_obj = self.base_query_srv._parse_date_flex(start_date_str)
        else:
            user_start_date_obj = today.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        
        if end_date_str:
            user_end_date_obj = self.base_query_srv._parse_date_flex(end_date_str)
        else:
            user_end_date_obj = today.replace(hour=23, minute=59, second=59, microsecond=999999)

        if not user_start_date_obj or not user_end_date_obj:
            logger.error("[AdvQuerySrv.calculate_gross_margin_ttm] Failed to parse start or end dates.")
            return results_by_ticker

        # Look back further for fundamental data to ensure enough history for TTM calculation
        fundamental_query_start_date_obj = user_start_date_obj - timedelta(days=5*365) 
        # fundamental_query_start_date_str = fundamental_query_start_date_obj.strftime("%Y-%m-%d") # Not directly used by get_data_items_by_criteria
        # fundamental_query_end_date_str = user_end_date_obj.strftime("%Y-%m-%d") # Not directly used

        for ticker_symbol in tickers:
            try:
                logger.debug(f"[AdvQuerySrv.GROSS_MARGIN] Processing Ticker: {ticker_symbol}")

                # Fetch Quarterly Gross Profit and Total Revenue (from Income Statement)
                quarterly_is_statements_raw = await self.base_query_srv.db_repo.get_data_items_by_criteria(
                    ticker=ticker_symbol, # Ensure this uses singular ticker
                    item_type="INCOME_STATEMENT", 
                    item_time_coverage="QUARTER",
                    start_date=fundamental_query_start_date_obj, 
                    end_date=user_end_date_obj, 
                    order_by_key_date_desc=False
                )
                quarterly_gross_profit_points: List[Dict[str, Any]] = []
                quarterly_total_revenue_points: List[Dict[str, Any]] = []

                for item_data in quarterly_is_statements_raw:
                    payload = item_data.get('item_data_payload')
                    key_date_str = item_data.get('item_key_date')
                    if not isinstance(payload, dict) or not key_date_str: continue
                    date_obj = self.base_query_srv._parse_date_flex(key_date_str)
                    if not date_obj: continue

                    gross_profit_val = payload.get("Gross Profit")
                    total_revenue_val = payload.get("Total Revenue")

                    if gross_profit_val is not None:
                        try: quarterly_gross_profit_points.append({"date_obj": date_obj, "value": float(gross_profit_val)})
                        except (ValueError, TypeError): pass
                    if total_revenue_val is not None:
                        try: quarterly_total_revenue_points.append({"date_obj": date_obj, "value": float(total_revenue_val)})
                        except (ValueError, TypeError): pass
                
                quarterly_gross_profit_points.sort(key=lambda x: x['date_obj'])
                quarterly_total_revenue_points.sort(key=lambda x: x['date_obj'])
                logger.debug(f"[AdvQuerySrv.GROSS_MARGIN_DEBUG_POP_Q_GP] Ticker: {ticker_symbol}, First 2 Q_GP points: {quarterly_gross_profit_points[:2]}")
                logger.debug(f"[AdvQuerySrv.GROSS_MARGIN_DEBUG_POP_Q_REV] Ticker: {ticker_symbol}, First 2 Q_REV points: {quarterly_total_revenue_points[:2]}")


                # Fetch Annual Gross Profit and Total Revenue (from Income Statement)
                annual_is_statements_raw = await self.base_query_srv.db_repo.get_data_items_by_criteria(
                    ticker=ticker_symbol, # Ensure this uses singular ticker
                    item_type="INCOME_STATEMENT", 
                    item_time_coverage="FYEAR",
                    start_date=fundamental_query_start_date_obj, 
                    end_date=user_end_date_obj, 
                    order_by_key_date_desc=False
                )
                annual_gross_profit_points: List[Dict[str, Any]] = []
                annual_total_revenue_points: List[Dict[str, Any]] = []

                for item_data in annual_is_statements_raw:
                    payload = item_data.get('item_data_payload')
                    key_date_str = item_data.get('item_key_date')
                    if not isinstance(payload, dict) or not key_date_str: continue
                    date_obj = self.base_query_srv._parse_date_flex(key_date_str)
                    if not date_obj: continue

                    gross_profit_val = payload.get("Gross Profit")
                    total_revenue_val = payload.get("Total Revenue")

                    if gross_profit_val is not None:
                        try: annual_gross_profit_points.append({"date_obj": date_obj, "value": float(gross_profit_val)})
                        except (ValueError, TypeError): pass
                    if total_revenue_val is not None:
                        try: annual_total_revenue_points.append({"date_obj": date_obj, "value": float(total_revenue_val)})
                        except (ValueError, TypeError): pass
                
                annual_gross_profit_points.sort(key=lambda x: x['date_obj'])
                annual_total_revenue_points.sort(key=lambda x: x['date_obj'])
                logger.debug(f"[AdvQuerySrv.GROSS_MARGIN_DEBUG_POP_A_GP] Ticker: {ticker_symbol}, First A_GP point: {annual_gross_profit_points[:1]}")
                logger.debug(f"[AdvQuerySrv.GROSS_MARGIN_DEBUG_POP_A_REV] Ticker: {ticker_symbol}, First A_REV point: {annual_total_revenue_points[:1]}")

                # Generate daily TTM series and Calculate Gross Margin
                final_margin_series: List[Dict[str, Any]] = []
                current_eval_date = user_start_date_obj
                log_count = 0

                while current_eval_date <= user_end_date_obj:
                    ttm_gross_profit = self.base_query_srv._calculate_ttm_value_generic(
                        current_eval_date, quarterly_gross_profit_points, annual_gross_profit_points, "value",
                        debug_identifier=f"TTM_GP_FOR_GROSS_MARGIN_{ticker_symbol}"
                    )
                    ttm_total_revenue = self.base_query_srv._calculate_ttm_value_generic(
                        current_eval_date, quarterly_total_revenue_points, annual_total_revenue_points, "value",
                        debug_identifier=f"TTM_REVENUE_FOR_GROSS_MARGIN_{ticker_symbol}"
                    )

                    if log_count < 5: # Log first 5 TTM calculations
                        logger.debug(f"[AdvQuerySrv.GROSS_MARGIN_DEBUG_TTM_VALS] Ticker: {ticker_symbol}, EvalDate: {current_eval_date.strftime('%Y-%m-%d')}, TTM_GP: {ttm_gross_profit}, TTM_Revenue: {ttm_total_revenue}")
                        log_count +=1
                    
                    gross_margin: Optional[float] = None
                    if ttm_gross_profit is not None and ttm_total_revenue is not None and ttm_total_revenue != 0:
                        gross_margin = (ttm_gross_profit / ttm_total_revenue) * 100
                    
                    final_margin_series.append({
                        "date": current_eval_date.strftime("%Y-%m-%d"),
                        "value": gross_margin
                    })
                    current_eval_date += timedelta(days=1)
                
                results_by_ticker[ticker_symbol] = final_margin_series
                logger.info(f"[AdvQuerySrv.GROSS_MARGIN] Ticker {ticker_symbol}: Generated {len(final_margin_series)} gross margin points.")

            except Exception as e:
                logger.error(f"[AdvQuerySrv.GROSS_MARGIN] Error processing ticker {ticker_symbol} for Gross Margin: {e}", exc_info=True)
                results_by_ticker[ticker_symbol] = []
        return results_by_ticker

    async def calculate_operating_margin_ttm(
        self,
        tickers: List[str],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        logger.info(f"[AdvQuerySrv.calculate_operating_margin_ttm] Request for Tickers: {tickers}, Start: {start_date_str}, End: {end_date_str}")
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}

        user_start_date_obj: Optional[datetime] = None
        user_end_date_obj: Optional[datetime] = None
        today = datetime.today()

        if start_date_str:
            user_start_date_obj = self.base_query_srv._parse_date_flex(start_date_str)
        else:
            user_start_date_obj = today.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        
        if end_date_str:
            user_end_date_obj = self.base_query_srv._parse_date_flex(end_date_str)
        else:
            user_end_date_obj = today.replace(hour=23, minute=59, second=59, microsecond=999999)

        if not user_start_date_obj or not user_end_date_obj:
            logger.error("[AdvQuerySrv.calculate_operating_margin_ttm] Failed to parse start or end dates.")
            return results_by_ticker

        fundamental_query_start_date_obj = user_start_date_obj - timedelta(days=5*365)

        for ticker_symbol in tickers:
            try:
                logger.debug(f"[AdvQuerySrv.OPERATING_MARGIN] Processing Ticker: {ticker_symbol}")

                # Fetch Quarterly Operating Income and Total Revenue (from Income Statement)
                quarterly_is_statements_raw = await self.base_query_srv.db_repo.get_data_items_by_criteria(
                    ticker=ticker_symbol, # Ensure this uses singular ticker
                    item_type="INCOME_STATEMENT", 
                    item_time_coverage="QUARTER",
                    start_date=fundamental_query_start_date_obj, 
                    end_date=user_end_date_obj, 
                    order_by_key_date_desc=False
                )
                quarterly_operating_income_points: List[Dict[str, Any]] = []
                quarterly_total_revenue_points: List[Dict[str, Any]] = [] # Reusing from gross margin, can be defined once if refactored

                for item_data in quarterly_is_statements_raw:
                    payload = item_data.get('item_data_payload')
                    key_date_str = item_data.get('item_key_date')
                    if not isinstance(payload, dict) or not key_date_str: continue
                    date_obj = self.base_query_srv._parse_date_flex(key_date_str)
                    if not date_obj: continue
                    
                    operating_income_val = payload.get("Operating Income")
                    total_revenue_val = payload.get("Total Revenue")

                    if operating_income_val is not None:
                        try: quarterly_operating_income_points.append({"date_obj": date_obj, "value": float(operating_income_val)})
                        except (ValueError, TypeError): pass
                    if total_revenue_val is not None:
                        try: quarterly_total_revenue_points.append({"date_obj": date_obj, "value": float(total_revenue_val)})
                        except (ValueError, TypeError): pass
                
                quarterly_operating_income_points.sort(key=lambda x: x['date_obj'])
                quarterly_total_revenue_points.sort(key=lambda x: x['date_obj'])
                logger.debug(f"[AdvQuerySrv.OPERATING_MARGIN_DEBUG_POP_Q_OI] Ticker: {ticker_symbol}, First 2 Q_OI points: {quarterly_operating_income_points[:2]}")
                logger.debug(f"[AdvQuerySrv.OPERATING_MARGIN_DEBUG_POP_Q_REV] Ticker: {ticker_symbol}, First 2 Q_REV points: {quarterly_total_revenue_points[:2]}")

                # Fetch Annual Operating Income and Total Revenue (from Income Statement)
                annual_is_statements_raw = await self.base_query_srv.db_repo.get_data_items_by_criteria(
                    ticker=ticker_symbol, # Ensure this uses singular ticker
                    item_type="INCOME_STATEMENT", 
                    item_time_coverage="FYEAR",
                    start_date=fundamental_query_start_date_obj, 
                    end_date=user_end_date_obj, 
                    order_by_key_date_desc=False
                )
                annual_operating_income_points: List[Dict[str, Any]] = []
                annual_total_revenue_points: List[Dict[str, Any]] = [] # Reusing

                for item_data in annual_is_statements_raw:
                    payload = item_data.get('item_data_payload')
                    key_date_str = item_data.get('item_key_date')
                    if not isinstance(payload, dict) or not key_date_str: continue
                    date_obj = self.base_query_srv._parse_date_flex(key_date_str)
                    if not date_obj: continue

                    operating_income_val = payload.get("Operating Income")
                    total_revenue_val = payload.get("Total Revenue")

                    if operating_income_val is not None:
                        try: annual_operating_income_points.append({"date_obj": date_obj, "value": float(operating_income_val)})
                        except (ValueError, TypeError): pass
                    if total_revenue_val is not None:
                        try: annual_total_revenue_points.append({"date_obj": date_obj, "value": float(total_revenue_val)})
                        except (ValueError, TypeError): pass
                
                annual_operating_income_points.sort(key=lambda x: x['date_obj'])
                annual_total_revenue_points.sort(key=lambda x: x['date_obj'])
                logger.debug(f"[AdvQuerySrv.OPERATING_MARGIN_DEBUG_POP_A_OI] Ticker: {ticker_symbol}, First A_OI point: {annual_operating_income_points[:1]}")
                logger.debug(f"[AdvQuerySrv.OPERATING_MARGIN_DEBUG_POP_A_REV] Ticker: {ticker_symbol}, First A_REV point: {annual_total_revenue_points[:1]}")

                final_margin_series: List[Dict[str, Any]] = []
                current_eval_date = user_start_date_obj
                log_count = 0

                while current_eval_date <= user_end_date_obj:
                    ttm_operating_income = self.base_query_srv._calculate_ttm_value_generic(
                        current_eval_date, quarterly_operating_income_points, annual_operating_income_points, "value",
                        debug_identifier=f"TTM_OI_FOR_OPER_MARGIN_{ticker_symbol}"
                    )
                    ttm_total_revenue = self.base_query_srv._calculate_ttm_value_generic(
                        current_eval_date, quarterly_total_revenue_points, annual_total_revenue_points, "value",
                        debug_identifier=f"TTM_REVENUE_FOR_OPER_MARGIN_{ticker_symbol}"
                    )

                    if log_count < 5:
                        logger.debug(f"[AdvQuerySrv.OPERATING_MARGIN_DEBUG_TTM_VALS] Ticker: {ticker_symbol}, EvalDate: {current_eval_date.strftime('%Y-%m-%d')}, TTM_OI: {ttm_operating_income}, TTM_Revenue: {ttm_total_revenue}")
                        log_count +=1
                    
                    operating_margin: Optional[float] = None
                    if ttm_operating_income is not None and ttm_total_revenue is not None and ttm_total_revenue != 0:
                        operating_margin = (ttm_operating_income / ttm_total_revenue) * 100
                    
                    final_margin_series.append({
                        "date": current_eval_date.strftime("%Y-%m-%d"),
                        "value": operating_margin
                    })
                    current_eval_date += timedelta(days=1)
                
                results_by_ticker[ticker_symbol] = final_margin_series
                logger.info(f"[AdvQuerySrv.OPERATING_MARGIN] Ticker {ticker_symbol}: Generated {len(final_margin_series)} operating margin points.")

            except Exception as e:
                logger.error(f"[AdvQuerySrv.OPERATING_MARGIN] Error processing ticker {ticker_symbol} for Operating Margin: {e}", exc_info=True)
                results_by_ticker[ticker_symbol] = []
        return results_by_ticker

    async def calculate_net_profit_margin_ttm(
        self,
        tickers: List[str],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        logger.info(f"[AdvQuerySrv.calculate_net_profit_margin_ttm] Request for Tickers: {tickers}, Start: {start_date_str}, End: {end_date_str}")
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}

        user_start_date_obj: Optional[datetime] = None
        user_end_date_obj: Optional[datetime] = None
        today = datetime.today()

        if start_date_str:
            user_start_date_obj = self.base_query_srv._parse_date_flex(start_date_str)
        else:
            user_start_date_obj = today.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        
        if end_date_str:
            user_end_date_obj = self.base_query_srv._parse_date_flex(end_date_str)
        else:
            user_end_date_obj = today.replace(hour=23, minute=59, second=59, microsecond=999999)

        if not user_start_date_obj or not user_end_date_obj:
            logger.error("[AdvQuerySrv.calculate_net_profit_margin_ttm] Failed to parse start or end dates.")
            return results_by_ticker

        fundamental_query_start_date_obj = user_start_date_obj - timedelta(days=5*365)

        for ticker_symbol in tickers:
            try:
                logger.debug(f"[AdvQuerySrv.NET_PROFIT_MARGIN] Processing Ticker: {ticker_symbol}")

                # Fetch Quarterly Net Income and Total Revenue (from Income Statement)
                quarterly_is_statements_raw = await self.base_query_srv.db_repo.get_data_items_by_criteria(
                    ticker=ticker_symbol, 
                    item_type="INCOME_STATEMENT", 
                    item_time_coverage="QUARTER",
                    start_date=fundamental_query_start_date_obj, 
                    end_date=user_end_date_obj, 
                    order_by_key_date_desc=False
                )
                quarterly_net_income_points: List[Dict[str, Any]] = []
                quarterly_total_revenue_points: List[Dict[str, Any]] = []

                for item_data in quarterly_is_statements_raw:
                    payload = item_data.get('item_data_payload')
                    key_date_str = item_data.get('item_key_date')
                    if not isinstance(payload, dict) or not key_date_str: continue
                    date_obj = self.base_query_srv._parse_date_flex(key_date_str)
                    if not date_obj: continue
                    
                    # Prioritize "Net Income" (with space) as per user instruction
                    net_income_val = payload.get("Net Income") 
                    if net_income_val is None: # Fallback if "Net Income" is not found
                        net_income_val = payload.get("NetIncome")

                    total_revenue_val = payload.get("Total Revenue")

                    if net_income_val is not None:
                        try: quarterly_net_income_points.append({"date_obj": date_obj, "value": float(net_income_val)})
                        except (ValueError, TypeError): pass
                    if total_revenue_val is not None:
                        try: quarterly_total_revenue_points.append({"date_obj": date_obj, "value": float(total_revenue_val)})
                        except (ValueError, TypeError): pass
                
                quarterly_net_income_points.sort(key=lambda x: x['date_obj'])
                quarterly_total_revenue_points.sort(key=lambda x: x['date_obj'])
                logger.debug(f"[AdvQuerySrv.NET_PROFIT_MARGIN_DEBUG_POP_Q_NI] Ticker: {ticker_symbol}, First 2 Q_NI points: {quarterly_net_income_points[:2]}")
                logger.debug(f"[AdvQuerySrv.NET_PROFIT_MARGIN_DEBUG_POP_Q_REV] Ticker: {ticker_symbol}, First 2 Q_REV points: {quarterly_total_revenue_points[:2]}")

                # Fetch Annual Net Income and Total Revenue (from Income Statement)
                annual_is_statements_raw = await self.base_query_srv.db_repo.get_data_items_by_criteria(
                    ticker=ticker_symbol, 
                    item_type="INCOME_STATEMENT", 
                    item_time_coverage="FYEAR",
                    start_date=fundamental_query_start_date_obj, 
                    end_date=user_end_date_obj, 
                    order_by_key_date_desc=False
                )
                annual_net_income_points: List[Dict[str, Any]] = []
                annual_total_revenue_points: List[Dict[str, Any]] = []

                for item_data in annual_is_statements_raw:
                    payload = item_data.get('item_data_payload')
                    key_date_str = item_data.get('item_key_date')
                    if not isinstance(payload, dict) or not key_date_str: continue
                    date_obj = self.base_query_srv._parse_date_flex(key_date_str)
                    if not date_obj: continue

                    # Prioritize "Net Income" (with space)
                    net_income_val = payload.get("Net Income") 
                    if net_income_val is None: # Fallback
                        net_income_val = payload.get("NetIncome")
                    total_revenue_val = payload.get("Total Revenue")

                    if net_income_val is not None:
                        try: annual_net_income_points.append({"date_obj": date_obj, "value": float(net_income_val)})
                        except (ValueError, TypeError): pass
                    if total_revenue_val is not None:
                        try: annual_total_revenue_points.append({"date_obj": date_obj, "value": float(total_revenue_val)})
                        except (ValueError, TypeError): pass
                
                annual_net_income_points.sort(key=lambda x: x['date_obj'])
                annual_total_revenue_points.sort(key=lambda x: x['date_obj'])
                logger.debug(f"[AdvQuerySrv.NET_PROFIT_MARGIN_DEBUG_POP_A_NI] Ticker: {ticker_symbol}, First A_NI point: {annual_net_income_points[:1]}")
                logger.debug(f"[AdvQuerySrv.NET_PROFIT_MARGIN_DEBUG_POP_A_REV] Ticker: {ticker_symbol}, First A_REV point: {annual_total_revenue_points[:1]}")

                final_margin_series: List[Dict[str, Any]] = []
                current_eval_date = user_start_date_obj
                log_count = 0

                while current_eval_date <= user_end_date_obj:
                    ttm_net_income = self.base_query_srv._calculate_ttm_value_generic(
                        current_eval_date, quarterly_net_income_points, annual_net_income_points, "value",
                        debug_identifier=f"TTM_NI_FOR_NET_MARGIN_{ticker_symbol}"
                    )
                    ttm_total_revenue = self.base_query_srv._calculate_ttm_value_generic(
                        current_eval_date, quarterly_total_revenue_points, annual_total_revenue_points, "value",
                        debug_identifier=f"TTM_REVENUE_FOR_NET_MARGIN_{ticker_symbol}"
                    )

                    if log_count < 5:
                        logger.debug(f"[AdvQuerySrv.NET_PROFIT_MARGIN_DEBUG_TTM_VALS] Ticker: {ticker_symbol}, EvalDate: {current_eval_date.strftime('%Y-%m-%d')}, TTM_NI: {ttm_net_income}, TTM_Revenue: {ttm_total_revenue}")
                        log_count +=1
                    
                    net_profit_margin: Optional[float] = None
                    if ttm_net_income is not None and ttm_total_revenue is not None and ttm_total_revenue != 0:
                        net_profit_margin = (ttm_net_income / ttm_total_revenue) * 100
                    
                    final_margin_series.append({
                        "date": current_eval_date.strftime("%Y-%m-%d"),
                        "value": net_profit_margin # Will be None if conditions not met
                    })
                    current_eval_date += timedelta(days=1)
                
                results_by_ticker[ticker_symbol] = final_margin_series
                logger.info(f"[AdvQuerySrv.NET_PROFIT_MARGIN] Ticker {ticker_symbol}: Generated {len(final_margin_series)} net profit margin points.")

            except Exception as e:
                logger.error(f"[AdvQuerySrv.NET_PROFIT_MARGIN] Error processing ticker {ticker_symbol} for Net Profit Margin: {e}", exc_info=True)
                results_by_ticker[ticker_symbol] = [] # Return empty list on error for this ticker
        
        logger.info(f"[AdvQuerySrv.calculate_net_profit_margin_ttm] Completed for {len(tickers)} tickers.")
        return results_by_ticker

    async def _get_applicable_shares_for_date(
        self,
        eval_date: datetime,
        quarterly_shares_series: List[Dict[str, Any]],
        annual_shares_series: List[Dict[str, Any]],
        debug_ticker_symbol: str = "UNKNOWN_TICKER" # Added for logging
    ) -> Optional[float]:
        """
        Helper to find the latest shares outstanding figure for a given evaluation date.
        Combines quarterly and annual, prefers the latest point on or before eval_date.
        """
        # Combine and sort all available shares data by date
        all_shares_data = []
        if quarterly_shares_series:
            all_shares_data.extend(quarterly_shares_series)
        if annual_shares_series:
            all_shares_data.extend(annual_shares_series) # annual_shares_series from _get_annual_shares_series is already good format
        
        if not all_shares_data:
            logger.warning(f"[SHARES_FOR_DATE_HELPER] Ticker: {debug_ticker_symbol}, EvalDate: {eval_date.strftime('%Y-%m-%d')}, No quarterly or annual shares data provided to select from.")
            return None

        # Sort by date_obj descending to easily find the latest applicable
        all_shares_data.sort(key=lambda x: x['date_obj'], reverse=True)

        logger.debug(f"[SHARES_FOR_DATE_HELPER] Ticker: {debug_ticker_symbol}, EvalDate: {eval_date.strftime('%Y-%m-%d')}, Searching in {len(all_shares_data)} combined shares points. First few (most recent): {[{ 'date': p['date_obj'].strftime('%Y-%m-%d'), 'value': p.get('value')} for p in all_shares_data[:3]]}")

        applicable_shares_value: Optional[float] = None
        selected_point_date: Optional[str] = None

        for point in all_shares_data: # Iterates from most recent due to reverse sort
            point_date_obj = point.get('date_obj')
            point_value = point.get('value')

            if not point_date_obj or point_value is None: # Skip points with missing date or value
                logger.debug(f"[SHARES_FOR_DATE_HELPER] Ticker: {debug_ticker_symbol}, EvalDate: {eval_date.strftime('%Y-%m-%d')}, Skipping invalid shares point: {point}")
                continue

            if point_date_obj <= eval_date:
                try:
                    applicable_shares_value = float(point_value)
                    selected_point_date = point_date_obj.strftime('%Y-%m-%d')
                    logger.debug(f"[SHARES_FOR_DATE_HELPER] Ticker: {debug_ticker_symbol}, EvalDate: {eval_date.strftime('%Y-%m-%d')}, Selected shares: {applicable_shares_value} from date: {selected_point_date}.")
                    break # Found the most recent applicable point
                except (ValueError, TypeError):
                    logger.warning(f"[SHARES_FOR_DATE_HELPER] Ticker: {debug_ticker_symbol}, EvalDate: {eval_date.strftime('%Y-%m-%d')}, Could not convert shares value '{point_value}' from date {point_date_obj.strftime('%Y-%m-%d')} to float.")
                    # Continue to see if an older point is usable, though this point is now skipped
        
        if applicable_shares_value is None:
            logger.warning(f"[SHARES_FOR_DATE_HELPER] Ticker: {debug_ticker_symbol}, EvalDate: {eval_date.strftime('%Y-%m-%d')}, No applicable shares data found on or before this date.")
        elif applicable_shares_value == 0:
            logger.warning(f"[SHARES_FOR_DATE_HELPER] Ticker: {debug_ticker_symbol}, EvalDate: {eval_date.strftime('%Y-%m-%d')}, Applicable shares value is zero from date {selected_point_date}. This will cause division by zero for per-share metrics.")
            # Return 0 here as it is a valid number, calling functions must handle it.

        return applicable_shares_value

    async def calculate_price_to_sales_ttm(
        self,
        tickers: List[str],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        logger.info(f"[AdvQuerySrv.calculate_price_to_sales_ttm] Request for Tickers: {tickers}, Start: {start_date_str}, End: {end_date_str}")
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}

        user_start_date_obj: Optional[datetime] = None
        user_end_date_obj: Optional[datetime] = None
        today = datetime.today()

        if start_date_str:
            user_start_date_obj = self.base_query_srv._parse_date_flex(start_date_str)
        else:
            user_start_date_obj = today.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        
        if end_date_str:
            user_end_date_obj = self.base_query_srv._parse_date_flex(end_date_str)
        else:
            user_end_date_obj = today.replace(hour=23, minute=59, second=59, microsecond=999999)

        if not user_start_date_obj or not user_end_date_obj:
            logger.error("[AdvQuerySrv.calculate_price_to_sales_ttm] Failed to parse start or end dates.")
            return results_by_ticker

        # Look back further for fundamental data
        fundamental_query_start_date_obj = user_start_date_obj - timedelta(days=5*365)

        for ticker_symbol in tickers:
            try:
                # logger.debug(f"[AdvQuerySrv.PRICE_SALES_TTM] Processing Ticker: {ticker_symbol}")
                final_ratio_series: List[Dict[str, Any]] = []

                # 1. Fetch Total Revenue components (Quarterly and Annual)
                quarterly_revenue_data_raw = await self.base_query_srv.get_specific_field_timeseries(
                    field_identifier="yf_item_income_statement_quarterly_TotalRevenue",
                    tickers=[ticker_symbol], 
                    start_date_str=fundamental_query_start_date_obj.strftime("%Y-%m-%d"),
                    end_date_str=user_end_date_obj.strftime("%Y-%m-%d")
                )
                quarterly_total_revenue_points: List[Dict[str, Any]] = []
                if quarterly_revenue_data_raw.get(ticker_symbol):
                    for item in quarterly_revenue_data_raw[ticker_symbol]:
                        date_obj = self.base_query_srv._parse_date_flex(item.get('date'))
                        value = item.get('value')
                        if date_obj and value is not None:
                            try: quarterly_total_revenue_points.append({"date_obj": date_obj, "value": float(value)})
                            except (ValueError, TypeError): 
                                logger.warning(f"[AdvQuerySrv.PRICE_SALES_TTM] Could not convert quarterly revenue value '{value}' to float for {ticker_symbol} on {item.get('date')}")
                quarterly_total_revenue_points.sort(key=lambda x: x['date_obj'])
                # logger.debug(f"[AdvQuerySrv.PRICE_SALES_TTM_DEBUG] Ticker: {ticker_symbol}, Fetched {len(quarterly_total_revenue_points)} quarterly revenue points via get_specific_field_timeseries. First 2: {quarterly_total_revenue_points[:2]}")

                annual_revenue_data_raw = await self.base_query_srv.get_specific_field_timeseries(
                    field_identifier="yf_item_income_statement_annual_TotalRevenue",
                    tickers=[ticker_symbol],
                    start_date_str=fundamental_query_start_date_obj.strftime("%Y-%m-%d"),
                    end_date_str=user_end_date_obj.strftime("%Y-%m-%d")
                )
                annual_total_revenue_points: List[Dict[str, Any]] = []
                if annual_revenue_data_raw.get(ticker_symbol):
                    for item in annual_revenue_data_raw[ticker_symbol]:
                        date_obj = self.base_query_srv._parse_date_flex(item.get('date'))
                        value = item.get('value')
                        if date_obj and value is not None:
                            try: annual_total_revenue_points.append({"date_obj": date_obj, "value": float(value)})
                            except (ValueError, TypeError):
                                logger.warning(f"[AdvQuerySrv.PRICE_SALES_TTM] Could not convert annual revenue value '{value}' to float for {ticker_symbol} on {item.get('date')}")
                annual_total_revenue_points.sort(key=lambda x: x['date_obj'])
                # logger.debug(f"[AdvQuerySrv.PRICE_SALES_TTM_DEBUG] Ticker: {ticker_symbol}, Fetched {len(annual_total_revenue_points)} annual revenue points via get_specific_field_timeseries. First 2: {annual_total_revenue_points[:2]}")

                # 2. Fetch Daily Prices - MODIFIED to match P/E pattern
                price_data = await self.base_query_srv.get_price_history(
                    ticker=ticker_symbol,
                    interval="1d",
                    start_date=user_start_date_obj.strftime("%Y-%m-%d"),
                    end_date=user_end_date_obj.strftime("%Y-%m-%d")
                )

                if not price_data:
                    logger.warning(f"[AdvQuerySrv.PRICE_SALES_TTM] No price data returned for {ticker_symbol} from {user_start_date_obj.strftime('%Y-%m-%d')} to {user_end_date_obj.strftime('%Y-%m-%d')}.")
                    results_by_ticker[ticker_symbol] = []
                    continue

                # logger.debug(f"[AdvQuerySrv.PRICE_SALES_TTM] Received {len(price_data)} price points for {ticker_symbol}.")

                # 3. Fetch Shares Outstanding Series
                q_shares_series = await self.base_query_srv._get_quarterly_shares_series(
                    ticker_symbol, 
                    fundamental_query_start_date_obj.strftime("%Y-%m-%d"), 
                    user_end_date_obj.strftime("%Y-%m-%d")
                )
                a_shares_series = await self.base_query_srv._get_annual_shares_series(
                    ticker_symbol, 
                    fundamental_query_start_date_obj.strftime("%Y-%m-%d"), 
                    user_end_date_obj.strftime("%Y-%m-%d")
                )

                # 4. Process each price point - MODIFIED to match P/E pattern
                debug_log_count = 0
                for price_point in price_data:
                    price_date_str = price_point['Date'].split("T")[0]  # Ensure YYYY-MM-DD format
                    price_value = price_point.get('Close')

                    if price_value is None:
                        continue  # Skip points without price data

                    # Get TTM revenue for this date
                    price_date_obj = self.base_query_srv._parse_date_flex(price_date_str)
                    if not price_date_obj:
                        continue

                    ttm_revenue = self.base_query_srv._calculate_ttm_value_generic(
                        price_date_obj, 
                        quarterly_total_revenue_points, 
                        annual_total_revenue_points, 
                        "value",
                        debug_identifier=f"TTM_REVENUE_FOR_PS_{ticker_symbol}"
                    )

                    # Get shares for this date
                    current_shares = await self._get_applicable_shares_for_date(
                        price_date_obj, 
                        q_shares_series, 
                        a_shares_series,
                        debug_ticker_symbol=ticker_symbol
                    )

                    # Calculate Price/Sales TTM
                    price_to_sales_ttm: Optional[float] = None
                    # if debug_log_count < 5:
                    #     logger.info(
                    #         f"[AdvQuerySrv.PRICE_SALES_TTM_INPUTS_DEBUG] Date: {price_date_str}, "
                    #         f"Price: {price_value}, Shares: {current_shares}, TTM_Revenue: {ttm_revenue}"
                    #     )

                    if current_shares is not None and current_shares != 0 and ttm_revenue is not None and ttm_revenue != 0:
                        market_cap = float(price_value) * current_shares
                        price_to_sales_ttm = market_cap / ttm_revenue

                    # if debug_log_count < 5:
                    #     logger.info(
                    #         f"[AdvQuerySrv.PRICE_SALES_TTM_RESULT_DEBUG] Date: {price_date_str}, "
                    #         f"P/S_TTM_Calculated: {price_to_sales_ttm}"
                    #     )
                    #     debug_log_count += 1

                    final_ratio_series.append({
                        "date": price_date_str,
                        "value": price_to_sales_ttm
                    })

                results_by_ticker[ticker_symbol] = final_ratio_series
                logger.info(f"[AdvQuerySrv.PRICE_SALES_TTM] Ticker {ticker_symbol}: Generated {len(final_ratio_series)} P/S TTM points.")

            except Exception as e:
                logger.error(f"[AdvQuerySrv.PRICE_SALES_TTM] Error processing ticker {ticker_symbol} for P/S TTM: {e}", exc_info=True)
                results_by_ticker[ticker_symbol] = []

        logger.info(f"[AdvQuerySrv.calculate_price_to_sales_ttm] Completed for {len(tickers)} tickers.")
        return results_by_ticker

    async def calculate_debt_to_equity_for_tickers(
        self,
        tickers: List[str],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        results_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        logger.info(f"DEBT_TO_EQUITY: Starting calculation for tickers: {tickers}, start: {start_date_str}, end: {end_date_str}")

        user_start_date_obj = self.base_query_srv._parse_date_flex(start_date_str) if start_date_str else datetime.now() - timedelta(days=365*5) # Default 5 years
        user_end_date_obj = self.base_query_srv._parse_date_flex(end_date_str) if end_date_str else datetime.now()

        fundamental_query_start_date_obj = user_start_date_obj - timedelta(days=180) # Approx 6 months buffer
        fundamental_query_start_date_str = fundamental_query_start_date_obj.strftime('%Y-%m-%d')
        fundamental_query_end_date_str = user_end_date_obj.strftime('%Y-%m-%d')

        # Field IDs for Total Debt and Stockholders Equity
        debt_quarterly_field_id = "yf_item_balance_sheet_quarterly_TotalDebt"
        debt_annual_field_id = "yf_item_balance_sheet_annual_TotalDebt"
        equity_quarterly_field_id = "yf_item_balance_sheet_quarterly_CommonStockEquity"
        equity_annual_field_id = "yf_item_balance_sheet_annual_CommonStockEquity"

        for ticker_symbol in tickers:
            try:
                # 1. Fetch Total Debt Data
                # Fetch Quarterly Debt
                logger.debug(f"DEBT_TO_EQUITY [{ticker_symbol}]: Fetching Q Debt ({debt_quarterly_field_id})")
                debt_q_raw = await self.base_query_srv.get_specific_field_timeseries(
                    field_identifier=debt_quarterly_field_id,
                    tickers=[ticker_symbol],
                    start_date_str=fundamental_query_start_date_str,
                    end_date_str=fundamental_query_end_date_str
                )
                quarterly_debt_points: List[Dict[str, Any]] = []
                if debt_q_raw.get(ticker_symbol):
                    for item in debt_q_raw[ticker_symbol]:
                        date_obj = self.base_query_srv._parse_date_flex(item.get('date'))
                        value = item.get('value')
                        if date_obj and value is not None:
                            try: quarterly_debt_points.append({'date_obj': date_obj, 'value': float(value), 'source': 'quarterly'})
                            except (ValueError, TypeError): pass
                logger.info(f"DEBT_TO_EQUITY [{ticker_symbol}]: Parsed {len(quarterly_debt_points)} quarterly debt points.")

                # Fetch Annual Debt
                logger.debug(f"DEBT_TO_EQUITY [{ticker_symbol}]: Fetching A Debt ({debt_annual_field_id})")
                debt_a_raw = await self.base_query_srv.get_specific_field_timeseries(
                    field_identifier=debt_annual_field_id,
                    tickers=[ticker_symbol],
                    start_date_str=fundamental_query_start_date_str,
                    end_date_str=fundamental_query_end_date_str
                )
                annual_debt_points: List[Dict[str, Any]] = []
                if debt_a_raw.get(ticker_symbol):
                    for item in debt_a_raw[ticker_symbol]:
                        date_obj = self.base_query_srv._parse_date_flex(item.get('date'))
                        value = item.get('value')
                        if date_obj and value is not None:
                            try: annual_debt_points.append({'date_obj': date_obj, 'value': float(value), 'source': 'annual'})
                            except (ValueError, TypeError): pass
                logger.info(f"DEBT_TO_EQUITY [{ticker_symbol}]: Parsed {len(annual_debt_points)} annual debt points.")

                # 2. Fetch Stockholders Equity Data
                # Fetch Quarterly Equity
                logger.debug(f"DEBT_TO_EQUITY [{ticker_symbol}]: Fetching Q Equity ({equity_quarterly_field_id})")
                equity_q_raw = await self.base_query_srv.get_specific_field_timeseries(
                    field_identifier=equity_quarterly_field_id,
                    tickers=[ticker_symbol],
                    start_date_str=fundamental_query_start_date_str,
                    end_date_str=fundamental_query_end_date_str
                )
                quarterly_equity_points: List[Dict[str, Any]] = []
                if equity_q_raw.get(ticker_symbol):
                    for item in equity_q_raw[ticker_symbol]:
                        date_obj = self.base_query_srv._parse_date_flex(item.get('date'))
                        value = item.get('value')
                        if date_obj and value is not None:
                            try: quarterly_equity_points.append({'date_obj': date_obj, 'value': float(value), 'source': 'quarterly'})
                            except (ValueError, TypeError): pass
                logger.info(f"DEBT_TO_EQUITY [{ticker_symbol}]: Parsed {len(quarterly_equity_points)} quarterly equity points.")

                # Fetch Annual Equity
                logger.debug(f"DEBT_TO_EQUITY [{ticker_symbol}]: Fetching A Equity ({equity_annual_field_id})")
                equity_a_raw = await self.base_query_srv.get_specific_field_timeseries(
                    field_identifier=equity_annual_field_id,
                    tickers=[ticker_symbol],
                    start_date_str=fundamental_query_start_date_str,
                    end_date_str=fundamental_query_end_date_str
                )
                annual_equity_points: List[Dict[str, Any]] = []
                if equity_a_raw.get(ticker_symbol):
                    for item in equity_a_raw[ticker_symbol]:
                        date_obj = self.base_query_srv._parse_date_flex(item.get('date'))
                        value = item.get('value')
                        if date_obj and value is not None:
                            try: annual_equity_points.append({'date_obj': date_obj, 'value': float(value), 'source': 'annual'})
                            except (ValueError, TypeError): pass
                logger.info(f"DEBT_TO_EQUITY [{ticker_symbol}]: Parsed {len(annual_equity_points)} annual equity points.")

                # 3. Combine data points, giving preference to quarterly if dates overlap
                combined_debt_map: Dict[datetime, float] = {}
                for point in annual_debt_points:  # Annual first
                    combined_debt_map[point['date_obj']] = point['value']
                for point in quarterly_debt_points:  # Quarterly overwrites if date matches
                    combined_debt_map[point['date_obj']] = point['value']
                
                combined_equity_map: Dict[datetime, float] = {}
                for point in annual_equity_points:  # Annual first
                    combined_equity_map[point['date_obj']] = point['value']
                for point in quarterly_equity_points:  # Quarterly overwrites if date matches
                    combined_equity_map[point['date_obj']] = point['value']

                # Get all unique dates from both maps
                all_dates = sorted(set(combined_debt_map.keys()) & set(combined_equity_map.keys()))
                
                if not all_dates:
                    logger.warning(f"DEBT_TO_EQUITY [{ticker_symbol}]: No overlapping dates between debt and equity data. Skipping.")
                    results_by_ticker[ticker_symbol] = []
                    continue

                # 4. Calculate Debt/Equity ratio for each date
                point_in_time_ratio_series: List[Dict[str, Any]] = []
                for date_obj in all_dates:
                    debt_value = combined_debt_map[date_obj]
                    equity_value = combined_equity_map[date_obj]
                    
                    if equity_value != 0:  # Avoid division by zero
                        try:
                            ratio_value = float(debt_value) / float(equity_value)
                            point_in_time_ratio_series.append({
                                'date_obj': date_obj,
                                'value': ratio_value
                            })
                        except (ValueError, TypeError) as e:
                            logger.warning(f"DEBT_TO_EQUITY [{ticker_symbol}]: Error calculating ratio for date {date_obj}: {e}")
                    else:
                        logger.warning(f"DEBT_TO_EQUITY [{ticker_symbol}]: Equity value is zero for date {date_obj}. Skipping ratio calculation.")

                if not point_in_time_ratio_series:
                    logger.warning(f"DEBT_TO_EQUITY [{ticker_symbol}]: No valid debt/equity ratios calculated. Skipping.")
                    results_by_ticker[ticker_symbol] = []
                    continue

                # 5. Generate daily series
                daily_series: List[Dict[str, Any]] = []
                current_iter_date = user_start_date_obj
                last_known_val = None

                # Find initial value for start of user period
                for point in reversed(point_in_time_ratio_series):
                    if point['date_obj'] <= current_iter_date:
                        last_known_val = point['value']
                        break

                point_idx = 0
                while current_iter_date <= user_end_date_obj:
                    while point_idx < len(point_in_time_ratio_series) and point_in_time_ratio_series[point_idx]['date_obj'] <= current_iter_date:
                        last_known_val = point_in_time_ratio_series[point_idx]['value']
                        point_idx += 1
                    if last_known_val is not None:
                        if point_in_time_ratio_series and current_iter_date >= point_in_time_ratio_series[0]['date_obj']:
                            daily_series.append({
                                'date': current_iter_date.strftime('%Y-%m-%d'),
                                'value': last_known_val
                            })
                    current_iter_date += timedelta(days=1)

                results_by_ticker[ticker_symbol] = daily_series
                logger.info(f"DEBT_TO_EQUITY [{ticker_symbol}]: Successfully generated {len(daily_series)} daily debt/equity ratio points.")

            except Exception as e:
                logger.error(f"DEBT_TO_EQUITY [{ticker_symbol}]: Unhandled error: {e}", exc_info=True)
                results_by_ticker[ticker_symbol] = []

        return results_by_ticker

    async def calculate_total_liabilities_to_equity_for_tickers(
        self,
        tickers: List[str],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate Total Liabilities/Equity ratio timeseries for given tickers.
        Ratio = Total Liabilities Net Minority Interest / Stockholders Equity
        Both values from most recent balance sheet (quarterly or annual).
        """
        logger.info(f"Calculating Total Liabilities/Equity ratio for tickers: {tickers}")
        
        # Initialize results dictionary
        results: Dict[str, List[Dict[str, Any]]] = {ticker: [] for ticker in tickers}
        
        try:
            # Fetch Total Liabilities Net Minority Interest data (quarterly and annual)
            total_liabilities_quarterly = await self.base_query_srv.get_specific_field_timeseries(
                field_identifier="yf_item_balance_sheet_quarterly_TotalLiabilitiesNetMinorityInterest",
                tickers=tickers,
                start_date_str=start_date_str,
                end_date_str=end_date_str
            )
            
            total_liabilities_annual = await self.base_query_srv.get_specific_field_timeseries(
                field_identifier="yf_item_balance_sheet_annual_TotalLiabilitiesNetMinorityInterest",
                tickers=tickers,
                start_date_str=start_date_str,
                end_date_str=end_date_str
            )
            
            # Fetch Stockholders Equity data (quarterly and annual)
            equity_quarterly = await self.base_query_srv.get_specific_field_timeseries(
                field_identifier="yf_item_balance_sheet_quarterly_CommonStockEquity",
                tickers=tickers,
                start_date_str=start_date_str,
                end_date_str=end_date_str
            )
            
            equity_annual = await self.base_query_srv.get_specific_field_timeseries(
                field_identifier="yf_item_balance_sheet_annual_CommonStockEquity",
                tickers=tickers,
                start_date_str=start_date_str,
                end_date_str=end_date_str
            )
            
            # Process each ticker
            for ticker in tickers:
                logger.info(f"Processing Total Liabilities/Equity ratio for {ticker}")
                
                # Combine quarterly and annual data points for both metrics
                # For Total Liabilities
                all_liabilities_points = []
                if ticker in total_liabilities_quarterly:
                    all_liabilities_points.extend([
                        {
                            'date_obj': datetime.fromisoformat(point['date']),
                            'value': point['value'],
                            'is_quarterly': True
                        }
                        for point in total_liabilities_quarterly[ticker]
                    ])
                if ticker in total_liabilities_annual:
                    all_liabilities_points.extend([
                        {
                            'date_obj': datetime.fromisoformat(point['date']),
                            'value': point['value'],
                            'is_quarterly': False
                        }
                        for point in total_liabilities_annual[ticker]
                    ])
                
                # For Equity
                all_equity_points = []
                if ticker in equity_quarterly:
                    all_equity_points.extend([
                        {
                            'date_obj': datetime.fromisoformat(point['date']),
                            'value': point['value'],
                            'is_quarterly': True
                        }
                        for point in equity_quarterly[ticker]
                    ])
                if ticker in equity_annual:
                    all_equity_points.extend([
                        {
                            'date_obj': datetime.fromisoformat(point['date']),
                            'value': point['value'],
                            'is_quarterly': False
                        }
                        for point in equity_annual[ticker]
                    ])
                
                # Sort points by date
                all_liabilities_points.sort(key=lambda x: x['date_obj'])
                all_equity_points.sort(key=lambda x: x['date_obj'])
                
                # Calculate ratio for each date where we have both metrics
                ratio_points = []
                for liab_point in all_liabilities_points:
                    # Find the most recent equity point before or on this date
                    applicable_equity_points = [
                        p for p in all_equity_points 
                        if p['date_obj'] <= liab_point['date_obj']
                    ]
                    
                    if applicable_equity_points:
                        # Get the most recent equity point
                        equity_point = max(applicable_equity_points, key=lambda x: x['date_obj'])
                        
                        # Calculate ratio if equity is not zero
                        if equity_point['value'] != 0:
                            ratio = liab_point['value'] / equity_point['value']
                            ratio_points.append({
                                'date_obj': liab_point['date_obj'],
                                'value': ratio,
                                'is_quarterly': liab_point['is_quarterly']
                            })
                
                # Sort ratio points by date
                ratio_points.sort(key=lambda x: x['date_obj'])
                
                # Generate daily series
                if ratio_points:
                    # Use user's date range instead of ratio points date range
                    user_start_date = self.base_query_srv._parse_date_flex(start_date_str) if start_date_str else datetime.now() - timedelta(days=365*5)
                    user_end_date = self.base_query_srv._parse_date_flex(end_date_str) if end_date_str else datetime.now()
                    
                    # Create daily series
                    current_date = user_start_date
                    last_known_val = None
                    
                    # Find initial value for start of user period
                    for point in reversed(ratio_points):
                        if point['date_obj'] <= current_date:
                            last_known_val = point['value']
                            break
                    
                    point_idx = 0
                    while current_date <= user_end_date:
                        while point_idx < len(ratio_points) and ratio_points[point_idx]['date_obj'] <= current_date:
                            last_known_val = ratio_points[point_idx]['value']
                            point_idx += 1
                        
                        if last_known_val is not None:
                            results[ticker].append({
                                'date': current_date.isoformat(),
                                'value': last_known_val
                            })
                        
                        current_date += timedelta(days=1)
                
                logger.info(f"Generated {len(results[ticker])} daily Total Liabilities/Equity ratio points for {ticker}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error calculating Total Liabilities/Equity ratio: {str(e)}")
            raise

    async def calculate_total_liabilities_to_assets_for_tickers(
        self,
        tickers: List[str],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate Total Liabilities/Total Assets ratio timeseries for given tickers.
        Ratio = Total Liabilities Net Minority Interest / Total Assets
        Both values from most recent balance sheet (quarterly or annual).
        """
        logger.info(f"Calculating Total Liabilities/Total Assets ratio for tickers: {tickers}")
        
        # Initialize results dictionary
        results: Dict[str, List[Dict[str, Any]]] = {ticker: [] for ticker in tickers}
        
        try:
            # Fetch Total Liabilities Net Minority Interest data (quarterly and annual)
            total_liabilities_quarterly = await self.base_query_srv.get_specific_field_timeseries(
                field_identifier="yf_item_balance_sheet_quarterly_TotalLiabilitiesNetMinorityInterest",
                tickers=tickers,
                start_date_str=start_date_str,
                end_date_str=end_date_str
            )
            
            total_liabilities_annual = await self.base_query_srv.get_specific_field_timeseries(
                field_identifier="yf_item_balance_sheet_annual_TotalLiabilitiesNetMinorityInterest",
                tickers=tickers,
                start_date_str=start_date_str,
                end_date_str=end_date_str
            )
            
            # Fetch Total Assets data (quarterly and annual)
            total_assets_quarterly = await self.base_query_srv.get_specific_field_timeseries(
                field_identifier="yf_item_balance_sheet_quarterly_TotalAssets",
                tickers=tickers,
                start_date_str=start_date_str,
                end_date_str=end_date_str
            )
            
            total_assets_annual = await self.base_query_srv.get_specific_field_timeseries(
                field_identifier="yf_item_balance_sheet_annual_TotalAssets",
                tickers=tickers,
                start_date_str=start_date_str,
                end_date_str=end_date_str
            )
            
            # Process each ticker
            for ticker in tickers:
                logger.info(f"Processing Total Liabilities/Total Assets ratio for {ticker}")
                
                # Combine quarterly and annual data points for both metrics
                # For Total Liabilities
                all_liabilities_points = []
                if ticker in total_liabilities_quarterly:
                    all_liabilities_points.extend([
                        {
                            'date_obj': datetime.fromisoformat(point['date']),
                            'value': point['value'],
                            'is_quarterly': True
                        }
                        for point in total_liabilities_quarterly[ticker]
                    ])
                if ticker in total_liabilities_annual:
                    all_liabilities_points.extend([
                        {
                            'date_obj': datetime.fromisoformat(point['date']),
                            'value': point['value'],
                            'is_quarterly': False
                        }
                        for point in total_liabilities_annual[ticker]
                    ])
                
                # For Total Assets
                all_assets_points = []
                if ticker in total_assets_quarterly:
                    all_assets_points.extend([
                        {
                            'date_obj': datetime.fromisoformat(point['date']),
                            'value': point['value'],
                            'is_quarterly': True
                        }
                        for point in total_assets_quarterly[ticker]
                    ])
                if ticker in total_assets_annual:
                    all_assets_points.extend([
                        {
                            'date_obj': datetime.fromisoformat(point['date']),
                            'value': point['value'],
                            'is_quarterly': False
                        }
                        for point in total_assets_annual[ticker]
                    ])
                
                # Sort points by date
                all_liabilities_points.sort(key=lambda x: x['date_obj'])
                all_assets_points.sort(key=lambda x: x['date_obj'])
                
                # Calculate ratio for each date where we have both metrics
                ratio_points = []
                for liab_point in all_liabilities_points:
                    # Find the most recent assets point before or on this date
                    applicable_assets_points = [
                        p for p in all_assets_points 
                        if p['date_obj'] <= liab_point['date_obj']
                    ]
                    
                    if applicable_assets_points:
                        # Get the most recent assets point
                        assets_point = max(applicable_assets_points, key=lambda x: x['date_obj'])
                        
                        # Calculate ratio if assets is not zero
                        if assets_point['value'] != 0:
                            ratio = liab_point['value'] / assets_point['value']
                            ratio_points.append({
                                'date_obj': liab_point['date_obj'],
                                'value': ratio,
                                'is_quarterly': liab_point['is_quarterly']
                            })
                
                # Sort ratio points by date
                ratio_points.sort(key=lambda x: x['date_obj'])
                
                # Generate daily series
                if ratio_points:
                    # Use user's date range instead of ratio points date range
                    user_start_date = self.base_query_srv._parse_date_flex(start_date_str) if start_date_str else datetime.now() - timedelta(days=365*5)
                    user_end_date = self.base_query_srv._parse_date_flex(end_date_str) if end_date_str else datetime.now()
                    
                    # Create daily series
                    current_date = user_start_date
                    last_known_val = None
                    
                    # Find initial value for start of user period
                    for point in reversed(ratio_points):
                        if point['date_obj'] <= current_date:
                            last_known_val = point['value']
                            break
                    
                    point_idx = 0
                    while current_date <= user_end_date:
                        while point_idx < len(ratio_points) and ratio_points[point_idx]['date_obj'] <= current_date:
                            last_known_val = ratio_points[point_idx]['value']
                            point_idx += 1
                        
                        if last_known_val is not None:
                            results[ticker].append({
                                'date': current_date.isoformat(),
                                'value': last_known_val
                            })
                        
                        current_date += timedelta(days=1)
                
                logger.info(f"Generated {len(results[ticker])} daily Total Liabilities/Total Assets ratio points for {ticker}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error calculating Total Liabilities/Total Assets ratio: {str(e)}")
            raise

    async def calculate_debt_to_assets_for_tickers(
        self,
        tickers: List[str],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate the Debt/Total Assets ratio for given tickers.
        Ratio = Total Debt / Total Assets
        Both values come from the most recent balance sheet (quarterly or annual).
        No currency conversion needed since both values are in the same currency.
        """
        logger.info(f"Calculating Debt/Total Assets ratio for tickers: {tickers}")
        results = {}

        # Parse date range using base service's method
        user_start_date = self.base_query_srv._parse_date_flex(start_date_str) if start_date_str else datetime.now() - timedelta(days=5*365)
        user_end_date = self.base_query_srv._parse_date_flex(end_date_str) if end_date_str else datetime.now()

        for ticker in tickers:
            try:
                # Fetch Total Debt data (both quarterly and annual)
                total_debt_quarterly = await self.base_query_srv.get_specific_field_timeseries(
                    field_identifier="yf_item_balance_sheet_quarterly_TotalDebt",
                    tickers=ticker,
                    start_date_str=start_date_str,
                    end_date_str=end_date_str
                )
                total_debt_annual = await self.base_query_srv.get_specific_field_timeseries(
                    field_identifier="yf_item_balance_sheet_annual_TotalDebt",
                    tickers=ticker,
                    start_date_str=start_date_str,
                    end_date_str=end_date_str
                )

                # Fetch Total Assets data (both quarterly and annual)
                total_assets_quarterly = await self.base_query_srv.get_specific_field_timeseries(
                    field_identifier="yf_item_balance_sheet_quarterly_TotalAssets",
                    tickers=ticker,
                    start_date_str=start_date_str,
                    end_date_str=end_date_str
                )
                total_assets_annual = await self.base_query_srv.get_specific_field_timeseries(
                    field_identifier="yf_item_balance_sheet_annual_TotalAssets",
                    tickers=ticker,
                    start_date_str=start_date_str,
                    end_date_str=end_date_str
                )

                # Process the data points
                debt_points = []
                assets_points = []

                # Process quarterly data
                for point in total_debt_quarterly.get(ticker, []):
                    debt_points.append({
                        'date_obj': datetime.fromisoformat(point['date']),
                        'value': point['value']
                    })
                for point in total_assets_quarterly.get(ticker, []):
                    assets_points.append({
                        'date_obj': datetime.fromisoformat(point['date']),
                        'value': point['value']
                    })

                # Process annual data
                for point in total_debt_annual.get(ticker, []):
                    debt_points.append({
                        'date_obj': datetime.fromisoformat(point['date']),
                        'value': point['value']
                    })
                for point in total_assets_annual.get(ticker, []):
                    assets_points.append({
                        'date_obj': datetime.fromisoformat(point['date']),
                        'value': point['value']
                    })

                # Sort points by date
                debt_points.sort(key=lambda x: x['date_obj'])
                assets_points.sort(key=lambda x: x['date_obj'])

                # Calculate ratio points
                ratio_points = []
                for debt_point in debt_points:
                    # Find the most recent assets point before or on the debt point date
                    applicable_assets_point = None
                    for assets_point in reversed(assets_points):
                        if assets_point['date_obj'] <= debt_point['date_obj']:
                            applicable_assets_point = assets_point
                            break

                    if applicable_assets_point and applicable_assets_point['value'] != 0:
                        ratio = debt_point['value'] / applicable_assets_point['value']
                        ratio_points.append({
                            'date_obj': debt_point['date_obj'],
                            'value': ratio
                        })

                # Generate daily series using user's requested date range
                daily_series = []
                if ratio_points:
                    # Sort ratio points by date
                    ratio_points.sort(key=lambda x: x['date_obj'])
                    
                    # Generate daily series from user's start date to end date
                    current_date = user_start_date
                    last_known_ratio = None

                    while current_date <= user_end_date:
                        # Find the most recent ratio point before or on the current date
                        applicable_ratio_point = None
                        for point in reversed(ratio_points):
                            if point['date_obj'] <= current_date:
                                applicable_ratio_point = point
                                break

                        if applicable_ratio_point:
                            last_known_ratio = applicable_ratio_point['value']
                            daily_series.append({
                                'date': current_date.isoformat(),
                                'value': last_known_ratio
                            })
                        elif last_known_ratio is not None:
                            # Carry forward the last known ratio
                            daily_series.append({
                                'date': current_date.isoformat(),
                                'value': last_known_ratio
                            })
                        
                        current_date += timedelta(days=1)

                results[ticker] = daily_series
                logger.info(f"Generated {len(daily_series)} daily points for {ticker} Debt/Total Assets ratio")

            except Exception as e:
                logger.error(f"Error calculating Debt/Total Assets ratio for {ticker}: {str(e)}")
                results[ticker] = []

        return results

    async def calculate_asset_turnover_ttm(
        self,
        tickers: List[str],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate Asset Turnover (TTM) ratio timeseries for given tickers.
        Ratio = Net Income (TTM) / Total Assets
        Net Income uses TTM calculation from quarterly/annual income statements
        Total Assets uses most recent balance sheet value (quarterly or annual)
        """
        logger.info(f"Calculating Asset Turnover (TTM) ratio for tickers: {tickers}")
        results: Dict[str, List[Dict[str, Any]]] = {ticker: [] for ticker in tickers}
        
        try:
            # Parse date range using base service's method
            user_start_date = self.base_query_srv._parse_date_flex(start_date_str) if start_date_str else datetime.now() - timedelta(days=5*365)
            user_end_date = self.base_query_srv._parse_date_flex(end_date_str) if end_date_str else datetime.now()

            # Look back further for fundamental data to ensure enough history for TTM calculation
            fundamental_query_start_date = user_start_date - timedelta(days=5*365)

            for ticker in tickers:
                try:
                    logger.info(f"Processing Asset Turnover (TTM) for {ticker}")

                    # 1. Fetch Net Income data (quarterly and annual)
                    net_income_quarterly = await self.base_query_srv.get_specific_field_timeseries(
                        field_identifier="yf_item_income_statement_quarterly_NetIncome",
                        tickers=[ticker],
                        start_date_str=fundamental_query_start_date.strftime("%Y-%m-%d"),
                        end_date_str=user_end_date.strftime("%Y-%m-%d")
                    )
                    
                    net_income_annual = await self.base_query_srv.get_specific_field_timeseries(
                        field_identifier="yf_item_income_statement_annual_NetIncome",
                        tickers=[ticker],
                        start_date_str=fundamental_query_start_date.strftime("%Y-%m-%d"),
                        end_date_str=user_end_date.strftime("%Y-%m-%d")
                    )

                    # 2. Fetch Total Assets data (quarterly and annual)
                    total_assets_quarterly = await self.base_query_srv.get_specific_field_timeseries(
                        field_identifier="yf_item_balance_sheet_quarterly_TotalAssets",
                        tickers=[ticker],
                        start_date_str=fundamental_query_start_date.strftime("%Y-%m-%d"),
                        end_date_str=user_end_date.strftime("%Y-%m-%d")
                    )
                    
                    total_assets_annual = await self.base_query_srv.get_specific_field_timeseries(
                        field_identifier="yf_item_balance_sheet_annual_TotalAssets",
                        tickers=[ticker],
                        start_date_str=fundamental_query_start_date.strftime("%Y-%m-%d"),
                        end_date_str=user_end_date.strftime("%Y-%m-%d")
                    )

                    # Process Net Income data points
                    quarterly_net_income_points: List[Dict[str, Any]] = []
                    if ticker in net_income_quarterly:
                        for point in net_income_quarterly[ticker]:
                            date_obj = self.base_query_srv._parse_date_flex(point['date'])
                            value = point['value']
                            if date_obj and value is not None:
                                try:
                                    quarterly_net_income_points.append({
                                        'date_obj': date_obj,
                                        'value': float(value)
                                    })
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert quarterly net income value '{value}' to float for {ticker} on {point['date']}")

                    annual_net_income_points: List[Dict[str, Any]] = []
                    if ticker in net_income_annual:
                        for point in net_income_annual[ticker]:
                            date_obj = self.base_query_srv._parse_date_flex(point['date'])
                            value = point['value']
                            if date_obj and value is not None:
                                try:
                                    annual_net_income_points.append({
                                        'date_obj': date_obj,
                                        'value': float(value)
                                    })
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert annual net income value '{value}' to float for {ticker} on {point['date']}")

                    # Process Total Assets data points
                    quarterly_assets_points: List[Dict[str, Any]] = []
                    if ticker in total_assets_quarterly:
                        for point in total_assets_quarterly[ticker]:
                            date_obj = self.base_query_srv._parse_date_flex(point['date'])
                            value = point['value']
                            if date_obj and value is not None:
                                try:
                                    quarterly_assets_points.append({
                                        'date_obj': date_obj,
                                        'value': float(value)
                                    })
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert quarterly total assets value '{value}' to float for {ticker} on {point['date']}")

                    annual_assets_points: List[Dict[str, Any]] = []
                    if ticker in total_assets_annual:
                        for point in total_assets_annual[ticker]:
                            date_obj = self.base_query_srv._parse_date_flex(point['date'])
                            value = point['value']
                            if date_obj and value is not None:
                                try:
                                    annual_assets_points.append({
                                        'date_obj': date_obj,
                                        'value': float(value)
                                    })
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert annual total assets value '{value}' to float for {ticker} on {point['date']}")

                    # Sort all points by date
                    quarterly_net_income_points.sort(key=lambda x: x['date_obj'])
                    annual_net_income_points.sort(key=lambda x: x['date_obj'])
                    quarterly_assets_points.sort(key=lambda x: x['date_obj'])
                    annual_assets_points.sort(key=lambda x: x['date_obj'])

                    # Generate daily series
                    daily_series: List[Dict[str, Any]] = []
                    current_date = user_start_date

                    while current_date <= user_end_date:
                        # Calculate TTM Net Income
                        ttm_net_income = self.base_query_srv._calculate_ttm_value_generic(
                            current_date,
                            quarterly_net_income_points,
                            annual_net_income_points,
                            "value",
                            debug_identifier=f"TTM_NET_INCOME_FOR_ASSET_TURNOVER_{ticker}"
                        )

                        # Get most recent Total Assets value
                        applicable_assets_points = []
                        if quarterly_assets_points:
                            applicable_assets_points.extend(quarterly_assets_points)
                        if annual_assets_points:
                            applicable_assets_points.extend(annual_assets_points)
                        
                        applicable_assets_points.sort(key=lambda x: x['date_obj'], reverse=True)
                        
                        current_assets = None
                        for point in applicable_assets_points:
                            if point['date_obj'] <= current_date:
                                current_assets = point['value']
                                break

                        # Calculate Asset Turnover (TTM)
                        asset_turnover: Optional[float] = None
                        if ttm_net_income is not None and current_assets is not None and current_assets != 0:
                            asset_turnover = ttm_net_income / current_assets

                        daily_series.append({
                            'date': current_date.strftime('%Y-%m-%d'),
                            'value': asset_turnover
                        })

                        current_date += timedelta(days=1)

                    results[ticker] = daily_series
                    logger.info(f"Generated {len(daily_series)} daily Asset Turnover (TTM) points for {ticker}")

                except Exception as e:
                    logger.error(f"Error calculating Asset Turnover (TTM) for {ticker}: {str(e)}", exc_info=True)
                    results[ticker] = []

            return results

        except Exception as e:
            logger.error(f"Error in calculate_asset_turnover_ttm: {str(e)}", exc_info=True)
            raise

    async def calculate_inventory_turnover_ttm(
        self,
        tickers: List[str],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None,
        ticker_profiles_cache: Optional[Dict[str, Dict[str, Any]]] = None  # Added to match other methods
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate Inventory Turnover (TTM) ratio timeseries for given tickers.
        Ratio = Cost of Revenue (TTM) / Inventory
        Cost of Revenue uses TTM calculation from quarterly/annual income statements
        Inventory uses most recent balance sheet value (quarterly or annual)
        """
        logger.info(f"Calculating Inventory Turnover (TTM) ratio for tickers: {tickers}")
        results: Dict[str, List[Dict[str, Any]]] = {ticker: [] for ticker in tickers}
        
        try:
            # Parse date range using base service's method
            user_start_date = self.base_query_srv._parse_date_flex(start_date_str) if start_date_str else datetime.now() - timedelta(days=5*365)
            user_end_date = self.base_query_srv._parse_date_flex(end_date_str) if end_date_str else datetime.now()

            # Look back further for fundamental data to ensure enough history for TTM calculation
            fundamental_query_start_date = user_start_date - timedelta(days=5*365)

            for ticker in tickers:
                try:
                    logger.info(f"Processing Inventory Turnover (TTM) for {ticker}")

                    # 1. Fetch Cost of Revenue data (quarterly and annual)
                    cost_of_revenue_quarterly = await self.base_query_srv.get_specific_field_timeseries(
                        field_identifier="yf_item_income_statement_quarterly_CostOfRevenue",
                        tickers=[ticker],
                        start_date_str=fundamental_query_start_date.strftime("%Y-%m-%d"),
                        end_date_str=user_end_date.strftime("%Y-%m-%d")
                    )
                    
                    cost_of_revenue_annual = await self.base_query_srv.get_specific_field_timeseries(
                        field_identifier="yf_item_income_statement_annual_CostOfRevenue",
                        tickers=[ticker],
                        start_date_str=fundamental_query_start_date.strftime("%Y-%m-%d"),
                        end_date_str=user_end_date.strftime("%Y-%m-%d")
                    )

                    # 2. Fetch Inventory data (quarterly and annual)
                    inventory_quarterly = await self.base_query_srv.get_specific_field_timeseries(
                        field_identifier="yf_item_balance_sheet_quarterly_Inventory",
                        tickers=[ticker],
                        start_date_str=fundamental_query_start_date.strftime("%Y-%m-%d"),
                        end_date_str=user_end_date.strftime("%Y-%m-%d")
                    )
                    
                    inventory_annual = await self.base_query_srv.get_specific_field_timeseries(
                        field_identifier="yf_item_balance_sheet_annual_Inventory",
                        tickers=[ticker],
                        start_date_str=fundamental_query_start_date.strftime("%Y-%m-%d"),
                        end_date_str=user_end_date.strftime("%Y-%m-%d")
                    )

                    # Process Cost of Revenue data points
                    quarterly_cost_of_revenue_points: List[Dict[str, Any]] = []
                    if ticker in cost_of_revenue_quarterly:
                        for point in cost_of_revenue_quarterly[ticker]:
                            date_obj = self.base_query_srv._parse_date_flex(point['date'])
                            value = point['value']
                            if date_obj and value is not None:
                                try:
                                    quarterly_cost_of_revenue_points.append({
                                        'date_obj': date_obj,
                                        'value': float(value)
                                    })
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert quarterly cost of revenue value '{value}' to float for {ticker} on {point['date']}")

                    annual_cost_of_revenue_points: List[Dict[str, Any]] = []
                    if ticker in cost_of_revenue_annual:
                        for point in cost_of_revenue_annual[ticker]:
                            date_obj = self.base_query_srv._parse_date_flex(point['date'])
                            value = point['value']
                            if date_obj and value is not None:
                                try:
                                    annual_cost_of_revenue_points.append({
                                        'date_obj': date_obj,
                                        'value': float(value)
                                    })
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert annual cost of revenue value '{value}' to float for {ticker} on {point['date']}")

                    # Process Inventory data points
                    quarterly_inventory_points: List[Dict[str, Any]] = []
                    if ticker in inventory_quarterly:
                        for point in inventory_quarterly[ticker]:
                            date_obj = self.base_query_srv._parse_date_flex(point['date'])
                            value = point['value']
                            if date_obj and value is not None:
                                try:
                                    quarterly_inventory_points.append({
                                        'date_obj': date_obj,
                                        'value': float(value)
                                    })
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert quarterly inventory value '{value}' to float for {ticker} on {point['date']}")

                    annual_inventory_points: List[Dict[str, Any]] = []
                    if ticker in inventory_annual:
                        for point in inventory_annual[ticker]:
                            date_obj = self.base_query_srv._parse_date_flex(point['date'])
                            value = point['value']
                            if date_obj and value is not None:
                                try:
                                    annual_inventory_points.append({
                                        'date_obj': date_obj,
                                        'value': float(value)
                                    })
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert annual inventory value '{value}' to float for {ticker} on {point['date']}")

                    # Sort all points by date
                    quarterly_cost_of_revenue_points.sort(key=lambda x: x['date_obj'])
                    annual_cost_of_revenue_points.sort(key=lambda x: x['date_obj'])
                    quarterly_inventory_points.sort(key=lambda x: x['date_obj'])
                    annual_inventory_points.sort(key=lambda x: x['date_obj'])

                    # Generate daily series
                    daily_series: List[Dict[str, Any]] = []
                    current_date = user_start_date

                    while current_date <= user_end_date:
                        # Calculate TTM Cost of Revenue
                        ttm_cost_of_revenue = self.base_query_srv._calculate_ttm_value_generic(
                            current_date,
                            quarterly_cost_of_revenue_points,
                            annual_cost_of_revenue_points,
                            "value",
                            debug_identifier=f"TTM_COST_OF_REVENUE_FOR_INVENTORY_TURNOVER_{ticker}"
                        )

                        # Get most recent Inventory value
                        applicable_inventory_points = []
                        if quarterly_inventory_points:
                            applicable_inventory_points.extend(quarterly_inventory_points)
                        if annual_inventory_points:
                            applicable_inventory_points.extend(annual_inventory_points)
                        
                        applicable_inventory_points.sort(key=lambda x: x['date_obj'], reverse=True)
                        
                        current_inventory = None
                        for point in applicable_inventory_points:
                            if point['date_obj'] <= current_date:
                                current_inventory = point['value']
                                break

                        # Calculate Inventory Turnover (TTM)
                        inventory_turnover: Optional[float] = None
                        if ttm_cost_of_revenue is not None and current_inventory is not None and current_inventory != 0:
                            inventory_turnover = ttm_cost_of_revenue / current_inventory

                        daily_series.append({
                            'date': current_date.strftime('%Y-%m-%d'),
                            'value': inventory_turnover
                        })

                        current_date += timedelta(days=1)

                    results[ticker] = daily_series
                    logger.info(f"Generated {len(daily_series)} daily Inventory Turnover (TTM) points for {ticker}")

                except Exception as e:
                    logger.error(f"Error calculating Inventory Turnover (TTM) for {ticker}: {str(e)}", exc_info=True)
                    results[ticker] = []

            return results

        except Exception as e:
            logger.error(f"Error in calculate_inventory_turnover_ttm: {str(e)}", exc_info=True)
            raise

    async def calculate_interest_to_income_ttm(
        self,
        tickers: List[str],
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None,
        ticker_profiles_cache: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate Interest/Income (TTM) ratio timeseries for given tickers.
        Ratio = Interest Expense (TTM) / EBIT (TTM)
        Falls back to Operating Income (TTM) if EBIT is not available
        Both values use TTM calculation from quarterly/annual income statements
        """
        logger.info(f"Calculating Interest/Income (TTM) ratio for tickers: {tickers}")
        results: Dict[str, List[Dict[str, Any]]] = {ticker: [] for ticker in tickers}
        
        try:
            # Parse date range using base service's method
            user_start_date = self.base_query_srv._parse_date_flex(start_date_str) if start_date_str else datetime.now() - timedelta(days=5*365)
            user_end_date = self.base_query_srv._parse_date_flex(end_date_str) if end_date_str else datetime.now()

            # Look back further for fundamental data to ensure enough history for TTM calculation
            fundamental_query_start_date = user_start_date - timedelta(days=5*365)

            for ticker in tickers:
                try:
                    logger.info(f"Processing Interest/Income (TTM) for {ticker}")

                    # 1. Fetch Interest Expense data (quarterly and annual)
                    interest_expense_quarterly = await self.base_query_srv.get_specific_field_timeseries(
                        field_identifier="yf_item_income_statement_quarterly_InterestExpense",
                        tickers=[ticker],
                        start_date_str=fundamental_query_start_date.strftime("%Y-%m-%d"),
                        end_date_str=user_end_date.strftime("%Y-%m-%d")
                    )
                    
                    interest_expense_annual = await self.base_query_srv.get_specific_field_timeseries(
                        field_identifier="yf_item_income_statement_annual_InterestExpense",
                        tickers=[ticker],
                        start_date_str=fundamental_query_start_date.strftime("%Y-%m-%d"),
                        end_date_str=user_end_date.strftime("%Y-%m-%d")
                    )

                    # 2. Fetch EBIT data (quarterly and annual)
                    ebit_quarterly = await self.base_query_srv.get_specific_field_timeseries(
                        field_identifier="yf_item_income_statement_quarterly_EBIT",
                        tickers=[ticker],
                        start_date_str=fundamental_query_start_date.strftime("%Y-%m-%d"),
                        end_date_str=user_end_date.strftime("%Y-%m-%d")
                    )
                    
                    ebit_annual = await self.base_query_srv.get_specific_field_timeseries(
                        field_identifier="yf_item_income_statement_annual_EBIT",
                        tickers=[ticker],
                        start_date_str=fundamental_query_start_date.strftime("%Y-%m-%d"),
                        end_date_str=user_end_date.strftime("%Y-%m-%d")
                    )

                    # 3. Fetch Operating Income data as fallback (quarterly and annual)
                    operating_income_quarterly = await self.base_query_srv.get_specific_field_timeseries(
                        field_identifier="yf_item_income_statement_quarterly_OperatingIncome",
                        tickers=[ticker],
                        start_date_str=fundamental_query_start_date.strftime("%Y-%m-%d"),
                        end_date_str=user_end_date.strftime("%Y-%m-%d")
                    )
                    
                    operating_income_annual = await self.base_query_srv.get_specific_field_timeseries(
                        field_identifier="yf_item_income_statement_annual_OperatingIncome",
                        tickers=[ticker],
                        start_date_str=fundamental_query_start_date.strftime("%Y-%m-%d"),
                        end_date_str=user_end_date.strftime("%Y-%m-%d")
                    )

                    # Process Interest Expense data points
                    quarterly_interest_points: List[Dict[str, Any]] = []
                    if ticker in interest_expense_quarterly:
                        for point in interest_expense_quarterly[ticker]:
                            date_obj = self.base_query_srv._parse_date_flex(point['date'])
                            value = point['value']
                            if date_obj and value is not None:
                                try:
                                    quarterly_interest_points.append({
                                        'date_obj': date_obj,
                                        'value': float(value)
                                    })
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert quarterly interest expense value '{value}' to float for {ticker} on {point['date']}")

                    annual_interest_points: List[Dict[str, Any]] = []
                    if ticker in interest_expense_annual:
                        for point in interest_expense_annual[ticker]:
                            date_obj = self.base_query_srv._parse_date_flex(point['date'])
                            value = point['value']
                            if date_obj and value is not None:
                                try:
                                    annual_interest_points.append({
                                        'date_obj': date_obj,
                                        'value': float(value)
                                    })
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert annual interest expense value '{value}' to float for {ticker} on {point['date']}")

                    # Process EBIT data points
                    quarterly_ebit_points: List[Dict[str, Any]] = []
                    if ticker in ebit_quarterly:
                        for point in ebit_quarterly[ticker]:
                            date_obj = self.base_query_srv._parse_date_flex(point['date'])
                            value = point['value']
                            if date_obj and value is not None:
                                try:
                                    quarterly_ebit_points.append({
                                        'date_obj': date_obj,
                                        'value': float(value)
                                    })
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert quarterly EBIT value '{value}' to float for {ticker} on {point['date']}")

                    annual_ebit_points: List[Dict[str, Any]] = []
                    if ticker in ebit_annual:
                        for point in ebit_annual[ticker]:
                            date_obj = self.base_query_srv._parse_date_flex(point['date'])
                            value = point['value']
                            if date_obj and value is not None:
                                try:
                                    annual_ebit_points.append({
                                        'date_obj': date_obj,
                                        'value': float(value)
                                    })
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert annual EBIT value '{value}' to float for {ticker} on {point['date']}")

                    # Process Operating Income data points (fallback)
                    quarterly_operating_points: List[Dict[str, Any]] = []
                    if ticker in operating_income_quarterly:
                        for point in operating_income_quarterly[ticker]:
                            date_obj = self.base_query_srv._parse_date_flex(point['date'])
                            value = point['value']
                            if date_obj and value is not None:
                                try:
                                    quarterly_operating_points.append({
                                        'date_obj': date_obj,
                                        'value': float(value)
                                    })
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert quarterly operating income value '{value}' to float for {ticker} on {point['date']}")

                    annual_operating_points: List[Dict[str, Any]] = []
                    if ticker in operating_income_annual:
                        for point in operating_income_annual[ticker]:
                            date_obj = self.base_query_srv._parse_date_flex(point['date'])
                            value = point['value']
                            if date_obj and value is not None:
                                try:
                                    annual_operating_points.append({
                                        'date_obj': date_obj,
                                        'value': float(value)
                                    })
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert annual operating income value '{value}' to float for {ticker} on {point['date']}")

                    # Sort all points by date
                    quarterly_interest_points.sort(key=lambda x: x['date_obj'])
                    annual_interest_points.sort(key=lambda x: x['date_obj'])
                    quarterly_ebit_points.sort(key=lambda x: x['date_obj'])
                    annual_ebit_points.sort(key=lambda x: x['date_obj'])
                    quarterly_operating_points.sort(key=lambda x: x['date_obj'])
                    annual_operating_points.sort(key=lambda x: x['date_obj'])

                    # Generate daily series
                    daily_series: List[Dict[str, Any]] = []
                    current_date = user_start_date

                    while current_date <= user_end_date:
                        # Calculate TTM Interest Expense
                        ttm_interest = self.base_query_srv._calculate_ttm_value_generic(
                            current_date,
                            quarterly_interest_points,
                            annual_interest_points,
                            "value",
                            debug_identifier=f"TTM_INTEREST_FOR_INTEREST_INCOME_{ticker}"
                        )

                        # Try to calculate TTM EBIT first
                        ttm_income = self.base_query_srv._calculate_ttm_value_generic(
                            current_date,
                            quarterly_ebit_points,
                            annual_ebit_points,
                            "value",
                            debug_identifier=f"TTM_EBIT_FOR_INTEREST_INCOME_{ticker}"
                        )

                        # If EBIT is not available, fall back to Operating Income
                        if ttm_income is None:
                            logger.debug(f"EBIT not available for {ticker} on {current_date.strftime('%Y-%m-%d')}, falling back to Operating Income")
                            ttm_income = self.base_query_srv._calculate_ttm_value_generic(
                                current_date,
                                quarterly_operating_points,
                                annual_operating_points,
                                "value",
                                debug_identifier=f"TTM_OPERATING_INCOME_FOR_INTEREST_INCOME_{ticker}"
                            )

                        # Calculate Interest/Income (TTM) ratio
                        interest_to_income: Optional[float] = None
                        if ttm_interest is not None and ttm_income is not None and ttm_income != 0:
                            # Return null if income (EBIT or Operating Income) is negative
                            if ttm_income < 0:
                                logger.debug(f"Income (EBIT/Operating) is negative ({ttm_income}) for {ticker} on {current_date.strftime('%Y-%m-%d')}, setting ratio to null")
                                interest_to_income = None
                            else:
                                # Multiply by 100 to present as percentage
                                interest_to_income = (ttm_interest / ttm_income) * 100

                        daily_series.append({
                            'date': current_date.strftime('%Y-%m-%d'),
                            'value': interest_to_income
                        })

                        current_date += timedelta(days=1)

                    results[ticker] = daily_series
                    logger.info(f"Generated {len(daily_series)} daily Interest/Income (TTM) points for {ticker}")

                except Exception as e:
                    logger.error(f"Error calculating Interest/Income (TTM) for {ticker}: {str(e)}", exc_info=True)
                    results[ticker] = []

            return results

        except Exception as e:
            logger.error(f"Error in calculate_interest_to_income_ttm: {str(e)}", exc_info=True)
            raise