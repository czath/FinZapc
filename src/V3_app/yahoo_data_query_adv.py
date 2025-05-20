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