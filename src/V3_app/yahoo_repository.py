"""Repository specifically for handling Yahoo Finance related database operations."""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy import delete, update, insert, select
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import json
import sqlalchemy
from sqlalchemy import inspect

# Import the models specific to Yahoo
from .yahoo_models import YahooTickerMasterModel, TickerDataItemsModel

# Configure logging for this repository
logger = logging.getLogger(__name__)

class YahooDataRepository:
    """Repository for accessing ticker_master and ticker_data_items tables."""
    
    def __init__(self, database_url: str):
        """Initialize the repository with a database URL."""
        self.database_url = database_url
        # Consider sharing the engine if multiple repositories are used frequently
        # For now, create a separate one for isolation.
        self.engine = create_async_engine(database_url)
        self.async_session_factory = sessionmaker(
            self.engine, expire_on_commit=False, class_=AsyncSession
        )
        logger.info(f"[Yahoo Repo] Initialized with DB URL: {database_url}")

    async def create_tables(self) -> None:
        """Creates only the Yahoo-specific tables (ticker_master, ticker_data_items)."""
        logger.info("[DB Yahoo Repo] Creating Yahoo-specific tables (ticker_master, ticker_data_items).")
        try:
            async with self.engine.begin() as conn:
                # Use the metadata associated with the specific models
                await conn.run_sync(YahooTickerMasterModel.metadata.create_all)
            logger.info("[DB Yahoo Repo] Yahoo-specific tables checked/created successfully.")
        except Exception as e:
            logger.error(f"[DB Yahoo Repo] Error during Yahoo table creation: {e}", exc_info=True)
            raise

    # --- Methods moved from SQLiteRepository will be added here in the next step ---
    # upsert_yahoo_ticker_master
    # update_ticker_master_fields
    # insert_ticker_data_item
    # insert_ticker_data_items
    # upsert_ticker_data_item 

    # --- Upsert Yahoo Ticker Master Data ---
    async def upsert_yahoo_ticker_master(self, ticker_data: Dict[str, Any]) -> None:
        """
        Upserts (inserts or updates) a record in the ticker_master table.
        Expects ticker_data to be a dictionary where keys match YahooTickerMasterModel column names.
        Uses the models defined in yahoo_models.py.
        """
        if not ticker_data or 'ticker' not in ticker_data:
            logger.error("[DB Yahoo Master Upsert - Yahoo Repo] Ticker data is empty or missing 'ticker' field. Skipping upsert.")
            return

        ticker_symbol = ticker_data['ticker']
        logger.info(f"[DB Yahoo Master Upsert - Yahoo Repo] Upserting data for ticker: {ticker_symbol}")

        try:
            async with self.engine.begin() as conn:
                ticker_data['update_last_full'] = datetime.now()

                stmt = sqlite_insert(YahooTickerMasterModel).values(**ticker_data)

                update_dict = {
                    c.name: getattr(stmt.excluded, c.name)
                    for c in YahooTickerMasterModel.__table__.columns
                    if not c.primary_key
                }
                
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=['ticker'],
                    set_=update_dict
                )
                
                await conn.execute(upsert_stmt)
            logger.info(f"[DB Yahoo Master Upsert - Yahoo Repo] Successfully upserted data for ticker: {ticker_symbol}")
        except SQLAlchemyError as e:
            logger.error(f"[DB Yahoo Master Upsert - Yahoo Repo] SQLAlchemyError upserting data for {ticker_symbol}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"[DB Yahoo Master Upsert - Yahoo Repo] Unexpected error upserting data for {ticker_symbol}: {e}", exc_info=True)
            raise
    # --- End Upsert Yahoo Ticker Master Data ---

    # --- Update Specific Yahoo Ticker Master Fields ---
    async def update_ticker_master_fields(self, ticker_symbol: str, updates: Dict[str, Any]) -> bool:
        """Updates specific fields for a given ticker in the ticker_master table.
           Uses the models defined in yahoo_models.py.

        Args:
            ticker_symbol: The ticker symbol to update.
            updates: A dictionary where keys are column names and values are the new values.

        Returns:
            True if the update affected at least one row, False otherwise.
        """
        if not updates:
            logger.warning(f"[DB Yahoo Master Update Fields - Yahoo Repo] No updates provided for ticker {ticker_symbol}. Skipping.")
            return False
        if not ticker_symbol:
            logger.error("[DB Yahoo Master Update Fields - Yahoo Repo] Ticker symbol is required.")
            return False

        logger.info(f"[DB Yahoo Master Update Fields - Yahoo Repo] Updating fields for ticker: {ticker_symbol}")
        logger.debug(f"[DB Yahoo Master Update Fields - Yahoo Repo] Update data: {updates}")

        try:
            async with self.engine.begin() as conn:
                stmt = (
                    update(YahooTickerMasterModel)
                    .where(YahooTickerMasterModel.ticker == ticker_symbol)
                    .values(**updates)
                )
                result = await conn.execute(stmt)
                
                if result.rowcount > 0:
                    logger.info(f"[DB Yahoo Master Update Fields - Yahoo Repo] Successfully updated {len(updates)} fields for {ticker_symbol} ({result.rowcount} row(s) affected).")
                    return True
                else:
                    logger.warning(f"[DB Yahoo Master Update Fields - Yahoo Repo] Ticker {ticker_symbol} not found for update, or values were unchanged.")
                    return False
        except SQLAlchemyError as e:
            logger.error(f"[DB Yahoo Master Update Fields - Yahoo Repo] SQLAlchemyError updating {ticker_symbol}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"[DB Yahoo Master Update Fields - Yahoo Repo] Unexpected error updating {ticker_symbol}: {e}", exc_info=True)
            raise
    # --- End Update Specific Yahoo Ticker Master Fields ---

    # --- Method to Insert Single Ticker Data Item (Table 2) - From Class --- 
    async def insert_ticker_data_item(self, item_data: Dict[str, Any]) -> Optional[int]:
        """Inserts a single data item into the ticker_data_items table.
        Validates required fields. Uses models from yahoo_models.py.
        If a unique constraint (ticker, item_type, item_time_coverage, item_key_date) is violated,
        the insert is ignored (ON CONFLICT DO NOTHING).
        Returns the ID of the inserted row, or None if ignored or on error.
        """
        item_copy = item_data.copy()

        # Use naive local time
        if 'item_key_date' not in item_copy:
             item_copy['item_key_date'] = datetime.now()
        if 'fetch_timestamp_utc' not in item_copy:
            item_copy['fetch_timestamp_utc'] = datetime.now()
        
        # Convert string date to datetime object if needed (naive)
        if isinstance(item_copy.get('item_key_date'), str):
            try:
                item_copy['item_key_date'] = datetime.fromisoformat(item_copy['item_key_date'])
            except ValueError:
                logger.error(f"[DB DataItems Insert - Yahoo Repo] Invalid date format: {item_copy.get('item_key_date')}. Must be ISO format for ticker {item_copy.get('ticker')}.")
                return None
        
        if not isinstance(item_copy.get('item_data_payload'), str):
            try:
                item_copy['item_data_payload'] = json.dumps(item_copy['item_data_payload'])
            except TypeError as e:
                logger.error(f"[DB DataItems Insert - Yahoo Repo] Could not serialize payload for ticker {item_copy.get('ticker')}: {e}")
                return None

        required_fields = ['ticker', 'item_type', 'item_time_coverage', 'item_key_date', 'item_data_payload']
        missing_fields = [field for field in required_fields if field not in item_copy or item_copy[field] is None]
        if missing_fields:
            logger.error(f"[DB DataItems Insert - Yahoo Repo] Missing fields {missing_fields} for ticker {item_copy.get('ticker')}. Cannot insert.")
            return None

        # Use sqlite_insert for ON CONFLICT DO NOTHING
        stmt = sqlite_insert(TickerDataItemsModel).values(**item_copy)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=['ticker', 'item_type', 'item_time_coverage', 'item_key_date']
        )
        
        try:
            async with self.async_session_factory() as session:
                async with session.begin():
                    result = await session.execute(stmt)
                    inserted_id = result.inserted_primary_key[0] if result.inserted_primary_key else None
                    if inserted_id:
                        logger.info(f"[DB DataItems Insert - Yahoo Repo] Inserted item for ticker '{item_copy.get('ticker')}', type '{item_copy.get('item_type')}', id {inserted_id}.")
                    else:
                        # This case means the conflict occurred and the row was ignored.
                        logger.info(f"[DB DataItems Insert - Yahoo Repo] Record for ticker '{item_copy.get('ticker')}', type '{item_copy.get('item_type')}', date '{item_copy.get('item_key_date')}' already exists or was ignored due to conflict.")
                    return inserted_id
        except IntegrityError as e: # Should ideally be caught by on_conflict_do_nothing for unique constraint
            logger.error(f"[DB DataItems Insert - Yahoo Repo] IntegrityError for ticker '{item_copy.get('ticker')}', type '{item_copy.get('item_type')}': {e}", exc_info=False)
            return None
        except Exception as e:
            logger.error(f"[DB DataItems Insert - Yahoo Repo] Unexpected error for ticker '{item_copy.get('ticker')}', type '{item_copy.get('item_type')}': {e}", exc_info=True)
            return None
    # --- End Insert Single Ticker Data Item --- 

    # --- Insert Multiple Ticker Data Items (Table 2) - From Class ---
    async def insert_ticker_data_items(self, items_data: List[Dict[str, Any]]) -> int:
        """Inserts multiple data items into the ticker_data_items table in a batch.
           Uses models from yahoo_models.py.

        Args:
            items_data: A list of dictionaries for new items.

        Returns:
            The number of items successfully prepared for bulk insert.
        """
        if not items_data:
            logger.info("[DB DataItems Batch - Yahoo Repo] No items provided for batch insert.")
            return 0

        processed_items = []
        now_local = datetime.now() # Get current local time once
        for item_data in items_data:
            item_copy = item_data.copy()

            # Use naive local time
            if 'item_key_date' not in item_copy:
                 item_copy['item_key_date'] = now_local
            if 'fetch_timestamp_utc' not in item_copy:
                item_copy['fetch_timestamp_utc'] = now_local
            
            # Convert string date to datetime object if needed (naive)
            if isinstance(item_copy.get('item_key_date'), str):
                try:
                    item_copy['item_key_date'] = datetime.fromisoformat(item_copy['item_key_date'])
                except ValueError:
                    logger.error(f"[DB DataItems Batch - Yahoo Repo] Invalid date format {item_copy.get('item_key_date')} for ticker {item_copy.get('ticker')}. Skipping.")
                    continue
            
            if not isinstance(item_copy.get('item_data_payload'), str):
                try:
                    item_copy['item_data_payload'] = json.dumps(item_copy['item_data_payload'])
                except TypeError as e:
                    logger.error(f"[DB DataItems Batch - Yahoo Repo] Could not serialize payload for {item_copy.get('ticker')}: {e}. Skipping.")
                    continue
            
            required_fields = ['ticker', 'item_type', 'item_time_coverage', 'item_key_date', 'item_data_payload']
            missing_fields = [field for field in required_fields if field not in item_copy or item_copy[field] is None]
            if missing_fields:
                logger.error(f"[DB DataItems Batch - Yahoo Repo] Missing fields {missing_fields} for ticker {item_copy.get('ticker')}. Skipping.")
                continue

            processed_items.append(item_copy)

        if not processed_items:
            logger.warning("[DB DataItems Batch - Yahoo Repo] No valid items for batch insert after preprocessing.")
            return 0
        
        try:
            async with self.async_session_factory() as session:
                async with session.begin():
                    await session.execute(insert(TickerDataItemsModel), processed_items)
                logger.info(f"[DB DataItems Batch - Yahoo Repo] Attempted bulk insert for {len(processed_items)} items.")
                return len(processed_items)
        except IntegrityError as e:
            logger.error(f"[DB DataItems Batch - Yahoo Repo] IntegrityError during bulk insert: {e}. Batch rolled back.", exc_info=False)
            return 0 
        except Exception as e:
            logger.error(f"[DB DataItems Batch - Yahoo Repo] Unexpected error during bulk insert: {e}. Batch rolled back.", exc_info=True)
            return 0
    # --- End Insert Multiple Ticker Data Items ---

    # --- Upsert Single Ticker Data Item (Delete-then-Insert) ---
    async def upsert_ticker_data_item(self, item_data: Dict[str, Any]) -> Optional[int]:
        """Upserts a single data item by deleting existing items with the same 
           ticker and item_type, then inserting the new item. 
           Suitable for CUMULATIVE_SNAPSHOT or similar. Uses models from yahoo_models.py.
        """
        ticker = item_data.get('ticker')
        item_type = item_data.get('item_type')

        if not ticker or not item_type:
            logger.error("[DB DataItems Upsert - Yahoo Repo] Ticker and Item Type required. Skipping.")
            return None

        if not isinstance(item_data.get('item_data_payload'), str):
            try:
                item_data['item_data_payload'] = json.dumps(item_data['item_data_payload'])
            except TypeError as e:
                logger.error(f"[DB DataItems Upsert - Yahoo Repo] Could not serialize payload for {ticker}/{item_type}: {e}")
                return None
        if isinstance(item_data.get('item_key_date'), str):
             try:
                 item_data['item_key_date'] = datetime.fromisoformat(item_data.get('item_key_date'))
             except ValueError:
                 logger.error(f"[DB DataItems Upsert - Yahoo Repo] Invalid date format {item_data.get('item_key_date')} for {ticker}/{item_type}.")
                 return None

        logger.info(f"[DB DataItems Upsert - Yahoo Repo] Upserting item for ticker '{ticker}', type '{item_type}'")

        try:
            async with self.async_session_factory() as session:
                async with session.begin():
                    delete_stmt = delete(TickerDataItemsModel).where(
                        TickerDataItemsModel.ticker == ticker,
                        TickerDataItemsModel.item_type == item_type
                    )
                    delete_result = await session.execute(delete_stmt)
                    logger.debug(f"[DB DataItems Upsert - Yahoo Repo] Deleted {delete_result.rowcount} existing for {ticker}/{item_type}.")

                    if 'fetch_timestamp_utc' not in item_data:
                        item_data['fetch_timestamp_utc'] = datetime.now()
                        
                    insert_stmt = insert(TickerDataItemsModel).values(**item_data)
                    insert_result = await session.execute(insert_stmt)
                    inserted_id = insert_result.inserted_primary_key[0] if insert_result.inserted_primary_key else None
                    
                    if inserted_id:
                        logger.info(f"[DB DataItems Upsert - Yahoo Repo] Successfully upserted {ticker}/{item_type}, new id {inserted_id}.")
                    else:
                        logger.warning(f"[DB DataItems Upsert - Yahoo Repo] Insert for {ticker}/{item_type} gave no ID.")
                    return inserted_id

        except IntegrityError as e:
            logger.error(f"[DB DataItems Upsert - Yahoo Repo] IntegrityError for {ticker}/{item_type}: {e}", exc_info=False)
            return None
        except Exception as e:
            logger.error(f"[DB DataItems Upsert - Yahoo Repo] Unexpected error for {ticker}/{item_type}: {e}", exc_info=True)
            return None
    # --- End Upsert Single Ticker Data Item --- 

    async def upsert_single_ttm_statement(self, item_data: Dict[str, Any]) -> Optional[int]:
        """
        Upserts a TTM (or similar single-representative) data item.
        1. Tries to insert the new item. If it has an identical unique key (ticker, type, coverage, key_date),
           the insert is ignored.
        2. If the new item was successfully inserted (meaning its key_date is new for this TTM item),
           it then deletes all other items for the same ticker/type/coverage
           that have a *different* item_key_date.
        This ensures only one TTM record (the one with the specific item_key_date from item_data) remains.
        """
        ticker = item_data.get('ticker')
        item_type = item_data.get('item_type')
        item_time_coverage = item_data.get('item_time_coverage')
        current_item_key_date = item_data.get('item_key_date') # This is the key_date of the TTM data being processed

        if not all([ticker, item_type, item_time_coverage, current_item_key_date]):
            logger.error(f"[DB TTM Upsert - Yahoo Repo] Missing critical fields for TTM upsert: ticker, item_type, item_time_coverage, or item_key_date. Data: {item_data}")
            return None

        # Ensure payload is JSON string and key_date is datetime
        if not isinstance(item_data.get('item_data_payload'), str):
            try:
                item_data['item_data_payload'] = json.dumps(item_data['item_data_payload'])
            except TypeError as e:
                logger.error(f"[DB TTM Upsert - Yahoo Repo] Could not serialize payload for {ticker}/{item_type}/{item_time_coverage}: {e}")
                return None
        
        if isinstance(current_item_key_date, str):
            try:
                current_item_key_date = datetime.fromisoformat(current_item_key_date)
                item_data['item_key_date'] = current_item_key_date # Update dict with datetime object
            except ValueError:
                logger.error(f"[DB TTM Upsert - Yahoo Repo] Invalid date format for item_key_date \'{current_item_key_date}\' for {ticker}/{item_type}/{item_time_coverage}.")
                return None
        
        if 'fetch_timestamp_utc' not in item_data: # Should be set before calling this normally
            item_data['fetch_timestamp_utc'] = datetime.now()


        logger.info(f"[DB TTM Upsert - Yahoo Repo] Processing TTM item for {ticker}, type '{item_type}', coverage '{item_time_coverage}', key_date '{current_item_key_date.date()}'.")

        inserted_id: Optional[int] = None
        
        try:
            async with self.async_session_factory() as session:
                async with session.begin():
                    # Step 1: Attempt to insert the new TTM record
                    # ON CONFLICT DO NOTHING for the exact same record (same ticker, type, coverage, key_date)
                    insert_stmt = sqlite_insert(TickerDataItemsModel).values(**item_data)
                    insert_stmt = insert_stmt.on_conflict_do_nothing(
                        index_elements=['ticker', 'item_type', 'item_time_coverage', 'item_key_date']
                    )
                    result = await session.execute(insert_stmt)
                    inserted_id = result.inserted_primary_key[0] if result.inserted_primary_key else None

                    if inserted_id:
                        logger.info(f"[DB TTM Upsert - Yahoo Repo] Successfully inserted new TTM record for {ticker}/{item_type}/{item_time_coverage} with key_date {current_item_key_date.date()}, ID: {inserted_id}.")
                        
                        # Step 2: If insert was successful, delete all other TTM records for this ticker/type/coverage
                        # that have a *different* item_key_date.
                        delete_stmt = delete(TickerDataItemsModel).where(
                            TickerDataItemsModel.ticker == ticker,
                            TickerDataItemsModel.item_type == item_type,
                            TickerDataItemsModel.item_time_coverage == item_time_coverage,
                            TickerDataItemsModel.item_key_date != current_item_key_date # Crucial: DO NOT delete the one just inserted
                        )
                        delete_result = await session.execute(delete_stmt)
                        if delete_result.rowcount > 0:
                            logger.info(f"[DB TTM Upsert - Yahoo Repo] Deleted {delete_result.rowcount} older TTM records for {ticker}/{item_type}/{item_time_coverage} to keep only key_date {current_item_key_date.date()}.")
                        else:
                            logger.info(f"[DB TTM Upsert - Yahoo Repo] No older TTM records found to delete for {ticker}/{item_type}/{item_time_coverage} (other than key_date {current_item_key_date.date()}).")
                    else:
                        # Insert was ignored due to conflict on the full unique key.
                        logger.info(f"[DB TTM Upsert - Yahoo Repo] TTM record for {ticker}/{item_type}/{item_time_coverage} with key_date {current_item_key_date.date()} already exists. No changes made.")
                
                # session.commit() is handled by async with session.begin()
            return inserted_id

        except IntegrityError as e: # Should ideally not be hit due to on_conflict_do_nothing for insert
            logger.error(f"[DB TTM Upsert - Yahoo Repo] IntegrityError for {ticker}/{item_type}/{item_time_coverage}: {e}", exc_info=False) # Less verbose for integrity
            return None
        except Exception as e:
            logger.error(f"[DB TTM Upsert - Yahoo Repo] Unexpected error for {ticker}/{item_type}/{item_time_coverage}: {e}", exc_info=True)
            return None
    # --- End Upsert Single TTM Statement ---

    async def get_latest_item_payload(
        self, 
        ticker: str, 
        item_type: str, 
        item_time_coverage: str
    ) -> Optional[Dict[str, Any]]:
        """Fetches the item_data_payload of the most recent item matching
           ticker, item_type, and item_time_coverage, ordered by item_key_date descending.
        """
        if not all([ticker, item_type, item_time_coverage]):
            logger.error("[DB Get Latest Payload] Ticker, item_type, and item_time_coverage are required.")
            return None

        logger.debug(f"[DB Get Latest Payload] Fetching latest payload for {ticker}, type '{item_type}', coverage '{item_time_coverage}'.")
        
        stmt = (
            select(TickerDataItemsModel.item_data_payload)
            .where(
                TickerDataItemsModel.ticker == ticker,
                TickerDataItemsModel.item_type == item_type,
                TickerDataItemsModel.item_time_coverage == item_time_coverage
            )
            .order_by(TickerDataItemsModel.item_key_date.desc())
            .limit(1)
        )

        try:
            async with self.async_session_factory() as session:
                result = await session.execute(stmt)
                scalar_result = result.scalar_one_or_none()
                
                if scalar_result:
                    logger.debug(f"[DB Get Latest Payload] Found existing payload for {ticker}/{item_type}/{item_time_coverage}.")
                    try:
                        payload_dict = json.loads(scalar_result) # scalar_result is the JSON string
                        return payload_dict
                    except json.JSONDecodeError as e:
                        logger.error(f"[DB Get Latest Payload] Error decoding JSON payload for {ticker}/{item_type}/{item_time_coverage}: {e}. Payload: {scalar_result[:200]}")
                        return None # Or an empty dict, depending on desired behavior for corrupted data
                else:
                    logger.info(f"[DB Get Latest Payload] No existing payload found for {ticker}/{item_type}/{item_time_coverage}.")
                    return None
        except SQLAlchemyError as e:
            logger.error(f"[DB Get Latest Payload] SQLAlchemyError fetching payload for {ticker}/{item_type}/{item_time_coverage}: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"[DB Get Latest Payload] Unexpected error fetching payload for {ticker}/{item_type}/{item_time_coverage}: {e}", exc_info=True)
            return None
    # --- End get_latest_item_payload ---

    @staticmethod
    def _model_to_dict(model_instance: Any) -> Optional[Dict[str, Any]]:
        """Converts a SQLAlchemy model instance to a dictionary.
           Datetime objects are converted to ISO format strings.
           Returns None if model_instance is None.
        """
        if model_instance is None:
            return None
        
        dict_representation = {}
        for column in model_instance.__table__.columns:
            value = getattr(model_instance, column.name)
            if isinstance(value, datetime):
                dict_representation[column.name] = value.isoformat()
            else:
                dict_representation[column.name] = value
        return dict_representation

    async def get_ticker_master_by_ticker(self, ticker_symbol: str) -> Optional[Dict[str, Any]]:
        """Retrieves a ticker_master record by ticker symbol (case-insensitive via DB collation) 
           and returns it as a dictionary.
        """
        if not ticker_symbol:
            logger.error("[DB Get Master] Ticker symbol is required.")
            return None
        
        logger.debug(f"[DB Get Master] Fetching ticker_master record for: {ticker_symbol}")
        # Direct comparison, relies on COLLATE NOCASE in schema
        stmt = select(YahooTickerMasterModel).where(YahooTickerMasterModel.ticker == ticker_symbol)
        
        try:
            async with self.async_session_factory() as session:
                result = await session.execute(stmt)
                model_instance = result.scalar_one_or_none()
                
                if model_instance:
                    logger.info(f"[DB Get Master] Found ticker_master record for {ticker_symbol}.")
                    return self._model_to_dict(model_instance)
                else:
                    logger.info(f"[DB Get Master] No ticker_master record found for {ticker_symbol}.")
                    return None
        except SQLAlchemyError as e:
            logger.error(f"[DB Get Master] SQLAlchemyError fetching master record for {ticker_symbol}: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"[DB Get Master] Unexpected error fetching master record for {ticker_symbol}: {e}", exc_info=True)
            return None

    async def get_ticker_masters_by_criteria(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Retrieves ticker_master records based on filter criteria (case-insensitivity for string fields 
           handled by DB collation) and returns them as a list of dictionaries.
        """
        logger.debug(f"[DB Get Masters By Criteria] Fetching records with filters: {filters}")
        
        # String columns list is no longer needed here as collation handles it
        # string_columns_for_case_insensitive_filter = [
        #     'ticker', 'company_name', 'country', 'exchange', 'industry', 'sector', 
        #     'trade_currency', 'asset_type', 'recommendation_key'
        # ]

        stmt = select(YahooTickerMasterModel)
        if filters:
            for column_name, value in filters.items():
                if hasattr(YahooTickerMasterModel, column_name):
                    # Direct comparison, relies on COLLATE NOCASE in schema for string columns
                    stmt = stmt.where(getattr(YahooTickerMasterModel, column_name) == value)
                else:
                    logger.warning(f"[DB Get Masters By Criteria] Invalid filter column: {column_name}. Skipping this filter.")
        
        records = []
        try:
            async with self.async_session_factory() as session:
                result = await session.execute(stmt)
                model_instances = result.scalars().all()
                
                for instance in model_instances:
                    dict_repr = self._model_to_dict(instance)
                    if dict_repr:
                        records.append(dict_repr)
                
                logger.info(f"[DB Get Masters By Criteria] Found {len(records)} records matching criteria.")
            return records
        except SQLAlchemyError as e:
            logger.error(f"[DB Get Masters By Criteria] SQLAlchemyError: {e}", exc_info=True)
            return [] # Return empty list on error
        except Exception as e:
            logger.error(f"[DB Get Masters By Criteria] Unexpected error: {e}", exc_info=True)
            return [] # Return empty list on error

    async def get_data_items_by_criteria(
        self,
        ticker: str,
        item_type: str,
        item_time_coverage: Optional[str] = None,
        key_date: Optional[datetime] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        order_by_key_date_desc: bool = True,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Retrieves ticker_data_items records based on criteria, parses JSON payload.
           String comparisons for ticker, item_type, and item_time_coverage are case-insensitive 
           via DB collation.
        """
        logger.debug(f"[DB Get DataItems By Criteria] Fetching for {ticker}, type: {item_type}, coverage: {item_time_coverage}, key_date: {key_date}, start: {start_date}, end: {end_date}, limit: {limit}")

        # Direct comparisons, relies on COLLATE NOCASE in schema
        stmt = select(TickerDataItemsModel).where(
            TickerDataItemsModel.ticker == ticker,
            TickerDataItemsModel.item_type == item_type
        )

        if item_time_coverage:
            # Direct comparison
            stmt = stmt.where(TickerDataItemsModel.item_time_coverage == item_time_coverage)
        if key_date:
            stmt = stmt.where(TickerDataItemsModel.item_key_date == key_date)
        if start_date:
            stmt = stmt.where(TickerDataItemsModel.item_key_date >= start_date)
        if end_date:
            stmt = stmt.where(TickerDataItemsModel.item_key_date <= end_date)
        
        if order_by_key_date_desc:
            stmt = stmt.order_by(TickerDataItemsModel.item_key_date.desc())
        else:
            stmt = stmt.order_by(TickerDataItemsModel.item_key_date.asc())
        
        if limit is not None and limit > 0:
            stmt = stmt.limit(limit)

        items = []
        try:
            async with self.async_session_factory() as session:
                result = await session.execute(stmt)
                model_instances = result.scalars().all()

                for instance in model_instances:
                    item_dict = self._model_to_dict(instance) # Use existing helper
                    if item_dict and 'item_data_payload' in item_dict and isinstance(item_dict['item_data_payload'], str):
                        try:
                            item_dict['item_data_payload'] = json.loads(item_dict['item_data_payload'])
                        except json.JSONDecodeError as e_json:
                            logger.error(f"[DB Get DataItems By Criteria] JSONDecodeError for item {item_dict.get('data_item_id')}: {e_json}. Payload: {item_dict['item_data_payload'][:200]}")
                            item_dict['item_data_payload'] = {"error": "Failed to parse payload"} # Or None, or keep string
                    if item_dict: # Ensure item_dict is not None before appending
                        items.append(item_dict)
                
                logger.info(f"[DB Get DataItems By Criteria] Found {len(items)} items for {ticker}/{item_type}.")
            return items
        except SQLAlchemyError as e_sql:
            logger.error(f"[DB Get DataItems By Criteria] SQLAlchemyError for {ticker}/{item_type}: {e_sql}", exc_info=True)
            return []
        except Exception as e_gen:
            logger.error(f"[DB Get DataItems By Criteria] Unexpected error for {ticker}/{item_type}: {e_gen}", exc_info=True)
            return []

    async def get_all_master_tickers(self) -> list[str]:
        """Return a list of all tickers in the Yahoo master ticker table."""
        records = await self.get_ticker_masters_by_criteria()
        return [rec['ticker'] for rec in records if 'ticker' in rec and rec['ticker']]

    async def get_all_master_tickers_with_names(self) -> list[dict]:
        """Return a list of dicts with 'ticker' and 'name' for all tickers in the master table."""
        records = await self.get_ticker_masters_by_criteria()
        # Use 'company_name' from the database and map it to 'name' in the output dictionary.
        # Provide an empty string as a default if company_name is missing or None.
        return [
            {"ticker": rec['ticker'], "name": rec.get('company_name') or ''}
            for rec in records
            if 'ticker' in rec and rec['ticker']
        ]

    async def yahoo_incremental_refresh(self, limit: int = 200) -> list[str]:
        """
        Fetches a limited number of tickers that were least recently updated.
        
        Args:
            limit (int): The maximum number of tickers to return. Defaults to 200.

        Returns:
            list[str]: A list of ticker symbols.
        """
        async with self.async_session_factory() as session:
            query = (
                select(YahooTickerMasterModel.ticker)
                .order_by(YahooTickerMasterModel.update_last_full.asc())
                .limit(limit)
            )
            result = await session.execute(query)
            return result.scalars().all()

    def _normalize_db_type(self, db_type: str) -> str:
        """Normalize SQLAlchemy type string to 'numeric', 'text', 'boolean', 'date', or 'unknown'."""
        t = db_type.lower()
        if any(x in t for x in ['float', 'real', 'integer', 'int', 'numeric', 'decimal', 'double']):
            return 'numeric'
        if any(x in t for x in ['varchar', 'text', 'char', 'string']):
            return 'text'
        if 'bool' in t:
            return 'boolean'
        if 'date' in t:  # Handles both 'date' and 'datetime'
            return 'date'
        return 'unknown'

    async def get_all_yahoo_fields_for_analytics(self) -> list[dict]:
        """
        Returns a list of all Yahoo fields for analytics configuration:
        - Ticker master fields: {name: 'yf_tm_<col>', type: <normalized_type>, example: <sample_value>}
        - Data item payload fields: {name: 'yf_<item_type>_<item_time_coverage>_<key>', type: <inferred_type>, example: <sample_value>}
        Only samples records where prun is False (0) to ensure we get active/valid data.
        """
        fields = []
        # --- Ticker Master fields ---
        async with self.engine.begin() as conn:
            def get_columns(sync_conn):
                insp = inspect(sync_conn)
                tm_table = YahooTickerMasterModel.__table__
                for col in tm_table.columns:
                    if col.primary_key:
                        continue
                    col_name = f"yf_tm_{col.name}"
                    col_type_str = str(col.type)
                    normalized_type = self._normalize_db_type(col_type_str)
                    
                    sample_value = None
                    try:
                        # Query the first non-null value for this column from YahooTickerMasterModel
                        # Ensure the column name used in getattr matches the model's attribute name (usually same as col.name)
                        stmt = select(getattr(YahooTickerMasterModel, col.name)).where(getattr(YahooTickerMasterModel, col.name) != None).limit(1)
                        result = sync_conn.execute(stmt)
                        row = result.scalar_one_or_none() # Use scalar_one_or_none to get the value directly
                        if row is not None:
                            sample_value = row
                            # If the sample value is a datetime object, convert it to ISO format string
                            if isinstance(sample_value, datetime):
                                sample_value = sample_value.isoformat()
                    except Exception as e:
                        logger.warning(f"Could not fetch sample for yf_tm field {col.name}: {e}")
                        pass # Continue if sample fetching fails for a column

                    fields.append({"name": col_name, "type": normalized_type, "example": sample_value})
            
            await conn.run_sync(get_columns)

        # --- Data Item payload fields ---
        async with self.async_session_factory() as session:
            # Get all unique (item_type, item_time_coverage) from non-pruned records
            stmt = sqlalchemy.select(
                TickerDataItemsModel.item_type,
                TickerDataItemsModel.item_time_coverage
            ).where(
                TickerDataItemsModel.prun == False  # Only get non-pruned records
            ).distinct()
            result = await session.execute(stmt)
            unique_types = result.all()
            
            # For each unique type/coverage, sample one non-pruned record to analyze payload
            for item_type, item_time_coverage in unique_types:
                sample_stmt = (
                    sqlalchemy.select(TickerDataItemsModel)
                    .where(TickerDataItemsModel.item_type == item_type)
                    .where(TickerDataItemsModel.item_time_coverage == item_time_coverage)
                    .where(TickerDataItemsModel.prun == False)  # Only sample non-pruned records
                    .limit(1)
                )
                sample_result = await session.execute(sample_stmt)
                sample_record = sample_result.scalar_one_or_none()
                if sample_record and sample_record.item_data_payload:
                    try:
                        payload = json.loads(sample_record.item_data_payload) if isinstance(sample_record.item_data_payload, str) else sample_record.item_data_payload
                        if isinstance(payload, dict):
                            for key, value in payload.items():
                                field_name = f"yf_{item_type.lower()}_{item_time_coverage.lower()}_{key}"
                                field_type_inferred = "unknown" # Default
                                # Infer type based on value
                                if isinstance(value, (int, float)) and not isinstance(value, bool):
                                    field_type_inferred = "numeric"
                                elif isinstance(value, str):
                                    field_type_inferred = "text"
                                elif isinstance(value, bool):
                                    field_type_inferred = "boolean"
                                # Assuming date strings in payload would be handled as 'text' by default
                                # or require specific parsing if they need to be 'date' type.
                                # For now, only basic types are inferred here.
                                elif value is None:
                                    field_type_inferred = "unknown" # Or perhaps 'empty' if that distinction is useful
                                else:
                                    # Fallback for other complex types, or could be refined
                                    field_type_inferred = type(value).__name__ 

                                # If the value is a datetime object (less likely for JSON, but defensive)
                                example_value_to_store = value
                                if isinstance(example_value_to_store, datetime):
                                    example_value_to_store = example_value_to_store.isoformat()

                                fields.append({"name": field_name, "type": field_type_inferred, "example": example_value_to_store})
                    except Exception as e:
                        logger.warning(f"Could not process payload sample for {item_type}/{item_time_coverage}: {e}")
                        continue
        return fields

    async def get_master_data_for_analytics(self) -> List[Dict[str, Any]]:
        """
        Fetches a specific subset of fields from YahooTickerMasterModel 
        for all tickers, relevant for the analytics page.
        """
        # Define the specific columns you need for analytics to optimize the query.
        # This list should be reviewed and adjusted based on the actual fields
        # used in your analytics UI and logic.
        fields_to_select = [
            YahooTickerMasterModel.ticker,
            YahooTickerMasterModel.company_name,
            YahooTickerMasterModel.asset_type,
            YahooTickerMasterModel.country,
            YahooTickerMasterModel.exchange,
            YahooTickerMasterModel.industry,
            YahooTickerMasterModel.sector,
            YahooTickerMasterModel.trade_currency,
            YahooTickerMasterModel.financial_currency,
            # Key Market Data
            YahooTickerMasterModel.current_price,
            YahooTickerMasterModel.market_cap,
            YahooTickerMasterModel.average_volume,
            YahooTickerMasterModel.beta,
            YahooTickerMasterModel.dividend_yield_ttm,
            YahooTickerMasterModel.fifty_two_week_high,
            YahooTickerMasterModel.fifty_two_week_low,
            # Key Valuation Ratios
            YahooTickerMasterModel.trailing_pe,
            YahooTickerMasterModel.pe_forward,
            YahooTickerMasterModel.price_to_book,
            YahooTickerMasterModel.price_to_sales_ttm,
            YahooTickerMasterModel.enterprise_to_revenue,
            YahooTickerMasterModel.enterprise_to_ebitda,
            # Key Financial Summary
            YahooTickerMasterModel.eps_ttm,
            YahooTickerMasterModel.eps_forward,
            YahooTickerMasterModel.book_value,
            YahooTickerMasterModel.current_ratio,
            YahooTickerMasterModel.debt_to_equity,
            YahooTickerMasterModel.ebitda_margin, # You mentioned this one
            YahooTickerMasterModel.operating_margin,
            YahooTickerMasterModel.profit_margin,
            YahooTickerMasterModel.return_on_assets,
            YahooTickerMasterModel.return_on_equity,
            YahooTickerMasterModel.revenue_growth,
            YahooTickerMasterModel.earnings_growth,
            YahooTickerMasterModel.shares_outstanding,
            YahooTickerMasterModel.shares_float,
            YahooTickerMasterModel.shares_percent_insiders,
            YahooTickerMasterModel.shares_percent_institutions,
            YahooTickerMasterModel.short_ratio,
            YahooTickerMasterModel.short_percent_of_float,
            YahooTickerMasterModel.update_last_full, # To know how fresh the master data is
            YahooTickerMasterModel.update_marketonly 
        ]

        try:
            async with self.async_session_factory() as session:
                stmt = select(*fields_to_select)
                result = await session.execute(stmt)
                # Use .mappings().all() to get a list of dict-like RowMapping objects
                # then convert each to a plain dict.
                ticker_data_list = [dict(row) for row in result.mappings().all()]
                logger.info(f"Fetched {len(ticker_data_list)} records from ticker_master for analytics.")
                return ticker_data_list
        except SQLAlchemyError as e:
            logger.error(f"Database error in get_master_data_for_analytics: {e}", exc_info=True)
            return [] # Return empty list on error
        except Exception as e:
            logger.error(f"Failed to get yahoo master data for analytics: {e}", exc_info=True)
            return []

    async def get_all_data_items(self) -> List[Dict[str, Any]]:
        """
        Fetches all records from the ticker_data_items table. Used for maintenance tasks.

        Returns:
            A list of dictionaries, where each dictionary represents a row.
        """
        async with self.async_session_factory() as session:
            try:
                stmt = select(TickerDataItemsModel)
                result = await session.execute(stmt)
                items = result.scalars().all()
                return [self._model_to_dict(item) for item in items]
            except Exception as e:
                logger.error(f"Error fetching all data items: {e}", exc_info=True)
                return []

    async def delete_yahoo_ticker_master(self, ticker_symbol: str) -> bool:
        """
        Deletes a ticker record from the ticker_master table.
        Note: This will cascade and delete related ticker_data_items due to DB relationship.

        Args:
            ticker_symbol: The ticker symbol to delete.

        Returns:
            True if deletion was successful, False otherwise.
        """
        async with self.async_session_factory() as session:
            try:
                stmt = delete(YahooTickerMasterModel).where(YahooTickerMasterModel.ticker == ticker_symbol)
                result = await session.execute(stmt)
                await session.commit()
                if result.rowcount > 0:
                    logger.info(f"Successfully deleted ticker '{ticker_symbol}' from ticker_master.")
                    return True
                else:
                    logger.warning(f"Ticker '{ticker_symbol}' not found in ticker_master for deletion.")
                    return False
            except Exception as e:
                logger.error(f"Error deleting ticker '{ticker_symbol}' from ticker_master: {e}", exc_info=True)
                await session.rollback()
                return False

    async def delete_ticker_data_item(self, data_item_id: int) -> bool:
        """
        Deletes a specific data item from the ticker_data_items table by its ID.

        Args:
            data_item_id: The primary key of the data item to delete.

        Returns:
            True if deletion was successful, False otherwise.
        """
        async with self.async_session_factory() as session:
            try:
                stmt = delete(TickerDataItemsModel).where(TickerDataItemsModel.data_item_id == data_item_id)
                result = await session.execute(stmt)
                await session.commit()
                if result.rowcount > 0:
                    logger.info(f"Successfully deleted data item with ID '{data_item_id}'.")
                    return True
                else:
                    logger.warning(f"Data item with ID '{data_item_id}' not found for deletion.")
                    return False
            except Exception as e:
                logger.error(f"Error deleting data item ID '{data_item_id}': {e}", exc_info=True)
                await session.rollback()
                return False