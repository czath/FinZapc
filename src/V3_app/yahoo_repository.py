"""Repository specifically for handling Yahoo Finance related database operations."""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy import delete, update, insert
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import json

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