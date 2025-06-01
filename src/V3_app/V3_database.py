"""
Database models and repository implementation for V3 of the financial application.
Combines SQLAlchemy models and repository functionality.

Key features:
- Database models for accounts, positions, orders, and job configurations
- Repository implementation for data access
- Error handling and logging
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Set, Union, Tuple, AsyncGenerator
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, create_engine, delete, MetaData, Table, insert, update, and_, distinct, Text, Boolean, text, func, UniqueConstraint, Index, event, inspect
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import relationship, declarative_base, sessionmaker, Session
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.future import select
import os
import json
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import sqlite3
import aiosqlite
import asyncio

# Remove the temporary Pydantic import and definitions here
# from pydantic import BaseModel
# from typing import Optional

# Import the schemas from the new file using relative import
# from .schemas import PortfolioRuleCreate, PortfolioRuleUpdate

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

Base = declarative_base()

class AccountModel(Base):
    """SQLAlchemy model for account data."""
    __tablename__ = 'accounts'
    
    id = Column(Integer, primary_key=True)
    account_id = Column(String, unique=True, nullable=False)
    net_liquidation = Column(Float)
    total_cash = Column(Float)
    gross_position_value = Column(Float)
    upd_mode = Column(String)
    last_update = Column(DateTime, default=datetime.now)
    
    positions = relationship("PositionModel", back_populates="account")
    orders = relationship("OrderModel", back_populates="account")

class PositionModel(Base):
    """SQLAlchemy model for position data."""
    __tablename__ = 'positions'
    
    id = Column(Integer, primary_key=True)
    account_id = Column(String, ForeignKey('accounts.account_id'))
    ticker = Column(String)
    name = Column(String)
    position = Column(Float)
    mkt_price = Column(Float)
    mkt_value = Column(Float)
    avg_cost = Column(Float)
    avg_price = Column(Float)
    unrealized_pnl = Column(Float)
    realized_pnl = Column(Float)
    pnl_percentage = Column(Float)
    sector = Column(String)
    group = Column(String)
    sector_group = Column(String)
    sec_type = Column(String)
    contract_desc = Column(String)
    currency = Column(String)
    last_update = Column(DateTime, default=datetime.now)
    
    account = relationship("AccountModel", back_populates="positions")

class OrderModel(Base):
    """SQLAlchemy model for order data."""
    __tablename__ = 'orders'
    
    id = Column(Integer, primary_key=True)
    account_id = Column(String, ForeignKey('accounts.account_id'))
    order_id = Column(String)
    ticker = Column(String)
    description = Column(String)
    status = Column(String)
    side = Column(String)
    order_type = Column(String)
    total_size = Column(Float)
    filled_qty = Column(Float)
    remaining_qty = Column(Float)
    stop_price = Column(Float)
    limit_price = Column(Float)
    limit_offset = Column(Float)
    trailing_amount = Column(Float)
    avg_price = Column(Float)
    currency = Column(String)
    last_update = Column(DateTime, default=datetime.now)
    
    account = relationship("AccountModel", back_populates="orders")

class JobConfigModel(Base):
    """SQLAlchemy model for job configurations."""
    __tablename__ = 'job_configs'
    
    id = Column(Integer, primary_key=True)
    job_id = Column(String, unique=True, nullable=False)
    job_type = Column(String)
    schedule = Column(String)  # JSON string
    is_active = Column(Integer, default=1)
    last_run = Column(DateTime)
    next_run = Column(DateTime)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class ScreenerModel(Base):
    __tablename__ = 'screener'
    
    ticker = Column(String, primary_key=True, nullable=False)
    status = Column(String, nullable=False) # Expected: 'portfolio', 'candidate', 'monitored'
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # --- NEW FIELDS --- 
    atr = Column(Float, nullable=True)
    atr_mult = Column(Integer, nullable=True)
    risk = Column(Float, nullable=True)
    beta = Column(Float, nullable=True)
    sector = Column(String, nullable=True)
    industry = Column(String, nullable=True)
    comments = Column(Text, nullable=True) # Use Text for longer comments
    # --- END NEW FIELDS ---

    # --- Tracker Source Fields ---
    t_source1 = Column(String, nullable=True)
    conid = Column(String, nullable=True) # Changed type to String to match existing VARCHAR DB column
    # --- End Tracker Source Fields ---
    
    # --- Company Name Field ---
    Company = Column(String, nullable=True)
    # --- End Company Name Field ---

    # --- Add Open Pos and Cost Base --- 
    open_pos = Column(Integer, nullable=True) # Assuming Integer for Open Pos
    cost_base = Column(Float, nullable=True)  # Assuming Float for Cost Base
    # --- End Open Pos and Cost Base ---

    # --- Add Currency --- 
    currency = Column(String, nullable=True)
    # --- End Currency ---

    # --- Rename Account to Acc ---
    acc = Column(String, nullable=True) 
    # --- End Rename ---

    # --- ADDED Price Field ---
    price = Column(Float, nullable=True)
    # --- END Price Field ---

    # --- ADDED daychange Field ---
    daychange = Column(Float, nullable=True)
    # --- END daychange Field ---

    # --- Finviz Raw Data Foreign Key ---
    # finviz_data = relationship("FinvizRawDataModel", back_populates="screener_entry", uselist=False, cascade="all, delete-orphan") # OLD
    # finviz_data = relationship("FinvizRawDataModel", back_populates="screener_ticker_entry", uselist=False, cascade="all, delete-orphan")
    # Re-evaluating relationship - FinvizRawDataModel.ticker is FK to ScreenerModel.ticker
    # A ScreenerModel entry can have one FinvizRawDataModel entry.

    # --- Analytics Raw Data Relationship ---
    # analytics_entries = relationship("AnalyticsRawDataModel", back_populates="screener_ticker_entry")
    # Re-evaluating relationship - AnalyticsRawDataModel.ticker is not directly FK to ScreenerModel.
    # No direct ORM relationship needed here based on current schema for AnalyticsRawDataModel.

    def __repr__(self):
        return f"<ScreenerModel(ticker='{self.ticker}', status='{self.status}', company='{self.Company}')>"

class PortfolioRuleModel(Base):
    __tablename__ = 'portfolio_rules'

    id = Column(Integer, primary_key=True) # Auto-incrementing primary key
    rule_name = Column(String, nullable=False) # e.g., 'Portfolio Leverage', 'Sector 1 Allocation'
    min_value = Column(Float, nullable=False, default=0.0)
    max_value = Column(Float, nullable=False, default=0.0)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True) # Using Boolean directly
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Add a unique constraint potentially? Or handle logic in application layer?
    # For now, handling uniqueness of active rules in application layer seems safer.
    # from sqlalchemy import UniqueConstraint
    # __table_args__ = (UniqueConstraint('rule_name', 'is_active', name='_rule_name_active_uc'),) -> This prevents duplicates but makes toggling harder.

# --- NEW Finviz Raw Data Model ---
class FinvizRawDataModel(Base):
    __tablename__ = 'finviz_raw'

    ticker = Column(String, ForeignKey('screener.ticker'), primary_key=True, nullable=False) # Link to screener
    # Store all fetched fields as a single comma-delimited string for now
    raw_data = Column(Text, nullable=True)
    last_fetched_at = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)

    # screener_ticker_entry = relationship("ScreenerModel", back_populates="finviz_data") # OLD

    def __repr__(self):
        return f"<FinvizRawDataModel(ticker='{self.ticker}', last_fetched_at='{self.last_fetched_at}')>"
    
    def to_dict(self):
        return {
            "ticker": self.ticker,
            "raw_data": self.raw_data,
            "last_fetched_at": self.last_fetched_at.isoformat() if self.last_fetched_at else None
        }

# --- NEW Analytics Raw Data Model ---
class AnalyticsRawDataModel(Base):
    __tablename__ = 'analytics_raw'

    ticker = Column(String, primary_key=True, nullable=False)
    source = Column(String, primary_key=True, nullable=False) # e.g., 'finviz', 'yahoo'
    raw_data = Column(Text, nullable=True) # Stores the fetched data (e.g., comma-delimited string)
    last_fetched_at = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)

    # Composite primary key defined by setting primary_key=True on both columns

    # --- ADDED to_dict METHOD ---
    def to_dict(self):
        """Converts the SQLAlchemy model instance to a dictionary."""
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    # --- END to_dict METHOD ---

# --- End Analytics Raw Data Model ---

# Define the exchange_rates table
metadata = MetaData()
exchange_rates = Table(
    'exchange_rates', metadata,
    Column('currency', String, primary_key=True),
    Column('rate', Float, nullable=False),
    Column('conid', String, nullable=True) # ADDED conid column
)

class CachedAnalyticsDataModel(Base):
    __tablename__ = 'cached_analytics_data'
    
    # id = Column(Integer, primary_key=True, default=1, unique=True) # As per plan, PK implies unique. default=1 helps ensure only ID 1 is used.
    id = Column(Integer, primary_key=True) # Simpler: PK means unique. Repo logic will ensure id=1.
    data_json = Column(Text, nullable=False)
    generated_at = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('id', name='uq_cached_analytics_data_id'), # Enforce id is 1 via repo. This ensures the column is unique.
    )

    def __repr__(self):
        ts = self.generated_at.isoformat() if self.generated_at else "None"
        return f"<CachedAnalyticsDataModel(id={self.id}, generated_at='{ts}')>"

class CachedAnalyticsMetadataModel(Base):
    __tablename__ = 'cached_analytics_metadata'

    # id = Column(Integer, primary_key=True, default=1, unique=True) # As per plan
    id = Column(Integer, primary_key=True)
    metadata_json = Column(Text, nullable=False)
    generated_at = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('id', name='uq_cached_analytics_metadata_id'),
    )
    
    def __repr__(self):
        ts = self.generated_at.isoformat() if self.generated_at else "None"
        return f"<CachedAnalyticsMetadataModel(id={self.id}, generated_at='{ts}')>"

class SQLiteRepository:
    """Repository for SQLite database operations."""
    print("--- SQLiteRepository class definition loaded ---") # <--- ADD THIS LINE
    
    def __init__(self, database_url: str):
        """Initialize the repository with a database URL."""
        self.database_url = database_url
        logger.info(f"SQLiteRepository initialized with DB URL: {self.database_url}")
        self.engine = create_async_engine(database_url)
        # --- ADDED ---
        self.async_session_factory = async_sessionmaker(
            bind=self.engine, 
            expire_on_commit=False, # Good practice for async sessions
            class_=AsyncSession # Explicitly state the session class to be used
        )
        # --- END ADDED ---

    async def get_db_path(self) -> str:
        # Extracts the file path from the SQLite URL
        if self.database_url.startswith("sqlite+aiosqlite:///"):
            return self.database_url[len("sqlite+aiosqlite:///"):]
        elif self.database_url.startswith("sqlite:///"):
            return self.database_url[len("sqlite:///"):]
        # Add handling for other sqlite URL formats if necessary
        raise ValueError(f"Unsupported SQLite URL format: {self.database_url}")

    async def execute_script(self, conn: aiosqlite.Connection, script: str):
        try:
            await conn.executescript(script)
            await conn.commit()
            logger.info("Database script executed successfully.")
        except aiosqlite.Error as e:
            logger.error(f"Error executing database script: {e}", exc_info=True)
            raise

    async def create_tables(self) -> None:
         """Creates tables using Base.metadata."""
         try:
             logger.info("[DB Init] Creating tables.")
             async with self.engine.begin() as conn:
                  await conn.run_sync(Base.metadata.create_all)
                  await conn.run_sync(lambda sync_conn: exchange_rates.create(sync_conn, checkfirst=True))
             logger.info("[DB Init] Tables created successfully.")
         except Exception as e:
             logger.error(f"[DB Init] Error during table creation: {e}", exc_info=True)
             raise

         # ADDED: SQL for persistent_job_states table
         sql_create_persistent_job_states_table = """
         CREATE TABLE IF NOT EXISTS persistent_job_states (
             job_id TEXT PRIMARY KEY,
             status TEXT NOT NULL,
             last_completion_time TEXT, -- ISO format datetime string
             last_run_summary TEXT,
             total_count INTEGER,
             successful_count INTEGER,
             failed_count INTEGER,
             job_specific_data TEXT, -- JSON string for extra data if needed
             updated_at TEXT NOT NULL -- ISO format datetime string
         );
         """
         db_path = await self.get_db_path()
         try:
             # Use aiosqlite directly for this one table, assuming self.database_url is usable by aiosqlite
             logger.info(f"[DB Init] Attempting to create 'persistent_job_states' table directly in {db_path}...")
             async with aiosqlite.connect(db_path) as db:
                 await db.execute(sql_create_persistent_job_states_table)
                 await db.commit()
             logger.info("[DB Init] 'persistent_job_states' table check/creation complete.")
         except Exception as e:
             logger.error(f"[DB Init] Error creating 'persistent_job_states' table: {e}", exc_info=True)
             raise # Ensure the exception is re-raised to halt startup if this critical table fails
         logger.info("'persistent_job_states' table check/creation process completed.") # Clarified log

    async def clear_positions(self, account_id: str) -> None:
        """Clear all positions for a specific account using ORM model."""
        try:
            logger.info(f"[DB] Clearing positions for account {account_id}")
            async with self.engine.begin() as conn:
                # Use ORM model for delete
                stmt = delete(PositionModel).where(PositionModel.account_id == account_id)
                await conn.execute(stmt)
            logger.info(f"[DB] Positions cleared for account {account_id}")
        except Exception as e:
            logger.error(f"[DB] Error clearing positions: {str(e)}")
            raise
    
    async def save_position(self, position: Dict[str, Any]) -> None:
        """Save position to database using ORM model."""
        try:
            logger.info(f"[DB] Saving position {position.get('ticker', 'N/A')} for account {position.get('account_id', 'N/A')}")
            async with self.engine.begin() as conn:
                 # Use ORM model for insert
                stmt = insert(PositionModel).values(**position)
                await conn.execute(stmt)
            logger.info(f"[DB] Position saved: {position.get('ticker', 'N/A')}")
        except Exception as e:
            logger.error(f"[DB] Error saving position: {str(e)}")
            raise
    
    async def get_all_positions(self) -> List[Dict[str, Any]]:
        """Get all positions from database using ORM model."""
        try:
            logger.info("[DB] Fetching all positions")
            async with self.engine.connect() as conn: # Use connect for select
                # Use ORM model for select
                result = await conn.execute(select(PositionModel))
                rows = result.mappings().all() # Use mappings() for dict-like rows
                positions = [dict(row) for row in rows] # Convert to list of dicts
                logger.info(f"[DB] Found {len(positions)} positions")
                return positions
        except Exception as e:
            logger.error(f"[DB] Error fetching positions: {str(e)}")
            raise
    
    async def save_account(self, account: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Save or update account data using ORM model (upsert)."""
        account_id = account.get('account_id')
        if not account_id:
            logger.error("[DB] Cannot save account without account_id")
            return None # Return None if no ID

        logger.info(f"[DB] Saving account {account_id}")
        try:
            async with self.engine.begin() as conn:
                # Check if exists
                result = await conn.execute(
                    select(AccountModel).where(AccountModel.account_id == account_id)
                )
                existing = result.scalar_one_or_none()
                
                if existing:
                    logger.info(f"[DB] Updating existing account {account_id}")
                    # Ensure last_update is set for updates too
                    account['last_update'] = datetime.now()
                    stmt = (
                        update(AccountModel)
                        .where(AccountModel.account_id == account_id)
                        .values(**account)
                    )
                else:
                    logger.info(f"[DB] Inserting new account {account_id}")
                    # Ensure last_update is set for inserts
                    account['last_update'] = datetime.now()
                    stmt = insert(AccountModel).values(**account)
                
                await conn.execute(stmt)
                # Commit is handled by engine.begin() context manager
                
            logger.info(f"[DB] Account {account_id} saved/updated successfully in DB.")
            
            # --- Fetch the saved/updated record to return it --- 
            async with self.engine.connect() as conn:
                result = await conn.execute(
                    select(AccountModel).where(AccountModel.account_id == account_id)
                )
                saved_record = result.mappings().first()
                if saved_record:
                    logger.info(f"[DB] Fetched saved record for {account_id} to return.")
                    return dict(saved_record) # Convert RowMapping to dict
                else:
                     logger.error(f"[DB] Failed to fetch account {account_id} immediately after saving!")
                     return None # Return None if fetch failed
            # --- End Fetch --- 

        except Exception as e:
            logger.error(f"[DB] Error saving account {account_id}: {str(e)}")
            logger.exception("Database save error:") # Log full traceback
            return None # Return None on error
    
    async def get_all_accounts(self) -> List[Dict[str, Any]]:
        """Get all accounts from database using ORM model."""
        try:
            logger.info("[DB] Fetching all accounts")
            async with self.engine.connect() as conn: # Use connect for select
                # Use ORM model for select
                result = await conn.execute(select(AccountModel))
                rows = result.mappings().all() # Use mappings() for dict-like rows
                accounts = [dict(row) for row in rows] # Convert to list of dicts
                logger.info(f"[DB] Found {len(accounts)} accounts")
                return accounts
        except Exception as e:
            logger.error(f"[DB] Error fetching accounts: {str(e)}")
            raise
    
    async def delete_account(self, account_id: str) -> None:
        """Delete an account using ORM model."""
        try:
            logger.info(f"[DB] Deleting account {account_id}")
            async with self.engine.begin() as conn:
                 # Use ORM model for delete
                stmt = delete(AccountModel).where(AccountModel.account_id == account_id)
                await conn.execute(stmt)
            logger.info(f"[DB] Account {account_id} deleted successfully")
        except Exception as e:
            logger.error(f"[DB] Error deleting account {account_id}: {str(e)}")
            raise
    
    async def get_account_by_id(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific account by its ID using ORM model."""
        try:
            logger.info(f"[DB] Fetching account {account_id}")
            async with self.engine.connect() as conn: # Use connect for select
                # Use ORM model for select
                result = await conn.execute(
                    select(AccountModel).where(AccountModel.account_id == account_id)
                )
                row = result.mappings().first() # Use mappings().first()
                account = dict(row) if row else None # Convert to dict if found
                logger.info(f"[DB] Account {account_id} {'found' if account else 'not found'}")
                return account
        except Exception as e:
            logger.error(f"[DB] Error fetching account {account_id}: {str(e)}")
            raise
    
    async def update_account(self, account_id: str, updates: Dict[str, Any]) -> bool:
        """Update an account using ORM model. Returns True if updated, False otherwise."""
        # Removed fetching account_id from dict
        if not account_id:
            # Raise error immediately if account_id is missing or empty
            raise ValueError("account_id cannot be empty")
            
        # Always update the timestamp
        updates['last_update'] = datetime.now()
            
        try:
            logger.info(f"[DB] Updating account {account_id} with data: {updates}")
            async with self.engine.begin() as conn:
                 # Use ORM model for update, pass the updates dictionary
                stmt = update(AccountModel).where(AccountModel.account_id == account_id).values(**updates)
                result = await conn.execute(stmt)
                
                if result.rowcount > 0:
                    logger.info(f"[DB] Account {account_id} updated successfully ({result.rowcount} row(s) affected).")
                    return True
                else:
                    logger.warning(f"[DB] Account {account_id} not found for update (0 rows affected). Update failed.")
                    return False
        except Exception as e:
            logger.error(f"[DB] Error updating account {account_id}: {str(e)}")
            raise
            
    async def get_all_orders(self) -> List[Dict[str, Any]]:
        """Get all orders from database using ORM model."""
        try:
            logger.info("[DB] Fetching all orders")
            async with self.engine.connect() as conn: # Use connect for select
                # Use ORM model for select
                result = await conn.execute(select(OrderModel))
                rows = result.mappings().all() # Use mappings() for dict-like rows
                orders = [dict(row) for row in rows] # Convert to list of dicts
                logger.info(f"[DB] Found {len(orders)} orders")
                return orders
        except Exception as e:
            logger.error(f"[DB] Error fetching orders: {str(e)}")
            raise
            
    async def save_order(self, order_data: dict) -> None:
        """Save order to database using ORM model."""
        order_id = order_data.get('order_id', 'Unknown')
        account_id = order_data.get('account_id', 'Unknown')
        try:
            logger.info(f"[DB] Saving order {order_id} for account {account_id}")
            async with self.engine.begin() as conn:
                 # Use ORM model for insert
                stmt = insert(OrderModel).values(**order_data)
                await conn.execute(stmt)
            logger.info(f"[DB] Order {order_id} saved successfully")
        except Exception as e:
            logger.error(f"[DB] Error saving order {order_id}: {str(e)}")
            raise
    
    async def get_job_config(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job configuration by ID."""
        try:
            async with self.engine.connect() as conn:
                stmt = select(JobConfigModel).filter_by(job_id=job_id)
                result = await conn.execute(stmt)
                # CORRECTED: Use mappings().first() to get a dict-like object or None
                job_config_row = result.mappings().first()
                # Convert the RowMapping to a dict if found
                return dict(job_config_row) if job_config_row else None
        except Exception as e:
            logger.error(f"Error fetching job config {job_id}: {str(e)}")
            raise
    
    async def save_job_config(self, job_config_data: Dict[str, Any]) -> None:
        """Save or update job configuration using SQLite upsert (for generic schedules)."""
        try:
            # Ensure updated_at is set
            job_config_data['updated_at'] = datetime.now()
            # Ensure necessary fields exist for insert, providing defaults
            job_config_data.setdefault('job_type', 'data_fetch')
            job_config_data.setdefault('is_active', 1)
            job_config_data.setdefault('created_at', job_config_data['updated_at'])
            
            # Create the initial insert statement
            stmt = sqlite_insert(JobConfigModel).values(**job_config_data)
            
            # Define the fields to update on conflict (exclude primary key and created_at)
            update_fields = {
                c.name: getattr(stmt.excluded, c.name) # Use getattr for excluded columns
                for c in JobConfigModel.__table__.columns
                if c.name not in ['id', 'job_id', 'created_at'] # Don't update id, job_id, or created_at
            }
            
            # Add the ON CONFLICT DO UPDATE clause
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=['job_id'], # The unique column causing the conflict
                set_=update_fields
            )
            
            async with self.engine.begin() as conn:
                await conn.execute(upsert_stmt)
            logger.info(f"[DB] Saved/Updated job config for: {job_config_data.get('job_id')}")
        except Exception as e:
            logger.error(f"Error saving/updating job config: {str(e)}", exc_info=True)
            raise
    
    async def update_job_config(self, job_id: str, updates: Dict[str, Any]) -> None:
        """Update job configuration."""
        try:
            async with self.engine.begin() as conn:
                await conn.execute(
                    update(JobConfigModel).where(JobConfigModel.job_id == job_id).values(**updates)
                )
        except Exception as e:
            logger.error(f"Error updating job config {job_id}: {str(e)}")
            raise
    
    async def get_fetch_interval_seconds(self, default_interval: int = 3600) -> int:
        """Get the fetch interval in seconds from job_configs (expects JSON format for ibkr_fetch)."""
        job_id = 'ibkr_fetch' # Hardcoded for original functionality
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(
                    select(JobConfigModel.schedule).where(JobConfigModel.job_id == job_id)
                )
                schedule_str = result.scalar_one_or_none()
                if schedule_str:
                    try:
                        schedule_data = json.loads(schedule_str)
                        # UPDATED: Look for the standard format: {"trigger": "interval", "seconds": NNN}
                        if isinstance(schedule_data, dict) and schedule_data.get('trigger') == 'interval' and 'seconds' in schedule_data:
                            interval = int(schedule_data['seconds'])
                            if interval > 0:
                                logger.info(f"[DB] Found IBKR fetch interval: {interval} seconds from standard JSON format.")
                                return interval
                            else:
                                logger.warning(f"[DB] Found non-positive seconds '{interval}' in standard JSON. Using default.")
                                return default_interval
                        # Check for old specific key as fallback (optional, can be removed if migration is done)
                        elif isinstance(schedule_data, dict) and 'interval_seconds' in schedule_data: 
                            interval = int(schedule_data['interval_seconds'])
                            if interval > 0:
                                logger.info(f"[DB] Found IBKR fetch interval: {interval} seconds from OLD JSON format.")
                                return interval
                            else:
                                logger.warning(f"[DB] Found non-positive IBKR interval '{interval}' in OLD JSON. Using default.")
                                return default_interval
                        else:
                            logger.warning(f"[DB] Found invalid or unexpected JSON format for {job_id}: '{schedule_str}'. Using default interval.")
                            return default_interval
                    except (json.JSONDecodeError, ValueError, TypeError) as e:
                        logger.warning(f"[DB] Error parsing schedule JSON for {job_id}: {e}. String was '{schedule_str}'. Using default interval.")
                        return default_interval
                else:
                    logger.info(f"[DB] No schedule found for {job_id}. Using default interval {default_interval} seconds.")
                    return default_interval
        except Exception as e:
            logger.error(f"[DB] Error fetching job config {job_id}: {str(e)}. Using default interval.")
            return default_interval

    async def set_fetch_interval_seconds(self, interval_seconds: int) -> None:
        """Save or update the fetch interval in job_configs (stores as JSON for ibkr_fetch)."""
        job_id = 'ibkr_fetch'
        schedule_data = {'interval_seconds': interval_seconds}
        schedule_str = json.dumps(schedule_data)
        try:
            async with self.engine.begin() as conn:
                result = await conn.execute(
                    select(JobConfigModel.id).where(JobConfigModel.job_id == job_id)
                )
                exists = result.scalar_one_or_none()
                now = datetime.now()
                if exists:
                    logger.info(f"[DB] Updating fetch interval for {job_id} to JSON: {schedule_str}")
                    await conn.execute(
                        update(JobConfigModel)
                        .where(JobConfigModel.job_id == job_id)
                        .values(schedule=schedule_str, job_type='data_fetch', is_active=1, updated_at=now)
                    )
                else:
                    logger.info(f"[DB] Setting initial fetch interval for {job_id} with JSON: {schedule_str}")
                    await conn.execute(
                        insert(JobConfigModel).values(
                            job_id=job_id,
                            job_type='data_fetch', 
                            schedule=schedule_str,
                            is_active=1,
                            created_at=now,
                            updated_at=now
                        )
                    )
            logger.info(f"[DB] Successfully set fetch interval for {job_id} with JSON.")
        except Exception as e:
            logger.error(f"[DB] Error setting fetch interval for {job_id}: {str(e)}")
            raise

    async def get_job_schedule_seconds(self, job_id: str, default_seconds: int = 0) -> int:
        """Get the schedule interval in seconds for a generic job_id, expecting standard JSON format."""
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(
                    select(JobConfigModel.schedule).where(JobConfigModel.job_id == job_id)
                )
                schedule_str = result.scalar_one_or_none()
                
                if schedule_str:
                    try:
                        schedule_data = json.loads(schedule_str)
                        # Expecting {"trigger": "interval", "seconds": NNNN}
                        if isinstance(schedule_data, dict) and schedule_data.get('trigger') == 'interval' and 'seconds' in schedule_data:
                            seconds = int(schedule_data['seconds'])
                            if seconds > 0:
                                logger.info(f"[DB] Found schedule for {job_id}: {seconds} seconds.")
                                return seconds
                            else:
                                logger.warning(f"[DB] Found non-positive seconds for {job_id} in schedule JSON: {schedule_str}. Returning default.")
                                return default_seconds
                        else:
                             logger.warning(f"[DB] Invalid or non-interval schedule JSON format for {job_id}: {schedule_str}. Returning default.")
                             return default_seconds
                    except (json.JSONDecodeError, ValueError, TypeError) as e:
                         logger.warning(f"[DB] Error parsing schedule JSON for {job_id}: {e}. String was '{schedule_str}'. Returning default.")
                         return default_seconds
                else:
                    logger.info(f"[DB] No schedule found for {job_id}. Returning default: {default_seconds}")
                    return default_seconds
        except Exception as e:
            logger.error(f"[DB] Error fetching schedule for {job_id}: {str(e)}. Returning default.")
            return default_seconds

    async def get_job_is_active(self, job_id: str, default_active: bool = True) -> bool:
        """Get the is_active status (as boolean) for a specific job_id."""
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(
                    select(JobConfigModel.is_active).where(JobConfigModel.job_id == job_id)
                )
                is_active_db = result.scalar_one_or_none()
                if is_active_db is not None:
                    is_active = bool(is_active_db) # Convert DB int/bool to Python bool
                    logger.info(f"[DB] Found is_active status for {job_id}: {is_active}")
                    return is_active
                else:
                    logger.info(f"[DB] No is_active status found for {job_id}. Returning default: {default_active}")
                    return default_active
        except Exception as e:
            logger.error(f"[DB] Error fetching is_active status for {job_id}: {str(e)}. Returning default.")
            return default_active

    async def update_job_active_status(self, job_id: str, is_active: bool) -> bool:
        """Update the is_active status for a specific job_id. Returns True on success, False otherwise."""
        try:
            logger.info(f"[DB] Updating is_active status for {job_id} to {is_active}")
            async with self.engine.begin() as conn:
                values_to_set = {'is_active': int(is_active), 'updated_at': datetime.now()}
                logger.debug(f"[DB Update Status] Prepared values: {values_to_set}") # Log prepared values
                
                stmt = update(JobConfigModel)\
                    .where(JobConfigModel.job_id == job_id)\
                    .values(**values_to_set) # Use the prepared dict
                
                logger.debug(f"[DB Update Status] Executing update for job_id: {job_id}")
                result = await conn.execute(stmt)
                logger.debug(f"[DB Update Status] Update executed. Result rowcount: {result.rowcount}") # Log rowcount
                
                if result.rowcount > 0:
                    logger.info(f"[DB] Successfully updated is_active status for {job_id} (rowcount > 0).") # Adjusted log
                    return True
                else:
                    logger.warning(f"[DB] Job ID {job_id} not found for status update.")
                    return False
        except Exception as e:
            logger.error(f"[DB] Error updating is_active status for {job_id}: {str(e)}")
            raise # Re-raise exception to be handled by caller

    async def clear_all_data(self) -> None:
        """Clear all data from the database using ORM models."""
        try:
            logger.info("[DB] Clearing all data from database")
            async with self.engine.begin() as conn:
                # Delete in order due to foreign key constraints, using ORM models
                await conn.execute(delete(PositionModel)) # Clear positions first
                await conn.execute(delete(OrderModel))    # Then orders
                await conn.execute(delete(AccountModel))  # Then accounts
                # Optionally clear job configs if needed, or handle separately
                # await conn.execute(delete(JobConfigModel)) 
            logger.info("[DB] All data cleared successfully")
        except Exception as e:
            logger.error(f"[DB] Error clearing data: {str(e)}")
            raise

    async def delete_orders_for_account(self, account_id: str) -> None:
        """Delete all orders for a specific account using ORM model."""
        try:
            logger.info(f"[DB] Deleting orders for account {account_id}")
            async with self.engine.begin() as conn:
                 # Use ORM model for delete
                stmt = delete(OrderModel).where(OrderModel.account_id == account_id)
                await conn.execute(stmt)
            logger.info(f"[DB] Orders deleted for account {account_id}")
        except Exception as e:
            logger.error(f"[DB] Error deleting orders: {str(e)}")
            raise

    # --- Screener Methods ---
    async def get_all_screened_tickers(self) -> List[Dict[str, Any]]:
        """Get all tickers and their source info from the screener table."""
        try:
            logger.info("[DB] Fetching all screened tickers (full model)") # Updated log
            async with self.engine.connect() as conn:
                # Select the full model again for the UI
                stmt = select(ScreenerModel).order_by(ScreenerModel.ticker) # Select full model and order
                result = await conn.execute(stmt)
                # Use mappings() to get results as list of dict-like RowMapping objects
                rows = result.mappings().all()
                # Convert RowMapping objects to standard dictionaries
                tickers_data = [dict(row) for row in rows]
                logger.info(f"[DB] Found {len(tickers_data)} screened tickers")
                return tickers_data
        except Exception as e:
            logger.error(f"[DB] Error fetching screened tickers: {str(e)}", exc_info=True)
            raise

    async def add_or_update_screener_ticker(self,
                                           ticker: str,
                                           status: str,
                                           # Add all other optional fields from the form/model
                                           conid: Optional[str] = None, # ADDED
                                           atr: Optional[float] = None,
                                           atr_mult: Optional[int] = None,
                                           risk: Optional[float] = None,
                                           beta: Optional[float] = None,
                                           sector: Optional[str] = None,
                                           industry: Optional[str] = None,
                                           comments: Optional[str] = None,
                                           Company: Optional[str] = None, # ADDED
                                           open_pos: Optional[int] = None, # ADDED
                                           cost_base: Optional[float] = None, # ADDED
                                           currency: Optional[str] = None, # ADDED
                                           acc: Optional[str] = None, # ADDED
                                           price: Optional[float] = None, # ADDED
                                           daychange: Optional[float] = None, # ADDED
                                           t_source1: Optional[str] = None # ADDED
                                           # **kwargs: Any # Alternative: Accept arbitrary kwargs
                                           ) -> Dict[str, str]: # <-- CHANGE RETURN TYPE HINT
        """Add a new ticker or update the status and details if it already exists.
        Returns a dictionary with status and message.
        """
        valid_statuses = ["portfolio", "candidate", "monitored", "indicator"] # Added indicator
        if status not in valid_statuses:
            # Return error dict instead of raising ValueError
            return {"status": "error", "message": f"Invalid status '{status}'. Must be one of {valid_statuses}"}
        if not ticker or not ticker.strip():
             # Return error dict instead of raising ValueError
             return {"status": "error", "message": "Ticker cannot be empty."}

        ticker_upper = ticker.strip().upper()

        # Prepare data, including new optional fields
        # Make sure all added parameters are included here
        data_to_insert = {
            'ticker': ticker_upper,
            'status': status,
            'conid': conid,
            'atr': atr,
            'atr_mult': atr_mult,
            'risk': risk,
            'beta': beta,
            'sector': sector,
            'industry': industry,
            'comments': comments,
            'Company': Company,
            'open_pos': open_pos,
            'cost_base': cost_base,
            'currency': currency,
            'acc': acc,
            'price': price,
            'daychange': daychange,
            't_source1': t_source1,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        # Data for updating existing records (don't update created_at)
        data_to_update = {
            'status': status,
            'conid': conid,
            'atr': atr,
            'atr_mult': atr_mult,
            'risk': risk,
            'beta': beta,
            'sector': sector,
            'industry': industry,
            'comments': comments,
            'Company': Company,
            'open_pos': open_pos,
            'cost_base': cost_base,
            'currency': currency,
            'acc': acc,
            'price': price,
            'daychange': daychange,
            't_source1': t_source1,
            'updated_at': datetime.now()
        }

        try:
            logger.info(f"[DB] Adding/Updating screener ticker: {ticker_upper} with status {status}, ConID: {conid}")
            async with self.engine.begin() as conn:
                # --- Check if ticker exists and fetch its conid --- 
                stmt_check = select(ScreenerModel.conid).where(ScreenerModel.ticker == ticker_upper)
                result = await conn.execute(stmt_check)
                existing_conid_row = result.first() # Fetch the row (tuple) or None
                # --- End Check ---

                if existing_conid_row is not None:
                    # --- Ticker Exists - Check ConID --- 
                    existing_conid = existing_conid_row[0] # Get conid from the tuple
                    logger.debug(f"[DB] Ticker {ticker_upper} exists. Existing ConID: {existing_conid}, Incoming ConID: {conid}")
                    
                    # Convert incoming conid to string for comparison, handling None
                    incoming_conid_str = str(conid) if conid is not None else None
                    
                    # Skip update only if BOTH ticker AND conid match
                    if existing_conid == incoming_conid_str:
                        logger.info(f"[DB] Ticker {ticker_upper} with ConID {conid} already exists. Skipping update.")
                        return {"status": "skipped", "message": f"Ticker {ticker_upper} with ConID {conid} already exists. No update performed."}
                    else:
                        # Ticker exists, but ConID is different or was NULL. Proceed with UPDATE.
                        logger.info(f"[DB] Ticker {ticker_upper} exists, but ConID differs (Existing: {existing_conid}, New: {conid}). Updating...")
                        stmt = update(ScreenerModel).where(ScreenerModel.ticker == ticker_upper).values(**data_to_update)
                        await conn.execute(stmt)
                        logger.info(f"[DB] Updated existing screener ticker: {ticker_upper}")
                        return {"status": "updated", "message": f"Updated existing screener ticker: {ticker_upper}"}
                    # --- End ConID Check ---

                else:
                    # --- Ticker Does Not Exist - Insert --- 
                    logger.info(f"[DB] Ticker {ticker_upper} does not exist. Inserting...")
                    stmt = insert(ScreenerModel).values(**data_to_insert)
                    await conn.execute(stmt)
                    logger.info(f"[DB] Added new screener ticker: {ticker_upper}")
                    return {"status": "inserted", "message": f"Added new screener ticker: {ticker_upper}"}
                    # --- End Insert ---
        except Exception as e:
            logger.error(f"[DB] Error adding/updating screener ticker {ticker_upper}: {str(e)}", exc_info=True)
            # Return error dict instead of raising
            return {"status": "error", "message": f"Error adding/updating screener ticker {ticker_upper}: {str(e)}"}
            # Remove raise

    async def update_screener_ticker_status(self, ticker: str, status: str) -> None:
        """Update the status of an existing screener ticker."""
        valid_statuses = ["portfolio", "candidate", "monitored"]
        if status not in valid_statuses:
            raise ValueError(f"Invalid status '{status}'. Must be one of {valid_statuses}")
        if not ticker or not ticker.strip():
             raise ValueError("Ticker cannot be empty.")
        
        ticker_upper = ticker.strip().upper()

        try:
            logger.info(f"[DB] Updating screener ticker status: {ticker_upper} to {status}")
            async with self.engine.begin() as conn:
                stmt = update(ScreenerModel).where(ScreenerModel.ticker == ticker_upper).values(status=status, updated_at=datetime.now())
                result = await conn.execute(stmt)
                if result.rowcount == 0:
                    logger.warning(f"[DB] Ticker {ticker_upper} not found for status update.")
                else:
                    logger.info(f"[DB] Updated status for screener ticker: {ticker_upper}")
        except Exception as e:
            logger.error(f"[DB] Error updating screener ticker status {ticker_upper}: {str(e)}")
            raise

    async def delete_screener_ticker(self, ticker: str) -> bool:
        """Delete a ticker from the screener table. Returns True if deleted, False if not found."""
        if not ticker or not ticker.strip():
             raise ValueError("Ticker cannot be empty.")
        ticker_upper = ticker.strip().upper()
        try:
            logger.info(f"[DB] Deleting screener ticker: {ticker_upper}")
            async with self.engine.begin() as conn:
                stmt = delete(ScreenerModel).where(ScreenerModel.ticker == ticker_upper)
                result = await conn.execute(stmt)
                if result.rowcount == 0:
                    logger.warning(f"[DB] Ticker {ticker_upper} not found for deletion.")
                    return False # Ticker not found
                else:
                    logger.info(f"[DB] Deleted screener ticker: {ticker_upper}")
                    return True # Ticker deleted
        except Exception as e:
            logger.error(f"[DB] Error deleting screener ticker {ticker_upper}: {str(e)}")
            raise

    async def update_screener_ticker_details(self, ticker: str, field: str, value: Union[str, float, int, None]) -> None:
        """Update a specific detail field for a ticker in the screener table."""
        ticker = ticker.strip().upper()
        # Define valid columns and their expected types (use None for text/flexible)
        # Ensure this map is consistent with the ScreenerModel definition
        valid_columns = {
            'status': str,
            'atr': float,
            'atr_mult': int,
            'risk': float,
            'beta': float,
            'sector': str,
            'industry': str,
            'comments': str,
            't_source1': str,
            'conid': str,
            'Company': str, 
            'open_pos': int, 
            'cost_base': float,
            'currency': str, 
            'acc': str,
            'price': float,  
            'daychange': float, # ADDED daychange validation
            'updated_at': datetime 
        }

        logger.debug(f"[Update Details Check] Checking field: '{field}'. Valid columns dict being used: {valid_columns}")

        if field not in valid_columns:
            raise ValueError(f"Invalid field name for screener update: {field}")

        # --- Type Conversion (Keep Existing Logic) --- 
        target_type = valid_columns[field]
        converted_value = value
        logger.debug(f"[Update Details - Conversion] Attempting conversion for field '{field}'. Input value: '{value}' (Type: {type(value)})")
        try:
            if value is not None and value != '':
                # Add specific handling for float/int conversion failures
                if target_type == float:
                    try:
                        converted_value = float(value)
                    except (ValueError, TypeError):
                        raise ValueError(f"Invalid float value: '{value}'")
                elif target_type == int:
                    try:
                        # Attempt to convert potential floats to int after rounding
                        converted_value = int(round(float(value)))
                    except (ValueError, TypeError):
                        raise ValueError(f"Invalid integer value: '{value}'")
                elif target_type == str:
                    converted_value = str(value).strip()
            elif value == '': # Handle empty string input -> store as NULL
                 converted_value = None
            # If value is None, keep it as None

        except ValueError as ve:
            logger.error(f"[Update Details - Conversion] Conversion failed: {ve!r}", exc_info=True) # Log original error
            # Re-raise with a more specific message if needed, or just re-raise
            raise ValueError(f"Invalid value type for {field}: '{value}'. Expected {target_type}. Error: {ve}")
        except Exception as e: # Catch any other unexpected conversion errors
             logger.error(f"[Update Details - Conversion] Unexpected conversion error: {e!r}", exc_info=True)
             raise ValueError(f"Unexpected error converting value '{value}' for field {field}.")
        # --- End Type Conversion ---

        # --- Refactored Update Logic using SQLAlchemy Core --- 
        try:
            async with self.engine.begin() as conn: # Use engine.begin() for transaction
                stmt = (
                    update(ScreenerModel)
                    .where(ScreenerModel.ticker == ticker)
                    .values(**{field: converted_value, 'updated_at': datetime.now()})
                )
                # Execute the SQLAlchemy statement
                result = await conn.execute(stmt)
                
                # Optionally check if any row was actually updated
                if result.rowcount == 0:
                    logger.warning(f"[DB Update Screener] Ticker '{ticker}' not found or value for '{field}' was already '{converted_value}'. No update performed.")
                    # Raise error if ticker MUST exist
                    # raise ValueError(f"Ticker {ticker} not found for update.")
                else:
                    logger.debug(f"[DB Update Screener] Updated {field} for ticker {ticker} to {converted_value}")

        except Exception as e:
            logger.error(f"[DB Update Screener] Error updating {field} for ticker {ticker}: {e}", exc_info=True)
            # Re-raise the exception to be handled by the caller
            raise
        # --- End Refactored Update Logic ---

    def get_session(self) -> AsyncSession:
        """Create and return a new database session."""
        return AsyncSession(self.engine)

    async def get_all_position_tickers(self) -> Set[str]:
        """Get a set of unique ticker symbols from the positions table."""
        try:
            logger.info("[DB] Fetching all unique position tickers")
            async with self.engine.connect() as conn:
                result = await conn.execute(select(distinct(PositionModel.ticker)))
                # Fetch all results and extract the ticker from each row (which is a tuple)
                tickers_set = {row[0] for row in result.fetchall() if row[0]}
                logger.info(f"[DB] Found {len(tickers_set)} unique position tickers")
                return tickers_set
        except Exception as e:
            logger.error(f"[DB] Error fetching position tickers: {str(e)}")
            raise

    async def ticker_exists_in_positions(self, ticker: str) -> bool:
        """Check if a specific ticker exists in the positions table."""
        try:
            async with self.engine.connect() as conn:
                # Select 1 is slightly more efficient than selecting the ticker itself
                stmt = select(PositionModel.ticker).where(PositionModel.ticker == ticker).limit(1)
                result = await conn.execute(stmt)
                exists = result.scalar_one_or_none() is not None
                logger.debug(f"[DB] Check if ticker '{ticker}' exists in positions: {exists}")
                return exists
        except Exception as e:
            logger.error(f"[DB] Error checking position existence for ticker {ticker}: {str(e)}")
            # Decide on behavior: re-raise, or return False? Returning False might be safer UI-wise.
            return False # Return False on error to avoid incorrect green indicators

    async def screener_ticker_exists(self, ticker: str) -> bool:
        """Check if a specific ticker exists in the screener table."""
        ticker_upper = ticker.strip().upper()
        try:
            async with self.engine.connect() as conn:
                stmt = select(ScreenerModel.ticker).where(ScreenerModel.ticker == ticker_upper).limit(1)
                result = await conn.execute(stmt)
                exists = result.scalar_one_or_none() is not None
                logger.debug(f"[DB] Screener ticker '{ticker_upper}' exists check: {exists}")
                return exists
        except Exception as e:
            logger.error(f"[DB] Error checking screener existence for {ticker_upper}: {str(e)}")
            raise # Re-raise errors during check

    async def get_all_portfolio_rules(self) -> List[Dict[str, Any]]:
        """Fetch all portfolio rules from the database."""
        try:
            logger.info("[DB] Fetching all portfolio rules")
            async with self.engine.connect() as conn:
                stmt = select(PortfolioRuleModel).order_by(PortfolioRuleModel.rule_name, PortfolioRuleModel.id)
                result = await conn.execute(stmt)
                rules = result.mappings().all()
                logger.info(f"[DB] Found {len(rules)} portfolio rules")
                return [dict(rule) for rule in rules]
        except Exception as e:
            logger.error(f"[DB] Error fetching portfolio rules: {str(e)}")
            raise

    async def add_portfolio_rule(self, rule_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Add a new portfolio rule to the database from a dictionary."""
        try:
            # Use the dict directly
            insert_dict = rule_data.copy() # Use a copy to avoid modifying original if needed
            logger.info(f"[DB] Adding portfolio rule: {insert_dict.get('rule_name')}")

            # Handle boolean conversion explicitly again
            if 'is_active' in insert_dict:
                insert_dict['is_active'] = bool(int(insert_dict['is_active'])) if isinstance(insert_dict['is_active'], (str, int)) else bool(insert_dict['is_active'])
            else:
                insert_dict['is_active'] = True # Default if not provided

            async with self.engine.begin() as conn:
                stmt = insert(PortfolioRuleModel).values(**insert_dict)
                result = await conn.execute(stmt)
                inserted_id = result.inserted_primary_key[0]

                # Fetch the newly inserted rule to return it
                select_stmt = select(PortfolioRuleModel).where(PortfolioRuleModel.id == inserted_id)
                new_rule_result = await conn.execute(select_stmt)
                new_rule = new_rule_result.mappings().first()

                if new_rule and new_rule['is_active']:
                    await self._deactivate_other_rules(conn, new_rule['rule_name'], new_rule['id'])

                logger.info(f"[DB] Portfolio rule added with ID: {inserted_id}")
                return dict(new_rule) if new_rule else None # Return as dict
        except Exception as e:
            logger.error(f"[DB] Error adding portfolio rule: {str(e)}", exc_info=True)
            raise

    async def update_portfolio_rule(self, rule_id: int, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing portfolio rule using a dictionary of updates.""" 
        try:
            # Use the dict directly
            updates_dict = updates.copy()
            logger.info(f"[DB] Updating portfolio rule ID: {rule_id} with data: {updates_dict}")
            
            if not updates_dict:
                 logger.warning(f"[DB] No update data provided for rule ID {rule_id}. Returning None.")
                 return None 
            
            # Handle boolean conversion explicitly again
            if 'is_active' in updates_dict:
                 updates_dict['is_active'] = bool(int(updates_dict['is_active'])) if isinstance(updates_dict['is_active'], (str, int)) else bool(updates_dict['is_active'])

            updates_dict['updated_at'] = datetime.now() # Manually update timestamp

            async with self.engine.begin() as conn:
                stmt = update(PortfolioRuleModel).where(PortfolioRuleModel.id == rule_id).values(**updates_dict)
                result = await conn.execute(stmt)

                if result.rowcount == 0:
                    logger.warning(f"[DB] Portfolio rule ID {rule_id} not found for update.")
                    return None

                # Fetch the updated rule
                select_stmt = select(PortfolioRuleModel).where(PortfolioRuleModel.id == rule_id)
                updated_rule_result = await conn.execute(select_stmt)
                updated_rule = updated_rule_result.mappings().first()

                # If the rule was updated and set to active, deactivate others
                if updated_rule and updates_dict.get('is_active') is True:
                     await self._deactivate_other_rules(conn, updated_rule['rule_name'], rule_id)

                logger.info(f"[DB] Portfolio rule ID {rule_id} updated successfully.")
                return dict(updated_rule) if updated_rule else None # Return as dict

        except Exception as e:
            logger.error(f"[DB] Error updating portfolio rule ID {rule_id}: {str(e)}", exc_info=True)
            raise

    async def delete_portfolio_rule(self, rule_id: int) -> bool:
        """Delete a portfolio rule from the database."""
        try:
            logger.info(f"[DB] Deleting portfolio rule ID: {rule_id}")
            async with self.engine.begin() as conn:
                stmt = delete(PortfolioRuleModel).where(PortfolioRuleModel.id == rule_id)
                result = await conn.execute(stmt)
                deleted = result.rowcount > 0
                if deleted:
                    logger.info(f"[DB] Portfolio rule ID {rule_id} deleted successfully.")
                else:
                    logger.warning(f"[DB] Portfolio rule ID {rule_id} not found for deletion.")
                return deleted
        except Exception as e:
            logger.error(f"[DB] Error deleting portfolio rule ID {rule_id}: {str(e)}")
            raise

    async def _deactivate_other_rules(self, conn, rule_name: str, active_rule_id: int):
        """Internal helper to deactivate other rules with the same name."""
        logger.info(f"[DB] Deactivating other active rules named '{rule_name}' except ID {active_rule_id}")
        deactivation_stmt = update(PortfolioRuleModel).\
            where(
                PortfolioRuleModel.rule_name == rule_name,
                PortfolioRuleModel.id != active_rule_id,
                PortfolioRuleModel.is_active == True # Use True for Boolean
            ).\
            values(is_active=False, updated_at=datetime.now()) # Use False for Boolean
        await conn.execute(deactivation_stmt)
        logger.info(f"[DB] Deactivation complete for rules named '{rule_name}'.")


    # --- NEW Method to Clear Finviz Raw Data ---
    async def clear_finviz_raw_data(self) -> None:
        """Deletes all records from the finviz_raw table."""
        logger.info("[DB] Clearing all data from finviz_raw table.")
        try:
            async with self.engine.begin() as conn:
                stmt = delete(FinvizRawDataModel) # Delete all rows from the model's table
                await conn.execute(stmt)
            logger.info("[DB] finviz_raw table cleared successfully.")
        except Exception as e:
            logger.error(f"[DB] Error clearing finviz_raw table: {e}", exc_info=True)
            raise # Re-raise the exception after logging
    # --- End Method to Clear Finviz Raw Data ---

    # --- NEW Method to Get All Finviz Raw Data ---
    async def get_all_finviz_raw_data(self) -> List[Dict[str, Any]]:
        """Fetches all records (ticker, raw_data) from the finviz_raw table."""
        logger.info("[DB] Fetching all data from finviz_raw table.")
        try:
            async with self.engine.connect() as conn:
                stmt = select(FinvizRawDataModel.ticker, FinvizRawDataModel.raw_data)
                result = await conn.execute(stmt)
                rows = result.mappings().all()
                logger.info(f"[DB] Fetched {len(rows)} records from finviz_raw.")
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"[DB] Error fetching from finviz_raw table: {e}", exc_info=True)
            raise
    # --- End Method to Get All Finviz Raw Data ---

    # --- NEW Method to Get Analytics Raw Data by Source ---
    async def get_analytics_raw_data_by_source(self, source_filter: str) -> List[Dict[str, Any]]:
        """Fetches all records from the analytics_raw table, filtered by a specific source."""
        logger.info(f"[DB Analytics Raw] Fetching data from analytics_raw table for source: {source_filter}")
        try:
            async with self.engine.connect() as conn:
                stmt = select(
                    AnalyticsRawDataModel.ticker,
                    AnalyticsRawDataModel.source,
                    AnalyticsRawDataModel.raw_data,
                    AnalyticsRawDataModel.last_fetched_at
                ).where(AnalyticsRawDataModel.source == source_filter)
                
                result = await conn.execute(stmt)
                rows = result.mappings().all() # Get results as dict-like rows
                logger.info(f"[DB Analytics Raw] Fetched {len(rows)} records for source '{source_filter}'.")
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"[DB Analytics Raw] Error fetching from analytics_raw for source '{source_filter}': {e}", exc_info=True)
            raise # Re-raise the exception after logging
    # --- End Method to Get Analytics Raw Data by Source ---

    # --- NEW Method for Analytics Raw Data Save/Update ---
    async def save_or_update_analytics_raw_data(self, ticker: str, source: str, raw_data: str) -> None:
        """Saves or updates raw analytics data for a specific ticker and source."""
        logger.info(f"[DB Analytics Raw] Saving/Updating data for ticker: {ticker}, source: {source}")
        if not ticker or not source:
            logger.error("[DB Analytics Raw] Ticker and Source cannot be empty.")
            return
        try:
            async with self.engine.begin() as conn:
                data_to_insert = {
                    'ticker': ticker,
                    'source': source,
                    'raw_data': raw_data,
                    'last_fetched_at': datetime.now()
                }
                # Use sqlite_insert for UPSERT functionality
                stmt = sqlite_insert(AnalyticsRawDataModel).values(data_to_insert)
                
                # Define what to do on conflict (composite key: ticker, source)
                # Update raw_data and last_fetched_at
                update_dict = {
                    'raw_data': stmt.excluded.raw_data, 
                    'last_fetched_at': stmt.excluded.last_fetched_at
                }
                
                stmt = stmt.on_conflict_do_update(
                    index_elements=['ticker', 'source'], # Conflict on the composite primary key
                    set_=update_dict
                )
                
                await conn.execute(stmt)
            logger.info(f"[DB Analytics Raw] Successfully saved/updated data for {ticker} from {source}")
        except Exception as e:
            logger.error(f"[DB Analytics Raw] Error saving/updating data for {ticker} from {source}: {e}", exc_info=True)
            raise
    # --- End Analytics Raw Data Save/Update ---

    # --- Add Finviz Raw Data Save/Update ---
    async def save_or_update_finviz_raw_data(self, ticker: str, raw_data: str) -> None:
        """Saves or updates raw Finviz data for a ticker."""
        logger.info(f"[DB] Saving/Updating Finviz raw data for ticker: {ticker}")
        try:
            async with self.engine.begin() as conn:
                # Prepare data dictionary, including the timestamp
                data_to_insert = {
                    'ticker': ticker,
                    'raw_data': raw_data,
                    'last_fetched_at': datetime.now()
                }
                # Use sqlite_insert for UPSERT functionality
                stmt = sqlite_insert(FinvizRawDataModel).values(data_to_insert)
                
                # Define what to do on conflict (ticker exists)
                # Update raw_data and last_fetched_at
                update_dict = {
                    'raw_data': stmt.excluded.raw_data, 
                    'last_fetched_at': stmt.excluded.last_fetched_at
                }
                
                stmt = stmt.on_conflict_do_update(
                    index_elements=['ticker'], # Conflict on the primary key
                    set_=update_dict
                )
                
                await conn.execute(stmt)
            logger.info(f"[DB] Successfully saved/updated Finviz raw data for {ticker}")
        except Exception as e:
            logger.error(f"[DB] Error saving/updating Finviz raw data for {ticker}: {e}", exc_info=True)
            raise
    # --- End Finviz Raw Data Save/Update ---

    # --- NEW Method to get active order configurations ---
    async def get_all_active_order_configs(self) -> Dict[str, Dict[str, float]]:
        """
        Fetches the latest order configuration (stop_price, limit_price, limit_offset)
        for each ticker where all three values are non-null.
        Returns a dictionary mapping ticker -> {'stop_price': float, 'limit_price': float, 'limit_offset': float}.
        """
        logger.info("[DB] Fetching latest active order configurations (stop/limit/offset).")
        configs = {}
        try:
            async with self.engine.connect() as conn:
                # Use a CTE with row_number() to get the latest order per ticker that meets the criteria
                ranked_orders_cte = (
                    select(
                        OrderModel.ticker,
                        OrderModel.stop_price,
                        OrderModel.trailing_amount, # Select trailing_amount
                        OrderModel.limit_offset,
                        # Use func.row_number() for better integration
                        func.row_number().over(partition_by=OrderModel.ticker, order_by=OrderModel.last_update.desc()).label('rn')
                    )
                    .where(
                        and_(
                            OrderModel.ticker.isnot(None),
                            OrderModel.ticker != '',
                            OrderModel.stop_price.isnot(None),
                            OrderModel.trailing_amount.isnot(None), # Check trailing_amount
                            OrderModel.limit_offset.isnot(None)
                        )
                    )
                    .cte("ranked_orders_cte")
                )

                # Select from the CTE where row_number is 1
                stmt = select(
                    ranked_orders_cte.c.ticker,
                    ranked_orders_cte.c.stop_price,
                    ranked_orders_cte.c.trailing_amount,
                    ranked_orders_cte.c.limit_offset
                ).where(ranked_orders_cte.c.rn == 1)

                result = await conn.execute(stmt)
                rows = result.mappings().all() # Get results as dict-like rows

                for row in rows:
                    ticker = row['ticker']
                    configs[ticker] = {
                        'stop_price': row['stop_price'],
                        'trailing_amount': row['trailing_amount'],
                        'limit_offset': row['limit_offset']
                    }

            logger.info(f"[DB] Found {len(configs)} tickers with active order configurations.")
            return configs
        except Exception as e:
            logger.error(f"[DB] Error fetching active order configurations: {e}", exc_info=True)
            return {} # Return empty dict on error
    # --- End NEW Method ---

    # --- NEW Method to Get Target Currencies --- 
    async def get_target_currencies(self) -> List[str]:
        """Fetches the distinct currency keys from the exchange_rates table."""
        logger.info("[DB] Fetching target currencies from exchange_rates table.")
        target_currencies = []
        try:
            # Create a session for this specific operation
            async with AsyncSession(self.engine) as session:
                # Call the existing standalone function
                rates_dict = await get_exchange_rates(session)
                # Extract the keys (currency codes)
                target_currencies = list(rates_dict.keys())
                logger.info(f"[DB] Found target currencies: {target_currencies}")
        except Exception as e:
            logger.error(f"[DB] Error fetching target currencies: {e}", exc_info=True)
            # Return empty list or re-raise depending on desired error handling
            # raise
        return target_currencies
    # --- End NEW Method --- 

    def update_exchange_rates_sync(self, rates: Dict[str, float]):
        """Synchronously updates exchange rates in the database using sqlite3."""
        if not rates:
            logger.info("[DB Sync Update Rates] No rates provided to update.")
            return

        logger.info(f"[DB Sync Update Rates] Updating rates for: {list(rates.keys())}")
        # Extract the raw DB path from the database_url (e.g., 'sqlite+aiosqlite:///path/to/db.db')
        db_path = None
        if self.database_url.startswith("sqlite+aiosqlite:///"):
            db_path = self.database_url.split("///", 1)[1]
        elif self.database_url.startswith("sqlite:///"): # Handle non-async URL prefix too
            db_path = self.database_url.split("///", 1)[1]

        if not db_path or ":memory:" in db_path:
            logger.error("[DB Sync Update Rates] Cannot determine valid database file path from URL for synchronous update.")
            return

        conn = None
        try:
            conn = sqlite3.connect(db_path, timeout=10) # Add timeout
            cursor = conn.cursor()
            updates_made = 0
            for currency, rate in rates.items():
                try:
                    # Use standard SQL UPSERT syntax for SQLite
                    sql = """
                    INSERT INTO exchange_rates (currency, rate)
                    VALUES (?, ?)
                    ON CONFLICT(currency) DO UPDATE SET rate=excluded.rate;
                    """
                    cursor.execute(sql, (currency, rate))
                    updates_made += cursor.rowcount # Count successful upserts
                except sqlite3.Error as single_err:
                    logger.error(f"[DB Sync Update Rates] Error upserting rate for {currency}: {single_err}")
                    # Continue with other currencies

            conn.commit()
            logger.info(f"[DB Sync Update Rates] Finished updating rates. {updates_made} rows affected.")

        except sqlite3.Error as e:
            logger.error(f"[DB Sync Update Rates] Database error: {e}", exc_info=True)
            if conn:
                conn.rollback()
        except Exception as e:
             logger.error(f"[DB Sync Update Rates] Unexpected error: {e}", exc_info=True)
             if conn:
                 conn.rollback()
        finally:
            if conn:
                conn.close()

    async def get_portfolio_rule_by_id(self, rule_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single portfolio rule by its ID."""
        logger.info(f"[DB] Fetching portfolio rule ID: {rule_id}")
        try:
            async with self.engine.connect() as conn:
                stmt = select(PortfolioRuleModel).where(PortfolioRuleModel.id == rule_id)
                result = await conn.execute(stmt)
                rule = result.mappings().first()
                if rule:
                    logger.info(f"[DB] Found portfolio rule ID: {rule_id}")
                    return dict(rule)
                else:
                    logger.warning(f"[DB] Portfolio rule ID {rule_id} not found.")
                    return None
        except Exception as e:
            logger.error(f"[DB] Error fetching portfolio rule ID {rule_id}: {str(e)}", exc_info=True)
            raise

    async def get_job_config_str(self, job_id: str) -> Optional[str]:
        """Get the raw schedule JSON string for a job configuration by ID."""
        try:
            async with self.engine.connect() as conn:
                stmt = select(JobConfigModel.schedule).where(JobConfigModel.job_id == job_id)
                result = await conn.execute(stmt)
                schedule_str = result.scalar_one_or_none()
                if schedule_str:
                    # Log only a snippet for brevity if the string is long
                    log_schedule_str = schedule_str if len(schedule_str) < 70 else schedule_str[:67] + "..."
                    logger.info(f"[DB] Found schedule string for job_id '{job_id}': '{log_schedule_str}'")
                else:
                    logger.info(f"[DB] No schedule string found for job_id '{job_id}'.")
                return schedule_str
        except Exception as e:
            logger.error(f"[DB] Error fetching schedule string for job_id '{job_id}': {str(e)}", exc_info=True)
            return None # Return None on error to allow calling code to handle default/creation

    async def save_job_config(self, job_config_data: Dict[str, Any]) -> None:
        """Save or update job configuration using SQLite upsert (for generic schedules)."""
        try:
            # Ensure updated_at is set
            job_config_data['updated_at'] = datetime.now()
            # Ensure necessary fields exist for insert, providing defaults
            job_config_data.setdefault('job_type', 'data_fetch')
            job_config_data.setdefault('is_active', 1)
            job_config_data.setdefault('created_at', job_config_data['updated_at'])
            
            # Create the initial insert statement
            stmt = sqlite_insert(JobConfigModel).values(**job_config_data)
            
            # Define the fields to update on conflict (exclude primary key and created_at)
            update_fields = {
                c.name: getattr(stmt.excluded, c.name) # Use getattr for excluded columns
                for c in JobConfigModel.__table__.columns
                if c.name not in ['id', 'job_id', 'created_at'] # Don't update id, job_id, or created_at
            }
            
            # Add the ON CONFLICT DO UPDATE clause
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=['job_id'], # The unique column causing the conflict
                set_=update_fields
            )
            
            async with self.engine.begin() as conn:
                await conn.execute(upsert_stmt)
            logger.info(f"[DB] Saved/Updated job config for: {job_config_data.get('job_id')}")
        except Exception as e:
            logger.error(f"Error saving/updating job config: {str(e)}", exc_info=True)
            raise

    async def save_persistent_job_state(self, job_data: Dict[str, Any]):
        """Saves or updates the state of a job in the persistent_job_states table."""
        sql = """
        INSERT OR REPLACE INTO persistent_job_states (
            job_id, status, last_completion_time, last_run_summary,
            total_count, successful_count, failed_count, job_specific_data, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        # Ensure all fields are present, providing defaults if necessary
        params = (
            job_data.get("job_id"),
            job_data.get("status"),
            job_data.get("last_completion_time"), # Should be ISO string
            job_data.get("last_run_summary"),
            job_data.get("total_count"),
            job_data.get("successful_count"),
            job_data.get("failed_count"),
            job_data.get("job_specific_data"), # e.g., JSON string of extra details
            job_data.get("updated_at", datetime.now().isoformat()) # Should be ISO string
        )
        db_path = await self.get_db_path()
        try:
            async with aiosqlite.connect(db_path) as conn:
                await conn.execute(sql, params)
                await conn.commit()
            logger.info(f"Persistent job state saved for job_id: {job_data.get('job_id')}")
        except aiosqlite.Error as e:
            logger.error(f"Error saving persistent job state for job_id {job_data.get('job_id')}: {e}", exc_info=True)
            raise # Re-raise the aiosqlite.Error
        except Exception as e: # Catch any other potential errors during the operation
            logger.error(f"Unexpected error saving persistent job state for job_id {job_data.get('job_id')}: {e}", exc_info=True)
            raise # Re-raise any other exception

    async def get_persistent_job_state(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves the persisted state of a job from the persistent_job_states table."""
        sql = "SELECT * FROM persistent_job_states WHERE job_id = ?;"
        db_path = await self.get_db_path()
        try:
            async with aiosqlite.connect(db_path) as conn:
                conn.row_factory = aiosqlite.Row # Access columns by name
                async with conn.execute(sql, (job_id,)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        logger.debug(f"Persistent job state found for job_id: {job_id}")
                        return dict(row) # Convert row object to dict
            logger.debug(f"No persistent job state found for job_id: {job_id}")
            return None
        except aiosqlite.Error as e:
            logger.error(f"Error retrieving persistent job state for job_id {job_id}: {e}", exc_info=True)
            return None

    # <<< START NEW CACHE METHODS >>>

    async def update_cached_analytics_data(self, data_json: str) -> None:
        """
        Updates or inserts the cached analytics data.
        The cache is designed to hold a single entry with id=1.
        """
        cache_id = 1
        try:
            async with self.async_session_factory() as session: # MODIFIED
                async with session.begin():
                    # Try to get existing cache entry
                    stmt_select = select(CachedAnalyticsDataModel).filter_by(id=cache_id)
                    result = await session.execute(stmt_select)
                    cache_entry = result.scalar_one_or_none()

                    current_time = datetime.now()
                    if cache_entry:
                        # Update existing entry
                        cache_entry.data_json = data_json
                        cache_entry.generated_at = current_time # Explicitly set, though onupdate should also work
                        logging.info(f"Updating cached analytics data (id={cache_id}).")
                    else:
                        # Insert new entry
                        cache_entry = CachedAnalyticsDataModel(
                            id=cache_id,
                            data_json=data_json,
                            generated_at=current_time
                        )
                        session.add(cache_entry)
                        logging.info(f"Inserting new cached analytics data (id={cache_id}).")
                    await session.commit()
        except IntegrityError as e: # Should not happen if logic ensures id=1 and UniqueConstraint is on id
            logging.error(f"Integrity error updating cached analytics data (id={cache_id}): {e}. This might indicate a race condition or schema issue if multiple entries are attempted for id 1.")
            # Depending on how strict, might re-raise or just log.
        except Exception as e:
            logging.error(f"Error updating cached analytics data (id={cache_id}): {e}")
            # Consider re-raising if the operation is critical and failure should halt process
            # raise # Re-raise the exception if needed

    async def get_cached_analytics_data(self) -> Optional[Tuple[str, datetime]]:
        """
        Retrieves the cached analytics data (id=1).
        Returns a tuple of (data_json, generated_at) or None if not found.
        """
        cache_id = 1
        try:
            async with self.async_session_factory() as session: # MODIFIED
                async with session.begin(): # begin_nested might be an option if part of larger transaction
                    stmt = select(CachedAnalyticsDataModel.data_json, CachedAnalyticsDataModel.generated_at).filter_by(id=cache_id)
                    result = await session.execute(stmt)
                    row = result.one_or_none()
                    if row:
                        logging.debug(f"Retrieved cached analytics data (id={cache_id}) generated at {row.generated_at.isoformat()}.")
                        return row.data_json, row.generated_at
                    else:
                        logging.info(f"No cached analytics data found for id={cache_id}.")
                        return None
        except Exception as e:
            logging.error(f"Error getting cached analytics data (id={cache_id}): {e}")
            return None # Or re-raise based on error handling strategy

    async def update_cached_analytics_metadata(self, metadata_json: str) -> None:
        """
        Updates or inserts the cached analytics metadata.
        The cache is designed to hold a single entry with id=1.
        """
        cache_id = 1
        try:
            async with self.async_session_factory() as session: # MODIFIED
                async with session.begin():
                    stmt_select = select(CachedAnalyticsMetadataModel).filter_by(id=cache_id)
                    result = await session.execute(stmt_select)
                    cache_entry = result.scalar_one_or_none()

                    current_time = datetime.now()
                    if cache_entry:
                        cache_entry.metadata_json = metadata_json
                        cache_entry.generated_at = current_time
                        logging.info(f"Updating cached analytics metadata (id={cache_id}).")
                    else:
                        cache_entry = CachedAnalyticsMetadataModel(
                            id=cache_id,
                            metadata_json=metadata_json,
                            generated_at=current_time
                        )
                        session.add(cache_entry)
                        logging.info(f"Inserting new cached analytics metadata (id={cache_id}).")
                    await session.commit()
        except IntegrityError as e:
             logging.error(f"Integrity error updating cached analytics metadata (id={cache_id}): {e}.")
        except Exception as e:
            logging.error(f"Error updating cached analytics metadata (id={cache_id}): {e}")
            # raise

    async def get_cached_analytics_metadata(self) -> Optional[Tuple[str, datetime]]:
        """
        Retrieves the cached analytics metadata (id=1).
        Returns a tuple of (metadata_json, generated_at) or None if not found.
        """
        cache_id = 1
        try:
            async with self.async_session_factory() as session: # MODIFIED
                async with session.begin():
                    stmt = select(CachedAnalyticsMetadataModel.metadata_json, CachedAnalyticsMetadataModel.generated_at).filter_by(id=cache_id)
                    result = await session.execute(stmt)
                    row = result.one_or_none()
                    if row:
                        logging.debug(f"Retrieved cached analytics metadata (id={cache_id}) generated at {row.generated_at.isoformat()}.")
                        return row.metadata_json, row.generated_at
                    else:
                        logging.info(f"No cached analytics metadata found for id={cache_id}.")
                        return None
        except Exception as e:
            logging.error(f"Error getting cached analytics metadata (id={cache_id}): {e}")
            return None
            
    # <<< END NEW CACHE METHODS >>>

# --- Database Initialization and Utility Functions ---
async def get_exchange_rates(session):
    try:
        result = await session.execute(exchange_rates.select())
        # Access Row elements by index (0 for currency, 1 for rate)
        return {row[0]: row[1] for row in result.fetchall()}
    except Exception as e:
        print(f"Error fetching exchange rates: {e}")
        return {}

# Method to update/insert exchange rates (Upsert)
async def update_exchange_rate(session, currency, rate):
    try:
        # Use SQLite dialect for ON CONFLICT
        stmt = sqlite_insert(exchange_rates).values(currency=currency, rate=rate)
        stmt = stmt.on_conflict_do_update(
            index_elements=['currency'], # Specify the primary key constraint
            set_=dict(rate=rate)          # Fields to update on conflict
        )
        await session.execute(stmt)
        await session.commit()
        logger.info(f"[DB] Upserted exchange rate for {currency} to {rate}")
    except Exception as e:
        logger.error(f"Error upserting exchange rate for {currency}: {e}")
        await session.rollback() 

# --- NEW Function to Add/Update Exchange Rate with ConID ---
async def add_or_update_exchange_rate_conid(session: AsyncSession, currency: str, conid: str) -> Dict[str, str]:
    """
    Adds or updates an exchange rate entry, specifically handling the ConID.

    Logic:
    1. Check if currency + conid already exists. If yes, do nothing (skip).
    2. Check if currency exists (regardless of conid).
       - If yes and conid is NULL/empty: Update the existing record's conid.
       - If yes and conid has a *different* value: Log warning and skip update (conflict).
    3. If currency does not exist: Insert a new record with currency, conid, and a default rate of 1.0.

    Args:
        session: The AsyncSession for database operations.
        currency: The currency code (e.g., 'USD').
        conid: The IBKR Contract ID.

    Returns:
        A dictionary containing 'status' and 'message'.
        Possible statuses: 'skipped', 'updated', 'conflict', 'inserted', 'error'.
    """
    currency_upper = currency.strip().upper()
    logger.info(f"[DB Add/Update Rate ConID] Processing: Currency={currency_upper}, ConID={conid}")

    try:
        # Define table name and columns directly
        tbl_name = 'exchange_rates'
        col_currency = 'currency'
        col_conid = 'conid'
        col_rate = 'rate'

        # --- Use Core select, update, insert --- 
        # 1. Check if currency + conid exists
        stmt_check_exact = text(f"SELECT {col_currency}, {col_conid}, {col_rate} FROM {tbl_name} WHERE {col_currency} = :curr AND {col_conid} = :conid")
        result_exact = await session.execute(stmt_check_exact, {"curr": currency_upper, "conid": conid})
        if result_exact.first() is not None:
            logger.info(f"[DB Add/Update Rate ConID] Record for {currency_upper} with ConID {conid} already exists. Skipping.")
            return {"status": "skipped", "message": f"Record for {currency_upper} with ConID {conid} already exists."}

        # 2. Check if currency exists (without specific conid)
        stmt_check_currency = text(f"SELECT {col_currency}, {col_conid}, {col_rate} FROM {tbl_name} WHERE {col_currency} = :curr")
        result_currency = await session.execute(stmt_check_currency, {"curr": currency_upper})
        existing_rate_row = result_currency.first() # Fetch the first matching row (as a tuple)

        if existing_rate_row is not None:
            # Currency exists, check its conid (assuming conid is the second column selected, index 1)
            existing_conid = existing_rate_row[1] 
            
            # --- NEW LOGIC: Always update if currency exists (unless exact match already caught) ---
            if existing_conid != conid: 
                logger.info(f"[DB Add/Update Rate ConID] Updating ConID for existing currency {currency_upper} from {existing_conid} to {conid}.")
            else: # existing_conid is None or matches the new conid
                logger.info(f"[DB Add/Update Rate ConID] Setting/Updating ConID for existing currency {currency_upper} to {conid}.")
            
            stmt_update = text(f"UPDATE {tbl_name} SET {col_conid} = :conid WHERE {col_currency} = :curr")
            await session.execute(stmt_update, {"conid": conid, "curr": currency_upper})
            await session.commit()
            # Return "updated" status in both cases (new conid or overwriting different conid)
            return {"status": "updated", "message": f"Updated ConID for {currency_upper}."}
            # --- END NEW LOGIC ---
            
            # --- REMOVE OLD CONFLICT/UPDATE LOGIC --- 
            # # if existing_conid is None or existing_conid == '':
            # #     # Update existing record with the new conid
            # #     logger.info(f"[DB Add/Update Rate ConID] Updating ConID for existing currency {currency_upper} to {conid}.")
            # #     stmt_update = text(f"UPDATE {tbl_name} SET {col_conid} = :conid WHERE {col_currency} = :curr")
            # #     await session.execute(stmt_update, {"conid": conid, "curr": currency_upper})
            # #     await session.commit()
            # #     return {"status": "updated", "message": f"Updated ConID for {currency_upper}."}
            # # elif existing_conid != conid:
            # #     # Currency exists but with a different ConID. Log warning and skip.
            # #     logger.warning(f"[DB Add/Update Rate ConID] Currency {currency_upper} exists but with different ConID ({existing_conid}). Update with {conid} skipped.")
            # #     return {"status": "conflict", "message": f"Record for {currency_upper} exists with a different ConID. Update skipped."}
            # # else:
            # #      # Logically shouldn't happen, but handle defensively.
            # #      logger.info(f"[DB Add/Update Rate ConID] Record for {currency_upper} with ConID {conid} confirmed to exist. Skipping.")
            # #      return {"status": "skipped", "message": f"Record for {currency_upper} with ConID {conid} already exists."}
            # --- END REMOVE OLD --- 

        else:
            # 3. Currency does not exist, insert new record
            logger.info(f"[DB Add/Update Rate ConID] Inserting new record for {currency_upper} with ConID {conid}.")
            stmt_insert = text(f"INSERT INTO {tbl_name} ({col_currency}, {col_conid}, {col_rate}) VALUES (:curr, :conid, :rate)")
            await session.execute(stmt_insert, {"curr": currency_upper, "conid": conid, "rate": 1.0})
            await session.commit()
            return {"status": "inserted", "message": f"Inserted new record for {currency_upper}."}

    except SQLAlchemyError as e: # Catch specific SQLAlchemy errors
        await session.rollback()
        logger.error(f"[DB Add/Update Rate ConID] Database error for {currency_upper}/{conid}: {e}", exc_info=True)
        return {"status": "error", "message": f"Database error: {e}"}
    except Exception as e: # Catch any other unexpected errors
        await session.rollback()
        logger.error(f"[DB Add/Update Rate ConID] Unexpected error for {currency_upper}/{conid}: {e}", exc_info=True)
        return {"status": "error", "message": f"Unexpected error: {e}"}


    # --- NEW: Method to fetch a single screener ticker --- 
    async def get_screener_ticker(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetches a single ticker record from the screener table."""
        ticker_upper = ticker.strip().upper()
        logger.debug(f"[DB] Fetching screener data for ticker: {ticker_upper}")
        try:
            async with self.engine.connect() as conn:
                stmt = select(ScreenerModel).where(ScreenerModel.ticker == ticker_upper).limit(1)
                result = await conn.execute(stmt)
                row = result.mappings().first()
                if row:
                    logger.debug(f"[DB] Found screener data for {ticker_upper}.")
                    return dict(row)
                else:
                    logger.debug(f"[DB] Ticker {ticker_upper} not found in screener.")
                    return None
        except Exception as e:
            logger.error(f"[DB] Error fetching screener ticker {ticker_upper}: {e}", exc_info=True)
            return None # Return None on error 

    # --- NEW: Method to update multiple screener fields --- 
    async def update_screener_multi_fields(self, ticker: str, updates: Dict[str, Any]) -> bool:
        """Updates multiple fields for a ticker in the screener table and the updated_at timestamp."""
        ticker_upper = ticker.strip().upper()
        if not updates:
            logger.warning(f"[DB Multi Update] No updates provided for ticker {ticker_upper}. Skipping.")
            return False

        # Ensure updated_at is always included
        updates['updated_at'] = datetime.now()

        logger.debug(f"[DB Multi Update] Updating screener for {ticker_upper} with: {updates}")
        try:
            async with self.engine.begin() as conn:
                stmt = (
                    update(ScreenerModel)
                    .where(ScreenerModel.ticker == ticker_upper)
                    .values(**updates)
                )
                result = await conn.execute(stmt)
                if result.rowcount > 0:
                    logger.debug(f"[DB Multi Update] Successfully updated {len(updates)-1} fields for {ticker_upper}.")
                    return True
                else:
                    logger.warning(f"[DB Multi Update] Ticker {ticker_upper} not found for multi-field update.")
                    return False
        except Exception as e:
            logger.error(f"[DB Multi Update] Error updating multiple fields for {ticker_upper}: {e}", exc_info=True)
            return False # Return False on error 

    # --- NEW: Add/Update Screener Entry --- 
    async def save_screener_entry(self, data: Dict[str, Any]) -> Dict[str, str]:
        """
        Adds a new entry to the screener table or updates an existing one 
        based on the 'ticker', ensuring ConID and other fields are saved/updated.

        Args:
            data: A dictionary containing form data, MUST include 'ticker' and 'status'.
                  May include 'conid', 'atr', 'atr_mult', 'risk', 'beta', 
                  'sector', 'industry', 'comments', etc.

        Returns:
            A dictionary with 'status' ('inserted', 'updated', 'error') 
            and 'message'.
        """
        ticker = data.get('ticker')
        status = data.get('status')

        if not ticker or not ticker.strip():
            return {"status": "error", "message": "Missing or empty required field: ticker."}
        if not status:
             return {"status": "error", "message": "Missing required field: status."}

        ticker_upper = ticker.strip().upper()
        conid = data.get('conid') # Optional but often provided

        logger.info(f"[DB Add/Update Screener] Processing: Ticker={ticker_upper}, Status={status}, ConID={conid}")

        # Use the repository's engine
        async with self.engine.begin() as conn: # Use conn instead of session
            try:
                # Define table and columns based on ScreenerModel
                # Use ticker as the primary key column
                col_ticker = ScreenerModel.ticker.name # Get column name string
                col_status = ScreenerModel.status.name
                col_conid = ScreenerModel.conid.name
                col_created_at = ScreenerModel.created_at.name
                col_updated_at = ScreenerModel.updated_at.name
                
                # Map form keys (data keys) to model column names
                # Only include columns present in the model
                optional_cols_map = {
                    'atr': ScreenerModel.atr.name,
                    'atr_mult': ScreenerModel.atr_mult.name,
                    'risk': ScreenerModel.risk.name,
                    'beta': ScreenerModel.beta.name,
                    'sector': ScreenerModel.sector.name,
                    'industry': ScreenerModel.industry.name,
                    'comments': ScreenerModel.comments.name,
                    'Company': ScreenerModel.Company.name, # Added Company
                    'open_pos': ScreenerModel.open_pos.name, # Added open_pos
                    'cost_base': ScreenerModel.cost_base.name, # Added cost_base
                    'currency': ScreenerModel.currency.name, # Added currency
                    'acc': ScreenerModel.acc.name, # Added acc
                    'price': ScreenerModel.price.name, # Added price
                    'daychange': ScreenerModel.daychange.name, # Added daychange
                    't_source1': ScreenerModel.t_source1.name # Added t_source1
                    # Add other ScreenerModel fields here if needed
                }

                # Check if ticker exists using SQLAlchemy Core Select
                stmt_check = select(ScreenerModel.ticker).where(ScreenerModel.ticker == ticker_upper)
                result_check = await conn.execute(stmt_check)
                exists = result_check.scalar_one_or_none() is not None

                now = datetime.now()
                
                values_to_process = {col_status: status, col_conid: conid} # Start with required/common fields
                
                # Prepare optional fields, handling types and empty strings
                for form_key, db_col in optional_cols_map.items():
                    if form_key in data: # Check if the key exists in the input data
                        value = data[form_key]
                        target_col = ScreenerModel.__table__.columns[db_col]
                        target_type = target_col.type.python_type

                        if value is None or value == '':
                             # For numeric types, store None if input is empty/None
                             if target_type in (float, int):
                                 values_to_process[db_col] = None
                             # For string types, store None if input is empty/None (or store empty string if desired)
                             elif target_type == str:
                                 values_to_process[db_col] = None # Store NULL for empty strings
                             else:
                                  values_to_process[db_col] = None # Default to None for other types
                        else:
                            # Attempt conversion for numeric types
                            if target_type in (float, int):
                                try:
                                    # Be flexible: try float first, then int if needed
                                    float_val = float(value)
                                    values_to_process[db_col] = int(float_val) if target_type == int else float_val
                                except (ValueError, TypeError):
                                    logger.warning(f"[DB Add/Update Screener] Could not convert {form_key}='{value}' to {target_type.__name__} for {ticker_upper}. Storing NULL.")
                                    values_to_process[db_col] = None
                            elif target_type == str:
                                values_to_process[db_col] = str(value) # Ensure string
                            else:
                                # Handle other types if necessary (e.g., boolean, datetime)
                                values_to_process[db_col] = value # Assume correct type for now

                if exists:
                    # --- UPDATE Logic --- 
                    logger.info(f"[DB Add/Update Screener] Ticker '{ticker_upper}' exists. Updating.")
                    values_to_process[col_updated_at] = now # Set update timestamp
                    
                    # Use SQLAlchemy Core Update
                    stmt_update = (
                        update(ScreenerModel)
                        .where(ScreenerModel.ticker == ticker_upper)
                        .values(**values_to_process) # Pass prepared dictionary
                    )
                    await conn.execute(stmt_update)
                    return {"status": "updated", "message": f"Updated entry for {ticker_upper}."}

                else:
                    # --- INSERT Logic --- 
                    logger.info(f"[DB Add/Update Screener] Ticker '{ticker_upper}' not found. Inserting.")
                    values_to_process[col_ticker] = ticker_upper # Add ticker for insert
                    values_to_process[col_created_at] = now # Set create timestamp
                    values_to_process[col_updated_at] = now # Also set update timestamp on insert

                    # Use SQLAlchemy Core Insert
                    stmt_insert = insert(ScreenerModel).values(**values_to_process)
                    await conn.execute(stmt_insert)
                    return {"status": "inserted", "message": f"Inserted new entry for {ticker_upper}."}

            except SQLAlchemyError as e:
                # Rollback is handled by engine.begin() context manager on exception
                logger.error(f"[DB Add/Update Screener] Database error for {ticker_upper}: {e}", exc_info=True)
                return {"status": "error", "message": f"Database error: {e}"}
            except Exception as e:
                # Rollback is handled by engine.begin() context manager on exception
                logger.error(f"[DB Add/Update Screener] Unexpected error for {ticker_upper}: {e}", exc_info=True)
                return {"status": "error", "message": f"Unexpected error: {e}"}
    # --- End Add/Update Screener --- 
    
# Example Usage (Conceptual)

# --- ADDED SYNC FUNCTIONS FOR CONID FETCHING (Placed outside the class) ---
def get_screener_tickers_and_conids_sync(db_path: str) -> List[Dict[str, Any]]:
    """Synchronously fetches tickers and valid conids from the screener table."""
    results = []
    logger.info(f"[DB Sync Screener Conids] Fetching tickers and conids from: {db_path}")
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        cursor = conn.cursor()
        # Select ticker and conid where conid is not NULL and not empty
        cursor.execute("SELECT ticker, conid FROM screener WHERE conid IS NOT NULL AND conid != ''")
        rows = cursor.fetchall()
        for row in rows:
            ticker, conid_str = row
            try:
                # Attempt to convert conid to int, skip if invalid
                conid_int = int(conid_str)
                if conid_int > 0: # Ensure conid is positive
                    results.append({'ticker': ticker, 'conid': conid_int})
                else:
                     logger.warning(f"[DB Sync Screener Conids] Skipping ticker {ticker}: Non-positive conid value '{conid_str}'.")
            except (ValueError, TypeError):
                logger.warning(f"[DB Sync Screener Conids] Skipping ticker {ticker}: Could not convert conid '{conid_str}' to a valid integer.")
        logger.info(f"[DB Sync Screener Conids] Found {len(results)} valid ticker/conid pairs.")
    except sqlite3.Error as e:
        logger.error(f"[DB Sync Screener Conids] Database error: {e}", exc_info=True)
    except Exception as e:
         logger.error(f"[DB Sync Screener Conids] Unexpected error: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    return results

def get_exchange_rates_and_conids_sync(db_path: str) -> List[Dict[str, Any]]:
    """Synchronously fetches currencies and valid conids from the exchange_rates table."""
    results = []
    logger.info(f"[DB Sync FX Conids] Fetching currencies and conids from: {db_path}")
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        cursor = conn.cursor()
        # Select currency and conid where conid is not NULL and not empty
        # Assuming the table has 'currency' and 'conid' columns
        cursor.execute("SELECT currency, conid FROM exchange_rates WHERE conid IS NOT NULL AND conid != ''")
        rows = cursor.fetchall()
        for row in rows:
            currency, conid_str = row
            try:
                # Attempt to convert conid to int, skip if invalid
                conid_int = int(conid_str)
                if conid_int > 0: # Ensure conid is positive
                    results.append({'currency': currency, 'conid': conid_int})
                else:
                     logger.warning(f"[DB Sync FX Conids] Skipping currency {currency}: Non-positive conid value '{conid_str}'.")
            except (ValueError, TypeError):
                logger.warning(f"[DB Sync FX Conids] Skipping currency {currency}: Could not convert conid '{conid_str}' to a valid integer.")
        logger.info(f"[DB Sync FX Conids] Found {len(results)} valid currency/conid pairs.")
    except sqlite3.Error as e:
        # Handle case where 'conid' column might not exist yet gracefully
        if "no such column: conid" in str(e):
             logger.warning(f"[DB Sync FX Conids] 'conid' column not found in exchange_rates table. Skipping FX conid fetch.")
        else:
             logger.error(f"[DB Sync FX Conids] Database error: {e}", exc_info=True)
    except Exception as e:
         logger.error(f"[DB Sync FX Conids] Unexpected error: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    return results

# --- END ADDED SYNC FUNCTIONS ---

# --- NEW SYNC FUNCTION FOR MULTI-FIELD SCREENER UPDATE ---
def update_screener_multi_fields_sync(db_path: str, ticker: str, updates: Dict[str, Any]) -> bool:
    """Synchronously updates multiple fields for a ticker in the screener table.

    Args:
        db_path: Path to the SQLite database file.
        ticker: The ticker symbol to update.
        updates: Dictionary of field names and their new values.

    Returns:
        True if the update was successful (row affected), False otherwise.
    """
    ticker_upper = ticker.strip().upper()
    if not updates:
        logger.warning(f"[DB Sync Multi Update] No updates provided for ticker {ticker_upper}. Skipping.")
        return False

    # Ensure updated_at is always included and is a datetime object
    updates['updated_at'] = datetime.now()

    logger.debug(f"[DB Sync Multi Update] Updating screener for {ticker_upper} with: {updates}")
    conn = None
    success = False
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        cursor = conn.cursor()

        # Prepare the SET part of the SQL query dynamically
        # Corrected f-string formatting for literal double quotes around the key
        set_clause = ", ".join([f'\"{k}\" = ?' for k in updates.keys()]) 
        values = list(updates.values())
        values.append(ticker_upper) # Add ticker for the WHERE clause

        # Convert datetime objects to ISO format strings for SQLite
        for i, val in enumerate(values):
            if isinstance(val, datetime):
                values[i] = val.isoformat()

        sql = f"UPDATE screener SET {set_clause} WHERE ticker = ?"
        
        logger.debug(f"[DB Sync Multi Update] Executing SQL: {sql} with values: {values}")
        cursor.execute(sql, values)
        conn.commit()

        if cursor.rowcount > 0:
            logger.debug(f"[DB Sync Multi Update] Successfully updated {len(updates)-1} fields for {ticker_upper}.")
            success = True
        else:
            logger.warning(f"[DB Sync Multi Update] Ticker {ticker_upper} not found for multi-field update or values unchanged.")
            success = False # Ensure success is False if no rows updated

    except sqlite3.Error as e:
        logger.error(f"[DB Sync Multi Update] Database error updating {ticker_upper}: {e}", exc_info=True)
        if conn:
            conn.rollback()
        success = False # Ensure success is False on error
    except Exception as e:
        logger.error(f"[DB Sync Multi Update] Unexpected error updating {ticker_upper}: {e}", exc_info=True)
        if conn:
            conn.rollback()
        success = False # Ensure success is False on error
    finally:
        if conn:
            conn.close()
            
    return success
# --- END NEW SYNC FUNCTION ---
