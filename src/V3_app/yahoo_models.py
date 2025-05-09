"""SQLAlchemy models for Yahoo Finance specific data."""

from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text, UniqueConstraint
from sqlalchemy.orm import relationship
from datetime import datetime

# Import Base from the main database module where it's defined
# This assumes Base = declarative_base() is in V3_database.py
try:
    from .V3_database import Base
except ImportError:
    # Fallback if run standalone or if Base definition moves later
    from sqlalchemy.ext.declarative import declarative_base
    Base = declarative_base()
    import logging
    logger = logging.getLogger(__name__)
    logger.warning("Could not import Base from .V3_database, using a locally defined Base for yahoo_models.py. Ensure Base is consistently defined.")


# --- Yahoo Ticker Master Model (Moved from V3_database.py) ---
class YahooTickerMasterModel(Base):
    __tablename__ = 'ticker_master' # Table name defined here
    __table_args__ = {'extend_existing': True}

    # Static Fields
    ticker = Column(String(collation='NOCASE'), primary_key=True, nullable=False)
    company_name = Column(String(collation='NOCASE'), nullable=True)
    country = Column(String(collation='NOCASE'), nullable=True)
    exchange = Column(String(collation='NOCASE'), nullable=True)
    industry = Column(String(collation='NOCASE'), nullable=True)
    sector = Column(String(collation='NOCASE'), nullable=True)
    trade_currency = Column(String(collation='NOCASE'), nullable=True)
    asset_type = Column(String(collation='NOCASE'), nullable=True) 

    # Market Fields (as defined in V3_database.py originally)
    average_volume = Column(Float, nullable=True) 
    beta = Column(Float, nullable=True)
    current_price = Column(Float, nullable=True)
    dividend_date = Column(DateTime, nullable=True) 
    dividend_date_last = Column(DateTime, nullable=True) 
    dividend_ex_date = Column(DateTime, nullable=True) 
    dividend_value_last = Column(Float, nullable=True)
    dividend_yield = Column(Float, nullable=True) 
    dividend_yield_ttm = Column(Float, nullable=True) 
    earnings_timestamp = Column(DateTime, nullable=True) 
    eps_forward = Column(Float, nullable=True)
    fifty_two_week_change = Column(Float, nullable=True)
    fifty_two_week_high = Column(Float, nullable=True)
    fifty_two_week_low = Column(Float, nullable=True)
    five_year_avg_dividend_yield = Column(Float, nullable=True) 
    market_cap = Column(Float, nullable=True) 
    overall_risk = Column(Integer, nullable=True)
    pe_forward = Column(Float, nullable=True)
    price_eps_current_year = Column(Float, nullable=True)
    price_to_book = Column(Float, nullable=True)
    price_to_sales_ttm = Column(Float, nullable=True)
    recommendation_key = Column(String(collation='NOCASE'), nullable=True)
    recommendation_mean = Column(Float, nullable=True)
    regular_market_change = Column(Float, nullable=True)
    regular_market_day_high = Column(Float, nullable=True)
    regular_market_day_low = Column(Float, nullable=True)
    regular_market_open = Column(Float, nullable=True)
    regular_market_previous_close = Column(Float, nullable=True)
    shares_percent_insiders = Column(Float, nullable=True)
    shares_percent_institutions = Column(Float, nullable=True)
    shares_short = Column(Float, nullable=True) 
    shares_short_prior_month = Column(Float, nullable=True) 
    shares_short_prior_month_date = Column(DateTime, nullable=True) 
    short_percent_of_float = Column(Float, nullable=True)
    short_ratio = Column(Float, nullable=True)
    sma_fifty_day = Column(Float, nullable=True)
    sma_two_hundred_day = Column(Float, nullable=True)
    target_mean_price = Column(Float, nullable=True)
    target_median_price = Column(Float, nullable=True)
    trailing_pe = Column(Float, nullable=True)
    trailing_peg_ratio = Column(Float, nullable=True)

    # Financial Summary Fields (as defined in V3_database.py originally)
    book_value = Column(Float, nullable=True)
    current_ratio = Column(Float, nullable=True)
    debt_to_equity = Column(Float, nullable=True)
    dividend_rate = Column(Float, nullable=True) 
    dividend_rate_ttm = Column(Float, nullable=True) 
    earnings_growth = Column(Float, nullable=True)
    earnings_quarterly_growth = Column(Float, nullable=True)
    ebitda_margin = Column(Float, nullable=True)
    enterprise_to_ebitda = Column(Float, nullable=True)
    enterprise_to_revenue = Column(Float, nullable=True)
    enterprise_value = Column(Float, nullable=True) 
    eps_current_year = Column(Float, nullable=True)
    gross_margin = Column(Float, nullable=True)
    last_fiscal_year_end = Column(DateTime, nullable=True) 
    operating_margin = Column(Float, nullable=True)
    payout_ratio = Column(Float, nullable=True)
    profit_margin = Column(Float, nullable=True)
    quick_ratio = Column(Float, nullable=True)
    return_on_assets = Column(Float, nullable=True)
    return_on_equity = Column(Float, nullable=True)
    revenue_growth = Column(Float, nullable=True)
    revenue_per_share = Column(Float, nullable=True)
    shares_float = Column(Float, nullable=True) 
    shares_outstanding = Column(Float, nullable=True) 
    shares_outstanding_implied = Column(Float, nullable=True) 
    total_cash_per_share = Column(Float, nullable=True)
    eps_ttm = Column(Float, nullable=True)

    # Update Timestamps
    update_last_full = Column(DateTime, default=datetime.now) 
    update_marketonly = Column(DateTime, nullable=True) 

    # Relationship to TickerDataItemsModel (defined below)
    data_items = relationship("src.V3_app.yahoo_models.TickerDataItemsModel", back_populates="ticker_master_record", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<YahooTickerMasterModel(ticker='{self.ticker}', company_name='{self.company_name}')>"
# --- End Yahoo Ticker Master Model ---


# --- Ticker Data Items Model (Moved from V3_database.py) ---
class TickerDataItemsModel(Base):
    __tablename__ = 'ticker_data_items'
    __table_args__ = (
        UniqueConstraint('ticker', 'item_type', 'item_time_coverage', 'item_key_date', name='uq_ticker_item_coverage_date'),
        {'extend_existing': True}
    )

    data_item_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign Key to ticker_master table using the primary key column name
    ticker = Column(String(collation='NOCASE'), ForeignKey('ticker_master.ticker'), nullable=False, index=True)
    
    item_type = Column(String(collation='NOCASE'), nullable=False, index=True)
    item_time_coverage = Column(String(collation='NOCASE'), nullable=False) 
    item_key_date = Column(DateTime, nullable=False, index=True)
    fetch_timestamp_utc = Column(DateTime, nullable=False, default=datetime.now)
    item_source = Column(String, nullable=True) 
    item_data_payload = Column(Text, nullable=False) # Use Text for potentially large JSON

    # Relationship back to YahooTickerMasterModel
    ticker_master_record = relationship("src.V3_app.yahoo_models.YahooTickerMasterModel", back_populates="data_items")

    def __repr__(self):
        return f"<TickerDataItemsModel(item_id={self.data_item_id}, ticker='{self.ticker}', type='{self.item_type}', date='{self.item_key_date}')>"
# --- END Ticker Data Items Model --- 