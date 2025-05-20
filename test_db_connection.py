import asyncio
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from src.V3_app.V3_database import Base, SQLiteRepository

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_database_connection():
    try:
        # Use the same database URL as the main application
        database_url = "sqlite+aiosqlite:///src/V3_app/V3_database.db"
        
        # Create repository instance
        repository = SQLiteRepository(database_url=database_url)
        logger.info("Repository initialized successfully")
        
        # Test table creation
        await repository.create_tables()
        logger.info("Tables created/verified successfully")
        
        # Test basic operations
        # 1. Get all accounts (should be empty or existing accounts)
        accounts = await repository.get_all_accounts()
        logger.info(f"Found {len(accounts)} accounts in database")
        
        # 2. Test job config operations
        job_id = "test_job"
        await repository.save_job_config({
            "job_id": job_id,
            "job_type": "test",
            "schedule": '{"trigger": "interval", "seconds": 3600}',
            "is_active": 1
        })
        logger.info("Test job config saved successfully")
        
        # 3. Verify job config was saved
        job_config = await repository.get_job_config(job_id)
        if job_config:
            logger.info(f"Successfully retrieved job config: {job_config}")
        else:
            logger.error("Failed to retrieve job config")
        
        # 4. Clean up test data
        await repository.update_job_config(job_id, {"is_active": 0})
        logger.info("Test job config deactivated")
        
        logger.info("All database tests completed successfully!")
        
    except Exception as e:
        logger.error(f"Database test failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(test_database_connection()) 