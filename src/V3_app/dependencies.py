"""
Application-specific dependencies for FastAPI.
"""
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Request, HTTPException
import logging

logger = logging.getLogger(__name__)

async def get_db(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency that provides a SQLAlchemy AsyncSession.
    It fetches the async_session_factory from the application state.
    """
    if not hasattr(request.app.state, 'repository') or \
       not hasattr(request.app.state.repository, 'async_session_factory'):
        logger.error("CRITICAL: SQLiteRepository or its async_session_factory not found in application state!")
        raise HTTPException(status_code=500, detail="Internal server error: Database session cannot be created.")

    async_session_factory = request.app.state.repository.async_session_factory
    
    session: AsyncSession | None = None
    try:
        async with async_session_factory() as session:
            yield session
    except Exception as e:
        logger.error(f"Error in get_db session management: {e}", exc_info=True)
        # Depending on the error, you might want to rollback if a transaction was started,
        # but simple yield and context management with 'async with' should handle most cases.
        # If session is not None and has an active transaction that needs rollback:
        # if session and session.in_transaction():
        #     await session.rollback()
        raise  # Re-raise the exception to be caught by FastAPI error handlers
    finally:
        if session and session.is_active:
            # Typically, 'async with async_session_factory() as session:' handles closure.
            # Explicit close might be needed if not using the context manager directly on the session.
            # However, the above 'async with' pattern is preferred and handles this.
            # logger.debug("Ensuring session is closed in get_db finally block.")
            # await session.close() # Usually not needed with 'async with async_session_factory() as session:'
            pass 