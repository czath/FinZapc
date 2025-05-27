"""
Router for job status related API endpoints.
"""
import logging
from typing import Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Request

# Assuming SQLiteRepository and get_repository are accessible
# Adjust import path as necessary based on your project structure
from ..V3_database import SQLiteRepository, get_repository # MODIFIED: Import get_repository from V3_database

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/job_status",
    tags=["Job Status"],
)

@router.get("/{job_type_id}", response_model=Optional[Dict[str, Any]])
async def get_job_status_details(
    job_type_id: str,
    repository: SQLiteRepository = Depends(get_repository)
):
    """
    Retrieves the last run details for a specific job type ID.
    Job type IDs are predefined strings like 'finviz_screener_analytics_fetch'.
    """
    logger.info(f"API request for job status details for job_type_id: {job_type_id}")
    try:
        details = await repository.get_last_job_run_details(job_type_id)
        if not details:
            # You might want to return a 404 if no job config exists for this ID yet,
            # or an empty dict/specific structure if it's expected that it might not exist initially.
            # For now, if details are None (job_id not found in DB), return None (FastAPI handles as 200 with null body if response_model=Optional[...]).
            # To be more explicit with 404:
            # raise HTTPException(status_code=404, detail=f"No job configuration or run details found for job type ID: {job_type_id}")
            logger.warning(f"No details found for job_type_id: {job_type_id}. Returning null response.")
            return None 
        
        # Convert datetime objects to ISO format strings for consistent JSON representation
        if details.get('last_run_timestamp') and hasattr(details['last_run_timestamp'], 'isoformat'):
            details['last_run_timestamp'] = details['last_run_timestamp'].isoformat()
        
        logger.debug(f"Returning job status details for {job_type_id}: {details}")
        return details
    except Exception as e:
        logger.error(f"Error fetching job status for {job_type_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error retrieving job status for {job_type_id}.")

# Example of how to define a job type ID if you had a list (for reference, not used by the endpoint directly here)
# SUPPORTED_JOB_TYPE_IDS = [
#     "finviz_screener_analytics_fetch",
#     "finviz_upload_analytics_fetch",
#     "yahoo_screener_analytics_fetch", # Assuming this might be a future job type
#     "yahoo_upload_analytics_fetch",   # Assuming this might be a future job type
# ] 