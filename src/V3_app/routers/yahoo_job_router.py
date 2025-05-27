from fastapi import APIRouter, Depends, HTTPException, Request, status
from typing import List

# Adjust relative imports based on your final project structure
from ..V3_database import SQLiteRepository
# Change import to use the new dependencies module
from ..dependencies import get_repository

from .. import yahoo_job_manager # The new manager module
from ..V3_models import TickerListPayload, JobDetailsResponse

router = APIRouter(
    prefix="/api/analytics/yahoo-job",
    tags=["Analytics", "Yahoo Job"]
)

@router.post("/trigger",
             response_model=JobDetailsResponse,
             status_code=status.HTTP_202_ACCEPTED)
async def trigger_yahoo_fetch_route(
    payload: TickerListPayload,
    repository: SQLiteRepository = Depends(get_repository) 
):
    """
    Triggers the Yahoo mass fetch background job.
    """
    try:
        job_details = await yahoo_job_manager.trigger_yahoo_mass_fetch_job(
            tickers_payload=payload,
            repository=repository
        )
        return job_details
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        yahoo_job_manager.logger.error(f"Error in trigger_yahoo_fetch_route: {e}", exc_info=True) # Ensure logging for unexpected errors
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger Yahoo mass fetch job: {str(e)}"
        )

@router.get("/details/{job_id_param}", response_model=JobDetailsResponse)
async def get_job_details_route(
    job_id_param: str,
    repository: SQLiteRepository = Depends(get_repository)
):
    """
    Retrieves the details and current status of a specific Yahoo mass fetch job.
    """
    try:
        # No need to check job_id_param against YAHOO_MASS_FETCH_JOB_ID here,
        # as get_job_details_internal handles unknown job_ids.
        job_details = await yahoo_job_manager.get_job_details_internal(job_id_param, repository)
        
        # get_job_details_internal now returns a dict that should align with JobDetailsResponse
        # or a specific error structure for unknown jobs. Pydantic will validate.
        if job_details.get("status") == "error" and job_details.get("message") == "Job ID not supported by this manager.":
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job ID '{job_id_param}' is not supported.")
        elif job_details.get("status") == "unknown" and "No active or prior execution data found" in job_details.get("message", ""):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job with ID '{job_id_param}' not found or has no data.")

        return job_details # Pydantic will validate against JobDetailsResponse
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        yahoo_job_manager.logger.error(f"Error in get_job_details_route for {job_id_param}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving details for job {job_id_param}: {str(e)}"
        )

# Removed stream_job_status_route and its helper async def event_generator() 