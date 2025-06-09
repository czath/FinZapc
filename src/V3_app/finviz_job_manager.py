import asyncio
import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable

# Assuming SQLiteRepository and TickerListPayload are accessible for type hinting
from .V3_database import SQLiteRepository 
from .V3_models import TickerListPayload, JobDetailsResponse # Using the models file
from .V3_finviz_fetch import fetch_and_store_analytics_finviz # Import the refactored Finviz function
from .services.notification_service import dispatch_notification

logger = logging.getLogger(__name__)

FINVIZ_MASS_FETCH_JOB_ID = "finviz_mass_fetch_main_v1" # Unique ID for this specific job

# Module-level in-memory store for job statuses - only for this specific job
_job_status: Dict[str, Any] = {}
_job_status_lock = asyncio.Lock()
finviz_sse_update_queue = asyncio.Queue() # SSE queue for Finviz jobs
logger.info(f"[FINVIZ_JOB_MANAGER_INIT] finviz_sse_update_queue created with id: {id(finviz_sse_update_queue)}")

async def _update_job_status_internal(
    updates: Dict[str, Any],
    repository: Optional[SQLiteRepository] = None
):
    timestamp_entry = datetime.now().isoformat() 
    # Selectively log only key status fields to keep logs cleaner
    update_summary = {k: v for k, v in updates.items() if k in ['status', 'progress_message', 'current_count', 'total_count', 'progress_percent', 'message']}
    for key_log, val_log in update_summary.items():
        if isinstance(val_log, str) and len(val_log) > 150: # Truncate long messages for logging
            update_summary[key_log] = val_log[:147] + "..."
    logger.debug(f"[FINVIZ_UPDATE_STATUS_ENTRY {timestamp_entry}] Update summary: {update_summary}")

    async with _job_status_lock:
        if not _job_status: 
            # Initialize if empty
            _job_status.update({
                "job_id": FINVIZ_MASS_FETCH_JOB_ID,
                "job_type": "finviz_mass_fetch", # Set job_type
                "status": "idle", "message": "Awaiting trigger.", "current_count": 0, "total_count": 0,
                "successful_count": 0, "failed_count": 0, "progress_percent": 0,
                "last_triggered_time": None, "last_started_time": None, "last_completion_time": None,
                "last_run_summary": None, "timestamp": datetime.now().isoformat()
            })
            logger.debug(f"[FINVIZ_UPDATE_STATUS_INIT {timestamp_entry}] Initialized _job_status with job_type 'finviz_mass_fetch'.")

        _job_status["job_type"] = "finviz_mass_fetch" # Ensure job_type is always set

        for key, value in updates.items():
            if isinstance(value, datetime):
                _job_status[key] = value.isoformat()
            else:
                _job_status[key] = value
        _job_status["timestamp"] = timestamp_entry

        # Log current state after update for debugging
        log_msg_short = _job_status.get('progress_message', _job_status.get('message'))
        if isinstance(log_msg_short, str) and len(log_msg_short) > 100:
            log_msg_short = log_msg_short[:97] + "..."
        
        logger.debug(
            f"[FINVIZ_UPDATE_STATUS_AFTER_LOCK {timestamp_entry}] TS: {_job_status.get('timestamp')}, Status: {_job_status.get('status')}, "
            f"Type: {_job_status.get('job_type')}, Msg: {log_msg_short}, Count: {_job_status.get('current_count')}/{_job_status.get('total_count')}, "
            f"Percent: {_job_status.get('progress_percent')}%"
        )
        
        # Ensure timestamp_entry is defined or passed if used in the log message
        timestamp_entry = _job_status.get('timestamp', datetime.now().isoformat()) # Get from status or fallback

        logger.debug(f"[FINVIZ_SSE_DISPATCH_ENTRY {timestamp_entry}] Attempting to dispatch status to SSE queue. Queue object: {type(finviz_sse_update_queue)}. Current status: {_job_status.get('status')}")

        if finviz_sse_update_queue is not None:
            try:
                status_copy_for_queue = dict(_job_status) # Explicit copy
                logger.debug(f"[FINVIZ_SSE_QUEUE_PRE_PUT {timestamp_entry}] Attempting to put status on queue (id: {id(finviz_sse_update_queue)}). Size before: {finviz_sse_update_queue.qsize()}. Data: {status_copy_for_queue.get('status')}")
                await finviz_sse_update_queue.put(status_copy_for_queue)
                await asyncio.sleep(0) # ADDED: Yield control to event loop
                logger.debug(f"[FINVIZ_SSE_QUEUE_POST_PUT {timestamp_entry}] Job status update ADDED to finviz_sse_update_queue. Size after: {finviz_sse_update_queue.qsize()}")
            except Exception as q_err:
                logger.error(f"[FINVIZ_SSE_QUEUE_ERROR {timestamp_entry}] Failed to put status on finviz_sse_update_queue: {q_err}", exc_info=True)

        # Persist final states to DB
        final_status_for_persistence = _job_status.get("status")
        if final_status_for_persistence in ["completed", "failed", "partial_failure"] and repository:
            try:
                persist_data = {
                    "job_id": FINVIZ_MASS_FETCH_JOB_ID,
                    "job_type": _job_status.get("job_type"), # Persist job_type
                    "status": final_status_for_persistence,
                    "last_completion_time": _job_status.get("last_completion_time"),
                    "last_run_summary": _job_status.get("last_run_summary"),
                    "total_count": _job_status.get("total_count"),
                    "successful_count": _job_status.get("successful_count"),
                    "failed_count": _job_status.get("failed_count"),
                    "updated_at": _job_status.get("timestamp") 
                }
                await repository.save_persistent_job_state(persist_data)
                logger.info(f"FINVIZ_PERSISTENCE: Job {FINVIZ_MASS_FETCH_JOB_ID} final state '{final_status_for_persistence}' saved with job_type.")
            except Exception as db_e:
                logger.error(f"FINVIZ_PERSISTENCE: Failed to save final state for job {FINVIZ_MASS_FETCH_JOB_ID}: {db_e}", exc_info=True)

async def _run_finviz_mass_fetch_background_internal(tickers: List[str], repository: SQLiteRepository):
    await asyncio.sleep(0.01) # Initial yield

    total_tickers = len(tickers)
    job_start_datetime = datetime.now() # Explicit datetime object for start
    job_start_iso = job_start_datetime.isoformat()
    logger.info(f"[Finviz BG Task - {FINVIZ_MASS_FETCH_JOB_ID} @ {job_start_iso}] STARTING. Tickers: {total_tickers}. Repo DB URL: {repository.database_url}")

    await _update_job_status_internal({
        "status": "running",
        "progress_message": f"Initializing Finviz mass fetch for {total_tickers} tickers...",
        "last_started_time": job_start_datetime, # Use the datetime object
        "current_count": 0, 
        "successful_count": 0,
        "failed_count": 0,
        "progress_percent": 0, 
        "total_count": total_tickers,
        "job_type": "finviz_mass_fetch" 
    }, repository)
    logger.info(f"[Finviz BG Task - {FINVIZ_MASS_FETCH_JOB_ID} @ {job_start_datetime.isoformat()}] INITIALIZING: Status updated to 'running/initializing'.")
    await asyncio.sleep(0.01)

    async def _job_progress_callback(current_idx: int, total_items: int, last_ticker_processed: str, ticker_had_errors: bool):
        callback_entry_ts = datetime.now().isoformat()
        # logger.info(f"[FINVIZ_PROGRESS_CALLBACK @ {callback_entry_ts}] CALLED. Idx: {current_idx}, Total: {total_items}, Ticker: {last_ticker_processed}, Errors: {ticker_had_errors}")
        
        current_progress_percent = int(((current_idx) / total_items) * 100) if total_items > 0 else 0
        
        update_payload = {
            "progress_message": f"Processing {last_ticker_processed} ({current_idx}/{total_items})...",
            "current_count": current_idx,
            "progress_percent": current_progress_percent,
            # Note: successful_count and failed_count are updated at the end based on fetch_results
        }
        # logger.debug(f"[FINVIZ_PROGRESS_CALLBACK @ {callback_entry_ts}] Update payload for _update_job_status_internal: {update_payload}")
        await _update_job_status_internal(update_payload, repository)
        await asyncio.sleep(0) # Yield control to allow other tasks (like SSE sender)

    try:
        logger.info(f"[Finviz BG Task - {FINVIZ_MASS_FETCH_JOB_ID} @ {job_start_iso}] PRE-CALL to fetch_and_store_analytics_finviz for {total_tickers} tickers.")
        
        errored_tickers_list_from_callback = [] # Initialize list to store errored tickers

        fetch_results = await fetch_and_store_analytics_finviz(
            repository=repository, 
            tickers=tickers, 
            progress_callback=_job_progress_callback # Pass the callback
        )
        
        logger.info(f"[Finviz BG Task - {FINVIZ_MASS_FETCH_JOB_ID} @ {job_start_iso}] POST-CALL fetch_and_store_analytics_finviz completed. Results summary: Success: {fetch_results.get('success_count')}, Failed: {fetch_results.get('failed_count')}")

        successful_items_count = fetch_results.get('success_count', 0)
        failed_items_list_details = fetch_results.get('errors', []) 
        failed_items_count = fetch_results.get('failed_count', len(failed_items_list_details))
        
        # Populate errored_tickers_list from the results
        errored_tickers_list_for_summary = [item.get('ticker', 'UNKNOWN_TICKER') for item in failed_items_list_details]

    except Exception as e_mass_load:
        logger.error(f"[Finviz BG Task - {FINVIZ_MASS_FETCH_JOB_ID} @ {job_start_iso}] Critical error during fetch_and_store_analytics_finviz: {e_mass_load}", exc_info=True)
        fetch_results = None 
        successful_items_count = 0
        failed_items_list_details = [{"ticker": t, "error": "Critical wrapper failure"} for t in tickers]
        failed_items_count = total_tickers
        errored_tickers_list_for_summary = tickers[:] # All tickers are considered errored

    job_end_datetime = datetime.now() # Explicit datetime object for end

    # New detailed summary format
    start_time_str = job_start_datetime.strftime('%d/%m/%Y %H:%M:%S')
    end_time_str = job_end_datetime.strftime('%d/%m/%Y %H:%M:%S')
    
    errored_tickers_str = ', '.join(errored_tickers_list_for_summary) if errored_tickers_list_for_summary else 'None'
    
    detailed_run_summary = (
        f"Job started at: {start_time_str}, Job ended at: {end_time_str}. "
        f"Processed: {total_tickers}, Successful: {successful_items_count}, Errors: {failed_items_count}. "
        f"Errored tickers: {errored_tickers_str}."
    )
    
    final_status_str = "completed"
    # summary_msg = f"Job completed at: {completion_time_formatted}. Processed: {total_tickers}, Successful: {successful_items_count}, Errors: {failed_items_count}." # Old summary_msg

    # The UI will still show a more concise message, but the detailed_run_summary goes to the DB.
    # For the 'message' field that might be shown in brief UI updates (like SSE), keep it concise.
    concise_summary_msg = f"Status: {final_status_str}. Tickers: {total_tickers}. Errors: {failed_items_count}."

    error_details_for_status = None # This was for a different field, keeping related logic
    if failed_items_count > 0:
        final_status_str = "partial_failure" if successful_items_count > 0 else "failed"
        # Update concise_summary_msg based on new final_status_str
        concise_summary_msg = f"Status: {final_status_str}. Tickers: {total_tickers}. Errors: {failed_items_count}."
        # The detailed_run_summary already contains the full error list.
        # error_details_for_status can remain as it was if used by other parts of the system,
        # or be deprecated if detailed_run_summary covers its needs.
        # For now, let's assume it might still be used for the _job_status internal dict's "last_error_details"
        error_details_for_status = f"Errored tickers: {errored_tickers_str}"

    elif successful_items_count == total_tickers and total_tickers > 0:
        concise_summary_msg = f"All {total_tickers} tickers processed successfully."
    elif total_tickers == 0:
        concise_summary_msg = "No tickers were provided to process."
        final_status_str = "completed" 
    
    if fetch_results is None: # Critical failure in the fetch function itself
        final_status_str = "failed"
        concise_summary_msg = f"Process critically failed. Assumed {total_tickers} failures. Check logs."
        error_details_for_status = "Mass load function encountered a critical failure."
        # Update detailed_run_summary for this case too
        detailed_run_summary = (
            f"Job started at: {start_time_str}, Job ended at: {end_time_str}. "
            f"CRITICAL FAILURE. Processed: {total_tickers}, Assumed Errors: {total_tickers}. "
            f"Errored tickers: All provided tickers (or check logs if list is too long)."
        )

    final_updates = {
        "status": final_status_str,
        "message": concise_summary_msg, # Use the concise message for general status updates
        "progress_message": "Processing finished.",
        "current_count": total_tickers, 
        "successful_count": successful_items_count,
        "failed_count": failed_items_count,
        "progress_percent": 100,
        "last_completion_time": job_end_datetime, # Use datetime object
        "last_run_summary": detailed_run_summary, # Store the NEW detailed summary here
        "last_error_details": error_details_for_status 
    }
    logger.info(f"[Finviz BG Task - {FINVIZ_MASS_FETCH_JOB_ID} @ {job_end_datetime.isoformat()}] COMPLETED. Final status: {final_status_str}. Full Summary: {detailed_run_summary}")
    await _update_job_status_internal(final_updates, repository)
    await dispatch_notification(db_repo=repository, task_id='finviz_mass_fetch', message=detailed_run_summary)

async def trigger_finviz_mass_fetch_job(
    request_payload: TickerListPayload, # Original payload
    source_identifier: str,             # Determined source from query param or default
    repository: SQLiteRepository
):
    logger.info(f"[FINVIZ_TRIGGER_JOB] Received trigger. Source Identifier: '{source_identifier}', Payload tickers count: {len(request_payload.tickers)}")

    actual_tickers_to_process: List[str] = []

    if source_identifier == "finviz_screener":
        logger.info(f"[FINVIZ_TRIGGER_JOB] Source is '{source_identifier}'. Fetching tickers from database screener table.")
        # The dummy ticker in payload for validation is ignored here.
        try:
            screened_tickers_data = await repository.get_all_screened_tickers()
            if screened_tickers_data:
                actual_tickers_to_process = [item['ticker'] for item in screened_tickers_data if item.get('ticker')]
            
            if not actual_tickers_to_process:
                logger.warning(f"[FINVIZ_TRIGGER_JOB] No tickers found in the screener table for source '{source_identifier}'.")
            else:
                logger.info(f"[FINVIZ_TRIGGER_JOB] Fetched {len(actual_tickers_to_process)} tickers from screener for source '{source_identifier}'.")
        except Exception as e:
            logger.error(f"[FINVIZ_TRIGGER_JOB] Error fetching tickers from DB for source '{source_identifier}': {e}", exc_info=True)
            actual_tickers_to_process = [] # Proceed with an empty list on error
    elif source_identifier == "upload_finviz_txt":
        # TickerListPayload ensures tickers is non-empty via min_items=1 for this path.
        logger.info(f"[FINVIZ_TRIGGER_JOB] Source is '{source_identifier}'. Using {len(request_payload.tickers)} tickers from payload.")
        actual_tickers_to_process = request_payload.tickers
    else:
        # This case should ideally not be hit if source_identifier is validated or defaulted in the API layer.
        # However, if it is, TickerListPayload guarantees request_payload.tickers is non-empty.
        logger.warning(f"[FINVIZ_TRIGGER_JOB] Unknown source identifier: '{source_identifier}'. Using tickers from payload as fallback.")
        actual_tickers_to_process = request_payload.tickers 

    num_tickers_to_process = len(actual_tickers_to_process)
    logger.info(f"[FINVIZ_TRIGGER_JOB] Final number of tickers to process: {num_tickers_to_process}")

    async with _job_status_lock:
        current_job_details = _job_status
        current_status = current_job_details.get("status")
        if current_status in ["running", "queued"]:
            logger.warning(f"Finviz mass fetch job ({FINVIZ_MASS_FETCH_JOB_ID}) is already {current_status}. New request for source '{source_identifier}' (processing {num_tickers_to_process} tickers) ignored.")
            return {
                "job_id": FINVIZ_MASS_FETCH_JOB_ID,
                "status": current_status,
                "message": f"Job is already {current_status}. Please wait.",
                "job_type": "finviz_mass_fetch",
                **current_job_details
            }

    now = datetime.now()
    message_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

    descriptive_message_part = f"{num_tickers_to_process} tickers (source: {source_identifier})"
    if source_identifier == "finviz_screener" and num_tickers_to_process == 0 and not actual_tickers_to_process: 
        descriptive_message_part = f"screened tickers (source: {source_identifier}, none found or error during DB fetch)"
    elif num_tickers_to_process == 0:
         descriptive_message_part = f"0 tickers (source: {source_identifier})"

    initial_message = f"Finviz mass fetch for {descriptive_message_part} queued at {message_timestamp}."

    initial_status_updates = {
        "job_id": FINVIZ_MASS_FETCH_JOB_ID,
        "status": "queued",
        "job_type": "finviz_mass_fetch",
        "message": initial_message,
        "progress_message": f"Job for {descriptive_message_part} received; server initializing...",
        "total_count": num_tickers_to_process, 
        "current_count": 0, "successful_count": 0, "failed_count": 0, "progress_percent": 0,
        "last_triggered_time": now, "last_started_time": None, "last_completion_time": None,
        "last_run_summary": None, "last_error_details": None, "timestamp": now
    }
    logger.info(f"[FINVIZ_TRIGGER_JOB] Updating initial status. Message: {initial_status_updates.get('message')}")
    await _update_job_status_internal(initial_status_updates, repository)

    job_details_to_return = {}
    async with _job_status_lock: 
        job_details_to_return = dict(_job_status)

    if num_tickers_to_process > 0:
        asyncio.create_task(_run_finviz_mass_fetch_background_internal(actual_tickers_to_process, repository))
        logger.info(f"[FINVIZ_TRIGGER_JOB] Created asyncio task for _run_finviz_mass_fetch_background_internal with {num_tickers_to_process} tickers from source '{source_identifier}'.")
    else:
        logger.info(f"[FINVIZ_TRIGGER_JOB] No tickers to process for source '{source_identifier}'. Background task not started. Job will transition to completed.")
        
        completion_message = f"Job completed: 0 tickers processed for source '{source_identifier}'."
        if source_identifier == "finviz_screener" and not actual_tickers_to_process : 
             completion_message = f"Job completed: 0 tickers found in screener for source '{source_identifier}' or error during DB fetch."
        
        completion_updates = {
            "status": "completed",
            "message": completion_message,
            "progress_message": "Processing finished.",
            "current_count": 0, 
            "successful_count": 0, 
            "failed_count": 0, 
            "progress_percent": 100,
            "last_completion_time": datetime.now(),
            "last_run_summary": f"Completed with 0 tickers for source '{source_identifier}'.",
            "total_count": 0 
        }
        await _update_job_status_internal(completion_updates, repository)
        async with _job_status_lock: 
             job_details_to_return = dict(_job_status)

    logger.info(f"[FINVIZ_TRIGGER_JOB] Returning initial job details from trigger: {job_details_to_return}")
    return job_details_to_return

async def get_job_details_internal(job_id: str, repository: SQLiteRepository) -> Dict[str, Any]:
    request_arrival_time = datetime.now().isoformat()
    logger.info(f"[FINVIZ_GET_DETAILS_ENTRY @ {request_arrival_time}] Called for job_id: {job_id}. Repo: {repository is not None}")
    if job_id != FINVIZ_MASS_FETCH_JOB_ID:
        return {"job_id": job_id, "status": "error", "message": "Job ID not supported by Finviz manager."}

    job_info_to_return = None
    should_load_from_db = False
    
    async with _job_status_lock:
        if _job_status and _job_status.get("job_id") == job_id:
            job_info_to_return = dict(_job_status)
            if "job_type" not in job_info_to_return or job_info_to_return["job_type"] is None:
                job_info_to_return["job_type"] = "finviz_mass_fetch" # Ensure job_type
        else:
            should_load_from_db = True
            
    if should_load_from_db:
        logger.info(f"[FINVIZ_GET_DETAILS_DB_LOAD @ {datetime.now().isoformat()}] For {job_id}. Attempting DB load.")
        if repository:
            persisted_state = await repository.get_persistent_job_state(job_id)
            if persisted_state:
                logger.info(f"[FINVIZ_GET_DETAILS_DB_SUCCESS] Loaded job {job_id} from DB. Status: {persisted_state.get('status')}")
                # Ensure datetime objects from DB are ISO formatted for consistency
                for key_dt in ["last_completion_time", "updated_at"]: # Add other dt fields if they exist
                    if key_dt in persisted_state and isinstance(persisted_state[key_dt], datetime):
                        persisted_state[key_dt] = persisted_state[key_dt].isoformat()
                
                job_info_to_return = persisted_state
                if "job_type" not in job_info_to_return or job_info_to_return["job_type"] is None:
                    job_info_to_return["job_type"] = "finviz_mass_fetch" # Ensure job_type

                # Restore to in-memory if it wasn't there or was for a different job_id
                async with _job_status_lock:
                    if not _job_status or _job_status.get("job_id") != job_id:
                        _job_status.clear()
                        _job_status.update(job_info_to_return)
                        logger.info(f"[FINVIZ_GET_DETAILS_DB_MERGE] Restored job {job_id} to in-memory from DB.")
            else:
                logger.warning(f"[FINVIZ_GET_DETAILS_DB_NOT_FOUND] No persisted info for job {job_id}.")
        else:
            logger.warning(f"[FINVIZ_GET_DETAILS_DB_NO_REPO] Repo not provided for job {job_id}.")

    if not job_info_to_return:
        logger.warning(f"[FINVIZ_GET_DETAILS_UNKNOWN] No current or loadable info for job {job_id}. Returning default.")
        # Use JobDetailsResponse model for default structure
        default_response = JobDetailsResponse(
            job_id=job_id,
            job_type="finviz_mass_fetch", # Set job_type
            status="unknown",
            message="No active or prior execution data found for this Finviz job."
        ).model_dump() # Use .model_dump() if it's a Pydantic model
        return default_response
    
    # Ensure all relevant datetime fields are ISO strings before returning
    for key_dt_final in ["timestamp", "last_triggered_time", "last_started_time", "last_completion_time", "updated_at"]:
        if key_dt_final in job_info_to_return and isinstance(job_info_to_return[key_dt_final], datetime):
            job_info_to_return[key_dt_final] = job_info_to_return[key_dt_final].isoformat()
            
    logger.info(f"[FINVIZ_GET_DETAILS_RETURN_FINAL @ {datetime.now().isoformat()}] For job_id: {job_id}. Returning status '{job_info_to_return.get('status')}'")
    return job_info_to_return

async def load_initial_job_state_from_db(repository: SQLiteRepository):
    logger.info(f"Attempting to load initial state for Finviz job ({FINVIZ_MASS_FETCH_JOB_ID}) from DB.")
    persisted_state = await repository.get_persistent_job_state(FINVIZ_MASS_FETCH_JOB_ID)
    async with _job_status_lock:
        if persisted_state:
            _job_status.update(persisted_state)
            if "job_type" not in _job_status or _job_status["job_type"] is None: # Ensure job_type if loaded from old schema
                _job_status["job_type"] = "finviz_mass_fetch"
            
            current_status = _job_status.get("status")
            if current_status in ["running", "queued"]:
                _job_status["status"] = "interrupted" # Mark as interrupted if found in active state
                _job_status["message"] = "Job was interrupted by server restart."
                _job_status["progress_message"] = _job_status["message"]
            _job_status["timestamp"] = datetime.now().isoformat() # Update timestamp
            logger.info(f"Restored persisted state for Finviz job ({FINVIZ_MASS_FETCH_JOB_ID}): Status {_job_status.get('status')}, JobType: {_job_status.get('job_type')}")
        else:
            # Initialize default state if no persisted state found
            _job_status.update({
                "job_id": FINVIZ_MASS_FETCH_JOB_ID,
                "job_type": "finviz_mass_fetch",
                "status": "idle",
                "message": "System initialized. Awaiting Finviz job trigger.",
                "last_completion_time": None, "last_run_summary": "No previous run data found.",
                "current_count": 0, "total_count": 0, "successful_count": 0, "failed_count": 0,
                "progress_percent": 0, "timestamp": datetime.now().isoformat()
            })
            logger.info(f"Initialized default state for Finviz job ({FINVIZ_MASS_FETCH_JOB_ID}).")
        
        # Ensure the SSE queue gets an initial state if a job was loaded/initialized
        if _job_status:
             await finviz_sse_update_queue.put(dict(_job_status))