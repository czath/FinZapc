import asyncio
import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable

# Removed: from fastapi import Request # For SSE request object
# Removed: from fastapi.responses import StreamingResponse # Ensure StreamingResponse is imported globally

# Assuming SQLiteRepository and TickerListPayload are accessible for type hinting
# This will require correct pathing when integrated into the full app structure
from .V3_database import SQLiteRepository 
from .V3_models import TickerListPayload, JobDetailsResponse # Using the new models file
from .V3_yahoo_fetch import mass_load_yahoo_data_from_file # <-- IMPORT THE REAL FUNCTION
from .yahoo_repository import YahooDataRepository # <-- IMPORT YahooDataRepository
from .services.notification_service import dispatch_notification

logger = logging.getLogger(__name__)

YAHOO_MASS_FETCH_JOB_ID = "yahoo_mass_fetch_main_v2" # Unique ID for this specific job

# Module-level in-memory store for job statuses - only for this specific job
_job_status: Dict[str, Any] = {}
_job_status_lock = asyncio.Lock()
# Removed: sse_update_queue = asyncio.Queue()

# Placeholder for the actual single ticker processing function
# This would ideally be imported from V3_yahoo_fetch.py after it's refactored
async def _placeholder_process_single_yahoo_ticker(ticker: str, repo_instance: Any) -> Dict[str, Any]:
    await asyncio.sleep(0.1) # Simulate work
    logger.debug(f"Simulating processing for ticker: {ticker}")
    if ticker == "FAIL-SIM": # Simulate a failure for testing
        return {"success": False, "ticker": ticker, "error": "Simulated processing failure"}
    return {"success": True, "ticker": ticker, "message": "Successfully processed."}

async def _update_job_status_internal(
    updates: Dict[str, Any],
    repository: Optional[SQLiteRepository] = None
):
    timestamp_entry = datetime.now().isoformat() 
    update_summary = {k: v for k, v in updates.items() if k in ['status', 'progress_message', 'current_count', 'total_count', 'progress_percent', 'message']}
    for key_log, val_log in update_summary.items():
        if isinstance(val_log, str) and len(val_log) > 150:
            update_summary[key_log] = val_log[:147] + "..."
    logger.debug(f"[UPDATE_STATUS_ENTRY {timestamp_entry}] Called. Update summary: {update_summary}")

    async with _job_status_lock:
        if not _job_status: 
            _job_status.update({
                "job_id": YAHOO_MASS_FETCH_JOB_ID,
                "job_type": "yahoo_mass_fetch",
                "status": "idle", "message": "Awaiting trigger.", "current_count": 0, "total_count": 0,
                "successful_count": 0, "failed_count": 0, "progress_percent": 0,
                "last_triggered_time": None, "last_started_time": None, "last_completion_time": None,
                "last_run_summary": None, "timestamp": datetime.now().isoformat()
            })
            logger.debug(f"[UPDATE_STATUS_INIT {timestamp_entry}] Initialized _job_status with job_type.")

        _job_status["job_type"] = "yahoo_mass_fetch"

        for key, value in updates.items():
            if isinstance(value, datetime):
                _job_status[key] = value.isoformat()
            else:
                _job_status[key] = value
        _job_status["timestamp"] = timestamp_entry

        log_msg_short = _job_status.get('progress_message', _job_status.get('message'))
        if isinstance(log_msg_short, str) and len(log_msg_short) > 100:
            log_msg_short = log_msg_short[:97] + "..."

        logger.debug(
            f"[UPDATE_STATUS_AFTER_LOCK {timestamp_entry}] TS: {_job_status.get('timestamp')}, Status: {_job_status.get('status')}, "
            f"Type: {_job_status.get('job_type')}, Msg: {log_msg_short}, Count: {_job_status.get('current_count')}/{_job_status.get('total_count')}, "
            f"Percent: {_job_status.get('progress_percent')}%"
        )

        final_status_for_persistence = _job_status.get("status")
        if final_status_for_persistence in ["completed", "failed", "partial_failure"] and repository:
            try:
                persist_data = {
                    "job_id": YAHOO_MASS_FETCH_JOB_ID,
                    "job_type": _job_status.get("job_type"),
                    "status": final_status_for_persistence,
                    "last_completion_time": _job_status.get("last_completion_time"),
                    "last_run_summary": _job_status.get("last_run_summary"),
                    "total_count": _job_status.get("total_count"),
                    "successful_count": _job_status.get("successful_count"),
                    "failed_count": _job_status.get("failed_count"),
                    "updated_at": _job_status.get("timestamp") 
                }
                await repository.save_persistent_job_state(persist_data)
                logger.info(f"PERSISTENCE: Job {YAHOO_MASS_FETCH_JOB_ID} final state saved with job_type.")
            except Exception as db_e:
                logger.error(f"PERSISTENCE: Failed to save final state for job {YAHOO_MASS_FETCH_JOB_ID}: {db_e}", exc_info=True)
    
    # SSE queue logic removed from here

async def _run_yahoo_mass_fetch_background_internal(tickers: List[str], repository: SQLiteRepository, send_notification: bool = False):
    await asyncio.sleep(0.01) # Initial yield immediately upon entry

    total_tickers = len(tickers)

    yahoo_specific_repo = YahooDataRepository(repository.database_url) 
    job_start_datetime = datetime.now() # Explicit datetime object for start
    job_start_iso = job_start_datetime.isoformat()
    logger.info(f"[Yahoo BG Task - {YAHOO_MASS_FETCH_JOB_ID} @ {job_start_iso}] STARTING FORMAL INIT. Tickers: {total_tickers}. Repo DB URL: {repository.database_url}")

    await _update_job_status_internal({
        "status": "running",
        "progress_message": f"Initializing Yahoo mass fetch for {total_tickers} tickers...",
        "last_started_time": job_start_datetime, # Use datetime object
        "current_count": 0, 
        "successful_count": 0,
        "failed_count": 0,
        "progress_percent": 0, 
        "total_count": total_tickers,
        "job_type": "yahoo_mass_fetch" 
    }, repository)
    logger.info(f"[Yahoo BG Task - {YAHOO_MASS_FETCH_JOB_ID} @ {job_start_datetime.isoformat()}] INITIALIZING: Status updated to 'running/initializing'.")
    await asyncio.sleep(0.01) # REDUCE this sleep back to 0.01

    async def _job_progress_callback(current_idx: int, total_items: int, last_ticker_processed: str, ticker_had_errors: bool):
        callback_entry_ts = datetime.now().isoformat()
        logger.info(f"[PROGRESS_CALLBACK @ {callback_entry_ts}] CALLED. Idx: {current_idx}, Total: {total_items}, Ticker: {last_ticker_processed}, Errors: {ticker_had_errors}")
        
        current_progress_percent = int(((current_idx) / total_items) * 100) if total_items > 0 else 0
        
        update_payload = {
            "progress_message": f"Processing {last_ticker_processed} ({current_idx}/{total_items})...",
            "current_count": current_idx,
            "progress_percent": current_progress_percent
        }
        logger.debug(f"[PROGRESS_CALLBACK @ {callback_entry_ts}] Update payload for _update_job_status_internal: {update_payload}")
        await _update_job_status_internal(update_payload, repository)
        await asyncio.sleep(0) 

    try:
        logger.info(f"[Yahoo BG Task - {YAHOO_MASS_FETCH_JOB_ID} @ {job_start_iso}] PRE-CALL to mass_load_yahoo_data_from_file for {total_tickers} tickers. Using repo: {yahoo_specific_repo}")
        
        async def wrapper_progress_callback(idx, total, ticker_symbol, had_error):
            # This wrapper can be enhanced if individual error tickers need to be collected live
            # For now, mass_load_yahoo_data_from_file returns a summary of errors.
            await _job_progress_callback(idx, total, ticker_symbol, had_error)

        fetch_results = await mass_load_yahoo_data_from_file(
            ticker_source=tickers, 
            db_repo=yahoo_specific_repo, 
            progress_callback=wrapper_progress_callback
        )
        
        logger.info(f"[Yahoo BG Task - {YAHOO_MASS_FETCH_JOB_ID} @ {job_start_iso}] POST-CALL mass_load_yahoo_data_from_file completed. Results: {fetch_results}")

        successful_items_count = fetch_results.get('success_count', 0)
        # failed_items_list_tickers directly contains the list of errored ticker symbols
        errored_tickers_list_for_summary = fetch_results.get('errors', []) 
        failed_items_count = len(errored_tickers_list_for_summary) 

    except Exception as e_mass_load:
        logger.error(f"[Yahoo BG Task - {YAHOO_MASS_FETCH_JOB_ID} @ {job_start_iso}] Critical error during mass_load_yahoo_data_from_file: {e_mass_load}", exc_info=True)
        fetch_results = None 
        successful_items_count = 0
        errored_tickers_list_for_summary = tickers[:] # All tickers are considered errored in a critical failure
        failed_items_count = total_tickers

    job_end_datetime = datetime.now() # Explicit datetime object for end
    # completion_time_formatted = completion_time.strftime("%d/%m/%Y %H:%M") # Old formatting

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
    # The UI will still show a more concise message, but the detailed_run_summary goes to the DB.
    concise_summary_msg = f"Status: {final_status_str}. Tickers: {total_tickers}. Errors: {failed_items_count}."

    if failed_items_count > 0:
        final_status_str = "partial_failure" if successful_items_count > 0 else "failed"
        concise_summary_msg = f"Status: {final_status_str}. Tickers: {total_tickers}. Errors: {failed_items_count}."
        # detailed_run_summary already contains the full error list.
    elif successful_items_count == total_tickers and total_tickers > 0:
        concise_summary_msg = f"All {total_tickers} tickers processed successfully."
    elif total_tickers == 0:
        concise_summary_msg = "No tickers were provided to process."
        final_status_str = "completed"

    if fetch_results is None: # This means the whole mass_load call failed critically
        final_status_str = "failed"
        concise_summary_msg = f"Process critically failed. Assumed {total_tickers} failures. Check logs."
        # Update detailed_run_summary for this case too
        detailed_run_summary = (
            f"Job started at: {start_time_str}, Job ended at: {end_time_str}. "
            f"CRITICAL FAILURE. Processed: {total_tickers}, Assumed Errors: {total_tickers}. "
            f"Errored tickers: All provided tickers (or check logs if list is too long)."
        )

    # The last_error_details can be made more consistent if needed, or removed if detailed_run_summary suffices
    last_error_details_content = f"Errored tickers: {errored_tickers_str}" if errored_tickers_list_for_summary else None
    if fetch_results is None:
        last_error_details_content = "Mass load function encountered a critical failure."

    final_updates = {
        "status": final_status_str,
        "message": concise_summary_msg, # Concise message for general status updates
        "progress_message": "Processing finished.",
        "current_count": total_tickers, 
        "successful_count": successful_items_count,
        "failed_count": failed_items_count,
        "progress_percent": 100,
        "last_completion_time": job_end_datetime, # Use datetime object
        "last_run_summary": detailed_run_summary, # Store the NEW detailed summary here
        "last_error_details": last_error_details_content
    }
    logger.info(f"[Yahoo BG Task - {YAHOO_MASS_FETCH_JOB_ID} @ {job_end_datetime.isoformat()}] COMPLETED. Final status: {final_status_str}. Full Summary: {detailed_run_summary}")
    await _update_job_status_internal(final_updates, repository)
    
    await dispatch_notification(db_repo=repository, task_id='yahoo_mass_fetch', message=detailed_run_summary)

async def trigger_yahoo_mass_fetch_job(
    tickers_payload: TickerListPayload,
    repository: SQLiteRepository
):
    tickers = tickers_payload.tickers
    send_notification = tickers_payload.send_telegram_notification

    async with _job_status_lock:
        current_job_details = _job_status 
        current_status = current_job_details.get("status")
        if current_status in ["running", "queued"]:
            logger.warning(f"Yahoo mass fetch job ({YAHOO_MASS_FETCH_JOB_ID}) is already {current_status}. New request ignored.")
            from fastapi import HTTPException, status 
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Yahoo mass fetch is already {current_status}. Please wait for it to complete."
            )

    now = datetime.now()
    message_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    initial_status_updates = {
        "job_id": YAHOO_MASS_FETCH_JOB_ID, 
        "status": "queued",
        "job_type": "yahoo_mass_fetch", 
        "message": f"Yahoo mass fetch for {len(tickers)} tickers queued at {message_timestamp}.",
        "progress_message": f"Job for {len(tickers)} tickers received; server initializing...",
        "total_count": len(tickers),
        "current_count": 0,
        "successful_count": 0,
        "failed_count": 0,
        "progress_percent": 0,
        "last_triggered_time": now,
        "last_started_time": None,
        "last_completion_time": None, 
        "last_run_summary": None,     
        "last_error_details": None,
        "timestamp": now 
    }
    logger.info(f"[TRIGGER_JOB] Triggering Yahoo mass fetch. Initial status to set (before _update_job_status_internal): {initial_status_updates}")
    await _update_job_status_internal(initial_status_updates, repository)

    job_details_to_return = {}
    async with _job_status_lock:
        logger.info(f"[TRIGGER_JOB] _job_status content after _update_job_status_internal call, before returning: {_job_status}")
        if _job_status: 
            job_details_to_return = dict(_job_status) 

    asyncio.create_task(_run_yahoo_mass_fetch_background_internal(tickers, repository, send_notification))
    logger.info(f"[TRIGGER_JOB] Created asyncio task for _run_yahoo_mass_fetch_background_internal.")
    
    logger.info(f"[TRIGGER_JOB] Returning initial job details from trigger: {job_details_to_return}")
    return job_details_to_return 

async def get_job_details_internal(job_id: str, repository: SQLiteRepository) -> Dict[str, Any]:
    request_arrival_time = datetime.now().isoformat()
    logger.info(f"[GET_DETAILS_ENTRY @ {request_arrival_time}] Called for job_id: {job_id}. Repository provided: {repository is not None}")
    if job_id != YAHOO_MASS_FETCH_JOB_ID:
        return_val = {"job_id": job_id, "status": "error", "message": "Job ID not supported by this manager."}
        logger.info(f"[GET_DETAILS_RETURN_UNSUPPORTED_JOB @ {request_arrival_time}] Returning: {return_val}")
        return return_val

    job_info_to_return = None
    should_load_from_db = False
    in_memory_status_at_lock_time = {} # For logging

    async with _job_status_lock:
        lock_acquired_time = datetime.now().isoformat()
        # Deep copy for logging, to avoid issues if _job_status changes immediately after lock release by another task (though unlikely here)
        in_memory_status_at_lock_time = json.loads(json.dumps(dict(_job_status))) if _job_status else {}
        
        logger.info(f"[GET_DETAILS_LOCK_ACQUIRED @ {lock_acquired_time}] For job_id: {job_id}. Current _job_status snapshot: {in_memory_status_at_lock_time}")
        
        if _job_status and _job_status.get("job_id") == job_id:
            logger.info(f"[GET_DETAILS_DECISION @ {datetime.now().isoformat()}] IN-MEMORY HIT for job_id: {job_id}. Status found: '{_job_status.get('status')}'.")
            job_info_to_return = dict(_job_status)
            if "job_type" not in job_info_to_return or job_info_to_return["job_type"] is None:
                job_info_to_return["job_type"] = "yahoo_mass_fetch"
        else:
            logger.warning(f"[GET_DETAILS_DECISION @ {datetime.now().isoformat()}] IN-MEMORY MISS for job_id: {job_id}. _job_status job_id: '{_job_status.get('job_id') if _job_status else 'N/A'}', _job_status exists: {True if _job_status else False}. Flagging for DB load.")
            should_load_from_db = True
            
    # Log after lock release, before potential DB call
    decision_point_time = datetime.now().isoformat()
    logger.info(f"[GET_DETAILS_POST_LOCK @ {decision_point_time}] For job_id: {job_id}. In-memory hit: {True if job_info_to_return else False}. Should load from DB: {should_load_from_db}.")

    if should_load_from_db:
        db_load_start_time = datetime.now().isoformat()
        logger.info(f"[GET_DETAILS_DB_LOAD_ATTEMPT @ {db_load_start_time}] For job_id: {job_id}. Attempting to load from persistent store.")
        if repository:
            try:
                persisted_state = await repository.get_persistent_job_state(job_id)
                db_load_end_time = datetime.now().isoformat()
                logger.info(f"[GET_DETAILS_DB_CALL_COMPLETE @ {db_load_end_time}] For job_id: {job_id}. DB call duration: {(datetime.fromisoformat(db_load_end_time) - datetime.fromisoformat(db_load_start_time)).total_seconds()}s.")
            except Exception as e_db_get:
                db_load_fail_time = datetime.now().isoformat()
                logger.error(f"[GET_DETAILS_DB_CALL_EXCEPTION @ {db_load_fail_time}] For job_id: {job_id}. Exception during repository.get_persistent_job_state: {e_db_get}", exc_info=True)
                persisted_state = None

            if persisted_state:
                logger.info(f"[GET_DETAILS_DB_LOAD_SUCCESS] Loaded job {job_id} from DB. Status: {persisted_state.get('status')}, Data: {persisted_state}")
                for key_in_loop in ["last_completion_time", "updated_at"]:
                    if key_in_loop in persisted_state and isinstance(persisted_state[key_in_loop], datetime):
                        persisted_state[key_in_loop] = persisted_state[key_in_loop].isoformat()
            
                job_info_to_return = persisted_state
                if "job_type" not in job_info_to_return or job_info_to_return["job_type"] is None:
                    job_info_to_return["job_type"] = "yahoo_mass_fetch"
                
                async with _job_status_lock:
                    logger.debug(f"[GET_DETAILS_DB_MERGE_LOCK_ACQUIRED] _job_status before potential DB merge: {_job_status}")
                    if not _job_status or _job_status.get("job_id") != job_id:
                        _job_status.clear()
                        _job_status.update(job_info_to_return)
                        logger.info(f"[GET_DETAILS_DB_MERGE_APPLIED] Restored job {job_id} to in-memory _job_status from DB. New _job_status: {_job_status}")
            else:
                logger.warning(f"[GET_DETAILS_DB_LOAD_NOT_FOUND] No persisted information available for job ID {job_id} in DB.")
        else:
            logger.warning(f"[GET_DETAILS_DB_LOAD_NO_REPO] Repository not provided for job {job_id}, cannot load from DB.")

    if not job_info_to_return:
        logger.warning(f"[GET_DETAILS_RETURN_UNKNOWN] No current or loadable information for job ID {job_id}. Returning default 'unknown' structure.")
        default_response = JobDetailsResponse(
            job_id=job_id,
            job_type="yahoo_mass_fetch",
            status="unknown",
            message="No active or prior execution data found for this job."
        ).model_dump()
        logger.info(f"[GET_DETAILS_RETURN_UNKNOWN_FINAL] Returning: {default_response}")
        return default_response
    
    final_conversion_log = {}
    for key_dt in ["timestamp", "last_triggered_time", "last_started_time", "last_completion_time"]:
        if key_dt in job_info_to_return and isinstance(job_info_to_return[key_dt], datetime):
            original_dt = job_info_to_return[key_dt]
            job_info_to_return[key_dt] = job_info_to_return[key_dt].isoformat()
            final_conversion_log[key_dt] = f"Converted {original_dt} to {job_info_to_return[key_dt]}"

    if final_conversion_log:
        logger.debug(f"[GET_DETAILS_FINAL_CONVERSIONS] Applied datetime to ISO string conversions: {final_conversion_log}")
    
    logger.info(f"[GET_DETAILS_RETURN_FINAL @ {datetime.now().isoformat()}] For job_id: {job_id}. Returning job_info_to_return: Status '{job_info_to_return.get('status') if job_info_to_return else 'N/A'}'")
    # logger.debug(f"[GET_DETAILS_RETURN_FINAL_DATA @ {datetime.now().isoformat()}] For job_id: {job_id}. Full data: {job_info_to_return}")
    return job_info_to_return

# get_sse_event_generator_internal and related SSE code removed.

async def load_initial_job_state_from_db(repository: SQLiteRepository):
    logger.info(f"Attempting to load initial state for Yahoo job ({YAHOO_MASS_FETCH_JOB_ID}) from DB.")
    persisted_state = await repository.get_persistent_job_state(YAHOO_MASS_FETCH_JOB_ID)
    async with _job_status_lock:
        if persisted_state:
            _job_status.update(persisted_state)
            if "job_type" not in _job_status or _job_status["job_type"] is None: 
                _job_status["job_type"] = "yahoo_mass_fetch"
            current_status = _job_status.get("status")
            if current_status in ["running", "queued"]:
                _job_status["status"] = "interrupted"
                _job_status["message"] = "Job was interrupted by server restart."
                _job_status["progress_message"] = _job_status["message"]
            _job_status["timestamp"] = datetime.now().isoformat()
            logger.info(f"Restored persisted state for Yahoo job ({YAHOO_MASS_FETCH_JOB_ID}): Status {_job_status.get('status')}, JobType: {_job_status.get('job_type')}")
        else:
            _job_status.update({
                "job_id": YAHOO_MASS_FETCH_JOB_ID,
                "job_type": "yahoo_mass_fetch",
                "status": "idle",
                "message": "System initialized. Awaiting job trigger.",
                "last_completion_time": None,
                "last_run_summary": "No previous run data found.",
                "current_count": 0,
                "total_count": 0,
                "successful_count": 0,
                "failed_count": 0,
                "progress_percent": 0,
                "timestamp": datetime.now().isoformat()
            })
            logger.info(f"Initialized default state for Yahoo job ({YAHOO_MASS_FETCH_JOB_ID}) as no persisted state was found.") 