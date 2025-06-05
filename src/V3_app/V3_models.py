from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class TickerListPayload(BaseModel):
    source: str
    tickers: List[str] = []
    send_telegram_notification: Optional[bool] = False

class YahooFetchTriggerResponse(BaseModel):
    message: str
    job_id: str

class JobDetailsResponse(BaseModel):
    job_id: str
    job_type: Optional[str] = None
    status: str
    message: Optional[str] = None
    progress_message: Optional[str] = None
    current_count: Optional[int] = None
    total_count: Optional[int] = None
    successful_count: Optional[int] = None
    failed_count: Optional[int] = None
    progress_percent: Optional[int] = None
    last_triggered_time: Optional[str] = None # ISO string
    last_started_time: Optional[str] = None   # ISO string
    last_completion_time: Optional[str] = None # ISO string
    last_run_summary: Optional[str] = None
    last_error_details: Optional[str] = None
    timestamp: Optional[str] = None # ISO string

class JobDetailedLogResponse(BaseModel):
    job_id: str
    detailed_log: Optional[str] = None
    error: Optional[str] = None

# You might have other shared models here in the future 