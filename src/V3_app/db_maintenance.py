"""
Module for database maintenance tasks, such as cleaning up invalid records.
"""
import logging
from typing import List, Dict, Any, Optional
import json
from datetime import datetime

from .yahoo_repository import YahooDataRepository

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
INVALID_THRESHOLD_PERCENT = 0.70  # 70%

def _is_value_empty(value: Any) -> bool:
    """Checks if a value is considered empty (None, empty string, etc.)."""
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    # Consider empty JSON objects/arrays in payload as empty
    if isinstance(value, str) and value.strip() in ['{}', '[]']:
        return True
    return False

async def find_invalid_records(
    repo: YahooDataRepository,
    threshold: float,
    start_date_str: Optional[str],
    end_date_str: Optional[str]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Scans Yahoo DB tables for records that meet specific emptiness and date criteria.
    - For ticker_master, it checks the columns of the record itself based on the threshold.
    - For ticker_data_items, it checks fields within the payload and the item_key_date.
    """
    logger.info(f"Starting scan for invalid records with threshold: {threshold}, start: {start_date_str}, end: {end_date_str}.")
    invalid_records_summary = {
        "ticker_master": [],
        "ticker_data_items": [],
    }

    # 1. Scan ticker_master table
    try:
        logger.info("Scanning 'ticker_master' table...")
        all_master_records = await repo.get_ticker_masters_by_criteria(filters=None)
        logger.info(f"Found {len(all_master_records)} records in 'ticker_master'. Analyzing...")

        for record in all_master_records:
            if not record or not isinstance(record, dict):
                continue

            total_fields = len(record)
            empty_fields = sum(1 for value in record.values() if _is_value_empty(value))

            if total_fields > 0:
                empty_percentage = empty_fields / total_fields
                if empty_percentage >= threshold:
                    reason = f"{empty_percentage:.0%} of fields are empty ({empty_fields}/{total_fields})."
                    invalid_records_summary["ticker_master"].append({
                        "primary_key": record.get("ticker"),
                        "reason": reason,
                        "details": record
                    })

        logger.info(f"Finished scanning 'ticker_master'. Found {len(invalid_records_summary['ticker_master'])} invalid records.")

    except Exception as e:
        logger.error(f"Error scanning 'ticker_master' table: {e}", exc_info=True)
        invalid_records_summary["ticker_master"].append({"error": f"Failed to scan table: {e}"})

    # 2. Scan ticker_data_items table
    try:
        logger.info("Scanning 'ticker_data_items' table...")
        # Convert date strings to datetime objects once
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else None
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else None

        all_data_items = await repo.get_all_data_items()
        logger.info(f"Found {len(all_data_items)} records in 'ticker_data_items'. Analyzing payload content and dates...")

        for record in all_data_items:
            if not record or not isinstance(record, dict):
                continue
            
            # Date filtering
            item_key_date_any = record.get("item_key_date")
            if isinstance(item_key_date_any, str):
                item_key_date = datetime.strptime(item_key_date_any.split('T')[0], "%Y-%m-%d")
            elif isinstance(item_key_date_any, datetime):
                item_key_date = item_key_date_any
            else:
                item_key_date = None

            if start_date and (not item_key_date or item_key_date < start_date):
                continue
            if end_date and (not item_key_date or item_key_date > end_date):
                continue

            payload_str = record.get("item_data_payload")
            if not payload_str or not isinstance(payload_str, str):
                continue

            try:
                payload_dict = json.loads(payload_str)
                if not isinstance(payload_dict, dict) or not payload_dict:
                    continue  # Skip non-dict or empty dict payloads

                total_fields = len(payload_dict)
                if total_fields == 0:
                    continue

                empty_fields = sum(1 for value in payload_dict.values() if _is_value_empty(value))

                empty_percentage = empty_fields / total_fields
                if empty_percentage >= threshold:
                    reason = f"{empty_percentage:.0%} of payload fields are empty ({empty_fields}/{total_fields})."
                    invalid_records_summary["ticker_data_items"].append({
                        "primary_key": record.get("data_item_id"),
                        "ticker": record.get("ticker"),
                        "item_key_date": record.get("item_key_date"),
                        "reason": reason,
                        "details": record  # Keep the full record for context
                    })
            except json.JSONDecodeError:
                logger.warning(f"Could not parse item_data_payload for data_item_id: {record.get('data_item_id')}.")
                continue

        logger.info(f"Finished scanning 'ticker_data_items'. Found {len(invalid_records_summary['ticker_data_items'])} invalid records.")

    except Exception as e:
        logger.error(f"Error scanning 'ticker_data_items' table: {e}", exc_info=True)
        invalid_records_summary["ticker_data_items"].append({"error": f"Failed to scan table: {e}"})

    logger.info("Finished scanning all tables.")
    return invalid_records_summary

async def delete_invalid_records(repo: YahooDataRepository, records_to_delete: Dict[str, List[Any]]) -> Dict[str, Any]:
    """
    Deletes a list of specified records from the database.

    Args:
        repo: An instance of YahooDataRepository.
        records_to_delete: A dictionary specifying which records to delete.
                           Example: {"ticker_master": ["TICKER1", "TICKER2"], "ticker_data_items": [101, 102]}

    Returns:
        A summary of the deletion operation.
    """
    master_keys_to_delete = records_to_delete.get("ticker_master", [])
    item_ids_to_delete = records_to_delete.get("ticker_data_items", [])

    logger.info(f"Starting deletion of invalid records. Master tickers: {len(master_keys_to_delete)}, Data items: {len(item_ids_to_delete)}.")

    deletion_summary = {
        "deleted_master_count": 0,
        "deleted_items_count": 0,
        "errors": []
    }

    # 1. Delete from ticker_master
    if master_keys_to_delete:
        logger.info(f"Deleting {len(master_keys_to_delete)} records from 'ticker_master'.")
        for ticker in master_keys_to_delete:
            try:
                # This repo method will be created in the next step.
                success = await repo.delete_yahoo_ticker_master(ticker)
                if success:
                    deletion_summary["deleted_master_count"] += 1
                else:
                    deletion_summary["errors"].append({
                        "table": "ticker_master",
                        "primary_key": ticker,
                        "error": "Deletion failed or record not found."
                    })
            except Exception as e:
                logger.error(f"Error deleting ticker '{ticker}' from master table: {e}", exc_info=True)
                deletion_summary["errors"].append({
                    "table": "ticker_master",
                    "primary_key": ticker,
                    "error": str(e)
                })

    # 2. Delete from ticker_data_items
    if item_ids_to_delete:
        logger.info(f"Deleting {len(item_ids_to_delete)} records from 'ticker_data_items'.")
        for item_id in item_ids_to_delete:
            try:
                # This repo method will be created in the next step.
                success = await repo.delete_ticker_data_item(item_id)
                if success:
                    deletion_summary["deleted_items_count"] += 1
                else:
                    deletion_summary["errors"].append({
                        "table": "ticker_data_items",
                        "primary_key": item_id,
                        "error": "Deletion failed or record not found."
                    })
            except Exception as e:
                logger.error(f"Error deleting data item ID '{item_id}': {e}", exc_info=True)
                deletion_summary["errors"].append({
                    "table": "ticker_data_items",
                    "primary_key": item_id,
                    "error": str(e)
                })

    logger.info(f"Deletion complete. Summary: {deletion_summary}")
    return deletion_summary 