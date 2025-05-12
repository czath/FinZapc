import logging
from typing import Dict, Any, List, Optional, Union

logger = logging.getLogger(__name__)

def _parse_finviz_value(value_str: Optional[str]) -> Union[float, int, str, None]:
    """
    Attempts to parse a Finviz string value into a float, int, or handles
    common suffixes (K, M, B, T, %) and missing values ('-', 'N/A', '').
    If parsing fails, the original string is returned.
    """
    if value_str is None or not isinstance(value_str, str):
        return None

    original_value_str = value_str # Store original value
    processed_value_str = value_str.strip()

    # Handle empty string explicitly after strip
    if processed_value_str in ('-', 'N/A', ''):
        return None

    try:
        # Percentage
        if processed_value_str.endswith('%'):
            # Return as the number before division by 100 for now, easier to display
            # Change to '/ 100.0' if decimal representation is preferred
            return float(processed_value_str[:-1])

        value_to_convert = processed_value_str # Start with the stripped string
        multiplier = 1
        if processed_value_str.endswith('K'):
            multiplier = 1_000
            value_to_convert = processed_value_str[:-1]
        elif processed_value_str.endswith('M'):
            multiplier = 1_000_000
            value_to_convert = processed_value_str[:-1]
        elif processed_value_str.endswith('B'):
            multiplier = 1_000_000_000
            value_to_convert = processed_value_str[:-1]
        elif processed_value_str.endswith('T'): # Added Trillion
             multiplier = 1_000_000_000_000
             value_to_convert = processed_value_str[:-1]

        # Try float conversion first (handles decimals), then int
        try:
            num_val = float(value_to_convert) * multiplier
            # Return as int if it's effectively an integer
            if num_val == int(num_val):
                return int(num_val)
            return num_val
        except ValueError:
            # If float conversion fails, return the original unmodified string
            logger.debug(f"Could not convert '{value_to_convert}' (from '{original_value_str}') to float. Returning original string.")
            return original_value_str

    except Exception as e:
        logger.warning(f"Could not parse value '{original_value_str}': {e}")
        return original_value_str # Return original string on unexpected error

def preprocess_raw_analytics_data(raw_analytics_entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Preprocesses raw analytics data strings (e.g., from Finviz, potentially others)
    from the database into structured dictionaries.
    Input format assumes: [{'ticker': str, 'source': str, 'raw_data': str}, ...]
    Raw data format assumes: "key1=value1,key2=value2,key3=value3,..."

    Args:
        raw_analytics_entries: List of dicts, each containing 'ticker', 'source', 'raw_data'.

    Returns:
        List of dicts, each {'ticker': str, 'source': str, 'processed_data': dict, 'error': Optional[str]}.
    """
    processed_list = []
    for entry in raw_analytics_entries:
        ticker = entry.get('ticker')
        source = entry.get('source')
        raw_data = entry.get('raw_data')
        
        processed_entry = {'ticker': ticker, 'source': source, 'processed_data': {}, 'error': None}

        if not ticker:
            logger.warning(f"Skipping entry with missing ticker (source: {source}).")
            processed_entry['error'] = "Missing ticker"
            processed_list.append(processed_entry)
            continue
        
        if not source:
             logger.warning(f"Processing entry with missing source (ticker: {ticker}).")

        if not raw_data or not isinstance(raw_data, str):
            logger.warning(f"Skipping entry for ticker {ticker} (source: {source}) due to missing or invalid raw_data.")
            processed_entry['error'] = "Missing or invalid raw_data"
            processed_list.append(processed_entry)
            continue

        try:
            processed_fields = {}
            # Split the raw data string by commas
            kv_pairs = raw_data.split(',')

            for kv_pair_string in kv_pairs:
                kv_pair_string = kv_pair_string.strip()
                if not kv_pair_string: # Skip empty parts
                    continue

                # Split each part by the first '=' sign
                parts = kv_pair_string.split('=', 1)
                key = parts[0].strip()

                if not key: # Skip if key is empty
                    logger.debug(f"Empty key found for ticker {ticker} (source: {source}) in pair '{kv_pair_string}'")
                    continue

                # Get value, handle case where '=' is missing or value is empty after '=' 
                value_str = parts[1].strip() if len(parts) > 1 else '' 

                # Parse the value string (using the existing Finviz parser for now)
                parsed_value = _parse_finviz_value(value_str)
                processed_fields[key] = parsed_value

            processed_entry['processed_data'] = processed_fields

        except Exception as e:
            logger.error(f"Error processing raw data for ticker {ticker} (source: {source}): {e}", exc_info=True)
            processed_entry['error'] = f"Parsing failed: {e}"

        processed_list.append(processed_entry)

    return processed_list

# --- Example Usage (Conceptual - keep commented out in final file) ---
# if __name__ == '__main__':
#     sample_raw_data = [
#         {'ticker': 'AAPL', 'raw_data': 'Index=,P/E=25.5,EPS (ttm)=5.89,Market Cap=2.1T,Volume=80M,Avg Volume=95.5K,Sector=Technology,Dividend=0.5%'}, # Added Index=,
#         {'ticker': 'GOOG', 'raw_data': 'P/E=20.1,EPS (ttm)=10.2,Market Cap=1.8B,Volume=1.5M,Avg Volume=2.1M,Sector=Communication Services,Change=-1.2%,'}, # Added trailing ,
#         {'ticker': 'MSFT', 'raw_data': 'P/E=30,Market Cap=2.5T,SomethingElse,EmptyKey='}, # Added EmptyKey=
#         {'ticker': 'NVDA', 'raw_data': None},
#         {'ticker': 'FAKE', 'raw_data': 'Invalid Data Format, NoEqualsHere'}, # Malformed
#         {'ticker': None, 'raw_data': 'P/E=10'},
#         {'ticker': 'EMPTY', 'raw_data': ''},
#         {'ticker': 'NUMONLY', 'raw_data': 'Price=123.45,Shares=1000'}
#     ]
#
#     processed_data = preprocess_finviz_data(sample_raw_data)
#     import json
#     print(json.dumps(processed_data, indent=2)) 

# --- Helper to expose Finviz field metadata for analytics config ---
def get_finviz_fields_for_analytics(processed_data: list[dict]) -> list[dict]:
    """
    Returns a list of Finviz fields for analytics configuration:
    - Each field: {name, type, description}
    - Uses the same logic as the frontend (fieldMetadata)
    """
    if not processed_data:
        return []
    # Discover fields
    discovered_fields = set()
    for item in processed_data:
        if item and 'processed_data' in item:
            discovered_fields.update(item['processed_data'].keys())
    discovered_fields.add('source')
    available_fields = sorted(discovered_fields)
    # Calculate metadata
    field_metadata = {}
    for field in available_fields:
        values = []
        for item in processed_data:
            if item and 'processed_data' in item and field in item['processed_data']:
                v = item['processed_data'][field]
                if v is not None:
                    values.append(v)
        existing_value_count = len(values)
        if existing_value_count == 0:
            field_metadata[field] = { 'type': 'empty', 'description': 'Empty' }
        else:
            numeric_count = sum(1 for v in values if isinstance(v, (int, float)) and not isinstance(v, bool))
            if numeric_count / existing_value_count >= 0.8:
                # Numeric field
                min_v = min((v for v in values if isinstance(v, (int, float))), default=None)
                max_v = max((v for v in values if isinstance(v, (int, float))), default=None)
                avg_v = sum((v for v in values if isinstance(v, (int, float))), 0) / numeric_count if numeric_count else None
                sorted_vals = sorted(v for v in values if isinstance(v, (int, float)))
                median_v = None
                if sorted_vals:
                    mid = len(sorted_vals) // 2
                    if len(sorted_vals) % 2 == 0:
                        median_v = (sorted_vals[mid-1] + sorted_vals[mid]) / 2
                    else:
                        median_v = sorted_vals[mid]
                desc = f"Numeric | Min: {min_v} | Max: {max_v} | Avg: {avg_v} | Median: {median_v}"
                field_metadata[field] = { 'type': 'numeric', 'description': desc }
            else:
                unique_vals = set(str(v) for v in values if isinstance(v, str))
                desc = f"Text ({len(unique_vals)} unique)"
                field_metadata[field] = { 'type': 'text', 'description': desc }
    # Build output
    return [
        { 'name': field, 'type': field_metadata[field]['type'], 'description': field_metadata[field]['description'] }
        for field in available_fields
    ] 