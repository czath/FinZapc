import logging
from typing import Dict, Any, List, Optional, Union

logger = logging.getLogger(__name__)

def _parse_finviz_value(value_str: Optional[str]) -> Union[float, int, str, None]:
    """
    Attempts to parse a Finviz string value into a float, int, or handles
    common suffixes (K, M, B, T, %) and missing values ('-', 'N/A', '').
    """
    if value_str is None or not isinstance(value_str, str):
        return None

    value_str = value_str.strip()
    # Handle empty string explicitly after strip
    if value_str in ('-', 'N/A', ''):
        return None

    try:
        # Percentage
        if value_str.endswith('%'):
            # Return as the number before division by 100 for now, easier to display
            # Change to '/ 100.0' if decimal representation is preferred
            return float(value_str[:-1])

        multiplier = 1
        if value_str.endswith('K'):
            multiplier = 1_000
            value_str = value_str[:-1]
        elif value_str.endswith('M'):
            multiplier = 1_000_000
            value_str = value_str[:-1]
        elif value_str.endswith('B'):
            multiplier = 1_000_000_000
            value_str = value_str[:-1]
        elif value_str.endswith('T'): # Added Trillion
             multiplier = 1_000_000_000_000
             value_str = value_str[:-1]

        # Try float conversion first (handles decimals), then int
        try:
            num_val = float(value_str) * multiplier
            # Return as int if it's effectively an integer
            if num_val == int(num_val):
                return int(num_val)
            return num_val
        except ValueError:
            # If float conversion fails, return original string (handles non-numeric data)
            # Reconstruct original if suffix was removed
            if multiplier > 1:
                 original_suffix = ''
                 if multiplier == 1_000: original_suffix = 'K'
                 elif multiplier == 1_000_000: original_suffix = 'M'
                 elif multiplier == 1_000_000_000: original_suffix = 'B'
                 elif multiplier == 1_000_000_000_000: original_suffix = 'T'
                 # Check if the original value string itself was numeric before adding suffix back
                 try:
                     float(value_str) # Can it be converted back to float?
                     return value_str + original_suffix # Return string like "1.2T"
                 except ValueError:
                     # If the part before the suffix wasn't numeric, return that part only
                     return value_str
            return value_str # Return original string if simple float conversion failed

    except Exception as e:
        logger.warning(f"Could not parse value '{value_str}': {e}")
        return value_str # Return original string on unexpected error

def preprocess_finviz_data(raw_finviz_entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Preprocesses raw Finviz data strings from the database into structured dictionaries.
    Assumes format like: "key1=value1,key2=value2,key3=value3,..."

    Args:
        raw_finviz_entries: List of dicts, each {'ticker': str, 'raw_data': str}.

    Returns:
        List of dicts, each {'ticker': str, 'processed_data': dict, 'error': Optional[str]}.
    """
    processed_list = []
    for entry in raw_finviz_entries:
        ticker = entry.get('ticker')
        raw_data = entry.get('raw_data')
        processed_entry = {'ticker': ticker, 'processed_data': {}, 'error': None}

        if not ticker:
            logger.warning("Skipping entry with missing ticker.")
            processed_entry['error'] = "Missing ticker"
            processed_list.append(processed_entry)
            continue

        if not raw_data or not isinstance(raw_data, str):
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
                    logger.debug(f"Empty key found for ticker {ticker} in pair '{kv_pair_string}'")
                    continue

                # Get value, handle case where '=' is missing or value is empty after '='
                value_str = parts[1].strip() if len(parts) > 1 else ''

                # Parse the value string
                parsed_value = _parse_finviz_value(value_str)
                processed_fields[key] = parsed_value

            processed_entry['processed_data'] = processed_fields

        except Exception as e:
            logger.error(f"Error processing raw data for ticker {ticker}: {e}", exc_info=True)
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