# Service for interacting with Google's Gemini LLM.
import httpx
import os
import json
import logging
from typing import Dict, Any, List
from pathlib import Path
import textwrap

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _parse_env_value(value: str) -> Any:
    """
    Parses an environment variable string into a Python type (int, float, bool, str).
    Handles quoted strings and escaped newlines.
    """
    value = value.strip()
    if (value.startswith('"') and value.endswith('"')) or \
       (value.startswith("'") and value.endswith("'")):
        value = value[1:-1].replace('\\n', '\n')

    if value.lower() == 'true':
        return True
    if value.lower() == 'false':
        return False

    try:
        return int(value)
    except ValueError:
        pass
    
    try:
        return float(value)
    except ValueError:
        pass
        
    return value

def _load_config_from_dotenv() -> Dict[str, Any]:
    """
    Manually parses the .env file to find all variables.
    This is a robust alternative to using a library that may fail in some environments.
    Returns a dictionary of settings.
    """
    config = {}
    try:
        # The service file is at /src/V3_app/services/, so we go up 3 parents for project root.
        project_root = Path(__file__).resolve().parents[3]
        dotenv_path = project_root / '.env'

        if not dotenv_path.exists():
            logger.warning(f"Manual search: .env file not found at {dotenv_path}")
            return config

        with open(dotenv_path, 'r', encoding='utf-8-sig') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    config[key] = _parse_env_value(value)
        
        logger.info(f"Manual load: Successfully loaded {len(config)} variables from {dotenv_path}")
        return config
    except Exception as e:
        logger.error(f"Manual load: An error occurred while reading the .env file: {e}", exc_info=True)
        return config

class LLMService:
    """
    A service to interact with Google's Gemini LLM via REST API.
    This approach avoids the google-generativeai SDK and its dependency conflicts.
    """
    def __init__(self):
        # Load all config from .env, then get specific values with defaults.
        env_config = _load_config_from_dotenv()
        
        # API Key handling
        self.api_key = env_config.get("GOOGLE_API_KEY") or os.getenv("GOOGLE_API_KEY")

        if not self.api_key:
            logger.critical("FINAL ATTEMPT FAILED: GOOGLE_API_KEY could not be found in .env or system variables.")
            raise ValueError("GOOGLE_API_KEY not found. Please ensure it is in the .env file at the project root or set as a system environment variable.")
        
        self.api_url = env_config.get("LLM_API_URL")
        if not self.api_url:
            logger.critical("LLM_API_URL not found in config.")
            raise ValueError("LLM_API_URL must be set in the .env file.")

        # Extract model name for display purposes
        try:
            # e.g., https://.../v1beta/models/gemini-1.5-flash:generateContent
            self.model_name = self.api_url.split('/models/')[1].split(':')[0]
        except IndexError:
            self.model_name = "Unknown Model"

        # Generation Config with type casting and defaults
        self.temperature = float(env_config.get("LLM_TEMPERATURE", 0.3))
        self.top_k = int(env_config.get("LLM_TOP_K", 1))
        self.top_p = int(env_config.get("LLM_TOP_P", 1))
        self.max_output_tokens = int(env_config.get("LLM_MAX_OUTPUT_TOKENS", 2048))

        # Prompt components are now expected to be in the .env file.
        # The fallback values have been removed to enforce this.
        self.prompt_role = env_config.get("LLM_PROMPT_ROLE")
        self.prompt_instructions = env_config.get("LLM_PROMPT_INSTRUCTIONS")

        if not self.prompt_role or not self.prompt_instructions:
            logger.critical("LLM_PROMPT_ROLE and/or LLM_PROMPT_INSTRUCTIONS not found in config.")
            raise ValueError("LLM_PROMPT_ROLE and LLM_PROMPT_INSTRUCTIONS must be set in the .env file.")

    async def generate_report(self, tickers_data: List[Dict[str, Any]], user_prompt: str) -> Dict[str, Any]:
        """
        Generates the initial financial report and provides the conversation history.

        Args:
            tickers_data: A list of dictionaries, where each dict contains data for a ticker.
            user_prompt: The user's specific query or prompt for the analysis.

        Returns:
            A dictionary containing the report markdown and the initial conversation history.
        """
        if not tickers_data:
            logger.warning("generate_report called with no tickers_data.")
            return {"report_markdown": "Error: No data was provided for the financial report.", "history": []}

        # 1. Construct the initial detailed prompt
        system_prompt = self._construct_prompt(tickers_data, user_prompt)

        # This is the first "user" turn in the conversation
        history = [
            {"role": "user", "parts": [{"text": system_prompt}]}
        ]

        # 2. Call the chat method to get the first response
        report_text = await self.continue_chat(history)

        if not report_text.startswith("Error:"):
            # Add the model's response to the history
            history.append({"role": "model", "parts": [{"text": report_text}]})
        
        return {"report_markdown": report_text, "history": history}

    async def continue_chat(self, history: List[Dict[str, Any]]) -> str:
        """
        Sends the conversation history to the Gemini LLM and gets a response.

        Args:
            history: The full conversation history.

        Returns:
            The LLM-generated response in Markdown format.
        """
        if not history:
            logger.warning("continue_chat called with empty history.")
            return "Error: Conversation history is empty."
        
        # 2. Prepare the request payload for the Gemini API
        payload = {
            "contents": history, # The entire conversation history is sent
            "generationConfig": {
                "temperature": self.temperature,
                "topK": self.top_k,
                "topP": self.top_p,
                "maxOutputTokens": self.max_output_tokens,
                "stopSequences": []
            },
            "safetySettings": [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"}
            ]
        }

        headers = {
            'Content-Type': 'application/json'
        }
        
        query_params = {
            'key': self.api_key
        }

        # 3. Make the asynchronous API call
        try:
            async with httpx.AsyncClient(timeout=90.0) as client:
                logger.info("Sending request to Gemini API...")
                response = await client.post(self.api_url, headers=headers, params=query_params, json=payload)
                response.raise_for_status() # Raises HTTPStatusError for 4xx/5xx responses
                
                logger.info("Successfully received response from Gemini API.")
                
                # 4. Extract and return the content
                response_data = response.json()
                
                # Navigate the JSON structure to get the text
                content_parts = response_data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])
                if content_parts and 'text' in content_parts[0]:
                    return content_parts[0]['text']
                else:
                    logger.error(f"Could not extract content from API response. Full response: {response.text}")
                    return "Error: Failed to extract a valid report from the API response."

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred while calling Gemini API: {e.response.status_code} - {e.response.text}")
            return f"Error: The API returned a status of {e.response.status_code}. Please check the server logs."
        except Exception as e:
            logger.error(f"An unexpected error occurred in continue_chat: {e}", exc_info=True)
            return "Error: An unexpected error occurred while generating the report."

    def _construct_prompt(self, tickers_data: List[Dict[str, Any]], user_prompt: str) -> str:
        """
        Constructs a detailed, structured prompt for the LLM based on the available data.
        """
        prompt = f"""{self.prompt_role}

        **User's Request:** "{user_prompt}"

        **Available Data:**
        Here is the financial data for the requested tickers. For each ticker, a profile is provided along with its last update date, and various financial statements, each with its own 'key date' indicating the time of that specific data point.
        ---
        """

        for data in tickers_data:
            # Safely get the ticker symbol from the profile data
            ticker = data.get("profile", {}).get("ticker", "N/A")
            prompt += f"\n## Analysis for Ticker: {ticker}\n"

            # --- Data Sources Section ---
            prompt += "### Data Sources Used:\n"
            source_info = []
            
            # Add profile source date if available
            if data.get("profile") and data["profile"].get("update_last_full"):
                profile_date_str = data["profile"]["update_last_full"]
                # Re-format timestamp to just the date part (YYYY-MM-DD) by splitting at 'T'
                profile_date = profile_date_str.split("T")[0]
                source_info.append(f"- Company Profile updated as of: {profile_date}")
            
            # Dynamically iterate over all keys in the data dictionary to find financial statements
            for key, statement_data in data.items():
                # Skip the profile as it's handled above, and skip any non-dictionary or empty items
                if key == "profile" or not isinstance(statement_data, dict) or not statement_data:
                    continue

                # The payload is in 'item_data_payload' and the date is in 'item_key_date'.
                if "item_key_date" in statement_data and "item_data_payload" in statement_data:
                    name = ' '.join(word.capitalize() for word in key.split('_'))
                    statement_date_str = statement_data['item_key_date']
                    # Re-format timestamp to just the date part (YYYY-MM-DD) by splitting at 'T'
                    statement_date = statement_date_str.split("T")[0] if statement_date_str else "N/A"
                    source_info.append(f"- {name} data as of: {statement_date}")
            
            if source_info:
                prompt += "\n".join(source_info)
            else:
                prompt += "No specific source dates found for this ticker."

            # --- Financial Data Section ---
            prompt += "\n\n### Financial Data Details:\n"
            for key, statement_data in data.items():
                if key == "profile":
                    prompt += f"\n**{key.capitalize()}**:\n```json\n{json.dumps(statement_data, indent=2)}\n```\n"
                elif isinstance(statement_data, dict) and "item_data_payload" in statement_data:
                    name = ' '.join(word.capitalize() for word in key.split('_'))
                    payload = statement_data.get("item_data_payload", {})
                    prompt += f"\n**{name}**:\n```json\n{json.dumps(payload, indent=2)}\n```\n"
            
            prompt += "\n---\n" # Separator between tickers

        prompt += "\nRemember to follow all instructions, especially the one about including the 'Sources' section at the end of your report."
        return prompt 