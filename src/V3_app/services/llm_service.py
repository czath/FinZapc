# Service for interacting with Google's Gemini LLM.
import httpx
import os
import json
import logging
from typing import Dict, Any, List
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _load_api_key_from_dotenv() -> str | None:
    """
    Manually parses the .env file to find the GOOGLE_API_KEY.
    This is a robust alternative to using a library that may fail in some environments.
    """
    try:
        # The service file is at /src/V3_app/services/, so we go up 3 parents for project root.
        project_root = Path(__file__).resolve().parents[3]
        dotenv_path = project_root / '.env'

        if not dotenv_path.exists():
            logger.warning(f"Manual search: .env file not found at {dotenv_path}")
            return None

        with open(dotenv_path, 'r', encoding='utf-8-sig') as f:
            for line in f:
                line = line.strip()
                if line.startswith("GOOGLE_API_KEY="):
                    key, value = line.split('=', 1)
                    # Remove potential quotes around the value
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    logger.info(f"Manual load: Successfully found GOOGLE_API_KEY in {dotenv_path}")
                    return value
        
        logger.warning(f"Manual search: GOOGLE_API_KEY not found within {dotenv_path}")
        return None
    except Exception as e:
        logger.error(f"Manual load: An error occurred while reading the .env file: {e}", exc_info=True)
        return None

class LLMService:
    """
    A service to interact with Google's Gemini LLM via REST API.
    This approach avoids the google-generativeai SDK and its dependency conflicts.
    """
    def __init__(self):
        # Manually read the key from the .env file to ensure it is loaded.
        self.api_key = _load_api_key_from_dotenv()
        
        # As a fallback, check system environment variables.
        if not self.api_key:
            logger.info("Manual load failed. Checking system environment variables for GOOGLE_API_KEY.")
            self.api_key = os.getenv("GOOGLE_API_KEY")

        if not self.api_key:
            logger.critical("FINAL ATTEMPT FAILED: GOOGLE_API_KEY could not be found in .env or system variables.")
            raise ValueError("GOOGLE_API_KEY not found. Please ensure it is in the .env file at the project root or set as a system environment variable.")
        
        self.api_url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"

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
                "temperature": 0.3,
                "topK": 1,
                "topP": 1,
                "maxOutputTokens": 2048,
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
        prompt = f"""
        **Role:** You are a Financial Analyst AI. Your task is to generate a concise, insightful financial report based on the provided data and user query. The report should be in clean, well-formatted Markdown.

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
                profile_date = data["profile"]["update_last_full"]
                source_info.append(f"- Company Profile updated as of: {profile_date}")
            
            # Dynamically iterate over all keys in the data dictionary to find financial statements
            for key, statement_data in data.items():
                # Skip the profile as it's handled above, and skip any non-dictionary or empty items
                if key == "profile" or not isinstance(statement_data, dict) or not statement_data:
                    continue

                # The payload is in 'item_data_payload' and the date is in 'item_key_date'.
                if "item_key_date" in statement_data and "item_data_payload" in statement_data:
                    name = ' '.join(word.capitalize() for word in key.split('_'))
                    key_date = statement_data["item_key_date"]
                    source_info.append(f"- {name} key date: {key_date}")
            
            if source_info:
                prompt += "\n".join(source_info) + "\n\n"
            else:
                prompt += "No specific source dates available.\n\n"

            # --- Detailed Data Section ---
            
            # Append data sections if they exist and are not empty
            if data.get("profile"):
                # We'll create a copy to avoid modifying the original data dict
                profile_copy = data["profile"].copy()
                # The raw update date is already in the source list, no need to show it raw again
                profile_copy.pop("update_last_full", None) 
                prompt += f"#### Company Profile Details:\n```json\n{json.dumps(profile_copy, indent=2)}\n```\n"

            # Dynamically add all available data payloads to the prompt
            for key, statement_data in data.items():
                if key != "profile" and isinstance(statement_data, dict):
                    payload_dict = statement_data.get("item_data_payload")
                    # The payload should already be a dictionary. Use it directly.
                    if isinstance(payload_dict, dict):
                        name = ' '.join(word.capitalize() for word in key.split('_'))
                        prompt += f"#### {name}:\n```json\n{json.dumps(payload_dict, indent=2)}\n```\n"
            
            prompt += "---\n"

        prompt += """
        **Instructions:**
        1.  Analyze the provided data in the context of the user's request.
        2.  Do not hallucinate or invent data not present in this prompt.
        3.  Structure your response clearly using Markdown (headings, bold text, lists).
        4.  Provide a concluding summary that directly addresses the user's core question.
        5.  After this initial analysis, the user may ask follow-up questions. Be ready to answer them based on the data you have been provided.
        """
        return prompt 