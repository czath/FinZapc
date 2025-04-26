import asyncio
import httpx
import logging
import json
import os
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# Get a logger instance
logger = logging.getLogger(__name__)

# --- Module-level variable to store the latest status --- 
latest_status_payload = json.dumps({"type": "ibkr_status", "status": "INITIALIZING"})

# --- WebSocket Connection Manager (Dedicated for IBKR Status) --- 
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"IBKR Status WS connected: {websocket.client}. Total status connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"IBKR Status WS disconnected: {websocket.client}. Total status connections: {len(self.active_connections)}")
        else:
            logger.warning(f"Attempted to disconnect an unknown IBKR Status WebSocket: {websocket.client}")

    async def broadcast(self, message: str):
        tasks = [connection.send_text(message) for connection in self.active_connections]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed_connection = self.active_connections[i]
                logger.error(f"Failed to send IBKR status to WebSocket {failed_connection.client}: {result}")
                # Optionally disconnect problematic clients
                # self.disconnect(failed_connection) 
# --- End WebSocket Connection Manager ---

# --- IBKR Connection Monitoring Task ---
async def monitor_ibkr_connection(manager: ConnectionManager):
    """Background task to poll IBKR /tickle endpoint and broadcast status via WebSocket."""
    global latest_status_payload # Declare we are modifying the global variable
    # Ideally, get this from config/env var
    gateway_base_url = os.getenv("IBKR_GATEWAY_URL", "https://localhost:5000")
    tickle_url = f"{gateway_base_url}/v1/api/tickle"
    # Disable SSL verification for localhost if using default self-signed cert
    ssl_verify = not gateway_base_url.startswith("https://localhost") 
    
    logger.info(f"IBKR Monitor Task: Starting. Polling: {tickle_url}")
    if not ssl_verify:
        logger.warning("IBKR Monitor Task: SSL verification DISABLED for localhost.")

    async with httpx.AsyncClient(verify=ssl_verify) as client:
        while True:
            status_code = "ERROR_UNKNOWN" # Default to unknown error
            status_message = "Error: Unknown"
            status_details = {}
            try:
                response = await client.get(tickle_url, timeout=10.0) # Add timeout
                response.raise_for_status() # Raise HTTPStatusError for 4xx/5xx
                
                tickle_data = response.json()
                status_details = tickle_data # Store details for potential logging
                
                iserver_status = tickle_data.get('iserver', {}).get('authStatus', {})
                authenticated = iserver_status.get('authenticated')
                connected = iserver_status.get('connected')

                if authenticated and connected:
                    status_code = "CONNECTED"
                elif not connected:
                    status_code = "DISCONNECTED_GW_ERROR"
                elif not authenticated:
                    status_code = "DISCONNECTED_NO_AUTH"
                else:
                    status_code = "DISCONNECTED_UNKNOWN"

            except httpx.TimeoutException:
                status_code = "ERROR_TIMEOUT"
                logger.warning(f"IBKR Monitor Task: Timeout reaching {tickle_url}")
            except httpx.RequestError as e:
                status_code = "ERROR_NETWORK"
                # Reduce log level for frequent network errors if gateway is often down
                logger.warning(f"IBKR Monitor Task: Request error reaching {tickle_url}: {e}") 
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401 or e.response.status_code == 403:
                    status_code = "ERROR_API_AUTH"
                else:
                    status_code = f"ERROR_API_OTHER_{e.response.status_code}"
                logger.error(f"IBKR Monitor Task: HTTP error from {tickle_url}: {e}")
                try:
                    logger.error(f"IBKR Monitor Task: Error Response Body: {e.response.text}")
                except Exception:
                    pass # Ignore errors reading body
            except json.JSONDecodeError:
                status_code = "ERROR_INVALID_RESPONSE"
                logger.error(f"IBKR Monitor Task: Failed to decode JSON from {tickle_url}")
            except Exception as e:
                status_code = "ERROR_UNKNOWN"
                logger.error(f"IBKR Monitor Task: Unexpected error: {e}", exc_info=True)
                if status_details:
                    logger.error(f"IBKR Monitor Task: Data at time of error: {status_details}")

            # Store the latest status payload globally
            latest_status_payload = json.dumps({"type": "ibkr_status", "status": status_code})
            
            # Broadcast the latest status
            try:
                await manager.broadcast(latest_status_payload)
                # Reduce frequency of successful broadcast logging
                # logger.debug(f"IBKR Monitor Task: Broadcasted status code: {status_code}") 
            except Exception as broadcast_err:
                logger.error(f"IBKR Monitor Task: Failed to broadcast status: {broadcast_err}")

            await asyncio.sleep(30) # Poll every 30 seconds
# --- End IBKR Connection Monitoring Task ---

# --- Registration Function --- 
def register_ibkr_monitor(app: FastAPI):
    """Registers the IBKR status WebSocket endpoint and starts the monitoring task."""
    
    # Create a dedicated manager instance for this monitor
    ibkr_status_manager = ConnectionManager()
    
    # Define the WebSocket endpoint within this function's scope
    @app.websocket("/ws/ibkr_status")
    async def websocket_ibkr_status_endpoint(websocket: WebSocket):
        global latest_status_payload # Access the global status
        await ibkr_status_manager.connect(websocket)
        # Send the current status immediately on connection
        try:
            await websocket.send_text(latest_status_payload)
        except Exception as send_err:
            logger.warning(f"IBKR Status WS: Failed to send initial status to {websocket.client}: {send_err}")
            # Continue trying to handle the connection anyway
            
        try:
            while True:
                # Keep connection open and listen for disconnect
                await websocket.receive_text() # We don't expect messages here
        except WebSocketDisconnect:
            ibkr_status_manager.disconnect(websocket)
            logger.info(f"IBKR Status WS client disconnected: {websocket.client}")
        except Exception as e:
            logger.error(f"IBKR Status WS error: {e}", exc_info=True)
            ibkr_status_manager.disconnect(websocket) # Disconnect on error

    # Define the startup task function
    async def startup_monitor_task():
        logger.info("App startup: Creating IBKR connection monitoring task.")
        asyncio.create_task(monitor_ibkr_connection(ibkr_status_manager))

    # Register the startup event handler
    app.add_event_handler("startup", startup_monitor_task)
    logger.info("IBKR connection monitor WebSocket route and startup task registered.")
# --- End Registration Function --- 

# New file for IBKR connection monitoring logic 