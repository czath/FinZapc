#!/usr/bin/env python
import websocket
import time
import ssl
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("websocket-client-test")

def on_message(ws, message):
    logger.info(f"<<< Received Message: {message}")
    # Optional: Try parsing as JSON
    try:
        data = json.loads(message)
        # You could add checks for topics like 'system', 'act', 'sts', 'mktData' here
        if 'topic' in data:
             logger.info(f"<<< Parsed Topic: {data.get('topic')}")
    except json.JSONDecodeError:
        logger.warning("<<< Received non-JSON message")

def on_error(ws, error):
    logger.error(f"!!! Error: {error}")

def on_close(ws, close_status_code, close_msg):
    # Note: The signature provided by the user was slightly off, added status/msg
    logger.warning(f"### CLOSED! Code: {close_status_code}, Msg: {close_msg} ###") 

def on_open(ws):
    logger.info("### Connection Opened ###")
    # Add a slight delay before sending, as per example
    logger.info("Waiting 3 seconds before sending subscriptions...")
    time.sleep(3)
    
    # Example Conids (Use ones known to work with REST snapshot if possible)
    conids = ["265598"] # Example: AAPL 
    fields = ["31"]   # Example: Last Price
    
    logger.info(f"Sending subscriptions for conids: {conids} with fields: {fields}...")
    for conid in conids:
        payload_dict = {"fields": fields}
        try:
            payload_json = json.dumps(payload_dict, separators=(',', ':'))
            subscription_message = f"smd+{conid}+{payload_json}"
            logger.info(f">>> Sending: {subscription_message}")
            ws.send(subscription_message)
        except Exception as e:
            logger.error(f"!!! Error formatting/sending subscription for {conid}: {e}")

    # Optional: Send a tic ping after subscribing
    # logger.info(">>> Sending: tic")
    # ws.send("tic")

if __name__ == "__main__":
    logger.info("Starting websocket-client test...")
    # Enable trace for debugging connection details if needed
    # websocket.enableTrace(True)
    
    ws_url = "wss://localhost:5000/v1/api/ws"
    logger.info(f"Connecting to: {ws_url}")
    
    ws = websocket.WebSocketApp(
        url=ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        # Note: NO cookie header is being sent here
    )
    
    # Run forever, ignoring SSL certificate validation
    logger.info("Starting run_forever (blocking call)... Press Ctrl+C to exit.")
    try:
        ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, closing connection...")
        ws.close() # Attempt graceful close
    except Exception as e:
         logger.error(f"!!! Unexpected error in run_forever: {e}", exc_info=True)
    finally:
         logger.info("Exiting websocket-client test.") 