import asyncio
import aiohttp
import sqlite3
import time
import pandas as pd
from datetime import datetime
import logging
import sys
import json
import websockets # pip install websockets

# --- Configuration ---
CONFIG = {
    "EXCHANGES": {
        "binance": {
            "rest_url": "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
            "ws_url": "wss://stream.binance.com:9443/ws/btcusdt@trade", # Public trade stream
            "rest_rate_limit_delay_s": 0.5, # Minimum delay between REST requests to Binance
            "retry_after_header_key": "Retry-After" # Binance uses this header for rate limits
        },
        "coinbase": {
            "rest_url": "https://api.coinbase.com/v2/prices/BTC-USD/spot",
            "ws_url": None, # Coinbase public WS for spot prices is more complex (needs subscription)
            "rest_rate_limit_delay_s": 1.0, # Minimum delay between REST requests to Coinbase
            "retry_after_header_key": None # Coinbase might not send this or uses different headers
        },
        # Add more exchanges here. Remember to adjust 'rest_rate_limit_delay_s'
        # and parse their specific REST/WS responses.
        # "kraken": {
        #     "rest_url": "https://api.kraken.com/0/public/Ticker?pair=XBTUSD",
        #     "ws_url": "wss://ws.kraken.com/", # Needs subscription message
        #     "rest_rate_limit_delay_s": 1.5,
        #     "retry_after_header_key": None
        # },
    },
    "DATABASE_NAME": "crypto_data_advanced_ws.db",
    "REST_FETCH_INTERVAL_SECONDS": 10, # How often to poll REST APIs (WebSockets are continuous)
    "RETRY_ATTEMPTS": 5,
    "RETRY_BASE_DELAY_SECONDS": 1, # Initial delay for retries, will double (1, 2, 4, 8, 16s)
    "WS_RECONNECT_DELAY_SECONDS": 5, # Delay before attempting WebSocket reconnect
    "LOG_LEVEL": logging.INFO, # Set to logging.DEBUG for more verbose output
    "DISPLAY_DATA_INTERVAL_SECONDS": 5 # How often to display latest data from DB
}

# --- Setup Logging ---
logging.basicConfig(
    level=CONFIG["LOG_LEVEL"],
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Dictionary to store last request time for rate limiting
last_request_time = {exchange: 0.0 for exchange in CONFIG["EXCHANGES"]}
# Dictionary to store per-exchange locks for rate limiting
exchange_locks = {exchange: asyncio.Lock() for exchange in CONFIG["EXCHANGES"]}


# --- Database Operations ---

async def init_db():
    """
    Initializes the SQLite database and creates the table if it doesn't exist.
    """
    conn = sqlite3.connect(CONFIG["DATABASE_NAME"])
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                timestamp TEXT NOT NULL,
                source TEXT NOT NULL -- 'REST' or 'WS'
            )
        """)
        conn.commit()
        logger.info(f"Database '{CONFIG['DATABASE_NAME']}' initialized.")
    except sqlite3.Error as e:
        logger.error(f"Database initialization error: {e}")
    finally:
        conn.close()

async def insert_data(exchange: str, symbol: str, price: float, timestamp: str, source: str):
    """
    Inserts fetched market data into the SQLite database.
    """
    conn = sqlite3.connect(CONFIG["DATABASE_NAME"])
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO market_data (exchange, symbol, price, timestamp, source) VALUES (?, ?, ?, ?, ?)",
            (exchange, symbol, price, timestamp, source)
        )
        conn.commit()
        logger.info(f"Inserted: {exchange} | {symbol} | {price} | {source} at {timestamp}") # Changed to INFO
    except sqlite3.Error as e:
        logger.error(f"Database error inserting data: {e}")
    finally:
        conn.close()

# --- Data Parsing ---

def parse_exchange_data(exchange_name: str, raw_data: dict, source: str) -> dict | None:
    """
    Parses raw exchange data (from REST or WS) into a standardized format.
    Returns a dict with 'exchange', 'symbol', 'price', 'timestamp', 'source'.
    """
    timestamp = datetime.now().isoformat()
    price = None
    symbol = "UNKNOWN"

    try:
        if exchange_name == "binance":
            if source == "REST":
                price = float(raw_data.get("price"))
                symbol = raw_data.get("symbol", "BTCUSDT")
            elif source == "WS":
                if raw_data.get("e") == "trade": # Binance trade stream
                    price = float(raw_data.get("p"))
                    symbol = raw_data.get("s") # Corrected: use 'raw_data'
        elif exchange_name == "coinbase":
            if source == "REST":
                data_node = raw_data.get("data", {})
                price = float(data_node.get("amount"))
                base = data_node.get("base")
                currency = data_node.get("currency")
                if base and currency:
                    symbol = f"{base}{currency}"
                else:
                    symbol = "BTCUSD" # Fallback
            # Coinbase WS needs specific subscription messages, not just raw trades like Binance
            # For this example, we're not implementing Coinbase WS parsing yet, as it's more complex.
            # If WS data comes for Coinbase, it would be handled here.
            # For now, it's None in CONFIG.
        # Add parsing for other exchanges here for both REST and WS

        if price is not None and symbol != "UNKNOWN":
            return {
                "exchange": exchange_name,
                "symbol": symbol,
                "price": price,
                "timestamp": timestamp,
                "source": source
            }
        else:
            logger.debug(f"Could not parse price/symbol for {exchange_name} ({source}). Raw: {raw_data}")
            return None
    except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
        logger.error(f"Error parsing {source} data from {exchange_name}: {e}. Raw: {raw_data}")
        return None

# --- Data Fetching Operations (REST) ---

async def fetch_data_with_retry(session: aiohttp.ClientSession, exchange_name: str, url: str) -> dict:
    """
    Fetches data from a given exchange API URL with retry logic and rate limiting.
    Returns a dictionary with parsed data or an error.
    """
    async with exchange_locks[exchange_name]: # Acquire lock for this exchange
        # Enforce minimum delay between requests to this exchange
        elapsed = time.monotonic() - last_request_time[exchange_name]
        delay_needed = CONFIG["EXCHANGES"][exchange_name]["rest_rate_limit_delay_s"] - elapsed
        if delay_needed > 0:
            logger.debug(f"Rate limiting {exchange_name}: waiting {delay_needed:.2f}s")
            await asyncio.sleep(delay_needed)

        retries = 0
        while retries < CONFIG["RETRY_ATTEMPTS"]:
            start_time = time.monotonic()
            try:
                async with session.get(url, timeout=10) as response:
                    last_request_time[exchange_name] = time.monotonic() # Update last request time

                    if response.status == 429: # Too Many Requests
                        retry_after = response.headers.get(CONFIG["EXCHANGES"][exchange_name]["retry_after_header_key"])
                        if retry_after:
                            wait_time = int(retry_after)
                            logger.warning(f"Rate limited by {exchange_name}. Retrying after {wait_time}s.")
                            await asyncio.sleep(wait_time)
                            continue # Skip incrementing retries, try again immediately after wait
                        else:
                            logger.warning(f"Rate limited by {exchange_name}, but no 'Retry-After' header. Applying default retry delay.")

                    response.raise_for_status() # Raise an exception for other bad status codes (4xx or 5xx)
                    raw_data = await response.json()
                    latency = (time.monotonic() - start_time) * 1000 # Latency in ms
                    logger.info(f"Fetched {exchange_name} REST data (Latency: {latency:.2f}ms)")

                    # Parse data immediately after fetching and before putting into queue
                    parsed_item = parse_exchange_data(exchange_name, raw_data, "REST")
                    if parsed_item:
                        return parsed_item # Return the fully parsed item
                    else:
                        # If parsing fails, return an error state
                        return {"exchange": exchange_name, "error": "Parsing failed", "source": "REST", "raw_data": raw_data}
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                retries += 1
                current_delay = CONFIG["RETRY_BASE_DELAY_SECONDS"] * (2 ** (retries - 1))
                logger.warning(f"Error fetching from {exchange_name} ({url}): {e}. Retrying in {current_delay:.1f}s (Attempt {retries}/{CONFIG['RETRY_ATTEMPTS']})")
                await asyncio.sleep(current_delay)
            except Exception as e:
                logger.error(f"An unexpected error occurred fetching from {exchange_name}: {e}")
                break # No point in retrying for unexpected errors
        logger.error(f"Failed to fetch REST data from {exchange_name} after {CONFIG['RETRY_ATTEMPTS']} attempts.")
        return {"exchange": exchange_name, "error": f"Failed after {CONFIG['RETRY_ATTEMPTS']} retries", "source": "REST"}

# --- Data Streaming Operations (WebSockets) ---

async def websocket_client(exchange_name: str, uri: str, data_queue: asyncio.Queue, stop_event: asyncio.Event):
    """
    Connects to a WebSocket, receives messages, and puts parsed data into the queue.
    Handles reconnection logic.
    """
    while not stop_event.is_set():
        try:
            logger.info(f"Connecting to {exchange_name} WebSocket: {uri}")
            async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                logger.info(f"Connected to {exchange_name} WebSocket.")
                while not stop_event.is_set():
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=1.0) # Timeout to check stop_event
                        raw_ws_data = json.loads(message) # Load JSON here
                        parsed_data = parse_exchange_data(exchange_name, raw_ws_data, "WS") # Use the unified parser
                        if parsed_data:
                            await data_queue.put(parsed_data)
                    except asyncio.TimeoutError:
                        # No message received in timeout, check stop_event and continue loop
                        pass
                    except websockets.exceptions.ConnectionClosedOK:
                        logger.info(f"{exchange_name} WebSocket connection closed gracefully.")
                        break # Exit inner loop to attempt reconnect
                    except websockets.exceptions.ConnectionClosedError as e:
                        logger.warning(f"{exchange_name} WebSocket connection closed with error: {e}. Attempting reconnect.")
                        break # Exit inner loop to attempt reconnect
                    except Exception as e:
                        logger.error(f"Error processing WebSocket message from {exchange_name}: {e}")
                        # Continue trying to receive messages
        except (websockets.exceptions.WebSocketException, OSError) as e:
            logger.error(f"WebSocket connection error for {exchange_name}: {e}. Retrying in {CONFIG['WS_RECONNECT_DELAY_SECONDS']}s...")
            await asyncio.sleep(CONFIG["WS_RECONNECT_DELAY_SECONDS"])
        except asyncio.CancelledError:
            logger.info(f"WebSocket client for {exchange_name} cancelled.")
            break # Exit loop if cancelled
        except Exception as e:
            logger.critical(f"Unhandled critical error in {exchange_name} WebSocket client: {e}")
            await asyncio.sleep(CONFIG["WS_RECONNECT_DELAY_SECONDS"]) # Prevent tight loop on critical error

# --- Data Processing and Storage (Consumer) ---

async def data_processor_consumer(data_queue: asyncio.Queue, stop_event: asyncio.Event):
    """
    Processes already parsed and standardized data from the queue and stores it.
    """
    while not stop_event.is_set() or not data_queue.empty():
        try:
            # Use a timeout to allow the loop to periodically check the stop_event
            fetched_item = await asyncio.wait_for(data_queue.get(), timeout=1.0)
            # Now, fetched_item should already contain 'symbol', 'price', 'source'
            exchange = fetched_item.get("exchange")
            symbol = fetched_item.get("symbol")
            price = fetched_item.get("price")
            timestamp = fetched_item.get("timestamp")
            source = fetched_item.get("source")
            error = fetched_item.get("error") # Check for errors from fetcher/parser

            if error:
                logger.warning(f"Skipping storage for {exchange} due to error: {error}. Raw data: {fetched_item.get('raw_data', 'N/A')}")
            elif all([exchange, symbol, price is not None, timestamp, source]):
                await insert_data(exchange, symbol, price, timestamp, source)
            else:
                logger.warning(f"Incomplete data received for storage (after parsing): {fetched_item}")

            data_queue.task_done()
        except asyncio.TimeoutError:
            # Queue was empty for the timeout duration, continue loop to check stop_event
            pass
        except Exception as e:
            logger.error(f"Error processing data from queue: {e}")
            # Do not call data_queue.task_done() if an exception occurred before processing,
            # as the item might still be considered "in progress" if not retrieved correctly.

# --- Data Display Task ---

async def display_latest_data(stop_event: asyncio.Event):
    """
    Periodically reads and displays the latest data from the database.
    """
    while not stop_event.is_set():
        try:
            conn = sqlite3.connect(CONFIG["DATABASE_NAME"])
            # Fetch latest 10 records, ordered by timestamp
            df = pd.read_sql_query(
                "SELECT exchange, symbol, price, timestamp, source FROM market_data ORDER BY timestamp DESC LIMIT 10",
                conn
            )
            conn.close()

            if not df.empty:
                logger.info("\n--- Latest Market Data (from SQLite) ---")
                # Format timestamp for better readability
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%H:%M:%S')
                print(df.to_string(index=False)) # print as string to avoid pandas repr issues in some logs
            else:
                logger.info("\n--- No data yet in database ---")

        except Exception as e:
            logger.error(f"Error displaying data: {e}")

        await asyncio.sleep(CONFIG["DISPLAY_DATA_INTERVAL_SECONDS"])


# --- Main Aggregator Orchestration ---

async def aggregate_data_full_featured():
    """
    The main asynchronous orchestrator for the full-featured aggregator.
    Manages REST polling, WebSocket clients, and data processing.
    """
    await init_db()

    stop_event = asyncio.Event()

    # Attempt to set up signal handlers for graceful shutdown (Ctrl+C)
    try:
        import signal
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, stop_event.set)
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)
        logger.info("Signal handlers set for graceful shutdown.")
    except (RuntimeError, AttributeError, ImportError):
        logger.warning("Could not set signal handlers (e.g., in Jupyter or non-main thread/OS). Use Kernel->Interrupt or manual stop.")

    async with aiohttp.ClientSession() as session:
        data_queue = asyncio.Queue()
        tasks = []

        # Start REST API fetchers
        async def rest_fetcher_loop():
            while not stop_event.is_set():
                logger.info(f"\n--- Fetching REST data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
                fetch_tasks = []
                for exchange_name, exchange_config in CONFIG["EXCHANGES"].items():
                    if exchange_config.get("rest_url"):
                        fetch_tasks.append(
                            asyncio.create_task(
                                fetch_data_with_retry(session, exchange_name, exchange_config["rest_url"])
                            )
                        )
                if fetch_tasks:
                    fetched_results = await asyncio.gather(*fetch_tasks)
                    for result in fetched_results:
                        # Only put valid parsed results into the queue
                        if not result.get("error"): # Check if the result indicates an error
                            await data_queue.put(result)
                        else:
                            logger.error(f"Skipping queueing REST result due to error: {result.get('error')}")

                logger.info(f"Waiting for {CONFIG['REST_FETCH_INTERVAL_SECONDS']}s before next REST fetch cycle...")
                await asyncio.sleep(CONFIG["REST_FETCH_INTERVAL_SECONDS"])

        tasks.append(asyncio.create_task(rest_fetcher_loop()))

        # Start WebSocket clients
        for exchange_name, exchange_config in CONFIG["EXCHANGES"].items():
            if exchange_config.get("ws_url"):
                tasks.append(
                    asyncio.create_task(
                        websocket_client(exchange_name, exchange_config["ws_url"], data_queue, stop_event)
                    )
                )

        # Start the data processor consumer
        tasks.append(asyncio.create_task(data_processor_consumer(data_queue, stop_event)))

        # Start the data display task
        tasks.append(asyncio.create_task(display_latest_data(stop_event)))


        logger.info("Aggregator started. Monitoring REST and WebSocket data streams.")

        try:
            # Wait for any task to complete (or for stop_event to be set)
            # This will effectively keep the program running until a shutdown signal
            await stop_event.wait()
            logger.info("Stop event received. Initiating graceful shutdown...")

            # Cancel all running tasks
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True) # Wait for tasks to finish cancelling

            # Ensure all remaining items in the queue are processed
            await data_queue.join()
            logger.info("All pending data processed from queue.")

            logger.info("Aggregator gracefully shut down.")

        except asyncio.CancelledError:
            logger.info("Aggregator orchestration cancelled.")
        except Exception as e:
            logger.critical(f"An unhandled critical error occurred during aggregation: {e}")
            import traceback
            traceback.print_exc(file=sys.stderr)

# --- Entry Point ---

if __name__ == "__main__":
    # Optional: Install uvloop for performance: pip install uvloop
    # try:
    #     import uvloop
    #     uvloop.install()
    #     logger.info("Using uvloop for event loop.")
    # except ImportError:
    #     logger.info("uvloop not found, using default asyncio event loop.")

    try:
        # Check if an event loop is already running (e.g., in Jupyter/IPython)
        loop = asyncio.get_event_loop()
        if loop.is_running():
            logger.info("Detected running event loop. Scheduling aggregator task.")
            # If a loop is running, schedule the coroutine as a task
            task = loop.create_task(aggregate_data_full_featured())
            # In Jupyter, the task will run in the background.
            # Use Kernel -> Interrupt to stop it.
        else:
            logger.info("No running event loop detected. Starting new loop.")
            # If no loop is running, run the coroutine directly
            asyncio.run(aggregate_data_full_featured())
    except KeyboardInterrupt:
        logger.info("\nAggregator stopped by user (KeyboardInterrupt).")
    except Exception as e:
        logger.critical(f"An unhandled error occurred at startup: {e}")
        import traceback
        traceback.print_exc(file=sys.stderr)
