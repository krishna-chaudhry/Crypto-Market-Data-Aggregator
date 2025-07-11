# Crypto-Market-Data-Aggregator


This project implements a robust and real-time cryptocurrency market data aggregator using Python's `asyncio` framework. It's designed to concurrently fetch market data from multiple exchanges, process it, and store it locally, demonstrating advanced asynchronous programming patterns, error handling, and API interaction best practices.

## Features

- **Asynchronous Data Fetching:** Utilizes `asyncio` and `aiohttp` for efficient, non-blocking HTTP requests to exchange REST APIs.
    
- **Real-time Data Streaming:** Integrates `websockets` to connect to exchange WebSocket APIs (e.g., Binance) for instant trade data updates.
    
- **Concurrent Operations:** Fetches data from multiple sources (REST and WebSockets) simultaneously using `asyncio.gather`.
    
- **Producer-Consumer Pattern:** Employs `asyncio.Queue` to decouple data fetching/streaming from data processing and storage, enhancing resilience and scalability.
    
- **Robust Error Handling:**
    
    - Implements retry logic with exponential backoff for transient network errors (timeouts, connection issues).
        
    - Handles WebSocket connection errors and automatically attempts reconnection.
        
- **Rate Limit Management:**
    
    - Enforces per-exchange minimum delays between REST requests to respect API rate limits.
        
    - Parses `Retry-After` HTTP headers for explicit rate limit responses, pausing requests for the required duration.
        
- **Structured Logging:** Uses Python's built-in `logging` module for informative and configurable output (INFO, WARNING, ERROR, DEBUG levels).
    
- **Local Data Persistence:** Stores aggregated market data in a local SQLite database (`.db` file).
    
- **Periodic Data Display:** Periodically queries the SQLite database and displays the latest market data using `pandas`.
    
- **Graceful Shutdown:** Implements signal handlers to ensure the application shuts down cleanly, closing connections and processing pending data.
    

## Installation

1. Clone the repository (or save the code):
    
    Save the provided Python code into a file (e.g., advanced_aggregator.py).
    
2. **Create a Virtual Environment (Recommended):**
    
    ```
    python -m venv venv
    # On Windows:
    .\venv\Scripts\activate
    # On macOS/Linux:
    source venv/bin/activate
    ```
    
3. **Install Dependencies:**
    
    ```
    pip install aiohttp pandas websockets
    ```
    
    (Optional, for improved performance on some systems):
    
    ```
    pip install uvloop
    ```
    

## Usage

1. Run the Aggregator:
    
    Navigate to the directory where you saved advanced_aggregator.py in your terminal or command prompt and run:
    
    ```
    python advanced_aggregator.py
    ```
    
2. Observe Output:
    
    The console will display logs indicating:
    
    - Database initialization.
        
    - REST data fetching cycles.
        
    - WebSocket connection status and streamed data insertions.
        
    - Periodic tables showing the latest data stored in SQLite.
        
    - Any errors or warnings (e.g., connection issues, rate limits).
        
3. **Stop the Aggregator:**
    
    - **In Terminal/Command Prompt:** Press `Ctrl+C`. The application is designed for graceful shutdown.
        
    - **In Jupyter Notebook/IPython:** Go to the Jupyter menu bar, click on **Kernel**, then select **Interrupt Kernel`. If needed, select` Restart Kernel` for a clean slate.
        

## Configuration

All key parameters are defined in the `CONFIG` dictionary at the top of the `advanced_aggregator.py` file. You can modify these to suit your needs:

- `EXCHANGES`: Define REST and WebSocket URLs, rate limit delays, and `Retry-After` header keys for each exchange.
    
- `DATABASE_NAME`: Name of the SQLite database file.
    
- `REST_FETCH_INTERVAL_SECONDS`: How often to poll REST APIs.
    
- `RETRY_ATTEMPTS`: Number of retries for failed requests.
    
- `RETRY_BASE_DELAY_SECONDS`: Initial delay for retries (doubles on each attempt).
    
- `WS_RECONNECT_DELAY_SECONDS`: Delay before attempting WebSocket reconnection.
    
- `LOG_LEVEL`: Adjust logging verbosity (e.g., `logging.DEBUG` for more detailed messages).
    
- `DISPLAY_DATA_INTERVAL_SECONDS`: How often to print the latest data summary.
    

## Design Choices

- **`asyncio` Core:** The entire application is built around Python's `asyncio` event loop. This allows for efficient handling of many concurrent I/O operations (network requests, database writes) without blocking the main program flow, making it ideal for real-time data aggregation.
    
- **`aiohttp` for HTTP:** Chosen for its asynchronous capabilities, efficient connection pooling, and robust error handling for HTTP requests.
    
- **`websockets` for Real-time Streams:** Provides a native asynchronous client for WebSocket connections, enabling low-latency data reception.
    
- **Producer-Consumer Pattern (`asyncio.Queue`):** This architectural pattern separates the concerns of data acquisition (producers: REST fetchers, WebSocket clients) from data processing and storage (consumer). This decoupling:
    
    - **Improves Resilience:** If the database or parsing logic temporarily slows down, fetchers can continue to acquire data and queue it without blocking.
        
    - **Enhances Scalability:** Allows for easy addition of more producers or consumers, or even multiple consumer instances, if processing becomes a bottleneck.
        
- **Centralized Parsing (`parse_exchange_data`):** A single function is responsible for standardizing data from both REST and WebSocket sources. This keeps the data format consistent before storage, simplifying downstream processing.
    
- **Per-Exchange Rate Limiting:** Using `asyncio.Lock` and tracking `last_request_time` ensures that requests to individual exchanges respect their specific rate limits, preventing IP bans and ensuring polite API usage.
    
- **Structured Logging:** Essential for monitoring the application in production, quickly identifying issues, and understanding its operational state.
    
- **SQLite for Simplicity:** For a free and self-contained project, SQLite provides a simple yet effective way to persist data without requiring an external database server setup.
    

## Future Enhancements

- **Expand Exchange Coverage:** Integrate more cryptocurrency exchanges (e.g., Kraken, Bybit, OKX, Huobi) by adding their configurations and specific parsing logic.
    
- **Multiple Trading Pairs:** Extend the aggregator to fetch and store data for multiple cryptocurrency pairs (e.g., ETH/USD, SOL/BTC) from each exchange.
    
- **Advanced Rate Limiting:** Implement more sophisticated rate limiting algorithms (e.g., token bucket) for finer-grained control and proactive prevention of hitting API limits.
    
- **Asynchronous Database Driver:** For higher throughput or if migrating to a more robust relational database (PostgreSQL, MySQL), use an asynchronous database driver (e.g., `aiosqlite`, `asyncpg`, `aiomysql`) to ensure database operations don't block the event loop.
    
- **Alternative Data Storage:**
    
    - **Kafka Integration:** Use `aiokafka` to stream processed data to a Kafka topic, enabling real-time analytics pipelines.
        
    - **Redis Caching/Pub/Sub:** Store the latest prices in Redis for low-latency access or use Redis Pub/Sub for broadcasting real-time updates to other services.
        
- **Data Validation & Transformation:** Implement more rigorous data validation rules and advanced data transformations to ensure data quality and consistency.
    
- **Dockerization:** Create a `Dockerfile` to containerize the application, simplifying deployment and ensuring environment consistency.
    
- **Unit and Integration Tests:** Develop comprehensive test suites using `pytest` to ensure code correctness and reliability.
    
- **Monitoring & Alerting:** Integrate with monitoring systems (e.g., Prometheus, Grafana) for metrics collection and set up alerting for critical events (e.g., exchange downtime, significant price deviations).
    
- **Configuration Management:** Use a dedicated configuration library (e.g., `PyYAML` for YAML files, `python-dotenv` for environment variables) for more flexible and secure configuration.
