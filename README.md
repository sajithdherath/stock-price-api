# Stock Price API

## How to run the project

This project uses `uv` as the package manager and runner, and supports Docker for containerized deployment. 

### Running with Docker Compose (Recommended)

1. Make sure you have Docker and Docker Compose installed.
2. In the project root, run:
```bash
docker compose up --build
```
This will build the application and start it on `http://127.0.0.1`
### Running Locally with `uv`

1. Ensure `uv` is installed on your system.
2. Run `uv sync` to install dependencies
3. Create the .env file from the .env.example file
4. Start the FastAPI development server:

```bash
uv run fastapi dev api/main.py
```

This will automatically resolve dependencies, create an isolated environment if necessary, and start the API server with auto reload enabled (accessible at `http://127.0.0.1:8000`).

## System Design and Optimizations

The application is built to provide high performance and minimize reliance on external APIs (AlphaVantage), ensuring fast response times while respecting rate limits.

### 1. Minimizing AlphaVantage API Calls
The system caches historical monthly stock data in a local SQLite database (`stock_prices` table). When a request is made for a specific symbol and year, the application first checks the database:
- **Missing Data:** If no data exists for a symbol, all available historical data is fetched at once and stored.
- **Partial Missing Data:** If data exists but the requested year is missing, the system specifically fetches data only for the needed timeframe.
- **Cache Renewal:** For the current year, the application calculates whether the data is sufficiently fresh. If the stored data is older than `CACHE_UPDATE_INTERVAL` hours, a partial sync occurs to ensure the current year's data is aligned with the latest figures without requiring a full sync.

### 2. Handling Race Conditions and Concurrency
To avoid sending duplicate concurrent requests to AlphaVantage for the exact same symbol (e.g., when a traffic spike occurs), the system implements a robust locking mechanism:
- **Synchronization Lock:** A separate `sync_status` table keeps track of active sync operations per symbol with an `'IDLE'` or `'SYNCING'` status.
- **Concurrency Control:** When a sync is needed, the request attempts to acquire a lock using an atomic SQLite `UPDATE` statement. If successful, it proceeds to call the API.
- **Wait and Retry Loop:** If another request is already fetching data for the same symbol (the lock is `SYNCING`), the current request drops into an asynchronous `wait and retry` loop (`asyncio.sleep(settings.SYNC_WAIT_INTERVAL)`). It waits for the other request to finish populating the database and releases the lock, then serves the result directly from the cache without ever making an extra API call.
- **Stale Lock Prevention:** A `last_heartbeat` timestamp prevents deadlocks. If a process crashes while holding a lock, new requests will forcefully take over the lock if the heartbeat is older than 5 minutes.
