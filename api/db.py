import logging
import sqlite3
from datetime import datetime, timezone

from fastapi import HTTPException
from textwrap import dedent
from typing import List, Tuple, Optional, Dict
from aiosqlite import connect, Connection
from starlette import status

from api.config import settings

logger = logging.getLogger('uvicorn.error')


async def get_db():
    """
    Get the database connection
    :return:
    """
    db = await connect(settings.DB_FILE,
                       detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    await db.execute("PRAGMA synchronous = NORMAL;")
    try:
        yield db
    finally:
        await db.close()


async def init_db():
    # Initialize the database if it doesn't exist.
    async with connect(settings.DB_FILE) as db:
        await db.execute("PRAGMA journal_mode = WAL;")
        await db.execute(dedent("""
                                CREATE TABLE IF NOT EXISTS stock_prices
                                (
                                    symbol
                                    TEXT
                                    NOT
                                    NULL,
                                    date
                                    TEXT
                                    NOT
                                    NULL,
                                    high
                                    REAL,
                                    low
                                    REAL,
                                    volume
                                    INTEGER,
                                    last_updated_at
                                    TEXT
                                    DEFAULT
                                    CURRENT_TIMESTAMP,
                                    PRIMARY
                                    KEY
                                (
                                    symbol,
                                    date
                                )
                                    ) WITHOUT ROWID;
                                """))
        # Create sync_status table
        await db.execute(dedent("""
                                CREATE TABLE IF NOT EXISTS sync_status
                                (
                                    symbol
                                    TEXT
                                    PRIMARY
                                    KEY,
                                    status
                                    TEXT
                                    CHECK (
                                    status
                                    IN
                                (
                                    'IDLE',
                                    'SYNCING'
                                )) DEFAULT 'IDLE',
                                    last_heartbeat TEXT DEFAULT CURRENT_TIMESTAMP
                                    ) WITHOUT ROWID;
                                """))
        await db.execute("UPDATE sync_status SET status = 'IDLE'")
        await db.commit()


async def fetch_min_max_dates(db: Connection, symbol: str) -> Optional[Dict[str, datetime]]:
    """
    Fetches the minimum and maximum dates for a given stock symbol from the database,
    along with the last updated timestamp for the maximum date.

    :param db: The database connection object used to execute the query.
    :type db: Connection
    :param symbol: The stock symbol for which date information needs to be fetched.
    :type symbol: str
    :return: A dictionary containing `min_date`, `max_date`, and `last_updated_at`,
             or None if no data is found for the symbol.
    :rtype: dict | None
    :raises HTTPException: If there is an error executing the query or processing data.
    """
    query = dedent(""" \
                   SELECT MIN(date),
                          MAX(date),
                          MAX(last_updated_at) FILTER (WHERE date = max_date_val)
                   FROM (SELECT date, last_updated_at, MAX (date) OVER () as max_date_val
                         FROM stock_prices
                         WHERE symbol = ?) AS subquery""")
    try:
        async with await db.execute(query, (symbol,)) as cursor:
            row = await cursor.fetchone()
            if row[0] is None:
                return None
            min_date = datetime.fromisoformat(row[0])
            max_date = datetime.fromisoformat(row[1])
            last_updated_at = datetime.fromisoformat(row[2]).replace(tzinfo=timezone.utc)
            return {
                "min_date": min_date,
                "max_date": max_date,
                "last_updated_at": last_updated_at
            }
    except Exception as e:
        logger.error(f"Error fetching min max dates: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred")


async def insert_monthly_data(db: Connection, data: List[Tuple]):
    """
    Inserts or updates monthly stock price data into the database. If on conflict rows that are matched with the symbol
    and the date will be updated high, low, volume values set it's last_update_at value to current timestamp

    :param db: The database connection to be used for executing the query.
    :type db: Connection
    :param data: A list of tuples, where each tuple contains the stock data for insertion or update.
                 Each tuple must follow the structure `(symbol, date, high, low, volume)`.
    :type data: List[Tuple]
    :return: None
    :rtype: None
    :raises HTTPException: Raised with a 500 status code if any unexpected error occurs during the
                           insertion or update process. The exception contains details about the error.
    """
    query = dedent(""" \
                   INSERT INTO stock_prices (symbol, date, high, low, volume)
                   values (?, ?, ?, ?, ?) ON CONFLICT (symbol, date) DO
                   UPDATE SET high = excluded.high,
                       low = excluded.low,
                       volume = excluded.volume,
                       last_updated_at = CURRENT_TIMESTAMP""")
    try:
        await db.executemany(query, data)
        await db.commit()
    except Exception as e:
        logger.error(f"Error inserting or updating data: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred")


async def get_annual_aggregation(db: Connection, symbol: str, year: int) -> sqlite3.Row:
    """
    Fetches annual aggregation of stock data for a given symbol and year from the database.

    :param db: A database connection object used to execute the query.
    :type db: Connection
    :param symbol: The stock symbol for which the aggregation is performed.
    :type symbol: str
    :param year: The year for which the aggregation is to be fetched.
    :type year: int
    :return: A single row containing the aggregated data: maximum high, minimum low,
             and total volume.
    :rtype: sqlite3.Row
    :raises HTTPException: Raised with a 500 status code if there is an error during
                           query execution.
    """
    query = dedent(""" \
                   SELECT MAX(high),
                          MIN(low),
                          SUM(volume)
                   FROM stock_prices
                   WHERE symbol = ?
                     AND strftime('%Y', date) = ?
                   """)
    try:
        async with await db.execute(query, (symbol, str(year))) as cursor:
            return await cursor.fetchone()
    except Exception as e:
        logger.error(f"Error fetching annual aggregation: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred")


async def try_acquire_sync_lock(db: Connection, symbol: str) -> bool:
    """
    Attempts to acquire a synchronization lock for the given symbol in the
    database. If the symbol is not present in the sync_status table, it will
    be inserted with a status of 'IDLE'. The method then tries to update the
    status to 'SYNCING' if the current status is either 'IDLE' or its last
    heartbeat was recorded more than 5 minutes ago. Upon successful acquisition,
    it returns True. Otherwise, it returns False.

    :param db: Database connection used to execute SQL commands.
    :type db: Connection
    :param symbol: Symbol for which the synchronization status needs to be
                   updated or checked.
    :type symbol: str
    :return: Whether the lock was successfully acquired.
    :rtype: bool
    """
    await db.execute(
        "INSERT OR IGNORE INTO sync_status (symbol, status) VALUES (?, 'IDLE')",
        (symbol,)
    )

    query = dedent("""
                   UPDATE sync_status
                   SET status         = 'SYNCING',
                       last_heartbeat = CURRENT_TIMESTAMP
                   WHERE symbol = ?
                     AND (
                       status = 'IDLE' OR
                       datetime(last_heartbeat, '+5 minutes') < datetime('now')
                       )
                   """)

    try:
        async with await db.execute(query, (symbol,)) as cursor:
            await db.commit()
            # If rowcount == 1, the state is successfully changed from IDLE to SYNCING
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"Lock acquisition failed for {symbol}: {e}")
        return False


async def release_sync_lock(db: Connection, symbol: str):
    """
    Releases the lock by setting the status back to 'IDLE'.
    """
    query = "UPDATE sync_status SET status = 'IDLE', last_heartbeat = CURRENT_TIMESTAMP WHERE symbol = ?"
    try:
        await db.execute(query, (symbol,))
        await db.commit()
    except Exception as e:
        logger.error(f"Failed to release lock for {symbol}: {e}")
