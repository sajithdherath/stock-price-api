import asyncio
import logging

from datetime import datetime, UTC
from dateutil.relativedelta import relativedelta
from sqlite3 import Connection
from typing import Annotated
from fastapi import FastAPI, Path, Depends, HTTPException
from contextlib import asynccontextmanager

from starlette import status

from api.config import settings
from api.db import (init_db, get_db, insert_monthly_data, get_annual_aggregation, fetch_min_max_dates,
                    try_acquire_sync_lock, release_sync_lock)
from api.alpha_vantage import fetch_monthly_data
from api.models import AnnualAvgRes

logger = logging.getLogger('uvicorn.error')


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/symbols/{symbol}/annual/{year}", response_model=AnnualAvgRes)
async def get_annual(symbol: Annotated[str, Path(description="The stock Symbol", pattern=r"^[a-zA-Z\.]+$")],
                     year: Annotated[int, Path(description="The year", gt=1900)],
                     db: Annotated[Connection, Depends(get_db)]):
    # Future year validation
    if year > datetime.now().year:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Year cannot be in the future")
    await check_for_sync(db, symbol, year)
    res = await get_annual_aggregation(db, symbol, year)
    if res[0] is None:
        data = await fetch_monthly_data(symbol, year)
        if not data:
            raise HTTPException(
                status_code=status.HTTP_200_OK,
                detail=f"No data found for {symbol} in {year}"
            )
        await insert_monthly_data(db, data)
        res = await get_annual_aggregation(db, symbol, year)
    return AnnualAvgRes(high=res[0], low=res[1], volume=res[2])


async def check_for_sync(db: Connection, symbol: str, year: int):
    """
    Checks for synchronization status of a symbol's data within the database for a given year and
    performs synchronization if necessary. The function determines whether the data needs
    a full sync, a partial sync, or cache renewal based on the current state in the database,
    the requested year, and the current year's data completeness.

    :param db: The database connection used to query and update data.
    :type db: Connection
    :param symbol: The symbol identifier for which synchronization is to be checked.
    :type symbol: str
    :param year: The specific year of the data to validate and synchronize.
    :type year: int
    :return: None. Performs the synchronization process or validates the data.
    :rtype: None
    :raises HTTPException: If the requested year's data is unavailable and cannot be synchronized.
    """
    for _ in range(settings.SYNC_WAIT_RETRIES):
        # Check if the symbol exists in the database
        data_in_db = await fetch_min_max_dates(db, symbol)
        needs_sync = False
        sync_year = year
        # Complete sync since data is not available in the database
        if data_in_db is None:
            needs_sync = True
            sync_year = -1
        # Update the cache only for the current year
        elif year == datetime.now().year == data_in_db['max_date'].year:
            cache_t_diff = relativedelta(datetime.now(UTC), data_in_db['last_updated_at'])
            # Update the cache with an interval
            if cache_t_diff.hours > settings.CACHE_UPDATE_INTERVAL:
                needs_sync = True
        # Partial sync for a specific year
        elif data_in_db['max_date'].year < year:
            needs_sync = True
        # Validate minimum available year and delisted symbols
        elif data_in_db['min_date'].year > year:
            raise HTTPException(
                status_code=status.HTTP_200_OK,
                detail=f"Year {year} is not available for {symbol}"
            )
        if not needs_sync:
            break
        if await try_acquire_sync_lock(db, symbol):
            try:
                await sync(db, symbol, sync_year)
                break
            finally:
                await release_sync_lock(db, symbol)
        await asyncio.sleep(settings.SYNC_WAIT_INTERVAL)


async def sync(db: Connection, symbol: str, year: int):
    """
    Synchronizes monthly data for a given symbol and year by fetching data from an external source
    and inserting it into the database.

    :param db: The database connection instance where the data will be inserted.
    :type db: Connection
    :param symbol: The unique identifier or symbol for which the data will be fetched.
    :type symbol: str
    :param year: The target year for fetching the data.
    :type year: int
    :return: None
    """
    data = await fetch_monthly_data(symbol, year)
    if not data:
        raise HTTPException(
            status_code=status.HTTP_200_OK,
            detail=f"No data found for {symbol} in {year}"
        )
    await insert_monthly_data(db, data)
