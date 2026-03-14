import httpx
import calendar

from fastapi import HTTPException
from typing import List, Tuple
from datetime import datetime
from api.config import settings
from api.models import AlphaVantageMonthlyResponse

async def fetch_monthly_data(symbol: str, year: int = -1) -> List[Tuple]:
    """
    Fetches monthly time series data for a given symbol from Alpha Vantage.
    """
    params = {
        "function": "TIME_SERIES_MONTHLY",
        "symbol": symbol,
        "apikey": settings.ALPHAVANTAGE_API_KEY,
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(settings.ALPHAVANTAGE_API_URL, params=params)
            response.raise_for_status()
            data = AlphaVantageMonthlyResponse(**response.json())
            if not data.monthly_time_series:
                raise HTTPException(status_code=404, detail=f"No monthly time series data found for {symbol}")
            if year != -1:
                data.monthly_time_series = {k: v for k, v in data.monthly_time_series.items() if k.year == year}
            data = set_current_month_end(data)
            return [(symbol, k, v.high, v.low, v.volume) for k, v in data.monthly_time_series.items()]


        except httpx.HTTPStatusError as e:
            raise HTTPException(500, detail=f"HTTP error occurred: {e}")
        except Exception as e:
            raise HTTPException(500, detail=f"An unexpected error occurred: {e}")


def set_current_month_end(data: AlphaVantageMonthlyResponse):
    """
    Updates the monthly time series data to align with the end of the current month, if the latest
    entry falls within the current month.

    :param data: An instance of AlphaVantageMonthlyResponse containing the monthly time
        series data. The data object is expected to provide a dictionary-like structure
        with date keys and associated data values in the `monthly_time_series` attribute.
    :return: The updated AlphaVantageMonthlyResponse instance with the adjusted keys in its
        `monthly_time_series` if applicable.
    """
    last_date = next(iter(data.monthly_time_series))
    now = datetime.now()
    current_month_start = now.replace(day=1)
    _, current_month_end_day = calendar.monthrange(datetime.now().year, datetime.now().month)
    current_month_end = now.replace(day=current_month_end_day,hour=0, minute=0, second=0, microsecond=0)
    if current_month_start <= last_date < current_month_end:
        data.monthly_time_series[current_month_end] = data.monthly_time_series.pop(last_date)
    return data
