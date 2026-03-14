from datetime import datetime
from typing import Dict

from pydantic import BaseModel, Field


class AnnualAvgRes(BaseModel):
    high: float
    low: float
    volume: int


class MonthlyDataPoint(BaseModel):
    high: float = Field(alias="2. high")
    low: float = Field(alias="3. low")
    volume: int = Field(alias="5. volume")


class AlphaVantageMonthlyResponse(BaseModel):
    monthly_time_series: Dict[datetime, MonthlyDataPoint] = Field(default_factory=dict, alias="Monthly Time Series")
