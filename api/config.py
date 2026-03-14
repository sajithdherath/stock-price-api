from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DB_FILE: str = "db.sqlite"

    ALPHAVANTAGE_API_KEY: str
    ALPHAVANTAGE_API_URL: str = "https://www.alphavantage.co/query"

    CACHE_UPDATE_INTERVAL: int = 1

    SYNC_WAIT_RETRIES: int = 10
    SYNC_WAIT_INTERVAL: float = 0.3

settings = Settings()