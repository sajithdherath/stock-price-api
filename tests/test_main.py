import os

os.environ['ALPHAVANTAGE_API_KEY'] = 'demo'
os.environ['DB_FILE'] = f"/tmp/db.sqlite"

from datetime import datetime

from fastapi.testclient import TestClient
from api.main import app
from api.config import settings


DEFAULT_SYMBOL = "IBM"


def test_health():
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


def test_complete_database_sync():
    """Test complete database sync when symbol is not in DB."""
    symbol = DEFAULT_SYMBOL
    year = 2023
    
    with TestClient(app) as client:
        response = client.get(f"/symbols/{symbol}/annual/{year}")
        
    assert response.status_code == 200
    data = response.json()
    assert "high" in data
    assert "low" in data
    assert "volume" in data
    assert data["high"] > 0
    assert data["low"] > 0
    assert data["volume"] > 0


def test_partial_database_sync_current_year():
    """Test partial database sync for the current year."""
    symbol = DEFAULT_SYMBOL
    current_year = datetime.now().year
    
    with TestClient(app) as client:
        response1 = client.get(f"/symbols/{symbol}/annual/{current_year}")
        assert response1.status_code == 200
        
        settings.CACHE_UPDATE_INTERVAL = -1
        response2 = client.get(f"/symbols/{symbol}/annual/{current_year}")
        assert response2.status_code == 200
        
        data = response2.json()
        assert "high" in data
        assert "low" in data
        assert "volume" in data


def test_invalid_year_future():
    """Test invalid year (future)."""
    symbol = DEFAULT_SYMBOL
    future_year = datetime.now().year + 1
    
    with TestClient(app) as client:
        response = client.get(f"/symbols/{symbol}/annual/{future_year}")
    
    assert response.status_code == 400
    assert "Year cannot be in the future" in response.json()["detail"]


def test_invalid_symbol():
    """Test invalid symbol (non-alphabetic)."""
    symbol = "IBM123"
    year = 2023
    
    with TestClient(app) as client:
        response = client.get(f"/symbols/{symbol}/annual/{year}")
    
    # FastAPI path parameter validation returns 422 for regex mismatch
    assert response.status_code == 422


def test_year_not_available():
    """Test when requested year is earlier than available data in DB."""
    symbol = DEFAULT_SYMBOL
    year = 1900
    
    with TestClient(app) as client:
        response1 = client.get(f"/symbols/{symbol}/annual/2023")
        assert response1.status_code == 200
        
        response2 = client.get(f"/symbols/{symbol}/annual/{year}")
    
    assert response2.status_code == 422
