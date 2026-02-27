"""
Basic API tests (health, CRUD) using test DB and fake Redis.

Verifies the FastAPI app starts correctly with the test environment.
"""

import pytest

from models import BOResult


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "healthy"


def test_create_bo_market_order(client, test_bot):
    """MARKET order should create BO with price from REST fallback."""
    bot_name, api_key = test_bot

    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
        },
        headers={"x-api-key": api_key},
    )
    # May fail with 502 if Polymarket REST is unavailable — that's expected
    # in CI without network. In local tests with internet, should be 201.
    assert resp.status_code in (201, 502)

    if resp.status_code == 201:
        data = resp.json()
        assert data["bot_name"] == bot_name
        assert data["symbol"] == "BTC"
        assert data["timeframe"] == "M5"
        assert data["forecast"] == "GREEN"
        assert data["amount"] == 10.0
        assert data["result"] == BOResult.PENDING.value


def test_create_bo_invalid_api_key(client):
    """Invalid API key should return 401."""
    resp = client.post(
        "/poly-arena/binary-options/",
        json={
            "symbol": "BTC",
            "timeframe": "M5",
            "forecast": "GREEN",
            "amount": 10.0,
        },
        headers={"x-api-key": "bad-key"},
    )
    assert resp.status_code == 401


def test_list_bo_empty(client):
    """Should return empty list when no BOs exist."""
    resp = client.get("/poly-arena/binary-options/")
    assert resp.status_code == 200
    assert resp.json() == []


def test_stats_summary_empty(client):
    """Stats should work with empty DB."""
    resp = client.get("/poly-arena/binary-options/stats/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 0
    assert data["wins"] == 0
    assert data["losses"] == 0
