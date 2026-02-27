# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PolyArena is a Polymarket Binary Options paper trading platform with a shadow matching engine. It runs as three independent processes communicating via Redis:

1. **FastAPI API** (`main.py`) — Order CRUD, Redis stream consumers for bracket exits/fills/cancels, served via uvicorn
2. **WS Feed Service** (`ws_feed_service/`) — Standalone process connecting to Polymarket WebSocket, runs the MatchingEngine, publishes prices to Redis
3. **Scheduler Service** (`scheduler_service/`) — APScheduler process for settlement (Binance candle comparison), stuck order sweeping, and heartbeat

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run FastAPI dev server
./run.sh  # or: uvicorn main:app --reload --port 8010

# Run all services (API + WS Feed + Scheduler + Redis + UI + test bot)
docker-compose up -d

# Run tests (uses fakeredis, SQLite test_orders.db)
pytest tests/
pytest tests/test_api_basic.py          # single file
pytest -k "bracket"                     # filter by name

# Production
docker-compose -f docker-compose.prod.yml up -d
```

## Architecture

### IPC via Redis
- `price:{SYM}:{TF}:{DIR}` — price hashes (best_ask, best_bid, token_id) with 120s TTL
- `queue:orders:new` — list for LIMIT/bracket orders consumed by WS Feed
- `stream:bracket:exits`, `stream:order:fills`, `stream:order:cancels` — Redis streams consumed by FastAPI via XREADGROUP
- `scheduler:heartbeat` — liveness probe key (60s TTL)

### Order Lifecycle
1. `POST /poly-arena/binary-options/` — authenticates via API key, gets price from MatchingEngine (fallback: Polymarket REST), saves PENDING order
2. Bracket orders (TP/SL) are monitored in real-time by MatchingEngine in WS Feed; exits publish to `stream:bracket:exits`
3. Settlement runs every minute at `:05s` via scheduler — fetches Binance OHLC candle, compares forecast (GREEN=up, RED=down) to actual direction

### Key Design Decisions
- **Decimal precision** in `matching_engine.py` for price arithmetic
- **Thread-safe orderbook** — `ShadowOrderbook` uses `threading.Lock` per order for bracket/fill race conditions
- **Settlement uses Binance candles** as canonical price truth, not Polymarket
- MARKET orders are IOC; LIMIT orders respect `ttl` field
- Full bracket exit settles immediately; partial exit waits for scheduler

## Key Modules

| Module | Purpose |
|--------|---------|
| `services/matching_engine.py` | Shadow orderbook, SimulatedOrder, bracket TP/SL monitoring |
| `services/ws_feed.py` | Polymarket WebSocket client with reconnection |
| `services/token_registry.py` | Auto-refresh Polymarket token IDs at candle boundaries |
| `services/settlement.py` | Settle trades using Binance candle data |
| `services/polymarket.py` | Polymarket REST client (orderbook, token discovery) |
| `models.py` | SQLAlchemy models: Bot, BinaryOption, BalanceHistory |
| `schemas.py` | Pydantic request/response schemas |
| `database.py` | SQLAlchemy engine + session factory (SQLite) |

## Testing

- **conftest.py** provides fixtures: `db` (test SQLite session), `client` (FastAPI TestClient), `test_bot`, `fake_sync_redis`, `fake_async_redis`
- Tests use `fakeredis` by default; docker-compose tests use real Redis
- `pytest.ini` sets `asyncio_mode = auto`

## API Routes

All routes prefixed with `/poly-arena/`:
- `routers/binary_options.py` — order create/list/cancel, stats summary, per-bot stats
- `routers/bots.py` — bot CRUD (register, list, rename)
- `routers/dashboard.py` — health, scheduler heartbeat status

## Supported Symbols & Timeframes

- Symbols: BTC, ETH, SOL, XRP
- Timeframes: M5, M15, H1
- Forecasts: GREEN (price up), RED (price down)
