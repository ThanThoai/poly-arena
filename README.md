# PolyArena

Binary Options paper trading platform built on top of Polymarket with a shadow matching engine. Three independent services communicate via Redis to provide real-time order matching, bracket orders (TP/SL), and automated settlement.

## Architecture

```
                ┌─────────────┐
                │  Next.js UI │ :3002
                └──────┬──────┘
                       │
                ┌──────▼──────┐
                │  FastAPI API│ :8099
                │  (main.py)  │
                └──┬───┬───┬──┘
                   │   │   │        Redis Streams
      ┌────────────┘   │   └────────────┐
      ▼                ▼                ▼
 stream:bracket   stream:order     stream:order
   :exits           :fills          :cancels
      ▲                ▲                ▲
      │                │                │
┌─────┴────────────────┴────────────────┴─────┐
│              Redis  :6379                    │
│  price:{SYM}:{TF}:{DIR}  queue:orders:new   │
└─────┬──────────────────────────────┬────────┘
      │                              │
┌─────▼──────────┐          ┌───────▼─────────┐
│  WS Feed Svc   │          │  Scheduler Svc  │
│  matching engine│          │  settlement     │
│  Polymarket WS │          │  (Binance OHLC) │
└────────────────┘          └─────────────────┘
```

| Service | Description |
|---------|-------------|
| **FastAPI API** (`main.py`) | Order CRUD, Redis stream consumers for bracket exits / fills / cancels |
| **WS Feed Service** (`ws_feed_service/`) | Polymarket WebSocket client, runs the MatchingEngine, publishes prices to Redis |
| **Scheduler Service** (`scheduler_service/`) | APScheduler — settlement via Binance candles, stuck order sweeping, heartbeat |

## Quick Start

```bash
# All services (API + WS Feed + Scheduler + Redis + TimescaleDB + UI)
docker compose up -d

# Dev server only
pip install -r requirements.txt
./run.sh

# Run tests
pytest tests/
```

## Supported Markets

| Symbols | Timeframes | Forecasts |
|---------|------------|-----------|
| BTC, ETH, SOL, XRP | M5, M15, H1 | GREEN (up), RED (down) |

## Order Types

- **MARKET** — IOC fill against shadow orderbook, fallback to Polymarket REST
- **LIMIT** — Placed in shadow orderbook with optional TTL (auto-cancel)
- **Bracket (TP/SL)** — Take Profit / Stop Loss monitored in real-time by MatchingEngine; fires partial or full exit

## API

All routes prefixed with `/poly-arena/`.

### Bots

```
POST   /bots/              Create bot → returns api_key
GET    /bots/              List bots
PUT    /bots/{name}/rename Rename bot
```

### Binary Options

```
POST   /binary-options/                     Create order (X-API-Key header)
GET    /binary-options/                     List orders
DELETE /binary-options/{id}                 Cancel order
GET    /binary-options/stats/summary        Overall stats
GET    /binary-options/stats/by-bot         Per-bot stats
GET    /binary-options/stats/by-timeframe   Per-timeframe stats
GET    /binary-options/stats/by-forecast    Per-forecast stats
GET    /binary-options/engine/orderbook     Shadow orderbook depth
```

### Example

```bash
# Register a bot
curl -X POST http://localhost:8099/poly-arena/bots/ \
  -H "Content-Type: application/json" \
  -d '{"bot_name": "my-bot"}'

# Place a MARKET order
curl -X POST http://localhost:8099/poly-arena/binary-options/ \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <api_key>" \
  -d '{"symbol": "BTC", "timeframe": "M5", "forecast": "GREEN", "amount": 100}'

# Place a LIMIT order with TP/SL
curl -X POST http://localhost:8099/poly-arena/binary-options/ \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <api_key>" \
  -d '{
    "symbol": "ETH",
    "timeframe": "M5",
    "forecast": "GREEN",
    "amount": 50,
    "limit_price": 0.45,
    "tp_price": 0.70,
    "sl_price": 0.30,
    "ttl": 120
  }'
```

## Project Structure

```
├── main.py                    # FastAPI app + Redis stream consumers
├── models.py                  # SQLAlchemy models (Bot, BinaryOption, BalanceHistory)
├── schemas.py                 # Pydantic request/response schemas
├── database.py                # SQLAlchemy engine + session factory
├── routers/
│   ├── binary_options.py      # Order endpoints
│   ├── bots.py                # Bot CRUD
│   └── dashboard.py           # Health & scheduler status
├── services/
│   ├── matching_engine.py     # Shadow orderbook, bracket TP/SL monitoring
│   ├── ws_feed.py             # Polymarket WebSocket client
│   ├── token_registry.py      # Auto-refresh token IDs at candle boundaries
│   ├── settlement.py          # Settle trades using Binance candle data
│   ├── polymarket.py          # Polymarket REST client
│   ├── scheduler.py           # APScheduler wrapper
│   └── redis_client.py        # Sync/async Redis factory
├── ws_feed_service/           # Standalone WS feed process
│   ├── main.py
│   ├── order_consumer.py
│   └── redis_writer.py
├── scheduler_service/         # Standalone scheduler process
│   └── main.py
├── bots/                      # Trading bot strategies
│   ├── base_bot.py
│   ├── baseline_bots.py
│   ├── llm_strategy.py
│   └── price_action_bot.py
├── tests/                     # pytest suite (fakeredis + SQLite)
├── ui/                        # Next.js frontend (git submodule)
├── docs/                      # Architecture & flow docs
├── alembic/                   # DB migrations
├── docker-compose.yml         # Dev/test environment
└── docker-compose.prod.yml    # Production deployment
```

## Database

PostgreSQL with TimescaleDB extension. Migrations managed by Alembic.

```bash
# Run migrations manually
python migrate.py
```

## Redis IPC

| Key / Stream | Purpose |
|--------------|---------|
| `price:{SYM}:{TF}:{DIR}` | Price hash (best_ask, best_bid, token_id) — 120s TTL |
| `queue:orders:new` | LIMIT/bracket order queue consumed by WS Feed |
| `stream:bracket:exits` | Bracket exit events → API consumer |
| `stream:order:fills` | Fill updates → API consumer |
| `stream:order:cancels` | Cancel events → API consumer |
| `scheduler:heartbeat` | Liveness probe — 60s TTL |

## Testing

```bash
pytest tests/                          # all tests
pytest tests/test_api_basic.py         # single file
pytest -k "bracket"                    # filter by name
```

Tests use `fakeredis` and SQLite (`test_orders.db`) — no external services required.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://polyarena:polyarena@localhost:5432/polyarena` | PostgreSQL connection |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection |
| `DEBUG` | `0` | Enable debug logging (`1`, `true`, `yes`) |
