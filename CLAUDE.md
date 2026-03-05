# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PolyArena is a Polymarket Binary Options paper trading platform with a shadow matching engine. It runs as three independent processes communicating via Redis:

1. **FastAPI API** (`main.py`) — Order CRUD, Redis stream consumers for bracket exits/fills/cancels, served via uvicorn
2. **WS Feed Service** (`ws_feed_service/`) — REST Poller (200ms) fetches Polymarket orderbooks, runs SessionManager/SessionEngine matching, publishes prices to Redis
3. **Scheduler Service** (`scheduler_service/`) — APScheduler process for settlement (Binance candle comparison), stuck order sweeping, balance snapshots, and heartbeat

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run FastAPI dev server
./run.sh  # or: uvicorn main:app --reload --port 8010

# Run all services (API + WS Feed + Scheduler + Redis + UI + test bot)
docker-compose up -d

# Run tests (uses fakeredis, SQLite in-memory)
pytest tests/
pytest tests/test_api_basic.py          # single file
pytest -k "bracket"                     # filter by name

# Database migrations (PostgreSQL + TimescaleDB)
alembic upgrade head
alembic revision --autogenerate -m "description"

# Utility scripts
python scripts/reset_bots.py --user <name> --apply   # reset bot trades/balances
python scripts/delete_bot.py <id_or_name> --yes       # delete bot + all data
python scripts/reset_balances.py                       # reset all balances

# Production
docker-compose -f docker-compose.prod.yml up -d
```

## Architecture

### Three-Process Design

```
┌─────────────────┐     ┌──────────────────────┐     ┌───────────────────┐
│   FastAPI API    │     │   WS Feed Service    │     │ Scheduler Service │
│   (main.py)     │◄───►│ (ws_feed_service/)   │     │(scheduler_service)│
│                 │Redis│                      │     │                   │
│ - Order CRUD    │     │ - RestPoller (200ms)  │     │ - Settlement :05s │
│ - Stream consumers│   │ - SessionManager     │     │ - Stuck sweeps    │
│ - Price reads   │     │ - RedisWriter        │     │ - Balance snapshots│
└─────────────────┘     └──────────────────────┘     └───────────────────┘
         │                        │                           │
         └────────────────────────┼───────────────────────────┘
                                  │
                            ┌─────┴─────┐
                            │   Redis   │
                            │ + TimescaleDB │
                            └───────────┘
```

### Database

**PostgreSQL + TimescaleDB** (not SQLite). Migrations managed via Alembic (`alembic/versions/`). The `database.py` auto-enables TimescaleDB extension with safe fallback to plain PostgreSQL.

### Redis Key Patterns

**Session-keyed orderbook (primary):**
- `orderbook:{SYM}:{TF}:{DIR}:{candle_open}` — full depth per session (bids/asks JSON, 120s TTL)
- `orderbook:{SYM}:{TF}:{DIR}` — legacy key for backward compat (same data as current session)

**Price & tokens:**
- `price:{SYM}:{TF}:{DIR}` — price hash (best_ask, best_bid, token_id, updated_at)
- `tokens:{SYM}:{TF}` — token mapping JSON (current + future sessions per direction)

**IPC streams & queues:**
- `queue:orders:new` — list for LIMIT/bracket orders consumed by WS Feed
- `stream:bracket:exits`, `stream:order:fills`, `stream:order:cancels` — Redis streams consumed by FastAPI via XREADGROUP
- `orderbook:updates` — pub/sub channel for real-time book changes (consumed by WS proxy)
- `scheduler:heartbeat` — liveness probe key (60s TTL)

### Multi-Session Architecture

Each (symbol, timeframe) pair has multiple concurrent sessions managed by `SessionManager` → `SessionEngine`:

**Session lifecycle:** `PREFETCH → ACTIVE → SETTLING → ARCHIVED`
- **PREFETCH**: Token discovered, orderbook pre-loading (20s before candle boundary)
- **ACTIVE**: Accepting orders, matching, bracket monitoring
- **SETTLING**: Candle closed, waiting for Binance settlement
- **ARCHIVED**: Settled, read-only

The system maintains current + 3 future sessions. Each session has its own `ShadowOrderbook` per direction (UP/DOWN) and its own Polymarket token_id.

### Price Data Flow

```
Polymarket REST API
       │
   RestPoller (200ms)              ← ws_feed_service/rest_poller.py
       │
   ┌───┴───┐
   │       │
   ▼       ▼
 Redis    SessionEngine.apply_snapshot()
 (orderbook:*)    │
   │              ▼
   │         ShadowOrderbook.match/bracket
   │              │
   ▼              ▼
 FastAPI      stream:bracket:exits
 (reads)      stream:order:fills
```

**Critical rule:** FastAPI reads prices from Redis (populated by WS Feed), NOT from Polymarket REST API directly. The REST API is only a fallback when Redis has no data.

### Order Lifecycle

1. `POST /poly-arena/binary-options/` — authenticates via API key, reads price from Redis orderbook snapshot, fills against shadow orderbook, saves PENDING order
2. LIMIT/bracket orders pushed to `queue:orders:new` → consumed by WS Feed's OrderConsumer
3. Bracket orders (TP/SL) monitored in real-time by SessionEngine; exits publish to `stream:bracket:exits`
4. Settlement runs every minute at `:05s` via scheduler — fetches Binance OHLC candle, compares forecast (GREEN=up, RED=down) to actual direction
5. Order types: MARKET (IOC), LIMIT (respects `ttl`), FAK (Fill-And-Kill), FOK (Fill-Or-Kill)

### Key Design Decisions

- **Decimal precision** in `matching_engine.py` for all price arithmetic
- **Thread-safe orderbook** — `ShadowOrderbook` uses `threading.Lock` per order for bracket/fill race conditions
- **Settlement uses Binance candles** as canonical price truth, not Polymarket
- **Session-keyed Redis keys** include `candle_open` timestamp to prevent cross-session data mixing
- **Dual-write strategy** in RedisWriter: writes both session-keyed and legacy keys for backward compatibility
- **Default balances**: Bot initial balance = $10,000, User initial balance = $50,000

## Key Modules

| Module | Purpose |
|--------|---------|
| `services/session_manager.py` | Orchestrates SessionEngine instances, thread-safe token index |
| `services/session_engine.py` | Per-session state machine with ShadowOrderbook per direction |
| `services/matching_engine.py` | ShadowOrderbook, SimulatedOrder, bracket TP/SL monitoring |
| `services/token_registry.py` | Auto-refresh Polymarket token IDs at candle boundaries |
| `services/settlement.py` | Settle trades using Binance candle data |
| `services/polymarket.py` | Polymarket REST client (GET `/book`, token discovery via Gamma API) |
| `services/user_balance.py` | User balance snapshots with unrealized P&L from Redis orderbook |
| `services/orderbook_broadcaster.py` | Pub/sub broadcaster + snapshot/token discovery caches for WS proxy |
| `services/redis_client.py` | Sync/async Redis client factory |
| `ws_feed_service/rest_poller.py` | Polls Polymarket REST every 200ms, applies to matching engine |
| `ws_feed_service/redis_writer.py` | Writes prices, orderbook depth, token mappings to Redis |
| `ws_feed_service/order_consumer.py` | Consumes LIMIT/bracket orders from Redis queue |
| `ws_feed_service/session_lifecycle.py` | Pre-creates future sessions, handles rotation/cleanup |
| `config/timing.py` | All timing constants (intervals, TTLs, timeouts) in one place |

## Testing

- **conftest.py** provides fixtures: `db` (test SQLite session), `client` (FastAPI TestClient), `test_bot` (balance=$10,000), `fake_sync_redis`, `fake_async_redis`
- Tests use `fakeredis` by default; docker-compose tests use real Redis
- `pytest.ini` sets `asyncio_mode = auto`
- Production DB is PostgreSQL/TimescaleDB; tests use SQLite in-memory

## API Routes

All routes prefixed with `/poly-arena/`:

| Router | Prefix | Purpose |
|--------|--------|---------|
| `routers/binary_options.py` | `/binary-options` | Order create/list/cancel, stats, engine prices, trade inspector |
| `routers/bots.py` | `/bots` | Bot CRUD (register, list, rename, pause, delete — requires API key) |
| `routers/auth.py` | `/auth` | User registration & login (JWT) |
| `routers/admin.py` | `/admin` | Admin-only: seed data, reset, user management |
| `routers/achievements.py` | `/achievements` | Achievement queries |
| `routers/dashboard.py` | `/dashboard` | Health check, scheduler heartbeat |
| `routers/ws.py` | `/ws` | WebSocket orderbook updates |
| `routers/ws_polymarket.py` | `/ws/polymarket` | Proxied Polymarket orderbook feed for UI |

## Supported Symbols & Timeframes

- Symbols: BTC, ETH
- Timeframes: M5, M15
- Forecasts: GREEN (price up), RED (price down)
- Directions: UP (maps to GREEN), DOWN (maps to RED)
