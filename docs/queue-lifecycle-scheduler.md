# Queue & Session Lifecycle Management

## Overview

Session lifecycle is managed by two independent systems:

| System | Responsibility |
|--------|---------------|
| **WS Feed Service** | Session creation, prefetch, activation, archival, Redis key cleanup |
| **Scheduler Service** | Settlement (Binance candles), stuck order sweep, heartbeat |

## WS Feed Service — Session Lifecycle

### Invariant

Always maintain **4 sessions** per (sym, tf): 1 ACTIVE + 3 PREFETCH (future).

### Implementation

Module: `ws_feed_service/session_lifecycle.py`

Two functions run every `SESSION_LIFECYCLE_TICK_S` (5s):

#### `ensure_future_sessions()`

For each (sym, tf) × (4 symbols × 3 timeframes = 12 combos):
1. Calculate expected candle_opens: current + 3 future
2. If <20s to next boundary: also create +1 extra future (pre-create)
3. For each missing session:
   - Resolve UP+DOWN tokens (registry cache → REST fallback)
   - `sm.create_session()` → PREFETCH (or ACTIVE if current candle)
   - `feed.add_tokens()` → Polymarket WS re-subscribe
   - `writer.register_session_tokens()` → orderbook writes

#### `cleanup_expired_sessions()`

For each non-ARCHIVED session where `now > candle_open + period + 10s`:
1. Transition: ACTIVE → SETTLING → ARCHIVED (or SETTLING → ARCHIVED)
2. RPOP orphaned orders from queue → log warnings
3. Delete Redis keys:
   - `queue:orders:{session_id}`
   - `orderbook:{SYM}:{TF}:UP:{candle_open}`
   - `orderbook:{SYM}:{TF}:DOWN:{candle_open}`

### Configuration (`config/timing.py`)

```python
REQUIRED_FUTURE_SESSIONS = 3       # current + 3 future = 4 total
SESSION_PRE_CREATE_BUFFER_S = 20   # pre-create 20s before boundary
SESSION_CLEANUP_DELAY_S = 10       # cleanup 10s after session ends
SESSION_LIFECYCLE_TICK_S = 5       # lifecycle check interval
```

### M5 Timeline Example

```
14:20:00  Sessions: 14:20(A) 14:25(P) 14:30(P) 14:35(P) → 4 ✓
14:24:40  Tick: <20s to boundary → create 14:40(P) → 5 sessions
14:25:00  TokenRegistry: 14:25 promoted PREFETCH→ACTIVE
14:25:10  Tick: 14:20 expired (now > 14:20+300+10) → ARCHIVE + delete keys
          Sessions: 14:25(A) 14:30(P) 14:35(P) 14:40(P) → 4 ✓
```

### Edge Cases

| Case | Handling |
|------|----------|
| Polymarket hasn't published future market | `get_token_id_at()` returns None → skip, retry next tick (5s) |
| WS Feed restart mid-candle | `ensure_future_sessions()` recreates all 4 on first tick |
| Token already in WS Feed list | `feed.add_tokens()` deduplicates via set difference |
| Race: cleanup vs OrderConsumer BRPOP | Cleanup runs 10s after session end; OrderConsumer stops polling ARCHIVED — no race |
| Orphaned orders in expired queue | `cleanup_expired_sessions()` RPOPs and logs them |

## Scheduler Service — Settlement & Monitoring

### Existing Jobs (unchanged)

| Job | Schedule | Purpose |
|-----|----------|---------|
| Settlement | cron :05s each minute | Fetch Binance OHLC, compare forecast, settle trades |
| Stuck sweep | interval 5m | Cancel orders stuck beyond threshold |
| Heartbeat | interval 30s | Publish liveness probe to Redis |

### What Scheduler Does NOT Do

- Session creation/archival (owned by WS Feed)
- Redis key cleanup (owned by WS Feed)
- Queue monitoring (moved to WS Feed lifecycle tick)

## Redis Key Lifecycle

| Key Pattern | Created By | Deleted By | TTL |
|-------------|-----------|-----------|-----|
| `queue:orders:{session_id}` | Router LPUSH | WS Feed cleanup | None (explicit delete) |
| `orderbook:{SYM}:{TF}:{DIR}:{candle_ts}` | RedisWriter | WS Feed cleanup | 120s (also explicit) |
| `price:{SYM}:{TF}:{DIR}` | RedisWriter | Never (overwritten) | 120s |
| `tokens:{SYM}:{TF}` | RedisWriter | Never (overwritten) | 600s |
