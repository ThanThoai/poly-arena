# Phase 2 & 3: Multi-Session Matching Engine Implementation Plan

## Overview

Replace the global singleton MatchingEngine + single OrderConsumer with per-session `SessionEngine` instances managed by a `SessionManager`. Each candle session gets isolated ordering, matching, and event publishing.

**Scope**: 4 symbols × 3 timeframes × 4 sessions (current + 3 future) = up to ~96 session engines. Each has UP+DOWN books = ~192 books max.

---

## New Files

### 1. `services/session_engine.py` — Per-session isolated unit

```python
class SessionState(Enum):
    PREFETCH = "PREFETCH"
    ACTIVE = "ACTIVE"
    SETTLING = "SETTLING"
    ARCHIVED = "ARCHIVED"

class SessionEngine:
    """One matching engine per candle session."""

    session_id: str           # "BTC:M5:1709313000"
    symbol: str
    timeframe: str
    candle_open: int
    state: SessionState

    # Per-direction orderbooks (reuse existing ShadowOrderbook)
    books: dict[str, ShadowOrderbook]    # {"UP": book, "DOWN": book}
    tokens: dict[str, str]               # {"UP": token_id, "DOWN": token_id}

    # Per-session Redis keys
    queue_key: str            # queue:orders:BTC:M5:1709313000
    fills_stream: str         # stream:fills:BTC:M5:1709313000
    cancels_stream: str       # stream:cancels:BTC:M5:1709313000
    brackets_stream: str      # stream:brackets:BTC:M5:1709313000

    # Consumer thread (BRPOP on session queue)
    _consumer: SessionOrderConsumer
```

Key design decisions:
- **Reuse `ShadowOrderbook`** as-is — no changes to matching logic
- Each SessionEngine creates 2 books (UP/DOWN), keyed by their Polymarket token_id
- Consumer thread per session does BRPOP on `queue:orders:{SID}`
- State transitions enforce lifecycle: PREFETCH→ACTIVE→SETTLING→ARCHIVED
- PREFETCH: accepts orders into queue but consumer not started yet (orders queue up)
- ACTIVE: consumer running, matching active
- SETTLING: stop accepting new orders, cancel unfilled, expire books
- ARCHIVED: cleanup all Redis keys, destroy books

### 2. `services/session_manager.py` — Orchestrator

```python
class SessionManager:
    _engines: dict[str, SessionEngine]  # session_id → engine
    _token_to_sessions: dict[str, list[str]]  # token_id → [session_ids]

    def get_or_create_session(session_id, symbol, tf, candle_open, tokens) → SessionEngine
    def get_engine(session_id) → SessionEngine | None
    def get_engines_for_token(token_id) → list[SessionEngine]
    def dispatch_ws_event(event)  # fan-out to all matching engines
    def on_candle_boundary(symbol, tf, new_candle_ts)  # lifecycle transitions
    def route_order(session_id, order_data)  # push to session queue
    def cleanup_archived()
    def shutdown()
```

Key design decisions:
- Maintains reverse index `token_id → [session_id]` for WS event fan-out
- On candle boundary: old→SETTLING, current→ACTIVE, prefetch next
- `dispatch_ws_event()` replaces the global `engine.dispatch_event()` — fans out to all sessions with matching token_id
- Cleanup task removes ARCHIVED engines periodically

### 3. `services/session_order_consumer.py` — Per-session consumer

Thin wrapper around the existing OrderConsumer logic but:
- BRPOP on `queue:orders:{SID}` instead of `queue:orders:new`
- Publishes to per-session streams (`stream:fills:{SID}`, etc.)
- Uses the SessionEngine's books instead of global MatchingEngine
- Rejects orders if session state != ACTIVE/PREFETCH

---

## Modified Files

### 4. `ws_feed_service/main.py` — Replace global ME with SessionManager

**Before**: Single `MatchingEngine` + single `OrderConsumer` + monkey-patched dispatch
**After**: `SessionManager` manages all sessions; `_patch_dispatch_event` routes through SessionManager

Changes:
- Replace `engine = get_engine()` with `session_mgr = SessionManager(async_redis, sync_redis, writer, loop)`
- `_patch_dispatch_event` calls `session_mgr.dispatch_ws_event(event)` instead of `engine.dispatch_event(event)`
- `on_new_tokens` callback: create/update sessions via `session_mgr.on_token_refresh()`
- Remove single `OrderConsumer` — each SessionEngine has its own
- Recovery: route pending orders to correct session queue
- Expiry tick: iterate all active session engines

### 5. `routers/binary_options.py` — Route orders to session queues

**Before**: `sr.lpush(QUEUE_ORDERS_NEW, order_payload)`
**After**: `sr.lpush(f"queue:orders:{session_id}", order_payload)`

Changes to `_queue_prefilled_to_me()`:
- Accept `session_id` parameter
- Push to `queue:orders:{session_id}` instead of `QUEUE_ORDERS_NEW`

Changes to LIMIT order deferred path:
- Push to `queue:orders:{session_id}` instead of `QUEUE_ORDERS_NEW`

### 6. `ws_feed_service/redis_writer.py` — Per-session stream publishing

Add new methods for session-scoped stream publishing:
- `publish_order_fill_session(session_id, ...)` → writes to `stream:fills:{SID}`
- `publish_order_cancel_session(session_id, ...)` → writes to `stream:cancels:{SID}`
- `publish_bracket_exit_session(session_id, ...)` → writes to `stream:brackets:{SID}`

**Dual-write**: During Phase 2, keep writing to both legacy streams AND per-session streams. Phase 3 removes legacy writes.

### 7. `main.py` (FastAPI) — Consume per-session streams

**Before**: 4 consumer tasks reading from 4 fixed streams
**After**: Dynamic consumers that discover active sessions and read from per-session streams

New approach:
- `_consume_session_streams()` — single task that:
  1. Periodically checks `session:active` Redis set for active session IDs
  2. For each active session, reads from `stream:fills:{SID}`, `stream:cancels:{SID}`, `stream:brackets:{SID}`
  3. Uses XREADGROUP with multiple streams in one call
  4. Routes messages to existing handlers (`_handle_order_fill`, `_handle_bracket_exit`, etc.)

**Dual-read**: During Phase 2, keep legacy consumers AND add session consumers. Handlers are idempotent (check `bo.exit_trigger is None` etc.) so duplicate processing is safe. Phase 3 removes legacy consumers.

### 8. `ws_feed_service/config.py` — Add session queue/stream key patterns

```python
def session_queue_key(session_id: str) -> str:
    return f"queue:orders:{session_id}"

def session_fills_stream(session_id: str) -> str:
    return f"stream:fills:{session_id}"

def session_cancels_stream(session_id: str) -> str:
    return f"stream:cancels:{session_id}"

def session_brackets_stream(session_id: str) -> str:
    return f"stream:brackets:{session_id}"

SESSION_ACTIVE_KEY = "session:active"
SESSION_STATE_KEY_PREFIX = "session:state"
```

### 9. `services/matching_engine.py` — Add `session_id` to SimulatedOrder

Add `session_id: str = ""` field to `SimulatedOrder` dataclass. This is backward-compatible (empty string for legacy orders).

---

## Phase 2 Implementation Order

1. **Config**: Add session key helpers to `ws_feed_service/config.py`
2. **SimulatedOrder**: Add `session_id` field
3. **SessionEngine**: Create `services/session_engine.py`
4. **SessionOrderConsumer**: Create `services/session_order_consumer.py`
5. **SessionManager**: Create `services/session_manager.py`
6. **RedisWriter**: Add per-session stream publish methods (dual-write)
7. **ws_feed_service/main.py**: Replace global ME with SessionManager
8. **routers/binary_options.py**: Route orders to session queues (dual-write to both legacy and session queue)
9. **main.py (FastAPI)**: Add session stream consumers (dual-read)
10. **Tests**: Verify existing tests pass + add session isolation tests

## Phase 3 Implementation Order (in same PR)

11. **routers/binary_options.py**: Remove legacy `QUEUE_ORDERS_NEW` writes
12. **RedisWriter**: Remove legacy stream writes
13. **main.py**: Remove legacy stream consumers
14. **ws_feed_service/main.py**: Remove global `MatchingEngine` singleton usage
15. **Cleanup**: Remove unused imports, dead code

---

## Key Design Decisions

1. **No changes to ShadowOrderbook or matching logic** — SessionEngine wraps existing code
2. **Dual-write/dual-read during Phase 2** ensures zero downtime and backward compat
3. **Session state in Redis** (`session:active` set + `session:state:{SID}` hash) enables FastAPI to discover active sessions
4. **BRPOP per session** means each session has its own consumer thread — max ~24 threads for active sessions (4sym × 3tf × 2 states). This is acceptable.
5. **WS fan-out**: One Polymarket WS event may update books in multiple sessions (if same token_id used across sessions). The SessionManager maintains a token→sessions reverse index.
6. **Recovery**: On restart, SessionManager recreates sessions from `session:active` set and re-processes queued orders.

---

## Risk Mitigation

- **Thread count**: Max ~24 active sessions × 1 consumer thread = manageable
- **Memory**: ~192 books × 50KB = ~10MB — negligible
- **Redis keys**: Cleanup on ARCHIVED prevents key accumulation
- **Backward compat**: Dual-write ensures API and WS Feed can be deployed independently
