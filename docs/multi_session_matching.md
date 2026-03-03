# Multi-Session Matching Engine — Design Document

## 1. Problem Statement

### 1.1 Current Architecture (Single-Session)

```
                 queue:orders:new (1 FIFO cho tất cả)
                        │
                        ▼
               ┌─────────────────┐
               │  OrderConsumer   │ ← 1 daemon thread, BRPOP loop
               │  (single queue)  │
               └────────┬────────┘
                        │
                        ▼
               ┌─────────────────┐
               │ MatchingEngine   │ ← 1 singleton, registry of books
               │  _books = {      │
               │    token_abc: ShadowOrderbook,  ← phiên A
               │    token_def: ShadowOrderbook,  ← phiên A+1 (cùng pool)
               │  }               │
               └────────┬────────┘
                        │
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
  stream:order:fills  stream:order:cancels  stream:bracket:exits
        │               │               │
        └───────────────┼───────────────┘
                        ▼
                    FastAPI consumers
```

**Vấn đề:**

| # | Vấn đề | Hậu quả |
|---|--------|---------|
| 1 | **1 Redis queue duy nhất** (`queue:orders:new`) | Lệnh phiên A và A+1 xếp chung hàng, không thể ưu tiên hay xử lý song song |
| 2 | **1 MatchingEngine singleton** | Tất cả orders đều đi qua 1 điểm, không có session isolation |
| 3 | **Token rotation xóa book cũ** | Khi phiên A kết thúc → `invalidate_books([old_tokens])` → cancel tất cả PENDING orders trên book đó, kể cả lệnh A+1 đang resting |
| 4 | **Không phân biệt session trong order** | `SimulatedOrder` không có `session_id`, `_order_to_bo` mapping là global |
| 5 | **Redis keys không có session** | `price:BTC:M5:UP` → chỉ 1 giá, overwrite khi session rotate |
| 6 | **PriceHistory không tag session** | Inspector không phân biệt snapshot từ phiên nào |

### 1.2 Target Architecture (Multi-Session)

Mỗi phiên (candle session) được isolate hoàn toàn:

- Redis key riêng
- Matching Engine instance riêng
- Lifecycle riêng (create → active → settling → archived)

---

## 2. Session Concept

### 2.1 Session ID

```
session_id = "{symbol}:{timeframe}:{candle_open_ts}"
```

Ví dụ:
```
BTC:M5:1709312700    ← phiên BTC M5 bắt đầu 14:25:00 UTC
BTC:M5:1709313000    ← phiên BTC M5 bắt đầu 14:30:00 UTC (A+1)
ETH:M15:1709312700   ← phiên ETH M15 bắt đầu 14:25:00 UTC
```

### 2.2 Session Lifecycle

```
PREFETCH → ACTIVE → SETTLING → ARCHIVED
   │          │          │          │
   │          │          │          └─ Book destroyed, orders cleaned
   │          │          └─ No new orders, settlement running
   │          └─ Accepting orders, matching active
   └─ Token resolved, book created, pre-populated from WS
```

**Timeline ví dụ cho M5:**

```
14:24:55  PREFETCH   BTC:M5:1709313000  ← Resolve future token, create book
14:25:00  ACTIVE     BTC:M5:1709313000  ← Candle open, accept orders
14:25:00  SETTLING   BTC:M5:1709312700  ← Old candle close, no new orders
14:25:05  Settlement runs for BTC:M5:1709312700
14:25:10  ARCHIVED   BTC:M5:1709312700  ← Destroy book, free memory

14:29:55  PREFETCH   BTC:M5:1709313300  ← Next session pre-created
14:30:00  ACTIVE     BTC:M5:1709313300
14:30:00  SETTLING   BTC:M5:1709313000
...
```

---

## 3. New Architecture

### 3.1 Overview

```
                    ┌──────────────────────────────┐
                    │      Order Router (API)       │
                    │  resolve session → pick queue │
                    └──────────┬───────────────────┘
                               │
               ┌───────────────┼───────────────────┐
               ▼               ▼                   ▼
    queue:orders:BTC:M5:      queue:orders:BTC:M5:     queue:orders:ETH:M5:
    1709312700                1709313000               1709312700
               │               │                   │
               ▼               ▼                   ▼
    ┌──────────────┐  ┌──────────────┐    ┌──────────────┐
    │ SessionEngine│  │ SessionEngine│    │ SessionEngine│
    │ BTC:M5:...700│  │ BTC:M5:...000│    │ ETH:M5:...700│
    │              │  │              │    │              │
    │ OrderConsumer│  │ OrderConsumer│    │ OrderConsumer│
    │ ShadowBook   │  │ ShadowBook   │    │ ShadowBook   │
    │ BracketMon   │  │ BracketMon   │    │ BracketMon   │
    └──────┬───────┘  └──────┬───────┘    └──────┬───────┘
           │                 │                   │
           ▼                 ▼                   ▼
    stream:fills:BTC:M5:    stream:fills:BTC:M5:     stream:fills:ETH:M5:
    1709312700              1709313000               1709312700
```

### 3.2 Redis Key Schema

| Pattern | Mô tả | TTL |
|---------|--------|-----|
| `queue:orders:{SID}` | Order queue per session | ∞ (dọn khi ARCHIVED) |
| `price:{SYM}:{TF}:{DIR}:{candle_ts}` | Price cache per session | 120s |
| `orderbook:{SYM}:{TF}:{DIR}:{candle_ts}` | Orderbook depth per session | 120s |
| `stream:fills:{SID}` | Fill events per session | MAXLEN 10k |
| `stream:cancels:{SID}` | Cancel events per session | MAXLEN 10k |
| `stream:brackets:{SID}` | Bracket exit events per session | MAXLEN 10k |
| `session:state:{SID}` | Session state (JSON) | 2 × period |
| `session:active` | Set of active session IDs | ∞ |

Trong đó `{SID}` = `{SYM}:{TF}:{candle_open_ts}`.

**Backward compatibility**: Giữ legacy keys (`price:BTC:M5:UP`, `queue:orders:new`) cho phiên hiện tại, đồng thời ghi dual vào session-keyed.

### 3.3 Session State Object

```json
// Redis key: session:state:BTC:M5:1709313000
{
  "session_id": "BTC:M5:1709313000",
  "symbol": "BTC",
  "timeframe": "M5",
  "candle_open": 1709313000,
  "candle_close": 1709313300,
  "state": "ACTIVE",
  "tokens": {
    "UP": "0xabc123...",
    "DOWN": "0xdef456..."
  },
  "created_at": "2025-03-02T14:29:55Z",
  "activated_at": "2025-03-02T14:30:00Z"
}
```

---

## 4. Component Design

### 4.1 SessionManager (New — orchestrator)

**File**: `services/session_manager.py`

```python
class SessionManager:
    """Manages lifecycle of all active SessionEngine instances."""

    _engines: dict[str, SessionEngine]    # session_id → engine
    _redis: aioredis.Redis

    async def on_candle_boundary(self, symbol: str, tf: str, new_candle_ts: int):
        """Called by TokenRegistry at each candle boundary."""
        old_sid = f"{symbol}:{tf}:{new_candle_ts - period}"
        new_sid = f"{symbol}:{tf}:{new_candle_ts}"

        # 1. Transition old session: ACTIVE → SETTLING
        if old_sid in self._engines:
            await self._engines[old_sid].transition(SessionState.SETTLING)

        # 2. Activate new session (pre-created in PREFETCH)
        if new_sid in self._engines:
            await self._engines[new_sid].transition(SessionState.ACTIVE)

        # 3. Pre-create next session (PREFETCH)
        next_sid = f"{symbol}:{tf}:{new_candle_ts + period}"
        await self._prefetch_session(next_sid)

        # 4. Archive old sessions (after settlement completes)
        await self._cleanup_stale_sessions()

    async def _prefetch_session(self, session_id: str):
        """Resolve future token, create book, subscribe WS."""
        tokens = await resolve_tokens_for_session(session_id)
        engine = SessionEngine(session_id, tokens, self._redis)
        self._engines[session_id] = engine
        engine.state = SessionState.PREFETCH

    async def route_order(self, session_id: str, order_data: dict):
        """Push order to the correct session queue."""
        queue_key = f"queue:orders:{session_id}"
        await self._redis.lpush(queue_key, json.dumps(order_data))

    def get_engine(self, session_id: str) -> SessionEngine | None:
        return self._engines.get(session_id)
```

### 4.2 SessionEngine (New — per-session isolated unit)

**File**: `services/session_engine.py`

```python
class SessionEngine:
    """One matching engine per candle session, with its own:
    - ShadowOrderbook (1 per direction: UP + DOWN)
    - OrderConsumer thread (BRPOP from session queue)
    - Redis streams (fills, cancels, brackets)
    """

    session_id: str           # "BTC:M5:1709313000"
    state: SessionState       # PREFETCH | ACTIVE | SETTLING | ARCHIVED

    # Per-direction orderbooks
    books: dict[str, ShadowOrderbook]    # {"UP": book, "DOWN": book}
    tokens: dict[str, str]               # {"UP": token_id, "DOWN": token_id}

    # Redis keys (session-scoped)
    queue_key: str            # queue:orders:BTC:M5:1709313000
    fills_stream: str         # stream:fills:BTC:M5:1709313000
    cancels_stream: str       # stream:cancels:BTC:M5:1709313000
    brackets_stream: str      # stream:brackets:BTC:M5:1709313000

    # Consumer thread
    _consumer_thread: Thread

    async def transition(self, new_state: SessionState):
        """State machine transitions with side effects."""
        match new_state:
            case SessionState.ACTIVE:
                self._consumer_thread.start()    # Start consuming orders
            case SessionState.SETTLING:
                self._stop_accepting_orders()    # Reject new orders
                self._cancel_all_pending()       # Cancel unfilled LIMIT
                self._expire_books()
            case SessionState.ARCHIVED:
                self._consumer_thread.stop()
                self._cleanup_redis_keys()
                self.books.clear()

    def dispatch_ws_event(self, event: dict):
        """Route WS event to the correct book by token_id."""
        token_id = event.get("asset_id")
        for direction, tid in self.tokens.items():
            if tid == token_id:
                self.books[direction].apply_event(event)
                break
```

### 4.3 Order Router (Modified API endpoint)

**File**: `routers/binary_options.py` — changes to POST endpoint

```python
# Before (current):
redis.lpush("queue:orders:new", json.dumps(order_data))

# After (multi-session):
session_id = f"{symbol}:{tf}:{session.candle_open}"
queue_key = f"queue:orders:{session_id}"
redis.lpush(queue_key, json.dumps({
    **order_data,
    "session_id": session_id,
}))
```

### 4.4 WebSocket Event Router (Modified)

**File**: `services/ws_feed.py` — route events to correct SessionEngine

```python
# Before (current):
engine.dispatch_event(event)   # single global MatchingEngine

# After (multi-session):
token_id = event.get("asset_id")
for engine in session_manager.get_engines_for_token(token_id):
    engine.dispatch_ws_event(event)
```

**Lưu ý**: Cùng 1 token_id có thể thuộc nhiều session (ít xảy ra nhưng possible khi Polymarket reuse token). `session_manager` lookup bằng token → list[SessionEngine].

### 4.5 FastAPI Stream Consumers (Modified)

**File**: `main.py` — consume per-session streams

```python
# Before (current):
# 1 consumer per stream type, global
await r.xreadgroup("fills-group", "api", {"stream:order:fills": ">"})

# After (multi-session):
# Consumer per active session
active_sessions = await r.smembers("session:active")
streams = {f"stream:fills:{sid}": ">" for sid in active_sessions}
results = await r.xreadgroup("fills-group", "api", streams)
for stream_key, messages in results:
    session_id = stream_key.split(":", 2)[2]  # extract SID
    for msg_id, data in messages:
        await _handle_order_fill(r, stream_key, group, msg_id, data)
```

---

## 5. Data Flow — Ví dụ Chi Tiết

### 5.1 Scenario: User đặt lệnh A+1 LIMIT vào 14:24:50

**Context:**
- Phiên hiện tại: `BTC:M5:1709312700` (14:25:00-14:30:00) → ACTIVE
- Phiên kế tiếp: `BTC:M5:1709313000` (14:30:00-14:35:00) → PREFETCH
- User đặt LIMIT BUY $50 @ 0.48, session_offset=1

```
Timeline:

14:24:50  ┌─ API nhận lệnh ─────────────────────────────────────────┐
          │ resolve_session(M5, offset=1)                           │
          │ → candle_open=1709313000, settlement=14:35:00           │
          │ → session_id = "BTC:M5:1709313000"                     │
          │                                                         │
          │ _resolve_future_token() → token_id = "0xFUTURE123"     │
          │                                                         │
          │ _try_fill_limit_from_rest(token_override="0xFUTURE123")│
          │ → best_ask = 0.52 > limit 0.48 → CANNOT fill           │
          │ → Defer to Matching Engine                              │
          │                                                         │
          │ bot.balance -= $50                                      │
          │ entry_fee = 0 (maker)                                   │
          │ DB: INSERT BinaryOption(session_offset=1, ...)          │
          │                                                         │
          │ redis.lpush("queue:orders:BTC:M5:1709313000", {        │
          │   "bo_id": 456,                                        │
          │   "token_id": "0xFUTURE123",                           │
          │   "session_id": "BTC:M5:1709313000",                   │
          │   "limit_price": 0.48,                                 │
          │   "quantity": 104.17,  # 50/0.48                       │
          │   "settlement_at": "2025-03-02T14:35:00Z",             │
          │   ...                                                  │
          │ })                                                      │
          └─────────────────────────────────────────────────────────┘

14:24:50  ┌─ SessionEngine "BTC:M5:1709313000" (PREFETCH) ─────────┐
          │ OrderConsumer BRPOP "queue:orders:BTC:M5:1709313000"   │
          │ → Nhận order, place_virtual_order() vào book UP        │
          │ → Book chưa có data (no WS events yet)                 │
          │ → Order PENDING, đợi WS price                          │
          └─────────────────────────────────────────────────────────┘

14:25:00  ┌─ Candle Boundary ───────────────────────────────────────┐
          │ SessionManager.on_candle_boundary("BTC", "M5", ...000) │
          │                                                         │
          │ 1. BTC:M5:1709312700 → SETTLING                        │
          │    - Cancel tất cả PENDING orders                       │
          │    - Stop accepting new orders                          │
          │                                                         │
          │ 2. BTC:M5:1709313000 → ACTIVE                          │
          │    - WS đã subscribe "0xFUTURE123" từ PREFETCH          │
          │    - Book nhận snapshot, run_matching()                 │
          │    - Order #456 check: ask 0.52 > limit 0.48 → PENDING │
          │                                                         │
          │ 3. PREFETCH BTC:M5:1709313300                           │
          │    - Resolve token cho phiên tiếp theo                  │
          └─────────────────────────────────────────────────────────┘

14:27:15  ┌─ WS Price Update ───────────────────────────────────────┐
          │ Polymarket WS: price_change for "0xFUTURE123"          │
          │ → best_ask drops to 0.47                                │
          │                                                         │
          │ SessionEngine "BTC:M5:1709313000":                     │
          │   book.apply_changes() → book.run_matching()           │
          │   → Order #456: ask 0.47 <= limit 0.48 → MATCH!       │
          │   → Fill 104.17 shares @ 0.47, status=FILLED           │
          │   → Maker rebate calculated                             │
          │                                                         │
          │ Publish to stream:fills:BTC:M5:1709313000:             │
          │   {bo_id: 456, filled: 104.17, avg: 0.47, ...}        │
          └─────────────────────────────────────────────────────────┘

14:27:15  ┌─ FastAPI Consumer ──────────────────────────────────────┐
          │ XREADGROUP stream:fills:BTC:M5:1709313000              │
          │ → _handle_order_fill(bo_id=456)                        │
          │ → Update DB: avg_price=0.47, num_shares=104.17         │
          │ → Maker rebate: +$0.xx to bot balance                  │
          └─────────────────────────────────────────────────────────┘

14:35:00  ┌─ Settlement ────────────────────────────────────────────┐
          │ BTC:M5:1709313000 → SETTLING                           │
          │ Scheduler: fetch Binance candle, settle trade #456     │
          │ → Win/Loss based on actual price direction              │
          │ → 10s later: ARCHIVED, cleanup Redis keys              │
          └─────────────────────────────────────────────────────────┘
```

### 5.2 Scenario: 2 phiên chạy song song

```
Time    BTC:M5:1709312700        BTC:M5:1709313000
        (candle A)                (candle A+1)
─────── ──────────────────────── ─────────────────────
14:24   ACTIVE                   (not created)
        book: token_aaa
        orders: [#100,#101,#102]

14:24:55                          PREFETCH
                                  book: token_bbb (empty)
                                  orders: []

14:25   ACTIVE → SETTLING        PREFETCH → ACTIVE
        cancel pending            start matching
        settle: #100 WIN         orders: [#200 from A+1]
                #101 LOSS
                #102 CANCELLED

14:26   SETTLING                 ACTIVE
        cleanup in progress       book: token_bbb (live WS)
                                  #200 fills @ 0.47

14:27   ARCHIVED                 ACTIVE
        keys deleted              orders: [#200 FILLED, #201...]
        memory freed

14:30                            SETTLING → ARCHIVED
```

---

## 6. SimulatedOrder Changes

```python
@dataclass
class SimulatedOrder:
    order_id: str
    session_id: str             # NEW: "BTC:M5:1709313000"
    side: str
    price: Decimal
    quantity: Decimal
    order_type: str             # "MARKET" | "LIMIT"
    # ... existing fields ...
```

---

## 7. Migration Strategy

### Phase 1: Session-Keyed Redis (backward compatible)

**Thay đổi nhỏ, không breaking:**

1. **Dual-write order queue**: API ghi vào cả `queue:orders:new` (legacy) VÀ `queue:orders:{SID}`
2. **Dual-write streams**: ME ghi fills/cancels vào cả legacy VÀ session-keyed streams
3. **Session-keyed price cache**: `price:{SYM}:{TF}:{DIR}:{candle_ts}` song song với legacy key (đã có một phần)
4. **`session_id` field** thêm vào queue message

**Verify**: Hệ thống vẫn hoạt động bình thường với single MatchingEngine.

### Phase 2: SessionEngine per session

1. Tạo `SessionEngine` class wrapping `ShadowOrderbook` + per-session `OrderConsumer`
2. `SessionManager` quản lý lifecycle
3. OrderConsumer chuyển từ BRPOP `queue:orders:new` sang BRPOP `queue:orders:{SID}`
4. WS event routing: `ws_feed.py` dispatch event đến tất cả SessionEngine có token_id khớp

**Verify**: Mỗi session có book riêng, không ảnh hưởng lẫn nhau.

### Phase 3: Remove legacy single-queue path

1. Xóa `queue:orders:new`
2. Xóa global `MatchingEngine` singleton
3. FastAPI consumers đọc từ session-keyed streams
4. Cleanup code, remove dual-write

---

## 8. Edge Cases

### 8.1 Token Reuse

Polymarket **có thể** reuse cùng token_id cho phiên kế tiếp (hiếm nhưng xảy ra). Giải pháp:
- SessionEngine lookup bằng `(token_id, session_id)`, không chỉ `token_id`
- WS event dispatch: fan-out đến tất cả engine có token_id khớp

### 8.2 Late Fill (fill đến sau settlement_at)

Đã có cơ chế `_handle_order_fill()` check `settlement_at <= now` → settle ngay. Multi-session không thay đổi logic này, chỉ thay đổi stream source.

### 8.3 Order Placed Exactly at Boundary

```
14:29:59.999  API nhận order, session_offset=0
14:30:00.000  Candle boundary, session rotate
```

Giải pháp: `resolve_session()` đã handle bằng cách check `candle_open` vs `current_open`. Nếu order target candle vừa close → reject với error "past candle".

### 8.4 PREFETCH Session Token Not Yet Available

Polymarket có thể chưa publish market cho phiên A+2 khi ta prefetch. Giải pháp:
- PREFETCH retry loop (đã có trong `TokenRegistry`)
- SessionEngine ở trạng thái PREFETCH chấp nhận orders vào queue nhưng chưa match
- Khi token resolved → populate book → start matching

### 8.5 Memory / Resource Limits

Với 4 symbols × 3 timeframes × 2 directions × 3 sessions (past+current+future) = **72 books** maximum.

Mỗi `ShadowOrderbook` ~ 50KB memory (bids/asks sorted dict + order list). Tổng ~ 3.6MB — negligible.

---

## 9. Redis Key Lifecycle

```
T = candle_open
P = period (300s cho M5)

T - 5s:   CREATE  queue:orders:{SID}
                   session:state:{SID}  = {state: "PREFETCH"}
                   SADD session:active {SID}

T + 0s:   UPDATE  session:state:{SID}  = {state: "ACTIVE"}
          CREATE  price:{SYM}:{TF}:{DIR}:{T}       ← WS writes
                  orderbook:{SYM}:{TF}:{DIR}:{T}   ← WS writes

T + P:    UPDATE  session:state:{SID}  = {state: "SETTLING"}
          ← No new orders accepted

T + P+10: UPDATE  session:state:{SID}  = {state: "ARCHIVED"}
          DELETE  queue:orders:{SID}
          DELETE  stream:fills:{SID}
          DELETE  stream:cancels:{SID}
          DELETE  stream:brackets:{SID}
          DELETE  session:state:{SID}
          SREM   session:active {SID}
          ← price/orderbook keys tự expire bởi TTL
```

---

## 10. API Changes Summary

| Component | Before | After |
|-----------|--------|-------|
| Order queue | `queue:orders:new` | `queue:orders:{SID}` |
| Price key | `price:BTC:M5:UP` | `price:BTC:M5:UP:1709313000` |
| Orderbook key | `orderbook:BTC:M5:UP` | `orderbook:BTC:M5:UP:1709313000` |
| Fill stream | `stream:order:fills` | `stream:fills:{SID}` |
| Cancel stream | `stream:order:cancels` | `stream:cancels:{SID}` |
| Bracket stream | `stream:bracket:exits` | `stream:brackets:{SID}` |
| ME instance | 1 global singleton | 1 per active session |
| Consumer thread | 1 BRPOP on single queue | 1 per SessionEngine |
| WS dispatch | `engine.dispatch_event()` | Fan-out to matching SessionEngines |
| SimulatedOrder | No session_id | `session_id` field added |
| PriceHistory | No session tag | Derived from `recorded_at` in session window |
