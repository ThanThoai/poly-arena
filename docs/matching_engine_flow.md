# Matching Engine — Chi tiết luồng xử lý từng step

## Tổng quan kiến trúc

```
Polymarket WebSocket
    │  wss://ws-subscriptions-clob.polymarket.com/ws/market
    │  Events: book, price_change, best_bid_ask, last_trade_price, market_resolved
    ▼
PolymarketFeed (services/ws_feed.py)
    │  _handle_message() → json.loads → engine.dispatch_event()
    ▼
MatchingEngine.dispatch_event()  (services/matching_engine.py:1404)
    │  Route theo event_type → handler tương ứng
    ├──→ _handle_book()          → ShadowOrderbook.apply_snapshot()
    ├──→ _handle_price_change()  → ShadowOrderbook.apply_changes()
    ├──→ _handle_best_bid_ask()  → ShadowOrderbook.apply_changes() (inferred size)
    ├──→ _handle_last_trade()    → ShadowOrderbook.record_trade()
    └──→ _handle_market_resolved() → cancel TP/SL, mark position_closed
    │
    │  Sau mỗi event:
    ├──→ ShadowOrderbook.run_matching()         ← khớp lệnh LIMIT/MARKET
    ├──→ ShadowOrderbook.monitor_bracket_orders() ← check TP/SL
    └──→ RedisWriter.update_price() + update_orderbook()  ← ghi Redis, pub/sub
```

---

## Step 1: Polymarket WebSocket → PolymarketFeed

**File:** `services/ws_feed.py`

### 1.1 Kết nối

```
PolymarketFeed.start()
  └─→ asyncio.create_task(_run_forever())
        └─→ _connect_and_listen()
              ├─→ websockets.connect("wss://ws-subscriptions-clob.polymarket.com/ws/market")
              ├─→ ws.send({"assets_ids": [...], "type": "market"})  ← subscribe
              ├─→ asyncio.create_task(_heartbeat(ws))               ← PING mỗi 10s
              └─→ async for raw_msg in ws:
                    _handle_message(raw_msg)                        ← parse + dispatch
```

### 1.2 Reconnect

```
_run_forever() loop:
  try:
    _connect_and_listen()        ← chạy cho đến disconnect
    reconnect_delay = 1s         ← reset on clean close
  except ConnectionClosed:
    reconnect_delay = 1s         ← reset (đã fix, trước là giữ delay)
  except Exception:
    pass                         ← giữ delay hiện tại

  sleep(reconnect_delay)
  reconnect_delay = min(delay * 2, 10s)  ← max 10s (đã fix, trước là 60s)
```

### 1.3 Message dispatch

```python
# services/ws_feed.py:198-220
def _handle_message(self, raw: str):
    data = json.loads(raw)
    events = data if isinstance(data, list) else [data]
    for event in events:
        engine.dispatch_event(event)  # ← monkey-patched version
```

### 1.4 Monkey-patch trong ws_feed_service/main.py

`dispatch_event()` bị wrap tại `ws_feed_service/main.py:171-214`:

```
original_dispatch(event)           ← gọi matching engine gốc
  │
  ├─→ Trích best_ask, best_bid từ event
  ├─→ writer.update_price(token_id, best_ask, best_bid)      ← Redis HSET + TTL
  └─→ writer.update_orderbook(token_id, bids, asks)          ← Redis HSET + PUBLISH
```

---

## Step 2: MatchingEngine.dispatch_event() — Routing

**File:** `services/matching_engine.py:1404-1420`

```
dispatch_event(event)
  │
  │  event = {"event_type": "book", "asset_id": "0xabc...", "bids": [...], "asks": [...]}
  │
  ├─ "book"            → _handle_book(asset_id, event)
  ├─ "price_change"    → _handle_price_change(asset_id, event)
  ├─ "best_bid_ask"    → _handle_best_bid_ask(asset_id, event)
  ├─ "last_trade_price"→ _handle_last_trade(asset_id, event)
  └─ "market_resolved" → _handle_market_resolved(asset_id)
```

### 2.1 get_book(asset_id)

```python
# matching_engine.py:1355-1370
def get_book(self, token_id: str) -> ShadowOrderbook | None:
    with self._lock:                    # ← MatchingEngine-level lock
        if token_id not in self._books:
            if token_id not in self._valid_tokens:
                return None             # reject unknown token
            self._books[token_id] = ShadowOrderbook(token_id)
        return self._books[token_id]
```

- Mỗi `token_id` (asset) có 1 `ShadowOrderbook` riêng
- Book được tạo lazy khi event đầu tiên đến
- `_valid_tokens` whitelist token_id hợp lệ (từ TokenRegistry)

---

## Step 3: ShadowOrderbook — Cập nhật orderbook

**File:** `services/matching_engine.py:330-410`

### 3.1 Book event (full snapshot)

```
_handle_book(asset_id, event)
  └─→ book.apply_snapshot(bids, asks)
        │  LOCK: book._lock
        ├─→ self.bids.clear()                  ← SortedDict
        ├─→ self.asks.clear()                  ← SortedDict
        ├─→ for entry in bids:
        │     self.bids[Decimal(price)] = Decimal(size)
        ├─→ for entry in asks:
        │     self.asks[Decimal(price)] = Decimal(size)
        ├─→ self._raw_bids = copy (Polymarket state trước shadow deduction)
        ├─→ self._raw_asks = copy
        └─→ self.last_update = now()
```

**Data structure:**
```
self.bids = SortedDict({
    Decimal("0.40"): Decimal("500.0"),   # 500 shares @ $0.40
    Decimal("0.41"): Decimal("300.0"),   # 300 shares @ $0.41
    ...                                   # ascending by price
})
self.asks = SortedDict({
    Decimal("0.55"): Decimal("200.0"),   # 200 shares @ $0.55
    Decimal("0.56"): Decimal("150.0"),   # 150 shares @ $0.56
    ...
})
```

### 3.2 Price change event (delta)

```
_handle_price_change(asset_id, event)
  └─→ book.apply_changes(changes)
        │  LOCK: book._lock
        └─→ for ch in changes:
              side = ch["side"]           # "bid" | "ask"
              price = Decimal(ch["price"])
              size = Decimal(ch["size"])
              if size <= 0:
                  del target[price]       ← xoá level
              else:
                  target[price] = size    ← upsert level
```

### 3.3 Best bid/ask event (lightweight)

```
_handle_best_bid_ask(asset_id, event)
  │
  │  Nếu có bid_size/ask_size → apply_changes() bình thường
  │  Nếu THIẾU size → infer từ book hiện tại:
  │    - new_bid > current best_bid → dùng best_bid_size
  │    - new_ask < current best_ask → dùng best_ask_size
  │
  └─→ Luôn refresh book.last_update (chống stale)
```

---

## Step 4: run_matching() — Khớp lệnh

**File:** `services/matching_engine.py:769-819`

### 4.1 Trigger

`run_matching()` được gọi SAU MỖI event cập nhật orderbook:

```
_handle_book()       → book.run_matching()
_handle_price_change → book.run_matching()
_handle_best_bid_ask → book.run_matching()
place_virtual_order  → book.run_matching()   ← khi đặt lệnh mới
```

### 4.2 Flow

```
run_matching()
  │  LOCK: book._lock (giữ suốt quá trình)
  │
  ├─ 1. _expire_pending_orders()              ← check TTL, cancel hết hạn
  │     for order in _virtual_orders:
  │       if order.expire_at <= now:
  │         order.status = CANCELED
  │
  ├─ 2. Stale guard
  │     if book._expired or book.is_stale(120s):
  │       SKIP matching → log warning → return
  │
  ├─ 3. Iterate all active orders
  │     for order in _virtual_orders:
  │       if order.status in (FILLED, CANCELED): continue
  │       _match_order(order)                   ← core matching
  │
  │       # MARKET IOC: cancel unfilled remainder
  │       if order.order_type == "MARKET" and status != FILLED:
  │         if had_liquidity or filled > 0:
  │           order.status = CANCELED
  │
  ├─ 4. collect_state_changes()                ← detect FILL/CANCEL deltas
  │     for order in _virtual_orders:
  │       if order.filled > last_reported.filled:
  │         emit FILL event
  │       if order.status == CANCELED and was != CANCELED:
  │         emit CANCEL event
  │
  ├─ 5. _prune_terminal_orders()               ← cleanup mỗi 50 calls
  │     remove FILLED/CANCELED từ _virtual_orders
  │
  │  UNLOCK
  │
  └─ 6. _fire_state_change_callbacks(events)   ← callback NGOÀI lock
        for cb in _state_change_callbacks:
          cb(events)                            ← OrderConsumer._on_state_changes()
```

### 4.3 _match_order() — Core matching algorithm

**File:** `services/matching_engine.py:912-1081`

#### BUY order (walk asks ascending):

```
_match_order(order)   # order.side == BUY
  │
  │  # Slippage reference (lock in at first match)
  │  if MARKET and _slippage_ref_price is None:
  │    _slippage_ref_price = asks.peekitem(0)[0]   ← best ask
  │    slippage_limit = ref * (1 + slippage_pct)    ← max price willing to pay
  │
  │  for ask_price in self.asks.keys():   ← ascending order
  │    │
  │    ├─ MARKET: if ask_price > slippage_limit → STOP
  │    ├─ LIMIT:  if order.price < ask_price → STOP (no more matches)
  │    │
  │    ├─ match_qty = min(order.remaining_qty, ask_size)
  │    │
  │    ├─ # Cost cap check (MARKET only)
  │    │  if max_cost set:
  │    │    budget_remaining = max_cost - _entry_cost
  │    │    affordable_qty = budget_remaining / ask_price
  │    │    match_qty = min(match_qty, affordable_qty)
  │    │
  │    ├─ if match_qty < dust_threshold → STOP
  │    │
  │    ├─ # Execute fill
  │    │  order.filled += match_qty
  │    │  order._entry_cost += match_qty * ask_price
  │    │  order._fill_levels.append((ask_price, match_qty))
  │    │  self.asks[ask_price] -= match_qty
  │    │  if asks[ask_price] < dust → del asks[ask_price]
  │    │
  │    ├─ order._update_status()   ← PENDING→PARTIAL or PARTIAL→FILLED
  │    │
  │    └─ if FILLED → break
```

#### SELL order (walk bids descending): tương tự nhưng ngược hướng.

### 4.4 Ví dụ cụ thể — LIMIT BUY

```
Orderbook asks:
  $0.50 × 100 shares
  $0.52 × 200 shares
  $0.55 × 300 shares

LIMIT BUY order: price=0.53, quantity=250 shares

Step 1: ask=$0.50 ≤ limit=$0.53 → match 100 @ $0.50
  filled=100, cost=$50, asks[$0.50] deleted

Step 2: ask=$0.52 ≤ limit=$0.53 → match 150 @ $0.52 (remaining=150)
  filled=250, cost=$50+$78=$128, asks[$0.52]=50 remaining

Step 3: FILLED (250/250)
  avg_entry_price = $128/250 = $0.512
  _fill_levels = [($0.50, 100), ($0.52, 150)]
```

### 4.5 Ví dụ — MARKET BUY with slippage

```
Orderbook asks:
  $0.50 × 100 shares    ← ref price
  $0.55 × 200 shares
  $0.60 × 300 shares

MARKET BUY: quantity=500, slippage=10%
  slippage_limit = $0.50 × 1.10 = $0.55

Step 1: $0.50 ≤ $0.55 → match 100 @ $0.50
Step 2: $0.55 ≤ $0.55 → match 200 @ $0.55
Step 3: $0.60 > $0.55 → STOP (slippage exceeded)

Result: filled=300/500 → PARTIAL → MARKET IOC → CANCELED
  avg_entry_price = ($50 + $110) / 300 = $0.5333
  Unfilled 200 shares → cancel event published
```

---

## Step 5: Order Consumer — Luồng lệnh LIMIT

**File:** `ws_feed_service/order_consumer.py`

### 5.1 Flow tổng quan

```
FastAPI POST /binary-options/
  ├─→ Validate bot, balance, price
  ├─→ Save BinaryOption to DB (status=PENDING)
  ├─→ Deduct amount from bot.balance
  └─→ redis.lpush("queue:orders:new", JSON payload)

     ┌──────────────────────────────────────────┐
     │  OrderConsumer (daemon thread)            │
     │                                          │
     │  while running:                          │
     │    raw = redis.brpop("queue:orders:new") │ ← blocking pop, 1s timeout
     │    _process_order(raw)                   │
     └──────────────────────────────────────────┘
```

### 5.2 _process_order() — Parse + route

```
_process_order(raw_json)
  │
  ├─ Parse JSON → data dict
  │  {
  │    "bo_id": 123,
  │    "token_id": "0xabc...",
  │    "side": "BUY",
  │    "price": 0.50,
  │    "quantity": 100.0,
  │    "limit_price": 0.45,      ← None = MARKET, set = LIMIT
  │    "tp_price": null,
  │    "sl_price": null,
  │    "ttl": 300,
  │    "slippage_tolerance": 0.10,
  │    "settlement_at": "..."
  │  }
  │
  ├─ if token_id is None → publish cancel("NO_TOKEN_ID") → return
  │
  ├─ engine.add_valid_token(token_id)
  │
  ├─ if prefilled → _process_prefilled_order()   ← recovery case
  └─ else         → _process_standard_order()    ← normal case
```

### 5.3 _process_standard_order() — Chi tiết

```
_process_standard_order(data, bo_id, token_id, on_bracket_exit)
  │
  │  # 1. TTL calculation
  │  if ttl is not None:
  │    ttl_seconds = float(ttl)                  ← từ payload
  │  elif session_offset == 1 and settlement_at:
  │    ttl_seconds = settlement_at - now         ← tính từ settlement
  │  else:
  │    ttl_seconds = None                        ← no expiry
  │
  │  # 2. MARKET qty adjustment
  │  if limit_price is None (MARKET):
  │    best_ask = engine.best_ask(token_id)
  │    quantity = amount / best_ask              ← convert $ → shares
  │
  │  # 3. Cost cap (MARKET only)
  │  cost_cap = Decimal(str(amount)) if is_market else None
  │
  │  # 4. Place order in matching engine
  │  order, bracket_results = engine.place_virtual_order(
  │    token_id, side, price, quantity,
  │    tp_price, sl_price, timeframe,
  │    ttl_seconds, on_bracket_exit,
  │    order_type="MARKET" | "LIMIT",
  │    max_slippage, max_cost=cost_cap,
  │  )
  │
  │  # 5. IMMEDIATE fill publish (before callbacks)
  │  if bo_id and order.filled > 0:
  │    writer.publish_order_fill(bo_id, order_id, filled, avg_price, status)
  │
  │  # 6. Register for centralized monitoring
  │  _order_to_bo[order.order_id] = bo_id
  │  _order_to_token[order.order_id] = token_id
  │  book.seed_last_reported(order_id, filled, status)  ← prevent duplicate fill
  │  if token_id not in _registered_books:
  │    book.register_state_change_callback(_on_state_changes)
  │    _registered_books.add(token_id)
  │
  │  # 7. Handle immediate MARKET IOC cancel
  │  if order.status == CANCELED:
  │    writer.publish_order_cancel(bo_id, order_id, "MARKET_IOC_CANCEL", filled, avg)
  │
  │  # 8. Fire bracket exit results (if TP/SL violated at entry)
  │  for result in bracket_results:
  │    on_bracket_exit(result)
  │    writer.publish_bracket_exit(...)
```

---

## Step 6: LIMIT Order Lifecycle — Full Example

```
Timeline:
  t=0    API nhận lệnh LIMIT BUY $0.45 × 200 shares, TTL=300s
  t=0    Save DB: bo.status=PENDING, deduct bot.balance
  t=0    redis.lpush("queue:orders:new", payload)

  t=0.1  OrderConsumer.brpop() → _process_standard_order()
         engine.place_virtual_order(token_id, BUY, $0.45, 200, ttl=300)
           │
           │  ShadowOrderbook._match_order():
           │    asks = {$0.50: 100, $0.55: 200}
           │    $0.50 > limit $0.45 → STOP (no match)
           │
           │  order.status = PENDING (not filled)
           │  order.expire_at = now + 300s
           │
         No fill → no fill event published
         Register _order_to_bo, callback

  t=5    Polymarket WS: price_change → asks = {$0.44: 50, $0.50: 100}
         book.apply_changes() → run_matching()
           │
           │  _match_order(limit_order):
           │    $0.44 ≤ limit $0.45 → match 50 @ $0.44
           │    $0.50 > limit $0.45 → STOP
           │    status = PARTIAL (50/200)
           │
         collect_state_changes() → FILL event
         _on_state_changes() → writer.publish_order_fill(
           bo_id=123, filled=50, avg=$0.44, status="PARTIAL"
         )
         → Redis stream: stream:order:fills

  t=5    FastAPI _consume_order_fills() → XREADGROUP
         → DB update: bo.avg_price=$0.44, bo.num_shares=50, bo.me_order_status="PARTIAL"

  t=30   Polymarket WS: price_change → asks = {$0.43: 180, $0.55: 200}
         run_matching():
           │  $0.43 ≤ $0.45 → match 150 @ $0.43 (remaining=150)
           │  status = FILLED (200/200)
           │
         collect_state_changes() → FILL event
         _on_state_changes() → writer.publish_order_fill(
           bo_id=123, filled=200, avg=$0.434, status="FILLED"
         )

  t=30   FastAPI → DB update: bo.avg_price=$0.434, bo.num_shares=200, status="FILLED"

  t=300  Settlement: scheduler fetches Binance candle
         compare forecast vs candle direction → WIN/LOSS
         update bo.result, bo.profit, bot.balance
```

---

## Step 7: LIMIT Order TTL Expiry

```
  t=0     LIMIT BUY $0.45 × 200, TTL=60s placed
  t=0     order.expire_at = now + 60s

  t=5     price_change → run_matching() → no match (asks too high)
  t=10    price_change → run_matching() → no match
  ...

  t=60    Expiry tick (_expiry_tick runs every 5s)
          engine.expire_all_pending()
            └─→ book._expire_pending_orders()
                  order.expire_at <= now → order.status = CANCELED

  t=60    run_matching() → collect_state_changes() → CANCEL event
          _on_state_changes() → writer.publish_order_cancel(
            bo_id=123, reason="TTL_EXPIRED", filled=0, avg=0
          )

  t=60    FastAPI _consume_order_cancels():
          filled=0 → bo.result=CANCELLED, refund amount to bot.balance
```

---

## Step 8: LIMIT Order Partial Fill + TTL Expiry

```
  t=0     LIMIT BUY $0.45 × 200, TTL=60s

  t=10    price_change → match 80 @ $0.44
          FILL event: filled=80, status=PARTIAL

  t=60    TTL expires → order.status = CANCELED
          CANCEL event: filled=80, avg=$0.44, reason="TTL_EXPIRED"

  t=60    FastAPI _consume_order_cancels():
          filled=80 > 0 → PARTIAL FILL EXPIRY:
            actual_cost = 80 × $0.44 = $35.20
            unfilled_refund = amount - actual_cost
            bo.avg_price = $0.44
            bo.num_shares = 80
            bo.amount = $35.20 (chỉ phần đã fill)
            bot.balance += unfilled_refund
            bo stays PENDING → settlement sẽ resolve WIN/LOSS
```

---

## Step 9: Bracket Order Monitoring (TP/SL)

**File:** `services/matching_engine.py:1085-1236`

```
Sau MỖI event cập nhật book:
  _handle_book()        → monitor_bracket_orders()
  _handle_price_change  → monitor_bracket_orders()
  _handle_best_bid_ask  → monitor_bracket_orders()

monitor_bracket_orders()
  │  LOCK: book._lock
  │
  │  current_best_bid = bids.peekitem(-1)[0]   ← best bid (highest)
  │
  │  for order in _virtual_orders:
  │    if not order.is_eligible_for_bracket:
  │      continue
  │    │
  │    │  Eligibility: side==BUY, has TP/SL, filled>0,
  │    │               not position_closed,
  │    │               status in (FILLED, PARTIAL, CANCELED)
  │    │
  │    ├─ TP check: best_bid >= tp_price
  │    │    → _execute_bracket_exit(order, best_bid, "TP")
  │    │
  │    └─ SL check: best_bid <= sl_price
  │         → _execute_bracket_exit(order, best_bid, "SL")
  │
  │  UNLOCK
  │
  │  Fire bracket callbacks (outside lock)
  │    → writer.publish_bracket_exit(bo_id, trigger, exit_price, exit_filled)
  │    → Redis stream: stream:bracket:exits
```

### 9.1 _execute_bracket_exit() — SELL against bids

```
_execute_bracket_exit(order, market_bid, trigger="TP")
  │  LOCK already held
  │
  │  qty_to_close = order.filled - already_exited
  │
  │  for bid_price in reversed(bids.keys()):  ← descending (best first)
  │    fill_qty = min(remaining, bid_size)
  │    qty_exited += fill_qty
  │    total_value += fill_qty × bid_price
  │    bids[bid_price] -= fill_qty
  │    if bids[price] < dust → delete
  │    if qty_exited >= qty_to_close → break
  │
  │  avg_exit = total_value / qty_exited
  │  order.position_closed = (total_exited >= order.filled)
  │  order.exit_price = avg_exit
  │  order.exit_trigger = trigger
  │  order.exit_filled = total_exited
  │
  │  return BracketFillResult(...)
```

---

## Step 10: State Machine

```
                    ┌──────────┐
                    │ PENDING  │
                    └────┬─────┘
                         │
              ┌──────────┼──────────┐
              │          │          │
         fill partial  fill all   TTL/IOC
              │          │          │
              ▼          ▼          ▼
         ┌────────┐ ┌────────┐ ┌──────────┐
         │PARTIAL │ │ FILLED │ │ CANCELED │
         └───┬────┘ └────┬───┘ └──────────┘
             │           │
        ┌────┼────┐      │  has TP/SL + filled>0
        │    │    │      │
   fill all TTL  IOC  bracket
        │    │    │   monitoring
        ▼    ▼    ▼      │
   FILLED CANCELED    TP/SL trigger
                         │
                         ▼
                  _execute_bracket_exit()
                         │
                         ▼
                  position_closed=true
```

---

## Step 11: Threading & Locking

### Locks

| Lock | Scope | Protects |
|------|-------|----------|
| `MatchingEngine._lock` | Global | `_books` dict (book creation/lookup) |
| `ShadowOrderbook._lock` | Per book | bids, asks, _virtual_orders, matching |

### Thread model

```
Thread 1: asyncio event loop (PolymarketFeed + RedisWriter)
  ├─→ dispatch_event() → acquire book._lock → match → release
  └─→ monitor_bracket_orders() → acquire book._lock → check → release

Thread 2: OrderConsumer daemon thread
  ├─→ place_virtual_order() → acquire book._lock → match → release
  └─→ _publish_async() → asyncio.run_coroutine_threadsafe() → RedisWriter

Thread 3: FastAPI (uvicorn workers)
  └─→ XREADGROUP consumers → DB updates
```

### Tại sao không deadlock?

- Book lock là per-token (mỗi book riêng biệt)
- Callbacks (state change, bracket exit) fire NGOÀI lock
- Redis I/O không giữ lock
- `run_coroutine_threadsafe()` bridge thread→async không block

---

## Step 12: Recovery khi restart

**File:** `ws_feed_service/main.py:40-168`

```
Startup:
  1. Connect Redis
  2. Init MatchingEngine + RedisWriter
  3. TokenRegistry discover token_ids
  4. Register tokens in RedisWriter

  5. _recover_pending_orders():
     │  Query DB: BinaryOption.result == PENDING
     │            AND exit_trigger IS NULL
     │
     │  for each bo:
     │    ├─ Recalculate TTL = original_ttl - elapsed_time
     │    │  if ttl <= 0 → skip (settlement will handle)
     │    │
     │    ├─ Lookup token_id from registry
     │    │  if no token → skip
     │    │
     │    ├─ if already filled (has avg_price, num_shares) AND has bracket:
     │    │    payload["prefilled"] = True   ← register for TP/SL monitoring only
     │    │
     │    └─ redis.lpush("queue:orders:new", payload)
     │       → OrderConsumer picks up → re-places in matching engine

  6. Start PolymarketFeed
  7. Start OrderConsumer daemon thread
  8. Start expiry tick (every 5s)
```

---

## Appendix: Key Constants

| Constant | Value | Source | Purpose |
|----------|-------|--------|---------|
| `ME_BOOK_STALE_MAX_S` | 120s | config/timing.py | Skip matching if book older than this |
| `ME_DUST_THRESHOLD` | 0.000001 | config/timing.py | Min fill qty |
| `ME_DEFAULT_SLIPPAGE` | 0.10 (10%) | config/timing.py | Default MARKET slippage |
| `ME_CLEANUP_INTERVAL` | 50 | config/timing.py | Prune terminal orders every N calls |
| `BRPOP_TIMEOUT_S` | 1s | config/timing.py | OrderConsumer poll interval |
| `PRICE_CACHE_TTL_S` | 120s | ws_feed_service/config.py | Redis price key TTL |
| `STREAM_MAXLEN` | 10000 | ws_feed_service/config.py | Max stream length |
| `_RECONNECT_MAX` | 10s | services/ws_feed.py | Max WS reconnect delay |

## Appendix: Redis Keys & Streams

| Key/Stream | Format | TTL | Producer | Consumer |
|------------|--------|-----|----------|----------|
| `price:{SYM}:{TF}:{DIR}` | Hash | 120s | RedisWriter | FastAPI /prices |
| `orderbook:{SYM}:{TF}:{DIR}` | Hash | 120s | RedisWriter | SnapshotCache |
| `queue:orders:new` | List | — | FastAPI | OrderConsumer |
| `stream:order:fills` | Stream | ~10K entries | OrderConsumer | FastAPI |
| `stream:order:cancels` | Stream | ~10K entries | OrderConsumer | FastAPI |
| `stream:bracket:exits` | Stream | ~10K entries | OrderConsumer | FastAPI |
| `stream:market:resolved` | Stream | ~10K entries | MatchingEngine | FastAPI |
| `orderbook:updates` | Pub/Sub | — | RedisWriter | Broadcaster |
