# Order Fill Logic — Snapshot-Based Matching

How an order flows from the API to the matching engine, gets filled against
a Polymarket book snapshot, and publishes results back via Redis streams.

---

## End-to-End Flow

```
[API Process]
  POST /poly-arena/binary-options/
    ├── Create BinaryOption in DB (PENDING)
    └── LPUSH order JSON → "queue:orders:BTC:M5:1709313000"

[WS Feed Service — OrderConsumer thread]
  BRPOP "queue:orders:*" → raw JSON
    ├── Parse JSON, validate token_id, resolve session
    ├── _process_standard_order()
    │     ├── Compute TTL, recalculate MARKET qty from fresh best_ask
    │     └── session.place_virtual_order() → book.place_virtual_order()
    │           ├── [book._lock acquired]
    │           ├── Create SimulatedOrder, append to _virtual_orders
    │           ├── _match_order(): sweep asks ascending
    │           │     ├── For each ask_price ≤ limit_price (or within slippage):
    │           │     │     fill += min(remaining_qty, ask_size)
    │           │     │     deduct from shadow asks
    │           │     └── Update status: PENDING → PARTIAL → FILLED
    │           ├── MARKET IOC: cancel unfilled remainder
    │           ├── Immediate bracket check (TP/SL vs best_bid)
    │           ├── collect_state_changes()
    │           └── [book._lock released]
    │           └── Fire state-change callbacks
    ├── Publish fill → stream:order:fills
    ├── Register for centralized monitoring
    └── Fire bracket exit callbacks → stream:bracket:exits

[WS Feed Service — WS event dispatch]
  Polymarket WS → {type: "book", asset_id, bids, asks}
    ├── SessionManager.dispatch_event()
    │     └── SessionEngine.dispatch_ws_event()
    │           ├── book.apply_snapshot(bids, asks)
    │           ├── book.run_matching()     ← re-match ALL active orders
    │           └── book.monitor_bracket_orders()

[API Process — Redis stream consumers]
  XREADGROUP stream:order:fills   → update DB (avg_price, num_shares, status)
  XREADGROUP stream:order:cancels → update DB (status=CANCELLED, refund balance)
  XREADGROUP stream:bracket:exits → update DB (exit_price, exit_trigger, P&L)
```

---

## Step 1: API Creates Order → LPUSH to Per-Session Queue

**File:** `routers/binary_options.py`

Two paths push orders to the queue:

### Path A — LIMIT order that cannot fill from REST

When `best_ask > limit_price`, the order cannot fill immediately.

1. Create `BinaryOption` DB record: `avg_price=None`, `num_shares=None`,
   `me_order_status="PENDING"`.
2. Build JSON payload:
   ```json
   {
     "bo_id": 42,
     "token_id": "0xabc...",
     "side": "BUY",
     "price": 0.45,
     "quantity": 22.22,
     "amount": 10.0,
     "limit_price": 0.45,
     "tp_price": null,
     "sl_price": null,
     "timeframe": "M5",
     "ttl": 300,
     "session_offset": 0,
     "settlement_at": "2024-03-01T12:15:00+00:00",
     "session_id": "BTC:M5:1709313000"
   }
   ```
3. Compute queue key: `f"queue:orders:{session_id}"`.
4. `sr.lpush(session_queue_key, order_payload)` — pushes to LEFT of list.

### Path B — Pre-filled MARKET with bracket (TP/SL)

When a MARKET order was already filled via Polymarket REST but needs TP/SL
monitoring in the matching engine.

1. Payload includes `"prefilled": True`, `"prefilled_avg_price"`, `"prefilled_filled"`.
2. Same `sr.lpush(session_queue_key, order_payload)`.

### Key decisions

- `session_id` = `"{SYM}:{TF}:{candle_open_ts}"` routes to the correct session.
- MARKET orders without brackets are NOT queued (fully handled via REST).
- LIMIT orders always go through the queue.

---

## Step 2: OrderConsumer — Multi-Key BRPOP Loop

**File:** `ws_feed_service/order_consumer.py`

### `_run()` — Main loop (daemon thread)

```python
def _run(self):
    while self._running:
        keys = self._session_manager.active_queue_keys()
        if not keys:
            time.sleep(BRPOP_TIMEOUT_S)
            continue
        result = self._r.brpop(keys, timeout=BRPOP_TIMEOUT_S)
        if result is None:
            continue
        queue_key, raw = result
        session_id = queue_key.split(":", 2)[2]  # "queue:orders:BTC:M5:..." → "BTC:M5:..."
        self._process_order(raw, session_id)
```

- `active_queue_keys()` returns queue keys for ALL non-ARCHIVED sessions.
- `BRPOP` blocks until an item arrives on ANY queue (FIFO — pops from RIGHT).
- Single consumer thread: order processing is serialized.

### `_process_order()` — Routing

1. Parse JSON.
2. Validate `token_id` present — reject with `NO_TOKEN_ID` cancel if missing.
3. Resolve session via `session_manager.get_session(session_id)` — reject with
   `SESSION_NOT_FOUND` cancel if missing.
4. If payload has `tp_price` or `sl_price`, create bracket exit callback.
5. Route to `_process_prefilled_order()` or `_process_standard_order()`.

---

## Step 3: Standard Order Processing

**File:** `ws_feed_service/order_consumer.py` — `_process_standard_order()`

### 3a. Parameter computation

| Parameter | Logic |
|-----------|-------|
| `is_market` | `True` if `limit_price is None` |
| `ttl_seconds` | User TTL clamped for future sessions; auto-computed if `session_offset >= 1` |
| `quantity` (MARKET) | Recalculated as `amount / best_ask` from session manager's fresh price |
| `cost_cap` (MARKET BUY) | Set to `amount` to cap cumulative fill cost |

### 3b. Delegation chain

```
OrderConsumer._process_standard_order()
  └── session.place_virtual_order(token_id, side, price, quantity, ...)
        └── SessionEngine.place_virtual_order()                  # session_engine.py
              ├── State check: reject if SETTLING or ARCHIVED
              ├── Book lookup: _token_to_dir[token_id] → direction → books[direction]
              ├── Expired check: reject if book._expired
              └── book.place_virtual_order(side, price, qty, ...)  # matching_engine.py
```

### 3c. Post-placement (back in OrderConsumer)

1. **Publish fill** (if `order.filled > 0`): `publish_order_fill()` to `stream:order:fills`
   with `bo_id`, `filled`, `avg_entry_price`, `walk_prices`.
2. **Register monitoring**: `_order_to_bo[order_id] = bo_id`, seed `last_reported`,
   register `_on_state_changes` callback on the book.
3. **MARKET IOC cancel**: If order was CANCELED during placement (no/partial fill),
   publish explicit cancel event.
4. **Fire bracket exits**: Iterate `bracket_results` and fire callbacks (fill was
   published first so the DB has `avg_price`/`num_shares` before any bracket exit).

---

## Step 4: ShadowOrderbook — The Core Matching

**File:** `services/matching_engine.py`

### `place_virtual_order()` — Entry point

1. **Compute `expire_at`**: Priority: `ttl_seconds` > `timeframe` (candle-aligned) > `None`.
2. **Create `SimulatedOrder`**: UUID, `status=PENDING`, `filled=0`.
3. **Lock and match**:
   ```python
   with self._lock:
       self._virtual_orders.append(order)
       had_liquidity = bool(self.asks)  # for BUY
       self._match_order(order)
   ```
4. **MARKET IOC semantics**: If MARKET and not FILLED:
   - Book had liquidity or partial fill → cancel remainder.
   - Book empty (WS not arrived yet) → leave PENDING for `run_matching()` retry.
5. **Immediate bracket check**: If filled > 0 and has TP/SL:
   - `best_bid >= tp_price` → execute TP exit.
   - `best_bid <= sl_price` → execute SL exit.
6. **Fire state-change callbacks** outside lock.

### `_match_order()` — Core algorithm (BUY side)

```
for ask_price in sorted(self.asks.keys()):   # ascending
    ├── MARKET: stop if ask_price > ref_price * (1 + slippage)
    ├── LIMIT:  stop if ask_price > order.price
    │
    ├── match_qty = min(remaining_qty, ask_size)
    │
    ├── Cost cap (MARKET BUY):
    │     budget_remaining = max_cost - _entry_cost
    │     if budget exhausted: stop
    │     cap match_qty to budget_remaining / ask_price
    │
    ├── Dust check: if match_qty < 1e-10: stop
    │
    └── Execute fill:
          order.filled      += match_qty
          order._entry_cost += match_qty * ask_price
          order._fill_levels.append((ask_price, match_qty))
          self.asks[ask_price] -= match_qty     ← shadow liquidity deduction
          if asks[ask_price] < dust: del asks[ask_price]
          order._update_status()                ← PENDING → PARTIAL → FILLED
```

**Key details:**
- Fills are against the **shadow** orderbook — the real Polymarket book is unaffected.
- `_fill_levels` accumulates `[(price, qty), ...]` for walk-price reporting.
- `avg_entry_price = _entry_cost / filled` (weighted average across all levels).
- `_raw_bids` / `_raw_asks` store the pre-deduction Polymarket data.
- Slippage reference is locked at first fill: `_slippage_ref_price = best_ask` (prevents
  cascading slippage if the book shifts mid-match).

---

## Step 5: Book Snapshots Feed Into Matching

### Event routing

```
Polymarket WS → PolymarketFeed._dispatch(event)
  └── session_manager.dispatch_event(event)        # patched to also write Redis
        └── For each session owning asset_id:
              session.dispatch_ws_event(event)
```

### `book` event — full snapshot

**`ShadowOrderbook.apply_snapshot()`:**
```python
with self._lock:
    self.bids.clear()
    self.asks.clear()
    for entry in bids:
        self.bids[Decimal(price)] = Decimal(size)
    for entry in asks:
        self.asks[Decimal(price)] = Decimal(size)
    self._raw_bids = SortedDict(self.bids)   # save pre-deduction copy
    self._raw_asks = SortedDict(self.asks)
    self.last_update = now
```

A full snapshot **replaces** the entire book. Previous shadow deductions are lost.
After snapshot, `run_matching()` re-attempts matching for ALL active orders against
the fresh data.

### `price_change` event — delta update

**`ShadowOrderbook.apply_changes()`:**
```python
with self._lock:
    for ch in changes:
        target = self.bids if ch["side"] == "bid" else self.asks
        if size <= 0:
            target.pop(price, None)
        else:
            target[price] = size
```

Incremental updates preserve existing shadow deductions on unaffected price levels.

### `best_bid_ask` event — top-of-book spread

The most frequent event. Handled by `SessionEngine._apply_best_bid_ask()`:

1. If `bid_size` is present → directly add as a `price_change`.
2. If `bid_size` is missing but `bid > current_best_bid` → infer size from
   current best bid's size (conservative estimate).
3. Same logic for ask side (infer from current best ask).
4. If no changes: just touch `last_update` (prevents staleness).
5. Always triggers `monitor_bracket_orders()`.

### `last_trade_price` event

Records trade and triggers bracket monitoring. Does NOT trigger `run_matching()` —
only bracket TP/SL checks run on trade events.

---

## Step 6: Re-Matching on Book Updates

**`ShadowOrderbook.run_matching()`:**

```python
with self._lock:
    self._expire_pending_orders()              # TTL check
    if self._expired or self.is_stale():       # guard: skip if stale
        return
    for order in self._virtual_orders:
        if order.status in (FILLED, CANCELED):
            continue
        self._match_order(order)               # re-attempt matching
        # MARKET IOC cancel logic...
    state_events = self.collect_state_changes()

# Outside lock:
self._fire_state_change_callbacks(state_events)
```

**This is how LIMIT orders eventually fill:** A LIMIT order placed when
`best_ask = $0.55` with `limit_price = $0.45` starts as PENDING. When a later
`book` or `price_change` event drops the best ask to $0.44, `run_matching()`
re-iterates the order and `_match_order()` fills it against the new ask.

**Staleness guard:** If `last_update` is older than `ME_BOOK_STALE_MAX_S`,
matching is skipped entirely — prevents fills against stale data.

---

## Step 7: Bracket Monitoring (TP/SL)

**`ShadowOrderbook.monitor_bracket_orders()`:**

Called after every book update and trade event.

```python
with self._lock:
    best_bid = self.bids.peekitem(-1)[0]     # highest bid
    for order in self._virtual_orders:
        if not order.is_eligible_for_bracket:
            continue
        if order.tp_price and best_bid >= order.tp_price:
            result = self._execute_bracket_exit(order, best_bid, "TP")
        elif order.sl_price and best_bid <= order.sl_price:
            result = self._execute_bracket_exit(order, best_bid, "SL")

# Callbacks fired outside lock
```

**`_execute_bracket_exit()`** simulates a SELL by walking bids descending:
- Consumes shadow bid liquidity across multiple levels.
- Records `exit_price` (weighted avg), `exit_filled`, `levels_consumed`.
- Sets `order.position_closed = True`.

---

## Thread Safety Model

| Lock | Protects | Held during |
|------|----------|-------------|
| `ShadowOrderbook._lock` | `bids`, `asks`, `_virtual_orders` | `apply_snapshot`, `apply_changes`, `run_matching`, `place_virtual_order`, `monitor_bracket_orders` |
| `SessionEngine._lock` | Lifecycle `state` transitions | `transition()` only |
| `SessionManager._lock` | `_engines`, `_token_index` | `create_session`, `get_session`, `transition_session` |

**Critical pattern:** All callbacks (bracket exits, state-change notifications) are
**collected while holding** the book lock but **fired outside** it. This prevents
deadlocks from callbacks that acquire other locks (e.g., Redis publish).

**Concurrency paths:**
- OrderConsumer thread: `place_virtual_order()` acquires `book._lock`.
- WS dispatch (main async loop): `run_matching()` acquires `book._lock`.
- Expiry tick (async loop): `expire_all_pending()` acquires `book._lock`.
- All three are serialized by `book._lock` — no data races.

---

## Error Handling

| Error | Handler | Effect |
|-------|---------|--------|
| Session not found | OrderConsumer publishes cancel | `SESSION_NOT_FOUND` → API marks CANCELLED |
| Token not in session | `ValueError` from `SessionEngine` | `TOKEN_ROTATED` cancel |
| Book expired | `ValueError` from `ShadowOrderbook` | `TOKEN_ROTATED` cancel |
| MARKET no fill (IOC) | `_process_standard_order` | Explicit cancel published |
| Stale book | `run_matching()` skips | Order stays PENDING until fresh data |
| TTL expired | `_expire_pending_orders()` | `TTL_EXPIRED` cancel via state-change callback |
