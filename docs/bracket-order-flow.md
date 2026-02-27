# Bracket Order (TP/SL) — Luồng xử lý chi tiết

## Tổng quan

Bracket Order là lệnh có kèm Take Profit (TP) và/hoặc Stop Loss (SL). Hệ thống hỗ trợ 2 loại:

| Order Type | Bracket | Cách xử lý |
|------------|---------|-------------|
| **MARKET + TP/SL** | Fill ngay qua REST Polymarket → đăng ký ME monitor TP/SL | ME chỉ theo dõi giá, không match |
| **LIMIT + TP/SL** | Đẩy vào ME queue → ME match + monitor TP/SL | ME vừa match vừa theo dõi |

---

## Luồng 1: MARKET order có TP/SL

### Phase 1 — Order Creation (`routers/binary_options.py`)

```
Bot → POST /poly-arena/binary-options/
      {symbol, timeframe, forecast, amount, tp_price, sl_price}
```

1. Xác thực bot via `x-api-key`, trừ `amount` khỏi balance
2. Fill ngay qua REST Polymarket CLOB: walk asks → tính `avg_price`, `num_shares`
3. Lưu DB: `avg_price=avg_price`, `num_shares=num_shares`, `me_order_status="PREFILLED"`
4. Push vào Redis queue `queue:orders:new`:
   ```json
   {
     "bo_id": 123,
     "token_id": "0xabc...",
     "prefilled": true,
     "prefilled_avg_price": 0.52,
     "prefilled_filled": 19.23,
     "tp_price": 0.60,
     "sl_price": 0.45,
     "timeframe": "M5"
   }
   ```

### Phase 2 — ME Registration (`ws_feed_service/order_consumer.py`)

`OrderConsumer._process_prefilled_order()`:

1. Gọi `engine.place_prefilled_bracket_order()` — inject order dạng FILLED, không match
2. ME kiểm tra TP/SL ngay lập tức với `best_bid` hiện tại:
   - `best_bid >= tp_price` → TP fires ngay
   - `best_bid <= sl_price` → SL fires ngay
3. Đăng ký `state_change_callback` trên book (dùng cho event-driven monitoring)
4. `seed_last_reported()` để tránh duplicate fill event

### Phase 3 — Realtime Monitoring (`services/matching_engine.py`)

ME theo dõi TP/SL qua **4 loại WS event** từ Polymarket:

| WS Event | Handler | Trigger bracket check? |
|----------|---------|----------------------|
| `book` (full snapshot) | `apply_snapshot()` → `run_matching()` | Yes |
| `price_change` (delta) | `apply_changes()` → `run_matching()` | Yes |
| `best_bid_ask` | `apply_changes()` (top-of-book) | Yes |
| `last_trade_price` | `record_trade()` | Yes |

Mỗi event đều gọi `book.monitor_bracket_orders()`:

```
monitor_bracket_orders():
  for each order where is_eligible_for_bracket:
    current_best_bid = max(bids.keys())

    # OCO logic: TP checked FIRST
    if tp_price is not None AND best_bid >= tp_price:
        → _execute_bracket_exit(order, best_bid, "TP")
        → skip SL check (OCO)

    elif sl_price is not None AND best_bid <= sl_price:
        → _execute_bracket_exit(order, best_bid, "SL")
```

**Điều kiện eligible** (`is_eligible_for_bracket`):
- `side == BUY`
- Có TP hoặc SL
- `filled > 0` (đã có vị thế)
- `position_closed == False`
- `status in (FILLED, PARTIAL, CANCELED)` — CANCELED orders có fill vẫn được monitor

### Phase 4 — Bracket Exit Execution (`_execute_bracket_exit`)

Simulate bán ra (taker SELL) qua shadow bids:

```
qty_to_close = filled - already_exited
for bid_price in sorted(bids, descending):
    fill_qty = min(remaining, bid_size)
    deduct from shadow bids
    accumulate total_value, qty_exited

avg_exit_price = total_value / qty_exited
```

- **Full exit** (`qty_exited >= filled`): `position_closed = True`
- **Partial exit** (bids exhausted): `position_closed = False` → tiếp tục monitor

Kết quả → `BracketFillResult`:
```
{order_id, trigger, trigger_price, market_bid,
 qty_to_close, qty_exited, avg_exit_price, levels_consumed}
```

### Phase 5 — Callback → Redis Stream (`order_consumer.py` → `redis_writer.py`)

`_on_bracket_exit` callback (registered lúc place order):

```
OrderConsumer._make_bracket_callback(bo_id)
  → RedisWriter.publish_bracket_exit()
    → XADD stream:bracket:exits {
        bo_id, trigger, exit_price, exit_filled, order_id, exit_at
      }
```

### Phase 6 — DB Update (`main.py` → `_handle_bracket_exit`)

API service consume từ `stream:bracket:exits` via XREADGROUP:

```
_handle_bracket_exit():
  bo = db.get(BinaryOption, bo_id)

  # Ghi exit data
  bo.exit_trigger = "TP" | "SL"
  bo.exit_price   = exit_price
  bo.exit_filled  = exit_filled
  bo.exit_at      = timestamp
  bo.me_order_status = "FILLED"

  # Full exit? → settle ngay lập tức
  if exit_filled >= num_shares:
      profit = (exit_price - avg_price) × exit_filled
      result = WIN if profit >= 0 else LOSS
      bo.result = result
      bo.profit = profit
      bot.balance += amount + profit
      → BalanceHistory record
      → commit + ACK

  # Partial exit? → chờ scheduler
  else:
      → commit + ACK
      → scheduler settles remainder via candle at settlement_at
```

### Phase 7 — Settlement (`services/settlement.py`)

#### Case A: Full bracket exit → đã settle ở Phase 6, scheduler skip

#### Case B: Partial bracket exit → scheduler settle remainder

```
_settle_single_trade():
  # exit_trigger = "TP"/"SL" + exit_filled < num_shares
  shadow_profit = (exit_price - avg_price) × exit_filled

  remainder_shares = num_shares - exit_filled
  binary_result = WIN if candle_dir == forecast else LOSS
  remainder_pnl =
    WIN:  (1 - avg_price) × remainder_shares
    LOSS: -avg_price × remainder_shares

  total_profit = shadow_profit + remainder_pnl
```

#### Case C: Bracket set nhưng KHÔNG fire → binary settlement

```
_settle_single_trade():
  # exit_trigger is None → pure binary formula
  WIN:  profit = (1 - avg_price) × num_shares
  LOSS: profit = -avg_price × num_shares
```

#### Case D: ME never filled (me_order_status = "PENDING") → cancel + refund

```
settle_pending_trades():
  if bo.me_order_status == "PENDING":
      bo.result = CANCELLED
      bo.profit = 0
      bot.balance += bo.amount  # full refund
```

---

## Luồng 2: LIMIT order có TP/SL

### Phase 1 — Order Creation (`routers/binary_options.py`)

```
Bot → POST /poly-arena/binary-options/
      {symbol, timeframe, forecast, amount,
       limit_price: 0.48, tp_price: 0.60, sl_price: 0.40}
```

1. Xác thực bot, trừ `amount` khỏi balance
2. Lấy `token_id` từ Redis (TokenRegistry cache) — **nếu không có → HTTP 503**
3. Lưu DB: `avg_price=None`, `num_shares=None`, `me_order_status="PENDING"`
4. Push vào Redis queue:
   ```json
   {
     "bo_id": 456,
     "token_id": "0xabc...",
     "side": "BUY",
     "price": 0.48,
     "limit_price": 0.48,
     "quantity": 20.83,
     "amount": 10.0,
     "tp_price": 0.60,
     "sl_price": 0.40,
     "timeframe": "M5",
     "ttl": 120,
     "slippage_tolerance": null
   }
   ```

### Phase 2 — ME Matching (`order_consumer.py` → `matching_engine.py`)

`OrderConsumer._process_standard_order()`:

1. Gọi `engine.place_virtual_order()`:
   - Tạo `SimulatedOrder` với `order_type="LIMIT"`, `price=limit_price`
   - Set `expire_at` = `now + ttl_seconds` (nếu có ttl) hoặc candle boundary
   - Gọi `_match_order()` ngay lập tức:
     - Walk sorted asks ascending
     - Match tại `ask_price <= limit_price`
     - Update `filled`, `_entry_cost`, `status`
   - Nếu book rỗng: order stays PENDING → `run_matching()` retry khi có data
   - Nếu **filled > 0** ngay lập tức:
     - Check TP/SL ngay: `best_bid >= tp_price` → TP fires
     - Publish immediate fill event → Redis stream

2. **Fill event** → `stream:order:fills`:
   ```
   {bo_id, order_id, filled, avg_entry_price, status}
   ```

3. API consume fill → `_handle_order_fill()`:
   ```python
   bo.avg_price = avg_entry   # NOW avg_price gets set
   bo.num_shares = filled
   bo.me_order_status = status  # "PARTIAL" or "FILLED"
   ```

### Phase 2b — Ongoing Matching

Mỗi WS event (`book`, `price_change`) → `run_matching()`:
- LIMIT order tiếp tục match nếu ask mới <= limit_price
- Mỗi fill mới → state_change callback → `publish_order_fill` → DB update

### Phase 2c — TTL Expiry

`_expire_pending_orders()` chạy trong mỗi `run_matching()`:

| Status khi expire | Action |
|-------------------|--------|
| PENDING (filled=0) | → CANCELED, publish cancel event |
| PARTIAL (filled>0) | → quantity clamped to filled, → CANCELED, publish cancel with partial fill data |

Cancel event → `stream:order:cancels` → `_handle_order_cancel()`:
- **Zero fill**: `result=CANCELLED`, refund full `amount`
- **Partial fill**: update `num_shares=filled`, `avg_price=avg_entry`, refund unfilled portion, keep PENDING for settlement

### Phase 3–7 — Giống MARKET flow

Sau khi LIMIT order đã fill (hoặc partial fill + expire), luồng TP/SL monitoring và settlement giống hệt MARKET flow ở trên.

---

## Profit Matrix tổng hợp

| TP | SL | TP fired? | SL fired? | Settlement method |
|----|-----|-----------|-----------|-------------------|
| - | - | N/A | N/A | Binary: candle direction vs forecast |
| Set | - | Yes (full) | N/A | Shadow: `(exit_price - avg_price) × exit_filled` |
| Set | - | Yes (partial) | N/A | Shadow (exited) + Binary (remainder) |
| Set | - | No | N/A | Binary: candle direction vs forecast |
| - | Set | N/A | Yes (full) | Shadow: `(exit_price - avg_price) × exit_filled` (negative) |
| - | Set | N/A | Yes (partial) | Shadow (exited) + Binary (remainder) |
| - | Set | N/A | No | Binary: candle direction vs forecast |
| Set | Set | Yes | Skipped (OCO) | Shadow profit |
| Set | Set | No | Yes | Shadow loss |
| Set | Set | No | No | Binary: candle direction vs forecast |

---

## Sequence Diagrams

### MARKET + TP/SL (full exit via TP)

```
Bot          API             Redis              OrderConsumer     ME/Book         WS Feed        API Consumer
 │            │                │                     │              │               │               │
 │─POST /bo/──│                │                     │              │               │               │
 │            │──REST fill─────│                     │              │               │               │
 │            │  avg=0.52      │                     │              │               │               │
 │            │  shares=19.2   │                     │              │               │               │
 │            │──LPUSH queue───│                     │              │               │               │
 │◄──201──────│                │                     │              │               │               │
 │            │                │──BRPOP──────────────│              │               │               │
 │            │                │                     │──prefilled───│               │               │
 │            │                │                     │  bracket reg │               │               │
 │            │                │                     │              │               │               │
 │            │                │                     │              │◄──best_bid_ask│               │
 │            │                │                     │              │  bid=0.61     │               │
 │            │                │                     │              │──TP trigger!──│               │
 │            │                │                     │              │  exit@0.61    │               │
 │            │                │                     │◄─callback────│               │               │
 │            │                │◄─XADD bracket:exits─│              │               │               │
 │            │                │                     │              │               │──XREADGROUP───│
 │            │                │                     │              │               │               │──settle
 │            │                │                     │              │               │               │  full exit
 │            │                │                     │              │               │               │  profit=+1.73
 │            │                │                     │              │               │               │  balance+=
```

### LIMIT + TP/SL (partial fill → expire → scheduler settle)

```
Bot          API             Redis              OrderConsumer     ME/Book         Scheduler
 │            │                │                     │              │               │
 │─POST /bo/──│                │                     │              │               │
 │  limit=0.48│                │                     │              │               │
 │  tp=0.60   │                │                     │              │               │
 │  sl=0.40   │                │                     │              │               │
 │  ttl=120   │                │                     │              │               │
 │            │──save DB───────│                     │              │               │
 │            │  avg=None      │                     │              │               │
 │            │  status=PENDING│                     │              │               │
 │            │──LPUSH queue───│                     │              │               │
 │◄──201──────│                │                     │              │               │
 │            │                │──BRPOP──────────────│              │               │
 │            │                │                     │──place_order─│               │
 │            │                │                     │  LIMIT 0.48  │               │
 │            │                │                     │              │──match 10/20──│
 │            │                │                     │◄─fill event──│               │
 │            │                │◄─XADD order:fills───│              │               │
 │            │  DB: avg=0.48  │                     │              │               │
 │            │  shares=10     │                     │              │               │
 │            │  status=PARTIAL│                     │              │               │
 │            │                │                     │              │               │
 │            │                │ ... 120s later ...   │              │               │
 │            │                │                     │              │──TTL expire───│
 │            │                │                     │              │  PARTIAL→CANCEL│
 │            │                │                     │◄─cancel event│               │
 │            │                │◄─XADD order:cancels─│              │               │
 │            │  DB: shares=10 │                     │              │               │
 │            │  refund unfill │                     │              │               │
 │            │                │                     │              │               │
 │            │                │                     │              │               │──settlement_at
 │            │                │                     │              │               │  candle check
 │            │                │                     │              │               │  binary settle
 │            │                │                     │              │               │  remainder
```

---

## IPC Channels

| Channel | Type | Producer | Consumer | Data |
|---------|------|----------|----------|------|
| `queue:orders:new` | Redis List (LPUSH/BRPOP) | API | OrderConsumer | Order JSON (LIMIT/MARKET+bracket) |
| `stream:order:fills` | Redis Stream | OrderConsumer | API (`_handle_order_fill`) | `{bo_id, order_id, filled, avg_entry_price, status}` |
| `stream:order:cancels` | Redis Stream | OrderConsumer | API (`_handle_order_cancel`) | `{bo_id, order_id, reason, filled, avg_entry_price}` |
| `stream:bracket:exits` | Redis Stream | OrderConsumer | API (`_handle_bracket_exit`) | `{bo_id, trigger, exit_price, exit_filled, order_id, exit_at}` |

---

## Edge Cases & Safety

| Scenario | Handling |
|----------|----------|
| TP/SL fire ngay lúc place order | Check TP/SL trong `place_virtual_order()` sau khi match (line 521-534) |
| Fill price vượt qua TP khi fill | `_handle_order_fill` detect `avg_entry >= tp_price` → instant settle (line 268-324) |
| Partial bracket exit (bids exhausted) | `position_closed=False` → ME tiếp tục monitor → fire lại khi có bids mới |
| Late fill (fill arrives after settlement_at) | `_handle_order_fill` detect → fetch Binance candle → settle ngay (line 329-355) |
| ME never fills (me_order_status=PENDING at settlement) | Scheduler cancels + full refund (settlement.py line 266-288) |
| ws_feed not running (no token_id) | LIMIT order → HTTP 503; MARKET order → fallback REST (no ME, scheduler settle) |
| Callback fails (Redis publish error) | Message NOT ACKed → stays in PEL → `_drain_pending()` replays on restart |
| Duplicate bracket exit message | Guard: `bo.exit_trigger is None and bo.result == PENDING` (main.py line 57) |
| OCO (One-Cancels-Other) | TP checked first; if TP fires, SL skipped for that order (line 1040-1041) |
