# Order Flow: Matching Engine

## Tổng quan

Hệ thống sử dụng mô hình **"trừ trước, hoàn sau"**:
- Khi đặt lệnh: `balance -= amount`
- Khi settle/cancel: `balance += amount + profit` (hoặc refund nếu cancel)

Tất cả lệnh (MARKET, LIMIT, có/không TP/SL) đều đi qua Matching Engine khi `token_id` khả dụng.

---

## 1. MARKET Order Flow

```
                        ┌─────────────────────────────────┐
                        │  POST /binary-options            │
                        │  routers/binary_options.py:120   │
                        └──────────────┬──────────────────┘
                                       │
                    ┌──────────────────┼──────────────────────┐
                    ▼                  ▼                      ▼
            Validate API key    Check balance           Get price
            (line 141)          (line 147)              Redis → REST fallback
                                                        (lines 178-202)
                                       │
                                       ▼
                              Deduct balance upfront
                              bot.balance -= amount
                              (line 154)
                                       │
                                       ▼
                        ┌─────────────────────────────────┐
                        │  Create BinaryOption record      │
                        │  avg_price = NULL                 │
                        │  num_shares = NULL                │
                        │  me_order_status = "PENDING"      │
                        │  (lines 222-240)                  │
                        └──────────────┬──────────────────┘
                                       │
                                       ▼
                        ┌─────────────────────────────────┐
                        │  LPUSH queue:orders:new          │
                        │  {bo_id, token_id, side,         │
                        │   price, quantity, amount,        │
                        │   limit_price, tp/sl, ttl}        │
                        │  (lines 254-268)                  │
                        └──────────────┬──────────────────┘
                                       │
                          ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─
                          WS Feed Service (daemon thread)
                          ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─
                                       │
                                       ▼
                        ┌─────────────────────────────────┐
                        │  OrderConsumer.BRPOP              │
                        │  order_consumer.py:72             │
                        │                                   │
                        │  MARKET detection:                │
                        │  - price = ME best_ask             │
                        │  - qty = amount / best_ask         │
                        │  (lines 113-122)                  │
                        └──────────────┬──────────────────┘
                                       │
                                       ▼
                        ┌─────────────────────────────────┐
                        │  MatchingEngine.place_virtual_order│
                        │  matching_engine.py:1018          │
                        │                                   │
                        │  → ShadowOrderbook._match_order() │
                        │    (line 570)                     │
                        │                                   │
                        │  Walk asks ascending:             │
                        │  price >= ask → match             │
                        │  Consume liquidity at each level  │
                        │  Calculate weighted avg_entry      │
                        └──────────────┬──────────────────┘
                                       │
                                       ▼
                              ┌─────────────────┐
                              │  Order FILLED    │
                              │  (near instant)  │
                              └────────┬────────┘
                                       │
                                       ▼
                        ┌─────────────────────────────────┐
                        │  publish_order_fill()             │
                        │  XADD stream:order:fills          │
                        │  {bo_id, filled, avg_entry_price, │
                        │   status="FILLED", order_id}      │
                        │  redis_writer.py:204              │
                        └──────────────┬──────────────────┘
                                       │
                          ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─
                          API Service (asyncio consumer)
                          ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─
                                       │
                                       ▼
                        ┌─────────────────────────────────┐
                        │  _handle_order_fill()             │
                        │  main.py:387                      │
                        │                                   │
                        │  UPDATE binary_options SET         │
                        │    avg_price = 0.505,              │
                        │    num_shares = 196.08,            │
                        │    me_order_status = "FILLED"      │
                        └──────────────┬──────────────────┘
                                       │
                                       ▼
                        ┌─────────────────────────────────┐
                        │  Chờ settlement (scheduler)       │
                        │  settlement.py:239                │
                        │  → Binary formula hoặc shadow     │
                        │  → balance += amount + profit      │
                        └─────────────────────────────────┘
```

### Matching Engine: Cách fill MARKET order

Shadow orderbook (ví dụ):
```
ASKS (sorted ascending):
  0.50: 100 shares
  0.51: 150 shares
  0.52: 200 shares     ← best_ask = 0.50

Order: BUY price=0.50 (best_ask), qty = amount / 0.50
```

Execution:
```
Level 0.50: match qty shares → filled=qty, cost = qty × 0.50

Status: FILLED
avg_entry_price = 0.50 (fill tại best_ask)
```

MARKET order fill tại giá `best_ask`, không sweep qua nhiều level.

---

## 2. TP/SL (Bracket) Exit Flow

### 2.1. Tổng quan luồng

Khi một lệnh có TP/SL được fill, hệ thống liên tục theo dõi giá thị trường.
Khi giá chạm ngưỡng TP hoặc SL, lệnh được **tự động chốt (exit)** và settle ngay lập tức.

```
  Lệnh FILLED (có TP/SL)
         │
         ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  BƯỚC 1: Kiểm tra TP/SL ngay khi fill                      │
  │  matching_engine.py:438-459                                  │
  │                                                              │
  │  Nếu best_bid đã vượt TP/SL tại thời điểm fill → exit ngay │
  │  Nếu chưa → chuyển sang monitoring liên tục                 │
  └────────────────────────┬────────────────────────────────────┘
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  BƯỚC 2: Monitoring liên tục                                 │
  │  monitor_bracket_orders() — matching_engine.py:624           │
  │                                                              │
  │  Được gọi trên MỌI event từ Polymarket WebSocket:           │
  │  ┌─────────────────────────────────────────────────────┐     │
  │  │  book           → full snapshot     → monitor TP/SL │     │
  │  │  price_change   → orderbook delta   → monitor TP/SL │     │
  │  │  best_bid_ask   → top-of-book       → monitor TP/SL │     │
  │  │  last_trade     → trade execution   → monitor TP/SL │     │
  │  └─────────────────────────────────────────────────────┘     │
  │                                                              │
  │  Eligibility check (is_eligible_for_bracket):                │
  │  ✓ side == BUY                                               │
  │  ✓ has TP or SL price                                        │
  │  ✓ filled > 0 (đã mua shares)                               │
  │  ✓ position_closed == False                                  │
  │  ✓ status in (FILLED, PARTIAL)                               │
  └────────────────────────┬────────────────────────────────────┘
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  BƯỚC 3: Kiểm tra điều kiện trigger                          │
  │  OCO (One-Cancels-Other) — TP ưu tiên trước SL              │
  │                                                              │
  │  ┌───────────────────────────────────────────────────┐       │
  │  │  1. TP Check (line 654):                          │       │
  │  │     best_bid >= tp_price ?                        │       │
  │  │     → YES: trigger TP, skip SL                    │       │
  │  │     → NO:  check SL                               │       │
  │  │                                                   │       │
  │  │  2. SL Check (line 672, chỉ khi TP không fire):   │       │
  │  │     best_bid <= sl_price ?                        │       │
  │  │     → YES: trigger SL                             │       │
  │  │     → NO:  không trigger, chờ tick tiếp theo      │       │
  │  └───────────────────────────────────────────────────┘       │
  │                                                              │
  │  Ví dụ:                                                      │
  │  Mua tại avg_price = 0.52                                    │
  │  TP = 0.60 → khi best_bid >= 0.60 → chốt lời               │
  │  SL = 0.45 → khi best_bid <= 0.45 → cắt lỗ                 │
  └────────────────────────┬────────────────────────────────────┘
                           │ (triggered)
                           ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  BƯỚC 4: Thực thi exit — _execute_bracket_exit()             │
  │  matching_engine.py:696-776                                  │
  │                                                              │
  │  Bán (SELL) toàn bộ shares đã fill vào bid side             │
  │  của shadow orderbook, với slippage thực tế:                 │
  │                                                              │
  │  ┌───────────────────────────────────────────────────┐       │
  │  │  Shadow BIDS (sorted descending — best bid first): │       │
  │  │    0.62: 80 shares                                 │       │
  │  │    0.61: 50 shares                                 │       │
  │  │    0.60: 120 shares                                │       │
  │  │    0.59: 200 shares                                │       │
  │  │                                                    │       │
  │  │  Cần bán: qty_to_close = 150 shares                │       │
  │  │                                                    │       │
  │  │  Level 0.62: sell 80  → value = 49.60              │       │
  │  │  Level 0.61: sell 50  → value = 30.50              │       │
  │  │  Level 0.60: sell 20  → value = 12.00              │       │
  │  │  ─────────────────────────────────                 │       │
  │  │  Total: 150 shares, value = 92.10                  │       │
  │  │  avg_exit_price = 92.10 / 150 = 0.614              │       │
  │  │  (slippage: trigger 0.60, actual avg 0.614)        │       │
  │  └───────────────────────────────────────────────────┘       │
  │                                                              │
  │  Cập nhật order state:                                       │
  │  • position_closed = True (nếu exit hết)                     │
  │  • exit_price = avg_exit_price                               │
  │  • exit_trigger = "TP" hoặc "SL"                             │
  │  • exit_filled = số shares đã exit                           │
  │                                                              │
  │  Liquidity bị trừ khỏi shadow orderbook:                     │
  │  • bids[0.62] giảm 80 → bị xoá                              │
  │  • bids[0.61] giảm 50 → bị xoá                              │
  │  • bids[0.60] giảm 20 → còn 100                             │
  └────────────────────────┬────────────────────────────────────┘
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  BƯỚC 5: Publish exit event                                  │
  │                                                              │
  │  on_bracket_exit callback (order_consumer.py:276-295)        │
  │     │                                                        │
  │     ▼                                                        │
  │  publish_bracket_exit() — redis_writer.py:138                │
  │     │                                                        │
  │     ▼                                                        │
  │  XADD stream:bracket:exits                                   │
  │  {                                                           │
  │    "bo_id":       "123",                                     │
  │    "trigger":     "TP",         ← "TP" hoặc "SL"            │
  │    "exit_price":  "0.614",      ← avg exit price (slippage)  │
  │    "exit_filled": "150.0",      ← số shares đã exit         │
  │    "order_id":    "uuid...",                                 │
  │    "exit_at":     "2025-02-27T10:30:45Z"                     │
  │  }                                                           │
  └────────────────────────┬────────────────────────────────────┘
                           │
          ─ ─ ─ ─ ─ ─ ─ ─ │ ─ ─ ─ ─ ─ ─ ─ ─
          API Service (asyncio consumer)
          ─ ─ ─ ─ ─ ─ ─ ─ │ ─ ─ ─ ─ ─ ─ ─ ─
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  BƯỚC 6: Xử lý exit — _handle_bracket_exit()                │
  │  main.py:223-307                                             │
  │                                                              │
  │  Idempotent: chỉ xử lý nếu bo.exit_trigger is None          │
  │                                                              │
  │  Cập nhật DB:                                                │
  │  • exit_trigger  = "TP" / "SL"                               │
  │  • exit_price    = 0.614 (avg from ME)                       │
  │  • exit_filled   = 150.0                                     │
  │  • exit_at       = timestamp                                 │
  │  • me_order_status = "FILLED"                                │
  │                                                              │
  │  Phân nhánh:                                                 │
  │  ┌─────────────────────┬──────────────────────────────┐      │
  │  │    Full Exit         │    Partial Exit               │      │
  │  │ (exit_filled >=     │ (exit_filled <                │      │
  │  │  num_shares)        │  num_shares)                  │      │
  │  │                     │                               │      │
  │  │ → Settle NGAY       │ → Ghi data, giữ PENDING      │      │
  │  │   (main.py:266)     │   (main.py:294)               │      │
  │  │                     │                               │      │
  │  │ profit =            │ Scheduler sẽ settle phần      │      │
  │  │  (exit_price -      │ remainder tại candle close    │      │
  │  │   avg_price) ×      │ bằng settlement.py:166        │      │
  │  │   exit_filled       │                               │      │
  │  │                     │ profit =                      │      │
  │  │ payout =            │   shadow_profit               │      │
  │  │  amount + profit    │   + remainder_binary          │      │
  │  │                     │                               │      │
  │  │ balance += payout   │                               │      │
  │  │ BalanceHistory ✓    │                               │      │
  │  │ result = WIN/LOSS   │                               │      │
  │  └─────────────────────┴──────────────────────────────┘      │
  └──────────────────────────────────────────────────────────────┘
```

### 2.2. Ví dụ chi tiết: TP trigger

```
═══════════════════════════════════════════════════════════════
  VÍ DỤ: MARKET BUY $100 BTC M5 GREEN, TP=0.60, SL=0.45
═══════════════════════════════════════════════════════════════

1. ĐẶT LỆNH
   ─────────
   balance: $1000 → $900 (trừ $100)
   best_ask: 0.52
   num_shares: 100/0.52 = 192.31

   DB: avg_price=NULL, me_order_status="PENDING"

2. ME FILL (gần ngay lập tức)
   ──────────────────────────
   OrderConsumer: price=0.52 (best_ask), qty=192.31
   ME matches asks ascending:
     0.52 × 192.31 = cost 100.0

   avg_entry_price = 0.52
   status = FILLED

   DB update: avg_price=0.52, num_shares=192.31, me_order_status="FILLED"

3. MONITORING (liên tục)
   ─────────────────────
   Mỗi WS event → monitor_bracket_orders()
   • best_bid=0.53 → 0.53 < 0.60 (TP) → chưa trigger
   • best_bid=0.55 → 0.55 < 0.60 (TP) → chưa trigger
   • best_bid=0.58 → 0.58 < 0.60 (TP) → chưa trigger
   • best_bid=0.61 → 0.61 >= 0.60 (TP) → TRIGGERED!

4. EXIT EXECUTION
   ──────────────
   _execute_bracket_exit(trigger="TP")

   Shadow BIDS:
     0.61: 200 shares

   Sell 192.31 shares:
     Level 0.61: sell 192.31 → value = 117.31

   avg_exit_price = 117.31 / 192.31 = 0.61
   position_closed = True

5. PUBLISH & SETTLE
   ────────────────
   stream:bracket:exits → {trigger="TP", exit_price=0.61, exit_filled=192.31}

   _handle_bracket_exit():
     profit = (0.61 - 0.52) × 192.31 = 17.31
     result = WIN
     payout = 100 + 17.31 = 117.31
     balance: $900 + $117.31 = $1017.31

═══════════════════════════════════════════════════════════════
```

### 2.3. Ví dụ chi tiết: SL trigger

```
═══════════════════════════════════════════════════════════════
  VÍ DỤ: Cùng lệnh trên nhưng giá giảm → SL trigger
═══════════════════════════════════════════════════════════════

3. MONITORING (liên tục)
   ─────────────────────
   • best_bid=0.50 → 0.50 > 0.45 (SL) → chưa trigger
   • best_bid=0.48 → 0.48 > 0.45 (SL) → chưa trigger
   • best_bid=0.44 → 0.44 <= 0.45 (SL) → TRIGGERED!

4. EXIT EXECUTION
   ──────────────
   _execute_bracket_exit(trigger="SL")

   Shadow BIDS:
     0.44: 100 shares
     0.43: 150 shares

   Sell 192.31 shares:
     Level 0.44: sell 100   → value = 44.0
     Level 0.43: sell 92.31 → value = 39.69

   avg_exit_price = 83.69 / 192.31 = 0.4352
   (slippage: trigger tại 0.45, actual exit 0.4352)

5. PUBLISH & SETTLE
   ────────────────
   profit = (0.4352 - 0.52) × 192.31 = -16.31
   result = LOSS
   payout = 100 + (-16.31) = 83.69
   balance: $900 + $83.69 = $983.69

═══════════════════════════════════════════════════════════════
```

### 2.4. OCO (One-Cancels-Other) Logic

```python
triggered = False

# 1. TP check FIRST (priority)
if tp_price is not None and best_bid >= tp_price:
    execute_exit(trigger="TP")
    triggered = True

# 2. SL check ONLY if TP didn't fire
if not triggered and sl_price is not None and best_bid <= sl_price:
    execute_exit(trigger="SL")
```

Trong cùng một tick, chỉ **một trong hai** (TP hoặc SL) được trigger. TP có priority cao hơn.

### 2.5. Partial Exit (bids exhausted)

Khi shadow orderbook không đủ liquidity để exit hết:

```
Cần sell: 200 shares
BIDS chỉ có: 0.61: 80 shares

Lần 1: exit 80/200 → position_closed = False
       → giữ monitoring, chờ tick tiếp theo

(Tick mới, bids được replenish)
BIDS: 0.60: 150 shares

Lần 2: exit 120/120 (remaining) → position_closed = True
       → publish callback, settle
```

---

## 3. Settlement

### Profit Formula Matrix

| Trường hợp | Formula | Code |
|---|---|---|
| **No exit** (MARKET/LIMIT thuần) | Binary: candle direction vs forecast | `settlement.py:196` |
| **TP/SL full exit** | Shadow: `(exit_price - avg_price) x exit_filled` | `settlement.py:166` |
| **TP/SL partial exit** | Shadow + Binary cho remainder | `settlement.py:174-190` |
| **Never filled** (PENDING at settle) | Cancel + refund full amount | `settlement.py:265` |

### Binary Formula (no bracket)

```
WIN:  profit = (1 - avg_price) x num_shares
LOSS: profit = -(avg_price x num_shares)
```

### Shadow Formula (bracket exit)

```
profit = (exit_price - avg_price) x exit_filled
```

### Partial Exit Formula

```
shadow_profit = (exit_price - avg_price) x exit_filled
remainder_shares = num_shares - exit_filled

if candle matches forecast:
    remainder_profit = (1 - avg_price) x remainder_shares
else:
    remainder_profit = -(avg_price x remainder_shares)

total_profit = shadow_profit + remainder_profit
```

---

## 4. Balance Update Locations

| # | File | Line | Operation | Trigger |
|---|---|---|---|---|
| 1 | `routers/binary_options.py` | 154 | `balance -= amount` | Dat lenh |
| 2 | `main.py` | 279 | `balance += payout` | Bracket full exit |
| 3 | `main.py` | 343 | `balance += refund` | Partial fill expiry |
| 4 | `main.py` | 365 | `balance += amount` | Cancel (zero fill) |
| 5 | `main.py` | 451 | `balance += payout` | Bracket instant settle |
| 6 | `settlement.py` | 222 | `balance += payout` | Scheduler settlement |
| 7 | `settlement.py` | 277 | `balance += amount` | Unfilled at settlement |
| 8 | `settlement.py` | 348 | `balance += amount` | Stuck sweep (no candle) |
| 9 | `settlement.py` | 388 | `balance += amount` | Stuck sweep (orphaned) |

`payout = amount + profit` (hoan goc + lai/lo)

---

## 5. Redis Streams & Queues

| Name | Type | Producer | Consumer |
|---|---|---|---|
| `queue:orders:new` | List (FIFO) | API (`LPUSH`) | OrderConsumer (`BRPOP`) |
| `stream:order:fills` | Stream | OrderConsumer (`XADD`) | API `_consume_order_fills` (`XREADGROUP`) |
| `stream:bracket:exits` | Stream | ME callback (`XADD`) | API `_consume_bracket_exits` (`XREADGROUP`) |
| `stream:order:cancels` | Stream | OrderConsumer (`XADD`) | API `_consume_order_cancels` (`XREADGROUP`) |

Consumer group: `api-workers`, at-least-once delivery with ACK.

---

## 6. Key Files

| File | Responsibility |
|---|---|
| `routers/binary_options.py` | API endpoint, create order, push to queue |
| `ws_feed_service/order_consumer.py` | BRPOP consumer, MARKET price adjustment, place in ME |
| `services/matching_engine.py` | Shadow orderbook, order matching, TP/SL monitoring & exit |
| `ws_feed_service/redis_writer.py` | Publish fill/exit/cancel events to Redis streams |
| `main.py` | Stream consumers, DB updates, bracket/fill/cancel handlers |
| `services/settlement.py` | Scheduler settlement, profit formulas, stuck sweep |
