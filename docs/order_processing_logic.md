# Order Processing Logic — PolyArena

## Tổng quan kiến trúc

```
┌──────────────┐     ┌──────────────┐     ┌──────────────────────┐
│  Bot / Client │────▶│  FastAPI API  │────▶│     SQLite DB        │
│  (POST /bo/)  │     │ binary_opts  │     │  (BinaryOption row)  │
└──────────────┘     └──────┬───────┘     └──────────────────────┘
                            │
              ┌─────────────┼──────────────────┐
              │ Redis queue  │                  │
              │ queue:orders │                  │
              ▼ :new         │                  │
     ┌────────────────┐     │     ┌────────────────────────┐
     │ OrderConsumer   │     │     │  Scheduler Service     │
     │ (daemon thread) │     │     │  (APScheduler)         │
     └───────┬────────┘     │     └───────────┬────────────┘
             │              │                  │
             ▼              │                  ▼
     ┌────────────────┐     │     ┌────────────────────────┐
     │ MatchingEngine │     │     │ Settlement             │
     │ (shadow book)  │     │     │ (Binance candle check) │
     └───────┬────────┘     │     └────────────────────────┘
             │              │
             ▼              │
     Redis Streams ─────────┘
     stream:order:fills
     stream:order:cancels
     stream:bracket:exits
```

**3 process độc lập giao tiếp qua Redis:**
- **FastAPI API** — Nhận lệnh, validate, ghi DB, publish Redis
- **WS Feed Service** — Kết nối Polymarket WS, chạy MatchingEngine, xử lý LIMIT fill
- **Scheduler Service** — Settlement mỗi phút, sweep stuck orders

---

## 1. MARKET Order (limit_price = null)

### Flow chi tiết

```
Bot ──POST──▶ API ──▶ Validate ──▶ Session Resolution ──▶ REST Fill ──▶ Save DB
                                          │                    │
                                     (boundary guard)     Polymarket
                                                          CLOB REST
```

**Bước 1: Validation** (`binary_options.py:541`)
- Xác thực bot qua `x-api-key`
- Check balance ≥ amount
- Pre-validate TP/SL nếu có:
  - `SL price < best_ask` (không được SL cao hơn giá mua)
  - `TP price > best_ask` (không được TP thấp hơn giá mua)

**Bước 2: Session Resolution** (`binary_options.py:644`)
- Xác định candle session cho lệnh dựa trên `session_offset` hoặc `timestamp`
- Tính `settlement_at` = candle close time
- Lệnh sát boundary vẫn thuộc candle hiện tại (bot tự quyết `session_offset`)

**Bước 3: Fill qua Polymarket REST** (`binary_options.py:904`)
- Gọi `_fill_market_from_rest()` lấy orderbook từ Polymarket CLOB REST
- Sweep qua asks từ thấp → cao, trong phạm vi slippage tolerance (mặc định 10%)
- Kết quả: `avg_price`, `num_shares`, `walk_levels`

**Bước 4: Save DB** (`binary_options.py:932`)
- Tạo `BinaryOption` row: status=PENDING, `avg_price`/`num_shares` đã có
- Trừ balance ngay lập tức

**Bước 5: Bracket handling** (nếu có TP/SL)
- **Slippage violation**: nếu `avg_price ≥ TP` hoặc `avg_price ≤ SL` → Auto-Exit ngay qua REST bids
- **Bình thường**: Queue prefilled order sang ME qua Redis → ME chỉ theo dõi TP/SL, không cần fill lại
- **Không có bracket**: Chờ settlement bởi Scheduler

### Settlement (MARKET không có bracket)

```
Scheduler (:05s mỗi phút)
    │
    ▼
Tìm PENDING trades có settlement_at ≤ now
    │
    ▼
Fetch Binance candle (open, close)
    │
    ▼
So sánh: close > open → GREEN, close < open → RED
    │
    ▼
forecast == candle_dir → WIN:  profit = (1 - avg_price) × num_shares
forecast != candle_dir → LOSS: profit = -(avg_price × num_shares)
    │
    ▼
Cập nhật balance: bot.balance += amount + profit
```

---

## 2. LIMIT Order (limit_price ≠ null)

### Flow chi tiết — Two-phase

```
Bot ──POST──▶ API ──▶ Validate ──▶ Session Resolution
                                          │
                                     ┌────┴────┐
                                     │ Phase 1 │
                                     │REST check│
                                     └────┬────┘
                                          │
                              ┌───────────┴───────────┐
                              │                       │
                       best_ask ≤ limit         best_ask > limit
                              │                       │
                         Fill ngay              Defer to ME
                         (Phase 1a)             (Phase 1b)
                              │                       │
                         Save DB                 Save DB
                         (có avg_price)          (avg_price=NULL)
                              │                       │
                         Handle bracket          Queue to ME
                              │                       │
                              │              ┌────────┴────────┐
                              │              │   OrderConsumer  │
                              │              │   (BRPOP loop)   │
                              │              └────────┬────────┘
                              │                       │
                              │              place_virtual_order()
                              │                       │
                              │              ┌────────┴────────┐
                              │              │                 │
                              │         Fill ngay          Chờ fill
                              │         (asks có sẵn)     (resting order)
                              │              │                 │
                              │         Publish fill      WS events cập nhật
                              │         via Redis         book → run_matching()
                              │              │                 │
                              │              │            Fill hoặc TTL expire
                              │              │                 │
                              │              └────────┬────────┘
                              │                       │
                              │              Publish fill/cancel
                              │              via Redis stream
                              │                       │
                              └───────────────────────┘
                                          │
                                     Settlement
                                     (Scheduler)
```

### Phase 1a: Immediate REST Fill (`binary_options.py:699`)

Khi `best_ask ≤ limit_price`:
- Sweep asks từ Polymarket REST lên đến `limit_price`
- Tạo DB row với `avg_price`, `num_shares` đã có
- `me_order_status = "FILLED"`
- Nếu có bracket → xử lý TP/SL giống MARKET

### Phase 1b: Defer to Matching Engine (`binary_options.py:815`)

Khi `best_ask > limit_price`:
- Tạo DB row: `avg_price = NULL`, `num_shares = NULL`, `me_order_status = "PENDING"`
- Push JSON vào Redis queue `queue:orders:new`:

```json
{
  "bo_id": 123,
  "token_id": "0xabc...",
  "side": "BUY",
  "price": 0.45,           // limit_price
  "quantity": 22.22,       // amount / limit_price
  "limit_price": 0.45,
  "tp_price": 0.55,
  "sl_price": 0.40,
  "timeframe": "M5",
  "ttl": 295,              // seconds until candle close
  "session_offset": 0,
  "settlement_at": "2026-03-02T14:25:00+00:00"
}
```

### OrderConsumer xử lý (`order_consumer.py:276`)

```
BRPOP queue:orders:new
    │
    ▼
Parse JSON payload
    │
    ├── limit_price != null → order_type = "LIMIT"
    │
    ▼
engine.place_virtual_order(
    token_id, BUY, price=limit_price,
    quantity, tp, sl, timeframe, ttl,
    order_type="LIMIT"
)
    │
    ▼
ShadowOrderbook._match_order()
    │
    ├── Có asks ≤ limit_price → Fill ngay (partial hoặc full)
    │   │
    │   ▼
    │   Publish fill via Redis stream
    │
    └── Không có asks ≤ limit_price → Resting order
        │
        ▼
        Đợi WS events cập nhật book
```

### Matching Engine — Core Algorithm (`matching_engine.py:967`)

```python
# LIMIT BUY: duyệt asks tăng dần
for ask_price in asks.keys():       # SortedDict — tự sắp xếp tăng dần
    if order.price < ask_price:
        break                        # Không match nếu limit < ask

    match_qty = min(remaining, ask_size)
    order.filled += match_qty
    asks[ask_price] -= match_qty     # Trừ liquidity
```

**Khi nào LIMIT order được match lại?**

Mỗi khi WS feed nhận event mới → book cập nhật → `run_matching()`:

| WS Event | Handler | Trigger matching? |
|----------|---------|:-:|
| `book` (snapshot) | Replace entire book | Yes |
| `price_change` (delta) | Update specific level | Yes |
| `best_bid_ask` | Update top level | Yes (*) |
| `last_trade_price` | Record only | No |

(*) `best_bid_ask` không có `ask_size` → infer size từ book's best level hiện tại khi giá cải thiện.

### TTL & Expiration

- LIMIT order có `expire_at = now + ttl_seconds`
- Mặc định TTL = thời gian còn lại đến candle close
- `session_offset=1`: TTL = `settlement_at - now`
- Hết TTL → `CANCELED` + publish cancel → API refund balance

### LIMIT Fill → DB Update Flow

```
OrderConsumer
    │
    ▼ publish_order_fill()
    │
Redis stream:order:fills
    │
    ▼ _handle_order_fill() [FastAPI stream consumer]
    │
    ▼
UPDATE binary_options SET
    avg_price = <weighted avg>,
    num_shares = <total filled>,
    me_order_id = <uuid>,
    me_order_status = "FILLED" / "PARTIALLY_FILLED",
    walk_prices.entry = [{price, qty, cost}, ...]
```

---

## 3. Bracket Orders (TP/SL)

### Monitoring Flow

```
                    ┌────────────────────────────┐
                    │    ShadowOrderbook          │
                    │    monitor_bracket_orders()  │
                    └────────────┬───────────────┘
                                 │
                    Mỗi khi WS event cập nhật best_bid:
                                 │
                    ┌────────────┴───────────────┐
                    │                            │
              best_bid ≥ TP               best_bid ≤ SL
                    │                            │
              Take Profit                   Stop Loss
                    │                            │
              _execute_bracket_exit()     _execute_bracket_exit()
                    │                            │
              Sweep bids (sell)           Sweep bids (sell)
              multi-level slippage        multi-level slippage
                    │                            │
                    └────────────┬───────────────┘
                                 │
                    BracketFillResult {
                        trigger: "TP" / "SL",
                        avg_exit_price,
                        qty_exited,
                        fill_levels
                    }
                                 │
                    Redis stream:bracket:exits
                                 │
                    _handle_bracket_exit() [FastAPI]
                                 │
                    UPDATE binary_options SET
                        exit_trigger, exit_price,
                        exit_filled, exit_at
```

### Settlement với Bracket

```
Settlement (Scheduler)
    │
    ├── exit_trigger IN ("TP", "SL")
    │       │
    │       ▼
    │   Shadow profit = (exit_price - avg_price) × exit_filled
    │       │
    │       ├── Partial exit? (exit_filled < num_shares)
    │       │       │
    │       │       ▼
    │       │   remainder_shares = num_shares - exit_filled
    │       │   binary formula áp dụng cho phần còn lại
    │       │
    │       ▼
    │   total_profit = shadow_profit + remainder_profit
    │
    └── exit_trigger IS NULL (no bracket fired)
            │
            ▼
        Binary formula: candle direction vs forecast
```

---

## 4. Boundary Time — Xử lý sát ranh giới candle

### Vấn đề

```
Timeline (M5 candle):
14:20:00 ──────────────── 14:24:45 ── 14:25:00 ── 14:25:05~35s ── 14:25:35
│         candle hiện tại          │   boundary    │  token rotation  │ new data
│                                  │               │  gap (5-35s)     │ arrives
│                                  │◄── danger ──►│                  │
```

**Race conditions tiềm ẩn:**
1. Order dùng token cũ → WS feed ngừng → không có giá → LIMIT không fill
2. TTL = candle close → order expire trước khi token mới sẵn sàng
3. Settlement ở candle hiện tại nhưng order chưa kịp fill

### Cách xử lý hiện tại: KHÔNG auto-bump

Lệnh tạo sát boundary vẫn thuộc candle hiện tại. Hệ thống **không** tự động bump sang session tiếp theo — bot tự quyết định `session_offset`.

```
Thời điểm: 14:24:48 (12s còn lại)
Timeframe: M5, session_offset=0

→ candle_open   = 14:20:00    (candle hiện tại)
→ settlement_at = 14:25:00    (12 giây nữa)
→ token_id      = token của 14:20
```

Bot muốn vào session tiếp theo phải tự gửi `session_offset=1` hoặc `timestamp` tương ứng.

### Prefetch Infrastructure

Hệ thống đã prefetch sẵn 5 candle tương lai — khi bot chọn `session_offset=1`, token đã sẵn sàng:

```
TokenRegistry._future_mapping:
  (BTC, M5, UP) → [token_14:25, token_14:30, token_14:35, ...]

WS Feed đã subscribe TẤT CẢ tokens (current + future)
→ Orderbook data sẵn sàng cho session tiếp theo
```

### Session Resolution

| Trường hợp | Session | Lý do |
|------------|---------|--------|
| `session_offset=0`, bất kỳ thời điểm nào | Candle hiện tại | Bot chọn session 0 |
| `session_offset=1` | Candle tiếp theo | Bot chỉ định next session |
| `timestamp` chỉ định rõ | Candle chứa timestamp | Bot chỉ định chính xác candle |

---

## 5. Tóm tắt trạng thái lệnh

```
                    ┌──────────┐
                    │  CREATE   │
                    └────┬─────┘
                         │
              ┌──────────┴──────────┐
              │                     │
         MARKET fill           LIMIT defer
         (có avg_price)        (avg_price=NULL)
              │                     │
              │              ┌──────┴──────┐
              │              │             │
              │          ME FILL      ME CANCEL
              │          (stream)     (TTL expire)
              │              │             │
              │              │         CANCELLED
              │              │         + refund
              │              │
              └──────┬───────┘
                     │
            ┌────────┴────────┐
            │                 │
       Has bracket?      No bracket
            │                 │
     ┌──────┴──────┐    Settlement
     │             │    (Scheduler)
   TP/SL fires   No fire    │
     │             │         │
  shadow P&L   Settlement    │
     │         (binary)      │
     └─────┬───────┘         │
           │                 │
    ┌──────┴──────┐   ┌──────┴──────┐
    │    WIN      │   │    WIN      │
    │ profit > 0  │   │ forecast=   │
    │             │   │ candle_dir  │
    └─────────────┘   └─────────────┘
    ┌──────┴──────┐   ┌──────┴──────┐
    │    LOSS     │   │    LOSS     │
    │ profit < 0  │   │ forecast≠   │
    │             │   │ candle_dir  │
    └─────────────┘   └─────────────┘
```

### Profit Formulas

| Trường hợp | Formula |
|------------|---------|
| WIN (binary) | `profit = (1 - avg_price) × num_shares` |
| LOSS (binary) | `profit = -(avg_price × num_shares)` |
| TP/SL (shadow) | `profit = (exit_price - avg_price) × exit_filled` |
| Partial exit (shadow + binary) | `shadow_profit + remainder_binary` |
| CANCELLED (unfilled) | `profit = 0, balance += amount (refund)` |

### Payout

```
payout = amount + profit
bot.balance += payout
```

Ví dụ:
- Bet $10, avg_price=0.45, 22.22 shares, WIN: `profit = (1 - 0.45) × 22.22 = $12.22` → payout = $22.22
- Bet $10, avg_price=0.45, 22.22 shares, LOSS: `profit = -(0.45 × 22.22) = -$10.00` → payout = $0.00

---

## 6. Key Files Reference

| File | Chức năng |
|------|-----------|
| `routers/binary_options.py` | Order creation, session resolution, REST fill |
| `ws_feed_service/order_consumer.py` | BRPOP loop, place virtual orders in ME |
| `services/matching_engine.py` | Shadow orderbook, matching, bracket monitoring |
| `services/settlement.py` | Binance candle fetch, WIN/LOSS resolution |
| `config/timing.py` | Tất cả constants: TTL, timeouts, thresholds |
