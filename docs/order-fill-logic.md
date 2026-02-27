# Order Fill Logic — MARKET vs LIMIT

Tài liệu mô tả chi tiết logic fill lệnh MARKET và LIMIT đã implement trong matching engine.

---

## 1. Tổng quan Flow

```
Bot API call (BOCreate)
  │
  ├─ Validate input (schemas.py)
  │    ├─ limit_price, tp_price, sl_price: phải trong (0, 1) hoặc None
  │    ├─ amount > 0
  │    └─ ttl > 0 hoặc None
  │
  ├─ Xác định loại lệnh (routers/binary_options.py)
  │    ├─ limit_price = None  →  MARKET
  │    └─ limit_price = 0.xx  →  LIMIT
  │
  ├─ Lấy giá & token_id
  │    ├─ MARKET: Redis price cache → fallback REST Polymarket API
  │    └─ LIMIT:  dùng limit_price, lấy token_id từ Redis
  │
  ├─ Tạo DB record (BinaryOption, status=PENDING)
  │
  └─ Push Redis queue → OrderConsumer
       │
       ├─ MARKET: tính lại quantity = amount / ME.best_ask
       │
       └─ MatchingEngine.place_virtual_order()
            │
            ├─ _match_order()          ← fill ngay
            ├─ IOC logic (MARKET)      ← cancel remainder
            └─ Bracket check (TP/SL)   ← trigger nếu đủ điều kiện
```

---

## 2. Giá trị Input (Decimal 0–1)

Tất cả giá trong hệ thống dùng format **decimal (0, 1)**, không phải cent.

| Field | Ví dụ | Ý nghĩa |
|---|---|---|
| `limit_price` | `0.55` | Mua tối đa ở giá 55¢ |
| `tp_price` | `0.70` | Chốt lời khi best_bid ≥ 70¢ |
| `sl_price` | `0.30` | Cắt lỗ khi best_bid ≤ 30¢ |
| Orderbook asks | `{0.52: 100}` | 100 shares bán ở 52¢ |
| Orderbook bids | `{0.48: 200}` | 200 shares mua ở 48¢ |

**Validator** (`schemas.py:27-32`):
```python
@field_validator("limit_price", "tp_price", "sl_price")
def price_in_range(cls, v):
    if v is not None and not (0 < v < 1):
        raise ValueError("price must be between 0 and 1 (exclusive)")
```

---

## 3. MARKET Order — Fill Logic

### 3.1. Đặc tính

- **Sweep tất cả levels**: không kiểm tra giá, quét toàn bộ asks (BUY) hoặc bids (SELL)
- **IOC (Immediate-Or-Cancel)**: phần không fill được bị cancel ngay
- **Không rematch**: sau khi IOC resolve, `run_matching()` không retry

### 3.2. Matching Algorithm (`_match_order`)

```
MARKET BUY:
  for ask_price in sorted(asks, ascending):
      match_qty = min(remaining_qty, ask_size)
      filled   += match_qty
      cost     += match_qty × ask_price    ← track entry cost
      asks[ask_price] -= match_qty
      if FILLED: break
      # KHÔNG có price check → sweep hết tất cả levels
```

**Ví dụ**: MARKET BUY qty=120, book asks = `{0.50: 50, 0.60: 50, 0.70: 50}`

| Step | Ask Level | Match Qty | Filled | Cost |
|---|---|---|---|---|
| 1 | 0.50 | 50 | 50 | 25.00 |
| 2 | 0.60 | 50 | 100 | 55.00 |
| 3 | 0.70 | 20 | 120 | 69.00 |

→ `avg_entry_price = 69.00 / 120 = 0.575`

### 3.3. IOC Logic (`place_virtual_order`)

```python
# Snapshot liquidity TRƯỚC khi match
had_liquidity = bool(self.asks)   # BUY side

self._match_order(order)

if order_type == "MARKET" and status != FILLED:
    if had_liquidity or filled > 0:
        status = CANCELED       # IOC cancel remainder
    # else: book trống → giữ PENDING, chờ book data
```

**Ba kịch bản**:

| Kịch bản | Book state | Kết quả |
|---|---|---|
| Full fill | Đủ liquidity | `FILLED` |
| Partial fill | Thiếu liquidity | `CANCELED` (giữ filled portion) |
| Empty book | Chưa có data (WS chưa connect) | `PENDING` → fill khi book update |

### 3.4. Empty Book — Deferred Fill

Khi order đến trước WS feed populate book:

```
1. place_virtual_order() → book trống → _match_order() không fill gì
2. had_liquidity = False, filled = 0 → KHÔNG IOC cancel → giữ PENDING
3. WS event "book" arrive → apply_snapshot() → run_matching()
4. run_matching() → _match_order() sweep asks → fill
5. IOC logic trong run_matching() → cancel remainder nếu không fill hết
```

### 3.5. run_matching() — MARKET IOC sau book update

```python
def run_matching(self):
    for order in virtual_orders:
        if status in (FILLED, CANCELED): continue

        had_liquidity = bool(self.asks)    # snapshot trước match
        self._match_order(order)

        # MARKET IOC: cancel sau khi match với book có data
        if order_type == "MARKET" and status != FILLED:
            if had_liquidity or filled > 0:
                status = CANCELED
```

---

## 4. LIMIT Order — Fill Logic

### 4.1. Đặc tính

- **Price-restricted**: chỉ fill ở `limit_price` hoặc tốt hơn
- **Resting**: nếu không match được, order nằm `PENDING`/`PARTIAL` chờ book update
- **Rematch**: `run_matching()` retry mỗi khi book thay đổi
- **TTL expiry**: cancel khi hết thời gian

### 4.2. Matching Algorithm (`_match_order`)

```
LIMIT BUY (price = 0.55):
  for ask_price in sorted(asks, ascending):
      if ask_price <= order.price:     ← price check
          match_qty = min(remaining_qty, ask_size)
          filled   += match_qty
          cost     += match_qty × ask_price
          asks[ask_price] -= match_qty
          if FILLED: break
      else:
          break    ← asks ascending, không cần check tiếp
```

**Ví dụ**: LIMIT BUY price=0.55 qty=100, asks = `{0.50: 50, 0.55: 30, 0.60: 50}`

| Step | Ask Level | Check | Match Qty | Filled |
|---|---|---|---|---|
| 1 | 0.50 | 0.50 ≤ 0.55 ✓ | 50 | 50 |
| 2 | 0.55 | 0.55 ≤ 0.55 ✓ | 30 | 80 |
| 3 | 0.60 | 0.60 ≤ 0.55 ✗ | — | break |

→ `status = PARTIAL`, `filled = 80`, `avg = (50×0.50 + 30×0.55) / 80 = 0.51875`
→ Order nằm PARTIAL, chờ asks ≤ 0.55 xuất hiện để fill 20 shares còn lại

### 4.3. Rematch trên Book Update

```
1. WS event "book" hoặc "price_change" → apply_snapshot() / apply_changes()
2. run_matching() chạy cho tất cả orders (PENDING, PARTIAL)
3. _match_order() kiểm tra asks mới
4. Nếu có asks ≤ limit_price → fill thêm
5. Lặp lại đến khi FILLED hoặc TTL expire
```

### 4.4. TTL Expiry (`_expire_pending_orders`)

```python
# Chạy đầu mỗi run_matching()
for order in virtual_orders:
    if now >= order.expire_at:
        if status == PENDING:
            status = CANCELED          # zero fill
        elif status == PARTIAL:
            quantity = filled           # clamp, remaining = 0
            status = CANCELED          # giữ filled portion
```

**Expiry priority**:
1. `ttl_seconds` → `expire_at = now + ttl`
2. `timeframe` → `expire_at = candle_close` (M5: align 5 phút, H1: align 1 giờ)
3. Cả hai None → Good-Till-Canceled (không expire)

---

## 5. SELL Side — Mirror Logic

Cả MARKET và LIMIT SELL đều là logic ngược lại:

| | BUY | SELL |
|---|---|---|
| Match against | `asks` (ascending) | `bids` (descending) |
| Price check (LIMIT) | `ask_price ≤ order.price` | `bid_price ≥ order.price` |
| MARKET | Sweep tất cả asks | Sweep tất cả bids |

---

## 6. Bracket Order (TP/SL) sau Fill

### 6.1. Điều kiện eligible

```python
is_eligible_for_bracket =
    side == BUY
    and has_bracket (tp_price or sl_price set)
    and filled > 0
    and not position_closed
    and status in (FILLED, PARTIAL, CANCELED)   # CANCELED = MARKET IOC partial
```

### 6.2. TP/SL Monitor (`monitor_bracket_orders`)

**Trigger**: mỗi khi nhận WS event `best_bid_ask`, `last_trade_price`, `book`, `price_change`

```
1. Lấy current_best_bid = max(bids.keys())
2. For each eligible order:
   a. TP check: best_bid >= tp_price  → execute SELL exit
   b. SL check: best_bid <= sl_price  → execute SELL exit  (OCO: skip nếu TP đã fire)
```

### 6.3. Exit Execution (`_execute_bracket_exit`)

Exit bán filled shares qua standard matching — sweep bids descending, chịu slippage thực:

```
qty_to_close = order.filled - already_exited
for bid_price in sorted(bids, descending):
    fill_qty = min(remaining, bid_size)
    qty_exited  += fill_qty
    total_value += fill_qty × bid_price
    bids[bid_price] -= fill_qty

avg_exit_price = total_value / qty_exited
position_closed = (total_exited >= filled)
```

### 6.4. TP/SL cho MARKET orders

MARKET order partial fill (IOC cancel remainder) → `status = CANCELED` nhưng `filled > 0`:

```
MARKET BUY qty=100, book chỉ có 30 shares
→ filled=30, status=CANCELED
→ is_eligible_for_bracket = True (CANCELED + filled > 0)
→ TP/SL monitor theo dõi 30 shares đã fill
```

---

## 7. Status Transitions

### MARKET Order

```
PENDING ──match──► FILLED                     (full fill, book có đủ liquidity)
PENDING ──match──► PARTIAL ──IOC──► CANCELED   (partial fill, IOC cancel remainder)
PENDING ──(empty book)──► PENDING              (chờ book data)
PENDING ──book update──► FILLED                (deferred fill)
PENDING ──book update──► CANCELED              (deferred partial + IOC)
```

### LIMIT Order

```
PENDING ──match──► FILLED                      (asks ≤ limit, đủ qty)
PENDING ──match──► PARTIAL                     (fill một phần, chờ thêm)
PARTIAL ──rematch──► FILLED                    (book update có asks mới)
PENDING ──TTL──► CANCELED                      (hết hạn, zero fill)
PARTIAL ──TTL──► CANCELED                      (hết hạn, giữ filled portion)
```

---

## 8. So sánh MARKET vs LIMIT

| Feature | MARKET | LIMIT |
|---|---|---|
| Price check | Không — sweep tất cả | `ask ≤ limit_price` (BUY) |
| Unfilled handling | IOC cancel ngay | Resting — chờ book update |
| Rematch trên update | Không (đã IOC) | Có — retry mỗi `run_matching()` |
| Empty book | Chờ PENDING → fill khi có data | Chờ PENDING → fill khi ask ≤ limit |
| TTL expiry | Không áp dụng (IOC nhanh hơn) | Cancel khi hết hạn |
| avg_entry_price | Weighted avg qua nhiều levels | Weighted avg ≤ limit_price |
| Bracket (TP/SL) | Áp dụng cho filled portion | Áp dụng cho filled portion |

---

## 9. Weighted Average Entry Price

Cả hai loại lệnh đều track `avg_entry_price` qua nhiều fill levels:

```python
_entry_cost += match_qty × fill_price     # tích lũy tại mỗi fill
avg_entry_price = _entry_cost / filled     # computed property
```

**Ví dụ**: BUY fill 50@0.50 + 30@0.55 + 20@0.60

```
_entry_cost = 50×0.50 + 30×0.55 + 20×0.60 = 25 + 16.5 + 12 = 53.5
avg_entry_price = 53.5 / 100 = 0.535
```

---

## 10. Thread Safety

| Component | Lock | Scope |
|---|---|---|
| `ShadowOrderbook._lock` | Per-book | `_match_order`, `run_matching`, `monitor_bracket_orders` |
| `MatchingEngine._books_lock` | Global registry | `get_or_create_book` |
| Bracket callbacks | Fire **ngoài** lock | Tránh deadlock với DB/Redis ops |
| OrderConsumer | Daemon thread | BRPOP loop, không block asyncio |
| Order monitor | Per-order daemon thread | Poll state mỗi 2s |
