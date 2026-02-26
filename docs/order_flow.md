# PolyArena — Đặc tả Flow Vào Lệnh Binary Options

## Tổng quan

Hệ thống có **hai lớp tính profit** hoàn toàn độc lập:

```
┌─────────────────────────────────────────────────────────────┐
│  SETTLEMENT LAYER  (profit thực tế, lưu vào DB)             │
│                                                             │
│  Áp dụng khi KHÔNG có TP/SL hoặc bracket không fire        │
│                                                             │
│  WIN:  profit = (1 - avg_price) × num_shares                │
│  LOSS: profit = -amount                                     │
│                                                             │
│  Ví dụ: avg_price=0.30, num_shares=333                      │
│    WIN  → (1 - 0.30) × 333 = +233.10                       │
│    LOSS → -amount = -100.00                                 │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  MATCHING ENGINE LAYER  (shadow tracking)                   │
│                                                             │
│  Áp dụng khi TP hoặc SL thực sự fire                       │
│  profit = (avg_exit - avg_entry) × exit_filled             │
│                                                             │
│  Dùng làm profit thực tế khi bracket được kích hoạt        │
└─────────────────────────────────────────────────────────────┘
```

### Ma trận công thức profit theo cấu hình lệnh

| Cấu hình TP/SL | Kịch bản | Profit thực tế |
|----------------|----------|----------------|
| Không TP, không SL | WIN (expiry) | Binary: `(1 - avg_price) × num_shares` |
| Không TP, không SL | LOSS (expiry) | Binary: `-amount` |
| Có TP, không SL | WIN — TP fire trước expiry | **Shadow**: `(avg_exit − avg_entry) × exit_filled` |
| Có TP, không SL | LOSS — TP không fire, expired | Binary: `-amount` |
| Không TP, có SL | WIN — SL không fire, expired | Binary: `(1 - avg_price) × num_shares` |
| Không TP, có SL | LOSS — SL fire trước expiry | **Shadow**: `(avg_exit − avg_entry) × exit_filled` (âm) |
| Có cả TP và SL | WIN — TP fire | **Shadow**: `(avg_exit − avg_entry) × exit_filled` |
| Có cả TP và SL | LOSS — SL fire | **Shadow**: `(avg_exit − avg_entry) × exit_filled` (âm) |

> **Nguyên tắc:** Shadow tracking profit được dùng làm profit thực tế **khi và chỉ khi**
> bracket exit (TP hoặc SL) thực sự được kích hoạt trước khi lệnh hết hạn.
> Mọi trường hợp còn lại đều dùng binary settlement formula.

Hệ thống hỗ trợ **8 loại lệnh** dựa trên 2 chiều × 4 kiểu:

| # | Loại | TP | SL | Đóng vị thế |
|---|------|----|----|-------------|
| 1 | Market GREEN  | — | — | Settlement khi nến đóng |
| 2 | Market RED    | — | — | Settlement khi nến đóng |
| 3 | Limit GREEN   | — | — | Settlement khi nến đóng |
| 4 | Limit RED     | — | — | Settlement khi nến đóng |
| 5 | Limit TP only GREEN/RED | Có | — | TP fire (shadow profit) hoặc binary settlement |
| 6 | Limit SL only GREEN/RED | — | Có | SL fire (shadow profit) hoặc binary settlement |
| 7 | Bracket GREEN | Có | Có | TP/SL fire (shadow profit) |
| 8 | Bracket RED   | Có | Có | TP/SL fire (shadow profit) |

**Quy ước Polymarket:**
- `GREEN` = dự đoán **UP** → BUY token YES → lấy `best_ask` của token YES
- `RED`   = dự đoán **DOWN** → BUY token NO → lấy `best_ask` của token NO

**Timeframe được hỗ trợ:** `M5` (5 phút), `M15` (15 phút), `H1` (1 giờ)

**expire_at** — căn chỉnh theo candle grid:
```
M5,  đặt lúc 12:12 → expire_at = 12:15:00
M5,  đặt lúc 12:14 → expire_at = 12:15:00
M5,  đặt lúc 12:15 → expire_at = 12:20:00  (sang kỳ tiếp)
```

---

## Công thức profit theo từng trường hợp

### Trường hợp 1 — Không có TP/SL (settlement thuần túy)

```
WIN  (forecast đúng hướng nến):
  profit = (1 - avg_price) × num_shares

LOSS (forecast sai hướng nến):
  profit = -amount

Ví dụ: amount=100, avg_price=0.30
  num_shares = 100 / 0.30 = 333.33 shares
  WIN  → (1 - 0.30) × 333.33 = +233.33
  LOSS → -100.00
```

### Trường hợp 2 — Có TP, không SL

```
TP fire trước expiry → WIN:
  profit = (avg_exit − avg_entry) × exit_filled
  (shadow tracking, TP được matched qua bid levels)

TP không fire, expired → LOSS (binary):
  profit = -amount

Ví dụ: avg_entry=0.50, TP @ bid=0.72, exit_filled=200
  Shadow WIN  → (0.72 − 0.50) × 200 = +44.00
  Binary LOSS → -100.00  (nếu TP không fire)
```

### Trường hợp 3 — Không TP, có SL

```
SL fire trước expiry → LOSS:
  profit = (avg_exit − avg_entry) × exit_filled  (âm)
  (shadow tracking, SL được matched qua bid levels)

SL không fire, expired → WIN (binary):
  profit = (1 - avg_price) × num_shares

Ví dụ: avg_entry=0.50, SL @ bid=0.33, exit_filled=200
  Shadow LOSS → (0.33 − 0.50) × 200 = -34.00
  Binary WIN  → (1 − 0.50) × 200 = +100.00  (nếu SL không fire)
```

### Trường hợp 4 — Có cả TP và SL (Bracket)

```
TP fire → WIN:
  profit = (avg_exit − avg_entry) × exit_filled

SL fire → LOSS:
  profit = (avg_exit − avg_entry) × exit_filled  (âm)

Ví dụ: avg_entry=0.50, qty=200
  TP @ 0.75 → shadow_profit = (0.73 − 0.50) × 200 = +46.00
  SL @ 0.30 → shadow_profit = (0.32 − 0.50) × 200 = -36.00
```

> **Lưu ý:** avg_price là giá mua trung bình (có thể fill qua nhiều ask levels).
> avg_exit là giá bán trung bình khi TP/SL fire (walk qua bid levels, có slippage).

---

## 1. Market GREEN

**Mục đích:** Vào lệnh BUY token YES ngay lập tức.

### Input
```
forecast=GREEN, symbol=BTC, timeframe=M5, amount=100
```

### Flow
```
Bước 1 — Lấy giá
  price = engine.best_ask(token_yes_id)   ← WebSocket shadow orderbook
        = REST fallback nếu engine stale
  → price = 0.52

Bước 2 — Đặt lệnh (taker, không có expire_at)
  order = place_virtual_order(BUY, price=0.52, qty=100/0.52=192.31)
  → FILLED ngay (fill qua nhiều ask levels nếu qty lớn)
  → avg_entry_price = weighted avg qua các levels đã fill

Bước 3 — Lưu DB
  avg_price   = 0.52      (hoặc thấp hơn nếu fill qua nhiều levels)
  num_shares  = 100/0.52 = 192.31
  settlement_at = candle_expire_at("M5")

Bước 4 — Settlement (nến M5 đóng)  [Không TP/SL → binary]
  close > open  →  WIN   profit = (1 - 0.52) × 192.31 = +92.31
  close ≤ open  →  LOSS  profit = -100.00
```

### Trạng thái
```
PLACED → FILLED → [settlement_at] → WIN / LOSS
```

---

## 2. Market RED

**Mục đích:** Vào lệnh BUY token NO ngay lập tức (dự đoán giá sẽ GIẢM).

### Input
```
forecast=RED, symbol=BTC, timeframe=M5, amount=100
```

### Flow
```
Bước 1 — Lấy giá token NO
  price = engine.best_ask(token_no_id)  →  0.48

Bước 2 — Đặt lệnh BUY token NO
  qty = 100 / 0.48 = 208.33
  → FILLED ngay

Bước 3 — Lưu DB
  avg_price=0.48, num_shares=208.33

Bước 4 — Settlement  [Không TP/SL → binary]
  close < open  →  WIN   profit = (1 - 0.48) × 208.33 = +108.33
  close ≥ open  →  LOSS  profit = -100.00
```

### Trạng thái
```
PLACED → FILLED → [settlement_at] → WIN / LOSS
```

---

## 3. Limit GREEN — Không có TP/SL

**Mục đích:** BUY token YES ở giá tốt hơn market, chấp nhận chờ đến cuối nến.

### Input
```
forecast=GREEN, order_type=LIMIT, limit_price=0.49, timeframe=M5, amount=100
```

### Flow
```
Bước 1 — Tạo SimulatedOrder
  side=BUY, price=0.49, qty=100/0.49=204.08
  expire_at = candle_expire_at("M5")  →  e.g. 12:15:00

Bước 2 — Matching Engine kiểm tra ngay
  best_ask=0.52 > 0.49  →  PENDING

Bước 3 — Mỗi WS event (book / price_change)
  run_matching() → fill nếu có ask ≤ 0.49

Bước 4a — FILLED trước expire_at  [Không TP/SL → binary]
  avg_price = avg_entry qua các levels đã fill
  WIN:  profit = (1 - avg_price) × num_shares
  LOSS: profit = -amount

Bước 4b — PENDING khi đến expire_at (filled=0)
  expire_pending_orders() → CANCELED
  Không có P&L

Bước 4c — PARTIAL khi đến expire_at (0 < filled < qty)
  quantity clamped = filled  (remaining_qty = 0, không fill thêm)
  avg_price = avg_entry của phần đã fill
  num_shares = filled
  → Settlement trên phần đã fill  [Không TP/SL → binary]
  WIN:  profit = (1 - avg_price) × filled
  LOSS: profit = -(avg_price × filled)   ← mất tiền đã bỏ ra cho phần đó
```

### Trạng thái
```
PLACED
  ├── PENDING ──[ask≤limit trước expire_at]──► FILLED ──► WIN / LOSS
  ├── PENDING ──[expire_at, filled=0]─────────► CANCELED
  └── PARTIAL ──[expire_at, filled>0]──────────► PARTIAL clamped ──► WIN / LOSS
                                                 (settlement trên phần đã fill)
```

### Timeline (M5, đặt lúc 12:12)

```
Kịch bản A — FILLED:
  12:12:00  PENDING @ 0.49, expire_at=12:15
  12:13:20  ask drop → 0.49 → FILLED 204 shares
  12:15:00  settlement → WIN profit=(1-0.49)×204=+104.04 / LOSS=-100

Kịch bản B — CANCELED:
  12:12:00  PENDING @ 0.49, expire_at=12:15
  12:15:00  ask vẫn 0.52 → expire → CANCELED, profit=0

Kịch bản C — PARTIAL clamped:
  12:12:00  PENDING 204 shares @ 0.49
  12:13:00  fill 80 shares @ 0.49, PARTIAL
  12:15:00  expire → clamp qty=80, không fill thêm
  12:15:00  settlement trên 80 shares:
            WIN:  (1-0.49)×80 = +40.80
            LOSS: -(0.49×80) = -39.20
```

---

## 4. Limit RED — Không có TP/SL

Giống Limit GREEN, chỉ khác token NO và điều kiện WIN/LOSS ngược lại.

```
WIN:  close < open  →  profit = (1 - avg_price) × num_shares
LOSS: close ≥ open  →  profit = -amount
```

---

## 5. Limit TP only — Có TP, không có SL (GREEN hoặc RED)

**Mục đích:** Vào lệnh limit với mục tiêu chốt lời tự động qua TP.
- Nếu TP fire → **profit thực tế = shadow tracking** (không dùng binary formula)
- Nếu TP không fire và lệnh hết hạn → dùng binary settlement

### Input
```
forecast=GREEN, limit_price=0.50, tp_price=0.70, sl_price=None, timeframe=M5
```

### Flow
```
Bước 1 — Tạo SimulatedOrder
  side=BUY, price=0.50, tp=0.70, sl=None
  expire_at = candle_expire_at("M5")

Bước 2 — Matching → FILLED
  avg_entry_price = 0.50  (hoặc weighted avg qua levels)

Bước 3 — Workflow E (mỗi WS tick)
  Chỉ kiểm tra TP (sl_price is None → SL block bị bỏ qua):

  IF best_bid >= tp_price (0.70):
    → TP EXIT: walk bids descending (shadow orderbook slippage)
    → exit_price = avg_exit qua bid levels
    → position_closed = True

Bước 4 — Kết quả

  4a. TP đã fire trước expire_at:
    → WIN
    → profit = (avg_exit − avg_entry) × exit_filled  [shadow tracking]

  4b. TP không fire, expire_at đến:
    → settlement theo binary formula
    close > open  →  WIN   profit = (1 - avg_price) × num_shares
    close ≤ open  →  LOSS  profit = -amount
```

### Trạng thái
```
PLACED
  ├── PENDING ──[expire_at]────────────────────► CANCELED
  └── FILLED
        ├── [TP fire]  → WIN  profit = shadow tracking
        └── [expire_at, TP chưa fire] → binary settlement
```

### Ví dụ — avg_entry=0.50, amount=100, num_shares=200

```
Kịch bản A — TP fire:
  best_bid đạt 0.73 → walk bids → avg_exit=0.718
  WIN: profit = (0.718 − 0.50) × 200 = +43.60  [shadow]

Kịch bản B — TP không fire, giá lên nhẹ:
  expire_at → binary settlement
  close > open  →  WIN profit = (1-0.50)×200 = +100.00  [binary]

Kịch bản C — TP không fire, giá đi ngược:
  expire_at → binary settlement
  close ≤ open  →  LOSS profit = -100.00  [binary]
```

---

## 6. Limit SL only — Có SL, không có TP (GREEN hoặc RED)

**Mục đích:** Vào lệnh limit với SL để giới hạn lỗ.
- Nếu SL fire → **profit thực tế = shadow tracking** (âm)
- Nếu SL không fire và lệnh hết hạn → dùng binary settlement

### Input
```
forecast=GREEN, limit_price=0.50, tp_price=None, sl_price=0.35, timeframe=M5
```

### Flow
```
Bước 1 — Tạo SimulatedOrder
  side=BUY, price=0.50, tp=None, sl=0.35
  expire_at = candle_expire_at("M5")

Bước 2 — Matching → FILLED

Bước 3 — Workflow E
  TP block bị bỏ qua (tp_price is None)
  Chỉ kiểm tra SL:

  IF best_bid <= sl_price (0.35):
    → SL EXIT: walk bids descending (shadow slippage)
    → exit_price = avg_exit qua bid levels
    → position_closed = True

Bước 4 — Kết quả

  4a. SL đã fire trước expire_at:
    → LOSS
    → profit = (avg_exit − avg_entry) × exit_filled  [shadow tracking, âm]

  4b. SL không fire, expire_at đến:
    → binary settlement
    close > open  →  WIN   profit = (1 - avg_price) × num_shares
    close ≤ open  →  LOSS  profit = -amount
```

### Trạng thái
```
PLACED
  ├── PENDING ──[expire_at]────────────────────► CANCELED
  └── FILLED
        ├── [SL fire]  → LOSS  profit = shadow tracking (âm)
        └── [expire_at, SL chưa fire] → binary settlement
```

### Ví dụ — avg_entry=0.50, amount=100, num_shares=200

```
Kịch bản A — SL fire:
  best_bid drop 0.33 → walk bids → avg_exit=0.335
  LOSS: profit = (0.335 − 0.50) × 200 = -33.00  [shadow]

Kịch bản B — SL không fire, giá hồi phục:
  expire_at → binary settlement
  close > open  →  WIN profit = (1-0.50)×200 = +100.00  [binary]

Kịch bản C — SL không fire, giá xuống nhẹ nhưng trên SL:
  expire_at → binary settlement
  close ≤ open  →  LOSS profit = -100.00  [binary]
```

---

## 7. Bracket GREEN — Có TP và SL

**Mục đích:** Vào lệnh với cả TP và SL. Profit thực tế **luôn** dùng shadow tracking
vì TP hoặc SL sẽ fire trước khi lệnh hết hạn (trong điều kiện bình thường).

### Input
```
forecast=GREEN, limit_price=0.50, tp_price=0.70, sl_price=0.35, timeframe=M5
```

### Flow
```
Bước 1 — Tạo SimulatedOrder
  side=BUY, price=0.50, tp=0.70, sl=0.35
  expire_at = candle_expire_at("M5")

Bước 2 — Matching → FILLED
  avg_entry_price = weighted avg qua các ask levels

Bước 3 — Workflow E (shadow tracking, OCO)
  Kiểm tra TP trước: IF best_bid >= 0.70 → TP EXIT (shadow)
  Kiểm tra SL:       IF best_bid <= 0.35 → SL EXIT (shadow)
  OCO: TP và SL không thể fire cùng một tick

Bước 4 — Kết quả

  4a. TP fire:
    WIN: profit = (avg_exit − avg_entry) × exit_filled  [shadow]

  4b. SL fire:
    LOSS: profit = (avg_exit − avg_entry) × exit_filled  [shadow, âm]

  4c. Không TP không SL fire trước expire_at (bất thường):
    → binary settlement fallback
    WIN:  profit = (1 - avg_price) × num_shares
    LOSS: profit = -amount
```

### Trạng thái
```
PLACED
  ├── PENDING ──[expire_at]────────────────────────► CANCELED
  └── FILLED
        ├── [TP fire] → WIN  profit = shadow tracking
        ├── [SL fire] → LOSS profit = shadow tracking (âm)
        └── [expire_at, cả hai chưa fire] → binary settlement (fallback)
```

### Ví dụ — avg_entry=0.50, qty=200 shares

```
TP fire @ best_bid=0.75:
  Walk bids: 50 @ 0.74, 100 @ 0.73, 50 @ 0.72 → avg_exit=0.733
  WIN: profit = (0.733 − 0.50) × 200 = +46.60  [shadow]

SL fire @ best_bid=0.30:
  Walk bids: 200 @ 0.32 → avg_exit=0.32
  LOSS: profit = (0.32 − 0.50) × 200 = -36.00  [shadow]
```

---

## 8. Bracket RED — Có TP và SL

Giống Bracket GREEN, áp dụng cho token NO. Profit thực tế luôn dùng shadow tracking:

```
TP fire → WIN:  profit = (avg_exit − avg_entry) × exit_filled
SL fire → LOSS: profit = (avg_exit − avg_entry) × exit_filled  (âm)
```

---

## Bảng tổng hợp — Quy tắc chọn công thức profit

| Cấu hình | TP fire | SL fire | Hết hạn (WIN) | Hết hạn (LOSS) |
|----------|---------|---------|---------------|----------------|
| Không TP, không SL | — | — | Binary | Binary |
| Có TP, không SL | Shadow | — | Binary | Binary |
| Không TP, có SL | — | Shadow | Binary | Binary |
| Có cả TP và SL | Shadow | Shadow | Binary (fallback) | Binary (fallback) |

> **Quy tắc chung:**
> - Bracket exit (TP hoặc SL) fire → **profit = shadow tracking**
> - Lệnh hết hạn mà không có bracket fire → **profit = binary settlement**

---

## Bảng tổng hợp — Điều kiện fill và expire

| Loại lệnh | expire_at | Khi PENDING hết hạn | Khi PARTIAL hết hạn | Khi FILLED hết hạn |
|-----------|-----------|---------------------|---------------------|-------------------|
| Market    | Không có  | —                   | —                   | Binary settlement |
| Limit     | `candle_expire_at(tf)` | CANCELED | qty clamped → binary settlement | Binary settlement |
| Limit TP only | `candle_expire_at(tf)` | CANCELED | qty clamped → binary settlement | Binary settlement (nếu TP chưa fire) |
| Limit SL only | `candle_expire_at(tf)` | CANCELED | qty clamped → binary settlement | Binary settlement (nếu SL chưa fire) |
| Bracket   | `candle_expire_at(tf)` | CANCELED | qty clamped → binary settlement | Shadow (nếu đã fire) / Binary fallback |

---

## Sơ đồ trạng thái tổng quát

```
 place_virtual_order()
         │
         ▼
    ┌─────────┐
    │ PENDING │ ◄── run_matching() mỗi WS event
    └────┬────┘
         │ ask ≤ limit?
         ├── Không → chờ tiếp
         └── Có ──► ┌─────────┐  (fill một phần)
                    │ PARTIAL │ ──[tiếp tục fill]──► FILLED
                    └────┬────┘
                         │ expire_at
                         ▼
                   qty clamped (remaining=0)
                   → settlement trên filled

    PENDING  ──[expire_at, filled=0]──► CANCELED  (no P&L)

    FILLED (không TP/SL)
      ──[settlement_at]──► WIN / LOSS
                           WIN:  (1 - avg_price) × num_shares
                           LOSS: -amount

    FILLED + has_bracket
         │
         └── Workflow E (shadow, mỗi WS tick)
               ├── bid ≥ tp  →  TP fire → WIN  = (avg_exit − avg_entry) × exit_filled
               ├── bid ≤ sl  →  SL fire → LOSS = (avg_exit − avg_entry) × exit_filled (âm)
               └── Không fire đến expire_at → binary settlement fallback
```

---

## Cấu hình timeframe → expire_at

```python
order = engine.place_virtual_order(
    token_id,
    side      = OrderSide.BUY,
    price     = Decimal("0.30"),
    quantity  = Decimal("333.33"),   # = amount / avg_price
    timeframe = "M5",
    tp_price  = Decimal("0.60"),     # optional — nếu fire: shadow profit
    sl_price  = Decimal("0.15"),     # optional — nếu fire: shadow profit (âm)
)
# expire_at: 12:12 → 12:15, 12:14 → 12:15, 12:15 → 12:20
```

| Timeframe | Candle close |
|-----------|-------------|
| `M5`  | :00 :05 :10 :15 :20 :25 :30 :35 :40 :45 :50 :55 |
| `M15` | :00 :15 :30 :45 |
| `H1`  | 00:00 01:00 02:00 … 23:00 |
