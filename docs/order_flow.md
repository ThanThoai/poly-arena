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
│  LOSS: profit = -(avg_price × num_shares)                   │
│                                                             │
│  Ví dụ: avg_price=0.30, num_shares=333                      │
│    WIN  → (1 - 0.30) × 333 = +233.10                       │
│    LOSS → -(0.30 × 333)    = -99.90                        │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  MATCHING ENGINE LAYER  (shadow tracking)                   │
│                                                             │
│  Áp dụng khi TP hoặc SL thực sự fire                       │
│  profit = (avg_exit - avg_entry) × exit_filled             │
│                                                             │
│  Dùng làm profit thực tế khi bracket được kích hoạt        │
│                                                             │
│  Bao gồm: bracket exit (real-time) và bracket instant      │
│  settle (fill price đã vượt TP/SL)                         │
└─────────────────────────────────────────────────────────────┘
```

### Ma trận công thức profit theo cấu hình lệnh

| Cấu hình TP/SL | Kịch bản | Profit thực tế |
|----------------|----------|----------------|
| Không TP, không SL | WIN (expiry) | Binary: `(1 - avg_price) × num_shares` |
| Không TP, không SL | LOSS (expiry) | Binary: `-(avg_price × num_shares)` |
| Có TP, không SL | TP fire trước expiry | **Shadow**: `(avg_exit − avg_entry) × exit_filled` |
| Có TP, không SL | TP không fire, expired (candle direction) | Binary: WIN hoặc LOSS tùy chiều candle |
| Không TP, có SL | SL fire trước expiry | **Shadow**: `(avg_exit − avg_entry) × exit_filled` (thường âm) |
| Không TP, có SL | SL không fire, expired (candle direction) | Binary: WIN hoặc LOSS tùy chiều candle |
| Có cả TP và SL | TP fire | **Shadow**: `(avg_exit − avg_entry) × exit_filled` |
| Có cả TP và SL | SL fire | **Shadow**: `(avg_exit − avg_entry) × exit_filled` (thường âm) |
| Có cả TP và SL | Không fire, expired | Binary: WIN hoặc LOSS tùy chiều candle (fallback) |
| Bất kỳ bracket | Fill price >= TP | **Instant**: `(tp_price − avg_entry) × num_shares` |
| Bất kỳ bracket | Fill price <= SL | **Instant**: `(sl_price − avg_entry) × num_shares` |

> **Nguyên tắc:**
> - Bracket exit (TP hoặc SL) fire → **profit = shadow tracking**
> - Fill price đã vượt TP/SL → **profit = bracket instant settle** (settle ngay tại fill consumer)
> - Lệnh hết hạn mà không có bracket fire → **profit = binary settlement** (candle direction vs forecast)

Hệ thống hỗ trợ **8 loại lệnh** dựa trên 2 chiều × 4 kiểu:

| # | Loại | TP | SL | Đóng vị thế |
|---|------|----|----|-------------|
| 1 | Market GREEN  | — | — | Settlement khi nến đóng |
| 2 | Market RED    | — | — | Settlement khi nến đóng |
| 3 | Limit GREEN   | — | — | Settlement khi nến đóng |
| 4 | Limit RED     | — | — | Settlement khi nến đóng |
| 5 | Limit TP only GREEN/RED | Có | — | TP fire (shadow) / TP instant / binary |
| 6 | Limit SL only GREEN/RED | — | Có | SL fire (shadow) / SL instant / binary |
| 7 | Bracket GREEN | Có | Có | TP/SL fire (shadow) / instant / binary fallback |
| 8 | Bracket RED   | Có | Có | TP/SL fire (shadow) / instant / binary fallback |

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
  profit = -(avg_price × num_shares)

Ví dụ: amount=100, avg_price=0.30
  num_shares = 100 / 0.30 = 333.33 shares
  WIN  → (1 - 0.30) × 333.33 = +233.33
  LOSS → -(0.30 × 333.33)    = -100.00
```

### Trường hợp 2 — Có TP, không SL

```
TP fire trước expiry → shadow profit:
  profit = (avg_exit − avg_entry) × exit_filled
  (shadow tracking, TP được matched qua bid levels)

Fill price >= TP → bracket instant settle:
  profit = (tp_price − avg_entry) × num_shares
  (settle ngay tại fill consumer, không chờ ME monitor)

TP không fire, expired → binary (candle direction vs forecast):
  WIN:  profit = (1 - avg_price) × num_shares
  LOSS: profit = -(avg_price × num_shares)

Ví dụ: avg_entry=0.50, TP @ bid=0.72, exit_filled=200
  Shadow WIN  → (0.72 − 0.50) × 200 = +44.00
  Binary WIN  → (1 − 0.50) × 200    = +100.00 (nếu candle đúng chiều)
  Binary LOSS → -(0.50 × 200)        = -100.00 (nếu candle sai chiều)
```

### Trường hợp 3 — Không TP, có SL

```
SL fire trước expiry → shadow profit:
  profit = (avg_exit − avg_entry) × exit_filled  (thường âm)

Fill price <= SL → bracket instant settle:
  profit = (sl_price − avg_entry) × num_shares

SL không fire, expired → binary (candle direction vs forecast):
  WIN:  profit = (1 - avg_price) × num_shares
  LOSS: profit = -(avg_price × num_shares)

Ví dụ: avg_entry=0.50, SL @ bid=0.33, exit_filled=200
  Shadow LOSS → (0.33 − 0.50) × 200 = -34.00
  Binary WIN  → (1 − 0.50) × 200    = +100.00 (nếu SL không fire, candle đúng)
  Binary LOSS → -(0.50 × 200)        = -100.00 (nếu SL không fire, candle sai)
```

### Trường hợp 4 — Có cả TP và SL (Bracket)

```
TP fire → shadow profit:
  profit = (avg_exit − avg_entry) × exit_filled

SL fire → shadow profit (thường âm):
  profit = (avg_exit − avg_entry) × exit_filled

Fill price >= TP → bracket instant settle:
  profit = (tp_price − avg_entry) × num_shares

Fill price <= SL → bracket instant settle:
  profit = (sl_price − avg_entry) × num_shares

Không TP không SL fire → binary fallback:
  WIN:  profit = (1 - avg_price) × num_shares
  LOSS: profit = -(avg_price × num_shares)

Ví dụ: avg_entry=0.50, qty=200
  TP @ 0.75 → shadow_profit = (0.73 − 0.50) × 200 = +46.00
  SL @ 0.30 → shadow_profit = (0.32 − 0.50) × 200 = -36.00
```

> **Lưu ý:** avg_price là giá mua trung bình (có thể fill qua nhiều ask levels).
> avg_exit là giá bán trung bình khi TP/SL fire (walk qua bid levels, có slippage).
> Bracket instant settle dùng tp_price/sl_price chính xác (không qua bid levels).

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
  price = Redis cache (WS shadow orderbook) hoặc REST fallback
  → price = 0.52

Bước 2 — Lưu DB
  avg_price    = 0.52
  num_shares   = 100/0.52 = 192.31
  settlement_at = candle close time (M5)
  me_order_status = NULL  (không qua ME)

Bước 3 — Settlement (nến M5 đóng)  [Không TP/SL → binary]
  close > open  →  WIN   profit = (1 - 0.52) × 192.31 = +92.31
  close ≤ open  →  LOSS  profit = -(0.52 × 192.31) = -100.00
```

### Trạng thái
```
CREATED → [settlement_at] → WIN / LOSS
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
  price = 0.48

Bước 2 — Lưu DB
  avg_price=0.48, num_shares=208.33

Bước 3 — Settlement  [Không TP/SL → binary]
  close < open  →  WIN   profit = (1 - 0.48) × 208.33 = +108.33
  close ≥ open  →  LOSS  profit = -(0.48 × 208.33)    = -100.00
```

### Trạng thái
```
CREATED → [settlement_at] → WIN / LOSS
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
Bước 1 — Tạo SimulatedOrder trong ME
  side=BUY, price=0.49, qty=100/0.49=204.08
  expire_at = candle_expire_at("M5")  →  e.g. 12:15:00

Bước 2 — Matching Engine kiểm tra ngay
  best_ask=0.52 > 0.49  →  PENDING

Bước 3 — Mỗi WS event (book / price_change)
  run_matching() → fill nếu có ask ≤ 0.49

Bước 4a — FILLED trước expire_at  [Không TP/SL → binary]
  avg_price = avg_entry qua các levels đã fill
  WIN:  profit = (1 - avg_price) × num_shares
  LOSS: profit = -(avg_price × num_shares)
  * Nếu fill đến sau settlement_at → settle ngay (late fill)

Bước 4b — PENDING khi đến expire_at (filled=0)
  Cancel consumer: CANCELLED + refund bo.amount
  Không có P&L

Bước 4c — PARTIAL khi đến expire_at (0 < filled < qty)
  Cancel consumer:
    bo.num_shares = filled
    bo.avg_price  = avg_entry
    bo.amount     = filled × avg_entry
    unfilled_refund = original_amount - actual_cost
    Refund unfilled → bot.balance
  → Settlement trên phần đã fill  [Không TP/SL → binary]
  WIN:  profit = (1 - avg_price) × filled
  LOSS: profit = -(avg_price × filled)

Bước 4d — ME chưa fill khi settlement_at đến
  Scheduler: me_order_status="PENDING" → cancel + refund ngay (không grace period)
```

### Trạng thái
```
CREATED → ME PENDING
  ├── [ask≤limit trước expire_at] → FILLED → WIN / LOSS
  ├── [expire_at, filled=0]       → CANCELLED + refund
  ├── [expire_at, filled>0]       → refund unfilled → WIN / LOSS (binary trên filled)
  └── [settlement_at, ME chưa fill] → CANCELLED + refund
```

### Timeline (M5, đặt lúc 12:12)

```
Kịch bản A — FILLED:
  12:12:00  PENDING @ 0.49, expire_at=12:15
  12:13:20  ask drop → 0.49 → FILLED 204 shares
  12:15:00  settlement → WIN profit=(1-0.49)×204=+104.04 / LOSS=-(0.49×204)=-99.96

Kịch bản B — CANCELED:
  12:12:00  PENDING @ 0.49, expire_at=12:15
  12:15:00  ask vẫn 0.52 → expire → CANCELED, refund 100.00

Kịch bản C — PARTIAL + refund unfilled:
  12:12:00  PENDING 204 shares @ 0.49
  12:13:00  fill 80 shares @ 0.49, PARTIAL
  12:15:00  expire → cancel
            actual_cost = 80 × 0.49 = 39.20
            unfilled_refund = 100 - 39.20 = 60.80  → refund
            settlement trên 80 shares:
            WIN:  (1-0.49)×80 = +40.80
            LOSS: -(0.49×80)  = -39.20
```

---

## 4. Limit RED — Không có TP/SL

Giống Limit GREEN, chỉ khác token NO và điều kiện WIN/LOSS ngược lại.

```
WIN:  close < open  →  profit = (1 - avg_price) × num_shares
LOSS: close ≥ open  →  profit = -(avg_price × num_shares)
```

---

## 5. Limit TP only — Có TP, không có SL (GREEN hoặc RED)

**Mục đích:** Vào lệnh limit với mục tiêu chốt lời tự động qua TP.
- Nếu TP fire → **profit thực tế = shadow tracking**
- Nếu fill price >= TP → **profit = bracket instant settle** (settle ngay)
- Nếu TP không fire và lệnh hết hạn → dùng binary settlement (candle direction)

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

  → Nếu avg_entry >= tp_price (0.70):
    Bracket instant settle tại fill consumer
    profit = (0.70 − avg_entry) × filled
    (settle ngay, không chờ ME monitor)

Bước 3 — Workflow E (mỗi WS tick, nếu chưa instant settle)
  Chỉ kiểm tra TP (sl_price is None → SL block bị bỏ qua):

  IF best_bid >= tp_price (0.70):
    → TP EXIT: walk bids descending (shadow orderbook slippage)
    → exit_price = avg_exit qua bid levels
    → Full exit → settle ngay
    → Partial exit → ghi data, chờ scheduler

Bước 4 — Kết quả

  4a. Bracket instant settle (fill price >= TP):
    → profit = (tp_price − avg_entry) × filled  [instant]

  4b. TP đã fire trước expire_at:
    → profit = (avg_exit − avg_entry) × exit_filled  [shadow tracking]

  4c. TP không fire, expire_at đến (candle direction):
    close đúng chiều  →  WIN   profit = (1 - avg_price) × num_shares
    close sai chiều   →  LOSS  profit = -(avg_price × num_shares)
```

### Trạng thái
```
CREATED → ME PENDING
  ├── [expire_at, filled=0]            → CANCELLED + refund
  └── FILLED
        ├── [fill price >= TP]         → instant settle
        ├── [TP fire, full exit]       → settle ngay (shadow)
        ├── [TP fire, partial exit]    → shadow + binary remainder
        └── [expire_at, TP chưa fire]  → binary settlement (candle direction)
```

### Ví dụ — avg_entry=0.50, amount=100, num_shares=200

```
Kịch bản A — TP fire:
  best_bid đạt 0.73 → walk bids → avg_exit=0.718
  WIN: profit = (0.718 − 0.50) × 200 = +43.60  [shadow]

Kịch bản B — Bracket instant settle:
  avg_entry = 0.75 >= tp_price 0.70
  profit = (0.70 − 0.75) × 200 = -10.00  [instant, LOSS]

Kịch bản C — TP không fire, giá lên nhẹ:
  expire_at → binary settlement
  close > open  →  WIN profit = (1-0.50)×200 = +100.00  [binary]

Kịch bản D — TP không fire, giá đi ngược:
  expire_at → binary settlement
  close ≤ open  →  LOSS profit = -(0.50×200) = -100.00  [binary]
```

---

## 6. Limit SL only — Có SL, không có TP (GREEN hoặc RED)

**Mục đích:** Vào lệnh limit với SL để giới hạn lỗ.
- Nếu SL fire → **profit thực tế = shadow tracking** (thường âm)
- Nếu fill price <= SL → **profit = bracket instant settle**
- Nếu SL không fire và lệnh hết hạn → dùng binary settlement (candle direction)

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

  → Nếu avg_entry <= sl_price (0.35):
    Bracket instant settle tại fill consumer
    profit = (0.35 − avg_entry) × filled

Bước 3 — Workflow E (nếu chưa instant settle)
  TP block bị bỏ qua (tp_price is None)
  Chỉ kiểm tra SL:

  IF best_bid <= sl_price (0.35):
    → SL EXIT: walk bids descending (shadow slippage)
    → Full exit → settle ngay
    → Partial exit → ghi data, chờ scheduler

Bước 4 — Kết quả

  4a. Bracket instant settle (fill price <= SL):
    → profit = (sl_price − avg_entry) × filled  [instant]

  4b. SL đã fire trước expire_at:
    → profit = (avg_exit − avg_entry) × exit_filled  [shadow tracking, thường âm]

  4c. SL không fire, expire_at đến (candle direction):
    close đúng chiều  →  WIN   profit = (1 - avg_price) × num_shares
    close sai chiều   →  LOSS  profit = -(avg_price × num_shares)
```

### Trạng thái
```
CREATED → ME PENDING
  ├── [expire_at, filled=0]            → CANCELLED + refund
  └── FILLED
        ├── [fill price <= SL]         → instant settle
        ├── [SL fire, full exit]       → settle ngay (shadow)
        ├── [SL fire, partial exit]    → shadow + binary remainder
        └── [expire_at, SL chưa fire]  → binary settlement (candle direction)
```

### Ví dụ — avg_entry=0.50, amount=100, num_shares=200

```
Kịch bản A — SL fire:
  best_bid drop 0.33 → walk bids → avg_exit=0.335
  LOSS: profit = (0.335 − 0.50) × 200 = -33.00  [shadow]

Kịch bản B — Bracket instant settle:
  avg_entry = 0.30 <= sl_price 0.35
  profit = (0.35 − 0.30) × 200 = +10.00  [instant, WIN]

Kịch bản C — SL không fire, giá hồi phục:
  expire_at → binary settlement
  close > open  →  WIN profit = (1-0.50)×200 = +100.00  [binary]

Kịch bản D — SL không fire, giá xuống nhẹ nhưng trên SL:
  expire_at → binary settlement
  close ≤ open  →  LOSS profit = -(0.50×200) = -100.00  [binary]
```

---

## 7. Bracket GREEN — Có TP và SL

**Mục đích:** Vào lệnh với cả TP và SL.

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

  → Nếu avg_entry >= tp_price → bracket instant settle (TP)
  → Nếu avg_entry <= sl_price → bracket instant settle (SL)

Bước 3 — Workflow E (shadow tracking, OCO, nếu chưa instant settle)
  Kiểm tra TP trước: IF best_bid >= 0.70 → TP EXIT (shadow)
  Kiểm tra SL:       IF best_bid <= 0.35 → SL EXIT (shadow)
  OCO: TP và SL không thể fire cùng một tick

Bước 4 — Kết quả

  4a. Bracket instant settle:
    profit = (tp_price − avg_entry) × filled  (TP)
    profit = (sl_price − avg_entry) × filled  (SL)

  4b. TP fire (full exit → settle ngay):
    WIN: profit = (avg_exit − avg_entry) × exit_filled  [shadow]

  4c. SL fire (full exit → settle ngay):
    LOSS: profit = (avg_exit − avg_entry) × exit_filled  [shadow, thường âm]

  4d. Partial exit + candle close:
    profit = shadow_profit + binary_remainder

  4e. Không TP không SL fire trước expire_at (candle direction, fallback):
    WIN:  profit = (1 - avg_price) × num_shares
    LOSS: profit = -(avg_price × num_shares)
```

### Trạng thái
```
CREATED → ME PENDING
  ├── [expire_at, filled=0]                    → CANCELLED + refund
  └── FILLED
        ├── [fill price >= TP]                 → instant settle
        ├── [fill price <= SL]                 → instant settle
        ├── [TP fire, full exit]               → settle ngay (shadow)
        ├── [SL fire, full exit]               → settle ngay (shadow)
        ├── [TP/SL fire, partial exit]         → shadow + binary remainder
        └── [expire_at, cả hai chưa fire]      → binary settlement (fallback)
```

### Ví dụ — avg_entry=0.50, qty=200 shares

```
TP fire @ best_bid=0.75:
  Walk bids: 50 @ 0.74, 100 @ 0.73, 50 @ 0.72 → avg_exit=0.733
  WIN: profit = (0.733 − 0.50) × 200 = +46.60  [shadow]

SL fire @ best_bid=0.30:
  Walk bids: 200 @ 0.32 → avg_exit=0.32
  LOSS: profit = (0.32 − 0.50) × 200 = -36.00  [shadow]

Bracket instant (fill 0.75 >= TP 0.70):
  profit = (0.70 − 0.75) × 200 = -10.00  [instant, LOSS]

Bracket instant (fill 0.30 <= SL 0.35):
  profit = (0.35 − 0.30) × 200 = +10.00  [instant, WIN]
```

---

## 8. Bracket RED — Có TP và SL

Giống Bracket GREEN, áp dụng cho token NO. Profit thực tế luôn dùng shadow tracking:

```
TP fire → profit = (avg_exit − avg_entry) × exit_filled
SL fire → profit = (avg_exit − avg_entry) × exit_filled  (thường âm)
Instant → profit = (tp/sl_price − avg_entry) × num_shares
```

---

## Bảng tổng hợp — Quy tắc chọn công thức profit

| Cấu hình | TP fire | SL fire | Fill >= TP | Fill <= SL | Hết hạn |
|----------|---------|---------|-----------|-----------|---------|
| Không TP, không SL | — | — | — | — | Binary |
| Có TP, không SL | Shadow | — | Instant | — | Binary |
| Không TP, có SL | — | Shadow | — | Instant | Binary |
| Có cả TP và SL | Shadow | Shadow | Instant | Instant | Binary (fallback) |

> **Quy tắc chung:**
> 1. Fill price vượt TP/SL → **bracket instant settle** (tại fill consumer, ưu tiên cao nhất)
> 2. Bracket exit (TP hoặc SL) fire real-time → **profit = shadow tracking**
> 3. Lệnh hết hạn mà không có bracket fire → **profit = binary settlement** (candle direction vs forecast)

---

## Bảng tổng hợp — Điều kiện fill và expire

| Loại lệnh | expire_at | Khi PENDING hết hạn | Khi PARTIAL hết hạn | Khi FILLED hết hạn |
|-----------|-----------|---------------------|---------------------|-------------------|
| Market    | Không có  | —                   | —                   | Binary settlement |
| Limit     | `candle_expire_at(tf)` hoặc `now+ttl` | CANCELED + refund | refund unfilled → binary settlement | Binary settlement |
| Limit TP only | `candle_expire_at(tf)` hoặc `now+ttl` | CANCELED + refund | refund unfilled → binary | Instant (fill>=TP) / Shadow (TP fire) / Binary |
| Limit SL only | `candle_expire_at(tf)` hoặc `now+ttl` | CANCELED + refund | refund unfilled → binary | Instant (fill<=SL) / Shadow (SL fire) / Binary |
| Bracket   | `candle_expire_at(tf)` hoặc `now+ttl` | CANCELED + refund | refund unfilled → binary | Instant / Shadow / Binary fallback |

---

## Sơ đồ trạng thái tổng quát

```
 POST /binary-options/ → DB (result=PENDING)
         │
         ├── me_order_status=NULL (MARKET thuần)
         │     └── Scheduler settle tại settlement_at → WIN/LOSS
         │
         └── me_order_status=PENDING (LIMIT / BRACKET)
               │
               └── Push → Redis queue → ME place_virtual_order()
                     │
                     ├── PENDING (chưa fill)
                     │     ├── [ask ≤ limit] → fill → PARTIAL/FILLED
                     │     ├── [expire_at, filled=0] → CANCELED + refund
                     │     ├── [expire_at, filled>0] → refund unfilled → settlement
                     │     └── [settlement_at, vẫn PENDING] → cancel + refund ngay
                     │
                     ├── FILLED (không bracket)
                     │     ├── [settlement_at chưa qua] → Scheduler settle
                     │     └── [settlement_at đã qua] → Late fill settle ngay
                     │
                     └── FILLED + has_bracket
                           │
                           ├── [fill price >= TP] → Bracket instant settle  ★
                           ├── [fill price <= SL] → Bracket instant settle  ★
                           │
                           └── Workflow E (shadow tracking, mỗi WS tick)
                                 ├── bid ≥ tp → TP fire
                                 │     ├── Full exit → settle ngay (shadow)
                                 │     └── Partial → shadow + binary remainder
                                 ├── bid ≤ sl → SL fire
                                 │     ├── Full exit → settle ngay (shadow)
                                 │     └── Partial → shadow + binary remainder
                                 └── [expire_at, no bracket] → binary settlement

★ = Xử lý mới: settle ngay khi fill price đã vượt bracket level
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
    sl_price  = Decimal("0.15"),     # optional — nếu fire: shadow profit (thường âm)
)
# expire_at: 12:12 → 12:15, 12:14 → 12:15, 12:15 → 12:20
```

| Timeframe | Candle close |
|-----------|-------------|
| `M5`  | :00 :05 :10 :15 :20 :25 :30 :35 :40 :45 :50 :55 |
| `M15` | :00 :15 :30 :45 |
| `H1`  | 00:00 01:00 02:00 … 23:00 |
