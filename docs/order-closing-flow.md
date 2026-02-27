# Order Closing Flow

Tài liệu mô tả toàn bộ các trường hợp đóng lệnh Binary Option trong hệ thống, từ lúc tạo đến khi có kết quả cuối cùng (`WIN` / `LOSS` / `CANCELLED`).

---

## Kiến trúc tổng quan

```
Bot/Client
    │
    │  POST /binary-options/
    ▼
FastAPI (main.py)
    │  DB: BinaryOption (result=PENDING)
    │  push → Redis Queue (nếu cần virtual order)
    │
    ├──── WS Feed Service ─────────────────────────────────────────────┐
    │     OrderConsumer (BRPOP)                                        │
    │         └─ MatchingEngine.place_virtual_order()                 │
    │                 │                                                │
    │         order_monitor thread (check mỗi 2s)                     │
    │              ├─ fill       → stream:order:fills                  │
    │              └─ canceled   → stream:order:cancels                │
    │                                                                  │
    │     MatchingEngine (realtime WS price events)                    │
    │         ├─ TP/SL hit → on_bracket_exit callback                 │
    │         │                → stream:bracket:exits                  │
    │         └─ Fill xong, check TP/SL ngay tại placement            │
    │                          → stream:bracket:exits (nếu trigger)   │
    └──────────────────────────────────────────────────────────────────┘
    │
    ├──── Redis Stream Consumers (main.py asyncio tasks) ─────────────┐
    │     _consume_order_fills      ← stream:order:fills              │
    │       ├─ Cập nhật avg_price, num_shares, me_order_status        │
    │       ├─ Bracket instant settle (fill price >= TP hoặc <= SL)   │
    │       └─ Late fill settle (settlement_at đã qua)                │
    │                                                                  │
    │     _consume_order_cancels    ← stream:order:cancels            │
    │       ├─ Zero fill → CANCELLED + refund                         │
    │       └─ Partial fill → refund unfilled, giữ PENDING            │
    │                                                                  │
    │     _consume_bracket_exits    ← stream:bracket:exits            │
    │       ├─ Full exit → settle ngay (shadow formula)               │
    │       └─ Partial exit → ghi data, chờ scheduler                 │
    │                                                                  │
    │     Tất cả consumer có PEL drain khi restart                    │
    └──────────────────────────────────────────────────────────────────┘
    │
    └──── APScheduler ────────────────────────────────────────────────┐
          settle_pending_trades()   — mỗi phút tại :05s              │
          sweep_stuck_orders()      — mỗi 5 phút                     │
          └──────────────────────────────────────────────────────────┘
```

---

## 1. Lệnh MARKET thuần

> Không có `limit_price`, không có `tp_price`/`sl_price`.

**Tạo lệnh:**
- `me_order_status = NULL` — không đưa vào matching engine
- Không push vào Redis queue

**Đóng lệnh — duy nhất qua Scheduler:**

```
Mỗi phút lúc :05s
└─ settle_pending_trades()
    └─ settlement_at <= now ?
        ├─ YES → me_order_status == "PENDING"? → NO (nó là NULL)
        │        → fetch Binance candle tại settlement_at
        │         ├─ candle OK → so sánh candle direction vs forecast
        │         │               → WIN hoặc LOSS  ✓
        │         └─ candle NULL → skip (retry phút sau)
        └─ NO  → skip
```

**Công thức profit (binary):**
- `WIN`:  `profit = (1 - avg_price) × num_shares`
- `LOSS`: `profit = -(avg_price × num_shares)`

---

## 2. Lệnh LIMIT (không có TP/SL)

> `limit_price` được đặt. Lệnh chờ giá ask xuống đến `limit_price`.

**Tạo lệnh:**
- `me_order_status = PENDING`
- Push vào Redis queue → `OrderConsumer.place_virtual_order(price=limit_price)`

**Expiry của virtual order trong ME:**
- Nếu có `ttl`: `expire_at = now + ttl_seconds`
- Nếu không có `ttl`: `expire_at = candle_expire_at(timeframe)` — tức là đến khi candle đóng

```
                     ┌── ME khớp được (ask <= limit_price) ──────────────────┐
                     │                                                         │
              PARTIAL fill                                               FULLY FILLED
                     │                                                         │
           stream:order:fills                                      stream:order:fills
                     │                                                         │
         bo.avg_price, bo.num_shares                             bo.me_order_status=FILLED
         bo.me_order_status=PARTIAL                                            │
         [tiếp tục chờ fill thêm]                                              │
                                                            ┌─ settlement_at đã qua?
                                                            │  YES → Late fill settle ngay ✓
                                                            │  NO  → Scheduler settle sau ✓
                                                            └──────────────────────────────

                     └── TTL hết hạn / candle đóng (chưa fill đủ) ───────────┘
                                    │
                          ME: order.status = CANCELED
                          order_monitor phát hiện
                          publish → stream:order:cancels
                                    │
                          _consume_order_cancels
                              │
                              ├─ filled > 0 (partial fill)
                              │     bo.avg_price   = avg_entry
                              │     bo.num_shares  = filled
                              │     bo.amount      = filled × avg_entry
                              │     bo.me_order_status = CANCELED
                              │     unfilled_refund = original_amount - actual_cost
                              │     refund unfilled → bot.balance  ✓
                              │     [giữ PENDING cho settlement]
                              │     Scheduler settle → WIN/LOSS (binary trên phần đã fill) ✓
                              │
                              └─ filled == 0 (không khớp được gì)
                                    bo.result  = CANCELLED
                                    bo.profit  = 0
                                    refund bo.amount → bot.balance ✓
```

### 2a. ME chưa fill khi settlement_at đến

```
settle_pending_trades():
  me_order_status == "PENDING"
  → Cancel ngay lập tức + refund bo.amount  ✓
    (không có grace period — ME đã có cơ hội fill trong suốt candle)
```

### 2b. Lệnh LIMIT với TTL tùy chỉnh

```
POST { limit_price: 0.45, ttl: 120 }
  └─ expire_at = now + 120s

Nếu sau 120s chưa fill → ME cancel → publish cancel event
  └─ xử lý như trên (filled==0 → CANCELLED + refund,
                      filled>0  → refund unfilled, PENDING → settle)
```

> **Lưu ý:** TTL có thể nhỏ hơn thời gian đến candle close. Nếu TTL hết mà candle chưa đóng, lệnh vẫn bị cancel.

---

## 3. Lệnh MARKET với Bracket (TP/SL)

> Không có `limit_price`, nhưng có `tp_price` và/hoặc `sl_price`.

**Tạo lệnh:**
- `me_order_status = PENDING`
- Push vào Redis queue → `place_virtual_order(price=best_ask, tp=..., sl=...)`
- ME gán `on_bracket_exit` callback ngay khi place

**Expiry:** `expire_at = candle_expire_at(timeframe)` (hoặc `now + ttl` nếu có TTL)

```
ME fill ngay tại best_ask (MARKET)
  └─ Ngay sau fill, ME check TP/SL vs current_best_bid
     ├─ best_bid >= tp_price → TP callback ngay tại placement  ✓
     └─ best_bid <= sl_price → SL callback ngay tại placement  ✓

order_monitor thấy FILLED → publish stream:order:fills
  └─ _handle_order_fill:
     ├─ Cập nhật avg_price, num_shares, me_order_status=FILLED
     ├─ avg_entry >= tp_price? → Bracket instant settle (TP)  ✓
     ├─ avg_entry <= sl_price? → Bracket instant settle (SL)  ✓
     └─ settlement_at đã qua? → Late fill settle ngay  ✓

Sau đó ME tiếp tục monitor TP/SL theo WS price events:

    ┌── Giá chạm TP ─────────────────────────────────────────────────┐
    │   on_bracket_exit(trigger=TP, exit_price, exit_filled)         │
    │   → publish → stream:bracket:exits                             │
    │   _handle_bracket_exit:                                         │
    │     ├─ Full exit (exit_filled >= num_shares):                  │
    │     │   Settle ngay:                                            │
    │     │   profit = (exit_price - avg_price) × exit_filled        │
    │     │   result = WIN nếu profit >= 0, LOSS nếu < 0            │
    │     │   Cập nhật bot.balance + BalanceHistory  ✓               │
    │     │                                                           │
    │     └─ Partial exit (exit_filled < num_shares):                │
    │        Ghi exit data, giữ PENDING                              │
    │        Scheduler settle: shadow phần exit + binary remainder   │
    │        profit = shadow_profit + remainder_profit  ✓            │
    └────────────────────────────────────────────────────────────────┘

    ┌── Giá chạm SL ─────────────────────────────────────────────────┐
    │   Tương tự TP, trigger="SL"                                    │
    │   profit = (exit_price - avg_price) × exit_filled  (< 0)       │
    │   result = LOSS  ✓                                             │
    └────────────────────────────────────────────────────────────────┘

    ┌── Candle đóng mà không chạm TP/SL ────────────────────────────┐
    │   bo.exit_trigger = NULL                                       │
    │   me_order_status = "FILLED"                                   │
    │   Scheduler settle → binary formula (candle direction)         │
    │   result = WIN/LOSS  ✓                                        │
    └────────────────────────────────────────────────────────────────┘

    ┌── ME chưa fill khi settlement_at đến ─────────────────────────┐
    │   me_order_status = "PENDING"                                  │
    │   → Cancel ngay + refund bo.amount  ✓                         │
    └────────────────────────────────────────────────────────────────┘
```

**Công thức profit (shadow tracking):**

| Trường hợp | Công thức |
|---|---|
| Full TP/SL exit | `(exit_price - avg_price) × exit_filled` |
| Bracket instant (fill >= TP) | `(tp_price - avg_price) × num_shares` |
| Bracket instant (fill <= SL) | `(sl_price - avg_price) × num_shares` |
| Partial exit + candle | shadow phần exit + binary phần còn lại |
| Không exit (binary) | `(1 - avg_price) × num_shares` (WIN) hoặc `-(avg_price × num_shares)` (LOSS) |

---

## 4. Lệnh LIMIT + Bracket (TP/SL)

> Có cả `limit_price` và `tp_price`/`sl_price`.

```
Phase 1 — Chờ fill tại limit_price:
    Giống flow LIMIT (mục 2)
    ├─ Chưa fill khi TTL/candle hết → CANCELLED + refund (nếu zero fill)
    │                                → refund unfilled, settle binary (nếu partial fill)
    └─ FILLED → Phase 2

Phase 2 — Đã fill, monitor bracket:
    Giống flow Bracket (mục 3)
    ├─ Fill price >= TP → Bracket instant settle (TP)
    ├─ Fill price <= SL → Bracket instant settle (SL)
    ├─ TP hit (real-time) → shadow settle (full hoặc partial)
    ├─ SL hit (real-time) → shadow settle (full hoặc partial)
    └─ Candle close, no bracket fired → binary WIN/LOSS
```

### 4a. LIMIT + Bracket + TTL tùy chỉnh

```
POST { limit_price: 0.45, tp_price: 0.65, sl_price: 0.35, ttl: 120 }
  └─ expire_at = now + 120s  (ưu tiên hơn candle expiry)

Nếu TTL hết trước khi fill:
  └─ CANCELLED + refund (zero fill)
     hoặc refund unfilled + settle binary (partial fill)

Nếu fill trong TTL:
  └─ TP/SL/binary như Phase 2 bình thường
     (bracket monitor không bị giới hạn bởi TTL — theo candle)
```

---

## 5. Safety Net — Stuck Order Sweeper

Chạy **mỗi 5 phút** để bắt những lệnh mà normal settlement bỏ sót (scheduler crash, Binance API lỗi, Redis lag...).

### Case A — Có `settlement_at`, quá hạn > 10 phút

```
settlement_at + 10min <= now  AND  result = PENDING

└─ fetch Binance candle tại settlement_at
    ├─ candle OK  → _settle_single_trade() → WIN/LOSS  ✓
    └─ candle NULL → CANCELLED + refund bo.amount  ✓

Log prefix: [STUCK_SWEEP]
```

### Case B — `settlement_at IS NULL`, quá 2 giờ

```
settlement_at IS NULL  AND  created_at + 2h <= now  AND  result = PENDING

└─ CANCELLED + refund bo.amount  ✓
   (không có settlement_at → không xác định được candle nào để settle)

Log prefix: [STUCK_SWEEP]
```

---

## 6. PEL Drain khi Restart

Khi API server restart, tất cả 3 Redis stream consumers gọi `_drain_pending()` để xử lý các message chưa ACK từ lần chạy trước:

```
_drain_pending(stream, handler):
  └─ Đọc message với ID "0" (pending, chưa ACK)
  └─ Xử lý từng message qua handler tương ứng
  └─ ACK sau khi xử lý thành công
  → Đảm bảo không mất fill/cancel/bracket event sau crash
```

---

## Bảng tổng hợp kết quả

| Loại lệnh | Điều kiện kết thúc | result | profit | Nơi xử lý |
|---|---|---|---|---|
| MARKET | Candle đóng, đúng chiều | WIN | `(1 - avg_price) × num_shares` | Scheduler |
| MARKET | Candle đóng, sai chiều | LOSS | `-(avg_price × num_shares)` | Scheduler |
| LIMIT | Không fill (zero), TTL/candle hết | CANCELLED | 0, hoàn tiền | Cancel consumer |
| LIMIT | Partial fill, TTL/candle hết | WIN/LOSS | binary trên phần đã fill, refund unfilled | Cancel consumer → Scheduler |
| LIMIT | Fully filled, candle đóng | WIN/LOSS | binary formula | Scheduler |
| LIMIT | Late fill (sau settlement_at) | WIN/LOSS | binary formula | Fill consumer |
| Bracket | Fill price >= TP | WIN/LOSS | `(tp_price - avg_price) × qty` | Fill consumer |
| Bracket | Fill price <= SL | WIN/LOSS | `(sl_price - avg_price) × qty` | Fill consumer |
| Bracket | TP chạm (real-time, full) | WIN/LOSS | `(exit_price - avg_price) × qty` | Bracket consumer |
| Bracket | SL chạm (real-time, full) | WIN/LOSS | `(exit_price - avg_price) × qty` | Bracket consumer |
| Bracket | TP/SL partial + candle | WIN/LOSS | shadow + binary remainder | Bracket consumer → Scheduler |
| Bracket | Candle đóng (không chạm TP/SL) | WIN/LOSS | binary formula | Scheduler |
| Any ME order | ME chưa fill tại settlement | CANCELLED | 0, hoàn tiền ngay | Scheduler |
| TTL hết, zero fill | Bất kỳ loại lệnh | CANCELLED | 0, hoàn tiền | Cancel consumer |
| TTL hết, partial fill | Bất kỳ loại lệnh | PENDING → settle | refund unfilled, binary trên filled | Cancel consumer → Scheduler |
| Stuck > 10 phút | Sweeper, có candle | WIN/LOSS | binary/shadow formula | Sweeper |
| Stuck > 10 phút | Sweeper, không có candle | CANCELLED | 0, hoàn tiền | Sweeper |
| Orphan (no settle) | Sweeper, > 2 giờ | CANCELLED | 0, hoàn tiền | Sweeper |

---

## Thứ tự ưu tiên khi đóng lệnh

```
1. Bracket instant settle    — fill price đã vượt TP/SL, settle ngay tại fill consumer
2. Bracket exit (TP/SL)      — ngay khi giá chạm, real-time (ME callback)
3. Late fill settle          — fill đến sau settlement_at, settle ngay tại fill consumer
4. Cancel consumer           — ngay khi TTL hết (zero → refund, partial → refund unfilled)
5. Normal settlement         — mỗi phút tại :05s sau settlement_at
   (ME chưa fill → cancel + refund ngay, không có grace period)
6. Stuck sweeper             — mỗi 5 phút, threshold +10 phút
```

---

## Bảo vệ chống double-settle

Tất cả các path đều check `bo.result == BOResult.PENDING` trước khi settle:

| Nơi xử lý | Check |
|---|---|
| `_handle_bracket_exit` | `bo.exit_trigger is None and bo.result == PENDING` |
| `_handle_order_fill` | `bo.result == PENDING` |
| `_handle_order_cancel` | `bo.result == PENDING` |
| `settle_pending_trades` | `bo.result != PENDING → continue` |
| `sweep_stuck_orders` | `bo.result != PENDING → continue` |

→ Ai đến trước settle, ai đến sau skip. Không thể double-settle.
