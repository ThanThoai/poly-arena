# PolyArena — System Flow

## Mục lục

1. [Flow 1 — Tạo lệnh BO (POST request)](#flow-1--tạo-lệnh-bo-post-request)
2. [Flow 2 — WebSocket Feed (Nhận dữ liệu Polymarket)](#flow-2--websocket-feed-nhận-dữ-liệu-polymarket)
3. [Flow 3 — Scheduler Settlement (Mỗi phút)](#flow-3--scheduler-settlement-mỗi-phút)
4. [Sơ đồ tổng hợp luồng dữ liệu](#sơ-đồ-tổng-hợp-luồng-dữ-liệu)

---

## Flow 1 — Tạo lệnh BO (POST request)

**Endpoint:** `POST /poly-arena/binary-options/`

```
Client (Bot)
     │
     │  POST /poly-arena/binary-options/
     │  Header: x-api-key: <key>
     │  Body: { symbol, timeframe, forecast, amount, tp_price?, sl_price? }
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  routers/binary_options.py :: create_bo()                                           │
│                                                                                     │
│  1. Xác thực bot                                                                    │
│     db.query(Bot).filter(api_key=x_api_key, is_active=True)                        │
│     └── 401 nếu không tìm thấy                                                      │
│                                                                                     │
│  2. Lấy giá — Fast path                                                             │
│     _try_engine_price(symbol, timeframe, pm_status)                                 │
│        ├── PolymarketClient.get_orderbook()  → lấy token_id                        │
│        └── engine.best_ask(token_id)         → đọc ShadowOrderbook (in-memory)     │
│                                                                                     │
│            HIT ──────────────────────────────────── min_ask, token_id              │
│            MISS ──► Slow path (REST fallback)                                       │
│                     PolymarketClient.get_orderbook() → REST API Polymarket CLOB     │
│                     → min_ask, token_id                                             │
│                     └── 502 nếu Polymarket unavailable                              │
│                                                                                     │
│  3. Tính toán                                                                       │
│     num_shares    = amount / min_ask                                                │
│     settlement_at = calc_settlement_time(timeframe, now)                            │
│                     └── (floor(now/period)+1) × period  [Binance candle close]      │
│                                                                                     │
│  4. Lưu DB                                                                          │
│     INSERT BinaryOption(                                                            │
│       bot_name, symbol, timeframe, forecast, amount,                                │
│       avg_price=min_ask, num_shares, settlement_at,                                 │
│       tp_price?, sl_price?,                                                         │
│       result=PENDING, profit=NULL                                                   │
│     )                                                                               │
└─────────────────────────┬───────────────────────────────────────────────────────────┘
                          │
              ┌───────────┴──────────────┐
              │ has_bracket?             │
              │ (tp_price OR sl_price)   │
              └───────────┬──────────────┘
                    NO    │     YES
                    │     │
                    │     ▼
                    │  ┌────────────────────────────────────────────────────────────┐
                    │  │  engine.place_virtual_order(token_id, BUY, price, qty,    │
                    │  │    tp=tp_price, sl=sl_price, timeframe=timeframe,          │
                    │  │    on_bracket_exit=_make_bracket_exit_callback(bo.id))    │
                    │  │                                                            │
                    │  │  ShadowOrderbook.place_virtual_order()                    │
                    │  │    ├── expire_at = candle_expire_at(timeframe)            │
                    │  │    ├── SimulatedOrder(... _on_bracket_exit=callback)      │
                    │  │    └── _match_order()  ← thử fill ngay với asks hiện có  │
                    │  │         ├── ask ≤ price → fill (FILLED / PARTIAL)         │
                    │  │         └── ask > price → PENDING                         │
                    │  │                                                            │
                    │  │  bo.me_order_id = me_order.order_id                       │
                    │  │  db.commit()                                               │
                    │  └────────────────────────────────────────────────────────────┘
                    │
                    ▼
              db.refresh(bo)
              return BOResponse(id, avg_price, settlement_at, me_order_id, ...)
                    │
                    ▼
              HTTP 201 Created  →  Client
```

---

## Flow 2 — WebSocket Feed (Nhận dữ liệu Polymarket)

```
Polymarket
CLOB WS                        services/ws_feed.py              services/matching_engine.py
──────────                     ───────────────────              ───────────────────────────

wss://ws-subscriptions          PolymarketFeed
-clob.polymarket.com             _run_forever()
     │                               │
     │   [reconnect loop]            │  ← exponential back-off (2s→60s) on disconnect
     │                               │
     │◄── subscribe({               │
     │      assets_ids: [token_ids] │
     │      type: "market"          │
     │    })                        │
     │                               │
     │──── PING ────────────────────►│  (every 10s heartbeat)
     │◄─── PONG ─────────────────────│
     │                               │
     │                               │  _handle_message(raw_msg)
     │                               │    json.loads(raw)
     │                               │    events = data if list else [data]
     │                               │    for event in events:
     │                               │      engine.dispatch_event(event)
     │                               │               │
     │                               │               ▼
     │                        ┌──────┴──── event_type? ────────────────────────┐
     │                        │                                                 │
     │   "book" ──────────────►  _handle_book()                                │
     │   (full snapshot)      │    book.apply_snapshot(bids, asks)             │
     │                        │      bids.clear(); asks.clear()                │
     │                        │      rebuild từ payload                        │
     │                        │    book.run_matching()                         │
     │                        │      _expire_pending_orders()                  │
     │                        │        PENDING → CANCELED  (nếu hết TTL)       │
     │                        │        PARTIAL → clamp qty=filled              │
     │                        │      for each PENDING/PARTIAL order:           │
     │                        │        _match_order(order)                     │
     │                        │          walk asks ascending                   │
     │                        │          fill nếu ask ≤ order.price            │
     │                        │          order._entry_cost += qty × ask_price  │
     │                        │          → PARTIAL / FILLED                    │
     │                        │                                                 │
     │   "price_change" ──────►  _handle_price_change()                        │
     │   (delta update)       │    book.apply_changes(changes)                 │
     │                        │      upsert / delete individual levels         │
     │                        │    book.run_matching()  (giống trên)           │
     │                        │                                                 │
     │   "best_bid_ask" ──────►  _handle_best_bid_ask()                        │
     │   (top of book)        │    book.apply_changes([bid, ask])              │
     │                        │    book.monitor_bracket_orders()               │
     │                        │      current_best_bid = max(bids)              │
     │                        │      for each eligible order:                  │
     │                        │        [ACQUIRE LOCK]                          │
     │                        │        TP check: bid >= tp_price?              │
     │                        │          YES → _execute_bracket_exit("TP")     │
     │                        │                walk bids descending (slippage) │
     │                        │                order.exit_price  = avg_exit    │
     │                        │                order.exit_trigger = "TP"       │
     │                        │                order.position_closed = True    │
     │                        │        SL check (OCO): bid <= sl_price?        │
     │                        │          YES → _execute_bracket_exit("SL")     │
     │                        │        pending_callbacks = [(cb, result), ...] │
     │                        │        [RELEASE LOCK]                          │
     │                        │        for cb, res in pending_callbacks:       │
     │                        │          cb(res)                               │
     │                        │            db = SessionLocal()                 │
     │                        │            bo.exit_trigger = "TP" / "SL"      │
     │                        │            bo.exit_price  = avg_exit           │
     │                        │            bo.exit_filled = qty_exited         │
     │                        │            db.commit()                         │
     │                        │                                                 │
     │   "last_trade_price" ──►  _handle_last_trade()                          │
     │   (real execution)     │    book.record_trade(price, size, side)       │
     │                        │    book.monitor_bracket_orders()  (giống trên)│
     │                        │                                                 │
     │   "market_resolved" ───►  _handle_market_resolved()                    │
     │                        │    book.cancel_all_virtual()                   │
     │                        │    tất cả PENDING/PARTIAL → CANCELED          │
     └────────────────────────┘                                                │
```

---

## Flow 3 — Scheduler Settlement (Mỗi phút)

```
APScheduler (background thread)
     │
     │  trigger: cron  minute="*"  second=5
     │  (chạy vào giây :05 mỗi phút — đợi Binance publish candle đã đóng)
     ▼
_run_settlement()
  db = SessionLocal()
  settle_pending_trades(db)
     │
     │  SELECT * FROM binary_options
     │  WHERE result = PENDING
     │    AND settlement_at IS NOT NULL
     │    AND settlement_at <= now
     │
     ├── không có → return
     │
     └── for each bo in pending:
               │
               ▼
          fetch_binance_candle(bo.symbol, bo.timeframe, bo.settlement_at)
          GET https://api.binance.com/api/v3/klines
            params: symbol=BTCUSDT, interval=5m,
                    startTime=settlement_at - period, limit=1
               │
               ├── None (lỗi / timeout) → skip, warning log
               │
               └── (open_price, close_price)
                       │
                       │  candle_dir = GREEN (close > open)
                       │             = RED   (close < open)
                       │             = GREEN (doji, close == open)
                       │
                       ▼
              ┌─────────────────────────────────────────┐
              │  bo.exit_trigger in ("TP", "SL")?       │
              │  AND bo.exit_price  is not None?        │
              │  AND bo.exit_filled is not None?        │
              └──────────────┬──────────────────────────┘
                     YES     │          NO
                      │      │           │
                      ▼      │           ▼
              Shadow formula │    Binary formula
              ───────────── │    ─────────────
              result = WIN   │    result = WIN  if candle_dir == bo.forecast
                  if "TP"    │              else LOSS
                    else LOSS│
                             │    WIN:  profit = (1 - avg_price) × num_shares
              profit =       │    LOSS: profit = -amount
              (exit_price    │
               - avg_price)  │
              × exit_filled  │
                      │      │           │
                      └──────┴───────────┘
                                  │
                                  ▼
                         bo.result      = WIN / LOSS
                         bo.profit      = profit
                         bo.price_open  = open_price
                         bo.price_close = close_price
                         bot.balance   += profit
                         INSERT BalanceHistory(bot_name, balance, trade_id)
                         db.commit()
```

---

## Sơ đồ tổng hợp luồng dữ liệu

```
                    ┌─────────────┐
                    │  Polymarket │
                    │  CLOB WS    │
                    └──────┬──────┘
                           │ book / price_change /
                           │ best_bid_ask / last_trade_price
                           ▼
              ┌─────────────────────────┐
              │   MatchingEngine        │     RAM (in-process)
              │   ShadowOrderbook       │◄──── lưu bids / asks / virtual orders
              │   (per token_id)        │
              └──────────┬──────────────┘
                         │
           ┌─────────────┼──────────────────┐
           │             │                  │
           ▼             ▼                  ▼
      run_matching() monitor_bracket()  best_ask()
      fill orders    TP/SL check        price query
      expire TTL     → callback()       ◄────────── create_bo() fast path
           │             │
           │             │  db = SessionLocal()
           │             │  UPDATE binary_options SET
           │             │    exit_trigger, exit_price, exit_filled
           │             │  db.commit()
           │             │
           └─────────────┼──────────────────────────┐
                         │                          │
                         ▼                          │
                  ┌──────────────┐                  │
                  │  SQLite DB   │                  │
                  │  orders.db   │                  │
                  │              │                  │
                  │ BinaryOption │◄─────────────────┘ create_bo() INSERT
                  │  result      │
                  │  profit      │◄── settlement UPDATE
                  │  exit_trigger│◄── bracket callback UPDATE
                  └──────┬───────┘
                         │
                         │  SELECT PENDING WHERE settlement_at <= now
                         ▼
              ┌─────────────────────┐
              │  APScheduler        │
              │  settle_pending_    │
              │  trades() :05/min   │
              └──────────┬──────────┘
                         │
                         │  GET /api/v3/klines
                         ▼
                  ┌──────────────┐
                  │  Binance API │
                  │  (open/close │
                  │   candle)    │
                  └──────────────┘
```

---

## Thread Safety

| Component | Threading model | Ghi chú |
|-----------|----------------|---------|
| `ShadowOrderbook` | `threading.Lock` per book | WS feed (asyncio thread) và FastAPI sync routes đều an toàn |
| `MatchingEngine._books` | `threading.Lock` riêng | Registry lock tách biệt với book lock |
| Bracket callbacks | Gọi **ngoài** book lock | Tránh deadlock khi callback mở DB session |
| `APScheduler` | Background thread riêng | File lock `/tmp/poly-arena-scheduler.lock` để chỉ 1 worker chạy |
| SQLite | `check_same_thread=False` | Mỗi thread mở `SessionLocal()` riêng |
