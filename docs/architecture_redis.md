# PolyArena — Thiết kế kiến trúc với Redis

## Mục lục

1. [Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
2. [So sánh kiến trúc hiện tại vs mới](#2-so-sánh-kiến-trúc-hiện-tại-vs-mới)
3. [Redis Key Schema](#3-redis-key-schema)
4. [WS Feed Service](#4-ws-feed-service)
5. [FastAPI App (thay đổi)](#5-fastapi-app-thay-đổi)
6. [Các luồng giao tiếp chi tiết](#6-các-luồng-giao-tiếp-chi-tiết)
7. [Deployment](#7-deployment)
8. [Xử lý lỗi và edge cases](#8-xử-lý-lỗi-và-edge-cases)
9. [So sánh điểm kết nối trước / sau](#9-so-sánh-điểm-kết-nối-trước--sau)
10. [Profit Matrix — Công thức tính lãi/lỗ](#10-profit-matrix--công-thức-tính-lãilỗ)

---

## 1. Tổng quan kiến trúc

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              EXTERNAL                                       │
│                                                                             │
│   ┌──────────────────┐              ┌──────────────────────┐               │
│   │  Polymarket      │              │  Binance REST API    │               │
│   │  CLOB WebSocket  │              │  /api/v3/klines      │               │
│   └────────┬─────────┘              └──────────┬───────────┘               │
└────────────│───────────────────────────────────│───────────────────────────┘
             │ WS events                          │ candle data (settlement)
             │                                    │
┌────────────▼───────────┐            ┌───────────▼───────────────────────────┐
│                        │            │                                       │
│   WS FEED SERVICE      │            │   FASTAPI APP                         │
│   (standalone process) │            │   (uvicorn workers)                   │
│                        │            │                                       │
│   ┌──────────────────┐ │            │   ┌─────────────────────────────────┐ │
│   │ PolymarketFeed   │ │            │   │ POST /binary-options/           │ │
│   │ (asyncio WS)     │ │            │   │  → MARKET: read price Redis     │ │
│   └────────┬─────────┘ │            │   │  → LIMIT:  use bot's price      │ │
│            │           │            │   │  → INSERT DB                    │ │
│   ┌────────▼─────────┐ │            │   │  → push order to Redis queue    │ │
│   │ MatchingEngine   │ │            │   │    (if LIMIT or has TP/SL)      │ │
│   │ ShadowOrderbook  │ │            │   └─────────────────────────────────┘ │
│   │ (in-memory)      │ │            │                                       │
│   └────────┬─────────┘ │            │   ┌─────────────────────────────────┐ │
│            │           │            │   │ Background: consume             │ │
│   ┌────────▼─────────┐ │            │   │ stream:bracket:exits            │ │
│   │ Redis Writer     │ │            │   │  → UPDATE DB exit data          │ │
│   │ + Queue Consumer │ │            │   └─────────────────────────────────┘ │
│   │ + TokenRegistry  │ │            │                                       │
│   └──────────────────┘ │            │   ┌─────────────────────────────────┐ │
│                        │            │   │ APScheduler                     │ │
└───────────┬────────────┘            │   │  → settle_pending_trades()      │ │
            │                         │   │  → Binance fetch + DB update    │ │
            │                         │   └─────────────────────────────────┘ │
            │                         └──────────────┬────────────────────────┘
            │                                        │
┌───────────▼────────────────────────────────────────▼────────────────────────┐
│                              REDIS                                          │
│                                                                             │
│  price:{SYM}:{TF}:{DIR}       ← price cache (WS Feed writes)               │
│  queue:orders:new             ← new virtual orders (FastAPI → WS Feed)     │
│  stream:bracket:exits         ← TP/SL events (WS Feed → FastAPI)           │
│                                                                             │
└──────────────────────────────────────────────┬──────────────────────────────┘
                                               │
                                    ┌──────────▼──────────┐
                                    │   SQLite / DB        │
                                    │   orders.db          │
                                    │   binary_options     │
                                    │   bots               │
                                    └─────────────────────┘
```

---

## 2. So sánh kiến trúc hiện tại vs mới

### Kiến trúc hiện tại — vấn đề

```
FastAPI process
├── lifespan startup:
│     _discover_token_ids()  ← gọi REST Polymarket mỗi lần khởi động
│     start_feed(token_ids)  ← WS Feed chạy bên trong FastAPI
│                               (asyncio task trong cùng event loop)
│
├── create_bo():
│     get_engine().best_ask()        ← đọc trực tiếp từ MatchingEngine (in-process)
│     get_engine().place_virtual_order() ← gọi trực tiếp
│
└── WS Feed:
      dispatch_event() → ShadowOrderbook
      bracket exit callback → SessionLocal() → DB write
      (từ matching engine thread, không liên quan event loop)
```

**Hạn chế:**
| Vấn đề | Mô tả |
|--------|-------|
| Tight coupling | WS Feed phụ thuộc vào FastAPI process — restart API = mất WS connection |
| Single process | Không scale được API workers (mỗi worker có matching engine riêng) |
| Token discovery | Gọi Polymarket REST mỗi lần API khởi động |
| Token rotation | token_id đổi mỗi candle — không có cơ chế refresh tự động |
| Thread model phức tạp | Matching engine thread gọi `SessionLocal()` để write DB |

### Kiến trúc mới — tách biệt

```
Process 1: WS Feed Service (độc lập)
  - Kết nối Polymarket WS
  - Chạy MatchingEngine + ShadowOrderbook
  - TokenRegistry: refresh token_ids tại candle boundary
  - Đọc/ghi Redis
  - Không biết gì về FastAPI

Process 2: FastAPI App (stateless về price data)
  - Đọc price từ Redis (MARKET orders)
  - Dùng limit_price từ bot (LIMIT orders)
  - Push virtual orders vào Redis queue (nếu LIMIT hoặc có TP/SL)
  - Consume bracket exit stream từ Redis → DB
  - Không import WS Feed, không import MatchingEngine
```

**Lợi ích:**
| Điểm | Mô tả |
|------|-------|
| Độc lập | Restart API không ảnh hưởng WS connection |
| Horizontal scale | Nhiều API workers đọc cùng Redis — price data nhất quán |
| Đơn giản hóa API | Không cần threading phức tạp trong FastAPI |
| Khả năng quan sát | Redis là single source of truth cho price data |
| Token rotation | TokenRegistry tự xử lý trong WS Feed service |

---

## 3. Redis Key Schema

### 3.1 Price Cache

```
Key:    price:{SYMBOL}:{TIMEFRAME}:{DIRECTION}
Type:   Hash
TTL:    60 giây  (auto-expire nếu WS Feed mất kết nối)

Fields:
  price       string    "0.5231"
  token_id    string    "71321045019655954..."
  updated_at  string    "2024-01-01T12:00:05Z"  (ISO 8601 UTC)

Ví dụ:
  price:BTC:M5:UP   → { price:"0.5231", token_id:"abc...", updated_at:"..." }
  price:BTC:M5:DOWN → { price:"0.4769", token_id:"def...", updated_at:"..." }
  price:ETH:M15:UP  → { price:"0.5100", token_id:"ghi...", updated_at:"..." }

Tổng số keys = 4 symbols × 3 timeframes × 2 directions = 24 keys

Viết bởi:  WS Feed — mỗi khi nhận event best_bid_ask / price_change / book
Đọc bởi:   FastAPI create_bo() — chỉ cho MARKET orders
Stale nếu: updated_at > 30s trước HOẶC key không tồn tại (TTL hết)

Lưu ý: token_id trong key này tương ứng với candle hiện tại.
        WS Feed tự cập nhật sau mỗi candle boundary (qua TokenRegistry).
```

### 3.2 New Order Queue

```
Key:    queue:orders:new
Type:   List  (LPUSH producer / BRPOP consumer)

Value:  JSON string
{
  "bo_id":       1,
  "token_id":    "71321045...",
  "side":        "BUY",
  "price":       "0.5231",      # entry price (limit_price nếu LIMIT, avg_price nếu MARKET)
  "quantity":    "191.93",
  "limit_price": "0.48",        # null nếu là MARKET order; set nếu là LIMIT order
  "tp_price":    "0.70",        # null nếu không có
  "sl_price":    "0.35",        # null nếu không có
  "timeframe":   "M5",
  "expire_at":   "2024-01-01T12:05:00Z"   # candle_expire_at đã tính sẵn
}

Điều kiện push: (is_limit=True) HOẶC (tp_price is not None hoặc sl_price is not None)
  - LIMIT order (dù có hay không có TP/SL): push để WS Feed tracking TTL expiry
  - MARKET order có bracket (TP và/hoặc SL): push để WS Feed tracking TP/SL exit

Viết bởi:  FastAPI create_bo()
Đọc bởi:   WS Feed — background thread BRPOP (blocking pop, timeout=1s)
```

### 3.3 Bracket Exit Stream

```
Key:    stream:bracket:exits
Type:   Redis Stream  (XADD producer / XREADGROUP consumer)

Fields per entry:
  bo_id         "1"
  exit_trigger  "TP"  |  "SL"
  exit_price    "0.7230"
  exit_filled   "191.93"
  order_id      "550e8400-e29b-..."
  fired_at      "2024-01-01T12:03:45Z"

Consumer group:  api-workers
Consumer name:   api-{worker_pid}

MAXLEN:  ~10000 entries  (XADD MAXLEN ~ 10000)

Viết bởi:  WS Feed — khi TP hoặc SL fire
Đọc bởi:   FastAPI background task — XREADGROUP BLOCK 0
```

### Tổng hợp

```
┌──────────────────────────────┬────────────────┬────────────┬────────────┐
│ Key                          │ Type           │ Viết bởi   │ Đọc bởi    │
├──────────────────────────────┼────────────────┼────────────┼────────────┤
│ price:{S}:{TF}:{DIR}         │ Hash + TTL 60s │ WS Feed    │ FastAPI    │
│ queue:orders:new             │ List           │ FastAPI    │ WS Feed    │
│ stream:bracket:exits         │ Stream         │ WS Feed    │ FastAPI    │
└──────────────────────────────┴────────────────┴────────────┴────────────┘
```

---

## 4. WS Feed Service

### 4.1 Cấu trúc

```
ws_feed_service/
├── main.py              ← entry point, asyncio.run(main())
├── feed.py              ← PolymarketFeed (hiện tại services/ws_feed.py)
├── token_registry.py    ← TokenRegistry — candle-boundary token_id refresh
├── redis_writer.py      ← ghi price cache, publish bracket exits
├── order_consumer.py    ← BRPOP queue:orders:new, register virtual orders
└── config.py            ← REDIS_URL, symbols, timeframes, etc.
```

### 4.2 Startup flow

```
main()
  │
  ├── 1. Kết nối Redis
  │        redis = aioredis.from_url(REDIS_URL)
  │
  ├── 2. Khởi tạo TokenRegistry
  │        registry = TokenRegistry(
  │            symbols    = [BTC, ETH, SOL, XRP],
  │            timeframes = [M5, M15, H1],
  │            on_new_tokens = lambda ids: feed.add_tokens(ids),
  │        )
  │
  ├── 3. Discover token IDs lần đầu (blocking, trước khi event loop)
  │        token_ids = registry.discover_all()
  │        #  → gọi REST Polymarket cho mọi sym/tf/dir combo
  │        #  → lưu vào registry._mapping: {(sym,tf,dir): token_id}
  │
  ├── 4. Khởi động WS Feed
  │        feed = PolymarketFeed(token_ids)
  │        await feed.start()
  │
  ├── 5. Khởi động TokenRegistry refresh loop (background task)
  │        await registry.start()
  │        #  → _refresh_loop(): ngủ đến candle boundary + 5s
  │        #  → gọi _refresh_timeframe_with_retry() với 6 lần retry × 5s
  │        #  → token_ids mới → on_new_tokens() → feed.add_tokens()
  │        #  → add_tokens() gửi SUBSCRIBE ngay lên WS nếu đang kết nối
  │
  ├── 6. Khởi động Order Consumer (background task)
  │        asyncio.create_task(order_consumer.run())
  │
  └── 7. Chạy indefinitely (reconnect loop trong feed)
```

**Token rotation (TokenRegistry):**

```
Mỗi candle M5/M15/H1 trên Polymarket có token_id riêng theo slug:
  M5, M15: {symbol}-updown-{tf}-{candle_open_unix_ts}
  H1:      {symbol}-up-or-down-{month}-{day}-{hour}{am|pm}-et

_refresh_loop() hoạt động:
  loop:
    next_boundary_ts = (floor(now/period) + 1) * period   # boundary kế tiếp
    refresh_ts       = next_boundary_ts + REFRESH_OFFSET_S  (5s)
    sleep(refresh_ts - now)

    _refresh_timeframe_with_retry(tf):
      for attempt in range(6):
        new_ids = await _fetch_timeframe(tf)   # HTTP blocking in executor
        if new_ids is not None:
          on_new_tokens(new_ids)   # → feed.add_tokens()
          break
        await asyncio.sleep(5)    # retry sau 5s nếu market chưa publish
```

### 4.3 Xử lý WS events → Redis

```
Mỗi event từ Polymarket:
  │
  ├── book / price_change
  │     ShadowOrderbook.apply_snapshot() / apply_changes()
  │     ShadowOrderbook.run_matching()   ← fill PENDING/PARTIAL orders, expire TTL
  │     redis_writer.update_price(sym, tf, dir, best_ask, token_id)
  │       HSET price:{sym}:{tf}:{dir} price {val} token_id {id} updated_at {ts}
  │       EXPIRE price:{sym}:{tf}:{dir} 60
  │
  ├── best_bid_ask
  │     ShadowOrderbook.apply_changes()   ← lightweight update top-of-book
  │     ShadowOrderbook.monitor_bracket_orders()
  │       → nếu TP/SL fire:
  │           (callbacks collected inside lock, fired OUTSIDE lock)
  │           redis_writer.publish_bracket_exit(bo_id, trigger, exit_price, exit_filled)
  │             XADD stream:bracket:exits MAXLEN ~ 10000 *
  │               bo_id {id} exit_trigger {TP|SL} exit_price {p} exit_filled {q}
  │     redis_writer.update_price(...)   ← cập nhật best_ask từ event
  │
  ├── last_trade_price
  │     ShadowOrderbook.record_trade()
  │     ShadowOrderbook.monitor_bracket_orders()  (giống trên)
  │
  └── market_resolved
        ShadowOrderbook.cancel_all_virtual()
```

### 4.4 Order Consumer

```
order_consumer.run()     ← chạy trong background thread (bên cạnh asyncio loop)
  │
  └── loop:
        payload = redis.brpop("queue:orders:new", timeout=1)
        if payload is None: continue

        data = json.loads(payload[1])
        book = engine.get_or_create_book(data["token_id"])

        is_limit  = data["limit_price"] is not None
        has_bracket = data["tp_price"] or data["sl_price"]

        book.place_virtual_order(
          side            = BUY,
          price           = Decimal(data["price"]),
          quantity        = Decimal(data["quantity"]),
          tp_price        = Decimal(data["tp_price"]) if data["tp_price"] else None,
          sl_price        = Decimal(data["sl_price"]) if data["sl_price"] else None,
          timeframe       = data["timeframe"],
          expire_at       = data["expire_at"],   # candle settlement time — TTL for LIMIT
          on_bracket_exit = (
              lambda r: redis_writer.publish_bracket_exit(
                  bo_id       = data["bo_id"],
                  trigger     = r.trigger,
                  exit_price  = float(r.avg_exit_price),
                  exit_filled = float(r.qty_exited),
                  order_id    = r.order_id,
              )
          ) if has_bracket else None
        )

Lưu ý:
  - LIMIT order không có TP/SL: on_bracket_exit=None, nhưng vẫn có expire_at
    → ShadowOrderbook tự expire order khi candle kết thúc
  - LIMIT order có TP/SL: tracking cả fill + bracket exit
  - MARKET order có TP/SL: tracking bracket exit, không cần fill tracking
```

---

## 5. FastAPI App (thay đổi)

### 5.1 Startup lifespan — đơn giản hóa

```python
# TRƯỚC (main.py)
@asynccontextmanager
async def lifespan(app):
    Base.metadata.create_all(bind=engine)
    run_migrations()
    start_scheduler()
    me = get_engine()
    token_ids = _discover_token_ids()    # ← GỌI REST POLYMARKET
    if token_ids:
        await start_feed(token_ids)      # ← CHẠY WS FEED TRONG PROCESS NÀY
    yield
    await stop_feed()
    me.shutdown()
    stop_scheduler()

# SAU (main.py)
@asynccontextmanager
async def lifespan(app):
    Base.metadata.create_all(bind=engine)
    run_migrations()
    start_scheduler()
    asyncio.create_task(_consume_bracket_exits())   # ← CHỈ CÒN THẾ NÀY
    yield
    stop_scheduler()
```

### 5.2 create_bo() — MARKET vs LIMIT order type

```
POST /poly-arena/binary-options/
  │
  ├── 1. Xác thực bot (api_key header)
  │
  ├── 2. Xác định order type
  │       is_limit = (payload.limit_price is not None)
  │
  ├── 3a. LIMIT order (limit_price được bot chỉ định)
  │       entry_price = payload.limit_price     ← dùng trực tiếp
  │       token_id    = engine.best_ask(token_id)[1]  ← chỉ cần token_id
  │         fallback: REST Polymarket CLOB nếu engine miss
  │       price_source = "limit"
  │
  ├── 3b. MARKET order (limit_price=None)
  │       Ưu tiên 1 — Redis (WS Feed cache):
  │         redis.hgetall("price:BTC:M5:UP")
  │           → { price, token_id, updated_at }
  │           kiểm tra staleness: now - updated_at > 30s → STALE
  │         HIT + FRESH:  min_ask = price, price_source = "redis"
  │
  │       Ưu tiên 2 — REST Polymarket CLOB (nếu Redis miss/stale):
  │         GET /orderbook?symbol=BTC&tf=M5&direction=UP
  │           → { min_ask, token_id }
  │         price_source = "rest"
  │
  │       entry_price = min_ask
  │
  ├── 4. Tính toán + lưu DB
  │       num_shares    = payload.amount / entry_price
  │       settlement_at = calc_settlement_time(timeframe)
  │       bo = BinaryOption(
  │               avg_price=entry_price, num_shares=num_shares,
  │               limit_price=payload.limit_price,  # None nếu MARKET
  │               tp_price=payload.tp_price,
  │               sl_price=payload.sl_price,
  │               ...
  │            )
  │       db.add(bo) + db.commit()
  │
  └── 5. Virtual order — điều kiện: (is_limit) OR (has TP/SL)
          should_place_virtual = (is_limit or has_bracket) and token_id is not None
          if should_place_virtual:
            engine.place_virtual_order(
              token_id=token_id, price=entry_price, quantity=num_shares,
              tp_price=tp_price, sl_price=sl_price, timeframe=timeframe,
              on_bracket_exit=callback if has_bracket else None,
            )
            bo.me_order_id = me_order.order_id
            db.commit()

          Trong kiến trúc Redis: thay engine.place_virtual_order() bằng:
            redis.lpush("queue:orders:new", JSON({
              bo_id, token_id, price, quantity,
              limit_price, tp_price, sl_price,
              timeframe, expire_at,
            }))
```

### 5.3 Background task — consume bracket exits

```python
async def _consume_bracket_exits():
    """
    Đọc stream:bracket:exits từ Redis và ghi vào DB.
    Chạy vĩnh viễn trong background của FastAPI app.
    """
    redis = await aioredis.from_url(REDIS_URL)

    # Tạo consumer group nếu chưa có
    try:
        await redis.xgroup_create("stream:bracket:exits", "api-workers", id="0", mkstream=True)
    except ResponseError:
        pass  # group đã tồn tại

    consumer_name = f"api-{os.getpid()}"

    while True:
        entries = await redis.xreadgroup(
            groupname    = "api-workers",
            consumername = consumer_name,
            streams      = {"stream:bracket:exits": ">"},
            count        = 10,
            block        = 0,     # block indefinitely
        )

        for stream, messages in entries:
            for msg_id, fields in messages:
                bo_id        = int(fields["bo_id"])
                exit_trigger = fields["exit_trigger"]
                exit_price   = float(fields["exit_price"])
                exit_filled  = float(fields["exit_filled"])

                db = SessionLocal()
                try:
                    bo = db.get(BinaryOption, bo_id)
                    if bo and bo.exit_trigger is None:  # idempotent
                        bo.exit_trigger = exit_trigger
                        bo.exit_price   = exit_price
                        bo.exit_filled  = exit_filled
                        db.commit()
                finally:
                    db.close()

                await redis.xack("stream:bracket:exits", "api-workers", msg_id)
```

---

## 6. Các luồng giao tiếp chi tiết

### Flow A1 — MARKET order, không có TP/SL

```
Client          FastAPI                   Redis                  DB
  │                │                        │                     │
  │─── POST ──────►│  limit_price=None      │                     │
  │                │─── HGETALL price:.. ──►│                     │
  │                │◄── {price, token_id} ──│                     │
  │                │    (price_source=redis) │                     │
  │                │                        │                     │
  │                │─── INSERT bo ─────────────────────────────►  │
  │                │    (avg_price=price,    │                     │
  │                │     me_order_id=NULL)   │                     │
  │                │                        │                     │
  │◄─── 201 ───────│                        │                     │
  │                │  (không push queue)    │                     │
```

### Flow A2 — LIMIT order, không có TP/SL

```
Client          FastAPI                   Redis             WS Feed          DB
  │                │                        │                  │               │
  │─── POST ──────►│  limit_price=0.48      │                  │               │
  │                │  (bỏ qua giá Redis)    │                  │               │
  │                │─── HGETALL price:.. ──►│                  │               │
  │                │◄── {_, token_id} ──────│  (chỉ lấy token) │               │
  │                │                        │                  │               │
  │                │─── INSERT bo ─────────────────────────────────────────►  │
  │                │    (avg_price=0.48,     │                  │               │
  │                │     limit_price=0.48)  │                  │               │
  │                │                        │                  │               │
  │                │─── LPUSH orders:new ──►│                  │               │
  │                │    {bo_id,             │◄── BRPOP ────────│               │
  │                │     limit_price=0.48,  │                  │               │
  │                │     tp=null, sl=null}  │ place_virtual_order()            │
  │                │                        │ (LIMIT, no bracket)             │
  │                │                        │ expire_at=settlement_at         │
  │◄─── 201 ───────│                        │                  │               │

Mục đích: WS Feed theo dõi virtual order TTL.
Khi candle kết thúc, ShadowOrderbook tự expire order.
Lệnh được settle theo binary formula (candle open vs close).
```

### Flow B — MARKET order, có TP/SL

```
Client          FastAPI                   Redis             WS Feed          DB
  │                │                        │                  │               │
  │─── POST ──────►│  limit_price=None      │                  │               │
  │                │  tp_price=0.70         │                  │               │
  │                │─── HGETALL price:.. ──►│                  │               │
  │                │◄── {price, token_id} ──│                  │               │
  │                │                        │                  │               │
  │                │─── INSERT bo ─────────────────────────────────────────►  │
  │                │                        │                  │               │
  │                │─── LPUSH orders:new ──►│                  │               │
  │                │    {bo_id,             │◄── BRPOP ────────│               │
  │                │     tp=0.70, sl=null}  │                  │               │
  │                │                        │ place_virtual_order()            │
  │                │                        │ on_bracket_exit registered      │
  │◄─── 201 ───────│                        │                  │               │
```

### Flow C — WS event → TP fire → DB write

```
Polymarket      WS Feed                  Redis              FastAPI           DB
  │                │                        │                  │               │
  │── best_bid ───►│                        │                  │               │
  │   ask event    │ monitor_bracket()      │                  │               │
  │                │ bid >= tp_price        │                  │               │
  │                │ collect callbacks      │                  │               │
  │                │ (inside lock)          │                  │               │
  │                │ fire callbacks         │                  │               │
  │                │ (OUTSIDE lock)         │                  │               │
  │                │                        │                  │               │
  │                │── XADD bracket:exits ─►│                  │               │
  │                │   {bo_id,"TP",...}     │                  │               │
  │                │                        │◄─ XREADGROUP ────│               │
  │                │                        │   BLOCK 0        │               │
  │                │                        │                  │               │
  │                │                        │──── entries ─────►│              │
  │                │                        │                  │── UPDATE bo ─►│
  │                │                        │                  │   exit_trigger│
  │                │                        │                  │   exit_price  │
  │                │                        │                  │── XACK ──────►│
```

### Flow D — Redis MISS → REST fallback

```
Client          FastAPI                   Redis             Polymarket REST
  │                │                        │                  │
  │─── POST ──────►│                        │                  │
  │                │─── HGETALL price:.. ──►│                  │
  │                │◄── nil (TTL expired) ──│                  │
  │                │    STALE / MISS        │                  │
  │                │                        │                  │
  │                │── GET /orderbook ─────────────────────────►│
  │                │◄── {min_ask, token_id} ────────────────────│
  │                │                        │                  │
  │                │─── INSERT bo ──────────────────────────────────────────────► DB
  │◄─── 201 ───────│
```

### Flow E — Settlement (profit matrix)

```
APScheduler     FastAPI               Binance REST            DB
  │                │                        │                  │
  │ :05/min ──────►│                        │                  │
  │                │── SELECT PENDING ─────────────────────────►│
  │                │◄── [bo1, bo2, ...] ───────────────────────│
  │                │                        │                  │
  │                │── GET /klines ─────────►│                  │
  │                │◄── (open, close) ───────│                  │
  │                │                        │                  │
  │                │ check bo.exit_trigger?  │                  │
  │                │                        │                  │
  │                │   ── Shadow tracking (exit_trigger="TP"/"SL") ──────────────
  │                │   result  = WIN  if exit_trigger="TP" else LOSS
  │                │   profit  = (exit_price - avg_price) × exit_filled
  │                │                        │                  │
  │                │   ── Binary settlement (exit_trigger=None) ──────────────────
  │                │   candle_dir = GREEN if close > open else RED
  │                │   result  = WIN  if candle_dir == bo.forecast else LOSS
  │                │   profit  = (1 - avg_price) × num_shares   (WIN)
  │                │   profit  = -amount                         (LOSS)
  │                │                        │                  │
  │                │── UPDATE result/profit ────────────────────►│
  │                │── UPDATE bot.balance ──────────────────────►│
```

### Flow F — Token rotation (candle boundary)

```
TokenRegistry   WS Feed               Polymarket REST      PolymarketFeed (WS)
  │                │                        │                  │
  │ sleep to       │                        │                  │
  │ boundary+5s    │                        │                  │
  │                │                        │                  │
  │ _fetch_tf(M5) ►│                        │                  │
  │                │── GET /orderbook M5 ───►│                  │
  │                │◄── new_token_id ────────│                  │
  │                │                        │                  │
  │ on_new_tokens()│                        │                  │
  │ [new_id_up,    │                        │                  │
  │  new_id_down]  │                        │                  │
  │                │── feed.add_tokens() ───────────────────────►│
  │                │                        │    SUBSCRIBE {new_ids}
  │                │                        │    (sent immediately if WS active)
```

---

## 7. Deployment

### Docker Compose

```yaml
version: "3.9"

services:

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru
    volumes:
      - redis_data:/data

  ws_feed:
    build:
      context: .
      dockerfile: Dockerfile.ws_feed
    environment:
      - REDIS_URL=redis://redis:6379
      - POLYMARKET_WS=wss://ws-subscriptions-clob.polymarket.com/ws/market
    depends_on:
      - redis
    restart: always
    # Single instance — quản lý 1 WS connection + TokenRegistry

  api:
    build:
      context: .
      dockerfile: Dockerfile.api
    environment:
      - REDIS_URL=redis://redis:6379
      - DATABASE_URL=sqlite:///./orders.db
    ports:
      - "8000:8000"
    depends_on:
      - redis
    command: uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
    # Nhiều workers — đều đọc cùng Redis
    volumes:
      - db_data:/app/data

volumes:
  redis_data:
  db_data:
```

### Sơ đồ process

```
┌─────────────────────────────────────────────────────────────┐
│  Host / Docker network                                      │
│                                                             │
│  ┌──────────────┐     ┌─────────────┐     ┌─────────────┐ │
│  │  ws_feed     │     │  redis:6379 │     │  api        │ │
│  │  (1 process) │◄───►│             │◄───►│  (4 workers)│ │
│  │              │     │  price      │     │             │ │
│  │  Polymarket  │     │  queue      │     │  :8000      │ │
│  │  WS conn     │     │  stream     │     │             │ │
│  │  TokenReg.   │     │             │     │             │ │
│  └──────────────┘     └─────────────┘     └──────┬──────┘ │
│                                                   │        │
│                                          ┌────────▼──────┐ │
│                                          │  orders.db    │ │
│                                          │  (SQLite)     │ │
│                                          └───────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## 8. Xử lý lỗi và edge cases

### 8.1 Redis miss / stale — FastAPI

```
price:{SYM}:{TF}:{DIR} không tồn tại hoặc TTL hết
  └── FastAPI fallback REST Polymarket CLOB
      └── nếu Polymarket cũng down → 502 Bad Gateway

Lưu ý: LIMIT orders không đọc Redis để lấy giá — không bị ảnh hưởng.
        Nhưng vẫn cần Redis để lấy token_id cho virtual order placement.
```

### 8.2 WS Feed restart — virtual orders mất

Khi WS Feed restart, tất cả `SimulatedOrder` in-memory bị mất.

```
WS Feed startup:
  1. Kết nối Redis
  2. discover_all() qua TokenRegistry (blocking REST)
  3. Reconnect Polymarket WS
  4. Recovery: đọc tất cả BO PENDING có me_order_id từ DB
              re-push chúng vào queue:orders:new
              → order_consumer sẽ re-register vào ShadowOrderbook

  (DB là source of truth — WS Feed rebuild state từ DB)
```

### 8.3 Bracket exit trùng lặp (idempotency)

```
FastAPI consume stream:
  if bo.exit_trigger is None:   ← chỉ ghi nếu chưa có
    bo.exit_trigger = ...
    db.commit()
  XACK()   ← luôn ACK, kể cả khi đã xử lý rồi
```

### 8.4 Redis down — WS Feed

```
WS Feed redis_writer:
  try:
    HSET price:...
    EXPIRE ...
  except RedisConnectionError:
    logger.error(...)
    # WS Feed tiếp tục chạy — matching engine vẫn hoạt động
    # price cache không cập nhật → FastAPI tự động fallback REST
```

### 8.5 Redis down — FastAPI

```
create_bo():
  try:
    price = redis.hgetall(...)
  except RedisConnectionError:
    price = None   ← treat như MISS

  if price is None:
    # fallback REST (như bình thường)
```

### 8.6 Token rotation — candle mới chưa publish

```
TokenRegistry._refresh_timeframe_with_retry():
  Tại candle boundary + 5s, thị trường mới có thể chưa publish token_id mới.
  Strategy:
    retry 6 lần × 5s delay = tối đa 35s chờ sau boundary
    Mỗi retry: gọi PolymarketClient.get_orderbook() cho mọi sym/dir trong tf
    Nếu tất cả đều fail → return None (bỏ qua lần refresh này)
    Nếu một số fail → bỏ qua, cập nhật những cái thành công
    Nếu thành công → on_new_tokens([changed_ids]) → feed.add_tokens()

Trong khi chờ token mới:
  - WS Feed tiếp tục nhận events từ token cũ
  - Redis cache vẫn có price từ token cũ (TTL 60s)
  - FastAPI vẫn hoạt động bình thường
```

### 8.7 Bracket callbacks — thread safety

```
ShadowOrderbook.monitor_bracket_orders():
  Chạy trong matching engine thread.
  Callbacks (DB writes) phải chạy NGOÀI lock để tránh deadlock.

  Cách xử lý:
    pending_callbacks = []
    with self._lock:
        ... detect TP/SL ...
        if order._on_bracket_exit:
            pending_callbacks.append((cb, result))
    # Fire callbacks AFTER releasing lock
    for cb, res in pending_callbacks:
        try:
            cb(res)
        except Exception:
            logger.error(...)
```

---

## 9. So sánh điểm kết nối trước / sau

| Component | Trước | Sau |
|-----------|-------|-----|
| **WS Feed lifecycle** | Start trong FastAPI lifespan | Process độc lập, tự quản lý |
| **Price lookup** | `engine.best_ask(token_id)` — in-process | `redis.hgetall("price:...")` — network call |
| **LIMIT order price** | Không có | Bot chỉ định `limit_price`; không đọc Redis |
| **Token ID discovery** | `_discover_token_ids()` khi API khởi động | `TokenRegistry.discover_all()` trong WS Feed |
| **Token rotation** | Không có | `TokenRegistry._refresh_loop()` tại candle boundary |
| **Virtual order condition** | Chỉ MARKET + bracket | LIMIT order (bất kể bracket) hoặc MARKET + bracket |
| **Virtual order placement** | `engine.place_virtual_order()` — direct call | `redis.lpush("queue:orders:new", ...)` |
| **Bracket exit write-back** | Callback gọi `SessionLocal()` từ matching engine thread | WS Feed XADD → FastAPI task XREAD → DB |
| **Bracket callback thread safety** | Callback fired inside lock (risk) | Callbacks collected inside lock, fired outside |
| **API worker count** | 1 (dùng chung matching engine) | N (đều đọc Redis, stateless) |
| **Coupling** | FastAPI import `matching_engine`, `ws_feed` | FastAPI chỉ import `redis` |

---

## 10. Profit Matrix — Công thức tính lãi/lỗ

Công thức tính profit được chọn dựa vào cấu hình TP/SL và kết quả thực tế.

### Ma trận quyết định

| Cấu hình | TP/SL fire? | Result | Công thức profit |
|----------|-------------|--------|-----------------|
| Không có TP, không có SL | — | Binary (candle dir) | Binary settlement |
| Có TP, không có SL | TP fired | WIN | Shadow tracking |
| Có TP, không có SL | TP not fired | LOSS | Binary settlement |
| Không có TP, có SL | SL fired | LOSS | Shadow tracking |
| Không có TP, có SL | SL not fired | WIN | Binary settlement |
| Có cả TP và SL | TP fired | WIN | Shadow tracking |
| Có cả TP và SL | SL fired | LOSS | Shadow tracking |
| Có cả TP và SL | Không fired | Binary (candle dir) | Binary settlement |

### Công thức chi tiết

```
Shadow tracking (exit_trigger = "TP" hoặc "SL"):
  result = WIN  if exit_trigger == "TP"
  result = LOSS if exit_trigger == "SL"
  profit = (exit_price - avg_price) × exit_filled

Binary settlement (exit_trigger = None):
  candle_dir = GREEN if candle_close > candle_open else RED
  result = WIN  if candle_dir == bo.forecast else LOSS
  profit = (1 - avg_price) × num_shares   (WIN)
  profit = -amount                         (LOSS)
```

### Các trường liên quan trong BinaryOption

```
avg_price     float   Giá entry (Polymarket min_ask hoặc limit_price)
num_shares    float   amount / avg_price
limit_price   float?  None = MARKET order; set = LIMIT order
tp_price      float?  Giá Take Profit (tùy chọn)
sl_price      float?  Giá Stop Loss (tùy chọn)
exit_trigger  str?    "TP" | "SL" — set khi bracket fire; None = dùng binary
exit_price    float?  Avg exit price khi TP/SL fire
exit_filled   float?  Qty đã exit qua TP/SL
me_order_id   str?    ID virtual order trong ShadowOrderbook
```

### Luồng xử lý settlement

```python
# services/settlement.py
if bo.exit_trigger in ("TP", "SL") and bo.exit_price is not None:
    # Shadow tracking
    result = BOResult.WIN if bo.exit_trigger == "TP" else BOResult.LOSS
    profit = round((bo.exit_price - bo.avg_price) * bo.exit_filled, 8)
else:
    # Binary settlement
    candle_dir = "GREEN" if price_close > price_open else "RED"
    result = BOResult.WIN if candle_dir == bo.forecast else BOResult.LOSS
    if result == BOResult.WIN:
        profit = round((1 - bo.avg_price) * bo.num_shares, 8)
    else:
        profit = -bo.amount
```
