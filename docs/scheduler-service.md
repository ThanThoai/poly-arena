# Scheduler Service — Tách scheduler ra process độc lập

## Mục lục

1. [Vấn đề hiện tại](#1-vấn-đề-hiện-tại)
2. [Solution: Tách Scheduler Service](#2-solution-tách-scheduler-service)
3. [Kiến trúc mới (3 process)](#3-kiến-trúc-mới-3-process)
4. [Scheduler Service — Chi tiết thiết kế](#4-scheduler-service--chi-tiết-thiết-kế)
5. [Giao tiếp giữa các service](#5-giao-tiếp-giữa-các-service)
6. [TokenRegistry prefetch — Tính toán token_id trước](#6-tokenregistry-prefetch--tính-toán-token_id-trước)
7. [Thay đổi cần làm](#7-thay-đổi-cần-làm)
8. [Deployment](#8-deployment)
9. [Monitoring & Health check](#9-monitoring--health-check)
10. [Rollback plan](#10-rollback-plan)

---

## 1. Vấn đề hiện tại

### Kiến trúc hiện tại — Scheduler nằm trong FastAPI

```
FastAPI process (uvicorn)
├── API routes (create_bo, dashboard, ...)
├── APScheduler (background thread)
│     ├── settlement      — cron mỗi phút :05s
│     └── stuck-sweep     — interval 5 phút
├── _consume_bracket_exits()   — background asyncio task
├── _consume_order_fills()     — background asyncio task
└── _consume_order_cancels()   — background asyncio task
```

### Các vấn đề

| # | Vấn đề | Mô tả |
|---|--------|-------|
| 1 | **File lock hack** | `fcntl.flock(/tmp/poly-arena-scheduler.lock)` để chỉ 1 worker chạy scheduler — fragile, lock có thể bị stale nếu process crash |
| 2 | **Tight coupling** | Scheduler import `SessionLocal`, `settle_pending_trades` trực tiếp — phụ thuộc vào DB connection của FastAPI |
| 3 | **Không scale độc lập** | Restart API (deploy mới) → scheduler bị restart → có thể miss settlement window |
| 4 | **Resource contention** | Settlement job (Binance HTTP + DB write) chạy trên background thread cùng process với API → ảnh hưởng response time |
| 5 | **Observability kém** | Scheduler log lẫn với API log, khó phân tích khi có vấn đề |
| 6 | **Retry logic đơn giản** | Settlement fail → đợi đến lần chạy sau (1 phút) — không có backoff hay dead letter |
| 7 | **Single point of failure** | File lock không hoạt động cross-machine (nếu deploy nhiều host) |

---

## 2. Solution: Tách Scheduler Service

Tách scheduler ra thành **process độc lập** (Process 3), giống như đã tách WS Feed Service trước đó.

### So sánh trước / sau

| Khía cạnh | Trước (trong FastAPI) | Sau (service riêng) |
|-----------|----------------------|---------------------|
| **Lifecycle** | Start/stop cùng FastAPI lifespan | Process độc lập, tự quản lý |
| **Scaling** | File lock giữa workers | Duy nhất 1 process, không cần lock |
| **Deploy** | Restart API = restart scheduler | Deploy API không ảnh hưởng scheduler |
| **Monitoring** | Log lẫn với API | Log/metrics riêng, dễ alert |
| **Resource** | Chia sẻ CPU/memory với API | Riêng biệt, không tranh chấp |
| **DB access** | Qua SessionLocal (in-process) | Trực tiếp hoặc qua Redis command queue |
| **Retry** | Đợi lần schedule tiếp | Retry tức thì với backoff |

---

## 3. Kiến trúc mới (3 process)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              EXTERNAL                                           │
│                                                                                 │
│   ┌──────────────────┐       ┌──────────────────────┐                          │
│   │  Polymarket      │       │  Binance REST API    │                          │
│   │  CLOB WebSocket  │       │  /api/v3/klines      │                          │
│   └────────┬─────────┘       └──────────┬───────────┘                          │
└────────────│─────────────────────────────│──────────────────────────────────────┘
             │ WS events                    │ candle data
             │                              │
┌────────────▼──────────────┐   ┌───────────▼──────────────────────────────────────┐
│                            │   │                                                  │
│   PROCESS 1                │   │   PROCESS 3 ★ NEW                               │
│   WS FEED SERVICE          │   │   SCHEDULER SERVICE                             │
│   (standalone)             │   │   (standalone)                                   │
│                            │   │                                                  │
│   ┌──────────────────────┐ │   │   ┌──────────────────────────────────────────┐  │
│   │ PolymarketFeed       │ │   │   │ Settlement Loop                         │  │
│   │ MatchingEngine       │ │   │   │   settle_pending_trades()  — mỗi :05s   │  │
│   │ TokenRegistry        │ │   │   │   sweep_stuck_orders()     — mỗi 5m     │  │
│   │   + future prefetch  │ │   │   │   cancel_expired_limits()  — mỗi 30s    │  │
│   │ OrderConsumer        │ │   │   └──────────────────────────────────────────┘  │
│   │ RedisWriter          │ │   │                                                  │
│   └──────────────────────┘ │   │   ┌──────────────────────────────────────────┐  │
│                            │   │   │ Binance Client                           │  │
│                            │   │   │   fetch candle data với retry + backoff  │  │
└───────────┬────────────────┘   │   └──────────────────────────────────────────┘  │
            │                    │                                                  │
            │                    └──────────────────┬───────────────────────────────┘
            │                                       │
┌───────────▼───────────────────────────────────────▼───────────────────────────────┐
│                              REDIS                                                │
│                                                                                   │
│  price:{SYM}:{TF}:{DIR}         ← price cache (WS Feed writes)                   │
│  queue:orders:new               ← virtual orders (FastAPI → WS Feed)             │
│  stream:bracket:exits           ← TP/SL events (WS Feed → FastAPI)               │
│  stream:settlement:results ★    ← settlement results (Scheduler → FastAPI)       │
│  channel:scheduler:heartbeat ★  ← health monitoring                              │
│                                                                                   │
└───────────────────────────────────────────┬───────────────────────────────────────┘
                                            │
                              ┌─────────────▼────────────────┐
                              │                              │
                              │   PROCESS 2                  │
                              │   FASTAPI APP                │
                              │   (stateless API workers)    │
                              │                              │
                              │   ┌────────────────────────┐ │
                              │   │ POST /binary-options/  │ │
                              │   │ GET /dashboard/        │ │
                              │   └────────────────────────┘ │
                              │                              │
                              │   ┌────────────────────────┐ │
                              │   │ Background consumers:  │ │
                              │   │   bracket exits        │ │
                              │   │   order fills          │ │
                              │   │   order cancels        │ │
                              │   │   settlement results ★ │ │
                              │   └────────────────────────┘ │
                              │                              │
                              └──────────────┬───────────────┘
                                             │
                                   ┌─────────▼──────────┐
                                   │   SQLite / DB       │
                                   │   orders.db         │
                                   └────────────────────┘
```

---

## 4. Scheduler Service — Chi tiết thiết kế

### 4.1 Cấu trúc thư mục

```
scheduler_service/
├── main.py              ← entry point, asyncio.run(main())
├── config.py            ← REDIS_URL, schedule intervals, retry config
├── settlement.py        ← settlement + stuck sweep logic (di chuyển từ services/)
├── binance_client.py    ← Binance kline fetcher với retry + circuit breaker
└── health.py            ← health check endpoint + heartbeat publisher
```

### 4.2 Startup flow

```
main()
  │
  ├── 1. Kết nối Redis
  │        redis = aioredis.from_url(REDIS_URL)
  │
  ├── 2. Kết nối DB
  │        engine = create_engine(DATABASE_URL)
  │        SessionFactory = sessionmaker(bind=engine)
  │
  ├── 3. Đăng ký các scheduled jobs
  │        scheduler = AsyncIOScheduler()
  │        scheduler.add_job(settle_pending,    cron,     second=5)
  │        scheduler.add_job(sweep_stuck,       interval, minutes=5)
  │        scheduler.add_job(publish_heartbeat, interval, seconds=30)
  │
  ├── 4. Start scheduler
  │        scheduler.start()
  │
  └── 5. Wait for shutdown signal
           await shutdown_event.wait()
```

### 4.3 Settlement job — flow chi tiết

```
settle_pending_trades()
  │
  ├── 1. Query PENDING orders cần settle
  │       SELECT * FROM binary_options
  │       WHERE result = 'PENDING'
  │         AND settlement_at IS NOT NULL
  │         AND settlement_at <= now()
  │         AND me_order_status IN ('FILLED', NULL)
  │       ORDER BY settlement_at ASC
  │       LIMIT 100                        ← batch để tránh overload
  │
  ├── 2. Group theo (symbol, timeframe, settlement_at)
  │       → 1 Binance API call per group thay vì per order
  │       batches = group_by(orders, key=(symbol, tf, settlement_at))
  │
  ├── 3. Cho mỗi batch:
  │       candle = fetch_binance_candle(symbol, tf, settlement_at)
  │       if candle is None:
  │           retry với exponential backoff (1s, 2s, 4s, max 3 retries)
  │           if vẫn fail → skip batch, log warning
  │
  │       for bo in batch:
  │           result, profit = compute_settlement(bo, candle)
  │           bo.result = result
  │           bo.profit = profit
  │           bo.price_open  = candle.open
  │           bo.price_close = candle.close
  │
  │           # Update bot balance
  │           payout = bo.amount + profit
  │           bot.balance += payout
  │           INSERT BalanceHistory(...)
  │
  ├── 4. Commit DB
  │       db.commit()
  │
  └── 5. Publish kết quả qua Redis Stream (optional)
          XADD stream:settlement:results * bo_id {id} result {WIN/LOSS} profit {p}
          → FastAPI có thể notify client realtime
```

### 4.4 Stuck order sweep — flow

```
sweep_stuck_orders()
  │
  ├── Case A: settlement_at + 10min <= now, vẫn PENDING
  │     → Retry settlement (Binance candle chắc chắn available rồi)
  │
  ├── Case B: settlement_at IS NULL AND created_at + 2h <= now
  │     → Cancel + refund (bo.result = CANCELLED, profit = 0)
  │
  └── Case C: me_order_status = 'PENDING' AND created_at + 30min <= now
        → Cancel unfilled limit orders + refund
```

### 4.5 Retry & Error handling

```python
# scheduler_service/config.py

SETTLEMENT_RETRY_MAX     = 3
SETTLEMENT_RETRY_DELAY   = [1, 2, 4]    # exponential backoff (seconds)
BINANCE_TIMEOUT          = 10.0
BINANCE_CIRCUIT_BREAKER  = 5             # consecutive failures → circuit open 60s
SETTLEMENT_BATCH_SIZE    = 100           # max orders per settlement run
```

```
Retry flow:
  settle_pending_trades()
    ├── Binance API fail
    │     retry 1: wait 1s → retry
    │     retry 2: wait 2s → retry
    │     retry 3: wait 4s → retry
    │     max retries → skip batch, sẽ được sweep_stuck xử lý sau
    │
    ├── DB commit fail
    │     rollback → log error → retry batch lần chạy sau
    │
    └── Unexpected exception
          log.exception() → tiếp tục job tiếp theo
```

---

## 5. Giao tiếp giữa các service

### 5.1 Phương án A: Scheduler truy cập DB trực tiếp (recommended)

```
Scheduler Service ──── SQLAlchemy ────► SQLite/PostgreSQL ◄──── FastAPI
```

**Ưu điểm:** Đơn giản, settlement cần đọc/ghi nhiều trường, dùng ORM trực tiếp hiệu quả nhất.
**Nhược điểm:** Cả hai process share DB → cần cẩn thận với SQLite concurrent writes.

> **Lưu ý SQLite:** SQLite hỗ trợ WAL mode cho concurrent readers + 1 writer.
> Scheduler write ít (mỗi phút ~vài chục records) → conflict thấp.
> Nếu migrate sang PostgreSQL sau thì không có vấn đề gì.

### 5.2 Phương án B: Scheduler gửi kết quả qua Redis Stream

```
Scheduler ── XADD stream:settlement:results ──► Redis ◄── XREADGROUP ── FastAPI
```

FastAPI consume stream và ghi DB. Scheduler không cần truy cập DB trực tiếp.

**Ưu điểm:** Scheduler không cần DB connection, loose coupling hoàn toàn.
**Nhược điểm:** Phức tạp hơn, phải serialize/deserialize settlement data qua Redis.

### Đề xuất: Dùng **Phương án A** (DB trực tiếp) vì:
- Settlement logic cần đọc nhiều field từ BinaryOption
- Cần UPDATE bot.balance atomically
- Redis Stream chỉ thêm lớp complexity không cần thiết cho use case này

---

## 6. TokenRegistry prefetch — Tính toán token_id trước

### Hiện trạng (đã implement)

TokenRegistry bây giờ **prefetch token_id cho 5 candle tiếp theo**:

```
TokenRegistry._mapping:        (sym, tf, dir) → token_id hiện tại
TokenRegistry._future_mapping: (sym, tf, dir) → [token_id_+1, ..., token_id_+5]
```

### Flow tại candle boundary

```
Timeline (M5 example, 12:10:00 - 12:15:00):

12:10:00  candle opens
          _mapping[BTC,M5,UP] = "tok-abc" (current)
          _future_mapping[BTC,M5,UP] = ["tok-def", "tok-ghi", "tok-jkl", ...]
                                         +1 (12:15)  +2 (12:20)  +3 (12:25)

12:15:00  candle boundary → TokenRegistry refresh
          _mapping[BTC,M5,UP] = "tok-def"  (rotated, was future[0])
          _future_mapping[BTC,M5,UP] = ["tok-ghi", "tok-jkl", "tok-mno", ...]
                                         +1 (12:20)  +2 (12:25)  +3 (12:30)
```

### Lợi ích cho Scheduler Service

Khi Scheduler Service cần resolve `token_id` cho 1 order (ví dụ: recovery scenario),
nó có thể query Redis price cache bằng future token_id:

```
# Scheduler biết trước token_id cho candle tiếp theo
# → có thể pre-validate orders trước khi settlement
# → giảm delay khi cần cross-reference với Polymarket data
```

### API mới

```python
# Lấy token_id candle hiện tại
registry.get_token_id("BTC", "M5", "UP")
# → "tok-abc"

# Lấy token_id 5 candle tiếp theo
registry.get_future_token_ids("BTC", "M5", "UP")
# → ["tok-def", "tok-ghi", "tok-jkl", "tok-mno", "tok-pqr"]

# Tất cả token_ids (current + future, deduplicated)
registry.all_token_ids()
# → ["tok-abc", "tok-def", "tok-ghi", ...]
```

---

## 7. Thay đổi cần làm

### Phase 1: Tạo Scheduler Service (standalone)

| # | Task | File |
|---|------|------|
| 1 | Tạo `scheduler_service/main.py` — entry point | **NEW** |
| 2 | Tạo `scheduler_service/config.py` — config constants | **NEW** |
| 3 | Di chuyển settlement logic | `services/settlement.py` → giữ nguyên, import từ scheduler |
| 4 | Thêm retry + batch logic | `scheduler_service/main.py` |
| 5 | Thêm health check / heartbeat | `scheduler_service/health.py` |

### Phase 2: Xóa scheduler khỏi FastAPI

| # | Task | File |
|---|------|------|
| 6 | Xóa `start_scheduler()` / `stop_scheduler()` khỏi lifespan | `main.py` |
| 7 | Xóa hoặc deprecate | `services/scheduler.py` |
| 8 | Update dashboard endpoint đọc heartbeat từ Redis | `routers/dashboard.py` |
| 9 | Update docker-compose thêm scheduler service | `docker-compose.yml` |

### Phase 3: Nâng cấp (optional)

| # | Task | Mô tả |
|---|------|-------|
| 10 | Settlement result stream | XADD kết quả để FastAPI notify client realtime |
| 11 | Circuit breaker cho Binance | Tạm dừng fetch nếu Binance liên tục fail |
| 12 | Metrics export | Prometheus metrics: settlement_count, settlement_latency, errors |
| 13 | Dead letter queue | Orders fail settlement > N lần → chuyển sang manual review |

---

## 8. Deployment

### Docker Compose (3 services)

```yaml
version: "3.9"

services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru

  ws_feed:
    build: .
    command: python -m ws_feed_service.main
    environment:
      - REDIS_URL=redis://redis:6379
    depends_on:
      - redis
    restart: always
    # 1 instance — WS connection + TokenRegistry + MatchingEngine

  scheduler:                          # ★ NEW
    build: .
    command: python -m scheduler_service.main
    environment:
      - REDIS_URL=redis://redis:6379
      - DATABASE_URL=sqlite:///./data/orders.db
    depends_on:
      - redis
    restart: always
    volumes:
      - db_data:/app/data
    # 1 instance — settlement + stuck sweep

  api:
    build: .
    command: uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
    environment:
      - REDIS_URL=redis://redis:6379
      - DATABASE_URL=sqlite:///./data/orders.db
    ports:
      - "8000:8000"
    depends_on:
      - redis
    volumes:
      - db_data:/app/data
    # N workers — stateless, đọc Redis + DB

volumes:
  db_data:
```

### Sơ đồ process

```
┌──────────────────────────────────────────────────────────────────────┐
│  Host / Docker network                                               │
│                                                                      │
│  ┌──────────────┐  ┌─────────────┐  ┌──────────────┐  ┌──────────┐ │
│  │  ws_feed     │  │  redis:6379 │  │  scheduler   │  │  api     │ │
│  │  (1 process) │◄►│             │◄►│  (1 process) │  │  (:8000) │ │
│  │              │  │  price      │  │              │◄►│  4 wrkrs │ │
│  │  Polymarket  │  │  queue      │  │  settlement  │  │          │ │
│  │  WS conn     │  │  stream     │  │  stuck sweep │  │          │ │
│  │  TokenReg.   │  │  heartbeat  │  │  Binance API │  │          │ │
│  └──────────────┘  └─────────────┘  └──────┬───────┘  └────┬─────┘ │
│                                             │               │       │
│                                      ┌──────▼───────────────▼────┐  │
│                                      │  orders.db (shared volume)│  │
│                                      └───────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 9. Monitoring & Health check

### Heartbeat qua Redis

```python
# scheduler_service/health.py

async def publish_heartbeat(redis):
    """Publish mỗi 30s để API biết scheduler đang sống."""
    await redis.set(
        "scheduler:heartbeat",
        json.dumps({
            "pid": os.getpid(),
            "last_settlement": _last_settlement_ts,
            "orders_settled": _total_settled,
            "errors": _total_errors,
            "uptime_s": time.time() - _start_time,
        }),
        ex=60,  # TTL 60s — nếu không refresh = scheduler dead
    )
```

### Dashboard endpoint (FastAPI)

```python
# routers/dashboard.py

@router.get("/scheduler/status")
async def get_scheduler_status():
    redis = get_async_redis()
    raw = await redis.get("scheduler:heartbeat")
    if raw is None:
        return {"status": "DOWN", "message": "No heartbeat (scheduler not running?)"}

    data = json.loads(raw)
    return {
        "status": "UP",
        "pid": data["pid"],
        "last_settlement": data["last_settlement"],
        "orders_settled": data["orders_settled"],
        "errors": data["errors"],
        "uptime_s": data["uptime_s"],
    }
```

### Alert conditions

| Condition | Severity | Action |
|-----------|----------|--------|
| `scheduler:heartbeat` TTL expired | **CRITICAL** | Restart scheduler container |
| `errors` tăng liên tục | **WARNING** | Check Binance API / DB connection |
| `last_settlement` > 5 phút trước | **WARNING** | Check nếu có orders cần settle |
| Settlement batch > 100 orders | **INFO** | Scale check, có thể cần tăng frequency |

---

## 10. Rollback plan

Nếu Scheduler Service gặp vấn đề, rollback về kiến trúc cũ:

1. Stop scheduler container: `docker-compose stop scheduler`
2. Uncomment `start_scheduler()` trong `main.py` lifespan
3. Restart API: `docker-compose restart api`
4. Scheduler chạy lại trong FastAPI process như trước

**Không mất data** vì scheduler chỉ đọc/ghi DB — stop/start không ảnh hưởng state.
Orders chưa settle sẽ được sweep_stuck xử lý khi scheduler chạy lại.

---

## Tổng kết lợi ích

| # | Lợi ích | Chi tiết |
|---|---------|----------|
| 1 | **Isolation** | Settlement crash không ảnh hưởng API |
| 2 | **No file lock** | 1 process duy nhất, không cần `fcntl.flock` |
| 3 | **Independent deploy** | Update settlement logic không cần restart API |
| 4 | **Better retry** | Exponential backoff + circuit breaker cho Binance |
| 5 | **Observability** | Log/metrics riêng, heartbeat qua Redis |
| 6 | **Future-ready** | Dễ migrate sang distributed scheduler (Celery, Temporal) nếu cần |
| 7 | **Token prefetch** | TokenRegistry biết trước 5 candle → giảm latency tại boundary |
