# Polymarket Market Snapshot — Pseudo Code

## 1. Data Structures

```
// ═══════════════════════════════════════════════════════
// Core Data Model
// ═══════════════════════════════════════════════════════

struct PriceLevel:
    price: Decimal
    size:  Decimal        // size = 0 means level removed

struct TradeInfo:
    price:        Decimal
    size:         Decimal
    side:         "BUY" | "SELL"
    fee_rate_bps: string
    timestamp:    int64

struct MarketSnapshot:
    token_id:   string                          // asset_id
    market_id:  string                          // condition_id
    bids:       SortedMap<Decimal, Decimal>      // price → size, DESCENDING
    asks:       SortedMap<Decimal, Decimal>      // price → size, ASCENDING
    last_trade: TradeInfo | null
    tick_size:  Decimal                          // default 0.01
    best_bid:   Decimal | null
    best_ask:   Decimal | null
    spread:     Decimal | null
    book_hash:  string | null
    last_updated: int64                          // timestamp ms
    is_resolved:  bool                           // market đã resolved chưa
    
    // Derived getters
    fn midpoint() → (best_bid + best_ask) / 2
    fn display_price() →
        if spread > 0.04: last_trade.price      // wide spread → dùng last trade
        else:             midpoint()             // tight spread → dùng midpoint

// Global store: token_id → MarketSnapshot
STORE: HashMap<string, MarketSnapshot>
```

## 2. Connection & Subscription

```
// ═══════════════════════════════════════════════════════
// WebSocket Connection Manager
// ═══════════════════════════════════════════════════════

const WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
const MAX_ASSETS_PER_CONNECTION = 500
const RECONNECT_DELAYS = [1s, 2s, 5s, 10s, 30s]    // exponential backoff

class MarketWSClient:
    ws:           WebSocket
    token_ids:    Set<string>
    retry_count:  int = 0
    ping_timer:   Timer | null

    fn connect(token_ids: string[]):
        this.token_ids = Set(token_ids)
        
        // Enforce limit
        ASSERT len(token_ids) <= MAX_ASSETS_PER_CONNECTION
        
        this.ws = WebSocket(WS_URL)
        this.ws.on("open",    this.on_open)
        this.ws.on("message", this.on_message)
        this.ws.on("close",   this.on_close)
        this.ws.on("error",   this.on_error)

    fn on_open():
        this.retry_count = 0
        
        // Gửi subscription message
        this.ws.send(JSON.stringify({
            "assets_ids": Array.from(this.token_ids),
            "type": "market",
            "custom_feature_enabled": true      // ← enable best_bid_ask, new_market, market_resolved
        }))
        
        LOG("Connected, subscribed to {len(token_ids)} assets")

    fn on_close(code, reason):
        this.stop_ping()
        delay = RECONNECT_DELAYS[min(this.retry_count, len(RECONNECT_DELAYS) - 1)]
        this.retry_count += 1
        LOG("WS closed ({code}), reconnecting in {delay}...")
        SCHEDULE(delay, this.connect, Array.from(this.token_ids))

    fn on_error(err):
        LOG_ERROR("WS error: {err}")
        this.ws.close()     // triggers on_close → reconnect

    // ─── Dynamic subscribe/unsubscribe (sau khi đã connected) ───
    fn subscribe_more(new_token_ids: string[]):
        for id in new_token_ids:
            this.token_ids.add(id)
        
        this.ws.send(JSON.stringify({
            "assets_ids": new_token_ids,
            "operation": "subscribe",
            "custom_feature_enabled": true
        }))

    fn unsubscribe(remove_token_ids: string[]):
        for id in remove_token_ids:
            this.token_ids.remove(id)
            STORE.remove(id)
        
        this.ws.send(JSON.stringify({
            "assets_ids": remove_token_ids,
            "operation": "unsubscribe"
        }))
```

## 3. Event Router

```
// ═══════════════════════════════════════════════════════
// Message Dispatcher
// ═══════════════════════════════════════════════════════

fn on_message(raw: string):
    msg = JSON.parse(raw)
    
    // Một số message là array (batched), một số là single object
    events = msg if is_array(msg) else [msg]
    
    for event in events:
        type = event["event_type"]
        
        MATCH type:
            "book"             → handle_book(event)
            "price_change"     → handle_price_change(event)
            "last_trade_price" → handle_last_trade_price(event)
            "tick_size_change" → handle_tick_size_change(event)
            "best_bid_ask"     → handle_best_bid_ask(event)
            "new_market"       → handle_new_market(event)
            "market_resolved"  → handle_market_resolved(event)
            _                  → LOG_WARN("Unknown event_type: {type}")
```

## 4. Event Handlers (Chi tiết)

### 4.1 `book` — Khởi tạo / Reset full snapshot

```
// ═══════════════════════════════════════════════════════
// book: Full orderbook snapshot
// Trigger: lần đầu subscribe + sau mỗi trade ảnh hưởng book
// ═══════════════════════════════════════════════════════

fn handle_book(event):
    token_id  = event["asset_id"]
    market_id = event["market"]
    timestamp = int(event["timestamp"])
    
    // Lấy hoặc tạo snapshot
    snap = STORE.get_or_create(token_id, MarketSnapshot{
        token_id:  token_id,
        market_id: market_id,
    })
    
    // ── STALE CHECK ──
    // Nếu đã có snapshot mới hơn, bỏ qua event cũ
    if snap.last_updated > 0 AND timestamp < snap.last_updated:
        LOG_DEBUG("Stale book event for {token_id}, skipping")
        return
    
    // ── CLEAR & REBUILD ──
    // Book event = full snapshot → xóa sạch bids/asks cũ
    snap.bids.clear()
    snap.asks.clear()
    
    // Parse bids (price descending)
    for level in event["bids"]:
        price = Decimal(level["price"])
        size  = Decimal(level["size"])
        if size > 0:
            snap.bids.set(price, size)
    
    // Parse asks (price ascending)
    for level in event["asks"]:
        price = Decimal(level["price"])
        size  = Decimal(level["size"])
        if size > 0:
            snap.asks.set(price, size)
    
    // Update metadata
    snap.book_hash    = event.get("hash", null)
    snap.last_updated = timestamp
    
    // Derive best bid/ask from fresh book
    snap.best_bid = snap.bids.first_key()   // highest bid
    snap.best_ask = snap.asks.first_key()    // lowest ask
    if snap.best_bid AND snap.best_ask:
        snap.spread = snap.best_ask - snap.best_bid
    
    STORE.set(token_id, snap)
    EMIT("snapshot_updated", token_id, "book")
```

### 4.2 `price_change` — Incremental orderbook update

```
// ═══════════════════════════════════════════════════════
// price_change: Lệnh mới đặt hoặc lệnh bị hủy
// Đây là event QUAN TRỌNG NHẤT cho việc maintain snapshot
// ═══════════════════════════════════════════════════════

fn handle_price_change(event):
    market_id = event["market"]
    timestamp = int(event["timestamp"])
    
    for change in event["price_changes"]:
        token_id = change["asset_id"]
        price    = Decimal(change["price"])
        size     = Decimal(change["size"])
        side     = change["side"]            // "BUY" or "SELL"
        
        snap = STORE.get(token_id)
        if snap is null:
            // Chưa có snapshot (chưa nhận book event) → queue hoặc skip
            LOG_WARN("price_change for unknown token {token_id}, queuing...")
            PENDING_QUEUE.push(token_id, event)
            continue
        
        // ── APPLY CHANGE ──
        if side == "BUY":
            book = snap.bids
        else:  // "SELL"
            book = snap.asks
        
        if size == 0:
            // ★ Size = 0 → MỨC GIÁ ĐÃ BỊ XÓA khỏi book
            book.remove(price)
        else:
            // ★ Size > 0 → UPSERT mức giá (thêm mới hoặc cập nhật)
            book.set(price, size)
        
        // ── UPDATE BEST BID/ASK ──
        // Dùng dữ liệu từ event nếu có (reliable hơn tự tính)
        if "best_bid" in change:
            snap.best_bid = Decimal(change["best_bid"])
        else:
            snap.best_bid = snap.bids.first_key()
            
        if "best_ask" in change:
            snap.best_ask = Decimal(change["best_ask"])
        else:
            snap.best_ask = snap.asks.first_key()
        
        // Recalc spread
        if snap.best_bid AND snap.best_ask:
            snap.spread = snap.best_ask - snap.best_bid
        
        snap.last_updated = timestamp
        STORE.set(token_id, snap)
        EMIT("snapshot_updated", token_id, "price_change")
```

### 4.3 `last_trade_price` — Cập nhật giá giao dịch cuối

```
// ═══════════════════════════════════════════════════════
// last_trade_price: Maker + Taker matched → trade mới
// ═══════════════════════════════════════════════════════

fn handle_last_trade_price(event):
    token_id  = event["asset_id"]
    timestamp = int(event["timestamp"])
    
    snap = STORE.get(token_id)
    if snap is null:
        return
    
    snap.last_trade = TradeInfo{
        price:        Decimal(event["price"]),
        size:         Decimal(event["size"]),
        side:         event["side"],
        fee_rate_bps: event.get("fee_rate_bps", "0"),
        timestamp:    timestamp,
    }
    
    snap.last_updated = timestamp
    STORE.set(token_id, snap)
    EMIT("snapshot_updated", token_id, "last_trade_price")
```

### 4.4 `tick_size_change` — Thay đổi bước giá tối thiểu

```
// ═══════════════════════════════════════════════════════
// tick_size_change: Khi giá > 0.96 hoặc < 0.04
// Tick size nhỏ hơn → precision cao hơn gần biên
// ═══════════════════════════════════════════════════════

fn handle_tick_size_change(event):
    token_id = event["asset_id"]
    
    snap = STORE.get(token_id)
    if snap is null:
        return
    
    old_tick = Decimal(event["old_tick_size"])
    new_tick = Decimal(event["new_tick_size"])
    
    LOG_INFO("Tick size changed for {token_id}: {old_tick} → {new_tick}")
    
    snap.tick_size    = new_tick
    snap.last_updated = int(event["timestamp"])
    
    // ★ QUAN TRỌNG: Khi tick size thay đổi, các price level cũ
    // có thể cần được re-aligned. Chờ book event tiếp theo
    // để có snapshot chính xác với tick mới.
    // Optionally: flag snapshot as "tick_transitioning"
    
    STORE.set(token_id, snap)
    EMIT("snapshot_updated", token_id, "tick_size_change")
```

### 4.5 `best_bid_ask` — Best Bid/Ask thay đổi

```
// ═══════════════════════════════════════════════════════
// best_bid_ask: (cần custom_feature_enabled = true)
// Shortcut: không cần scan toàn bộ book để biết BBO
// ═══════════════════════════════════════════════════════

fn handle_best_bid_ask(event):
    token_id = event["asset_id"]
    
    snap = STORE.get(token_id)
    if snap is null:
        return
    
    snap.best_bid = Decimal(event["best_bid"])
    snap.best_ask = Decimal(event["best_ask"])
    snap.spread   = Decimal(event["spread"])
    snap.last_updated = int(event["timestamp"])
    
    STORE.set(token_id, snap)
    EMIT("snapshot_updated", token_id, "best_bid_ask")
```

### 4.6 `new_market` — Market mới được tạo

```
// ═══════════════════════════════════════════════════════
// new_market: (cần custom_feature_enabled = true)
// Market mới xuất hiện → có thể auto-subscribe
// ═══════════════════════════════════════════════════════

fn handle_new_market(event):
    market_id = event["market"]
    assets    = event["assets_ids"]      // [yes_token_id, no_token_id]
    question  = event["question"]
    tags      = event.get("tags", [])
    tick_size = Decimal(event.get("order_price_min_tick_size", "0.01"))
    
    LOG_INFO("New market: {question} [{market_id}]")
    
    // Tạo skeleton snapshot cho mỗi token
    for token_id in assets:
        if not STORE.has(token_id):
            STORE.set(token_id, MarketSnapshot{
                token_id:    token_id,
                market_id:   market_id,
                bids:        SortedMap.empty(),
                asks:        SortedMap.empty(),
                tick_size:   tick_size,
                is_resolved: false,
            })
    
    // ★ Nếu muốn auto-track, subscribe thêm
    // ws_client.subscribe_more(assets)
    
    EMIT("new_market_created", market_id, assets, question)
```

### 4.7 `market_resolved` — Market kết thúc

```
// ═══════════════════════════════════════════════════════
// market_resolved: Market có kết quả
// Token thắng → giá = $1.00, token thua → giá = $0.00
// ═══════════════════════════════════════════════════════

fn handle_market_resolved(event):
    market_id       = event["market"]
    winning_token   = event["winning_asset_id"]
    winning_outcome = event["winning_outcome"]
    all_tokens      = event["assets_ids"]
    
    LOG_INFO("Market resolved: {event['question']} → {winning_outcome}")
    
    for token_id in all_tokens:
        snap = STORE.get(token_id)
        if snap is null:
            continue
        
        snap.is_resolved = true
        snap.bids.clear()
        snap.asks.clear()
        
        // Set final price
        if token_id == winning_token:
            snap.best_bid = 1.00
            snap.best_ask = 1.00
        else:
            snap.best_bid = 0.00
            snap.best_ask = 0.00
        
        snap.spread = 0.00
        snap.last_updated = int(event["timestamp"])
        STORE.set(token_id, snap)
    
    // ★ Optionally: unsubscribe resolved market để giảm tải
    // ws_client.unsubscribe(all_tokens)
    
    EMIT("market_resolved", market_id, winning_token, winning_outcome)
```

## 5. Query API (đọc snapshot)

```
// ═══════════════════════════════════════════════════════
// Public API để consumer đọc dữ liệu snapshot
// ═══════════════════════════════════════════════════════

fn get_snapshot(token_id: string) → MarketSnapshot | null:
    return STORE.get(token_id)

fn get_orderbook(token_id: string, depth: int = 10) → {bids, asks}:
    snap = STORE.get(token_id)
    if snap is null:
        return null
    
    return {
        bids: snap.bids.take(depth),     // top N bids (highest first)
        asks: snap.asks.take(depth),      // top N asks (lowest first)
    }

fn get_midpoint(token_id: string) → Decimal | null:
    snap = STORE.get(token_id)
    if snap is null OR snap.best_bid is null OR snap.best_ask is null:
        return null
    return (snap.best_bid + snap.best_ask) / 2

fn get_display_price(token_id: string) → Decimal | null:
    // Logic giống Polymarket UI: spread > 4¢ → dùng last trade, else midpoint
    snap = STORE.get(token_id)
    if snap is null:
        return null
    return snap.display_price()

fn get_spread(token_id: string) → Decimal | null:
    snap = STORE.get(token_id)
    return snap?.spread

fn is_market_active(token_id: string) → bool:
    snap = STORE.get(token_id)
    return snap is not null AND NOT snap.is_resolved
```

## 6. Multi-Connection Manager (> 500 assets)

```
// ═══════════════════════════════════════════════════════
// Scale: Phân bổ token_ids ra nhiều WS connections
// Mỗi connection tối đa 500 assets
// ═══════════════════════════════════════════════════════

class MultiConnectionManager:
    connections: List<MarketWSClient>
    token_to_conn: HashMap<string, int>        // token_id → connection index
    
    fn subscribe_all(all_token_ids: string[]):
        chunks = chunk_array(all_token_ids, 500)
        
        for i, chunk in enumerate(chunks):
            client = new MarketWSClient()
            client.connect(chunk)
            this.connections.append(client)
            
            for token_id in chunk:
                this.token_to_conn.set(token_id, i)
    
    fn add_token(token_id: string):
        // Tìm connection còn slot
        for i, conn in enumerate(this.connections):
            if len(conn.token_ids) < 500:
                conn.subscribe_more([token_id])
                this.token_to_conn.set(token_id, i)
                return
        
        // Tất cả đều full → tạo connection mới
        client = new MarketWSClient()
        client.connect([token_id])
        this.connections.append(client)
        this.token_to_conn.set(token_id, len(this.connections) - 1)
```

## 7. Event Processing Order & Edge Cases

```
// ═══════════════════════════════════════════════════════
// Race Conditions & Edge Cases
// ═══════════════════════════════════════════════════════

/*
 IMPORTANT: Thứ tự xử lý events
 
 1. Khi mới subscribe → nhận `book` event ĐẦU TIÊN (full snapshot)
 2. Sau đó nhận `price_change` liên tục (incremental)
 3. `last_trade_price` có thể đến TRƯỚC `book` event tiếp theo
    → trade đã xảy ra nhưng book chưa update
    → Đây là bình thường, book sẽ update ngay sau
 
 EDGE CASES:
 
 A. price_change đến TRƯỚC book (chưa có snapshot)
    → Queue event, replay khi nhận được book
    → HOẶC skip (book event sẽ có dữ liệu đầy đủ)
 
 B. Mất kết nối rồi reconnect
    → Nhận book event mới → full reset (an toàn)
    → Giữa lúc disconnect, có thể miss price_changes
    → Book event khi reconnect sẽ sync lại
 
 C. Timestamp out of order
    → Luôn check: if event.timestamp < snap.last_updated → skip
    → NGOẠI TRỪ book event (luôn apply vì nó là full snapshot)
 
 D. tick_size_change giữa lúc có orders
    → Sau tick change, chờ book event mới để resync
    → Các price levels cũ vẫn valid nhưng precision thay đổi
 
 E. market_resolved trong lúc có pending orders
    → Clear book, set final price
    → Ignore mọi price_change sau resolved
*/

fn should_process(event, snap) → bool:
    if snap.is_resolved AND event.event_type in ["price_change", "last_trade_price"]:
        return false     // Market đã resolved, bỏ qua
    
    if event.event_type == "book":
        return true      // Book event luôn được xử lý (full reset)
    
    if snap.last_updated > 0 AND int(event.timestamp) < snap.last_updated:
        return false     // Stale event
    
    return true
```