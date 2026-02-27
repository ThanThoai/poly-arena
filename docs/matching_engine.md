# Polymarket Paper Trading: Advanced Shadow Matching & Bracket Order Specification

## 1. System Overview
This document specifies the architecture for a **Shadow Matching Engine** simulating Polymarket's Central Limit Order Book (CLOB). It processes real-time WebSocket data and evaluates virtual orders. 
The system supports both **LIMIT** and **MARKET** orders, advanced conditional parameters (`TP`, `SL`, `TTL`), and native Polymarket payout resolution for unclosed positions.

## 2. Core Data Models

### 2.1. Simulated Order (`SimulatedOrder`)
Represents a virtual trade placed by the user.
* `order_id` (String): Unique identifier.
* `type` (Enum): `LIMIT` or `MARKET`.
* `side` (Enum): `BUY` or `SELL`.
* `price` (Decimal | Null): The limit price (Null if `type == MARKET`).
* `quantity` (Decimal): Total requested size.
* `filled` (Decimal): The amount successfully matched. (Initial: `0.0`).
* `status` (Enum): `PENDING`, `PARTIAL`, `FILLED`, `CANCELED`, or `RESOLVED`.
* **Bracket & Expiration Parameters (Optional):**
    * `tp_price` (Decimal | Null): Take Profit trigger price.
    * `sl_price` (Decimal | Null): Stop Loss trigger price.
    * `expires_at` (Timestamp | Null): Time To Live (TTL) deadline.
* **State Trackers:**
    * `position_closed` (Boolean): True if TP/SL/User manually closed the entire filled position.
* **Computed Property:** `remaining_qty = quantity - filled`

### 2.2. Shadow Orderbook (`OrderbookState`)
* `bids` (Map): `{price: size}`. Sorted DESCENDING (highest first).
* `asks` (Map): `{price: size}`. Sorted ASCENDING (lowest first).

---

## 3. Order Execution Lifecycles

### 3.1. MARKET Orders (Immediate Execution)
* **Nature:** Market orders demand immediate liquidity. They cross the spread and execute at the best available prices.
* **TTL Handling:** For MARKET orders, `TTL` is practically evaluated as **IOC (Immediate or Cancel)**. The engine attempts to fill as much as possible instantly. Any `remaining_qty` after sweeping the available book is immediately `CANCELED`.
* **TP/SL Handling:** Once the MARKET order is `FILLED` or `PARTIAL` (and the rest canceled), the filled portion immediately becomes an active position monitored by the **Conditional Monitor (Workflow E)**.

### 3.2. LIMIT Orders (Resting/Passive Execution)
* **Nature:** Executed at the specified `price` or better. If liquidity is unavailable, the order rests in the system as `PENDING` or `PARTIAL`.
* **TTL Handling:** The `expires_at` timestamp is actively monitored. If `current_time >= expires_at` and the order is not fully `FILLED`, the order transitions to `CANCELED`.
* **TP/SL Handling:** TP/SL monitors ONLY apply to the `filled` portion of a LIMIT order. If a LIMIT order is 50% filled, the TP/SL logic operates solely on that 50%.

---

## 4. Workflows & Algorithms

### Workflow A: Market Data Ingestion
*(Processes `book` and `price_change` WebSocket events to update `OrderbookState`. Same as previous specification).*

### Workflow C: The Matching Algorithm (Updated for Market Orders)
**Trigger:** Called on Market Data updates or New Order placement.
**Algorithm Steps (Evaluating `Active_Virtual_Orders`):**

1. Sort `bids` descending. Sort `asks` ascending.
2. For each `order` in `Active_Virtual_Orders` (Ignore `FILLED`, `CANCELED`, `RESOLVED`):
    
    * **IF `order.type == MARKET` AND `order.side == BUY`:**
        * Loop through `sorted_asks` until `order.remaining_qty == 0`.
        * Execute match at `ask_price`, deduct `ask_size` from `OrderbookState`.
        * If `sorted_asks` is exhausted but `remaining_qty > 0` (Illiquid market), set `order.status = PARTIAL` (or `CANCELED` for the remainder) based on IOC logic. Break loop.

    * **IF `order.type == LIMIT` AND `order.side == BUY`:**
        * Loop through `sorted_asks`.
        * If `ask_price <= order.price`: Match and deduct shadow liquidity.
        * Else: Break loop (Prices are ascending, no further matches possible).

    *(Apply mirror logic for `SELL` orders against `sorted_bids`)*

### Workflow D: TTL (Time To Live) Monitor
**Trigger:** Runs on a periodic background loop (e.g., every 1 second) OR triggered by WebSocket timestamps.
**Objective:** Expire stale resting orders.
**Algorithm:**
1. Loop through `Active_Virtual_Orders` where `status` is `PENDING` or `PARTIAL`.
2. If `order.expires_at` is NOT Null AND `System.CurrentTime >= order.expires_at`:
    * Mark `order.status = CANCELED`.
    * Emit log: `"Order {order_id} canceled due to TTL expiration."`
    * *Note:* If the order was `PARTIAL`, the `filled` portion remains yours, but no further filling will occur.

### Workflow E: Conditional Monitor (TP / SL)
**Trigger:** Received `best_bid_ask` or `last_trade_price` WebSocket event.
**Objective:** Liquidate positions when target thresholds are hit.
**Logic (For a `BUY` position):**
1. Evaluate orders where `filled > 0`, `position_closed == False`, and TP/SL are set.
2. Fetch `current_best_bid` (The price you can sell at right now).
3. **Take Profit:** IF `current_best_bid >= order.tp_price`:
    * Generate a virtual MARKET `SELL` order for `order.filled` quantity.
    * Process immediately via Workflow C to calculate realistic slippage.
    * Mark `order.position_closed = True`.
4. **Stop Loss:** IF `current_best_bid <= order.sl_price`:
    * Generate a virtual MARKET `SELL` order for `order.filled` quantity.
    * Process immediately via Workflow C.
    * Mark `order.position_closed = True`.

### Workflow F: Default Polymarket Resolution (No TP/SL hit)
**Trigger:** Received `market_resolved` WebSocket event for the specific asset.
**Objective:** Resolve all open positions that did NOT hit TP/SL (or didn't have them).
**Context:** Polymarket binary options resolve to `$1.00` if correct, and `$0.00` if incorrect.
**Algorithm:**
1. Parse the resolution payload to determine the winning token (e.g., YES or NO).
2. Loop through all `Active_Virtual_Orders` for this market where `filled > 0` AND `position_closed == False`.
3. **Payout Calculation:**
    * If the order's asset matches the winning token: 
      `Payout = order.filled * 1.00` (User takes maximum profit).
    * If the order's asset matches the losing token:
      `Payout = order.filled * 0.00` (User loses the investment).
4. Update User's virtual balance with the `Payout`.
5. Mark `order.position_closed = True` and `order.status = RESOLVED`.
6. Emit log: `"Market Resolved. Order {order_id} paid out ${Payout}."`

---

## 5. Implementation Guidelines for Coding Agents
1. **Polymorphism for Order Execution:** Implement an `OrderProcessor` interface/class that handles `MarketProcessor` and `LimitProcessor` differently to keep Workflow C clean.
2. **Atomic Taker Logic:** When a TP or SL is triggered (Workflow E), the system spawns a `MARKET SELL` order. The agent must ensure this spawned order goes through the exact same shadow liquidity deduction (Workflow C) as a normal user order to account for real-world slippage.
3. **Event Loop Non-Blocking:** Workflows D (TTL) and E (TP/SL) must not block the WebSocket data ingestion thread (Workflow A). Use Async Tasks or Web Workers depending on the language (Python `asyncio.create_task` or Node.js event loop).