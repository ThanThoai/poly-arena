# Polymarket Paper Trading: Shadow Matching Engine Specification (Including Bracket Orders)

## 1. System Overview
This document outlines the architecture, data models, and algorithms required to build a **Shadow Matching Engine** for Polymarket. 
The system ingests real-time WebSocket market data, processes standard virtual limit orders, and manages advanced **Bracket Orders (BO)** with Take Profit (TP) and Stop Loss (SL) triggers.



## 2. Core Data Structures

### 2.1. Simulated Order (`SimulatedOrder`)
Represents a virtual limit order placed by the user. Now supports Bracket Order parameters.
* `order_id` (String): Unique identifier.
* `side` (Enum): `BUY` or `SELL`.
* `price` (Decimal): The limit price.
* `quantity` (Decimal): Total requested size.
* `filled` (Decimal): The amount successfully matched.
* `status` (Enum): `PENDING`, `PARTIAL`, `FILLED`, or `CANCELED`.
* **Bracket Order Fields (Optional):**
    * `tp_price` (Decimal | Null): Take Profit trigger price.
    * `sl_price` (Decimal | Null): Stop Loss trigger price.
    * `position_closed` (Boolean): Tracks if the spawned TP/SL has completely closed the position. Defaults to `False`.
* **Computed Property:** `remaining_qty = quantity - filled`

### 2.2. Shadow Orderbook (`OrderbookState`)
* `bids` (Map/Dictionary): `{price: size}`. Sorted in DESCENDING order (highest first).
* `asks` (Map/Dictionary): `{price: size}`. Sorted in ASCENDING order (lowest first).

---

## 3. WebSocket Event Handling & Routing
The WebSocket connection to `wss://ws-subscriptions-clob.polymarket.com/ws/market` pushes an array of events or single event objects. A central router must parse the `event_type` and dispatch it to the appropriate handler.

### 3.1. Event: `book` (Initial Snapshot)
* **Trigger:** Received upon successful subscription or major state resets.
* **Payload Structure:** Contains full arrays of `bids` and `asks` (e.g., `[{"price": "0.50", "size": "100"}, ...]`).
* **Processing Logic (Handler):**
    1. Clear the current `OrderbookState` (`bids.clear()`, `asks.clear()`).
    2. Iterate through the incoming `bids` array and populate `OrderbookState.bids`.
    3. Iterate through the incoming `asks` array and populate `OrderbookState.asks`.
    4. Trigger the Matching Algorithm (Section 4) to evaluate if any resting virtual orders can be filled against this new snapshot.

### 3.2. Event: `price_change` (Delta Updates)
* **Trigger:** Received when there is a change in liquidity at specific price levels.
* **Payload Structure:** Contains a `changes` array (e.g., `[{"side": "bid", "price": "0.50", "size": "0"}, ...]`).
* **Processing Logic (Handler):**
    1. Loop through each item in the `changes` array.
    2. Identify target map based on `side`.
    3. **Condition:** If `size == 0` -> Delete the `price` key from the target map.
    4. **Condition:** If `size > 0` -> Update/Upsert the `price` key with the new `size`.
    5. Trigger the Matching Algorithm (Section 4).

### 3.3. Event: `best_bid_ask` (Spread Tracker)
* **Trigger:** Received immediately when the top of the book changes (requires `"custom_feature_enabled": true` in subscription payload).
* **Payload Structure:** `{"bid": "0.49", "ask": "0.51"}`.
* **Processing Logic (Handler):**
    1. *Optional:* Use this for fast, lightweight checks (e.g., updating UI or triggering simple Taker order logic) without iterating through the entire orderbook map. 
    2. Usually, `price_change` is sufficient for deep orderbook matching, but `best_bid_ask` is excellent for high-level monitoring.

### 3.4. Event: `last_trade_price` (Market Execution)
* **Trigger:** Received when a real trade occurs on Polymarket.
* **Payload Structure:** `{"price": "0.50", "size": "50", "side": "BUY"}`.
* **Processing Logic (Handler):**
    1. Record the trade for charting/logging purposes.
    2. *Advanced Simulation:* If mimicking queue priority, the engine can check if a virtual limit order at this exact `price` should be partially filled, assuming the real market just consumed liquidity ahead of the virtual order in the queue.

### 3.5. Event: `market_resolved` (Market Closure)
* **Trigger:** The prediction market has finalized and trading is halted.
* **Processing Logic (Handler):**
    1. Stop the Matching Engine for this specific market.
    2. Update all virtual orders with `status == PENDING` or `PARTIAL` to `CANCELED`.

### 3.6. Triggering Conditions for TP/SL
To accurately simulate Stop Loss and Take Profit, the engine must monitor the market price actively.
* **Best Trigger Event:** `best_bid_ask` or `last_trade_price` updates should invoke the **Conditional Monitoring Algorithm (Workflow E)**. 
* *Reasoning:* A Stop Loss is usually a "Stop Market" order. When the market price drops to the `sl_price`, we want to liquidate the position immediately at the best available bid.

---


## 4. The Matching Algorithm (Core Logic)
**Trigger:** Called after processing `book` or `price_change` events, or upon New Virtual Order placement.

**Algorithm Steps:**
1. Sort `bids` descending. Sort `asks` ascending.
2. For each `order` in `Active_Virtual_Orders`:
    * If `order.status == FILLED` or `CANCELED`, continue.
    
    * **IF `order.side == BUY`:**
        1. Iterate through `sorted_asks` (`ask_price`, `ask_size`).
        2. If `ask_size <= 0`, continue.
        3. If `order.price >= ask_price`:
            * `match_qty = MIN(order.remaining_qty, ask_size)`
            * `order.filled += match_qty`
            * `asks[ask_price] -= match_qty` (Deduct shadow liquidity)
            * Update `order.status`.
            * If `order.status == FILLED`, break loop.
        4. Else: Break loop (prices are ascending, no further matches possible).

    * **IF `order.side == SELL`:**
        1. Iterate through `sorted_bids` (`bid_price`, `bid_size`).
        2. If `bid_size <= 0`, continue.
        3. If `order.price <= bid_price`:
            * `match_qty = MIN(order.remaining_qty, bid_size)`
            * `order.filled += match_qty`
            * `bids[bid_price] -= match_qty` (Deduct shadow liquidity)
            * Update `order.status`.
            * If `order.status == FILLED`, break loop.
        4. Else: Break loop (prices are descending, no further matches possible).

## 5. Workflow E: Bracket Order (TP/SL) Monitoring Algorithm
**Trigger:** Called continuously upon receiving `best_bid_ask` or `last_trade_price` from the WebSocket.
**Objective:** Evaluate if any active positions (Filled BUY orders with TP/SL attached) need to be liquidated.

**Assumptions for Polymarket:** Since Polymarket operates without margin/naked shorting, a user places a Bracket Order by `BUYING` shares (e.g., BUY YES). To realize profit or cut losses, the system must execute a simulated `SELL` order.

**Algorithm Steps:**
1. Fetch the `current_best_bid` from `OrderbookState.bids` (the highest price a real buyer is currently willing to pay).
2. For each `order` in `Active_Virtual_Orders`:
    * **Condition Check:** Ensure the order is a `BUY` order, has `status == FILLED` (or `PARTIAL` with `filled > 0`), has TP/SL parameters set, and `position_closed == False`.
    
    * **Check Take Profit (TP):**
        * **Logic:** If the market is willing to buy at or above our target profit price.
        * **Evaluation:** `IF current_best_bid >= order.tp_price`
        * **Execution:**
            1. Emit log: `"Take Profit triggered for Order {order_id} at price {current_best_bid}"`.
            2. Generate a simulated Taker `SELL` order for the `order.filled` amount at the `current_best_bid` price.
            3. Process this SELL order through the Main Matching Algorithm (Workflow C) to ensure it consumes real shadow liquidity (accounting for slippage if the size is large).
            4. Mark `order.position_closed = True`.
            5. Break to next order (OCO Logic: Since TP hit, SL is inherently canceled/ignored).

    * **Check Stop Loss (SL):**
        * **Logic:** If the market price drops to or below our risk threshold.
        * **Evaluation:** `IF current_best_bid <= order.sl_price`
        * **Execution:**
            1. Emit log: `"Stop Loss triggered for Order {order_id} at price {current_best_bid}"`.
            2. Generate a simulated Market/Taker `SELL` order for the `order.filled` amount at the `current_best_bid` price.
            3. Process this SELL order through Workflow C to consume shadow liquidity.
            4. Mark `order.position_closed = True`.
            5. Break to next order (OCO Logic: Since SL hit, TP is inherently canceled).

## 6. Implementation Constraints for Coding Agents (Updated)
1. **Precision Limitation:** Use `Decimal` types strictly.
2. **Slippage Handling on TP/SL:** Coding agents MUST pass the triggered TP/SL sell action through the standard matching engine (deducting from `OrderbookState.bids`). Do NOT just assume the entire position closes exactly at the trigger price. If the `order.filled` size is large, it might eat through multiple bid levels, resulting in an average exit price worse than the SL trigger price (realistic slippage).
3. **Partial Fills:** If the main `BUY` order is only `PARTIAL` filled, the TP/SL monitoring should only attempt to liquidate the `filled` portion.