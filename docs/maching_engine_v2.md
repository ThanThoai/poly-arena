PolyArena: Advanced Shadow Matching Engine Specification
1. System Overview
PolyArena is a distributed paper trading platform that simulates the Polymarket Central Limit Order Book (CLOB). It extends native functionality by supporting Single-Condition Orders (Take Profit or Stop Loss) managed through a proprietary "Client-Side Trigger" mechanism.

2. Order Ingestion & Pre-Validation
All orders are submitted via the API and must undergo logical validation before entering the execution pipeline.

2.1 Validation Rules
To prevent immediate "logical suicide" of an order, the system enforces the following:

Single Condition Policy: An order can have at most one condition: TP, SL, or NONE.

Price Logic: For a BUY order, the system fetches the current Best Ask from Redis as a proxy for the entry price:

Stop Loss (SL): Must verify condition_price < Best_Ask.

Take Profit (TP): Must verify condition_price > Best_Ask.

Rejection: If validation fails, the API returns a 400 Bad Request.

3. Hybrid Execution Logic
3.1 Market Order Flow (REST-First)
Market orders are executed "hot" using the latest Polymarket REST Orderbook snapshot.

Fetch: Call GET /orderbook for the specific tokenId.

Sweep Simulation: Iteratively match the requested quantity against available Asks (for BUY) or Bids (for SELL).

Calculate Entry: Compute avg_entry_price = total_spent / filled_qty.

Post-Fill Edge Case (Auto-Exit): * If avg_entry_price violates the user's TP or SL due to slippage (e.g., avg_entry_price >= TP_price), trigger the Auto-Exit Workflow immediately.

3.2 Limit Order Flow (Shadow Engine)
Orders that do not match immediately are placed in the Shadow Matching Engine.

Queueing: Order status is set to PENDING or PARTIAL.

WS Matching: As WebSocket price_change or book events arrive, the engine checks if market_price <= limit_price (for BUY).

Execution: On match, update filled_qty and deduct virtual liquidity from the shadow orderbook to simulate realistic slippage.

4. Conditional Monitoring & Auto-Exit
4.1 Post-Fill Validation
Once an order (Market or Limit) is filled, the system performs a final safety check.

Violation Check: If the avg_entry_price is already beyond the condition_price, the position is deemed "pre-triggered".

Auto-Exit Execution:

Immediately fetch the latest Bids via REST API.

Simulate a SELL FAK (Fill-And-Kill) order for the entire filled_qty.

Calculate avg_exit_price and mark the order as RESOLVED with the reason "Slippage Violation".

4.2 Active Monitoring
For valid positions, the Monitor Service tracks the market via WebSocket.

Trigger: Received best_bid_ask event.

TP Logic: If current_best_bid >= condition_price (TP), execute Auto-Exit.

SL Logic: If current_best_bid <= condition_price (SL), execute Auto-Exit.

5. Settlement & Market Resolution
The final phase occurs when Polymarket halts trading for an event.

5.1 Resolution Workflow
Halt Detection: Receive event_type: market_resolved via WebSocket.

Condition Erasure: Immediately cancel all active TP/SL monitoring for the affected asset_id.

Oracle Payout: Determine the winner (e.g., YES or NO).

Final PnL: * If user holds the winning token: Final_Value = filled_qty * 1.00.

If user holds the losing token: Final_Value = filled_qty * 0.00.

Update State: Set order status to RESOLVED and position_closed = True.


6. Summary of Edge Case Handling

Scenario,Point of Detection,Required Action
Slippage Violation,Post-Fill (REST/WS),Auto-Exit: Instant SELL sweep against REST Bids.
Empty Orderbook,REST Fetch,Revert Market order or keep Limit order PENDING.
Partial Fill,Shadow Engine,Condition applies only to the current filled_qty.
Resolution Event,WebSocket,Ignore all TP/SL; Settle at binary 1.0 or 0.0.


7. Order Trace System
The Order Trace system captures every micro-step of an order's lifecycle, from validation to final settlement. These logs are stored as an array of objects associated with each order_id to be displayed on the User UI.

7.1 Trace Object Structure
Each trace entry must contain:

timestamp: Precise ISO-8601 time.

stage: VALIDATION, MATCHING, MONITORING, or SETTLEMENT.

action: Specific operation performed (e.g., REST_SWEEP, WS_TRIGGER).

details: A human-readable string explaining the logic.

data: (Optional) JSON object containing prices and quantities involved.

7.2 Detailed Logging Stages
Stage A: Order Ingestion (Validation)
Log: "Pre-validation successful. Condition [TP/SL] at $[Price] is valid against current Best Ask $[Price]."

Edge Case Log: "Validation Failed: SL $[Price] must be lower than estimated entry $[Price]. Order Rejected."

Stage B: Execution (Matching)
Market Order (REST):

"Fetching real-time orderbook from Polymarket REST API..."

"Sweeping liquidity: Matched [Qty] @ $[Price]. (Remaining: [Qty])"

"Market Order filled. Avg Entry Price: $[Price]. Total Slippage: [X]%."

Limit Order (Shadow Engine):

"WebSocket Trigger: Market price $[Price] touched Limit $[Price]. Initiating match..."

"Shadow Match: Consumed [Qty] of virtual liquidity from Orderbook."

Stage C: Post-Fill & Auto-Exit
Violation Detected: "Post-fill check failed: Avg Entry $[Price] violates [TP/SL] threshold $[Price]. Triggering Auto-Exit..."

Exit Sweep: "Auto-Exit REST Sweep: Selling [Qty] against current Bids. Avg Exit Price: $[Price]."

Monitoring: "Active Monitoring: Best Bid $[Price] hit [TP/SL] threshold $[Price]. Initiating exit order."

Stage D: Final Settlement (Resolution)
Halt Detected: "Polymarket Event: [market_resolved] received. Halting all active monitoring for this asset."

Condition Cleanup: "Order condition [TP/SL] removed due to market resolution."

Payout: "Final Settlement: Asset resolved as [Winner]. Payout calculated: [Qty] * $[1.0/0.0] = $[Total]."

7.3 Logic for Coding Agents
Atomicity: Trace logs must be appended immediately after each database or state update.

Storage: * Live: Store the latest 10 steps in Redis for real-time UI updates (Pub/Sub).

Historical: Save the full trace as a JSONB field or a linked table in the SQL Database upon order finalization (RESOLVED or CANCELED).

Format: Ensure all prices in logs are formatted to 2-4 decimal places for readability (e.g., $0.5234).