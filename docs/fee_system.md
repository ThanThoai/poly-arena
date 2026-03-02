Đây là bản đặc tả đã được cập nhật hoàn chỉnh. Tôi đã thay thế hoàn toàn mô hình phí cố định (BPS) bằng **Mô hình Phí Động (Dynamic Fee Curve)** và cơ chế **Maker Rebate** chính xác của Polymarket dành cho thị trường Crypto.

Bản đặc tả này vẫn giữ nguyên cấu trúc chuẩn không có TP/SL như bạn đã yêu cầu trước đó.

---

# PolyArena: Core Shadow Trading Engine Specification (Dynamic Fee Revision)

## 1. System Architecture & Economics

PolyArena is a high-fidelity paper trading simulation of the Polymarket Central Limit Order Book (CLOB). To provide realistic execution, it utilizes a hybrid approach: **RESTful API** for instant liquidity sweeps (Market Orders) and **WebSocket streams** for resting liquidity (Limit Orders).

### 1.1 The Balance Invariant

The system tracks simulated wealth using a strict balance equation:


$$\text{User Total Equity} = \text{User Available Balance} + \sum_{i=1}^{n} \text{Bot Current Balance}_i$$

### 1.2 The Dynamic Fee Model & Maker Rebate

Unlike traditional exchanges using fixed percentages (BPS), Polymarket utilizes a dynamic fee curve based on the probability (price) of the shares being traded. Fees are highest at $\$0.50$ and approach zero at the extremes ($\$0.01$ and $\$0.99$).

For Crypto markets (e.g., BTC 5m/15m), the parameters are:

* **$\text{feeRate}$** = `0.25`
* **$\text{exponent}$** = `2`
* **Maker Rebate** = `20%` (0.20)

**The Universal Fee Formula per fill:**


$$\text{Nominal Fee} = \text{Matched Qty} \times \text{feeRate} \times (\text{Match Price} \times (1 - \text{Match Price}))^{\text{exponent}}$$

**Role-Based Application:**

* **Taker (Removes Liquidity):** Pays 100% of the `Nominal Fee`. It is deducted from the bot's balance.
* **Maker (Provides Liquidity):** Receives a **Rebate** equal to `20%` of the `Nominal Fee`. It is added to the bot's balance as an incentive.
* **Resolution Fee:** $\$0.00$. No fees are charged on final event settlement.

---

## 2. Order Ingestion & Pre-Validation

Every order submitted by a Bot must pass basic financial validation.

### 2.1 Validation Rules

* **Financial Integrity (Max Cost Check):** The Bot must have sufficient `current_balance` to cover the maximum potential cost of the order (Price $\times$ Quantity) PLUS an estimated maximum Taker fee (calculated at price $\$0.50$ to be safe).
* **Price Bounds:** Order prices must be strictly between $\$0.01$ and $\$0.99$ (inclusive).
* **Failure Handling:** Orders failing these checks immediately return an HTTP `400 Bad Request`.

---

## 3. Core Execution Workflows

### 3.1 Market Order Flow (Taker - Instant Execution & Sweeping)

Market orders require immediate execution against real Polymarket liquidity. Because they cross the spread instantly, they always act as **Takers**.

1. **Fetch Liquidity:** Call the Polymarket REST API `GET /orderbook` for the requested `tokenId`.
2. **Walk the Book (Sweep):** Iterate through the `Asks` (for a BUY) or `Bids` (for a SELL) from the best available price to the worst, until the requested `quantity` is filled.
3. **Multi-Level Fill & Dynamic Fee Calculation:**
For each price level $i$ matched during the sweep, the fee must be calculated individually because $p$ changes:
* $$\text{Level\_Value}_i = \text{matched\_qty}_i \times \text{price}_i$$


* $$\text{Level\_Fee}_i = \text{matched\_qty}_i \times 0.25 \times (\text{price}_i \times (1 - \text{price}_i))^2$$


* Accumulate `total_qty`, `total_cost`, and `total_fee_paid`.


4. **Finalization (BUY Order Example):**
* $$\text{avg\_entry\_price} = \frac{\text{total\_cost}}{\text{total\_qty}}$$


* Update Bot Balance: `Bot.current_balance -= (total_cost + total_fee_paid)`
* Set order status to `FILLED` (or `CANCELED` for any unfilled dust).



### 3.2 Limit Order Flow (Shadow Engine)

Limit orders are managed by the internal Shadow Engine and rely on WebSocket updates for resting liquidity.

1. **Marketable Check (Crosses the spread = TAKER):**
* If `Limit_Price >= Best_Ask` (for a BUY), the order acts as a **Taker**. It executes immediately via a REST sweep (following the exact logic in 3.1) and pays the **Taker Fee**.


2. **Resting Order (Queued = MAKER):**
* If `Limit_Price < Best_Ask`, the order cannot be filled immediately. It is placed in the Shadow Engine with a `PENDING` status.


3. **WebSocket Matching:**
* The Shadow Engine listens to WebSocket `price_change` and `book` events.
* When market liquidity drops to (or below) the `Limit_Price`, the engine matches the order.
* Because the order was resting, this execution acts as a **Maker**.
* **Maker Fee Calculation:** * Calculate `Nominal_Fee` using the formula in 1.2.
* $$\text{Earned\_Rebate} = \text{Nominal\_Fee} \times 0.20$$




* **Balance Update (BUY Order):** `Bot.current_balance -= (Level_Value - Earned_Rebate)` *(The bot pays less because of the rebate)*.
* *Critical Constraint:* The engine MUST deduct the matched quantity from its internal representation of the Polymarket orderbook to prevent "Phantom Liquidity".



---

## 4. Settlement & Market Resolution

This is the final lifecycle stage of a binary options market.

1. **Event Detection:** The system receives an `event_type: market_resolved` message via WebSocket or Oracle polling.
2. **Order Cleanup:** All `PENDING` or `PARTIAL` Limit orders for this asset are instantly marked as **CANCELED**. Trading is halted. Unused funds are refunded to the `Bot Current Balance`.
3. **Payout Calculation:**
* For shares of the Winning Token: 
$$\text{Payout} = \text{filled\_qty} \times 1.00$$


* For shares of the Losing Token: 
$$\text{Payout} = \text{filled\_qty} \times 0.00$$




4. **Balance Update:** Add the calculated Payout directly to `Bot.current_balance`. **No trading fees apply to this payout.**
5. **Final State:** Mark the position as `position_closed = True` and the overall order status as `RESOLVED`.

---

## 5. Order Trace & Audit System

To maintain transparency regarding the dynamic fees and maker rebates, every state mutation must be logged into a JSON array (`trace_logs`) attached to the order.

### 5.1 Trace Object Schema

```json
{
  "timestamp": "ISO-8601 string",
  "stage": "VALIDATION | MATCHING | CANCELLATION | RESOLUTION",
  "action": "REST_SWEEP | WS_TRIGGER | HALT | TTL_EXPIRE",
  "message": "Human readable explanation of the event",
  "meta": {
    "qty_matched": 100,
    "price": 0.50,
    "nominal_fee": 1.5625,
    "role": "TAKER | MAKER",
    "actual_fee_deducted": 1.5625, 
    "rebate_earned": 0.0
  }
}

```

### 5.2 Example Audit Trail (Resting Limit BUY - Maker)

1. `[VALIDATION]` - "Order passed financial validation."
2. `[MATCHING]` - "WS Trigger: Matched 100 @ $0.50. Role: MAKER. Nominal Fee: $1.5625. Earned Rebate: $0.3125."
3. `[MATCHING]` - "Order FILLED. Avg Price: $0.50. Total Cost: $49.6875 ($50.00 - $0.3125 rebate)."
4. `[RESOLUTION]` - "Market halted and resolved. Token is WINNER. Final payout $100.00."

---

Bạn có muốn tôi viết một hàm Python (với thư viện `decimal`) để hiện thực hóa phép tính phí động và xử lý logic cộng/trừ tiền cho cả Maker và Taker không?