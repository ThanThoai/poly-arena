This document specifies the **User & Bot Management System** for the PolyArena simulation platform, focusing on BTC 5m and 15m Binary Options (BO) markets. It outlines the financial logic, balance invariants, and performance tracking required for high-fidelity simulation.

---

# Specification: User & Bot Economic System

## 1. System Invariants & Balance Logic

The core financial engine must maintain the following equation at all times:


$$\text{User Total Equity} = \text{User Available Balance} + \sum_{i=1}^{n} \text{Bot Current Balance}_i$$

### 1.1 Balance Definitions

* **Initial Balance:** A fixed starting amount of **$5,000.00** credited to every new user.
* **User Available Balance:** "Idle" funds not currently managed by any bot.
* **Bot Allocated Balance:** The initial capital assigned to a bot upon creation.
* **Bot Current Balance:** The real-time value of the bot's wallet, fluctuating based on trade outcomes.

---

## 2. Database Schema (Entity Models)

### 2.1 User Model

| Field | Type | Description |
| --- | --- | --- |
| `id` | UUID | Primary Key. |
| `username` | String | Unique identifier. |
| `initial_balance` | Decimal | Fixed at 5000.00 for P&L baseline. |
| `available_balance` | Decimal | Funds available for new bot allocation. |

### 2.2 Bot Model

| Field | Type | Description |
| --- | --- | --- |
| `id` | UUID | Primary Key. |
| `user_id` | UUID | Foreign Key to User. |
| `market_type` | Enum | `BTC_5M` or `BTC_15M`. |
| `allocated_balance` | Decimal | Capital moved from User to Bot at creation. |
| `current_balance` | Decimal | Current wallet value after trades. |
| `status` | Enum | `ACTIVE`, `PAUSED`, `DELETED`. |

### 2.3 PnL Snapshot Model (For Linecharts)

| Field | Type | Description |
| --- | --- | --- |
| `id` | UUID | Primary Key. |
| `user_id` | UUID | Owner of the data. |
| `bot_id` | UUID | Source of the profit/loss. |
| `session_id` | String | Unique Polymarket Market ID (BTC 5m/15m). |
| `pnl_amount` | Decimal | Profit or Loss for that specific session. |
| `equity_snapshot` | Decimal | Total User Equity at the time of resolution. |
| `timestamp` | DateTime | Resolution time. |

---

## 3. Workflows & State Transitions

### 3.1 Bot Creation

1. **Request:** User specifies `name`, `market_type`, and `allocation_amount`.
2. **Validation:** Ensure `allocation_amount <= User.available_balance`.
3. **Execution:**
* `User.available_balance -= allocation_amount`.
* Create Bot entry with `allocated_balance = current_balance = allocation_amount`.



### 3.2 Bot Deletion

1. **Validation:** Bot must have no active orders in an unresolved session.
2. **Execution:**
* `User.available_balance += Bot.current_balance`.
* Mark Bot as `DELETED`.



### 3.3 Session Resolution (P&L Logic)

Binary Options payout is binary (1.0 or 0.0). P&L is **only** calculated upon session settlement.

1. **ROI Calculation:** 
$$\text{Bot ROI \%} = \left( \frac{\text{Current Balance} - \text{Allocated Balance}}{\text{Allocated Balance}} \right) \times 100$$


2. **Equity Snapshot:** Record the sum of all balances into `pnl_history` for chart rendering.

---

## 4. API Endpoints for Coding Agents

### 4.1 Account & Bot Management

* `GET /api/v1/user/balance`: Returns total equity, available balance, and initial balance.
* `POST /api/v1/bots`: Creates a bot.
* Payload: `{"name": string, "market": "BTC_5M", "allocation": number}`


* `DELETE /api/v1/bots/{id}`: Liquidates bot and returns funds to user.

### 4.2 Analytics & Charts

* `GET /api/v1/analytics/equity-curve`: Returns time-series data for the Linechart.
* Response: `[{"t": timestamp, "v": total_equity}, ...]`


* `GET /api/v1/bots/{id}/performance`: Returns specific bot P&L history and ROI.

---

## 5. Edge Case Handling

1. **Bankruptcy:** If a bot's `current_balance` drops to 0, it is automatically paused. The user must either delete the bot (recovering $0) or allocate more funds from `available_balance`.
2. **Halt Resolution:** If a Polymarket session is cancelled or tied, the `Stake` is returned to `Bot.current_balance` with 0 P&L.
3. **Simultaneous Deletion:** Prevent balance duplication by using database transactions (ACID) when moving funds between `User` and `Bot` tables.

---

## 6. Visualization Requirements

* **Performance Chart:** A Linechart displaying `User Total Equity` over time.
* **Markers:** Each point on the chart corresponds to a **Resolution Event** of a BTC 5m/15m market.
* **Comparison:** Option to overlay multiple Bot ROI curves on a single chart to identify which strategy/market performs better.
