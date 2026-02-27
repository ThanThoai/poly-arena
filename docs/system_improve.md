System Improvement Specification: PolyArena Distributed Matching Engine
1. Executive Summary
The current architecture splits the Order Ingestion (FastAPI API) and the Matching Engine (WS Feed Service) using Redis Lists/Streams. While scalable, this distributed nature introduces race conditions, state inconsistencies, and latency-induced slippage. Additionally, there is a critical domain logic risk regarding how Polymarket tokens are priced versus how Binance settlement is executed.

This document outlines the required refactoring to fix matching errors, ensure atomicity, and correct the settlement mathematics.

2. Critical Fix 1: The Settlement Domain Logic (Oracle Mismatch)
The Bug: The system matches orders against the Polymarket CLOB (where prices are probabilities between $0.01 and $0.99). However, CLAUDE.md states: "Settlement uses Binance candles as canonical price truth". If the system uses the raw Binance price (e.g., BTC at $65,000) to calculate PnL for a Polymarket token, the math will fail catastrophically.
The Fix: Binance is only the Oracle (Condition), not the Value (Multiplier).

Implementation Instructions for services/settlement.py:
Resolution Logic: When the APScheduler runs at :05s, it fetches the Binance OHLC candle to determine the outcome (e.g., Close > Open = GREEN, Close < Open = RED).

Payout Math: For all resting orders (status == FILLED or PARTIAL with position_closed == False):

If Order.forecast == Actual_Outcome: Payout = order.filled * 1.00 (User wins $1 per share).

If Order.forecast != Actual_Outcome: Payout = order.filled * 0.00 (User loses).

Action: The scheduler must publish these final payouts to a new Redis stream (e.g., stream:order:settlements) so the API can update user balances, rather than updating the DB directly if avoiding DB locks.

3. Critical Fix 2: Threading vs. Async in the WS Feed
The Bug: CLAUDE.md explicitly mentions: "ShadowOrderbook uses threading.Lock per order for bracket/fill race conditions".
Since the ws_feed_service is deeply reliant on websockets (which runs on Python's asyncio event loop), using synchronous threading.Lock will block the entire event loop. This causes the WS client to miss incoming price updates, leading to a stale orderbook and terrible matching errors (executing orders at prices that no longer exist).

Implementation Instructions for services/matching_engine.py:
Remove all threading.Lock.

Replace with asyncio.Lock.

Python
import asyncio

class SimulatedOrder:
    def __init__(self, ...):
        self._state_lock = asyncio.Lock() # Use this instead of threading.Lock
Ensure run_matching() and monitor_bracket_orders() are async functions and use async with order._state_lock: when mutating filled, status, or already_exited.

4. Critical Fix 3: IPC Latency and Market Order (IOC) Slippage
The Bug: 1. User sends MARKET order to API.
2. API validates, saves PENDING to DB, pushes to Redis queue:orders:new.
3. Network delay + WS Feed processing delay.
4. WS Feed pops the order and runs _match_order().
Because of the delay (steps 1 to 4), the Polymarket orderbook might have shifted significantly. The MARKET order will sweep the current book in the WS Feed, which might execute at terrible prices compared to what the user saw on the UI.

Implementation Instructions for Distributed IOC:
Slippage Protection: The API (routers/binary_options.py) MUST attach the current_best_ask (for BUY) or current_best_bid (for SELL) fetched from Redis cache directly into the MARKET order payload sent to queue:orders:new.

WS Feed Verification: When ws_feed_service pops the MARKET order:

It compares the order's expected_price with the ShadowOrderbook's actual price.

If the actual price is drastically worse (e.g., > 5% slippage), the MatchingEngine should immediately CANCEL the order (or partial cancel) instead of sweeping the book blindly.

Acknowledge Fast: After applying IOC logic, the WS Feed must immediately push the result to stream:order:fills or stream:order:cancels.

5. Critical Fix 4: Stream Consumption & State Consistency
The Bug: The API consumes stream:bracket:exits, stream:order:fills, and stream:order:cancels via XREADGROUP. If the API process restarts while processing a fill, the message might be acknowledged (XACK) but the SQLite DB isn't updated, leaving an order permanently PENDING.

Implementation Instructions for main.py (Stream Consumers):
Database Transaction Wrap: The consumer loop must wrap the DB update and the Redis XACK in a single atomic-like try-except block.

Python
try:
    # 1. Start DB Transaction
    db.begin()
    # 2. Update Order status = FILLED, update balance
    db.commit()
    # 3. ONLY XACK Redis if DB commit is successful
    redis.xack("stream:order:fills", "group_api", message_id)
except Exception:
    db.rollback()
    # Do not XACK. The message remains in the Pending Entries List (PEL).
PEL Recovery: On API startup, before calling XREADGROUP ... > (read new messages), the system MUST read from its Pending Entries List (XREADGROUP ... 0-0) to process any fills that crashed midway during the previous shutdown.

6. Testing Instructions for the AI
When writing tests for these fixes:

Mock the WS Feed: Simulate a rapidly changing orderbook. Push a MARKET order to Redis and ensure the engine respects the new asyncio.Lock and slippage boundaries.

Test the Scheduler: Explicitly mock a Binance API response returning a RED candle, and assert that a Polymarket order forecasting GREEN results in exactly 0.00 payout, and forecasting RED results in exactly 1.00 * filled_qty payout.