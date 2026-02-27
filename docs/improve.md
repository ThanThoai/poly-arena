# System Improvement Specification: Polymarket Shadow Matching Engine
**Document Goal:** Refactor the existing matching engine to resolve concurrency bottlenecks, prevent floating-point precision errors, ensure thread-safety during state mutations, and protect users from unbounded slippage.

---

## Improvement 1: Centralized Async Monitoring (Replacing Per-Order Threads)
**Problem:** The current architecture uses a "Per-order daemon thread" polling every 2 seconds. This operates at $O(N)$ thread complexity, leading to context-switching overhead, GIL bottlenecks (in Python), and potential Out-Of-Memory (OOM) crashes when thousands of orders are active.
**Solution:** Implement a single, event-driven centralized monitor.

### 1.1. Architecture Refactor
* **Remove:** Delete the logic that spawns a new thread/task inside `place_virtual_order()` or upon order creation.
* **Introduce:** `BracketOrderMonitor` (Single Task/Thread).
* **Trigger Mechanism:** Transition from "Time-based Polling" (every 2s) to "Event-Driven Polling". The monitor should ONLY run when the `OrderbookState` changes (specifically triggered by `best_bid_ask` or `last_trade_price` WebSocket events).

### 1.2. Pseudo-code Spec for Coding Agents
```python
class BracketOrderMonitor:
    def __init__(self, active_orders_registry):
        self.registry = active_orders_registry
        
    async def run_evaluation_cycle(self, current_best_bid, current_best_ask):
        """Triggered strictly by WebSocket events, NOT a while(True) sleep loop."""
        eligible_orders = self.registry.get_bracket_eligible_orders()
        for order in eligible_orders:
            # Execute TP/SL logic synchronously within this single event cycle
            self._evaluate_and_trigger(order, current_best_bid, current_best_ask)
Improvement 2: Precision & "Dust" EliminationProblem: Using standard IEEE 754 float for prices and quantities causes arithmetic artifacts (e.g., 0.3 - 0.2 = 0.09999999999999998). This prevents ask_size from reaching exactly 0, leaving "dust" in the orderbook and potentially causing infinite loops during the sweep.Solution: Enforce strict Decimal/Integer arithmetic.2.1. Implementation RulesData Types: All fields representing price, amount, quantity, filled, and remaining_qty MUST be instantiated using Python's decimal.Decimal (or multiplied by $10^6$ to use int).Validation: Update Pydantic schemas or input validators to cast raw floats from JSON/REST immediately into Decimal.Zero Threshold: When deducting shadow liquidity (asks[ask_price] -= match_qty), if the remaining ask_size is less than a minimal threshold (e.g., Decimal('0.000001')), explicitly pop/delete that price level from the dictionary.

Improvement 3: Thread-Safety & Race Condition PreventionProblem: A LIMIT order might receive a partial fill from the main WebSocket matching thread (_match_order) at the exact same microsecond the BracketOrderMonitor attempts to exit the position because a Stop Loss was hit. This concurrent mutation of order.filled and order.status will corrupt the database/state.Solution: Implement granular locking at the Order level.3.1. Lock ImplementationAdd: A threading.Lock or asyncio.Lock to the SimulatedOrder entity itself (e.g., order._state_lock).Rule: Any workflow attempting to mutate order.filled, order.status, or already_exited MUST acquire this lock first.3.2. Execution FlowPython# In Main Matching Engine (LIMIT FILL):
async def _match_order(self, order):
    async with order._state_lock:
        if order.status in (FILLED, CANCELED): return
        # ... logic to increase order.filled ...

# In Bracket Monitor (TP/SL EXIT):
async def _execute_bracket_exit(self, order, best_bid):
    async with order._state_lock:
        if order.position_closed: return
        qty_to_close = order.filled - order.already_exited
        # ... logic to spawn SELL order and mark position_closed = True ...

Improvement 4: Market Order Slippage ProtectionProblem: The current MARKET order logic sweeps all available asks/bids sequentially. In a thin orderbook (low liquidity), a large MARKET order could sweep the price from $0.50 all the way to $0.99, resulting in an unacceptable avg_entry_price.Solution: Implement a configurable Max Slippage bound.4.1. Parameter AdditionAdd: slippage_tolerance (Percentage, e.g., 0.05 for 5%) or max_price (Decimal) to the MARKET order payload.Default: If not provided, the engine should default to a hardcoded safety net (e.g., Max 10% away from the Best Ask at the time of order arrival).4.2. Modified Matching Algorithm (_match_order)When iterating through sorted(asks, ascending) for a MARKET BUY:Define limit_bound = best_ask * (1 + slippage_tolerance).For each ask_price:IF ask_price > limit_bound: BREAK the loop.Do not continue sweeping. Treat the rest of the order as if the book is empty.Apply standard IOC logic: Any remaining_qty that was prevented from filling due to the slippage bound is immediately CANCELED.