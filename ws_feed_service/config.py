"""
Constants and configuration for the WS Feed Service.
"""

from config.timing import (                         # noqa: F401 — re-exported
    PRICE_CACHE_TTL_S,
    PRICE_STALE_THRESHOLD_S as STALE_THRESHOLD_S,
    BRPOP_TIMEOUT_S,
)

# ── Redis keys ───────────────────────────────────────────────────────────────

QUEUE_ORDERS_PREFIX = "queue:orders"    # Per-session: queue:orders:{SYM}:{TF}:{CANDLE_TS}
QUEUE_ORDERS_NEW = "queue:orders:new"   # DEPRECATED — kept for backward reference only
STREAM_BRACKET_EXITS = "stream:bracket:exits"
STREAM_ORDER_CANCELS = "stream:order:cancels"
STREAM_ORDER_FILLS   = "stream:order:fills"
STREAM_MARKET_RESOLVED = "stream:market:resolved"

# ── Price cache ──────────────────────────────────────────────────────────────

PRICE_KEY_PREFIX = "price"          # price:{SYM}:{TF}:{DIR}

# ── Orderbook depth ─────────────────────────────────────────────────────────

ORDERBOOK_KEY_PREFIX = "orderbook"  # orderbook:{SYM}:{TF}:{DIR}
ORDERBOOK_DEPTH_LEVELS = 20         # Top N levels to publish per side

# ── Queue / stream tuning ────────────────────────────────────────────────────

STREAM_MAXLEN = 10_000              # XADD MAXLEN ~ for bracket exits

# ── UI Future Sessions ──────────────────────────────────────────────────

UI_FUTURE_SESSIONS = 3              # Number of future sessions to expose to UI (A+1, A+2, A+3)
UI_PAST_SESSIONS = 1                # Number of past sessions to keep for UI

# ── Market constants ─────────────────────────────────────────────────────────

SYMBOLS = ["BTC", "ETH"]
TIMEFRAMES = ["M5", "M15"]
DIRECTIONS = ["UP", "DOWN"]

# ── Price history recording ─────────────────────────────────────────────────

PRICE_HISTORY_INTERVAL_S = 10  # one snapshot per combo every 10s
