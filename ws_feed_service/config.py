"""
Constants and configuration for the WS Feed Service.
"""

# ── Redis keys ───────────────────────────────────────────────────────────────

QUEUE_ORDERS_NEW = "queue:orders:new"
STREAM_BRACKET_EXITS = "stream:bracket:exits"

# ── Price cache ──────────────────────────────────────────────────────────────

PRICE_KEY_PREFIX = "price"          # price:{SYM}:{TF}:{DIR}
PRICE_CACHE_TTL_S = 60              # EXPIRE on each HSET
STALE_THRESHOLD_S = 30              # FastAPI treats older prices as miss

# ── Queue / stream tuning ────────────────────────────────────────────────────

BRPOP_TIMEOUT_S = 1                 # OrderConsumer BRPOP blocking timeout
STREAM_MAXLEN = 10_000              # XADD MAXLEN ~ for bracket exits

# ── Market constants ─────────────────────────────────────────────────────────

SYMBOLS = ["BTC", "ETH", "SOL", "XRP"]
TIMEFRAMES = ["M5", "M15", "H1"]
DIRECTIONS = ["UP", "DOWN"]
