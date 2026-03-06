"""
Centralized timing & timeout constants for PolyArena.

All timeout, TTL, interval and threshold values live here so they can be
tuned in one place.  Import what you need:

    from config.timing import HTTP_TIMEOUT, ME_DUST_THRESHOLD
"""

import os
from decimal import Decimal

# ── Feed Mode ───────────────────────────────────────────────────────────────

FEED_MODE = os.getenv("FEED_MODE", "rest").lower()  # "rest" | "websocket"

# ── HTTP Timeouts ────────────────────────────────────────────────────────────

HTTP_TIMEOUT = 10.0              # default for REST calls (Polymarket, Binance, …)
HTTP_TIMEOUT_FAST = 8.0          # lighter endpoints (orderbook batch, prices)
HTTP_TIMEOUT_DISCOVERY = 15.0    # token discovery (Gamma API can be slow)

# ── WebSocket ────────────────────────────────────────────────────────────────

WS_PING_INTERVAL_S = 10         # PING heartbeat interval
WS_CLOSE_TIMEOUT_S = 5          # graceful close timeout

# ── Price Cache & Staleness ──────────────────────────────────────────────────

PRICE_CACHE_TTL_S = 120         # Redis EXPIRE on price hashes (survives session gaps)
PRICE_STALE_THRESHOLD_S = 45    # FastAPI treats older prices as stale
API_PRICE_CACHE_TTL_S = 5       # in-memory cache for /prices REST endpoint
API_ORDERBOOK_CACHE_TTL_S = 5   # in-memory cache for /orderbook REST endpoint
SLUG_CACHE_TTL_S = 300          # slug → token_ids cache (matches shortest candle M5)

# ── Matching Engine ──────────────────────────────────────────────────────────

ME_BOOK_STALE_MAX_S = 120       # skip matching when book is older than this
ME_BOOK_STALE_DEFAULT_S = 30.0  # default max_age_s for is_stale()
ME_CLEANUP_INTERVAL = 50        # run cleanup every N calls to run_matching
ME_DEFAULT_SLIPPAGE = Decimal("0.10")    # 10% max slippage for MARKET orders
ME_DUST_THRESHOLD = Decimal("0.000001")  # residual size below this → zero

# ── Boundary Guard ───────────────────────────────────────────────────────────

BOUNDARY_GUARD_S = 15           # auto-bump to next session if < N seconds remain

# ── Token Registry ───────────────────────────────────────────────────────────

TOKEN_PREFETCH_CANDLES = 5      # future candles to prefetch token_ids for
TOKEN_REFRESH_OFFSET_S = 5      # seconds after candle boundary before fetching
TOKEN_REFRESH_MAX_RETRIES = 6   # max retries when market not ready
TOKEN_REFRESH_RETRY_DELAY_S = 5 # seconds between retries

# ── Scheduler & Settlement ───────────────────────────────────────────────────

SETTLEMENT_CRON_SECOND = 5      # settlement runs at :05 each minute
STUCK_SWEEP_INTERVAL_MIN = 5    # sweep stuck orders every N minutes
STUCK_ORDER_THRESHOLD_MIN = 10  # settlement_at + N min → stuck
NULL_SETTLE_THRESHOLD_HOURS = 2  # created_at + N hours (no settlement_at) → stuck
HEARTBEAT_INTERVAL_S = 30       # publish heartbeat every N seconds
HEARTBEAT_TTL_S = 60            # Redis TTL for heartbeat key
BALANCE_SNAPSHOT_CRON_SECOND = 15  # balance snapshot at :15s (10s buffer after settlement at :05s)

# ── Queue & Stream ───────────────────────────────────────────────────────────

BRPOP_TIMEOUT_S = 1             # OrderConsumer BRPOP blocking timeout

# ── Session Lifecycle ──────────────────────────────────────────────────────

REQUIRED_FUTURE_SESSIONS = 3       # current + 3 future = 4 total per (sym, tf)
SESSION_PRE_CREATE_BUFFER_S = 20   # create next session 20s before candle boundary
SESSION_CLEANUP_DELAY_S = 10       # cleanup 10s after session period ends
SESSION_LIFECYCLE_TICK_S = 5       # lifecycle check interval

# ── REST Poller ──────────────────────────────────────────────────────────────

REST_POLL_INTERVAL_S = 0.2      # 200ms between poll cycles
REST_POLL_TIMEOUT_S = 2.0       # HTTP timeout per REST fetch
REST_POLL_MAX_CONCURRENT = 8    # Max concurrent REST fetches

# ── Timeframe Durations ─────────────────────────────────────────────────────

TF_SECONDS: dict[str, int] = {
    "M5":  300,
    "M15": 900,
}
