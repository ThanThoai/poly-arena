"""
Polymarket orderbook client.

Fetches best bid/ask for UP/DOWN prediction markets given symbol + timeframe.

    from services.polymarket import PolymarketClient

    client = PolymarketClient()
    result = client.get_orderbook("ETH", "5m", "UP")
    print(result["min_ask"], result["max_bid"])
"""

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo
import httpx

_GAMMA_URL = "https://gamma-api.polymarket.com/events"
_CLOB_URL = "https://clob.polymarket.com/book"

# Timeframe string normalization (accept both "M5" and "5m" styles)
_TF_NORMALIZE: dict[str, str] = {
    "M5": "5m",
    "M15": "15m",
    "H1": "1h",
    "5m": "5m",
    "15m": "15m",
    "1h": "1h",
}

_TF_SECONDS: dict[str, int] = {
    "5m": 5 * 60,
    "15m": 15 * 60,
    "1h": 60 * 60,
}

# Polymarket token index: 0 = UP, 1 = DOWN
_STATUS_INDEX: dict[str, int] = {
    "UP": 0,
    "DOWN": 1,
}

_SYMBOL_INDEX: dict[str, str] = {
    "btc": "bitcoin",
    "eth": "ethereum",
    "sol": "solana",
    "xrp": "xrp",
}


@dataclass
class OrderbookResult:
    symbol: str
    timeframe: str
    status: str
    min_ask: float
    max_bid: float
    token_id: str


def get_current_time_et():
    """
    Return the current ET time formatted as Polymarket H1 slug suffix.

    Polymarket slugs use non-zero-padded day and hour:
        bitcoin-up-or-down-february-26-2pm-et   ← correct
        bitcoin-up-or-down-february-26-02pm-et  ← WRONG (leading zero)
        bitcoin-up-or-down-february-06-2pm-et   ← WRONG (leading zero on day)
    """
    et_tz = ZoneInfo("America/New_York")
    now_et = datetime.now(et_tz)

    month = now_et.strftime("%B").lower()                      # "february"
    day   = str(now_et.day)                                    # "26" (no pad)
    hour  = str(now_et.hour % 12 or 12)                        # "2"  (no pad)
    ampm  = "am" if now_et.hour < 12 else "pm"

    return f"{month}-{day}-{hour}{ampm}-et"


class PolymarketClient:
    def __init__(self, timeout: float = 10.0):
        self._http = httpx.Client(timeout=timeout)

    # ── helpers ───────────────────────────────────────────────────────────────

    def _next_settlement(self, tf_norm: str) -> int:
        """Return the settlement timestamp of the current candle.

        Examples (period = 5 min):
            22:58 → 23:00   (2 min remaining in current candle)
            22:55 → 23:00   (exactly on boundary → end of THIS period)
            23:00 → 23:05   (new candle just started)
        """
        period = _TF_SECONDS[tf_norm]
        now = int(time.time())
        return now - (now % period)

    def _future_settlements(self, tf_norm: str, count: int = 5) -> list[int]:
        """Return settlement timestamps for the next `count` candles after the current one."""
        period = _TF_SECONDS[tf_norm]
        current = self._next_settlement(tf_norm)
        return [current + period * (i + 1) for i in range(count)]

    def _slug(self, symbol: str, tf_norm: str, settlement_ts: int) -> str:
        return f"{symbol.lower()}-updown-{tf_norm}-{settlement_ts}"

    def _slug_v2(self, symbol: str):
        symbol = _SYMBOL_INDEX[symbol.lower()]
        return f"{symbol}-up-or-down-{get_current_time_et()}"

    def _slug_v2_at(self, symbol: str, settlement_ts: int) -> str:
        """Build H1 slug for a specific settlement timestamp."""
        et_tz = ZoneInfo("America/New_York")
        dt = datetime.fromtimestamp(settlement_ts, tz=et_tz)
        month = dt.strftime("%B").lower()
        day = str(dt.day)
        hour = str(dt.hour % 12 or 12)
        ampm = "am" if dt.hour < 12 else "pm"
        sym = _SYMBOL_INDEX[symbol.lower()]
        return f"{sym}-up-or-down-{month}-{day}-{hour}{ampm}-et"

    def _token_ids(self, slug: str) -> list[str]:
        resp = self._http.get(_GAMMA_URL, params={"slug": slug})
        resp.raise_for_status()
        data = resp.json()
        raw = data[0]["markets"][0]["clobTokenIds"]
        ids = raw[2:-2].replace('"', "").split(",")
        return [i.strip() for i in ids]

    def _best_prices(self, token_id: str) -> tuple[float, float]:
        """Return (min_ask, max_bid) for the given token."""
        resp = self._http.get(_CLOB_URL, params={"token_id": token_id})
        resp.raise_for_status()
        book = resp.json()
        min_ask = min(book["asks"], key=lambda x: float(x["price"]))
        max_bid = max(book["bids"], key=lambda x: float(x["price"]))
        return float(min_ask["price"]), float(max_bid["price"])

    # ── public API ────────────────────────────────────────────────────────────

    def get_orderbook(
        self, symbol: str, timeframe: str, status: str
    ) -> OrderbookResult:
        """
        Fetch best ask / best bid for a Polymarket UP/DOWN market.

        Args:
            symbol:        e.g. "ETH", "BTC"
            timeframe:     e.g. "5m", "15m", "1h"  (or "M5", "M15", "H1")
            status:        "UP" or "DOWN"
            settlement_ts: Unix timestamp of the settlement candle.
                           Defaults to the next candle boundary.
        """
        tf_norm = _TF_NORMALIZE.get(timeframe.upper(), timeframe.lower())
        if tf_norm not in _TF_SECONDS:
            raise ValueError(f"Unsupported timeframe: {timeframe!r}")

        status = status.upper()
        if status not in _STATUS_INDEX:
            raise ValueError(f"status must be 'UP' or 'DOWN', got {status!r}")

        if tf_norm == "1h":
            slug = self._slug_v2(symbol)
        else:
            ts = self._next_settlement(tf_norm)
            slug = self._slug(symbol, tf_norm, ts)

        ids = self._token_ids(slug)
        token_id = ids[_STATUS_INDEX[status]]
        min_ask, max_bid = self._best_prices(token_id)

        return OrderbookResult(
            symbol=symbol.upper(),
            timeframe=tf_norm,
            status=status,
            min_ask=min_ask,
            max_bid=max_bid,
            token_id=token_id,
        )

    def get_token_id_at(
        self, symbol: str, timeframe: str, status: str, settlement_ts: int,
    ) -> Optional[str]:
        """
        Fetch token_id for a specific settlement timestamp.

        Returns token_id string, or None if the market doesn't exist yet.
        """
        tf_norm = _TF_NORMALIZE.get(timeframe.upper(), timeframe.lower())
        if tf_norm not in _TF_SECONDS:
            raise ValueError(f"Unsupported timeframe: {timeframe!r}")

        status = status.upper()
        if status not in _STATUS_INDEX:
            raise ValueError(f"status must be 'UP' or 'DOWN', got {status!r}")

        if tf_norm == "1h":
            slug = self._slug_v2_at(symbol, settlement_ts)
        else:
            slug = self._slug(symbol, tf_norm, settlement_ts)

        try:
            ids = self._token_ids(slug)
            return ids[_STATUS_INDEX[status]]
        except Exception:
            return None

    def close(self):
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ── Test ──────────────────────────────────────────────────────────────────────


def _test():
    """
    Quick smoke-test — runs against live Polymarket APIs.

    Usage:
        python services/polymarket.py
        python services/polymarket.py --symbol ETH --tf 15m
    """

    # print(f"Testing PolymarketClient  symbol={symbol}  tf={args.tf}")
    print("-" * 52)

    tf = "5m"
    symbol = "ETH"

    with PolymarketClient() as client:
        # Calculate and display the slug being queried
        tf_norm = _TF_NORMALIZE.get(tf.capitalize, tf.lower())
        ts = client._next_settlement(tf_norm)
        slug = client._slug(symbol, tf_norm, ts)
        print(f"Slug            : {slug}")
        print(f"Settlement ts   : {ts}")
        print()

        for status in ("UP", "DOWN"):
            try:
                result = client.get_orderbook(symbol, tf, status, settlement_ts=ts)
                print(f"[{status:<4}]  token  : {result.token_id[:24]}...")
                print(f"       min_ask : {result.min_ask}")
                print(f"       max_bid : {result.max_bid}")
                spread = round(result.min_ask - result.max_bid, 6)
                print(f"       spread  : {spread}")
            except Exception as e:
                print(f"[{status:<4}]  ERROR  : {e}")
            print()


if __name__ == "__main__":
    # _test()
    tf = "5m"
    symbol = "ETH"

    with PolymarketClient() as client:
        # Calculate and display the slug being queried
        # tf_norm = _TF_NORMALIZE.get(tf.capitalize, tf.lower())
        # ts = client._next_settlement(tf_norm)
        # slug = client._slug(symbol, tf_norm, ts)
        # print(f"Slug            : {slug}")
        # print(f"Settlement ts   : {ts}")
        # print()

        for status in ("UP", "DOWN"):
            try:
                result = client.get_orderbook(symbol, tf, status)
                print(f"[{status:<4}]  token  : {result.token_id}")
                print(f"       min_ask : {result.min_ask}")
                print(f"       max_bid : {result.max_bid}")
                spread = round(result.min_ask - result.max_bid, 6)
                print(f"       spread  : {spread}")
            except Exception as e:
                print(f"[{status:<4}]  ERROR  : {e}")
            print()
