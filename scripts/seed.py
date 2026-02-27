"""
Seed 3 baseline demo bots into orders.db.

Each bot trades a single timeframe with a pure-random forecast algorithm.
They serve as baselines to compare real strategy bots against.

Usage:
    python seed.py          # insert (skip if bots already exist)
    python seed.py --reset  # wipe everything first, then seed
"""

import argparse
import random
import secrets
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from database import Base, SessionLocal, engine
from models import INITIAL_BALANCE, BinaryOption, BOResult, BOSymbol, BOTimeframe, Bot
from services.settlement import calc_settlement_time

# ── Config ─────────────────────────────────────────────────────────────────────

SEED        = 42
PAYOUT_RATE = 1.00          # WIN → +100% amount

SYMBOLS     = ["BTC", "ETH", "SOL", "XRP"]
DIRECTIONS  = ["GREEN", "RED"]

# name, timeframe, n_trades (multiple of 4 for even symbol spread), amount_range
BOTS = [
    ("Baseline-M5",  BOTimeframe.M5,  240, (50, 200)),
    ("Baseline-M15", BOTimeframe.M15, 120, (50, 200)),
    ("Baseline-H1",  BOTimeframe.H1,   60, (50, 200)),
]

# ── Helpers ────────────────────────────────────────────────────────────────────

def make_rng() -> random.Random:
    return random.Random(SEED)


def spread_dates(n: int, days: int, rnd: random.Random) -> list[datetime]:
    """n UTC datetimes spread pseudo-randomly over the last `days` days."""
    now    = datetime.now(timezone.utc)
    start  = now - timedelta(days=days)
    total  = int(days * 24 * 3600)
    return sorted(
        start + timedelta(seconds=rnd.randint(0, total))
        for _ in range(n)
    )


def even_symbols(n: int, rnd: random.Random) -> list[str]:
    """Shuffle symbols with equal count per symbol. n must be multiple of 4."""
    reps = n // len(SYMBOLS)
    seq  = SYMBOLS * reps
    rnd.shuffle(seq)
    return seq


# ── Seed ───────────────────────────────────────────────────────────────────────

def seed(reset: bool = False) -> None:
    Base.metadata.create_all(bind=engine)

    db  = SessionLocal()
    rnd = make_rng()

    try:
        if reset:
            db.execute(text("DELETE FROM binary_options"))
            db.execute(text("DELETE FROM bots"))
            db.commit()
            print("Cleared all existing data.")

        for (name, timeframe, n_trades, amount_range) in BOTS:

            # ── create bot ─────────────────────────────────────────────────────
            existing = db.query(Bot).filter(Bot.bot_name == name).first()
            if existing:
                print(f"  Bot already exists, skipping: {name}")
                continue

            bot = Bot(
                bot_name        = name,
                api_key         = secrets.token_urlsafe(32),
                initial_balance = INITIAL_BALANCE,
                balance         = INITIAL_BALANCE,
            )
            db.add(bot)
            db.flush()
            print(f"  Created bot: {name}  ({n_trades} trades, {timeframe.value})")

            # ── generate trades ────────────────────────────────────────────────
            dates         = spread_dates(n_trades, days=30, rnd=rnd)
            symbols       = even_symbols(n_trades, rnd)
            balance_delta = 0.0
            pending_count = 3       # last 3 chronological trades stay PENDING

            for i, (created_at, sym_str) in enumerate(zip(dates, symbols)):
                forecast      = rnd.choice(DIRECTIONS)
                amount        = round(rnd.uniform(*amount_range), 2)
                settlement_at = calc_settlement_time(timeframe.value, created_at)
                is_pending    = i >= (n_trades - pending_count)

                if is_pending:
                    result = BOResult.PENDING
                    profit = None
                else:
                    # Pure random: ~50% win rate
                    if rnd.random() < 0.50:
                        result = BOResult.WIN
                        profit = round(amount * PAYOUT_RATE, 8)
                    else:
                        result = BOResult.LOSS
                        profit = -amount
                    balance_delta += profit

                db.add(BinaryOption(
                    bot_name      = name,
                    symbol        = BOSymbol(sym_str),
                    timeframe     = timeframe,
                    forecast      = forecast,
                    amount        = amount,
                    result        = result,
                    profit        = profit,
                    settlement_at = settlement_at,
                    created_at    = created_at,
                ))

            bot.balance = round(INITIAL_BALANCE + balance_delta, 8)

        db.commit()

        # ── summary ────────────────────────────────────────────────────────────
        total_bots   = db.query(Bot).count()
        total_trades = db.query(BinaryOption).count()
        wins         = db.query(BinaryOption).filter(BinaryOption.result == BOResult.WIN).count()
        losses       = db.query(BinaryOption).filter(BinaryOption.result == BOResult.LOSS).count()
        pending      = db.query(BinaryOption).filter(BinaryOption.result == BOResult.PENDING).count()
        decided      = wins + losses
        wr           = wins / decided * 100 if decided else 0.0

        print()
        print("── Seed complete ─────────────────────────────────────")
        print(f"  Bots    : {total_bots}")
        print(f"  Trades  : {total_trades}  (WIN {wins} / LOSS {losses} / PENDING {pending})")
        print(f"  Win rate: {wr:.1f}%  (expected ~50% random baseline)")
        print("──────────────────────────────────────────────────────")

    finally:
        db.close()


# ── Entry ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true",
                        help="Delete all bots and trades before seeding")
    args = parser.parse_args()

    try:
        seed(reset=args.reset)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
