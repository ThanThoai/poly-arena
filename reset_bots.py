"""
Reset script: clear trade data and reset bot balances.
Bots and users are preserved.

Tables cleared (in order):
  1. bot_achievements
  2. balance_history
  3. binary_options

Preserved:
  - bots (balance reset to initial_balance)
  - users (unchanged)

Usage:
  python reset_bots.py              # with confirmation prompt
  python reset_bots.py --yes        # skip confirmation
"""

import sys
from sqlalchemy import text
from database import SessionLocal
from models import Bot


TABLES_TO_CLEAR = [
    "bot_achievements",
    "balance_history",
    "binary_options",
]


def reset(skip_confirm: bool = False):
    db = SessionLocal()
    try:
        bots = db.query(Bot).all()
        if bots:
            print(f"Found {len(bots)} bot(s) (will be preserved):")
            for b in bots:
                print(f"  • {b.bot_name}  ${b.balance:,.2f} → ${b.initial_balance:,.2f}")

        if not skip_confirm:
            print()
            answer = input(
                "This will DELETE all trades, balance history, and achievements.\n"
                "Bots and users will be preserved (balances reset to initial).\n"
                "Type 'yes' to confirm: "
            )
            if answer.strip().lower() != "yes":
                print("Aborted.")
                return

        for table in TABLES_TO_CLEAR:
            result = db.execute(text(f"DELETE FROM {table}"))
            print(f"  {table}: {result.rowcount} rows deleted")

        # Reset bot balances to initial
        for b in bots:
            b.balance = b.initial_balance
        print(f"  bots: {len(bots)} bot balances reset to initial_balance")

        db.commit()
        print("Done. Trade data cleared, bots and users preserved.")
    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    skip = "--yes" in sys.argv or "-y" in sys.argv
    reset(skip_confirm=skip)
