"""Export all order/trade records for a bot to a JSON file."""

import argparse
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import SessionLocal
from models import Bot, BinaryOption


def find_bot(db, identifier: str):
    if identifier.isdigit():
        return db.query(Bot).filter(Bot.id == int(identifier)).first()
    return db.query(Bot).filter(Bot.bot_name == identifier).first()


def default_serializer(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def order_to_dict(o: BinaryOption) -> dict:
    return {
        "id": o.id,
        "bot_name": o.bot_name,
        "symbol": o.symbol.value if o.symbol else None,
        "timeframe": o.timeframe.value if o.timeframe else None,
        "forecast": o.forecast.value if o.forecast else None,
        "order_type": o.order_type,
        "amount": o.amount,
        "original_amount": o.original_amount,
        "num_shares": o.num_shares,
        "avg_price": o.avg_price,
        "limit_price": o.limit_price,
        "ceiling_price": o.ceiling_price,
        "entry_fee": o.entry_fee,
        "result": o.result.value if o.result else None,
        "profit": o.profit,
        "price_open": o.price_open,
        "price_close": o.price_close,
        "tp_price": o.tp_price,
        "sl_price": o.sl_price,
        "exit_price": o.exit_price,
        "exit_trigger": o.exit_trigger,
        "exit_filled": o.exit_filled,
        "exit_at": o.exit_at,
        "walk_prices": o.walk_prices,
        "traces": o.traces,
        "session_id": o.session_id,
        "candle_open": o.candle_open,
        "session_offset": o.session_offset,
        "position_closed": o.position_closed,
        "me_order_id": o.me_order_id,
        "me_order_status": o.me_order_status,
        "ttl": o.ttl,
        "reason": o.reason,
        "order_received_at": o.order_received_at,
        "ask_fetched_at": o.ask_fetched_at,
        "settlement_at": o.settlement_at,
        "created_at": o.created_at,
        "updated_at": o.updated_at,
    }


def main():
    parser = argparse.ArgumentParser(description="Export bot orders/trades to JSON")
    parser.add_argument("bot", help="Bot ID (numeric) or bot_name")
    parser.add_argument("-o", "--output", help="Output file path (default: orders_{bot_name}.json)")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        bot = find_bot(db, args.bot)
        if not bot:
            print(f"Bot not found: {args.bot}", file=sys.stderr)
            sys.exit(1)

        orders = (
            db.query(BinaryOption)
            .filter(BinaryOption.bot_name == bot.bot_name)
            .order_by(BinaryOption.created_at.asc())
            .all()
        )

        data = {
            "bot_id": bot.id,
            "bot_name": bot.bot_name,
            "initial_balance": bot.initial_balance,
            "balance": bot.balance,
            "exported_at": datetime.utcnow().isoformat(),
            "total_orders": len(orders),
            "orders": [order_to_dict(o) for o in orders],
        }

        output_path = args.output or f"orders_{bot.bot_name}.json"
        indent = 2 if args.pretty else None

        with open(output_path, "w") as f:
            json.dump(data, f, default=default_serializer, indent=indent, ensure_ascii=False)

        print(f"Exported {len(orders)} orders for bot '{bot.bot_name}' -> {output_path}")

    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
