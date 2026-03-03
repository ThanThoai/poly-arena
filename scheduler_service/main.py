"""
Scheduler Service — standalone entry point.

Runs settlement and stuck-order sweep as an independent process,
publishing a heartbeat to Redis so the API can report status.

Usage:
    python -m scheduler_service.main
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

# Ensure project root is on sys.path so we can import services.*
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apscheduler.schedulers.background import BackgroundScheduler

from database import SessionLocal
from services.redis_client import get_sync_redis, close_sync_redis
from services.settlement import settle_pending_trades, sweep_stuck_orders
from services.user_balance import snapshot_all_user_balances
from scheduler_service.config import (
    SETTLEMENT_CRON_SECOND,
    STUCK_SWEEP_INTERVAL_MIN,
    HEARTBEAT_INTERVAL_S,
    HEARTBEAT_KEY,
    HEARTBEAT_TTL_S,
    BALANCE_SNAPSHOT_CRON_SECOND,
)

_log_level = logging.DEBUG if os.getenv("DEBUG", "").strip() in ("1", "true", "yes") else logging.INFO

logging.basicConfig(
    level=_log_level,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)
log = logging.getLogger("scheduler_service")

_redis = None


def _run_settlement() -> None:
    """Called every minute at :05 to auto-settle pending trades."""
    db = SessionLocal()
    try:
        settle_pending_trades(db)
    except Exception as exc:
        log.error("Settlement job error: %s", exc)
    finally:
        db.close()


def _run_stuck_sweep() -> None:
    """Called every 5 minutes to catch PENDING trades missed by normal settlement."""
    db = SessionLocal()
    try:
        resolved = sweep_stuck_orders(db)
        if resolved:
            log.info("Stuck sweep resolved %d trade(s)", resolved)
    except Exception as exc:
        log.error("Stuck sweep job error: %s", exc)
    finally:
        db.close()


def _run_balance_snapshot() -> None:
    """Called every 5 minutes at :10s to snapshot all user balances."""
    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        # Align to M5 candle boundary (floor to nearest 5 min)
        candle_open = int(now.timestamp()) // 300 * 300
        session_label = f"M5:{candle_open}"
        count = snapshot_all_user_balances(db, session_label=session_label)
        log.info("Balance snapshot: %d users (session=%s)", count, session_label)
    except Exception as exc:
        log.error("Balance snapshot job error: %s", exc)
    finally:
        db.close()


def _publish_heartbeat() -> None:
    """Publish scheduler status to Redis with TTL."""
    try:
        payload = json.dumps({
            "status": "UP",
            "pid": os.getpid(),
            "ts": datetime.now(timezone.utc).isoformat(),
        })
        _redis.set(HEARTBEAT_KEY, payload, ex=HEARTBEAT_TTL_S)
    except Exception as exc:
        log.error("Heartbeat publish error: %s", exc)


async def main():
    global _redis

    log.info("Scheduler Service starting...")

    # 1. Connect Redis (sync — for heartbeat + APScheduler jobs are sync)
    _redis = get_sync_redis()
    try:
        _redis.ping()
        log.info("Redis connected: %s", os.getenv("REDIS_URL", "redis://localhost:6379"))
    except Exception as exc:
        log.error("Cannot connect to Redis: %s", exc)
        sys.exit(1)

    # 2. Create scheduler
    scheduler = BackgroundScheduler()

    scheduler.add_job(
        _run_settlement,
        trigger="cron",
        minute="*",
        second=SETTLEMENT_CRON_SECOND,
        id="settlement",
        replace_existing=True,
    )
    scheduler.add_job(
        _run_stuck_sweep,
        trigger="interval",
        minutes=STUCK_SWEEP_INTERVAL_MIN,
        id="stuck-sweep",
        replace_existing=True,
    )
    scheduler.add_job(
        _run_balance_snapshot,
        trigger="cron",
        minute="*/5",
        second=BALANCE_SNAPSHOT_CRON_SECOND,
        id="balance-snapshot",
        replace_existing=True,
    )
    scheduler.add_job(
        _publish_heartbeat,
        trigger="interval",
        seconds=HEARTBEAT_INTERVAL_S,
        id="heartbeat",
        replace_existing=True,
    )

    scheduler.start()
    log.info("Scheduler started (pid=%d)", os.getpid())

    # Publish first heartbeat immediately
    _publish_heartbeat()

    # 4. Wait for shutdown signal
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()

    def _signal_handler():
        log.info("Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    log.info("Scheduler Service running — press Ctrl+C to stop")
    await shutdown_event.wait()

    # 5. Cleanup
    log.info("Shutting down...")
    scheduler.shutdown(wait=False)
    close_sync_redis()
    log.info("Scheduler Service stopped")


if __name__ == "__main__":
    asyncio.run(main())
