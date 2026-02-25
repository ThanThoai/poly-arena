import fcntl
import logging
import os
from typing import Dict

from apscheduler.schedulers.background import BackgroundScheduler

from database import SessionLocal

logger = logging.getLogger(__name__)

_scheduler = BackgroundScheduler()
_lock_fd = None
_LOCK_PATH = "/tmp/poly-arena-scheduler.lock"


def _run_settlement() -> None:
    """Called every minute to auto-settle pending trades."""
    from services.settlement import settle_pending_trades  # lazy to avoid circular import
    db = SessionLocal()
    try:
        settle_pending_trades(db)
    except Exception as exc:
        logger.error("Settlement job error: %s", exc)
    finally:
        db.close()


def start_scheduler() -> None:
    global _lock_fd

    # Only one worker process should run the scheduler.
    # Try to acquire an exclusive non-blocking lock; skip if another worker holds it.
    try:
        _lock_fd = open(_LOCK_PATH, "w")
        fcntl.flock(_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lock_fd.write(str(os.getpid()))
        _lock_fd.flush()
    except OSError:
        logger.info("Scheduler already running in another worker (pid %s), skipping.", os.getpid())
        _lock_fd.close()
        _lock_fd = None
        return

    _scheduler.add_job(
        _run_settlement,
        trigger="cron",
        minute="*",   # every minute
        second=5,     # 5 s after the minute — gives Binance time to publish the closed candle
        id="settlement",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("Scheduler started in worker pid=%s.", os.getpid())


def stop_scheduler() -> None:
    global _lock_fd

    if _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")

    if _lock_fd is not None:
        fcntl.flock(_lock_fd, fcntl.LOCK_UN)
        _lock_fd.close()
        _lock_fd = None


def scheduler_status() -> Dict:
    job = _scheduler.get_job("settlement")
    return {
        "running":  _scheduler.running,
        "owner_pid": os.getpid() if _lock_fd is not None else None,
        "next_run": job.next_run_time.isoformat() if job and job.next_run_time else None,
    }
