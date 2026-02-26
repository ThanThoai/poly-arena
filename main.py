import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy import inspect, text

from database import Base, engine, SessionLocal
from models import BinaryOption, BOResult
from routers import binary_options, bots, dashboard
from services.scheduler import start_scheduler, stop_scheduler
from services.redis_client import get_async_redis, close_async_redis
from ws_feed_service.config import STREAM_BRACKET_EXITS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)

log = logging.getLogger(__name__)


def run_migrations() -> None:
    """Add any columns that exist in the model but are missing from the DB."""
    with engine.begin() as conn:
        inspector = inspect(engine)

        # ── bots table ───────────────────────────────────────────────────────
        if "bots" in inspector.get_table_names():
            existing = {c["name"] for c in inspector.get_columns("bots")}

            # Backfill NULL created_at (rows inserted before column existed)
            conn.execute(text(
                "UPDATE bots SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL"
            ))

            if "initial_balance" not in existing:
                log.info("Migration: adding bots.initial_balance")
                conn.execute(text(
                    "ALTER TABLE bots ADD COLUMN initial_balance REAL DEFAULT 10000.0"
                ))
                conn.execute(text(
                    "UPDATE bots SET initial_balance = 10000.0 WHERE initial_balance IS NULL"
                ))

            if "balance" not in existing:
                log.info("Migration: adding bots.balance")
                conn.execute(text(
                    "ALTER TABLE bots ADD COLUMN balance REAL DEFAULT 10000.0"
                ))
                conn.execute(text(
                    "UPDATE bots SET balance = 10000.0 WHERE balance IS NULL"
                ))

        # ── balance_history table ─────────────────────────────────────────────
        if "balance_history" in inspector.get_table_names():
            conn.execute(text(
                "UPDATE balance_history SET recorded_at = CURRENT_TIMESTAMP WHERE recorded_at IS NULL"
            ))

        # ── binary_options table ──────────────────────────────────────────────
        if "binary_options" in inspector.get_table_names():
            bo_cols = {c["name"] for c in inspector.get_columns("binary_options")}

            # Backfill NULL created_at
            conn.execute(text(
                "UPDATE binary_options SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL"
            ))

            # Rename CALL→GREEN / PUT→RED (legacy data)
            if "value" in bo_cols:
                rows = conn.execute(text(
                    "SELECT COUNT(*) FROM binary_options WHERE value IN ('CALL','PUT')"
                )).scalar()
                if rows:
                    log.info("Migration: renaming CALL→GREEN, PUT→RED (%d rows)", rows)
                    conn.execute(text(
                        "UPDATE binary_options SET value = 'GREEN' WHERE value = 'CALL'"
                    ))
                    conn.execute(text(
                        "UPDATE binary_options SET value = 'RED' WHERE value = 'PUT'"
                    ))

            # Rename column value → forecast
            if "value" in bo_cols and "forecast" not in bo_cols:
                log.info("Migration: renaming binary_options.value → forecast")
                conn.execute(text(
                    "ALTER TABLE binary_options RENAME COLUMN value TO forecast"
                ))

            if "price_open" not in bo_cols:
                log.info("Migration: adding binary_options.price_open")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN price_open REAL"))
                bo_cols.add("price_open")

            if "price_close" not in bo_cols:
                log.info("Migration: adding binary_options.price_close")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN price_close REAL"))
                bo_cols.add("price_close")

            if "settlement_at" not in bo_cols:
                log.info("Migration: adding binary_options.settlement_at")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN settlement_at DATETIME"))
                bo_cols.add("settlement_at")

            if "avg_price" not in bo_cols:
                log.info("Migration: adding binary_options.avg_price")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN avg_price REAL"))
                bo_cols.add("avg_price")

            if "num_shares" not in bo_cols:
                log.info("Migration: adding binary_options.num_shares")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN num_shares REAL"))
                bo_cols.add("num_shares")

            if "reason" not in bo_cols:
                log.info("Migration: adding binary_options.reason")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN reason TEXT"))
                bo_cols.add("reason")

            if "order_received_at" not in bo_cols:
                log.info("Migration: adding binary_options.order_received_at")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN order_received_at DATETIME"))
                bo_cols.add("order_received_at")

            if "ask_fetched_at" not in bo_cols:
                log.info("Migration: adding binary_options.ask_fetched_at")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN ask_fetched_at DATETIME"))
                bo_cols.add("ask_fetched_at")

            # ── Order type ────────────────────────────────────────────────────
            if "limit_price" not in bo_cols:
                log.info("Migration: adding binary_options.limit_price")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN limit_price REAL"))
                bo_cols.add("limit_price")

            # ── Bracket Order / profit matrix columns ─────────────────────────
            if "tp_price" not in bo_cols:
                log.info("Migration: adding binary_options.tp_price")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN tp_price REAL"))
                bo_cols.add("tp_price")

            if "sl_price" not in bo_cols:
                log.info("Migration: adding binary_options.sl_price")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN sl_price REAL"))
                bo_cols.add("sl_price")

            if "exit_price" not in bo_cols:
                log.info("Migration: adding binary_options.exit_price")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN exit_price REAL"))
                bo_cols.add("exit_price")

            if "exit_trigger" not in bo_cols:
                log.info("Migration: adding binary_options.exit_trigger")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN exit_trigger VARCHAR(20)"))
                bo_cols.add("exit_trigger")

            if "exit_filled" not in bo_cols:
                log.info("Migration: adding binary_options.exit_filled")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN exit_filled REAL"))
                bo_cols.add("exit_filled")

            if "me_order_id" not in bo_cols:
                log.info("Migration: adding binary_options.me_order_id")
                conn.execute(text("ALTER TABLE binary_options ADD COLUMN me_order_id VARCHAR(64)"))
                bo_cols.add("me_order_id")

            if "symbol" not in bo_cols:
                log.info("Migration: replacing ticket column with symbol (default BTC)")
                po = "price_open"  if "price_open"  in bo_cols else "NULL"
                pc = "price_close" if "price_close" in bo_cols else "NULL"
                conn.execute(text(f"""
                    CREATE TABLE _bo_new (
                        id          INTEGER NOT NULL PRIMARY KEY,
                        bot_name    VARCHAR(100) NOT NULL,
                        symbol      VARCHAR NOT NULL DEFAULT 'BTC',
                        timeframe   VARCHAR NOT NULL,
                        forecast    VARCHAR NOT NULL,
                        amount      REAL NOT NULL,
                        result      VARCHAR,
                        profit      REAL,
                        price_open  REAL,
                        price_close REAL,
                        created_at  DATETIME,
                        updated_at  DATETIME
                    )
                """))
                conn.execute(text(f"""
                    INSERT INTO _bo_new
                        (id, bot_name, symbol, timeframe, forecast, amount,
                         result, profit, price_open, price_close, created_at, updated_at)
                    SELECT id, bot_name, 'BTC', timeframe, forecast, amount,
                           result, profit, {po}, {pc}, created_at, updated_at
                    FROM binary_options
                """))
                conn.execute(text("DROP TABLE binary_options"))
                conn.execute(text("ALTER TABLE _bo_new RENAME TO binary_options"))
                conn.execute(text("CREATE INDEX ix_binary_options_id       ON binary_options (id)"))
                conn.execute(text("CREATE INDEX ix_binary_options_bot_name ON binary_options (bot_name)"))
                conn.execute(text("CREATE INDEX ix_binary_options_symbol   ON binary_options (symbol)"))


async def _consume_bracket_exits() -> None:
    """
    XREADGROUP consumer that reads bracket exit events from Redis Stream
    and updates the corresponding BinaryOption rows in the DB.

    Uses consumer group 'api-workers' with consumer name 'api-{pid}'.
    Idempotent: only writes if bo.exit_trigger is None.
    """
    r = get_async_redis()
    group = "api-workers"
    consumer = f"api-{os.getpid()}"

    # Create consumer group (idempotent)
    try:
        await r.xgroup_create(STREAM_BRACKET_EXITS, group, id="0", mkstream=True)
        log.info("Created consumer group '%s' on stream '%s'", group, STREAM_BRACKET_EXITS)
    except Exception:
        pass  # group already exists

    log.info("Bracket exit consumer started: group=%s consumer=%s", group, consumer)

    while True:
        try:
            results = await r.xreadgroup(
                group, consumer,
                {STREAM_BRACKET_EXITS: ">"},
                count=10,
                block=5000,
            )
            if not results:
                continue

            for _stream, messages in results:
                for msg_id, data in messages:
                    try:
                        bo_id = int(data["bo_id"])
                        trigger = data["trigger"]
                        exit_price = float(data["exit_price"])
                        exit_filled = float(data["exit_filled"])

                        db = SessionLocal()
                        try:
                            bo = db.get(BinaryOption, bo_id)
                            if bo is not None and bo.exit_trigger is None:
                                bo.exit_trigger = trigger
                                bo.exit_price = exit_price
                                bo.exit_filled = exit_filled
                                db.commit()
                                log.info(
                                    "Bracket exit applied: BO #%d trigger=%s "
                                    "exit_price=%.6f exit_filled=%.4f",
                                    bo_id, trigger, exit_price, exit_filled,
                                )
                            elif bo is None:
                                log.warning("Bracket exit: BO #%d not found", bo_id)
                        finally:
                            db.close()

                        # ACK the message
                        await r.xack(STREAM_BRACKET_EXITS, group, msg_id)
                    except Exception as exc:
                        log.error("Error processing bracket exit %s: %s", msg_id, exc)

        except asyncio.CancelledError:
            log.info("Bracket exit consumer shutting down")
            return
        except Exception as exc:
            log.error("Bracket exit consumer error: %s", exc)
            await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    run_migrations()
    start_scheduler()

    # Start bracket exit consumer (reads from Redis Stream)
    consumer_task = asyncio.create_task(
        _consume_bracket_exits(), name="bracket-exit-consumer",
    )

    yield

    # ── Shutdown ────────────────────────────────────────────────────────────
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    await close_async_redis()
    stop_scheduler()


app = FastAPI(
    title="PolyArena BO API",
    description="Binary Options trading dashboard — order tracking, P&L, and bot analytics.",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(binary_options.router, prefix="/poly-arena/binary-options", tags=["Binary Options"])
app.include_router(bots.router,           prefix="/poly-arena/bots",           tags=["Bots"])
app.include_router(dashboard.router,      prefix="/poly-arena/dashboard",      tags=["Dashboard"])

_UI = Path(__file__).parent / "templates" / "dashboard.html"


@app.get("/poly-arena", include_in_schema=False)
@app.get("/poly-arena/", include_in_schema=False)
def ui():
    return FileResponse(_UI, media_type="text/html")


@app.get("/health", tags=["Health"])
def health():
    return {"status": "healthy"}
