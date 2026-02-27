#!/usr/bin/env python
"""Standalone DB migration script. Run before starting any service."""
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)

from database import init_timescaledb, run_alembic_upgrade


def main():
    init_timescaledb()
    run_alembic_upgrade()


if __name__ == "__main__":
    main()
