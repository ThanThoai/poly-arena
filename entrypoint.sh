#!/bin/bash
set -e

# Auto-apply Alembic migrations on startup (idempotent — safe to run repeatedly)
echo "Running Alembic migrations..."
python -c "from database import run_alembic_upgrade; run_alembic_upgrade()" 2>&1 || {
    echo "WARNING: Alembic migration failed (DB may not be ready yet), continuing..."
}

exec "$@"
