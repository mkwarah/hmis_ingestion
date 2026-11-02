#!/usr/bin/env bash
# Start the Prefect worker that executes the HMIS pipeline runs
# (scheduled ones and those started from the UI). Run it in WSL and leave
# the terminal open: no worker, no runs.
#
#   ./start_worker.sh
#
# Needs: `docker compose up -d` (Silo, ClickHouse, Prefect server), the
# ingestion venv (HMIS_VENV, default ~/venvs/hmis-ingestion) and the
# Prefect profile `hmis` pointing at http://localhost:4200/api
# (see prefect.yaml).
set -euo pipefail

cd "$(dirname "$0")"
export HMIS_PROJECT_DIR="$(pwd)"        # used by the pull step in prefect.yaml
export PYTHONDONTWRITEBYTECODE=1        # keep __pycache__ out of OneDrive
VENV="${HMIS_VENV:-$HOME/venvs/hmis-ingestion}"

exec "$VENV/bin/prefect" --profile hmis worker start --pool hmis-process-pool --limit 1
