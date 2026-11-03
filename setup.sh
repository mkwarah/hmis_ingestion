#!/usr/bin/env bash
# One-command setup of the HMIS data platform on a laptop.
#
#   git clone https://github.com/mkwarah/hmis_ingestion.git
#   cd hmis_ingestion
#   ./setup.sh
#
# Runs on Ubuntu (on Windows: inside WSL) or any Linux/macOS with Docker.
# It is safe to run again: every step skips what is already done.
#
#   1. checks the prerequisites (Docker, Python 3.10+)
#   2. creates .env from .env.example, with a fresh hashing salt
#   3. creates two virtualenvs: Prefect (ingestion) and dbt
#   4. starts the services with Docker Compose and waits for them
#   5. registers the Prefect deployments (profile, work pool, schedules)
#   6. loads data once: DHIS2 -> landing -> dbt lake -> dbt warehouse -> docs
#   7. builds the Metabase dashboard
#
# Options:
#   --skip-data      stop after step 5 (no DHIS2 download, no dashboard)
#   --months N       months of history for the first load (default 12)
#   -h, --help       show this help
#
# Virtualenv folders can be changed with HMIS_VENV and HMIS_DBT_VENV.
set -euo pipefail

cd "$(dirname "$0")"
REPO_DIR="$(pwd)"
VENV="${HMIS_VENV:-$HOME/venvs/hmis-ingestion}"
DBT_VENV="${HMIS_DBT_VENV:-$HOME/venvs/hmis-dbt}"
LOAD_DATA=true
MONTHS=12

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-data) LOAD_DATA=false ;;
    --months) MONTHS="${2:?--months needs a number}"; shift ;;
    -h|--help) sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown option: $1 (try --help)" >&2; exit 2 ;;
  esac
  shift
done

bold=""; reset=""
if [[ -t 1 ]]; then bold=$(tput bold 2>/dev/null || true); reset=$(tput sgr0 2>/dev/null || true); fi
step() { echo; echo "${bold}==> $*${reset}"; }
ok()   { echo "    ok: $*"; }
die()  { echo; echo "ERROR: $*" >&2; exit 1; }

# Wait until a URL answers (any HTTP 2xx), up to $2 seconds.
wait_for() {
  local name="$1" url="$2" secs="${3:-180}" t=0
  printf '    waiting for %s ' "$name"
  until curl -fsS -o /dev/null "$url" 2>/dev/null; do
    (( t >= secs )) && { echo; die "$name did not start within ${secs}s (see: docker compose logs)"; }
    printf '.'; sleep 3; t=$((t + 3))
  done
  echo " up"
}

# Set KEY=VALUE in .env only when the key is missing or empty.
env_default() {
  local key="$1" value="$2"
  if grep -qE "^${key}=.+" .env; then return; fi
  if grep -qE "^${key}=" .env; then
    # sed with | as separator; values here never contain |
    sed -i.bak "s|^${key}=.*|${key}=${value}|" .env && rm -f .env.bak
  else
    echo "${key}=${value}" >> .env
  fi
  ok "set ${key} in .env"
}

# ---------------------------------------------------------------- 1. checks
step "1/7 Checking prerequisites"
if grep -qi microsoft /proc/version 2>/dev/null && [[ "$REPO_DIR" == /mnt/* ]]; then
  echo "    note: the repo is on the Windows drive ($REPO_DIR)."
  echo "          It works, but it is faster inside WSL, e.g. ~/hmis_ingestion."
fi
command -v docker >/dev/null || die "Docker is not installed. Install Docker Engine (or Docker Desktop with WSL integration)."
docker info >/dev/null 2>&1 || die "Docker is installed but not running, or your user cannot use it (try: sudo usermod -aG docker \$USER, then log out and in)."
docker compose version >/dev/null 2>&1 || die "The Docker Compose plugin is missing (package docker-compose-plugin)."
command -v curl >/dev/null || die "curl is missing (sudo apt install curl)."
PY="$(command -v python3 || true)"
[[ -n "$PY" ]] || die "python3 is missing (sudo apt install python3 python3-venv)."
"$PY" -c 'import sys; sys.exit(sys.version_info < (3, 10))' || die "Python 3.10 or newer is needed; found $("$PY" --version)."
"$PY" -c 'import venv, ensurepip' 2>/dev/null || die "python3-venv is missing (sudo apt install python3-venv)."
ok "docker $(docker version --format '{{.Server.Version}}'), $("$PY" --version)"

# ---------------------------------------------------------------- 2. .env
step "2/7 Preparing .env"
if [[ ! -f .env ]]; then
  cp .env.example .env
  ok "created .env from .env.example"
else
  ok ".env already exists; only empty settings are filled in"
fi
env_default HMIS_HASH_SALT "$("$PY" -c 'import secrets; print(secrets.token_hex(32))')"
env_default DBT_EXECUTABLE "$DBT_VENV/bin/dbt"
env_default HMIS_PROJECT_DIR "$REPO_DIR"
# The example ships DBT_EXECUTABLE=dbt (not empty); point it at the venv.
if grep -qE '^DBT_EXECUTABLE=dbt$' .env; then
  sed -i.bak "s|^DBT_EXECUTABLE=dbt$|DBT_EXECUTABLE=$DBT_VENV/bin/dbt|" .env && rm -f .env.bak
  ok "DBT_EXECUTABLE -> $DBT_VENV/bin/dbt"
fi
chmod 600 .env
mkdir -p "$HOME/hmis-data"          # DuckDB file lives here (HMIS_DUCKDB_PATH)

# ---------------------------------------------------------------- 3. venvs
step "3/7 Installing Python packages (first time takes a few minutes)"
[[ -x "$VENV/bin/python" ]] || "$PY" -m venv "$VENV"
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q -r requirements.txt
ok "ingestion venv: $VENV (prefect $("$VENV/bin/python" -c 'import prefect; print(prefect.__version__)'))"
[[ -x "$DBT_VENV/bin/python" ]] || "$PY" -m venv "$DBT_VENV"
"$DBT_VENV/bin/pip" install -q --upgrade pip
"$DBT_VENV/bin/pip" install -q -r requirements-dbt.txt
DBT_VER="$("$DBT_VENV/bin/dbt" --version 2>/dev/null | grep -m1 -oE 'installed: [0-9.]+' || true)"
ok "dbt venv: $DBT_VENV (dbt ${DBT_VER#installed: })"

# ---------------------------------------------------------------- 4. services
step "4/7 Starting services (Silo, ClickHouse, Prefect, Metabase, dbt docs, Mailpit)"
# dbt docs serves these folders; create them so Docker does not make them as root.
mkdir -p dbt/lake/target dbt/warehouse/target
docker compose up -d
wait_for "Silo"       "http://localhost:9100/minio/health/live"
wait_for "ClickHouse" "http://localhost:8124/ping"
wait_for "Prefect"    "http://localhost:4200/api/health"

# ---------------------------------------------------------------- 5. prefect
step "5/7 Registering Prefect deployments"
PREFECT="$VENV/bin/prefect"
"$PREFECT" profile create hmis >/dev/null 2>&1 || true
"$PREFECT" --profile hmis config set PREFECT_API_URL=http://localhost:4200/api >/dev/null
"$PREFECT" profile use hmis >/dev/null
if ! "$PREFECT" work-pool inspect hmis-process-pool >/dev/null 2>&1; then
  "$PREFECT" work-pool create hmis-process-pool --type process >/dev/null
  ok "created work pool hmis-process-pool"
fi
HMIS_PROJECT_DIR="$REPO_DIR" "$PREFECT" --no-prompt deploy --all >/dev/null
ok "deployments: hmis-daily-incremental, hmis-weekly-full-refresh, hmis-backfill"

# ---------------------------------------------------------------- 6-7. data
if $LOAD_DATA; then
  step "6/7 First load: ${MONTHS} months from the DHIS2 demo, then dbt (about 5-15 minutes)"
  PYTHONDONTWRITEBYTECODE=1 "$VENV/bin/python" dataset_ingestion.py --mode full --lookback-months "$MONTHS"
  ok "landing, staging, intermediate (Silo) and gold (ClickHouse) are built"

  step "7/7 Building the Metabase dashboard"
  "$VENV/bin/python" metabase/setup_metabase.py
else
  step "6-7/7 Skipped the first load (--skip-data)"
  echo "    Later: $VENV/bin/python dataset_ingestion.py --mode full --lookback-months $MONTHS"
  echo "           $VENV/bin/python metabase/setup_metabase.py"
fi

# ---------------------------------------------------------------- done
cat <<EOF

${bold}Setup complete.${reset}

  Silo console     http://localhost:9101      (minioadmin / minioadmin)
  Prefect UI       http://localhost:4200
  ClickHouse SQL   http://localhost:8124/play (hmis / hmis)
  Metabase         http://localhost:3010      (admin@hmis.local / see MB_ADMIN_PASSWORD in .env)
  dbt docs         http://localhost:8085
  Alert inbox      http://localhost:8025

Scheduled runs need a worker. Start it and keep the terminal open:

  ./start_worker.sh

These are local teaching passwords; change them in .env for anything shared.
EOF
