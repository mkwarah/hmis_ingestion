# HMIS data ingestion (DHIS2 → Silo/MinIO) with Prefect

```
DHIS2 ─Prefect─▶ Silo (data lake)                                   ClickHouse (warehouse)
                 raw/ → landing/ → staging/ → intermediate/ ─▶ gold marts
                 └─ Prefect ─┘             └─ dbt/lake (DuckDB) ─┘      └ dbt/warehouse
```

End-to-end pipeline that pulls aggregate HMIS data from the public DHIS2 demo
and lands it in a Silo data lake. Silo is the community fork of MinIO and
speaks the same S3 API, so the code uses the standard `minio` client.

| File | Purpose |
|---|---|
| `hmis.py` | Base layer: config loader, `DHIS2Client`, `DataLake` (MinIO), transforms |
| `dataset_ingestion.py` | Prefect flows and tasks (extract → raw → landing → watermark) |
| `ingestion_config.yml` | Source, data lake, metadata and dataset settings |
| `prefect.yaml` | Deployments: daily incremental, weekly full refresh, manual backfill |
| `docker-compose.yml` | Local Silo (lake) and ClickHouse (warehouse) for development |
| `.env.example` | Environment variables (copy to `.env`) |
| `dbt/lake/` | dbt + DuckDB: landing → staging → intermediate, written to Silo as Parquet |
| `dbt/warehouse/` | dbt + ClickHouse: gold marts read from the lake's intermediate layer |
| `clickhouse/` | ClickHouse config: `silo` named collection and user grant |

## Data lake layout (bucket `hmis-datalake`)

```
raw/dhis2/metadata/<entity>/run_id=<ts>/<entity>.json.gz
raw/dhis2/data_values/dataset=<key>/run_id=<ts>/<YYYY-MM|incremental>.json.gz
landing/dhis2/metadata/<entity>/<entity>.parquet                  # latest snapshot
landing/dhis2/data_values/dataset=<key>/period=<period>/data.parquet
_state/dhis2/data_values/<key>.json                              # incremental watermark
_runs/dhis2/<run_id>.json                                        # run manifest
```

Landing data values carry the natural key (`data_element_id, period,
org_unit_id, category_option_combo_id, attribute_option_combo_id`), the raw
`value` plus `value_numeric`, and the enrichment columns: data element name and
type, category option combo name, org unit name and level, the hierarchy
`ou_level_1..N`, data set name and `ingested_at`.

## How it loads

- **full**: requests one calendar month at a time over `lookback_months` and
  overwrites each period partition it touches. Each window starts
  `overlap_days` early, because DHIS2 only returns periods that fall entirely
  inside the requested dates. Without the overlap, weeks that cross a month
  boundary would be lost.
- **incremental**: one `lastUpdated=<watermark>` request per dataset, with
  the changed values upserted into existing partitions. The watermark is the
  DHIS2 server time taken at the *start* of the run. The first incremental
  run of a dataset falls back to a full load.
- A failing dataset doesn't stop the others. The run is still marked failed
  and its manifest records the error.

## Quick start (WSL)

Everything runs in WSL Ubuntu (Docker lives there). Two virtualenvs keep
Prefect and dbt dependencies apart.

```bash
cd "<repo>"
cp .env.example .env                       # then set DBT_EXECUTABLE (see below)

python3 -m venv ~/venvs/hmis-ingestion     # pipeline: Prefect, minio, pandas
~/venvs/hmis-ingestion/bin/pip install -r requirements.txt
python3 -m venv ~/venvs/hmis-dbt           # dbt + DuckDB + ClickHouse adapters
~/venvs/hmis-dbt/bin/pip install "dbt-core>=1.10" "dbt-duckdb>=1.9" "dbt-clickhouse>=1.9"
# .env: DBT_EXECUTABLE=/home/<you>/venvs/hmis-dbt/bin/dbt

docker compose up -d     # Silo :9100/:9101, ClickHouse :8124, Prefect UI :4200

source ~/venvs/hmis-ingestion/bin/activate
python dataset_ingestion.py                # ingest -> dbt lake -> dbt warehouse
python dataset_ingestion.py --skip-dbt     # ingest only
python dataset_ingestion.py --mode full --lookback-months 3 --datasets child_health
```

## Orchestration (Prefect)

The deployed flow is `hmis-pipeline` (`dataset_ingestion.py:hmis_pipeline`):

```
hmis-pipeline
├─ hmis-dataset-ingestion      DHIS2 -> raw/ + landing/        (subflow, one per data set)
├─ dbt-build-lake              landing -> staging -> intermediate   (Silo, DuckDB)
└─ dbt-build-warehouse         intermediate -> gold                  (ClickHouse)
```

Steps run in order and the run stops at the first failure: a failed data
set, a dbt model error or a failed dbt test. Gold is therefore never rebuilt
from bad data. dbt warnings (severity `warn`) are logged but do not stop it.
dbt runs as a separate program (`DBT_EXECUTABLE`), and its output appears in
the task logs.

| Deployment | Schedule (Africa/Kigali) | What it does |
|---|---|---|
| `hmis-daily-incremental` | daily 02:00 | values changed since last run, then dbt |
| `hmis-weekly-full-refresh` | Sunday 03:00 | reload last 12 months, then dbt |
| `hmis-backfill` | manual | full reload (default 24 months), then dbt |

One-time setup:

```bash
source ~/venvs/hmis-ingestion/bin/activate
prefect profile create hmis
prefect --profile hmis config set PREFECT_API_URL=http://localhost:4200/api
prefect profile use hmis
prefect work-pool create hmis-process-pool --type process
prefect deploy --all
```

Run the worker (keep the terminal open; it executes every run):

```bash
./start_worker.sh
```

Start a run by hand: Prefect UI http://localhost:4200 → Deployments →
Run, or `prefect deployment run "hmis-pipeline/hmis-daily-incremental"`.

**On a laptop:** WSL stops Ubuntu when no terminal is open, which stops
Docker and the worker. Scheduled runs only happen while Ubuntu is running;
missed ones show as *Late* in the UI. The containers restart by themselves
when Ubuntu starts; the worker needs `./start_worker.sh` again.

## Adding a dataset

Find its UID (`/api/dataSets.json?fields=id,name,periodType&paging=false`) and
add an entry under `datasets:` in `ingestion_config.yml`. For quarterly,
six-monthly or yearly data sets, set `chunk_months: 3`, `6` or `12` so each
request window holds whole periods.

## Notes

- The DHIS2 play server rotates versions. If the default URL returns 404,
  pick a current instance on https://play.dhis2.org and set `DHIS2_BASE_URL`.
- MinIO's community edition was archived in 2026. We use **Silo**
  (https://github.com/pgsty/silo), its maintained fork. It ships Windows,
  macOS and Linux binaries and `pgsty/silo` Docker images, and it keeps
  MinIO's ports, `MINIO_ROOT_*` variables and API. The `MINIO_*` settings
  here work with Silo, MinIO or any S3-compatible service.

  Run on Windows without Docker:
  ```powershell
  $env:MINIO_ROOT_USER="minioadmin"; $env:MINIO_ROOT_PASSWORD="minioadmin"
  .\silo.exe server C:\silo\data --console-address ":9001"
  ```
