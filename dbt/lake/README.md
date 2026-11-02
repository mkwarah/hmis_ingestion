# hmis_lake: the data lake layers (Silo)

dbt project that turns the landing Parquet written by the Prefect pipeline into
the staging and intermediate layers, **written back to Silo as Parquet**.
DuckDB does the work; the local `.duckdb` file only keeps views pointing at
the files. The gold marts are built from here by [`../warehouse`](../warehouse)
in ClickHouse.

```
s3://hmis-datalake/
  landing/dhis2/...                      landing  (Prefect writes; dbt sources)
  staging/dhis2/stg_dhis2__*.parquet    staging  (this project)
  intermediate/dhis2/int_dhis2__*.parquet   intermediate (this project) ──▶ ClickHouse gold
```

| Layer | Folder | What happens there |
|---|---|---|
| landing | `models/landing/` | Source definitions only. `external_location` points each source at Parquet in Silo. dbt never writes here. |
| staging | `models/staging/dhis2/` | One model per source: rename to `snake_case` IDs, cast types, trim names, parse DHIS2 periods into start/end dates. No joins. |
| intermediate | `models/intermediate/` | Joins: org unit hierarchy from `path`; data values with data element, disaggregation, data set and country → district → chiefdom → facility; weekly values harmonised to ISO weeks (`int_dhis2__weekly_values_iso`). |

Models use `materialized: external`. Where each file goes is decided by
`macros/external_location.sql`: `<bucket>/<layer>/dhis2/<model>.parquet`,
with the bucket from `external_root` in `profiles.yml`.

## Run it (WSL)

Silo must be running (`docker compose up -d` in the repo root).

```bash
python3 -m venv ~/venvs/hmis-dbt
~/venvs/hmis-dbt/bin/pip install "dbt-core>=1.10" "dbt-duckdb>=1.9" "dbt-clickhouse>=1.9"
source ~/venvs/hmis-dbt/bin/activate

cd "<repo>/dbt/lake"
mkdir -p ~/hmis-data
export HMIS_DUCKDB_PATH=~/hmis-data/hmis_lake.duckdb   # keep the DB off OneDrive

dbt debug            # checks profile and connection to Silo
dbt build            # runs models and tests, writes Parquet to Silo
dbt docs generate && dbt docs serve --port 8085   # lineage graph in the browser
```

`profiles.yml` reads the same `MINIO_*` variables as the pipeline (defaults:
`localhost:9100`, `minioadmin`). `HMIS_LAKE_BUCKET` changes the bucket.

## DHIS2 periods

`macros/dhis2_periods.sql` turns period IDs into `period_type`,
`period_start_date` and `period_end_date`: daily, weekly (ISO and
Wednesday/Thursday/Saturday/Sunday weeks), monthly, quarterly, six-monthly and
yearly. Weekly week 1 is the week containing 4 January, checked against the
DHIS2 API (`2026WedW1` = 2025-12-31 .. 2026-01-06).

## Tests

- **Generic** (in the `_*.yml` files): unique and not-null keys, accepted
  period types, and relationships from data values to every metadata table.
- **`assert_weekly_periods_start_on_their_weekday`**: checks the period macro.
- **`warn_last_updated_before_created`** (warning): the demo's dates were
  shifted, so many values were "updated" before they were "created".
- **`warn_period_type_differs_from_data_set`** (warning): IDSR Weekly is an
  ISO-weekly form but also holds Wednesday weeks; Reproductive Health has a
  few daily values. `int_dhis2__data_values_enriched.is_expected_period_type`
  flags these rows. Monthly marts leave them out; weekly values are moved to
  ISO weeks by `int_dhis2__weekly_values_iso`.
- **`assert_iso_weeks_start_on_monday`** and
  **`assert_one_week_type_per_iso_week`**: each harmonised week runs Monday
  to Sunday and comes from one report type, so no week is counted twice.
