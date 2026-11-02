# hmis_lake: the data lake layers (Silo)

dbt project that turns the DHIS2 payloads landed by the Prefect pipeline into
the staging and intermediate layers, **written back to Silo as Parquet**.
DuckDB does the work; the local `.duckdb` file only keeps views pointing at
the files. The gold marts are built from here by [`../warehouse`](../warehouse)
in ClickHouse.

```
s3://hmis-datalake/
  landing/dhis2/...                          landing       DHIS2 payloads AS RECEIVED (Prefect)
  staging/dhis2/stg_dhis2__*.parquet         staging       first transformations, PII hashed
  intermediate/dhis2/int_dhis2__*.parquet    intermediate  joins, PII removed  ──▶ ClickHouse gold
```

| Layer | Folder | What happens there |
|---|---|---|
| landing | `models/landing/` | Source definitions only. Each source reads the gzipped JSON with an explicit **column contract** (`read_json(..., columns = {...})`), so a field DHIS2 drops arrives as NULL and the tests catch it. Folder names (`dataset=`, `run_id=`, `mode=`) become columns. dbt never writes here. |
| staging | `models/staging/dhis2/` | **First transformations.** Unnest payloads; restore fields DHIS2 hoists to the payload level; keep the current snapshot (latest full run per data set and period, plus later incremental changes); remove duplicates; cast types; parse DHIS2 periods into start/end dates; md5 surrogate keys; **hash `storedBy`** (salted SHA-256). Metadata: newest snapshot only. Data set assignments unnested. |
| intermediate | `models/intermediate/` | Joins: org unit hierarchy from `path`; data values with data element, disaggregation, data set and country → district → chiefdom → facility; weekly values on ISO weeks; data set assignments with hierarchy. **No personal data**: the hashed user and the comment text are dropped. |

Models use `materialized: external`. Where each file goes is decided by
`macros/external_location.sql`: `<bucket>/<layer>/dhis2/<model>.parquet`,
with the bucket from `external_root` in `profiles.yml`.

## Personal data

| Column | landing | staging | intermediate |
|---|---|---|---|
| `storedBy` (user who entered the value) | as received | `stored_by_hash` | removed |
| `comment` (free text) | as received | `comment` | removed (`has_comment` only) |

`macros/hash_pii.sql` computes `sha256(salt || lower(trim(value)))`. The salt
comes from `HMIS_HASH_SALT`; `plugins/hmis_pii.py` (a dbt-duckdb plugin
enabled in `profiles.yml`) stores it in a DuckDB session variable on every
connection, so it never appears in SQL, logs or the docs site. Without it
the build fails rather than producing empty hashes.

## Run it (WSL)

Silo must be running (`docker compose up -d` in the repo root) and landing
must hold data (run the pipeline once).

```bash
source ~/venvs/hmis-dbt/bin/activate
cd "<repo>"
set -a; . ./.env; set +a          # HMIS_HASH_SALT, MINIO_*, HMIS_DUCKDB_PATH
cd dbt/lake

dbt debug            # checks profile and connection to Silo
dbt build            # runs models and tests, writes Parquet to Silo
dbt docs generate --static   # one-file docs site (served on :8085)
```

## DHIS2 periods

`macros/dhis2_periods.sql` turns period IDs into `period_type`,
`period_start_date` and `period_end_date`: daily, weekly (ISO and
Wednesday/Thursday/Saturday/Sunday weeks), monthly, quarterly, six-monthly and
yearly. Weekly week 1 is the week containing 4 January, checked against the
DHIS2 API (`2026WedW1` = 2025-12-31 .. 2026-01-06).

## Tests

- **Generic** (in the `_*.yml` files): unique and not-null keys, accepted
  period types and load modes, and relationships from data values and
  assignments to every metadata table.
- **`assert_weekly_periods_start_on_their_weekday`**,
  **`assert_iso_weeks_start_on_monday`**, **`assert_one_week_type_per_iso_week`**:
  the period macro and the ISO-week harmonising, so no week is counted twice.
- **`assert_stored_by_is_hashed`**: every landed user name has a
  well-formed hash in staging.
- **`assert_no_pii_in_intermediate`**: no intermediate model has a
  personal-data column.
- **`warn_last_updated_before_created`** (warning): the demo's dates were
  shifted, so many values were "updated" before they were "created".
- **`warn_period_type_differs_from_data_set`** (warning): IDSR Weekly is an
  ISO-weekly form but also holds Wednesday weeks; Reproductive Health has a
  few daily values. `int_dhis2__data_values_enriched.is_expected_period_type`
  flags these rows. Monthly marts leave them out; weekly values are moved to
  ISO weeks by `int_dhis2__weekly_values_iso`.
