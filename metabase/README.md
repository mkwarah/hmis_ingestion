# Metabase: dashboards on the gold layer

Metabase (http://localhost:3010) reads the **gold** marts in ClickHouse. It
never touches the lake or the `lake` views.

```
ClickHouse gold.*  ──(read-only user `metabase`)──▶  Metabase  ──▶  "HMIS Overview"
```

## Set up

Metabase starts with the rest of the stack (`docker compose up -d`). The
first time, run the setup script from the repo root (in WSL):

```bash
~/venvs/hmis-ingestion/bin/python metabase/setup_metabase.py
```

It is safe to re-run. It:

1. creates the admin account (first run only),
2. connects the database **HMIS Gold (ClickHouse)**: host `clickhouse`,
   port `8123`, database `gold`, user `metabase`,
3. saves 12 questions (native SQL) in the **HMIS** collection and runs each
   once, failing loudly if any query breaks,
4. builds the **HMIS Overview** dashboard with a **District** filter wired
   to every question.

Log in with `MB_ADMIN_EMAIL` / `MB_ADMIN_PASSWORD` from `.env`
(teaching defaults: `admin@hmis.local` / `HmisAdmin2026!`; change them for
anything beyond a laptop).

## The dashboard

| Question | Source mart |
|---|---|
| Child Health reporting rate % (last complete month) | `fct_reporting_monthly_district` |
| ANC 1st visits / Live births (last complete month) | `fct_monthly_indicators_district` |
| Immunisation coverage % by month (BCG, Penta3, Measles) | `fct_indicators_monthly_district` |
| Immunisation doses by month (BCG, Penta1, Penta3, Measles) | `fct_monthly_indicators_district` |
| Antenatal care visits by month (ANC 1st, ANC 4th+) | `fct_monthly_indicators_district` |
| ANC 1st visit coverage % by month | `fct_indicators_monthly_district` |
| Weekly notifiable disease cases (IDSR) | `fct_weekly_idsr_district` |
| Penta1 to Penta3 dropout % by district (last 3 months) | `fct_indicators_monthly_district` |
| Measles doses by district | `fct_monthly_indicators_district` |
| Stillbirth rate per 1,000 births by month | `fct_indicators_monthly_district` |
| Child Health reporting by district (table) | `fct_reporting_monthly_district` |

Rates are computed in each question as `sum(numerator) / sum(denominator)`,
so they stay correct with or without the District filter.

"Last complete month" skips the current month, which is still being
reported and would otherwise look like a sudden drop.

The District filter works through an optional SQL clause in each question:

```sql
WHERE ... [[AND district_name = {{district}}]]
```

Metabase removes the `[[ ]]` part when no district is chosen.

## Security

The `metabase` ClickHouse user (`clickhouse/users.d/metabase.xml`) can only
read the `gold` database: writes, DDL, the `lake` views and the Silo
connection are all refused. A dashboard tool should never be able to change
the warehouse.

## Changing the dashboard

Edit `CARDS` in `setup_metabase.py` and re-run it: questions are matched by
name and updated in place, and questions the script no longer defines are
archived (restorable from Metabase's Trash). Changes made by clicking in Metabase are kept
until the script is run again, which overwrites questions it manages (and
the dashboard layout). For one-off exploration, use **+ New → Question** or
**SQL query** in Metabase instead.
