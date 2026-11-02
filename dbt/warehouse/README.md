# hmis_warehouse: the gold layer (ClickHouse)

dbt project that builds the gold marts in ClickHouse from the intermediate
Parquet that [`../lake`](../lake) writes to Silo. Silo is the data lake;
ClickHouse is the warehouse dashboards query.

```
Silo  s3://hmis-datalake/intermediate/dhis2/*.parquet
        │  s3(silo, ...)   named collection, no credentials in SQL
        ▼
ClickHouse
  lake.lake_dhis2__data_values           views: read the Parquet live
  lake.lake_dhis2__weekly_values_iso
  lake.lake_dhis2__org_unit_hierarchy
  lake.lake_dhis2__data_set_assignments
        │
        ▼
  gold.dim_org_unit                      MergeTree tables (the marts)
  gold.fct_monthly_indicators_district
  gold.fct_weekly_idsr_district
  gold.fct_reporting_monthly_district
  gold.fct_indicators_monthly_district
```

| Mart | Grain | Notes |
|---|---|---|
| `dim_org_unit` | org unit | country → district → chiefdom → facility |
| `fct_monthly_indicators_district` | data set × month × district × data element × disaggregation | monthly data sets; values with a mismatched period left out |
| `fct_weekly_idsr_district` | ISO week × district × data element × disaggregation | Wednesday weeks fill ISO weeks with no ISO data (see below) |
| `fct_reporting_monthly_district` | data set × month × district | assigned facilities that reported any value ÷ facilities assigned the form (DHIS2 data set assignments); every district appears, 0% when nothing was reported |
| `fct_indicators_monthly_district` | indicator × month × district | coverage (BCG, Penta3, Measles, ANC1), dropout (Penta1→3, ANC1→4), low birth weight %, stillbirth rate; long format with numerator, denominator and multiplier |

**Rates are re-aggregated, never averaged.** National BCG coverage is
`sum(numerator) / sum(denominator) * 100` over all districts, not the mean
of district percentages. That is why the indicator mart keeps both parts.
Coverage denominators are the yearly Population data set ÷ 12 per month.

## Run it (WSL)

Silo and ClickHouse must be running (`docker compose up -d` in the repo root)
and `../lake` must have been built.

```bash
source ~/venvs/hmis-dbt/bin/activate      # dbt-core + dbt-clickhouse
cd "<repo>/dbt/warehouse"
dbt debug
dbt build
```

Query the marts in the browser at http://localhost:8124/play (user `hmis`,
password `hmis`), e.g.

```sql
SELECT district_name, sum(value)
FROM gold.fct_monthly_indicators_district
WHERE data_element_name = 'Measles doses given' AND month = '2026-09-01'
GROUP BY 1 ORDER BY 2 DESC;
```

## ClickHouse details worth knowing

- **Named collection `silo`** (`clickhouse/config.d/silo.xml`) holds the
  bucket URL and keys. `macros/silo_parquet.sql` builds the `s3()` call.
  The `hmis` user may use it through `clickhouse/users.d/hmis.xml`.
- **`use_structure_from_insertion_table_in_table_functions: 0`** in
  `profiles.yml`. dbt fills tables with `INSERT INTO gold.x SELECT … FROM
  s3(…)`. By default ClickHouse then reads `s3()` with the *target* table's
  columns, so a column the mart renames (`period_start_date` → `month`)
  "doesn't exist". Turning the setting off makes it read the Parquet schema.
- **Nullable keys.** Parquet columns arrive as `Nullable(...)`; MergeTree
  `ORDER BY` keys cannot be nullable, so the marts wrap them in `ifNull()`.

## Tests

- Generic: unique/not-null keys, every district in the facts exists in
  `dim_org_unit`.
- **`assert_gold_totals_match_lake`**: per data set, the gold total equals
  the lake total. Aggregation neither lost nor invented data.
- **`assert_reporting_rate_within_bounds`**: a reporting rate can never
  exceed 100% or the number of facilities expected.
- Indicator codes must be known; an indicator without a denominator (e.g. a
  missing population figure) is a warning.

Gold receives no personal data: the lake's intermediate layer has already
removed it (tested there).

## IDSR weeks

The IDSR Weekly form is ISO-weekly, but the demo stores some weeks as
Wednesday weeks (`2026WedW45`) and five weeks in both forms. The lake model
`int_dhis2__weekly_values_iso` puts them on one calendar:

1. A Wednesday week counts in the ISO week holding 5 of its 7 days.
2. One report per week: if a week has any ISO-week data, only that is used;
   Wednesday weeks fill weeks with no ISO data.

Rule 2 is at week level on purpose. In the overlap weeks each form is a
complete report (~206 facilities, only 45 shared), so matching per facility
kept both and doubled those weeks. `assert_one_week_type_per_iso_week`
guards against that. Result: 28 weeks instead of 16; weeks with no data in
either form stay empty. `values_from_wednesday_weeks` in the mart shows
which weeks were filled from Wednesday reports.

On a real DHIS2 instance a data set has one period type and this does not
arise.
