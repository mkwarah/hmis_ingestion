"""Set up Metabase on the gold layer: admin account, ClickHouse connection,
and the "HMIS Overview" dashboard. Dashboard as code: safe to re-run; it
updates the questions and dashboard in place instead of duplicating them.

    ~/venvs/hmis-ingestion/bin/python metabase/setup_metabase.py

Reads the repo's .env. Settings (defaults in brackets):
    MB_URL                  Metabase URL                  [http://localhost:3010]
    MB_ADMIN_EMAIL          admin login                   [admin@hmis.local]
    MB_ADMIN_PASSWORD       admin password                [HmisAdmin2026!]
    CLICKHOUSE_BI_PASSWORD  read-only `metabase` user     [metabase]

Metabase reaches ClickHouse inside the compose network (clickhouse:8123)
as the read-only `metabase` user, which can only read the `gold` database.
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parent.parent
try:
    from dotenv import load_dotenv

    load_dotenv(REPO / ".env")
except ImportError:
    pass

MB_URL = os.environ.get("MB_URL", "http://localhost:3010").rstrip("/")
ADMIN_EMAIL = os.environ.get("MB_ADMIN_EMAIL", "admin@hmis.local")
ADMIN_PASSWORD = os.environ.get("MB_ADMIN_PASSWORD", "HmisAdmin2026!")
BI_PASSWORD = os.environ.get("CLICKHOUSE_BI_PASSWORD", "metabase")

DATABASE_NAME = "HMIS Gold (ClickHouse)"
COLLECTION_NAME = "HMIS"
DASHBOARD_NAME = "HMIS Overview"

DISTRICTS = [
    "Bo", "Bombali", "Bonthe", "Kailahun", "Kambia", "Kenema", "Koinadugu",
    "Kono", "Moyamba", "Port Loko", "Pujehun", "Tonkolili", "Western Area",
]

# Optional SQL clause filled by the dashboard's District filter. Metabase
# drops everything inside [[ ]] when the filter is empty.
DISTRICT = "[[AND district_name = {{district}}]]"

# Latest *complete* month a data set has (the current month is still being
# reported, so its totals would look like a sudden drop).
LAST_MONTH = """(SELECT max(month) FROM gold.fct_monthly_indicators_district
     WHERE data_set_name = '{ds}' AND month < toStartOfMonth(today()))"""


# --------------------------------------------------------------------------- #
# Questions (cards). Each: name, SQL, display, visualization settings, and
# its place on a 24-column grid (col, row, width, height).
# --------------------------------------------------------------------------- #
CARDS = [
    # ---- headline numbers ------------------------------------------------ #
    {
        "name": "Child Health reporting rate % (last complete month)",
        "display": "scalar",
        "sql": f"""
SELECT round(100 * sum(facilities_reporting) / sum(facilities_expected), 1) AS reporting_rate_pct
FROM gold.fct_reporting_monthly_district
WHERE data_set_name = 'Child Health'
  AND month = {LAST_MONTH.format(ds='Child Health')}
  {DISTRICT}""",
        "viz": {"column_settings": {'["name","reporting_rate_pct"]': {"suffix": " %"}}},
        "pos": (0, 3, 8, 3),
    },
    {
        "name": "ANC 1st visits (last complete month)",
        "display": "scalar",
        "sql": f"""
SELECT sum(value) AS anc1
FROM gold.fct_monthly_indicators_district
WHERE data_element_name = 'ANC 1st visit'
  AND month = {LAST_MONTH.format(ds='Reproductive Health')}
  {DISTRICT}""",
        "viz": {},
        "pos": (8, 3, 8, 3),
    },
    {
        "name": "Live births (last complete month)",
        "display": "scalar",
        "sql": f"""
SELECT sum(value) AS live_births
FROM gold.fct_monthly_indicators_district
WHERE data_element_name = 'Live births'
  AND month = {LAST_MONTH.format(ds='Reproductive Health')}
  {DISTRICT}""",
        "viz": {},
        "pos": (16, 3, 8, 3),
    },
    # ---- immunisation ---------------------------------------------------- #
    {
        "name": "Immunisation coverage % by month (annualised)",
        "display": "line",
        "sql": f"""
SELECT month, indicator_name AS indicator,
       round(sum(numerator) / sum(denominator) * 100, 1) AS coverage_pct
FROM gold.fct_indicators_monthly_district
WHERE indicator_code IN ('IMM_BCG_COV', 'IMM_PENTA3_COV', 'IMM_MEASLES_COV')
  AND month < toStartOfMonth(today())
  {DISTRICT}
GROUP BY month, indicator
ORDER BY month""",
        "viz": {"graph.dimensions": ["month", "indicator"], "graph.metrics": ["coverage_pct"]},
        "pos": (0, 6, 12, 7),
    },
    {
        "name": "Immunisation doses by month",
        "display": "line",
        "sql": f"""
SELECT month,
       replaceOne(data_element_name, ' doses given', '') AS vaccine,
       sum(value) AS doses
FROM gold.fct_monthly_indicators_district
WHERE data_element_name IN ('BCG doses given', 'Penta1 doses given',
                            'Penta3 doses given', 'Measles doses given')
  {DISTRICT}
GROUP BY month, vaccine
ORDER BY month""",
        "viz": {"graph.dimensions": ["month", "vaccine"], "graph.metrics": ["doses"]},
        "pos": (12, 6, 12, 7),
    },
    # ---- maternal health ------------------------------------------------- #
    {
        "name": "Antenatal care visits by month",
        "display": "line",
        "sql": f"""
SELECT month, data_element_name AS visit, sum(value) AS visits
FROM gold.fct_monthly_indicators_district
WHERE data_element_name IN ('ANC 1st visit', 'ANC 4th or more visits')
  AND month < toStartOfMonth(today())
  {DISTRICT}
GROUP BY month, visit
ORDER BY month""",
        "viz": {"graph.dimensions": ["month", "visit"], "graph.metrics": ["visits"]},
        "pos": (0, 13, 12, 7),
    },
    {
        "name": "ANC 1st visit coverage % by month (annualised)",
        "display": "line",
        "sql": f"""
SELECT month, round(sum(numerator) / sum(denominator) * 100, 1) AS anc1_coverage_pct
FROM gold.fct_indicators_monthly_district
WHERE indicator_code = 'MAT_ANC1_COV'
  AND month < toStartOfMonth(today())
  {DISTRICT}
GROUP BY month
ORDER BY month""",
        "viz": {"graph.dimensions": ["month"], "graph.metrics": ["anc1_coverage_pct"]},
        "pos": (12, 13, 12, 7),
    },
    # ---- surveillance ---------------------------------------------------- #
    {
        "name": "Weekly notifiable disease cases (IDSR)",
        "display": "line",
        "sql": f"""
SELECT week_start,
       replaceOne(data_element_name, 'IDSR ', '') AS disease,
       sum(value) AS cases
FROM gold.fct_weekly_idsr_district
WHERE 1 = 1
  {DISTRICT}
GROUP BY week_start, disease
ORDER BY week_start""",
        "viz": {"graph.dimensions": ["week_start", "disease"], "graph.metrics": ["cases"]},
        "pos": (0, 20, 24, 7),
    },
    # ---- district comparison --------------------------------------------- #
    {
        "name": "Penta1 to Penta3 dropout % by district (last 3 complete months)",
        "display": "row",
        "sql": f"""
SELECT district_name, round(sum(numerator) / sum(denominator) * 100, 1) AS dropout_pct
FROM gold.fct_indicators_monthly_district
WHERE indicator_code = 'IMM_PENTA_DROPOUT'
  AND month >= addMonths(toStartOfMonth(today()), -3)
  AND month <  toStartOfMonth(today())
  {DISTRICT}
GROUP BY district_name
ORDER BY dropout_pct DESC""",
        "viz": {"graph.dimensions": ["district_name"], "graph.metrics": ["dropout_pct"]},
        "pos": (0, 27, 8, 8),
    },
    {
        "name": "Measles doses by district (last complete month)",
        "display": "row",
        "sql": f"""
SELECT district_name, sum(value) AS doses
FROM gold.fct_monthly_indicators_district
WHERE data_element_name = 'Measles doses given'
  AND month = {LAST_MONTH.format(ds='Child Health')}
  {DISTRICT}
GROUP BY district_name
ORDER BY doses DESC""",
        "viz": {"graph.dimensions": ["district_name"], "graph.metrics": ["doses"]},
        "pos": (8, 27, 8, 8),
    },
    {
        "name": "Stillbirth rate per 1,000 births by month",
        "display": "line",
        "sql": f"""
SELECT month, round(sum(numerator) / sum(denominator) * 1000, 1) AS stillbirths_per_1000
FROM gold.fct_indicators_monthly_district
WHERE indicator_code = 'MAT_STILLBIRTH_RATE'
  AND month < toStartOfMonth(today())
  {DISTRICT}
GROUP BY month
ORDER BY month""",
        "viz": {"graph.dimensions": ["month"], "graph.metrics": ["stillbirths_per_1000"]},
        "pos": (16, 27, 8, 8),
    },
    # ---- reporting ------------------------------------------------------- #
    {
        "name": "Child Health reporting by district",
        "display": "table",
        "sql": f"""
SELECT district_name AS district,
       formatDateTime(month, '%Y-%m') AS month,
       facilities_reporting,
       facilities_expected,
       reporting_rate_pct
FROM gold.fct_reporting_monthly_district
WHERE data_set_name = 'Child Health'
  {DISTRICT}
ORDER BY month DESC, district""",
        "viz": {},
        "pos": (0, 35, 24, 8),
    },
]

HEADER = """## HMIS Overview: Sierra Leone DHIS2 demo

Source: **gold** marts in ClickHouse, built by `dbt/warehouse` from the Silo
data lake and refreshed by the Prefect pipeline (daily 02:00). Use the
**District** filter to focus on one district.
*Reporting rate = assigned facilities that reported ÷ facilities assigned the
form. Coverage = doses ÷ (yearly target population ÷ 12). Rates are computed
from summed numerators and denominators, never averaged.*"""


# --------------------------------------------------------------------------- #
# Metabase API helpers
# --------------------------------------------------------------------------- #
class Metabase:
    def __init__(self, url: str):
        self.url = url
        self.s = requests.Session()

    def call(self, method: str, path: str, **kw):
        r = self.s.request(method, f"{self.url}/api{path}", timeout=120, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:500]}")
        return r.json() if r.content else None

    def wait_until_up(self, seconds: int = 300) -> None:
        for _ in range(seconds // 3):
            try:
                if self.s.get(f"{self.url}/api/health", timeout=5).json().get("status") == "ok":
                    return
            except requests.RequestException:
                pass
            time.sleep(3)
        raise RuntimeError(f"Metabase not reachable at {self.url}")

    def login(self) -> None:
        props = self.call("GET", "/session/properties")
        if not props.get("has-user-setup"):
            print("First run: creating admin account")
            session = self.call("POST", "/setup", json={
                "token": props["setup-token"],
                "user": {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD,
                         "first_name": "HMIS", "last_name": "Admin",
                         "site_name": "HMIS Analytics"},
                "prefs": {"site_name": "HMIS Analytics", "allow_tracking": False},
            })
        else:
            session = self.call("POST", "/session",
                                json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
        self.s.headers["X-Metabase-Session"] = session["id"]

    def find(self, items: list[dict], name: str) -> dict | None:
        return next((i for i in items if i.get("name") == name), None)


def upsert_database(mb: Metabase) -> int:
    payload = {
        "engine": "clickhouse",
        "name": DATABASE_NAME,
        "details": {
            "host": "clickhouse",      # compose service name; Metabase runs in Docker
            "port": 8123,
            "dbname": "gold",
            "user": "metabase",
            "password": BI_PASSWORD,
            "ssl": False,
        },
    }
    dbs = mb.call("GET", "/database")
    dbs = dbs.get("data", dbs) if isinstance(dbs, dict) else dbs
    existing = mb.find(dbs, DATABASE_NAME)
    if existing:
        mb.call("PUT", f"/database/{existing['id']}", json=payload)
        db_id = existing["id"]
        print(f"Database '{DATABASE_NAME}' updated (id {db_id})")
    else:
        db_id = mb.call("POST", "/database", json=payload)["id"]
        print(f"Database '{DATABASE_NAME}' connected (id {db_id})")
    mb.call("POST", f"/database/{db_id}/sync_schema")
    return db_id


def upsert_collection(mb: Metabase) -> int:
    existing = mb.find(mb.call("GET", "/collection"), COLLECTION_NAME)
    if existing:
        return existing["id"]
    return mb.call("POST", "/collection", json={
        "name": COLLECTION_NAME,
        "description": "Dashboards and questions on the HMIS gold layer.",
    })["id"]


def upsert_card(mb: Metabase, db_id: int, collection_id: int, card: dict, existing: dict) -> int:
    tags = {}
    if "{{district}}" in card["sql"]:
        tags["district"] = {"id": str(uuid.uuid5(uuid.NAMESPACE_URL, card["name"])),
                            "name": "district", "display-name": "District", "type": "text"}
    payload = {
        "name": card["name"],
        "collection_id": collection_id,
        "display": card["display"],
        "visualization_settings": card["viz"],
        "dataset_query": {
            "type": "native",
            "database": db_id,
            "native": {"query": card["sql"].strip(), "template-tags": tags},
        },
    }
    if card["name"] in existing:
        card_id = existing[card["name"]]
        mb.call("PUT", f"/card/{card_id}", json=payload)
    else:
        card_id = mb.call("POST", "/card", json=payload)["id"]
    return card_id


def upsert_dashboard(mb: Metabase, collection_id: int, card_ids: dict[str, int]) -> int:
    items = mb.call("GET", f"/collection/{collection_id}/items", params={"models": "dashboard"})
    items = items.get("data", items) if isinstance(items, dict) else items
    existing = mb.find(items, DASHBOARD_NAME)
    if existing:
        dash_id = existing["id"]
    else:
        dash_id = mb.call("POST", "/dashboard", json={
            "name": DASHBOARD_NAME, "collection_id": collection_id,
            "description": "HMIS indicators from the ClickHouse gold layer.",
        })["id"]

    parameters = [{
        "id": "district", "name": "District", "slug": "district",
        "type": "string/=", "sectionId": "string",
        "values_query_type": "list", "values_source_type": "static-list",
        "values_source_config": {"values": DISTRICTS},
    }]
    dashcards = [{
        "id": -1, "card_id": None, "col": 0, "row": 0, "size_x": 24, "size_y": 3,
        "parameter_mappings": [],
        "visualization_settings": {
            "text": HEADER,
            "virtual_card": {"name": None, "display": "text", "visualization_settings": {},
                             "dataset_query": {}, "archived": False},
        },
    }]
    for i, card in enumerate(CARDS, start=2):
        col, row, w, h = card["pos"]
        cid = card_ids[card["name"]]
        dashcards.append({
            "id": -i, "card_id": cid, "col": col, "row": row, "size_x": w, "size_y": h,
            "visualization_settings": {},
            "parameter_mappings": [{
                "parameter_id": "district", "card_id": cid,
                "target": ["variable", ["template-tag", "district"]],
            }] if "{{district}}" in card["sql"] else [],
        })
    mb.call("PUT", f"/dashboard/{dash_id}", json={
        "parameters": parameters, "dashcards": dashcards,
    })
    return dash_id


def main() -> int:
    mb = Metabase(MB_URL)
    mb.wait_until_up()
    mb.login()
    db_id = upsert_database(mb)
    collection_id = upsert_collection(mb)

    items = mb.call("GET", f"/collection/{collection_id}/items", params={"models": "card"})
    items = items.get("data", items) if isinstance(items, dict) else items
    existing = {i["name"]: i["id"] for i in items}
    card_ids = {c["name"]: upsert_card(mb, db_id, collection_id, c, existing) for c in CARDS}
    print(f"{len(card_ids)} questions saved in collection '{COLLECTION_NAME}'")

    # Questions this script managed before but no longer defines: archive
    # them (Metabase keeps archived items in Trash, restorable).
    for name, cid in existing.items():
        if name not in card_ids:
            mb.call("PUT", f"/card/{cid}", json={"archived": True})
            print(f"  archived old question: {name}")

    # Run every question once so a broken query fails here, not in front of a class.
    failed = []
    for name, cid in card_ids.items():
        result = mb.call("POST", f"/card/{cid}/query")
        if result.get("status") != "completed" or result.get("error"):
            failed.append((name, result.get("error")))
        else:
            print(f"  ok  {result['row_count']:>4} rows  {name}")
    if failed:
        for name, err in failed:
            print(f"  FAIL {name}: {err}")
        return 1

    dash_id = upsert_dashboard(mb, collection_id, card_ids)
    print(f"Dashboard: {MB_URL}/dashboard/{dash_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
