"""Prefect flows: DHIS2 (HMIS) -> Silo data lake -> ClickHouse warehouse.

``hmis_pipeline`` (the deployed flow) runs three steps in order and stops at
the first failure, so gold is never rebuilt from bad data:

    1. ``hmis_ingestion``  DHIS2 -> landing/ in Silo, payloads AS RECEIVED
         a. snapshot metadata (data elements, org units, category option
            combos, data sets + assignments)  -> landing JSON, one per run
         b. for each enabled data set:
              plan requests (full window or incremental since last watermark)
              -> extract data values in parallel -> landing JSON, one per request
              -> advance the watermark
         c. write a run manifest to ``_runs/``
       No transformation happens here: typing, de-duplication, hashing and
       joins are all done by dbt.
    2. ``dbt build`` in dbt/lake       landing -> staging -> intermediate (Silo)
    3. ``dbt build`` in dbt/warehouse  intermediate -> gold marts (ClickHouse)
       (+ ``dbt docs generate`` for both, served on :8085)

A failed or crashed run emails the data team (``alerts`` in the config).

Run locally:      python dataset_ingestion.py                (all three steps)
Ingest only:      python dataset_ingestion.py --skip-dbt
Run a backfill:   python dataset_ingestion.py --mode full --lookback-months 24
Deploy:           prefect deploy --all   (see prefect.yaml)

Only the config *path* is passed between flows and tasks, so credentials
never end up in Prefect's stored run parameters.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.task_runners import ThreadPoolTaskRunner

from hmis import (
    DataLake,
    DbtError,
    DHIS2Client,
    as_bool,
    date_windows,
    load_config,
    run_dbt,
    send_email,
    utc_now,
)

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = "ingestion_config.yml"


def _resolve_config(config_path: str) -> str:
    path = Path(config_path)
    return str(path if path.is_absolute() else PROJECT_DIR / path)


def _dataset_cfg(cfg: dict, dataset_key: str) -> dict:
    for ds in cfg["datasets"]:
        if ds["key"] == dataset_key:
            return ds
    raise KeyError(f"Dataset '{dataset_key}' not found in config")


def _state_key(cfg: dict, dataset_key: str) -> str:
    return f"{cfg['datalake']['state_prefix']}/data_values/{dataset_key}.json"


# --------------------------------------------------------------------------- #
# Tasks
# --------------------------------------------------------------------------- #
@task(retries=2, retry_delay_seconds=15, cache_policy=NO_CACHE)
def prepare_lake(config_path: str) -> str:
    cfg = load_config(config_path)
    lake = DataLake.from_config(cfg["datalake"])
    lake.ensure_bucket()
    return lake.bucket


@task(retries=3, retry_delay_seconds=30, cache_policy=NO_CACHE)
def get_server_time(config_path: str) -> str:
    cfg = load_config(config_path)
    return DHIS2Client.from_config(cfg["source"]).server_time()


@task(retries=3, retry_delay_seconds=30, cache_policy=NO_CACHE, task_run_name="metadata-{entity_name}")
def ingest_metadata(config_path: str, entity_name: str, run_id: str) -> dict:
    """Land one metadata resource as received: a JSON array of DHIS2 objects."""
    logger = get_run_logger()
    cfg = load_config(config_path)
    entity = next(m for m in cfg["metadata"] if m["name"] == entity_name)
    lake_cfg = cfg["datalake"]
    client = DHIS2Client.from_config(cfg["source"])
    lake = DataLake.from_config(lake_cfg)

    records = list(client.iter_metadata(entity["resource"], entity["fields"], entity.get("filters")))
    key = f"{lake_cfg['landing_prefix']}/metadata/{entity_name}/run_id={run_id}/{entity_name}.json.gz"
    lake.put_json(key, records)

    logger.info("%s: %d records -> %s", entity_name, len(records), lake.uri(key))
    return {"entity": entity_name, "records": len(records), "key": key}


@task(cache_policy=NO_CACHE, task_run_name="plan-{dataset_key}")
def plan_requests(
    config_path: str, dataset_key: str, mode: str, lookback_months: int, today: date
) -> tuple[str, list[dict]]:
    """Decide which API requests to make. Returns (effective_mode, requests)."""
    logger = get_run_logger()
    cfg = load_config(config_path)
    ds = _dataset_cfg(cfg, dataset_key)
    ing = cfg["ingestion"]
    lake = DataLake.from_config(cfg["datalake"])

    if mode == "incremental":
        state = lake.read_state(_state_key(cfg, dataset_key))
        if state and state.get("watermark"):
            logger.info("%s: incremental since %s", dataset_key, state["watermark"])
            return "incremental", [{"last_updated": state["watermark"]}]
        logger.info("%s: no watermark yet, running a full load", dataset_key)

    windows = date_windows(
        today,
        lookback_months,
        chunk_months=int(ds.get("chunk_months", ing.get("chunk_months", 1))),
        overlap_days=int(ds.get("overlap_days", ing.get("overlap_days", 0))),
    )
    return "full", [{"start_date": s.isoformat(), "end_date": e.isoformat()} for s, e in windows]


@task(
    retries=3,
    retry_delay_seconds=[30, 90, 180],
    cache_policy=NO_CACHE,
    task_run_name="extract-{dataset_key}-{request_id}",
)
def extract_data_values(
    config_path: str, dataset_key: str, request: dict, request_id: str, run_id: str, mode: str
) -> dict:
    """Call /api/dataValueSets once and land the payload untouched in landing/.

    The object path carries what dbt needs to interpret it (hive-style
    partitions): data set key and UID, run, and load mode. ``mode=full``
    marks a complete snapshot of its window; staging uses that to drop values
    deleted in DHIS2 since an earlier run.
    """
    logger = get_run_logger()
    cfg = load_config(config_path)
    ds = _dataset_cfg(cfg, dataset_key)
    client = DHIS2Client.from_config(cfg["source"])
    lake = DataLake.from_config(cfg["datalake"])

    payload = client.get_data_values(
        ds["id"],
        ds["org_unit"],
        start_date=date.fromisoformat(request["start_date"]) if "start_date" in request else None,
        end_date=date.fromisoformat(request["end_date"]) if "end_date" in request else None,
        last_updated=request.get("last_updated"),
        children=bool(ds.get("children", True)),
    )
    key = (
        f"{cfg['datalake']['landing_prefix']}/data_values/dataset={dataset_key}"
        f"/data_set_id={ds['id']}/run_id={run_id}/mode={mode}/{request_id}.json.gz"
    )
    lake.put_json(key, payload)
    rows = len(payload.get("dataValues", []))
    logger.info("%s [%s]: %d values -> %s", dataset_key, request_id, rows, lake.uri(key))
    return {"key": key, "rows": rows, "request": request}


@task(cache_policy=NO_CACHE, task_run_name="watermark-{dataset_key}")
def update_watermark(config_path: str, dataset_key: str, watermark: str, run_id: str, mode: str, rows: int) -> None:
    cfg = load_config(config_path)
    lake = DataLake.from_config(cfg["datalake"])
    lake.write_state(
        _state_key(cfg, dataset_key),
        {
            "dataset": dataset_key,
            "watermark": watermark,
            "run_id": run_id,
            "mode": mode,
            "rows": rows,
            "updated_at": utc_now().isoformat(),
        },
    )


@task(cache_policy=NO_CACHE)
def write_manifest(config_path: str, run_id: str, manifest: dict) -> str:
    cfg = load_config(config_path)
    lake = DataLake.from_config(cfg["datalake"])
    return lake.uri(lake.put_json(f"{cfg['datalake']['runs_prefix']}/{run_id}.json", manifest))


@task(retries=1, retry_delay_seconds=30, cache_policy=NO_CACHE, task_run_name="dbt-build-{project}")
def dbt_build(config_path: str, project: str) -> dict:
    """Run ``dbt build`` (models + tests, in dependency order) in one dbt project.

    dbt's output goes to the Prefect log line by line. Fails the task when a
    model errors or a test fails; warnings (severity: warn) do not fail it.
    The one retry covers a lake or warehouse that was briefly unreachable.
    """
    logger = get_run_logger()
    transform = _transform_cfg(config_path)
    try:
        # Skip dbt's "START" lines; the OK/PASS/WARN/ERROR line follows each one.
        summary = run_dbt(
            transform, project, ["build"],
            on_line=lambda line: None if " START " in line else logger.info(line),
        )
    except DbtError as exc:
        logger.error("dbt build failed in %s: %s", project, exc.summary["counts"])
        raise
    logger.info("dbt build %s: %s in %ss", project, summary["counts"], summary["elapsed"])
    return summary


def _transform_cfg(config_path: str) -> dict:
    cfg = load_config(config_path)
    transform = dict(cfg["transform"])
    base = Path(config_path).parent
    transform["projects"] = {
        name: str(path if Path(path).is_absolute() else base / path)
        for name, path in transform["projects"].items()
    }
    return transform


@task(cache_policy=NO_CACHE, task_run_name="dbt-docs-{project}")
def dbt_docs(config_path: str, project: str) -> str | None:
    """Regenerate the dbt docs site (one self-contained HTML file).

    Served by the `dbt-docs` container on http://localhost:8085. Docs are a
    convenience, so a failure here is logged as a warning and does not fail
    the pipeline.
    """
    logger = get_run_logger()
    transform = _transform_cfg(config_path)
    try:
        run_dbt(transform, project, ["docs", "generate", "--static"])
    except DbtError as exc:
        logger.warning("dbt docs generate failed in %s: %s", project, exc)
        return None
    path = Path(transform["projects"][project]) / "target" / "static_index.html"
    logger.info("dbt docs for %s -> %s", project, path)
    return str(path)


# --------------------------------------------------------------------------- #
# Alerts
# --------------------------------------------------------------------------- #
def notify_failure(flow, flow_run, state) -> None:
    """Flow hook: email the data team when a pipeline run fails or crashes."""
    config_path = _resolve_config(flow_run.parameters.get("config_path", DEFAULT_CONFIG))
    cfg = load_config(config_path)
    alerts = cfg.get("alerts", {})
    if not as_bool(alerts.get("enabled", False)):
        return
    url = f"{str(alerts.get('prefect_ui', '')).rstrip('/')}/runs/flow-run/{flow_run.id}"
    params = {k: v for k, v in flow_run.parameters.items() if k != "config_path"}
    subject = f"[HMIS pipeline] {state.type.value}: {flow.name} / {flow_run.name}"
    body = "\n".join([
        f"The HMIS pipeline run '{flow_run.name}' ended in state {state.name}.",
        "",
        f"Reason:      {state.message or 'no message'}",
        f"Parameters:  {params or 'defaults'}",
        f"Started:     {flow_run.start_time}",
        f"Run in UI:   {url}",
        "",
        "Gold marts in ClickHouse were NOT rebuilt by this run; dashboards show",
        "the data from the last successful run.",
    ])
    try:
        send_email(alerts["email"], subject, body)
        print(f"Alert email sent to {alerts['email']['to']}")
    except Exception as exc:  # an alert failure must not hide the real failure
        print(f"Could not send alert email: {exc}")


# --------------------------------------------------------------------------- #
# Flows
# --------------------------------------------------------------------------- #
@flow(
    name="hmis-dataset",
    flow_run_name="{dataset_key}",
    task_runner=ThreadPoolTaskRunner(max_workers=4),
)
def ingest_dataset(
    config_path: str,
    dataset_key: str,
    mode: str,
    lookback_months: int,
    run_id: str,
    watermark: str,
) -> dict:
    """Extract -> landing (as received) -> watermark for one DHIS2 data set."""
    today = date.fromisoformat(watermark[:10])
    effective_mode, requests_ = plan_requests(config_path, dataset_key, mode, lookback_months, today)

    futures = [
        extract_data_values.submit(
            config_path,
            dataset_key,
            req,
            req.get("end_date", "")[:7] or "incremental",  # window start may be in the prior month
            run_id,
            effective_mode,
        )
        for req in requests_
    ]
    extracted = [f.result() for f in futures]

    values = sum(e["rows"] for e in extracted)
    update_watermark(config_path, dataset_key, watermark, run_id, effective_mode, values)
    return {
        "dataset": dataset_key,
        "mode": effective_mode,
        "requests": len(requests_),
        "values_landed": values,
        "objects": [e["key"] for e in extracted],
    }


@flow(name="hmis-dataset-ingestion", log_prints=True)
def hmis_ingestion(
    config_path: str = DEFAULT_CONFIG,
    mode: str | None = None,
    datasets: list[str] | None = None,
    lookback_months: int | None = None,
) -> dict:
    """Land HMIS (DHIS2) metadata and aggregate data values in Silo, as received.

    Args:
        config_path: Path to ingestion_config.yml (relative to this file).
        mode: "full" or "incremental"; defaults to the config value.
        datasets: Dataset keys to run; defaults to all enabled datasets.
        lookback_months: Override the full-load window from the config.
    """
    logger = get_run_logger()
    config_path = _resolve_config(config_path)
    cfg = load_config(config_path)
    mode = mode or cfg["ingestion"].get("mode", "incremental")
    if mode not in {"full", "incremental"}:
        raise ValueError(f"mode must be 'full' or 'incremental', got '{mode}'")
    lookback_months = int(lookback_months or cfg["ingestion"].get("lookback_months", 12))

    started = utc_now()
    run_id = started.strftime("%Y%m%dT%H%M%SZ")
    bucket = prepare_lake(config_path)
    # Captured before extraction so changes made during the run are re-read next time.
    watermark = get_server_time(config_path)
    logger.info("Run %s | mode=%s | bucket=%s | watermark=%s", run_id, mode, bucket, watermark)

    metadata = [
        f.result()
        for f in [ingest_metadata.submit(config_path, m["name"], run_id) for m in cfg["metadata"]]
    ]

    selected = [
        ds["key"]
        for ds in cfg["datasets"]
        if (ds["key"] in datasets if datasets else ds.get("enabled", True))
    ]
    results, failures = [], []
    for key in selected:
        state = ingest_dataset(
            config_path, key, mode, lookback_months, run_id, watermark, return_state=True
        )
        if state.is_completed():
            results.append(state.result())
        else:
            logger.error("Dataset %s failed: %s", key, state.message)
            failures.append({"dataset": key, "error": state.message})

    manifest = {
        "run_id": run_id,
        "source": cfg["source"]["name"],
        "base_url": cfg["source"]["base_url"],
        "mode": mode,
        "lookback_months": lookback_months,
        "watermark": watermark,
        "started_at": started.isoformat(),
        "finished_at": utc_now().isoformat(),
        "metadata": metadata,
        "datasets": results,
        "failures": failures,
    }
    manifest_uri = write_manifest(config_path, run_id, manifest)
    for r in results:
        print(
            f"{r['dataset']:<22} {r['mode']:<12} requests={r['requests']:<3} "
            f"values landed={r['values_landed']}"
        )
    print(f"Manifest: {manifest_uri}")

    if failures:
        raise RuntimeError(f"{len(failures)} dataset(s) failed: {[f['dataset'] for f in failures]}")
    return manifest


@flow(
    name="hmis-pipeline",
    log_prints=True,
    on_failure=[notify_failure],
    on_crashed=[notify_failure],
)
def hmis_pipeline(
    config_path: str = DEFAULT_CONFIG,
    mode: str | None = None,
    datasets: list[str] | None = None,
    lookback_months: int | None = None,
    with_dbt: bool = True,
) -> dict:
    """End to end: DHIS2 -> Silo (lake) -> ClickHouse (warehouse).

    Steps run in order and the flow stops at the first failure:
      1. ingestion       hmis_ingestion subflow (raises if any data set failed)
      2. dbt lake        landing -> staging -> intermediate, Parquet in Silo
      3. dbt warehouse   intermediate -> gold marts in ClickHouse
      then the dbt docs site is regenerated (never fails the run).
    A failed or crashed run sends an alert email (notify_failure).

    Args:
        config_path, mode, datasets, lookback_months: passed to ingestion.
        with_dbt: False to ingest only (also skipped when transform.enabled
            is false in the config).
    """
    config_path = _resolve_config(config_path)
    cfg = load_config(config_path)

    manifest = hmis_ingestion(config_path, mode, datasets, lookback_months)
    result = {"run_id": manifest["run_id"], "ingestion": manifest["datasets"], "dbt": []}

    if not (with_dbt and cfg.get("transform", {}).get("enabled", True)):
        print("dbt skipped")
        return result

    for project in ("lake", "warehouse"):
        summary = dbt_build(config_path, project)
        result["dbt"].append(summary)
        print(f"dbt {project:<10} {summary['counts']}  ({summary['elapsed']}s)")

    if cfg["transform"].get("docs", True):
        result["docs"] = [dbt_docs(config_path, project) for project in ("lake", "warehouse")]
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the HMIS pipeline locally")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--mode", choices=["full", "incremental"])
    parser.add_argument("--datasets", nargs="+", help="Dataset keys (default: all enabled)")
    parser.add_argument("--lookback-months", type=int)
    parser.add_argument("--skip-dbt", action="store_true", help="Ingest only; do not run dbt")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    hmis_pipeline(
        config_path=args.config,
        mode=args.mode,
        datasets=args.datasets,
        lookback_months=args.lookback_months,
        with_dbt=not args.skip_dbt,
    )
