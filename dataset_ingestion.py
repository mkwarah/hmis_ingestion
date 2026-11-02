"""Prefect flow: ingest DHIS2 (HMIS) aggregate data into the MinIO data lake.

Pipeline per run:
    1. Snapshot metadata (data elements, org units, category option combos,
       data sets)        -> raw JSON + bronze Parquet
    2. For each enabled data set:
         plan requests (full window or incremental since last watermark)
         -> extract data values in parallel  -> raw JSON (one object/request)
         -> normalise, de-duplicate, enrich  -> bronze Parquet per period
         -> advance the watermark
    3. Write a run manifest to ``_runs/``.

Run locally:      python dataset_ingestion.py
Run a backfill:   python dataset_ingestion.py --mode full --lookback-months 24
Deploy:           prefect deploy --all   (see prefect.yaml)

Only the config *path* is passed between flows and tasks, so credentials
never end up in Prefect's stored run parameters.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.task_runners import ThreadPoolTaskRunner

from hmis import (
    DATA_VALUE_KEY,
    DataLake,
    DHIS2Client,
    data_values_to_frame,
    date_windows,
    enrich_data_values,
    load_config,
    metadata_to_frame,
    org_unit_hierarchy,
    payload_to_records,
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
    """Snapshot one metadata resource to raw JSON and a bronze Parquet table."""
    logger = get_run_logger()
    cfg = load_config(config_path)
    entity = next(m for m in cfg["metadata"] if m["name"] == entity_name)
    lake_cfg = cfg["datalake"]
    client = DHIS2Client.from_config(cfg["source"])
    lake = DataLake.from_config(lake_cfg)

    records = list(client.iter_metadata(entity["resource"], entity["fields"], entity.get("filters")))
    raw_key = f"{lake_cfg['raw_prefix']}/metadata/{entity_name}/run_id={run_id}/{entity_name}.json.gz"
    lake.put_json(raw_key, records)

    df = metadata_to_frame(records)
    if entity_name == "organisation_units" and not df.empty:
        df = org_unit_hierarchy(df)
    df["ingested_at"] = utc_now()
    bronze_key = f"{lake_cfg['bronze_prefix']}/metadata/{entity_name}/{entity_name}.parquet"
    lake.put_parquet(bronze_key, df)

    logger.info("%s: %d records -> %s", entity_name, len(df), lake.uri(bronze_key))
    return {"entity": entity_name, "rows": len(df), "raw_key": raw_key, "bronze_key": bronze_key}


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
    config_path: str, dataset_key: str, request: dict, request_id: str, run_id: str
) -> dict:
    """Call /api/dataValueSets once and land the untouched payload in raw/."""
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
    raw_key = (
        f"{cfg['datalake']['raw_prefix']}/data_values/dataset={dataset_key}"
        f"/run_id={run_id}/{request_id}.json.gz"
    )
    lake.put_json(raw_key, {"request": request, **payload})
    rows = len(payload.get("dataValues", []))
    logger.info("%s [%s]: %d values", dataset_key, request_id, rows)
    return {"raw_key": raw_key, "rows": rows}


@task(retries=1, retry_delay_seconds=30, cache_policy=NO_CACHE, task_run_name="load-bronze-{dataset_key}")
def load_bronze(config_path: str, dataset_key: str, raw_keys: list[str], mode: str) -> dict:
    """Raw JSON -> typed, enriched Parquet partitioned by period.

    full:        each touched period partition is overwritten.
    incremental: changed values are upserted into existing partitions.
    """
    logger = get_run_logger()
    cfg = load_config(config_path)
    ds = _dataset_cfg(cfg, dataset_key)
    lake_cfg = cfg["datalake"]
    lake = DataLake.from_config(lake_cfg)

    records: list[dict] = []
    for key in raw_keys:
        records.extend(payload_to_records(lake.get_json(key)))
    df = data_values_to_frame(records)
    if df.empty:
        logger.info("%s: nothing new to load", dataset_key)
        return {"dataset": dataset_key, "rows": 0, "partitions": []}

    md_prefix = f"{lake_cfg['bronze_prefix']}/metadata"
    lookups = {
        name: lake.get_parquet(f"{md_prefix}/{name}/{name}.parquet")
        for name in ("data_elements", "category_option_combos", "organisation_units", "data_sets")
        if lake.exists(f"{md_prefix}/{name}/{name}.parquet")
    }
    data_sets = lookups.get("data_sets", pd.DataFrame(columns=["id", "name"]))
    ds_name = next(iter(data_sets.loc[data_sets["id"] == ds["id"], "name"]), None)
    df = enrich_data_values(df, lookups, ds["id"], ds_name)

    partitions = []
    for period, part in df.groupby("period", sort=True):
        key = f"{lake_cfg['bronze_prefix']}/data_values/dataset={dataset_key}/period={period}/data.parquet"
        if mode == "incremental" and lake.exists(key):
            # New rows come last so they win the de-duplication.
            part = pd.concat([lake.get_parquet(key), part], ignore_index=True)
            part = part.drop_duplicates(subset=DATA_VALUE_KEY, keep="last")
        lake.put_parquet(key, part.reset_index(drop=True))
        partitions.append({"period": period, "rows": len(part), "key": key})

    logger.info(
        "%s: %d values across %d period partitions -> %s",
        dataset_key,
        len(df),
        len(partitions),
        lake.uri(f"{lake_cfg['bronze_prefix']}/data_values/dataset={dataset_key}/"),
    )
    return {"dataset": dataset_key, "rows": len(df), "partitions": partitions}


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
    """Extract -> raw -> bronze -> watermark for a single DHIS2 data set."""
    today = date.fromisoformat(watermark[:10])
    effective_mode, requests_ = plan_requests(config_path, dataset_key, mode, lookback_months, today)

    futures = [
        extract_data_values.submit(
            config_path,
            dataset_key,
            req,
            req.get("end_date", "")[:7] or "incremental",  # window start may be in the prior month
            run_id,
        )
        for req in requests_
    ]
    extracted = [f.result() for f in futures]

    summary = load_bronze(config_path, dataset_key, [e["raw_key"] for e in extracted], effective_mode)
    update_watermark(config_path, dataset_key, watermark, run_id, effective_mode, summary["rows"])
    return {
        "dataset": dataset_key,
        "mode": effective_mode,
        "requests": len(requests_),
        "values_extracted": sum(e["rows"] for e in extracted),
        "rows_loaded": summary["rows"],
        "partitions": len(summary["partitions"]),
    }


@flow(name="hmis-dataset-ingestion", log_prints=True)
def hmis_ingestion(
    config_path: str = DEFAULT_CONFIG,
    mode: str | None = None,
    datasets: list[str] | None = None,
    lookback_months: int | None = None,
) -> dict:
    """Ingest HMIS (DHIS2) metadata and aggregate data values into MinIO.

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
            f"{r['dataset']:<22} {r['mode']:<12} extracted={r['values_extracted']:<8} "
            f"loaded={r['rows_loaded']:<8} partitions={r['partitions']}"
        )
    print(f"Manifest: {manifest_uri}")

    if failures:
        raise RuntimeError(f"{len(failures)} dataset(s) failed: {[f['dataset'] for f in failures]}")
    return manifest


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the HMIS ingestion flow locally")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--mode", choices=["full", "incremental"])
    parser.add_argument("--datasets", nargs="+", help="Dataset keys (default: all enabled)")
    parser.add_argument("--lookback-months", type=int)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    hmis_ingestion(
        config_path=args.config,
        mode=args.mode,
        datasets=args.datasets,
        lookback_months=args.lookback_months,
    )
