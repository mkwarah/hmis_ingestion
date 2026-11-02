"""Base building blocks for HMIS ingestion.

Everything here is orchestration-agnostic (no Prefect imports), so it can be
reused from notebooks, tests or other flows:

* ``load_config``      - YAML config loader with ``${ENV_VAR:-default}`` expansion
* ``DHIS2Client``      - thin, retrying client for the DHIS2 Web API
* ``DataLake``         - MinIO / S3 object-store wrapper (JSON, state)
* ``run_dbt``          - run a dbt command in the lake or warehouse project
* ``send_email``       - plain SMTP email, used for pipeline alerts

Ingestion lands DHIS2 payloads *as received*; every transformation (typing,
de-duplication, hashing, joins) happens in dbt, not here.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import requests
import yaml
from minio import Minio
from minio.error import S3Error
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _expand_env(value: Any) -> Any:
    """Recursively replace ``${VAR}`` / ``${VAR:-default}`` with env values."""
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    if isinstance(value, str):

        def _sub(match: re.Match) -> str:
            name, default = match.group(1), match.group(2)
            resolved = os.environ.get(name, default)
            if resolved is None:
                raise KeyError(f"Environment variable '{name}' is not set and has no default")
            return resolved

        return _ENV_PATTERN.sub(_sub, value)
    return value


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def load_config(path: str | Path) -> dict:
    """Load the ingestion YAML config, expanding environment variables.

    A ``.env`` file next to the config is loaded first when python-dotenv is
    installed, so local secrets never need to live in the YAML.
    """
    path = Path(path)
    try:
        from dotenv import load_dotenv

        load_dotenv(path.parent / ".env")
    except ImportError:
        pass
    with path.open(encoding="utf-8") as fh:
        return _expand_env(yaml.safe_load(fh))


# --------------------------------------------------------------------------- #
# Source: DHIS2 Web API
# --------------------------------------------------------------------------- #
class DHIS2Client:
    """Minimal DHIS2 Web API client with connection pooling and HTTP retries."""

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: int = 180,
        max_retries: int = 5,
        page_size: int = 1000,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api = f"{self.base_url}/api"
        self.timeout = timeout
        self.page_size = page_size

        retry = Retry(
            total=max_retries,
            backoff_factor=2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            raise_on_status=False,
        )
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update({"Accept": "application/json"})
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))

    @classmethod
    def from_config(cls, source_cfg: dict) -> "DHIS2Client":
        return cls(
            base_url=source_cfg["base_url"],
            username=source_cfg["username"],
            password=source_cfg["password"],
            timeout=int(source_cfg.get("timeout_seconds", 180)),
            max_retries=int(source_cfg.get("max_retries", 5)),
            page_size=int(source_cfg.get("page_size", 1000)),
        )

    def get(self, endpoint: str, params: dict | None = None) -> dict:
        url = f"{self.api}/{endpoint.lstrip('/')}"
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def server_time(self) -> str:
        """Server clock, used as the incremental watermark (avoids clock skew)."""
        return self.get("system/info")["serverDate"]

    def iter_metadata(
        self, resource: str, fields: str, filters: list[str] | None = None
    ) -> Iterator[dict]:
        """Yield every object of a metadata resource, following the pager."""
        page = 1
        while True:
            params: dict[str, Any] = {"fields": fields, "page": page, "pageSize": self.page_size}
            if filters:
                params["filter"] = filters
            payload = self.get(f"{resource}.json", params)
            yield from payload.get(resource, [])
            if page >= payload.get("pager", {}).get("pageCount", 1):
                break
            page += 1

    def get_data_values(
        self,
        data_set: str,
        org_unit: str,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        last_updated: str | None = None,
        children: bool = True,
    ) -> dict:
        """Full ``/api/dataValueSets`` payload for a data set.

        The payload is returned untouched and landed as is. DHIS2 hoists
        fields shared by every value (e.g. ``attributeOptionCombo``) to the
        top level; dbt staging puts them back on each value.
        """
        params: dict[str, Any] = {
            "dataSet": data_set,
            "orgUnit": org_unit,
            "children": str(children).lower(),
        }
        if start_date:
            params["startDate"] = start_date.isoformat()
        if end_date:
            params["endDate"] = end_date.isoformat()
        if last_updated:
            params["lastUpdated"] = last_updated
        return self.get("dataValueSets.json", params)


# --------------------------------------------------------------------------- #
# Sink: MinIO / S3 data lake
# --------------------------------------------------------------------------- #
class DataLake:
    """Object-store wrapper. Works with MinIO and any S3-compatible endpoint."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool = False,
        region: str | None = None,
    ) -> None:
        self.client = Minio(
            endpoint, access_key=access_key, secret_key=secret_key, secure=secure, region=region
        )
        self.bucket = bucket

    @classmethod
    def from_config(cls, lake_cfg: dict) -> "DataLake":
        return cls(
            endpoint=lake_cfg["endpoint"],
            access_key=lake_cfg["access_key"],
            secret_key=lake_cfg["secret_key"],
            bucket=lake_cfg["bucket"],
            secure=as_bool(lake_cfg.get("secure", False)),
            region=lake_cfg.get("region") or None,
        )

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def ensure_bucket(self) -> None:
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
            log.info("Created bucket %s", self.bucket)

    # -- primitives --------------------------------------------------------- #
    def put_bytes(self, key: str, data: bytes, content_type: str) -> str:
        self.client.put_object(
            self.bucket, key, io.BytesIO(data), length=len(data), content_type=content_type
        )
        return key

    def get_bytes(self, key: str) -> bytes:
        response = self.client.get_object(self.bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def exists(self, key: str) -> bool:
        try:
            self.client.stat_object(self.bucket, key)
            return True
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject", "ResourceNotFound"}:
                return False
            raise

    def list_keys(self, prefix: str) -> list[str]:
        return [
            obj.object_name
            for obj in self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
        ]

    # -- formats ------------------------------------------------------------ #
    def put_json(self, key: str, obj: Any) -> str:
        data = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
        if key.endswith(".gz"):
            return self.put_bytes(key, gzip.compress(data), "application/gzip")
        return self.put_bytes(key, data, "application/json")

    def get_json(self, key: str) -> Any:
        data = self.get_bytes(key)
        if key.endswith(".gz"):
            data = gzip.decompress(data)
        return json.loads(data)

    # -- incremental state -------------------------------------------------- #
    def read_state(self, key: str) -> dict | None:
        return self.get_json(key) if self.exists(key) else None

    def write_state(self, key: str, state: dict) -> str:
        return self.put_json(key, state)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def date_windows(
    end: date, lookback_months: int, chunk_months: int = 1, overlap_days: int = 0
) -> list[tuple[date, date]]:
    """Calendar-aligned request windows covering ``lookback_months`` up to ``end``.

    DHIS2 only returns periods that fall *entirely* inside startDate/endDate,
    so a week spanning two months would be missed by plain monthly windows.
    ``overlap_days`` extends each window backwards to catch those periods;
    the resulting duplicates are removed in dbt staging. Use ``chunk_months``
    of 3/6/12 for quarterly, six-monthly or yearly data sets.
    """
    end_idx = end.year * 12 + end.month - 1
    idx = end_idx - lookback_months + 1
    idx -= idx % chunk_months  # align to Jan / Apr / Jul / Oct ...
    windows = []
    while idx <= end_idx:
        nxt = idx + chunk_months
        start = date(idx // 12, idx % 12 + 1, 1) - timedelta(days=overlap_days)
        stop = date(nxt // 12, nxt % 12 + 1, 1) - timedelta(days=1)
        windows.append((start, stop))
        idx = nxt
    return windows


# --------------------------------------------------------------------------- #
# Transform: dbt
# --------------------------------------------------------------------------- #
_ANSI = re.compile(r"\x1b\[[0-9;]*m")


class DbtError(RuntimeError):
    """dbt exited non-zero (a model errored or a test failed)."""

    def __init__(self, message: str, summary: dict):
        super().__init__(message)
        self.summary = summary


def run_dbt(
    transform_cfg: dict,
    project: str,
    args: list[str],
    on_line=None,
) -> dict:
    """Run a dbt command in one of the dbt projects and summarise the result.

    dbt runs as a separate program (``transform.dbt_executable``), usually
    from its own virtualenv, so its dependencies never clash with the
    pipeline's. The child inherits this process's environment (MINIO_*,
    CLICKHOUSE_*, loaded from .env by ``load_config``) plus
    ``transform.env``.

    Args:
        transform_cfg: the ``transform`` section of the config.
        project: key under ``transform.projects`` (``lake``, ``warehouse``).
        args: dbt arguments, e.g. ``["build"]``.
        on_line: called with each output line (for logging); optional.

    Returns:
        ``{"project", "command", "returncode", "counts": {status: n}, "elapsed"}``.
        Raises ``DbtError`` (carrying that summary) when dbt exits non-zero.
    """
    import subprocess

    project_dir = Path(transform_cfg["projects"][project]).expanduser().resolve()
    executable = str(Path(transform_cfg.get("dbt_executable", "dbt")).expanduser())
    env = {**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    env.update({k: str(Path(v).expanduser()) if str(v).startswith("~") else str(v)
                for k, v in (transform_cfg.get("env") or {}).items()})
    if "HMIS_DUCKDB_PATH" in env:
        Path(env["HMIS_DUCKDB_PATH"]).parent.mkdir(parents=True, exist_ok=True)

    cmd = [executable, *args, "--project-dir", str(project_dir),
           "--profiles-dir", str(project_dir), "--no-use-colors"]
    started = utc_now()
    proc = subprocess.Popen(
        cmd, cwd=project_dir, env=env, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        line = _ANSI.sub("", line.rstrip())
        if line and on_line:
            on_line(line)
    returncode = proc.wait()

    counts: dict[str, int] = {}
    results_path = project_dir / "target" / "run_results.json"
    if results_path.exists() and results_path.stat().st_mtime >= started.timestamp() - 1:
        for result in json.loads(results_path.read_text(encoding="utf-8")).get("results", []):
            status = str(result.get("status", "unknown")).lower()
            counts[status] = counts.get(status, 0) + 1

    summary = {
        "project": project,
        "command": " ".join(args),
        "returncode": returncode,
        "counts": counts,
        "elapsed": round((utc_now() - started).total_seconds(), 1),
    }
    if returncode != 0:
        raise DbtError(f"dbt {' '.join(args)} failed in '{project}' (exit {returncode}): {counts}", summary)
    return summary


# --------------------------------------------------------------------------- #
# Alerts: email
# --------------------------------------------------------------------------- #
def send_email(email_cfg: dict, subject: str, body: str) -> None:
    """Send a plain-text email with the ``alerts.email`` config section.

    Works with any SMTP server: Office 365 / Gmail (port 587, STARTTLS, user
    and app password) or the local Mailpit test inbox (port 1025, no auth).
    """
    import smtplib
    from email.message import EmailMessage

    recipients = [r.strip() for r in str(email_cfg["to"]).split(",") if r.strip()]
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = email_cfg["from"]
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    with smtplib.SMTP(email_cfg["smtp_host"], int(email_cfg["smtp_port"]), timeout=30) as smtp:
        if as_bool(email_cfg.get("starttls", False)):
            smtp.starttls()
        if email_cfg.get("username"):
            smtp.login(email_cfg["username"], email_cfg["password"])
        smtp.send_message(msg, to_addrs=recipients)
