"""Shared data access helpers for AccSafety applications.

This module centralises all direct database and API access so that the Dash
applications as well as the Flask gateway can reuse the same logic.  It exposes
helpers that return tidy :class:`pandas.DataFrame` objects following the shared
``site`` and ``counts`` schemas used by the forthcoming `/explore` portal.

The service currently knows how to talk to the primary PostgreSQL database
hosting the Eco-Counter and Hoan Bridge (``hr``) datasets as well as to the
Vivacity long-term sensors.  Placeholder loaders are also provided for future
crowdsourced / model datasets so that they can easily be normalised into the
same schema once their sources are connected.
"""

from __future__ import annotations

import io
import json
import os
import pathlib
import time
from datetime import datetime
from functools import lru_cache
from typing import Dict, List, Mapping, MutableMapping, Optional, Sequence

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# ---------------------------------------------------------------------------
# Database configuration helpers

DEFAULT_DB_URL = os.environ.get(
    "TRAFFIC_DB_URL",
    os.environ.get("DATABASE_URL", "postgresql://postgres:gw2ksoft@localhost/TrafficDB"),
)


@lru_cache(maxsize=1)
def get_engine(db_url: Optional[str] = None) -> Engine:
    """Return a cached SQLAlchemy engine.

    The configuration is shared by all callers so that connection pooling works
    across the entire application stack.  ``db_url`` can be used to override the
    default, mainly for tests.
    """

    return create_engine(db_url or DEFAULT_DB_URL)


# Mapping describing how each dataset should be normalised into the shared
# schema.  ``table`` is used for SQL queries, while the remaining fields are
# copied into the returned DataFrames to keep dataset metadata consistent.
DATASETS: Dict[str, Dict[str, str]] = {
    "eco": {
        "table": "eco_traffic_data",
        "mode": "active_transport",
        "facility": "temporary_counter",
        "type": "short_term",
        "bucket": "1h",
    },
    "trail": {
        "table": "hr_traffic_data",
        "mode": "active_transport",
        "facility": "trail_counter",
        "type": "short_term",
        "bucket": "1h",
    },
    "hr": {  # alias used in legacy code
        "table": "hr_traffic_data",
        "mode": "active_transport",
        "facility": "trail_counter",
        "type": "short_term",
        "bucket": "1h",
    },
}


SITE_SCHEMA = [
    "dataset",
    "site_id",
    "location_name",
    "name",
    "mode",
    "facility",
    "type",
    "start_date",
    "end_date",
    "total_counts",
    "average_hourly_count",
]

COUNT_SCHEMA = [
    "dataset",
    "site_id",
    "location_name",
    "timestamp",
    "date",
    "direction",
    "count",
    "mode",
    "facility",
    "type",
    "bucket",
]


def _dataset_key(dataset: str) -> str:
    key = (dataset or "").strip().lower()
    if key in DATASETS:
        return key
    raise ValueError(f"Unsupported dataset '{dataset}'. Known datasets: {sorted(DATASETS)}")


def _apply_dataset_metadata(df: pd.DataFrame, dataset: str) -> pd.DataFrame:
    cfg = DATASETS[_dataset_key(dataset)]
    df = df.copy()
    df["dataset"] = dataset
    df["mode"] = cfg["mode"]
    df["facility"] = cfg["facility"]
    df["type"] = cfg["type"]
    if "bucket" not in df.columns:
        df["bucket"] = cfg["bucket"]
    return df


# ---------------------------------------------------------------------------
# SQL backed loaders

def fetch_site_metadata(
    dataset: str,
    *,
    site_ids: Optional[Sequence[str]] = None,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    """Return aggregated metadata for the requested dataset.

    The returned frame follows ``SITE_SCHEMA`` while keeping backwards
    compatibility with the Dash apps through ``location_name``.
    """

    dataset_key = _dataset_key(dataset)
    cfg = DATASETS[dataset_key]
    engine = engine or get_engine()

    where_clauses: List[str] = []
    params: MutableMapping[str, object] = {}
    if site_ids:
        placeholders = ",".join(f":site_{i}" for i, _ in enumerate(site_ids))
        where_clauses.append(f"location_name IN ({placeholders})")
        params.update({f"site_{i}": site for i, site in enumerate(site_ids)})

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    query = f"""
        SELECT
            location_name AS site_id,
            MIN(date) AS start_date,
            MAX(date) AS end_date,
            SUM(count)::bigint AS total_counts,
            AVG(count)::numeric(12,2) AS average_hourly_count
        FROM {cfg['table']}
        {where_sql}
        GROUP BY location_name
        ORDER BY location_name
    """
    df = pd.read_sql(text(query), engine, params=params)
    if df.empty:
        return pd.DataFrame(columns=SITE_SCHEMA)

    df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
    df["end_date"] = pd.to_datetime(df["end_date"]).dt.date
    avg = pd.to_numeric(df["average_hourly_count"], errors="coerce")
    df["average_hourly_count"] = avg.round(0).fillna(0).astype(int)
    df["location_name"] = df["site_id"]
    df["name"] = df["site_id"]

    df = _apply_dataset_metadata(df, dataset_key)
    df = df.reindex(columns=SITE_SCHEMA)
    return df


def fetch_counts(
    dataset: str,
    *,
    site_ids: Optional[Sequence[str]] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    """Fetch time-series counts for the requested dataset.

    ``start`` and ``end`` are inclusive bounds.  The resulting DataFrame follows
    ``COUNT_SCHEMA`` and carries both ``date`` (legacy naming) and ``timestamp``
    columns to ease the migration of existing Dash apps.
    """

    dataset_key = _dataset_key(dataset)
    cfg = DATASETS[dataset_key]
    engine = engine or get_engine()

    clauses: List[str] = []
    params: MutableMapping[str, object] = {}
    if site_ids:
        placeholders = ",".join(f":site_{i}" for i, _ in enumerate(site_ids))
        clauses.append(f"location_name IN ({placeholders})")
        params.update({f"site_{i}": site for i, site in enumerate(site_ids)})
    if start is not None:
        clauses.append("date >= :start")
        params["start"] = pd.Timestamp(start)
    if end is not None:
        clauses.append("date <= :end")
        params["end"] = pd.Timestamp(end)

    where_sql = ""
    if clauses:
        where_sql = " WHERE " + " AND ".join(clauses)

    query = f"""
        SELECT
            location_name AS site_id,
            date,
            direction,
            count
        FROM {cfg['table']}
        {where_sql}
        ORDER BY location_name, date
    """

    df = pd.read_sql(text(query), engine, params=params)
    if df.empty:
        return pd.DataFrame(columns=COUNT_SCHEMA)

    df["timestamp"] = pd.to_datetime(df["date"], errors="coerce")
    df["location_name"] = df["site_id"]
    df = _apply_dataset_metadata(df, dataset_key)
    df = df.reindex(columns=COUNT_SCHEMA)
    return df


# ---------------------------------------------------------------------------
# Vivacity API helpers (refactored from ``vivacity_app``)

VIVACITY_API_BASE = os.environ.get("VIVACITY_API_BASE", "https://api.vivacitylabs.com")
VIVACITY_API_KEY = os.environ.get("VIVACITY_API_KEY", "")
VIVACITY_DEFAULT_CLASSES = ["pedestrian", "cyclist"]
VIVACITY_DEFAULT_BUCKET = "15m"
VIVACITY_TIMEOUT = 30
VIVACITY_MAX_RETRIES = 3

_vivacity_session = requests.Session()
_vivacity_session.headers.update({"User-Agent": "accsafety-gateway/1.0"})


def _vivacity_headers() -> Dict[str, str]:
    return {"x-vivacity-api-key": VIVACITY_API_KEY} if VIVACITY_API_KEY else {}


def _vivacity_get(path: str, params: Optional[Mapping[str, str]] = None) -> requests.Response:
    url = f"{VIVACITY_API_BASE}{path}"
    last_err: Optional[Exception] = None
    for attempt in range(1, VIVACITY_MAX_RETRIES + 1):
        try:
            resp = _vivacity_session.get(
                url,
                headers=_vivacity_headers(),
                params=params,
                timeout=VIVACITY_TIMEOUT,
            )
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else (1.5 ** attempt)
                time.sleep(wait)
                last_err = RuntimeError("Vivacity rate limited")
                continue
            if resp.status_code >= 500:
                last_err = RuntimeError(f"Vivacity server error {resp.status_code}")
                time.sleep(1.5 ** attempt)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:  # pragma: no cover - network
            last_err = exc
            time.sleep(1.5 ** attempt)
    raise RuntimeError(f"Vivacity request failed after {VIVACITY_MAX_RETRIES} attempts: {last_err}")


def _vivacity_normalise_site(cid: str, payload: Mapping[str, object]) -> Dict[str, object]:
    site = payload.get("site") if isinstance(payload.get("site"), Mapping) else {}
    site_name = site.get("name") if isinstance(site, Mapping) else None
    site_name = site_name or site.get("short_name") if isinstance(site, Mapping) else site_name
    label = payload.get("name") or payload.get("label") or cid
    if site_name and site_name not in label:
        label = f"{site_name} — {label}"
    return {
        "dataset": "vivacity",
        "site_id": str(cid),
        "location_name": str(cid),
        "name": label,
        "mode": "active_transport",
        "facility": "vivacity_sensor",
        "type": "long_term",
        "start_date": None,
        "end_date": None,
        "total_counts": None,
        "average_hourly_count": None,
    }


def fetch_vivacity_sites() -> pd.DataFrame:
    """Retrieve countline metadata from the Vivacity API."""

    try:
        data = _vivacity_get("/countline/metadata").json() if VIVACITY_API_KEY else {}
    except Exception:  # pragma: no cover - network
        data = {}

    rows: List[Dict[str, object]] = []
    if isinstance(data, Mapping):
        for cid, payload in data.items():
            if isinstance(payload, Mapping) and not payload.get("is_speed") and not payload.get("is_anpr"):
                rows.append(_vivacity_normalise_site(str(cid), payload))
    elif isinstance(data, list):  # pragma: no cover - depends on API version
        for payload in data:
            if isinstance(payload, Mapping):
                cid = payload.get("id") or payload.get("countline_id") or payload.get("uuid")
                if cid:
                    rows.append(_vivacity_normalise_site(str(cid), payload))

    df = pd.DataFrame(rows, columns=SITE_SCHEMA)
    if not df.empty:
        df = df.sort_values("name", kind="stable").reset_index(drop=True)
    return df


def fetch_vivacity_counts(
    countline_ids: Sequence[str],
    *,
    start: datetime,
    end: datetime,
    bucket: str = VIVACITY_DEFAULT_BUCKET,
    classes: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """Fetch counts for Vivacity countlines and normalise to ``COUNT_SCHEMA``."""

    if not countline_ids:
        return pd.DataFrame(columns=COUNT_SCHEMA)
    if start is None or end is None:
        raise ValueError("Vivacity requests require start and end datetimes")

    params: Dict[str, str] = {
        "countline_ids": ",".join(map(str, countline_ids)),
        "from": pd.Timestamp(start, tz="UTC").isoformat().replace("+00:00", "Z"),
        "to": pd.Timestamp(end, tz="UTC").isoformat().replace("+00:00", "Z"),
        "time_bucket": bucket,
        "fill_zeros": "true",
    }
    if classes:
        params["classes"] = ",".join(classes)

    try:
        payload = _vivacity_get("/countline/counts", params=params).json()
    except Exception:  # pragma: no cover - network
        payload = {}

    rows: List[Dict[str, object]] = []
    for cid, arr in (payload or {}).items():
        if not isinstance(arr, list):
            continue
        for rec in arr:
            timestamp = rec.get("from") or rec.get("to")
            for direction in ("clockwise", "anti_clockwise"):
                sub = rec.get(direction)
                if isinstance(sub, Mapping):
                    for cls, value in sub.items():
                        try:
                            count_val = float(value)
                        except Exception:
                            count_val = None
                        rows.append(
                            {
                                "dataset": "vivacity",
                                "site_id": str(cid),
                                "location_name": str(cid),
                                "timestamp": pd.to_datetime(timestamp, utc=True, errors="coerce"),
                                "date": pd.to_datetime(timestamp, utc=True, errors="coerce"),
                                "direction": direction,
                                "count": count_val,
                                "mode": "active_transport",
                                "facility": "vivacity_sensor",
                                "type": "long_term",
                                "bucket": bucket,
                            }
                        )

    df = pd.DataFrame(rows, columns=COUNT_SCHEMA)
    if not df.empty:
        df.sort_values(["site_id", "timestamp"], inplace=True)
        df.reset_index(drop=True, inplace=True)
    return df


# ---------------------------------------------------------------------------
# Placeholder loaders for crowdsourced / model datasets

def _ensure_dataframe(obj: object) -> pd.DataFrame:
    if isinstance(obj, pd.DataFrame):
        return obj
    if isinstance(obj, (list, tuple)):
        return pd.DataFrame(obj)
    if isinstance(obj, Mapping):
        return pd.DataFrame(obj if isinstance(obj, Mapping) else {})
    return pd.DataFrame()


def load_crowdsourced_sites(source: Optional[str] = None) -> pd.DataFrame:
    """Placeholder loader for crowdsourced site metadata.

    ``source`` can point to a CSV/JSON file or an HTTP endpoint.  The loader
    attempts to read the payload and reshape it into ``SITE_SCHEMA``.  When the
    source is missing this simply returns an empty DataFrame with the expected
    columns so callers can rely on the schema.
    """

    columns = SITE_SCHEMA
    if not source:
        return pd.DataFrame(columns=columns)

    path = pathlib.Path(source)
    data: Optional[pd.DataFrame] = None
    try:
        if path.exists():
            if path.suffix.lower() in {".csv", ".tsv"}:
                data = pd.read_csv(path)
            elif path.suffix.lower() in {".json", ".geojson"}:
                with path.open("r", encoding="utf-8") as fh:
                    payload = json.load(fh)
                data = _ensure_dataframe(payload)
        else:
            resp = requests.get(source, timeout=10)
            resp.raise_for_status()
            if "application/json" in resp.headers.get("Content-Type", ""):
                data = _ensure_dataframe(resp.json())
            else:
                data = pd.read_csv(io.StringIO(resp.text))
    except Exception:
        data = None

    if data is None or data.empty:
        return pd.DataFrame(columns=columns)

    records: List[Dict[str, object]] = []
    for row in data.to_dict("records"):
        site_id = row.get("site_id") or row.get("id") or row.get("location") or row.get("name")
        if not site_id:
            continue
        records.append(
            {
                "dataset": "crowdsourced",
                "site_id": str(site_id),
                "location_name": str(site_id),
                "name": row.get("name") or str(site_id),
                "mode": row.get("mode", "active_transport"),
                "facility": row.get("facility", "crowdsourced"),
                "type": row.get("type", "model"),
                "start_date": pd.to_datetime(row.get("start_date"), errors="coerce").date()
                if row.get("start_date")
                else None,
                "end_date": pd.to_datetime(row.get("end_date"), errors="coerce").date()
                if row.get("end_date")
                else None,
                "total_counts": row.get("total_counts"),
                "average_hourly_count": row.get("average_hourly_count"),
            }
        )

    return pd.DataFrame(records, columns=columns)


def load_crowdsourced_counts(source: Optional[str] = None) -> pd.DataFrame:
    """Placeholder loader that returns normalised count observations."""

    if not source:
        return pd.DataFrame(columns=COUNT_SCHEMA)

    path = pathlib.Path(source)
    data: Optional[pd.DataFrame] = None
    try:
        if path.exists():
            if path.suffix.lower() in {".csv", ".tsv"}:
                data = pd.read_csv(path)
            elif path.suffix.lower() in {".json", ".geojson"}:
                with path.open("r", encoding="utf-8") as fh:
                    payload = json.load(fh)
                data = _ensure_dataframe(payload)
        else:
            resp = requests.get(source, timeout=10)
            resp.raise_for_status()
            if "application/json" in resp.headers.get("Content-Type", ""):
                data = _ensure_dataframe(resp.json())
            else:
                data = pd.read_csv(io.StringIO(resp.text))
    except Exception:
        data = None

    if data is None or data.empty:
        return pd.DataFrame(columns=COUNT_SCHEMA)

    records: List[Dict[str, object]] = []
    for row in data.to_dict("records"):
        site_id = row.get("site_id") or row.get("id") or row.get("location") or row.get("name")
        timestamp = row.get("timestamp") or row.get("datetime") or row.get("date")
        records.append(
            {
                "dataset": "crowdsourced",
                "site_id": str(site_id) if site_id is not None else None,
                "location_name": str(site_id) if site_id is not None else None,
                "timestamp": pd.to_datetime(timestamp, errors="coerce"),
                "date": pd.to_datetime(timestamp, errors="coerce"),
                "direction": row.get("direction"),
                "count": row.get("count", row.get("value")),
                "mode": row.get("mode", "active_transport"),
                "facility": row.get("facility", "crowdsourced"),
                "type": row.get("type", "model"),
                "bucket": row.get("bucket", "1h"),
            }
        )

    return pd.DataFrame(records, columns=COUNT_SCHEMA)


# ---------------------------------------------------------------------------
# Public router helpers used by the Flask API

def get_sites(dataset: str, **kwargs) -> pd.DataFrame:
    dataset_key = (dataset or "").strip().lower()
    if dataset_key in DATASETS:
        return fetch_site_metadata(dataset_key, site_ids=kwargs.get("site_ids"))
    if dataset_key == "vivacity":
        return fetch_vivacity_sites()
    if dataset_key in {"crowdsourced", "model"}:
        return load_crowdsourced_sites(kwargs.get("source"))
    raise ValueError(f"Unsupported dataset '{dataset}'.")


def get_counts(dataset: str, **kwargs) -> pd.DataFrame:
    dataset_key = (dataset or "").strip().lower()
    if dataset_key in DATASETS:
        return fetch_counts(
            dataset_key,
            site_ids=kwargs.get("site_ids"),
            start=kwargs.get("start"),
            end=kwargs.get("end"),
        )
    if dataset_key == "vivacity":
        return fetch_vivacity_counts(
            kwargs.get("site_ids") or kwargs.get("countline_ids") or [],
            start=kwargs.get("start"),
            end=kwargs.get("end"),
            bucket=kwargs.get("bucket", VIVACITY_DEFAULT_BUCKET),
            classes=kwargs.get("classes", VIVACITY_DEFAULT_CLASSES),
        )
    if dataset_key in {"crowdsourced", "model"}:
        return load_crowdsourced_counts(kwargs.get("source"))
    raise ValueError(f"Unsupported dataset '{dataset}'.")


__all__ = [
    "COUNT_SCHEMA",
    "SITE_SCHEMA",
    "fetch_counts",
    "fetch_site_metadata",
    "fetch_vivacity_counts",
    "fetch_vivacity_sites",
    "get_counts",
    "get_engine",
    "get_sites",
    "load_crowdsourced_counts",
    "load_crowdsourced_sites",
]

