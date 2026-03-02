from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from unified_explore import ENGINE

UNIFIED_SEARCH_SQL = """
    SELECT
        "Location",
        "Longitude",
        "Latitude",
        "Total counts",
        "Source",
        "Facility type",
        "Mode"
    FROM unified_site_summary
    WHERE "Location" ILIKE %(pattern)s
"""

UNIFIED_NEARBY_SQL = """
    SELECT
        "Location",
        "Longitude",
        "Latitude",
        "Total counts",
        "Source",
        "Facility type",
        "Mode"
    FROM unified_site_summary
    WHERE "Longitude" IS NOT NULL
      AND "Latitude" IS NOT NULL
"""


@dataclass
class RetrievalResult:
    evidence: list[dict[str, Any]]
    citations: list[dict[str, Any]]
    stats: dict[str, list[dict[str, Any]]]


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _coerce_float(value: Any) -> float | None:
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        return float(value)
    except Exception:
        return None


def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 3958.8
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return radius * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


def _aggregate_locations(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []

    grouped: list[dict[str, Any]] = []
    working = df.copy()
    working["Location"] = working["Location"].fillna("").astype(str).str.strip()

    for location, group in working.groupby("Location"):
        if not location:
            continue
        lon = next((_coerce_float(v) for v in group["Longitude"].tolist() if _coerce_float(v) is not None), None)
        lat = next((_coerce_float(v) for v in group["Latitude"].tolist() if _coerce_float(v) is not None), None)

        datasets = []
        for _, row in group.iterrows():
            datasets.append(
                {
                    "Source": _normalize_text(row.get("Source")),
                    "Facility type": _normalize_text(row.get("Facility type")),
                    "Mode": _normalize_text(row.get("Mode")),
                    "Total counts": _coerce_float(row.get("Total counts")),
                }
            )

        grouped.append(
            {
                "Location": location,
                "Longitude": lon,
                "Latitude": lat,
                "datasets": datasets,
            }
        )

    return grouped


def location_matches(query: str, *, limit: int = 8) -> list[dict[str, Any]]:
    pattern = f"%{query.strip()}%"
    try:
        df = pd.read_sql(UNIFIED_SEARCH_SQL, ENGINE, params={"pattern": pattern})
    except Exception:
        return []
    return _aggregate_locations(df)[:limit]


def nearest_sites(matches: list[dict[str, Any]], *, radius_miles: float = 5, limit: int = 8) -> list[dict[str, Any]]:
    if not matches:
        return []
    try:
        all_df = pd.read_sql(UNIFIED_NEARBY_SQL, ENGINE)
    except Exception:
        return []

    all_locations = _aggregate_locations(all_df)
    base_points = [m for m in matches if m.get("Latitude") is not None and m.get("Longitude") is not None]
    if not base_points:
        return []

    nearby: dict[str, dict[str, Any]] = {}
    for base in base_points:
        for candidate in all_locations:
            if candidate["Location"] == base["Location"]:
                continue
            base_lat = base.get("Latitude")
            base_lon = base.get("Longitude")
            cand_lat = candidate.get("Latitude")
            cand_lon = candidate.get("Longitude")
            if None in (base_lat, base_lon, cand_lat, cand_lon):
                continue
            distance = _haversine_miles(base_lat, base_lon, cand_lat, cand_lon)
            if distance > radius_miles:
                continue
            existing = nearby.get(candidate["Location"])
            if existing is None or distance < existing["distance_miles"]:
                entry = dict(candidate)
                entry["distance_miles"] = round(distance, 2)
                nearby[candidate["Location"]] = entry

    return sorted(nearby.values(), key=lambda item: item["distance_miles"])[:limit]


def summary_stats(matches: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    datasets: list[dict[str, Any]] = []
    for match in matches:
        datasets.extend(match.get("datasets") or [])

    if not datasets:
        return {"by_source": [], "by_facility": [], "by_mode": []}

    df = pd.DataFrame(datasets)

    def _counts(column: str) -> list[dict[str, Any]]:
        if column not in df.columns:
            return []
        grouped = (
            df.assign(**{column: df[column].fillna("").astype(str).str.strip()})
            .query(f"`{column}` != ''")
            .groupby(column)
            .size()
            .sort_values(ascending=False)
        )
        return [
            {"name": str(name), "count": int(count)}
            for name, count in grouped.items()
        ]

    return {
        "by_source": _counts("Source"),
        "by_facility": _counts("Facility type"),
        "by_mode": _counts("Mode"),
    }


def _evidence_from_matches(matches: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    evidence: list[dict[str, Any]] = []
    citations: list[dict[str, Any]] = []

    for match in matches:
        location = match.get("Location") or "Unknown location"
        for dataset in match.get("datasets") or []:
            source = dataset.get("Source") or "Unknown source"
            facility = dataset.get("Facility type") or "Unknown facility"
            mode = dataset.get("Mode") or "Unknown mode"
            total_counts = dataset.get("Total counts")
            count_text = "unknown"
            if isinstance(total_counts, (float, int)):
                count_text = f"{total_counts:,.0f}"

            evidence.append(
                {
                    "title": location,
                    "snippet": f"{source} reports {count_text} total counts for {mode} at {facility}.",
                    "source": source,
                    "metadata": {
                        "location": location,
                        "facility_type": facility,
                        "mode": mode,
                        "total_counts": total_counts,
                        "latitude": match.get("Latitude"),
                        "longitude": match.get("Longitude"),
                    },
                }
            )

            citations.append(
                {
                    "title": location,
                    "source": source,
                    "facility_type": facility,
                    "mode": mode,
                }
            )

    return evidence, citations


def _extract_location_hint(message: str) -> str:
    normalized = re.sub(r"\s+", " ", (message or "").strip())
    return normalized[:120]


class EvidenceRetriever:
    def retrieve(self, *, message: str, intent: str) -> RetrievalResult:
        query = _extract_location_hint(message)
        if not query:
            return RetrievalResult(evidence=[], citations=[], stats={"by_source": [], "by_facility": [], "by_mode": []})

        matches = location_matches(query)
        if not matches:
            return RetrievalResult(evidence=[], citations=[], stats={"by_source": [], "by_facility": [], "by_mode": []})

        nearby = nearest_sites(matches)
        combined_matches = matches + nearby if intent in {"compare", "search"} else matches
        evidence, citations = _evidence_from_matches(combined_matches)
        stats = summary_stats(combined_matches)
        return RetrievalResult(evidence=evidence, citations=citations, stats=stats)
