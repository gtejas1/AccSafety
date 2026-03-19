from __future__ import annotations

import io
import math
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

UPLOAD_MODE_OPTIONS = ["Pedestrian", "Bicyclist", "Both", "Trail"]
UPLOAD_TARGET_TABLE = {
    "Pedestrian": "eco_ped_traffic_data",
    "Bicyclist": "eco_bike_traffic_data",
    "Both": "eco_both_traffic_data",
    "Trail": "trail_traffic_data",
}
_EXCLUDE_COLS = {"time", "date", "total", "sum"}
_MIN_DB_COUNT = -2147483648
_MAX_DB_COUNT = 2147483647


@dataclass
class ParsedUploadRow:
    count_time: Any
    direction: str
    count_value: int | None
    validation_status: str
    validation_message: str


@dataclass
class ParsedUpload:
    upload_id: str
    original_filename: str
    selected_mode: str
    location_name: str
    location_override: str
    notes: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    status: str
    error_message: str
    rows: list[ParsedUploadRow]


def normalize_mode(mode: str | None) -> str:
    mode_text = (mode or "").strip()
    if mode_text in UPLOAD_TARGET_TABLE:
        return mode_text
    return "Pedestrian"


def _parse_count_value(raw_count: Any) -> tuple[int | None, list[str]]:
    issues: list[str] = []
    numeric_value = pd.to_numeric(raw_count, errors="coerce")
    if pd.isna(numeric_value):
        issues.append("Count is not numeric")
        return None, issues

    try:
        numeric_float = float(numeric_value)
    except Exception:
        issues.append("Count is not numeric")
        return None, issues

    if not math.isfinite(numeric_float):
        issues.append("Count is not finite")
        return None, issues

    count_value = int(numeric_float)
    if abs(numeric_float - count_value) > 1e-9:
        issues.append("Count must be a whole number")
        return None, issues

    if count_value < _MIN_DB_COUNT or count_value > _MAX_DB_COUNT:
        issues.append("Count is out of range")
        return None, issues

    return count_value, issues


def ensure_tables(engine) -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS admin_upload_manifests (
          upload_id TEXT PRIMARY KEY,
          original_filename TEXT NOT NULL,
          selected_mode TEXT NOT NULL,
          location_name TEXT,
          location_override TEXT,
          notes TEXT,
          status TEXT NOT NULL,
          total_rows INTEGER NOT NULL DEFAULT 0,
          valid_rows INTEGER NOT NULL DEFAULT 0,
          invalid_rows INTEGER NOT NULL DEFAULT 0,
          error_message TEXT,
          uploaded_by TEXT,
          uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          published_by TEXT,
          published_at TIMESTAMPTZ
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS admin_upload_staging (
          id BIGSERIAL PRIMARY KEY,
          upload_id TEXT NOT NULL REFERENCES admin_upload_manifests(upload_id) ON DELETE CASCADE,
          original_filename TEXT NOT NULL,
          selected_mode TEXT NOT NULL,
          location_name TEXT,
          count_time TIMESTAMPTZ,
          direction TEXT,
          count_value BIGINT,
          validation_status TEXT NOT NULL,
          validation_message TEXT,
          uploaded_by TEXT,
          uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          published_by TEXT,
          published_at TIMESTAMPTZ
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_admin_upload_staging_upload_id ON admin_upload_staging(upload_id)",
        "ALTER TABLE admin_upload_staging ALTER COLUMN count_value TYPE BIGINT",
    ]
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))


def parse_excel_upload(
    file_bytes: bytes,
    *,
    filename: str,
    selected_mode: str,
    location_override: str = "",
    notes: str = "",
) -> ParsedUpload:
    upload_id = str(uuid.uuid4())
    selected_mode = normalize_mode(selected_mode)
    original_filename = Path(filename or "upload.xlsx").name or "upload.xlsx"
    default_location = Path(original_filename).stem.strip()
    location_override = (location_override or "").strip()
    location_name = location_override or default_location

    try:
        excel = pd.ExcelFile(io.BytesIO(file_bytes))
    except Exception as exc:
        return ParsedUpload(
            upload_id=upload_id,
            original_filename=original_filename,
            selected_mode=selected_mode,
            location_name=location_name,
            location_override=location_override,
            notes=(notes or "").strip(),
            total_rows=0,
            valid_rows=0,
            invalid_rows=0,
            status="invalid",
            error_message=f"Unable to read Excel file: {exc}",
            rows=[],
        )

    if not excel.sheet_names:
        return ParsedUpload(
            upload_id=upload_id,
            original_filename=original_filename,
            selected_mode=selected_mode,
            location_name=location_name,
            location_override=location_override,
            notes=(notes or "").strip(),
            total_rows=0,
            valid_rows=0,
            invalid_rows=0,
            status="invalid",
            error_message="The workbook does not contain any sheets.",
            rows=[],
        )

    sheet_name = excel.sheet_names[0]
    raw_df = pd.read_excel(excel, sheet_name=sheet_name, header=None)
    if raw_df.empty or raw_df.shape[1] == 0:
        return ParsedUpload(
            upload_id=upload_id,
            original_filename=original_filename,
            selected_mode=selected_mode,
            location_name=location_name,
            location_override=location_override,
            notes=(notes or "").strip(),
            total_rows=0,
            valid_rows=0,
            invalid_rows=0,
            status="invalid",
            error_message="The worksheet is empty.",
            rows=[],
        )

    header_matches = raw_df.index[
        raw_df.iloc[:, 0].astype(str).str.strip().str.casefold().eq("time")
    ]
    if len(header_matches) == 0:
        return ParsedUpload(
            upload_id=upload_id,
            original_filename=original_filename,
            selected_mode=selected_mode,
            location_name=location_name,
            location_override=location_override,
            notes=(notes or "").strip(),
            total_rows=0,
            valid_rows=0,
            invalid_rows=0,
            status="invalid",
            error_message="A header row starting with 'Time' is required.",
            rows=[],
        )

    header_row_index = int(header_matches[0])
    headers = [str(value).strip() if pd.notna(value) else "" for value in raw_df.iloc[header_row_index].tolist()]
    data_df = raw_df.iloc[header_row_index + 1 :].copy()
    data_df.columns = headers
    data_df = data_df.dropna(how="all")

    if "Time" not in data_df.columns:
        return ParsedUpload(
            upload_id=upload_id,
            original_filename=original_filename,
            selected_mode=selected_mode,
            location_name=location_name,
            location_override=location_override,
            notes=(notes or "").strip(),
            total_rows=0,
            valid_rows=0,
            invalid_rows=0,
            status="invalid",
            error_message="The worksheet is missing the 'Time' column.",
            rows=[],
        )

    direction_cols = [
        column
        for idx, column in enumerate(data_df.columns)
        if idx > 0 and str(column).strip() and str(column).strip().lower() not in _EXCLUDE_COLS
    ]
    if not direction_cols:
        return ParsedUpload(
            upload_id=upload_id,
            original_filename=original_filename,
            selected_mode=selected_mode,
            location_name=location_name,
            location_override=location_override,
            notes=(notes or "").strip(),
            total_rows=0,
            valid_rows=0,
            invalid_rows=0,
            status="invalid",
            error_message="No count columns were found after the 'Time' column.",
            rows=[],
        )

    single_series = len(direction_cols) == 1
    parsed_rows: list[ParsedUploadRow] = []
    for _, raw_row in data_df.iterrows():
        time_raw = raw_row.get("Time")
        for direction_col in direction_cols:
            raw_count = raw_row.get(direction_col)
            if pd.isna(time_raw) and pd.isna(raw_count):
                continue

            validation_issues: list[str] = []
            parsed_time = pd.to_datetime(time_raw, errors="coerce")
            if pd.isna(parsed_time):
                validation_issues.append("Invalid timestamp")

            count_value, count_issues = _parse_count_value(raw_count)
            validation_issues.extend(count_issues)

            direction = "Total" if single_series else str(direction_col).strip() or "Total"
            parsed_rows.append(
                ParsedUploadRow(
                    count_time=None if pd.isna(parsed_time) else parsed_time.to_pydatetime(),
                    direction=direction,
                    count_value=count_value,
                    validation_status="invalid" if validation_issues else "valid",
                    validation_message="; ".join(validation_issues),
                )
            )

    valid_rows = sum(1 for row in parsed_rows if row.validation_status == "valid")
    invalid_rows = len(parsed_rows) - valid_rows
    status = "ready_for_review" if valid_rows > 0 else "invalid"
    error_message = ""
    if not parsed_rows:
        status = "invalid"
        error_message = "The worksheet did not produce any count rows."
    elif valid_rows == 0:
        error_message = "All parsed rows failed validation."

    return ParsedUpload(
        upload_id=upload_id,
        original_filename=original_filename,
        selected_mode=selected_mode,
        location_name=location_name,
        location_override=location_override,
        notes=(notes or "").strip(),
        total_rows=len(parsed_rows),
        valid_rows=valid_rows,
        invalid_rows=invalid_rows,
        status=status,
        error_message=error_message,
        rows=parsed_rows,
    )


def stage_upload(engine, parsed_upload: ParsedUpload, *, uploaded_by: str) -> str:
    ensure_tables(engine)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO admin_upload_manifests (
                  upload_id,
                  original_filename,
                  selected_mode,
                  location_name,
                  location_override,
                  notes,
                  status,
                  total_rows,
                  valid_rows,
                  invalid_rows,
                  error_message,
                  uploaded_by
                ) VALUES (
                  :upload_id,
                  :original_filename,
                  :selected_mode,
                  :location_name,
                  :location_override,
                  :notes,
                  :status,
                  :total_rows,
                  :valid_rows,
                  :invalid_rows,
                  :error_message,
                  :uploaded_by
                )
                """
            ),
            {
                "upload_id": parsed_upload.upload_id,
                "original_filename": parsed_upload.original_filename,
                "selected_mode": parsed_upload.selected_mode,
                "location_name": parsed_upload.location_name,
                "location_override": parsed_upload.location_override,
                "notes": parsed_upload.notes,
                "status": parsed_upload.status,
                "total_rows": parsed_upload.total_rows,
                "valid_rows": parsed_upload.valid_rows,
                "invalid_rows": parsed_upload.invalid_rows,
                "error_message": parsed_upload.error_message,
                "uploaded_by": uploaded_by,
            },
        )

        for row in parsed_upload.rows:
            conn.execute(
                text(
                    """
                    INSERT INTO admin_upload_staging (
                      upload_id,
                      original_filename,
                      selected_mode,
                      location_name,
                      count_time,
                      direction,
                      count_value,
                      validation_status,
                      validation_message,
                      uploaded_by
                    ) VALUES (
                      :upload_id,
                      :original_filename,
                      :selected_mode,
                      :location_name,
                      :count_time,
                      :direction,
                      :count_value,
                      :validation_status,
                      :validation_message,
                      :uploaded_by
                    )
                    """
                ),
                {
                    "upload_id": parsed_upload.upload_id,
                    "original_filename": parsed_upload.original_filename,
                    "selected_mode": parsed_upload.selected_mode,
                    "location_name": parsed_upload.location_name,
                    "count_time": row.count_time,
                    "direction": row.direction,
                    "count_value": row.count_value,
                    "validation_status": row.validation_status,
                    "validation_message": row.validation_message,
                    "uploaded_by": uploaded_by,
                },
            )

    return parsed_upload.upload_id


def list_uploads(engine, *, limit: int = 50) -> list[dict[str, Any]]:
    ensure_tables(engine)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                  upload_id,
                  original_filename,
                  selected_mode,
                  location_name,
                  status,
                  total_rows,
                  valid_rows,
                  invalid_rows,
                  error_message,
                  uploaded_by,
                  uploaded_at,
                  published_by,
                  published_at
                FROM admin_upload_manifests
                ORDER BY uploaded_at DESC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings().all()
    return [dict(row) for row in rows]


def get_upload_detail(engine, upload_id: str) -> dict[str, Any] | None:
    ensure_tables(engine)
    with engine.connect() as conn:
        manifest = conn.execute(
            text(
                """
                SELECT
                  upload_id,
                  original_filename,
                  selected_mode,
                  location_name,
                  location_override,
                  notes,
                  status,
                  total_rows,
                  valid_rows,
                  invalid_rows,
                  error_message,
                  uploaded_by,
                  uploaded_at,
                  published_by,
                  published_at
                FROM admin_upload_manifests
                WHERE upload_id = :upload_id
                """
            ),
            {"upload_id": upload_id},
        ).mappings().first()
        if manifest is None:
            return None

        rows = conn.execute(
            text(
                """
                SELECT
                  count_time,
                  direction,
                  count_value,
                  validation_status,
                  validation_message,
                  published_by,
                  published_at
                FROM admin_upload_staging
                WHERE upload_id = :upload_id
                ORDER BY count_time NULLS LAST, direction
                LIMIT 250
                """
            ),
            {"upload_id": upload_id},
        ).mappings().all()

    detail_rows = [dict(row) for row in rows]
    direction_summary: list[dict[str, Any]] = []
    summary_source = [row for row in detail_rows if row.get("validation_status") == "valid"]
    if summary_source:
        summary_df = pd.DataFrame(summary_source)
        grouped = (
            summary_df.groupby("direction", dropna=False)["count_value"]
            .agg(["count", "sum"])
            .reset_index()
            .sort_values("direction")
        )
        direction_summary = [
            {
                "direction": record["direction"] or "Unknown",
                "row_count": int(record["count"]),
                "total_counts": int(record["sum"]) if pd.notna(record["sum"]) else 0,
            }
            for _, record in grouped.iterrows()
        ]

    return {
        "manifest": dict(manifest),
        "rows": detail_rows,
        "direction_summary": direction_summary,
    }


def publish_upload(engine, upload_id: str, *, published_by: str) -> dict[str, Any]:
    ensure_tables(engine)
    with engine.begin() as conn:
        manifest = conn.execute(
            text(
                """
                SELECT
                  upload_id,
                  selected_mode,
                  valid_rows,
                  status,
                  published_at
                FROM admin_upload_manifests
                WHERE upload_id = :upload_id
                FOR UPDATE
                """
            ),
            {"upload_id": upload_id},
        ).mappings().first()
        if manifest is None:
            raise KeyError(upload_id)

        if manifest.get("published_at") is not None:
            return {"status": "already_published", "inserted_rows": 0}

        if int(manifest.get("valid_rows") or 0) <= 0:
            return {"status": "invalid", "inserted_rows": 0}

        target_table = UPLOAD_TARGET_TABLE[normalize_mode(manifest.get("selected_mode"))]
        insert_sql = text(
            f"""
            INSERT INTO {target_table} (location_name, date, direction, count)
            SELECT
              m.location_name,
              s.count_time,
              s.direction,
              s.count_value
            FROM admin_upload_staging s
            JOIN admin_upload_manifests m
              ON m.upload_id = s.upload_id
            WHERE s.upload_id = :upload_id
              AND s.validation_status = 'valid'
            """
        )
        insert_result = conn.execute(insert_sql, {"upload_id": upload_id})
        inserted_rows = insert_result.rowcount if insert_result.rowcount is not None else int(manifest["valid_rows"])

        conn.execute(
            text(
                """
                UPDATE admin_upload_staging
                SET published_by = :published_by,
                    published_at = NOW()
                WHERE upload_id = :upload_id
                  AND validation_status = 'valid'
                  AND published_at IS NULL
                """
            ),
            {"upload_id": upload_id, "published_by": published_by},
        )
        conn.execute(
            text(
                """
                UPDATE admin_upload_manifests
                SET status = 'published',
                    published_by = :published_by,
                    published_at = NOW()
                WHERE upload_id = :upload_id
                """
            ),
            {"upload_id": upload_id, "published_by": published_by},
        )

    return {"status": "published", "inserted_rows": int(inserted_rows or 0)}
