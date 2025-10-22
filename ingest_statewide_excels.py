# ingest_statewide_excels.py
from __future__ import annotations

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

DB_URL = "postgresql://postgres:gw2ksoft@localhost/TrafficDB"
ENGINE = create_engine(DB_URL, future=True)

# ---- FILE PATHS ------------------------------------------------------------
FILES = {
    "ped": r"C:\Users\tejas\Desktop\AccSafety\WisconsinPedBikeCountDatabase_PedestrianCounts_032824.xlsx",
    "bic": r"C:\Users\tejas\Desktop\AccSafety\WisconsinPedBikeCountDatabase_BicyclistCounts_032824.xlsx",
    "trl": r"C:\Users\tejas\Desktop\AccSafety\WisconsinPedBikeCountDatabase_TrailUserCounts_032824.xlsx",
}

# ---- TABLE NAMES -----------------------------------------------------------
TARGET_TABLE = {
    "ped": "statewide_pedestrian",
    "bic": "statewide_bicyclist",
    "trl": "statewide_trailuser",
}

# ---- EXPECTED DESTINATION COLUMNS (WIDE SCHEMA) ---------------------------
DEST_COLS = [
    # identifiers / location
    "location_id", "location_name", "city", "wisdot_region", "year",
    "longitude", "latitude",
    # temporal / meta
    "month", "date_of_count", "duration", "interval_minutes",
    "day_of_week", "time_of_week", "time_of_day_start", "time_of_day_end",
    # conditions
    "weather_text", "precipitation_flag",
    # totals & rates during the period
    "total_users_during_period", "total_hours_counted", "users_per_hour",
    # expansion
    "hour_to_week_factor", "avg_hour_to_week_factor", "week_to_year_factor",
    # outputs
    "estimated_annual",
    # optional notes
    "footnote_ids",
]

# ---- HEADER ALIASES (left = df header options, right = destination col) ----
# Add/adjust aliases if your spreadsheets use slightly different text.
ALIASES_COMMON = {
    # ids / geo
    "Location ID": "location_id",
    "Location Name": "location_name",
    "City": "city",
    "WisDOT Region": "wisdot_region",
    "Year": "year",
    # duration & times
    "Duration": "duration",
    "Interval (minutes)": "interval_minutes",
    "Interval Minutes": "interval_minutes",
    "Day of Week (1=Sun)": "day_of_week",
    "Day of Week": "day_of_week",
    "Time of Week": "time_of_week",
    "Start Time": "time_of_day_start",
    "End Time": "time_of_day_end",
    "Month": "month",
    "Date": "date_of_count",
    "Date of Count": "date_of_count",
    # weather
    "Weather": "weather_text",
    "Precipitation? (0/1)": "precipitation_flag",
    "Precipitation Flag": "precipitation_flag",
    # period totals / rates
    "Total User Count During Count Period": "total_users_during_period",
    "Total Users During Count Period": "total_users_during_period",
    "Total Count During Period": "total_users_during_period",
    "Total Hours Counted": "total_hours_counted",
    "Users Per Hour": "users_per_hour",
    # factors
    "Hour to Week Expansion Factor": "hour_to_week_factor",
    "Average Hour to Week Expansion Factor": "avg_hour_to_week_factor",
    "Week to Year Expansion Factor": "week_to_year_factor",
    # geo (sometimes appear)
    "Longitude": "longitude",
    "Latitude": "latitude",
    "X": "longitude",  # if provided as X/Y
    "Y": "latitude",
    # notes
    "Footnote(s)": "footnote_ids",
    "Notes (IDs)": "footnote_ids",
}

# Kind-specific annual columns
ANNUAL_ALIASES = {
    "ped": [
        "Estimated Annual Pedestrian Count",
        "Estimated Annual Count (Pedestrians)",
        "Estimated Annual Users",
    ],
    "bic": [
        "Estimated Annual Bicyclist Count",
        "Estimated Annual Count (Bicyclists)",
        "Estimated Annual Users",
    ],
    "trl": [
        "Estimated Annual Trail User Count",
        "Estimated Annual Count (Trail Users)",
        "Estimated Annual Users",
    ],
}

# ---- DDL FOR WIDE TABLES ---------------------------------------------------
CREATE_TABLE_SQL = {
    "statewide_pedestrian": """
        CREATE TABLE IF NOT EXISTS statewide_pedestrian (
          location_id            TEXT,
          location_name          TEXT,
          city                   TEXT,
          wisdot_region          TEXT,
          year                   INT,
          longitude              DOUBLE PRECISION,
          latitude               DOUBLE PRECISION,
          month                  INT,
          date_of_count          DATE,
          duration               TEXT,
          interval_minutes       INT,
          day_of_week            INT,
          time_of_week           TEXT,
          time_of_day_start      TIME,
          time_of_day_end        TIME,
          weather_text           TEXT,
          precipitation_flag     INT,
          total_users_during_period  NUMERIC,
          total_hours_counted        NUMERIC,
          users_per_hour             NUMERIC,
          hour_to_week_factor        NUMERIC,
          avg_hour_to_week_factor    NUMERIC,
          week_to_year_factor        NUMERIC,
          estimated_annual           NUMERIC,
          footnote_ids               TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_statewide_ped_year ON statewide_pedestrian(year);
        CREATE INDEX IF NOT EXISTS idx_statewide_ped_loc  ON statewide_pedestrian(location_name);
    """,
    "statewide_bicyclist": """
        CREATE TABLE IF NOT EXISTS statewide_bicyclist (
          location_id            TEXT,
          location_name          TEXT,
          city                   TEXT,
          wisdot_region          TEXT,
          year                   INT,
          longitude              DOUBLE PRECISION,
          latitude               DOUBLE PRECISION,
          month                  INT,
          date_of_count          DATE,
          duration               TEXT,
          interval_minutes       INT,
          day_of_week            INT,
          time_of_week           TEXT,
          time_of_day_start      TIME,
          time_of_day_end        TIME,
          weather_text           TEXT,
          precipitation_flag     INT,
          total_users_during_period  NUMERIC,
          total_hours_counted        NUMERIC,
          users_per_hour             NUMERIC,
          hour_to_week_factor        NUMERIC,
          avg_hour_to_week_factor    NUMERIC,
          week_to_year_factor        NUMERIC,
          estimated_annual           NUMERIC,
          footnote_ids               TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_statewide_bic_year ON statewide_bicyclist(year);
        CREATE INDEX IF NOT EXISTS idx_statewide_bic_loc  ON statewide_bicyclist(location_name);
    """,
    "statewide_trailuser": """
        CREATE TABLE IF NOT EXISTS statewide_trailuser (
          location_id            TEXT,
          location_name          TEXT,
          city                   TEXT,
          wisdot_region          TEXT,
          year                   INT,
          longitude              DOUBLE PRECISION,
          latitude               DOUBLE PRECISION,
          month                  INT,
          date_of_count          DATE,
          duration               TEXT,
          interval_minutes       INT,
          day_of_week            INT,
          time_of_week           TEXT,
          time_of_day_start      TIME,
          time_of_day_end        TIME,
          weather_text           TEXT,
          precipitation_flag     INT,
          total_users_during_period  NUMERIC,
          total_hours_counted        NUMERIC,
          users_per_hour             NUMERIC,
          hour_to_week_factor        NUMERIC,
          avg_hour_to_week_factor    NUMERIC,
          week_to_year_factor        NUMERIC,
          estimated_annual           NUMERIC,
          footnote_ids               TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_statewide_trl_year ON statewide_trailuser(year);
        CREATE INDEX IF NOT EXISTS idx_statewide_trl_loc  ON statewide_trailuser(location_name);
    """,
}

# ---- Helpers ---------------------------------------------------------------
def ensure_table(table: str) -> None:
    sql = CREATE_TABLE_SQL[table]
    with ENGINE.begin() as conn:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.exec_driver_sql(stmt + ";")

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2.columns = df2.columns.map(lambda c: str(c).strip())
    return df2

def _choose_first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _coerce_numeric(series):
    return pd.to_numeric(series, errors="coerce")

def _coerce_int(series):
    return pd.to_numeric(series, errors="coerce").astype("Int64")

def _coerce_date(series):
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.date

def _coerce_time(series):
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.time

def _strip(series):
    return series.astype(str).str.strip()

def _safe_get(df: pd.DataFrame, col: str):
    return df[col] if (col is not None and col in df.columns) else pd.Series([np.nan] * len(df))

def _apply_aliases(df: pd.DataFrame, kind: str) -> pd.DataFrame:
    """
    Build an output frame containing DEST_COLS filled from any matching aliases.
    Missing columns remain NaN/NULL.
    """
    out = pd.DataFrame(index=df.index)

    # 1) common aliases
    for src, dst in ALIASES_COMMON.items():
        if src in df.columns:
            out[dst] = df[src]

    # 2) annual column (kind specific)
    annual_col = _choose_first_col(df, ANNUAL_ALIASES[kind])
    if annual_col:
        out["estimated_annual"] = df[annual_col]

    # Ensure all expected destination columns exist
    for c in DEST_COLS:
        if c not in out.columns:
            out[c] = np.nan

    # Type coercions / cleanups
    # strings
    for c in ["location_id", "location_name", "city", "wisdot_region", "duration", "time_of_week", "weather_text", "footnote_ids"]:
        out[c] = _strip(out[c])

    # ints
    for c in ["year", "month", "interval_minutes", "day_of_week", "precipitation_flag"]:
        out[c] = _coerce_int(out[c])

    # numerics
    for c in [
        "total_users_during_period", "total_hours_counted", "users_per_hour",
        "hour_to_week_factor", "avg_hour_to_week_factor", "week_to_year_factor",
        "estimated_annual",
        "longitude", "latitude",
    ]:
        out[c] = _coerce_numeric(out[c])

    # dates/times
    out["date_of_count"] = _coerce_date(out["date_of_count"])
    out["time_of_day_start"] = _coerce_time(out["time_of_day_start"])
    out["time_of_day_end"] = _coerce_time(out["time_of_day_end"])

    # Drop obvious empties (no name or no year)
    out = out[out["location_name"].notna() & out["location_name"].ne("")].copy()
    out = out[out["year"].notna()].copy()

    return out.reset_index(drop=True)

def load_one(kind: str, path: str) -> None:
    print(f"[{kind}] Reading: {path}")
    df = pd.read_excel(path, sheet_name=0)
    df = _norm_cols(df)

    # Build output using aliases; leave missing fields blank (NULL)
    out = _apply_aliases(df, kind)

    table = TARGET_TABLE[kind]
    ensure_table(table)

    with ENGINE.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE TABLE {table};")
        out.to_sql(table, con=conn, if_exists="append", index=False, method="multi", chunksize=5000)

    print(f"[{kind}] Loaded {len(out):,} rows into {table}")

def main():
    load_one("ped", FILES["ped"])
    load_one("bic", FILES["bic"])
    load_one("trl", FILES["trl"])
    print("All done.")

if __name__ == "__main__":
    main()
