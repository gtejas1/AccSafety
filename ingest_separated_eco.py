import os
import pandas as pd
from sqlalchemy import create_engine

DB_URL = "postgresql://postgres:gw2ksoft@localhost/TrafficDB"
engine = create_engine(DB_URL)

EXCLUDE_COLS = {"time", "date", "total", "sum"}  # common “total” header guard

def process_excel(path):
    """Extract and tidy ECO-format sheet into long form."""
    xls = pd.ExcelFile(path)
    sheet = xls.sheet_names[0]
    df = pd.read_excel(xls, sheet_name=sheet, header=None)

    # find header row (first col == 'Time')
    hdr_idx = df.index[df.iloc[:, 0].astype(str).str.strip().str.lower().eq("time")]
    if len(hdr_idx) == 0:
        print(f"⚠️  No header row found in {os.path.basename(path)}")
        return None
    hdr = hdr_idx[0]
    headers = [str(c).strip() for c in df.iloc[hdr].tolist()]
    df = df.iloc[hdr + 1:].copy()
    df.columns = headers
    df = df.dropna(how="all")

    # standardize date
    if "Time" not in df.columns:
        print(f"⚠️  Missing 'Time' column in {os.path.basename(path)}")
        return None
    df["Date"] = pd.to_datetime(df["Time"], errors="coerce")
    df = df.dropna(subset=["Date"])

    # location from file name
    location_name = os.path.splitext(os.path.basename(path))[0]

    # melt directions; drop any roll-up columns like Total/Sum
    direction_cols = [c for c in df.columns
                      if c.lower() not in EXCLUDE_COLS]
    if not direction_cols:
        print(f"⚠️  No direction columns in {os.path.basename(path)}")
        return None

    melted = df.melt(id_vars="Date", value_vars=direction_cols,
                     var_name="direction", value_name="count")
    melted["location_name"] = location_name
    melted["count"] = pd.to_numeric(melted["count"], errors="coerce").fillna(0).astype(int)
    melted.rename(columns={"Date": "date"}, inplace=True)
    return melted[["location_name", "date", "direction", "count"]]

def ingest_folder(folder, table):
    if not os.path.isdir(folder):
        print(f"ℹ️  Folder not found: {folder} (skipping)")
        return
    files = [f for f in os.listdir(folder) if f.lower().endswith(".xlsx")]
    total = 0
    for f in files:
        path = os.path.join(folder, f)
        df = process_excel(path)
        if df is not None and not df.empty:
            df.to_sql(table, engine, if_exists="append", index=False)
            total += len(df)
            print(f"✅ {table}: +{len(df)} rows from {f}")
    print(f"--- {table}: {total} total rows inserted ---")

if __name__ == "__main__":
    base = os.path.abspath(".")
    ingest_folder(os.path.join(base, "Pedestrian_Pilot_Counts"), "eco_ped_traffic_data")
    ingest_folder(os.path.join(base, "Cyclist_Pilot_Counts"),   "eco_bike_traffic_data")
    ingest_folder(os.path.join(base, "Both"),                   "eco_both_traffic_data")   # ← now included
    ingest_folder(os.path.join(base, "Trails_Pilot_Counts"),    "trail_traffic_data")
    print("All folders processed.")
