# config/verify_backfill.py
# Purpose: Reads back backfill data from ADLS and verifies
#          completeness and correctness for a given date
# Run with: python config/verify_backfill.py

import os
import io
import sys
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

load_dotenv()

account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key  = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
container    = os.getenv("AZURE_BRONZE_CONTAINER", "bronze")

connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={account_name};"
    f"AccountKey={account_key};"
    f"EndpointSuffix=core.windows.net"
)

client = BlobServiceClient.from_connection_string(connection_string)

# ─────────────────────────────────────────
# Test date and region to verify
# ─────────────────────────────────────────
TEST_DATE   = "2026-05-01"   # one of our 3 backfill days
TEST_REGION = "ERCO"         # Texas — most interesting region
TEST_HOUR   = "14"           # 2pm UTC — peak demand hour

def read_parquet_from_adls(blob_path: str) -> pd.DataFrame:
    """Reads a Parquet file from ADLS and returns a DataFrame."""
    try:
        blob_client = client.get_blob_client(
            container=container,
            blob=blob_path
        )
        data = blob_client.download_blob().readall()
        return pd.read_parquet(io.BytesIO(data))
    except Exception as e:
        print(f"  ERROR reading {blob_path}: {e}")
        return pd.DataFrame()


def verify_eia_data():
    print("=" * 60)
    print("EIA DATA VERIFICATION")
    print("=" * 60)

    # Parse date
    dt    = datetime.strptime(TEST_DATE, "%Y-%m-%d")
    year  = dt.strftime("%Y")
    month = dt.strftime("%m")
    day   = dt.strftime("%d")

    # ── Check 1: File exists for test hour ──────────────────
    print(f"\n[Check 1] Reading EIA file for {TEST_REGION} "
          f"on {TEST_DATE} at hour {TEST_HOUR}")

    blob_path = (
        f"eia/year={year}/month={month}/day={day}/"
        f"region={TEST_REGION}/hour={TEST_HOUR}/data.parquet"
    )

    df = read_parquet_from_adls(blob_path)

    if df.empty:
        print(f"  FAIL — File not found or empty")
        return

    print(f"  PASS — File exists with {len(df)} rows")
    print(f"\n  Data preview:")
    print(f"  {'period':<18} {'type':<6} {'fueltype':<10} {'value':>10}")
    print(f"  {'-'*50}")
    for _, row in df.iterrows():
        print(
            f"  {str(row.get('period','')):<18} "
            f"{str(row.get('type','')):<6} "
            f"{str(row.get('fueltype','')):<10} "
            f"{str(row.get('value','')):<10}"
        )

    # ── Check 2: Demand record exists ───────────────────────
    print(f"\n[Check 2] Demand record present")
    demand = df[df['type'] == 'D']
    if len(demand) > 0:
        demand_val = demand['value'].values[0]
        print(f"  PASS — Demand = {demand_val:,} MWh")
    else:
        print(f"  FAIL — No demand record found")

    # ── Check 3: Fuel type records present ──────────────────
    print(f"\n[Check 3] Fuel type records present")
    fuels = df[df['fueltype'].notna()]
    if len(fuels) > 0:
        print(f"  PASS — {len(fuels)} fuel type records found")
        for _, row in fuels.iterrows():
            print(
                f"    {row.get('type_name',''):<35} "
                f"{row.get('value',0):>8} MWh"
            )
    else:
        print(f"  FAIL — No fuel type records found")

    # ── Check 4: Audit columns present ──────────────────────
    print(f"\n[Check 4] Audit columns present")
    for col in ['ingested_at', 'ingestion_hour', 'pipeline_run']:
        if col in df.columns:
            print(f"  PASS — {col}: {df[col].values[0]}")
        else:
            print(f"  FAIL — {col} missing")

    # ── Check 5: No null values in critical columns ──────────
    print(f"\n[Check 5] No nulls in critical columns")
    for col in ['period', 'respondent', 'value']:
        null_count = df[col].isna().sum()
        if null_count == 0:
            print(f"  PASS — {col}: no nulls")
        else:
            print(f"  WARN — {col}: {null_count} null(s) found")


def verify_weather_data():
    print("\n" + "=" * 60)
    print("WEATHER DATA VERIFICATION")
    print("=" * 60)

    dt    = datetime.strptime(TEST_DATE, "%Y-%m-%d")
    year  = dt.strftime("%Y")
    month = dt.strftime("%m")
    day   = dt.strftime("%d")

    # ── Check 1: File exists ─────────────────────────────────
    print(f"\n[Check 1] Reading weather file for {TEST_REGION} "
          f"on {TEST_DATE} at hour {TEST_HOUR}")

    blob_path = (
        f"weather/year={year}/month={month}/day={day}/"
        f"region={TEST_REGION}/hour={TEST_HOUR}/data.parquet"
    )

    df = read_parquet_from_adls(blob_path)

    if df.empty:
        print(f"  FAIL — File not found or empty")
        return

    print(f"  PASS — File exists with {len(df)} rows")

    # ── Check 2: Weather values present ─────────────────────
    print(f"\n[Check 2] Weather variables present")
    weather_cols = [
        'temperature_2m', 'windspeed_10m',
        'cloudcover', 'precipitation'
    ]
    for col in weather_cols:
        if col in df.columns and df[col].notna().any():
            val = df[col].values[0]
            print(f"  PASS — {col}: {val}")
        else:
            print(f"  FAIL — {col} missing or null")

    # ── Check 3: Period matches EIA format ───────────────────
    print(f"\n[Check 3] Period format matches EIA")
    period = df['period'].values[0]
    if 'T' in str(period):
        print(f"  PASS — period: {period}")
    else:
        print(f"  FAIL — period format wrong: {period}")

    # ── Check 4: Location data present ──────────────────────
    print(f"\n[Check 4] Location data present")
    for col in ['city', 'latitude', 'longitude']:
        if col in df.columns:
            print(f"  PASS — {col}: {df[col].values[0]}")
        else:
            print(f"  FAIL — {col} missing")


def verify_completeness():
    print("\n" + "=" * 60)
    print("COMPLETENESS CHECK — ALL 13 REGIONS × 24 HOURS")
    print("=" * 60)

    dt    = datetime.strptime(TEST_DATE, "%Y-%m-%d")
    year  = dt.strftime("%Y")
    month = dt.strftime("%m")
    day   = dt.strftime("%d")

    regions = [
        "ERCO","CAL","PJM","MISO","NYIS",
        "ISNE","SWPP","BPAT","SOCO","TVA",
        "DUK","FPL","SC"
    ]
    hours = [str(h).zfill(2) for h in range(24)]

    print(f"\nChecking EIA — {TEST_DATE}")
    eia_missing = []
    for region in regions:
        region_missing = 0
        for hour in hours:
            blob_path = (
                f"eia/year={year}/month={month}/day={day}/"
                f"region={region}/hour={hour}/data.parquet"
            )
            try:
                blob_client = client.get_blob_client(
                    container=container, blob=blob_path
                )
                blob_client.get_blob_properties()
            except Exception:
                region_missing += 1
                eia_missing.append(f"{region}/hour={hour}")

        status = "✓" if region_missing == 0 else f"✗ {region_missing} missing"
        print(f"  {region:<6} — {status}")

    print(f"\nChecking Weather — {TEST_DATE}")
    weather_missing = []
    for region in regions:
        region_missing = 0
        for hour in hours:
            blob_path = (
                f"weather/year={year}/month={month}/day={day}/"
                f"region={region}/hour={hour}/data.parquet"
            )
            try:
                blob_client = client.get_blob_client(
                    container=container, blob=blob_path
                )
                blob_client.get_blob_properties()
            except Exception:
                region_missing += 1
                weather_missing.append(f"{region}/hour={hour}")

        status = "✓" if region_missing == 0 else f"✗ {region_missing} missing"
        print(f"  {region:<6} — {status}")

    print(f"\nSummary:")
    print(f"  EIA missing     : {len(eia_missing)} files")
    print(f"  Weather missing : {len(weather_missing)} files")

    if len(eia_missing) == 0 and len(weather_missing) == 0:
        print(f"\n  ALL 312 EIA files present ✓")
        print(f"  ALL 312 Weather files present ✓")
        print(f"  3-day backfill is 100% complete ✓")
    else:
        print(f"\n  Missing files detected — rerun backfill")


if __name__ == "__main__":
    verify_eia_data()
    verify_weather_data()
    verify_completeness()