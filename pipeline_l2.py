import json
from datetime import datetime, timezone

import pandas as pd
import ocha_stratus as stratus
from constants import PARTITION_COL, PARTITION_DATE_COL_BY_TABLE


def get_table_stats(table_name: str, partition_date_col: str) -> dict:
    engine = stratus.get_engine("prod")

    row = pd.read_sql(f"""
        SELECT
            COUNT(*)                        AS row_count,
            COUNT(DISTINCT {PARTITION_COL}) AS partition_count,
            MIN(valid_date)                 AS valid_date_min,
            MAX(valid_date)                 AS valid_date_max
        FROM {table_name}
    """, engine).iloc[0]

    countries = pd.read_sql(f"""
        SELECT DISTINCT {PARTITION_COL} FROM {table_name}
        ORDER BY {PARTITION_COL}
    """, engine)[PARTITION_COL].tolist()

    columns = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 0", engine).columns.tolist()

    return {
        "row_count": int(row["row_count"]),
        "countries": countries,
        "valid_date_min": row["valid_date_min"],
        "valid_date_max": row["valid_date_max"],
        "columns": columns,
    }


def read_partition(table_name: str, iso3: str, partition_date_col: str, year: int = None, after_date=None) -> pd.DataFrame:
    engine = stratus.get_engine("prod")
    query = f"SELECT * FROM {table_name} WHERE {PARTITION_COL} = %(iso3)s"
    params = {"iso3": iso3}
    if year is not None:
        query += f" AND EXTRACT(YEAR FROM {partition_date_col}) = %(year)s"
        params["year"] = year
    if after_date is not None:
        query += f" AND {partition_date_col} > %(after_date)s"
        params["after_date"] = after_date
    return pd.read_sql(query, engine, params=params)


def read_existing_partition(table_name: str, iso3: str, year: int, container_client) -> pd.DataFrame | None:
    """Read an existing year parquet file from blob. Returns None if it doesn't exist."""
    blob_path = f"{table_name}/{PARTITION_COL}={iso3}/{year}.parquet"
    try:
        return stratus.load_parquet_from_blob(container_client, blob_path=blob_path)
    except Exception:
        return None


def write_partition(df_partition: pd.DataFrame, iso3: str, year: int, table_name: str, container: str, stage: str) -> int:
    blob_path = f"{table_name}/{PARTITION_COL}={iso3}/{year}.parquet"
    stratus.upload_parquet_to_blob(df_partition.reset_index(drop=True), blob_path, stage=stage, container_name=container)
    return len(df_partition)


def write_metadata(container_client, table_name: str, stats: dict) -> None:
    meta = {
        "table": table_name,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "row_count": stats["row_count"],
        "partition_count": len(stats["countries"]),
        "temporal_extent": {
            "valid_date_min": str(stats["valid_date_min"]),
            "valid_date_max": str(stats["valid_date_max"]),
        },
        "columns": stats["columns"],
        "countries": stats["countries"],
    }
    blob_client = container_client.get_blob_client(f"{table_name}/_metadata.json")
    blob_client.upload_blob(json.dumps(meta, indent=2).encode(), overwrite=True)
    print(f"  Metadata written to {table_name}/_metadata.json")


def run(table_name: str, container: str, stage: str, mode: str) -> None:
    partition_date_col = PARTITION_DATE_COL_BY_TABLE[table_name]
    current_year = datetime.now().year

    stats = get_table_stats(table_name, partition_date_col)
    container_client = stratus.get_container_client(container, stage, write=True)

    print(f"Mode: {mode}")
    print(f"Writing {len(stats['countries'])} countries to {container}/{table_name}/")
    total_rows = 0

    for iso3 in stats["countries"]:
        if mode == "update":
            existing = read_existing_partition(table_name, iso3, current_year, container_client)
            if existing is not None:
                max_date = existing[partition_date_col].max()
                new_rows = read_partition(table_name, iso3, partition_date_col, year=current_year, after_date=max_date)
                if new_rows.empty:
                    print(f"  {iso3}/{current_year}: up to date")
                    continue
                df = pd.concat([existing, new_rows], ignore_index=True)
                print(f"  {iso3}/{current_year}: +{len(new_rows):,} new rows")
            else:
                df = read_partition(table_name, iso3, partition_date_col, year=current_year)
                print(f"  {iso3}/{current_year}: {len(df):,} rows (new partition)")
            write_partition(df, iso3, current_year, table_name, container, stage)
            total_rows += len(df)
        else:
            df = read_partition(table_name, iso3, partition_date_col)
            if df.empty:
                continue
            years = pd.to_datetime(df[partition_date_col]).dt.year.unique()
            for year in years:
                df_year = df[pd.to_datetime(df[partition_date_col]).dt.year == year]
                rows = write_partition(df_year, iso3, year, table_name, container, stage)
                total_rows += rows
                print(f"  {iso3}/{year}: {rows:,} rows")

    write_metadata(container_client, table_name, stats)
    print(f"Done — {total_rows:,} rows written across {len(stats['countries'])} countries")
