import json
from datetime import datetime, timezone

import pandas as pd
import ocha_stratus as stratus
from constants import PARTITION_COL, PARTITION_DATE_COL_BY_TABLE


def get_table_stats(table_name: str) -> dict:
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


def read_partition(table_name: str, iso3: str, after_date=None) -> pd.DataFrame:
    engine = stratus.get_engine("prod")
    query = f"SELECT * FROM {table_name} WHERE {PARTITION_COL} = %(iso3)s"
    params = {"iso3": iso3}
    if after_date is not None:
        date_col = PARTITION_DATE_COL_BY_TABLE[table_name]
        query += f" AND {date_col} > %(after_date)s"
        params["after_date"] = after_date
    return pd.read_sql(query, engine, params=params)


def read_existing_partition(table_name: str, iso3: str, container_client) -> pd.DataFrame | None:
    """Read an existing parquet partition from blob. Returns None if it doesn't exist."""
    blob_path = f"{table_name}/{PARTITION_COL}={iso3}/data.parquet"
    try:
        return stratus.load_parquet_from_blob(container_client, blob_path=blob_path)
    except Exception:
        return None


def write_partition(df_partition: pd.DataFrame, iso3: str, table_name: str, container: str, stage: str) -> int:
    blob_path = f"{table_name}/{PARTITION_COL}={iso3}/data.parquet"
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


def run(table_name: str, container: str, stage: str, mode: str = "full") -> None:
    date_col = PARTITION_DATE_COL_BY_TABLE[table_name]
    stats = get_table_stats(table_name)
    container_client = stratus.get_container_client(container, stage, write=True)

    print(f"Mode: {mode} | Writing {len(stats['countries'])} countries to {container}/{table_name}/")
    total_rows = 0

    for iso3 in stats["countries"]:
        if mode == "update":
            existing = read_existing_partition(table_name, iso3, container_client)
            if existing is not None:
                max_date = existing[date_col].max()
                new_rows = read_partition(table_name, iso3, after_date=max_date)
                if new_rows.empty:
                    print(f"  {iso3}: up to date")
                    continue
                df = pd.concat([existing, new_rows], ignore_index=True)
                print(f"  {iso3}: +{len(new_rows):,} new rows")
            else:
                df = read_partition(table_name, iso3)
                print(f"  {iso3}: {len(df):,} rows (new partition)")
        else:
            df = read_partition(table_name, iso3)
            print(f"  {iso3}: {len(df):,} rows")

        rows = write_partition(df, iso3, table_name, container, stage)
        total_rows += rows

    write_metadata(container_client, table_name, stats)
    print(f"Done — {total_rows:,} rows written across {len(stats['countries'])} countries")
