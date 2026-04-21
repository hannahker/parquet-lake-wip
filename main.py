import argparse
from dotenv import load_dotenv

load_dotenv()

import pipeline_l2 as l2
import pipeline_l1 as l1
from constants import PARTITION_DATE_COL_BY_TABLE

parser = argparse.ArgumentParser(description="Write partitioned parquet files to Azure Blob Storage.")
parser.add_argument("--table",               required=True, choices=list(PARTITION_DATE_COL_BY_TABLE), help="Table to export")
parser.add_argument("--stage",               default="dev", choices=["dev", "prod"], help="Storage stage")
parser.add_argument("--partition-strategy",  default="l2", choices=["l1", "l2"], help="l1: iso3 only; l2: iso3 + year")
parser.add_argument("--mode",                default="full", choices=["full", "update"], help="full: overwrite all data; update: append new dates only")
args = parser.parse_args()

container = f"rasterstats-public-{args.partition_strategy}"

if args.partition_strategy == "l1":
    l1.run(table_name=args.table, container=container, stage=args.stage, mode=args.mode)
else:
    l2.run(table_name=args.table, container=container, stage=args.stage, mode=args.mode)
