import argparse
from dotenv import load_dotenv
from pipeline import PARTITION_DATE_COL_BY_TABLE, run

load_dotenv()

parser = argparse.ArgumentParser(description="Write partitioned parquet files to Azure Blob Storage.")
parser.add_argument("--table",     required=True, choices=list(PARTITION_DATE_COL_BY_TABLE), help="Table to export")
parser.add_argument("--container", default="rasterstats-public", help="Azure Blob container name")
parser.add_argument("--stage",     default="dev", choices=["dev", "prod"], help="Storage stage")
parser.add_argument("--mode",      default="full", choices=["full", "current_year"], help="Overwrite all data or current year only")
args = parser.parse_args()

run(table_name=args.table, container=args.container, stage=args.stage, mode=args.mode)
