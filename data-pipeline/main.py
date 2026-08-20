import argparse

from daily_ingest import run_daily_pipeline
from spark_backfill import  run_backfill_pipeline
from config import settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "job",
        choices=["daily", "backfill"],
        help="Pipeline job to execute, input 'daily' or 'backfill'",
    )
    args = parser.parse_args()

    if args.job == "daily":
        run_daily_pipeline()
        return
    run_backfill_pipeline(
        input_json_path=settings.raw_data_path,
        parquet_output_path=settings.datalake_path,
        dlq_path=settings.dlq_path,
        pg_config={
            "url": settings.jdbc_url,
            "table": settings.target_table,
            "properties": {
                "user": settings.db_user,
                "password": settings.db_password,
            },
        },
    )


if __name__ == "__main__":
    main()