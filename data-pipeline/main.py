import argparse
import time

from daily_ingest import run_daily_pipeline
from spark_backfill import  run_backfill_pipeline
from spark_step2_embeddings import run_embeddings_pipeline
from spark_step1_metadata import run_metadata_pipeline

from config import settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "job",
        choices=["daily", "backfill", "embeddings", "metadata"],
        help="Job to execute",
    )
    args = parser.parse_args()

    if args.job == "daily":
        run_daily_pipeline()
  
    elif args.job == "backfill":
        run_backfill_pipeline(
            input_json_path=settings.raw_data_path,
            parquet_output_path=settings.datalake_path,
            dlq_path=settings.dlq_path,
        )
    elif args.job == "embeddings":
        run_embeddings_pipeline()
        
    elif args.job == "metadata":
        run_metadata_pipeline()


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    total_sec = end_time - start_time   

    print(f"⏱️[Benchmark] Finished in {total_sec:.2f} seconds ({total_sec / 60:.2f} minutes).")