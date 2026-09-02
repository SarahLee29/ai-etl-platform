import json
import time

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from tqdm import tqdm

from config import settings
from spark_udfs import extract_structured_metadata_batch, extract_structured_metadata_udf


def run_metadata_pipeline():
    spark = (
        SparkSession.builder
        .appName("ArXiv_Step1_Metadata_ETL")
        .config("spark.jars.packages", settings.postgres_jar_maven)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.executor.memory", settings.spark_executor_memory)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config(
            "spark.sql.execution.arrow.pyspark.maxRecordsPerBatch",
            str(settings.spark_metadata_max_records_per_batch),
        )
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///tmp/spark-events")
        .master(settings.spark_master_metadata)
        .getOrCreate()
    )

    try:
        print(f"Reading cleaned parquet from: {settings.datalake_path}")
        df = spark.read.parquet(settings.datalake_path)

        df = df.repartition(settings.spark_metadata_repartitions)

        print("Extracting structured metadata via LLM UDF...")
        df_structured = df.withColumn(
            "metadata_json",
            extract_structured_metadata_udf(F.col("abstract")),  # type: ignore[arg-type]
        )

        print("Writing intermediate metadata to Parquet...")
        (
            df_structured.write
            .mode("overwrite")
            .option("compression", "zstd")
            .parquet(settings.intermediate_metadata_path)
        )
    except Exception as e:
        print(f"❌ Error during metadata pipeline: {e}")
        raise
    finally:
        spark.stop()
        print("✅ SparkSession stopped successfully.")


def run_metadata_pipeline_colab_safe(batch_size: int = 256):
    """
    A non-Arrow fallback that keeps the current Spark pipeline intact but moves
    the metadata extraction itself to a pure Python batch loop. This version is
    intentionally kept alongside the original pandas_udf pipeline so both can be
    tested and compared.
    """
    spark = (
        SparkSession.builder
        .appName("ArXiv_Step1_Metadata_ETL_Colab_Safe")
        .config("spark.jars.packages", settings.postgres_jar_maven)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.executor.memory", settings.spark_executor_memory)
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///tmp/spark-events")
        .master(settings.spark_master_metadata)
        .getOrCreate()
    )

    try:
        print(f"Reading cleaned parquet from: {settings.datalake_path}")
        df = spark.read.parquet(settings.datalake_path)
        pdf = df.toPandas()

        if pdf.empty:
            print("No rows to process; writing empty metadata parquet.")
            spark.createDataFrame(pdf).write.mode("overwrite").option("compression", "zstd").parquet(
                settings.intermediate_metadata_path
            )
            return

        total_rows = len(pdf)
        print(f"Starting batch extraction for {total_rows} rows...")
        start_time = time.time()

        results = []
        processed_rows = 0

        for i in range(0, total_rows, batch_size):
            batch = pdf.iloc[i : i + batch_size]
            texts = batch["abstract"].fillna("").astype(str).tolist()

            batch_results = extract_structured_metadata_batch(texts)

            batch = batch.copy()
            batch["metadata_json"] = batch_results
            results.append(batch)

            processed_rows += len(batch)
            elapsed = time.time() - start_time
            speed = processed_rows / elapsed if elapsed > 0 else 0
            remaining_rows = total_rows - processed_rows
            eta_seconds = remaining_rows / speed if speed > 0 else 0

            print(
                f"Progress: {processed_rows}/{total_rows} rows "
                f"({processed_rows / total_rows * 100:.1f}%) | "
                f"Speed: {speed:.1f} rows/s | ETA: {eta_seconds / 60:.1f} minutes remaining"
            )

        output_pdf = pd.concat(results, ignore_index=True) if results else pdf.copy()
        output_pdf["metadata_json"] = output_pdf["metadata_json"].fillna(
            json.dumps(
                {
                    "core_method": "Not specified",
                    "dataset_used": "Not specified",
                    "key_findings": "Not specified",
                },
                ensure_ascii=False,
            )
        )

        print(f"Writing intermediate metadata to: {settings.intermediate_metadata_path}")
        spark.createDataFrame(output_pdf).write.mode("overwrite").option("compression", "zstd").parquet(
            settings.intermediate_metadata_path
        )
        print("✅ Successfully finished metadata extraction via safe batch path!")
    except Exception as e:
        print(f"❌ Error during safe metadata pipeline: {e}")
        raise
    finally:
        spark.stop()
        print("✅ SparkSession stopped successfully.")


if __name__ == "__main__":
    run_metadata_pipeline()