import time

import pandas as pd
from pyspark.sql import SparkSession
from config import settings
from spark_udfs import generate_embeddings_batch, generate_embeddings_udf
from pyspark.sql import functions as F


def run_embeddings_pipeline():
    spark = (
            SparkSession.builder
            .appName("ArXiv_Step2_Embeddings_ETL")        
            .config("spark.jars.packages", settings.postgres_jar_maven)
            .config("spark.driver.memory", settings.spark_driver_memory)
            .config("spark.executor.memory", settings.spark_executor_memory)
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")  
            .config("spark.sql.execution.arrow.pyspark.maxRecordsPerBatch", str(settings.spark_embeddings_max_records_per_batch))
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "file:///tmp/spark-events") 
            .master(settings.spark_master_embeddings) 
            .getOrCreate()
        )
    try:
  
        print(f"Reading intermediate parquet from: {settings.intermediate_metadata_path}")

        df = spark.read.parquet(settings.intermediate_metadata_path)

        df_text = df.withColumn(
            "embedding_text", 
            F.concat_ws(". ", F.col("title"), F.col("abstract"), 
            F.get_json_object(F.col("metadata_json"), "$.core_method"),
            F.get_json_object(F.col("metadata_json"), "$.dataset_used"),
            F.get_json_object(F.col("metadata_json"), "$.key_findings"))
        )

        df_text = df_text.repartition(settings.spark_embeddings_repartitions)
        #spark.conf.set("spark.sql.adaptive.enabled", "true")
        #spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

        print("Computing Vector Embeddings via PySpark Pandas UDF...")
        df_embedded = df_text.withColumn(
            "embedding", 
            generate_embeddings_udf(F.col("embedding_text")) # type: ignore
        ).drop("embedding_text")

        print("Writing intermediate embeddings to Parquet...")
        (
            df_embedded.write
            .mode("overwrite")
            .option("compression", "zstd")
            .parquet(settings.intermediate_embeddings_path)
        )
    except Exception as e:
        print(f"❌ Error during embeddings pipeline: {e}")
        raise e
    finally:
        spark.stop()
        print("✅ SparkSession stopped successfully.")


def run_embeddings_pipeline_colab_safe(batch_size: int = 1024):
    """Compute embeddings with Python batches instead of a Spark Pandas UDF."""
    spark = (
        SparkSession.builder
        .appName("ArXiv_Step2_Embeddings_ETL_Colab_Safe")
        .config("spark.jars.packages", settings.postgres_jar_maven)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.executor.memory", settings.spark_executor_memory)
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///tmp/spark-events")
        .master(settings.spark_master_embeddings)
        .getOrCreate()
    )

    try:
        print(f"Reading intermediate parquet from: {settings.intermediate_metadata_path}")
        df = spark.read.parquet(settings.intermediate_metadata_path)
        metadata_columns = [
            F.get_json_object(F.col("metadata_json"), "$.core_method"),
            F.get_json_object(F.col("metadata_json"), "$.dataset_used"),
            F.get_json_object(F.col("metadata_json"), "$.key_findings"),
        ]
        text_df = df.withColumn(
            "embedding_text",
            F.concat_ws(". ", F.col("title"), F.col("abstract"), *metadata_columns),
        ).select(*df.columns, "embedding_text")
        pdf = text_df.toPandas()

        if pdf.empty:
            print("No rows to process; writing empty embeddings parquet.")
            spark.createDataFrame(pdf).write.mode("overwrite").option("compression", "zstd").parquet(
                settings.intermediate_embeddings_path
            )
            return

        total_rows = len(pdf)
        embeddings = []
        start_time = time.time()

        for start in range(0, total_rows, batch_size):
            batch_texts = pdf.iloc[start : start + batch_size]["embedding_text"].fillna("").astype(str).tolist()
            embeddings.extend(generate_embeddings_batch(batch_texts))

            processed = len(embeddings)
            elapsed = time.time() - start_time
            speed = processed / elapsed if elapsed > 0 else 0
            print(f"Progress: {processed}/{total_rows} rows | Speed: {speed:.1f} rows/s")

        pdf["embedding"] = embeddings
        pdf = pdf.drop(columns=["embedding_text"])

        print(f"Writing intermediate embeddings to: {settings.intermediate_embeddings_path}")
        spark.createDataFrame(pdf).write.mode("overwrite").option("compression", "zstd").parquet(
            settings.intermediate_embeddings_path
        )
        print("✅ Successfully finished embeddings via safe batch path!")
    except Exception as e:
        print(f"❌ Error during safe embeddings pipeline: {e}")
        raise
    finally:
        spark.stop()
        print("✅ SparkSession stopped successfully.")

if __name__ == "__main__":
  run_embeddings_pipeline()