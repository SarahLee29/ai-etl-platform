import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from config import settings

def create_spark_session() -> SparkSession:
    """
    Builds and configures a SparkSession, automatically fetching and attaching
    the PostgreSQL JDBC driver and setting JVM memory parameters.
    """
    os.makedirs("/tmp/spark-events", exist_ok=True)
    print("Initializing SparkSession...")

    spark = (
        SparkSession.builder
        .appName("ArXiv_Historical_Backfill_ETL")        
        .config("spark.jars.packages", settings.postgres_jar_maven)  # Dynamically load Postgres JDBC driver package
        .config(
            "spark.driver.memory",
            settings.spark_driver_memory,
        )
        .config(
            "spark.executor.memory",
            settings.spark_executor_memory,
        )
        # Enable PyArrow for efficient column-level operations and JVM-Python serialization
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")  
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///tmp/spark-events")   
        .master("local[*]") # Bind master to local mode using all available CPU cores
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("✅ SparkSession initialized successfully!")
    return spark

def define_arxiv_schema() -> StructType:
    """
    Defines explicit Schema for Kaggle ArXiv JSONL dataset.
    Bypasses costly Schema Inference scans over 5.7GB data.
    """
    return StructType([
        StructField("id", StringType(), True),
        StructField("submitter", StringType(), True),
        StructField("authors", StringType(), True),
        StructField("title", StringType(), True),
        StructField("comments", StringType(), True),
        StructField("journal-ref", StringType(), True),
        StructField("doi", StringType(), True),
        StructField("report-no", StringType(), True),
        StructField("categories", StringType(), True),
        StructField("license", StringType(), True),
        StructField("abstract", StringType(), True),
        StructField("update_date", StringType(), True),
        # Complex nested array: [["Surname", "First name", "Suffix"]]
        StructField("authors_parsed", ArrayType(ArrayType(StringType())), True)
    ])

def extract_historical_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Reads bulk historical ArXiv dataset (JSON or Parquet format) into a PySpark DataFrame.
    Defines explicit schema hints to prevent costly schema inference overhead.
    """
    print(f"Extracting historical data from source: {file_path}")

    schema = define_arxiv_schema()
    try:
        df = spark.read.schema(schema).json(file_path)
        print(f"✅ Data extracted successfully! Initial record count: {df.count()}")
        return df

    except Exception as e:
        print(f"❌ Failed to extract data from {file_path}. Error: {str(e)}")
        raise e

def transform_data(df: DataFrame) -> DataFrame:
    """
    Cleans LaTeX linebreaks, builds deterministic URLs, extracts structured authors,
    and formats timestamps for downstream DB ingestion.
    """
    print("[Transform] Executing columnar text cleansing...")

    multiline_whitespace_pattern = r"[\r\n\t]+"
    consecutive_spaces_pattern = r"\s+"
    latex_math_pattern = r"\$.*?\$"                  # $...$
    latex_commands_pattern = r"\\[a-zA-Z]+\{[^}]*\}" # \cite{...}, \ref{...} 
    latex_symbols_pattern = r"\\[a-zA-Z]+"           # \alpha, \textbf 
    control_chars_pattern = r"[\x00-\x1F\x7F]"

    transformed_df = (
        df
        .withColumnRenamed("id", "paper_id")
        .withColumn("url", F.concat(F.lit("https://arxiv.org/abs/"), F.col("paper_id")))
        .withColumn("title", F.regexp_replace(F.col("title"), multiline_whitespace_pattern, " "))
        .withColumn("title", F.regexp_replace(F.col("title"), consecutive_spaces_pattern, " "))
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("abstract", F.regexp_replace(F.col("abstract"), multiline_whitespace_pattern, " "))
        .withColumn("abstract", F.regexp_replace(F.col("abstract"), latex_math_pattern, " [MATH] ")) 
        .withColumn("abstract", F.regexp_replace(F.col("abstract"), latex_commands_pattern, ""))
        .withColumn("abstract", F.regexp_replace(F.col("abstract"), latex_symbols_pattern, ""))
        .withColumn("abstract", F.regexp_replace(F.col("abstract"), control_chars_pattern, ""))
        .withColumn("abstract", F.regexp_replace(F.col("abstract"), consecutive_spaces_pattern, " "))
        .withColumn("abstract", F.trim(F.col("abstract")))
        .withColumn("categories", F.regexp_replace(F.trim(F.col("categories")), consecutive_spaces_pattern, ", "))        
        .withColumn(
            "clean_authors", F.expr("array_join(transform(authors_parsed, x -> trim(concat(coalesce(x[1], ''), ' ', coalesce(x[0], '')))), ', ')")
        )
        .withColumn("published_date", F.to_date(F.col("update_date"), "yyyy-MM-dd"))
        .withColumn("published_year", F.year(F.col("published_date")))
        .withColumn("ingested_at", F.current_timestamp())

        .select(
            F.col("paper_id"),
            F.col("title"),
            F.col("abstract"),
            F.coalesce(F.col("clean_authors"), F.col("authors")).alias("authors"),
            F.col("categories"),
            F.col("url"),
            F.col("published_date"),
            F.col("published_year"),
            F.col("ingested_at")
        )
    )

    return transformed_df    

def apply_sla_gate_and_circuit_breaker(df: DataFrame, dlq_output_path: str) -> DataFrame:
    """
    Validates data quality SLAs. Routes invalid records to Dead-Letter Queue (DLQ).
    Triggers Circuit Breaker if corruption rate exceeds the allowed threshold.
    """
    print("[SLA Gate] Evaluating record quality SLAs...")

    # Define SLA conditions for valid records
    valid_condition = (
        F.col("paper_id").isNotNull() &
        F.col("title").isNotNull() & (F.length(F.col("title")) > 0) &
        F.col("abstract").isNotNull() & (F.length(F.col("abstract")) >= 20)
    )

    # Split dataset into Valid Stream and Invalid Stream
    valid_df = df.filter(valid_condition).cache()
    invalid_df = df.filter(~valid_condition).cache()

    total_count = df.count()
    invalid_count = invalid_df.count()
    valid_count = valid_df.count()

    error_rate = (invalid_count / total_count) if total_count > 0 else 0.0

    print(f"[SLA Gate Metrics] Total: {total_count} | Valid: {valid_count} | Bad: {invalid_count} | Error Rate: {error_rate:.2%}")

    # Process Dead-Letter Queue if bad records exist
    if invalid_count > 0:
        print(f"[DLQ] Routing {invalid_count} invalid records to Dead-Letter Queue at {dlq_output_path}")
        (
            invalid_df.write
            .mode("append")
            .parquet(dlq_output_path)
        )

    # Circuit Breaker Verification
    if error_rate > settings.max_error_rate_backfill:
        error_msg = f"[Circuit Breaker Triggered] SLA Violation! Error rate {error_rate:.2%} exceeds threshold ({settings.max_error_rate_backfill}). Pipeline terminated."
        print(error_msg)
        raise ValueError(error_msg)

    print("✅ [SLA Gate] SLA checks passed successfully!")
    return valid_df

def load_to_data_lake(df: DataFrame, output_path: str):
    """
    Writes clean records to Parquet Data Lake, partitioned by year for optimal query pruning.
    """
    print(f"[Load] Persisting clean dataset to Parquet Data Lake at: {output_path}")

    (
        df.write
        .mode("overwrite")
        .partitionBy("published_year")
        .option("compression", "zstd")
        .parquet(output_path)
    )
    print("✅ [Load] Data successfully written to Partitioned Parquet Lake!")


def run_backfill_pipeline(
    input_json_path: str, 
    parquet_output_path: str, 
    dlq_path: str,
):
    """
    Executes The Backfill Pipeline: Extract Raw JSONL, Cleansing, SLA Validation, and Parquet Lake Persistence.
    """

    spark = create_spark_session()

    try:
        raw_df = extract_historical_data(spark, input_json_path)

        transformed_df = transform_data(raw_df)

        clean_df = apply_sla_gate_and_circuit_breaker(
            transformed_df, 
            dlq_output_path=dlq_path
        )

        load_to_data_lake(clean_df, parquet_output_path)

    except Exception as e:
        print(f"❌ Pipeline Execution Failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()
        print("Spark Session Terminated.")

if __name__ == "__main__":
    input_json_path = settings.raw_data_path
    parquet_output_path = settings.datalake_path
    dlq_path = settings.dlq_path

    run_backfill_pipeline(
        input_json_path=input_json_path,
        parquet_output_path=parquet_output_path,
        dlq_path=dlq_path
    )