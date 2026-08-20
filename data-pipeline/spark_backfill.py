import os
import sys
import time
from typing import Tuple
from dotenv import load_dotenv
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") 

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_DRIVER = "org.postgresql.Driver"
TARGET_TABLE = "history_articles"

def create_spark_session() -> SparkSession:
    """
    Builds and configures a SparkSession, automatically fetching and attaching
    the PostgreSQL JDBC driver and setting JVM memory parameters.
    """
    print("Initializing SparkSession...")
    
    postgres_jar_maven = "org.postgresql:postgresql:42.7.3"

    spark = (
        SparkSession.builder
        .appName("ArXiv_Historical_Backfill_ETL")        
        .config("spark.jars.packages", postgres_jar_maven)  # Dynamically load Postgres JDBC driver package
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        # Enable PyArrow for efficient column-level operations and JVM-Python serialization
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")     
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
        df = spark.read.schema(schema).option("multiline", "true").json(file_path)
        print(f"✅ Data extracted successfully! Initial record count: {df.count()}")
        df.show()
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

    transformed_df = (
        df
        .withColumnRenamed("id", "paper_id")
        .withColumn("url", F.concat(F.lit("https://arxiv.org/abs/"), F.col("paper_id")))
        .withColumn("title", F.regexp_replace(F.col("title"), multiline_whitespace_pattern, " "))
        .withColumn("title", F.regexp_replace(F.col("title"), consecutive_spaces_pattern, " "))
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("abstract", F.regexp_replace(F.col("abstract"), multiline_whitespace_pattern, " "))
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



if __name__ == "__main__":
    spark = create_spark_session()

    raw_df = extract_historical_data(spark, "/app/data/part_of_arxiv_history.json")
    raw_output_path = "/app/data/raw_preview"
    raw_df.coalesce(1).write.mode("overwrite").json(raw_output_path)

    transformed_df = transform_data(raw_df)
    transformed_output_path = "/app/data/transformed_preview"
    transformed_df.coalesce(1).write.mode("overwrite").json(transformed_output_path)