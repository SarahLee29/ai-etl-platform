import pandas as pd
from pyspark.sql import SparkSession
from tqdm import tqdm
from config import settings
from spark_udfs import extract_structured_metadata_udf, extract_structured_metadata_batch
from pyspark.sql import functions as F


def run_metadata_pipeline():
  spark = (
        SparkSession.builder
        .appName("ArXiv_Step1_Metadata_ETL")        
        .config("spark.jars.packages", settings.postgres_jar_maven)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.executor.memory", settings.spark_executor_memory)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")  
        .config("spark.sql.execution.arrow.pyspark.maxRecordsPerBatch", str(settings.spark_metadata_max_records_per_batch))
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
        extract_structured_metadata_udf(F.col("abstract"))# type: ignore
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
      raise e
  finally:
      spark.stop()
      print("✅ SparkSession stopped successfully.")

  '''print(f"Reading cleaned parquet from: {settings.datalake_path}")
  df = pd.read_parquet(settings.datalake_path)
  
  total_rows = len(df)
  batch_size = 16 
  all_metadata = []

  print(f"Starting pure Pandas metadata extraction for {total_rows} rows...")
  
  texts = df["abstract"].tolist()

  for i in tqdm(range(0, total_rows, batch_size), desc="Extracting Metadata"):
      batch_texts = texts[i : i + batch_size]
      batch_results = extract_structured_metadata_batch(batch_texts)
      all_metadata.extend(batch_results)

  df["metadata_json"] = all_metadata

  print(f"Writing intermediate metadata to: {settings.intermediate_metadata_path}")
  df.to_parquet(settings.intermediate_metadata_path, compression="zstd")
  print("✅ Successfully finished metadata extraction via Pandas!")'''


if __name__ == "__main__":
  run_metadata_pipeline()