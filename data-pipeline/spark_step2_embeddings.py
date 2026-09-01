from pyspark.sql import SparkSession
from config import settings
from spark_udfs import generate_embeddings_udf, load_to_postgres_with_vectors
from pyspark.sql import functions as F


def run_embeddings_pipeline():
    spark = (
            SparkSession.builder
            .appName("ArXiv_Step2_Embeddings_ETL")        
            .config("spark.jars.packages", settings.postgres_jar_maven)
            .config("spark.driver.memory", settings.spark_driver_memory)
            .config("spark.executor.memory", settings.spark_executor_memory)
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")  
            .config("spark.sql.execution.arrow.pyspark.maxRecordsPerBatch", "200")
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "file:///tmp/spark-events") 
            .master("local[8]") 
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

        df_text = df_text.repartition(100)
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

if __name__ == "__main__":
  run_embeddings_pipeline()