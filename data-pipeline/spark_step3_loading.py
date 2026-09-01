from spark_udfs import load_to_postgres_with_vectors 
from pyspark.sql import SparkSession
from config import settings

def load_to_postgres():     
    spark = (
                SparkSession.builder
                .appName("ArXiv_Step3_Load_ETL")        
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
        print("Loading embeddings into PostgreSQL...")
        # Bulk ingest into PostgreSQL with pgvector schema support
        pg_config = {
            "url": settings.jdbc_url,
            "table": settings.target_table,
            "properties": {
                "user": settings.db_user,
                "password": settings.db_password,
            },
        }
        df = spark.read.parquet(settings.intermediate_embeddings_path)
        load_to_postgres_with_vectors(
            df=df,
            pg_url=pg_config["url"],
            pg_table=pg_config["table"],
            pg_properties=pg_config["properties"],
            num_partitions=settings.spark_partitions
        )

    except Exception as e:
            print(f"❌ Error during loading pipeline: {e}")
            raise e
    finally:
        spark.stop()
        print("✅ SparkSession stopped successfully.")
    