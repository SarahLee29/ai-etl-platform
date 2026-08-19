import os
import sys
import time
from typing import Tuple
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

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
    print("⚙️ Initializing SparkSession...")
    
    postgres_jar_maven = "org.postgresql:postgresql:42.7.3"

    spark = (
        SparkSession.builder
        .appName("ArXiv_Historical_Backfill_ETL")        
        .config("spark.jars.packages", postgres_jar_maven)  # Dynamically load Postgres JDBC driver package
        .config("spark.driver.memory", "4g")                          # Allocate 4GB heap memory to Driver
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")  # Enable Apache Arrow acceleration for Pandas UDFs
        .config("spark.sql.shuffle.partitions", "10")                  # Reduce shuffle partitions for local dev speed        
        .master("local[*]") # Bind master to local mode using all available CPU cores
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("✅ SparkSession initialized successfully!")
    return spark

def test_step_2():
    print("--- Testing Step 2 Initialization ---")
    
    # 1. Initialize Spark Session
    spark = create_spark_session()
    
    # 2. Verify Spark Master and App Name
    print(f"App Name: {spark.sparkContext.appName}")
    print(f"Master URL: {spark.sparkContext.master}")
    
    # 3. Test Spark Engine with a Dummy DataFrame
    test_data = [("test_id_1", "ArXiv Paper Title 1"), ("test_id_2", "ArXiv Paper Title 2")]
    df = spark.createDataFrame(test_data, ["id", "title"])
    
    print("\nDummy DataFrame Output:")
    df.show()
    
    # 4. Clean up session
    spark.stop()
    print("✅ Step 2 Test Passed!")

if __name__ == "__main__":
    test_step_2()