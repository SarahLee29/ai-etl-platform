import os
import sys
import psycopg2
from psycopg2 import sql
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType, StringType
from pyspark.sql.functions import pandas_udf
import torch
from config import settings

os.environ["OMP_NUM_THREADS"] = "2"
os.environ["MKL_NUM_THREADS"] = "2"
torch.set_num_threads(2)

_MODEL_CACHE = None

def get_model():
    global _MODEL_CACHE
    if _MODEL_CACHE is None:
        from sentence_transformers import SentenceTransformer
   
        _MODEL_CACHE = SentenceTransformer('BAAI/bge-small-en-v1.5', device="cpu")
    return _MODEL_CACHE


@pandas_udf(returnType=ArrayType(FloatType())) # type: ignore
def generate_embeddings_udf(text_series: pd.Series) -> pd.Series:
    """
    Generates embeddings for a series of text.
    Executes batched text vectorization across PySpark Worker nodes using HuggingFace models.
    Leverages Arrow zero-copy memory transfers and batch Matrix operations.
    """  
    model = get_model()
    
    text_list = text_series.fillna("").tolist()
  
    embeddings = model.encode(
        text_list, 
        batch_size=32, 
        show_progress_bar=False, 
        normalize_embeddings=True
    )
    
    return pd.Series(embeddings.tolist())


def load_to_postgres_with_vectors(
    df: DataFrame, 
    pg_url: str, 
    pg_table: str, 
    pg_properties: dict,
    num_partitions: int = 8
):
    """
    Bulk loads DataFrame into PostgreSQL using Spark JDBC with a Staging Table pattern,
    followed by an atomic UPSERT operation casting string representations to pgvector type.
    """
    if df.isEmpty():
        print("[Load] No valid records to write to PostgreSQL.")
        return

    stage_table = f"{pg_table}_vector_stage"
    
    print(f"[Load] Preparing records for staging table '{stage_table}'...")

    # Format vector array into pgvector compatible string format: '[0.1, 0.2, ...]'
    formatted_df = df.withColumn(
        "embedding_str", 
        F.concat(F.lit("["), F.array_join(F.col("embedding"), ","), F.lit("]"))
    )

    staged_df = (
        formatted_df.select(
            "paper_id",
            "title",
            "abstract",
            "authors",
            "categories",
            "url",
            "published_date",
            "published_year",
            "ingested_at",
            "embedding_str"
        )
        .dropDuplicates(["paper_id"])
        .repartition(num_partitions)
    )

    print(f"[Load] JDBC bulk writing to staging table '{stage_table}'...")

    try:
        print("=== Spark Physical Execution Plan ===")
        staged_df.explain(True)
        print("=====================================")

        (
            staged_df.write
            .format("jdbc")
            .option("url", pg_url)
            .option("dbtable", stage_table)
            .option("user", pg_properties["user"])
            .option("password", pg_properties["password"])
            .option("driver", "org.postgresql.Driver")
            .option("reWriteBatchedInserts", "true")
            .option("batchsize", "200")
            .option("stringtype", "unspecified")  
            .option("isolationLevel", "READ_COMMITTED")
            .mode("append")
            .save()
        ) 
        print(f"✅ [Load] Staging table '{stage_table}' created & populated successfully!")

    except Exception as e:
        print(f"❌ [Load] JDBC Ingestion failed: {str(e)}")
        raise e

    # Merge Staging data into Target Table with explicit Vector Type Cast
    print(f"[Load] Merging & Upserting to final table '{pg_table}' with vector cast...")

    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            database=settings.db_name,
            user=pg_properties["user"],
            password=pg_properties["password"],
        )
        cursor = conn.cursor()

        # SQL Upsert query: Casts embedding_str to ::vector type on insertion
        merge_query = sql.SQL(
            """
            INSERT INTO {target} (
                paper_id,
                title,
                abstract,
                authors,
                categories,
                url,
                published_date,
                published_year,
                ingested_at,
                embedding
            )
            SELECT
                paper_id,
                title,
                abstract,
                authors,
                categories,
                url,
                published_date,
                published_year,
                ingested_at,
                embedding_str::vector AS embedding
            FROM {stage}
            ON CONFLICT (paper_id) DO UPDATE SET
                title = EXCLUDED.title,
                abstract = EXCLUDED.abstract,
                authors = EXCLUDED.authors,
                categories = EXCLUDED.categories,
                url = EXCLUDED.url,
                published_date = EXCLUDED.published_date,
                published_year = EXCLUDED.published_year,
                ingested_at = EXCLUDED.ingested_at,
                embedding = EXCLUDED.embedding;
            """
        ).format(
            target=sql.Identifier(pg_table),
            stage=sql.Identifier(stage_table),
        )

        cursor.execute(merge_query)
        conn.commit()

        print(f"✅ [Load] Successfully upserted records with vectors into '{pg_table}'!")

        # Drop temporary staging table after successful transaction
        cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {stage};").format(
                stage=sql.Identifier(stage_table),
            )
        )
        conn.commit()

    except Exception as error:
        if conn is not None:
            conn.rollback()
        print(f"❌ [Load] PostgreSQL vector upsert failed: {error}")
        raise

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
            print("PostgreSQL Connection Closed.")

def run_embeddings_pipeline():
    """
    Executes The Vector Embedding Pipeline: Reads Silver Parquet Lake, executes Pandas UDF vectorization,
    and bulk writes to PostgreSQL pgvector.
    """
    spark = (
        SparkSession.builder
        .appName("ArXiv_Vector_Embedding_Pipeline")
        .config("spark.jars.packages", settings.postgres_jar_maven)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.executor.memory", settings.spark_executor_memory)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///tmp/spark-events")
        .master("local[8]")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") 
    #spark.conf.set("spark.sql.adaptive.enabled", "true")
    #spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    try:
        # Load cleaned data from Silver Parquet Data Lake
        print(f"Reading cleaned parquet from: {settings.datalake_path}")
        df = spark.read.parquet(settings.datalake_path)

        # Construct input string for embeddings by combining Title and Abstract
        df_text = df.withColumn(
            "embedding_text", 
            F.concat_ws(". ", F.col("title"), F.col("abstract"))
        )

        df_text = df_text.repartition(200)
        print(f"Current partitions: {df_text.rdd.getNumPartitions()}")

        # Trigger distributed Pandas UDF vector computation
        print("Computing Vector Embeddings via PySpark Pandas UDF...")
        df_embedded = df_text.withColumn(
            "embedding", 
            generate_embeddings_udf(F.col("embedding_text")) # type: ignore
        ).drop("embedding_text")

        # Bulk ingest into PostgreSQL with pgvector schema support
        pg_config = {
            "url": settings.jdbc_url,
            "table": settings.target_table,
            "properties": {
                "user": settings.db_user,
                "password": settings.db_password,
            },
        }

        load_to_postgres_with_vectors(
            df=df_embedded,
            pg_url=pg_config["url"],
            pg_table=pg_config["table"],
            pg_properties=pg_config["properties"],
            num_partitions=settings.spark_partitions
        )

    except Exception as e:
        print(f"❌ Vector Embedding Pipeline Failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()
        print("Spark Session Terminated.")

if __name__ == "__main__":
    run_embeddings_pipeline()