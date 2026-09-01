import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: str = os.getenv("DB_PORT", "5432")
    db_name: str = os.getenv("DB_NAME", "")
    db_user: str = os.getenv("DB_USER", "")
    db_password: str = os.getenv("DB_PASSWORD", "")

    target_table: str = os.getenv(
        "TARGET_TABLE",
        "arxiv_documents",
    )

    rss_url: str = os.getenv(
        "ARXIV_RSS_URL",
        "https://rss.arxiv.org/rss/cs",
    )

    raw_data_path: str = os.getenv(
        "RAW_DATA_PATH",
        "/app/data/arxiv_metadata_oai_snapshot.json",
    )

    datalake_path: str = os.getenv(
        "DATALAKE_PATH",
        "/app/datalake/silver/arxiv",
    )

    intermediate_metadata_path: str = os.getenv(
        "INTERMEDIATE_METADATA_PATH",
        "/app/datalake/intermediate/arxiv_metadata",
    )

    intermediate_embeddings_path: str = os.getenv(
            "INTERMEDIATE_EMBEDDINGS_PATH",
            "/app/datalake/intermediate/arxiv_embeddings",
        )

    dlq_path: str = os.getenv(
        "DLQ_PATH",
        "/app/datalake/dlq/arxiv",
    )

    max_error_rate_daily: float = float(
        os.getenv("MAX_ERROR_RATE_DAILY", "0.10")
    )

    max_error_rate_backfill: float = float(
        os.getenv("MAX_ERROR_RATE_BACKFILL", "0.01")
    )

    spark_driver_memory: str = os.getenv(
        "SPARK_DRIVER_MEMORY",
        "4g",
    )

    spark_executor_memory: str = os.getenv(
        "SPARK_EXECUTOR_MEMORY",
        "4g",
    )

    spark_master_metadata: str = os.getenv(
        "SPARK_MASTER_METADATA",
        "local[4]",
    )

    spark_master_embeddings: str = os.getenv(
        "SPARK_MASTER_EMBEDDINGS",
        "local[8]",
    )

    spark_master_loading: str = os.getenv(
        "SPARK_MASTER_LOADING",
        "local[8]",
    )

    spark_metadata_repartitions: int = int(
        os.getenv("SPARK_METADATA_REPARTITIONS", "20")
    )

    spark_embeddings_repartitions: int = int(
        os.getenv("SPARK_EMBEDDINGS_REPARTITIONS", "100")
    )

    spark_loading_repartitions: int = int(
        os.getenv("SPARK_LOADING_REPARTITIONS", "24")
    )

    spark_metadata_max_records_per_batch: int = int(
        os.getenv("SPARK_METADATA_MAX_RECORDS_PER_BATCH", "50")
    )

    spark_embeddings_max_records_per_batch: int = int(
        os.getenv("SPARK_EMBEDDINGS_MAX_RECORDS_PER_BATCH", "200")
    )

    spark_loading_max_records_per_batch: int = int(
        os.getenv("SPARK_LOADING_MAX_RECORDS_PER_BATCH", "200")
    )

    omp_num_threads: int = int(
        os.getenv("OMP_NUM_THREADS", "2")
    )

    mkl_num_threads: int = int(
        os.getenv("MKL_NUM_THREADS", "2")
    )

    torch_num_threads: int = int(
        os.getenv("TORCH_NUM_THREADS", "2")
    )

    embedding_model_id: str = os.getenv(
        "EMBEDDING_MODEL_ID",
        "BAAI/bge-small-en-v1.5",
    )

    embedding_batch_size: int = int(
        os.getenv("EMBEDDING_BATCH_SIZE", "32")
    )

    llm_model_id: str = os.getenv(
        "LLM_MODEL_ID",
        os.getenv("LLM_MODEL_ID", "Qwen/Qwen2.5-0.5B-Instruct"),
    )

    llm_batch_size: int = int(
        os.getenv("LLM_BATCH_SIZE", "16")
    )

    llm_max_new_tokens: int = int(
        os.getenv("LLM_MAX_NEW_TOKENS", "64")
    )

    postgres_jar_maven: str = os.getenv(
        "POSTGRES_JAR_MAVEN",
        "org.postgresql:postgresql:42.7.3",
    )

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:postgresql://{self.db_host}:"
            f"{self.db_port}/{self.db_name}"
        )


settings = Settings()