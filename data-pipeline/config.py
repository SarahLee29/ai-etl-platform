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

    spark_partitions: int = int(
        os.getenv("SPARK_PARTITIONS", "4")
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