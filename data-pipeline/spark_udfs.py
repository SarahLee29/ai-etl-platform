import json
import os
import sys
from xml.parsers.expat import model
import psycopg2
from psycopg2 import sql
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType, StringType
from pyspark.sql.functions import pandas_udf
import torch
from config import settings

os.environ["OMP_NUM_THREADS"] = str(settings.omp_num_threads)
os.environ["MKL_NUM_THREADS"] = str(settings.mkl_num_threads)
torch.set_num_threads(settings.torch_num_threads)

_EMBEDDING_MODEL_CACHE = None


def get_torch_device() -> str:
    return "cuda" if torch.cuda.is_available() else "cpu"


def get_embedding_model():
    global _EMBEDDING_MODEL_CACHE
    if _EMBEDDING_MODEL_CACHE is None:
        from sentence_transformers import SentenceTransformer

        device = get_torch_device()
        print(f"[Embedding] Loading sentence-transformers model [{settings.embedding_model_id}] on device: {device}")
        _EMBEDDING_MODEL_CACHE = SentenceTransformer(settings.embedding_model_id, device=device)
    return _EMBEDDING_MODEL_CACHE

@pandas_udf(returnType=ArrayType(FloatType())) # type: ignore
def generate_embeddings_udf(text_series: pd.Series) -> pd.Series:
    """
    Generates embeddings for a series of text.
    Executes batched text vectorization across PySpark Worker nodes using HuggingFace models.
    Leverages Arrow zero-copy memory transfers and batch Matrix operations.
    """  
    model = get_embedding_model()
    
    text_list = text_series.fillna("").tolist()
  
    embeddings = model.encode(
        text_list,
        batch_size=settings.embedding_batch_size,
        show_progress_bar=False,
        normalize_embeddings=True,
    )
    
    return pd.Series(embeddings.tolist())


def generate_embeddings_batch(texts: list) -> list:
    """Generate normalized embeddings for a regular Python list of texts."""
    if not texts:
        return []

    model = get_embedding_model()
    text_list = [str(text) if text is not None else "" for text in texts]
    embeddings = model.encode(
        text_list,
        batch_size=settings.embedding_batch_size,
        show_progress_bar=False,
        normalize_embeddings=True,
    )
    return embeddings.tolist()

_LLM_PIPELINE_CACHE = None


def parse_metadata_response(response_content: str) -> dict:
    """Parse or recover the three metadata fields from a model response."""
    import re

    response = response_content.replace("```json", "").replace("```", "").strip()
    first_brace = response.find("{")
    candidate = response[first_brace:] if first_brace >= 0 else response
    candidate = re.sub(r"^\{\s*,\s*", "{", candidate, count=1)

    json_match = re.search(r"\{.*\}", candidate, re.DOTALL)
    complete_candidate = json_match.group(0) if json_match else candidate

    try:
        parsed = json.loads(complete_candidate)
        if not isinstance(parsed, dict):
            raise TypeError("Metadata response must be a JSON object")
        return {
            "core_method": str(parsed.get("core_method", "Not specified")),
            "dataset_used": str(parsed.get("dataset_used", "Not specified")),
            "key_findings": str(parsed.get("key_findings", "Not specified")),
        }
    except (json.JSONDecodeError, AttributeError, TypeError):
        recovered = {}
        for field in ("core_method", "dataset_used", "key_findings"):
            match = re.search(
                rf'"{field}"\s*:\s*"(?P<value>.*?)(?:"\s*[,}}]|$)',
                response,
                re.DOTALL,
            )
            if match:
                recovered[field] = match.group("value").strip()

        if not recovered:
            raise ValueError("Response is not valid JSON and no fields could be recovered")

        return {
            "core_method": recovered.get("core_method", "Not specified"),
            "dataset_used": recovered.get("dataset_used", "Not specified"),
            "key_findings": recovered.get("key_findings", "Not specified"),
        }

def get_llm_pipeline():
    """
    Lazily loads a local instruction-tuned LLM in each Spark Worker process.
    """
    global _LLM_PIPELINE_CACHE, _LLM_MODEL_CACHE, _LLM_TOKENIZER_CACHE
    if _LLM_PIPELINE_CACHE is None:
        from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline

        device = get_torch_device()
        model_id = settings.llm_model_id
        print(f"[LLM] Loading local extraction model [{model_id}] on device: {device}")

        tokenizer = AutoTokenizer.from_pretrained(model_id)
        model = AutoModelForCausalLM.from_pretrained(
            model_id,
            dtype=torch.float16 if device == "cuda" else torch.float32,
            device_map="auto" if device == "cuda" else "cpu",
        )

        _LLM_PIPELINE_CACHE = pipeline(
            "text-generation",
            model=model,
            tokenizer=tokenizer,
            device=0 if device == "cuda" else -1,
        )
    return _LLM_PIPELINE_CACHE

@pandas_udf(returnType=StringType()) # type: ignore
def extract_structured_metadata_udf(abstract_series: pd.Series) -> pd.Series:
    """
    Performs open-ended structured extraction on paper abstracts using a local Hugging Face Pipeline.
    """
    generator = get_llm_pipeline()
    if generator.tokenizer is None:
        raise RuntimeError("Tokenizer failed to initialize.")
    
    # Convert Pandas Series to a list for efficient processing
    texts = abstract_series.fillna("").tolist()

    results = [json.dumps({
            "core_method": "Not specified",
            "dataset_used": "Not specified",
            "key_findings": "Not specified"
        }) for _ in texts]
    
    # Construct prompt lists for batch inputs
    prompts = []
    
    for idx, text in enumerate(texts):
        if not text.strip():
            continue
            
        system_prompt = (
            "Return exactly one valid JSON object and nothing else. "
            "Do not write an explanation, prefix, suffix, or markdown code fence. "
            "The object must contain exactly these three keys: "
            "core_method, dataset_used, key_findings. "
            "Every value must be a short plain string. Use 'Not specified' when the abstract "
            "does not provide the information. "
            "core_method: name the proposed algorithm or model in at most 12 words; "
            "dataset_used: name the dataset or benchmark in at most 12 words; "
            "key_findings: state the main conclusion in at most 20 words. "
            "Read the abstract only to extract facts for these fields. "
            'Valid example: {"core_method":"Transformer model","dataset_used":"MNIST","key_findings":"The model improves classification accuracy."} '
            "Your entire response must start with { and end with }."
        )
        
        # Format messages matching Qwen / Llama Chat Template standard
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Abstract: {text}"}
        ]

        
        formatted_prompt = generator.tokenizer.apply_chat_template(
            messages, 
            tokenize=False, 
            add_generation_prompt=True
        )
        prompts.append((idx, formatted_prompt))

    if prompts:
        indices, batch_prompts = zip(*prompts)
        
        try:
            # Batch call local model generation (pipeline supports passing a list for batch inference)
            outputs = generator(
                list(batch_prompts),
                batch_size=settings.llm_batch_size,
                max_new_tokens=settings.llm_max_new_tokens,
                do_sample=False,
                pad_token_id=generator.tokenizer.eos_token_id,
                return_full_text=False,
            )
            
            for idx, output in zip(indices, outputs):
                try:
                    generated_text = output[0]["generated_text"]        
                    response_content = generated_text.strip()

                    parsed = parse_metadata_response(response_content)
                    results[idx] = json.dumps({
                        "core_method": parsed["core_method"],
                        "dataset_used": parsed["dataset_used"],
                        "key_findings": parsed["key_findings"]
                    }, ensure_ascii=False)
                    
                except Exception as error:
                    print(
                        f"⚠️ Metadata parse warning for row {idx}: {error}; "
                        f"raw output={generated_text[:300]!r}"
                    )
                    results[idx] = json.dumps({
                        "core_method": "Extraction Error",
                        "dataset_used": "Extraction Error",
                        "key_findings": "Extraction Error",
                    }, ensure_ascii=False)
                
        except Exception as e:
            # Fault tolerance fallback: Assign default structure if parsing fails for a batch to prevent task failure
            print(f"⚠️ LLM Extraction parsing warning: {str(e)}")
            for idx in indices:
                results[idx] = json.dumps({
                    "core_method": "Extraction Error",
                    "dataset_used": "Extraction Error",
                    "key_findings": "Extraction Error"
                }, ensure_ascii=False)

    return pd.Series(results, index=abstract_series.index)

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
            "embedding_str",
            "metadata_json"
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
                embedding,
                metadata
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
                embedding_str::vector AS embedding,
                metadata_json::jsonb AS metadata
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
                embedding = EXCLUDED.embedding,
                metadata = EXCLUDED.metadata;
            """
        ).format(
            target=sql.Identifier(pg_table),
            stage=sql.Identifier(stage_table),
        )

        cursor.execute(merge_query)
        conn.commit()

        print(f"✅ [Load] Successfully upserted records with vectors into '{pg_table}'!")
        
        print("[Load] Creating indexes on target table after bulk load...")
        
        index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_published_year ON arxiv_documents(published_year);",
            "CREATE INDEX IF NOT EXISTS idx_categories ON arxiv_documents(categories);",
            
            """
            CREATE INDEX IF NOT EXISTS idx_arxiv_fts ON arxiv_documents 
            USING gin (
                (setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
                 setweight(to_tsvector('english', coalesce(abstract, '')), 'B'))
            );
            """,
            
            """
            CREATE INDEX IF NOT EXISTS idx_arxiv_embedding_hnsw ON arxiv_documents 
            USING hnsw (embedding vector_cosine_ops) 
            WITH (m = 16, ef_construction = 64);
            """
        ]

        for idx_sql in index_queries:
            cursor.execute(idx_sql)
        
        conn.commit()
        print("✅ [Load] All indexes (including GIN and HNSW) created successfully!")

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


def extract_structured_metadata_batch(texts: list) -> list:
    """
    Performs open-ended structured extraction on a batch of abstracts using Hugging Face Pipeline.
    """
    generator = get_llm_pipeline()
    tokenizer = generator.tokenizer

    if tokenizer is None:
        raise RuntimeError("Tokenizer failed to initialize.")

    results = [json.dumps({
        "core_method": "Not specified",
        "dataset_used": "Not specified",
        "key_findings": "Not specified"
    }) for _ in texts]
    prompts = []

    for idx, text in enumerate(texts):
        text_str = str(text) if text is not None else ""
        if not text_str.strip():
            continue

        system_prompt = (
            "Return exactly one valid JSON object and nothing else. "
            "Do not write an explanation, prefix, suffix, or markdown code fence. "
            "The object must contain exactly these three keys: "
            "core_method, dataset_used, key_findings. "
            "Every value must be a short plain string. Use 'Not specified' when the abstract "
            "does not provide the information. "
            "core_method: name the proposed algorithm or model in at most 12 words; "
            "dataset_used: name the dataset or benchmark in at most 12 words; "
            "key_findings: state the main conclusion in at most 20 words. "
            "Read the abstract only to extract facts for these fields. "
            'Valid example: {"core_method":"Transformer model","dataset_used":"MNIST","key_findings":"The model improves classification accuracy."} '
            "Your entire response must start with { and end with }."
        )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Abstract: {text_str}"},
        ]

        formatted_prompt = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )
        prompts.append((idx, formatted_prompt))

    if prompts:
        indices, batch_prompts = zip(*prompts)
        
        try:
            # Batch call local model generation (pipeline supports passing a list for batch inference)
            outputs = generator(
                list(batch_prompts),
                batch_size=settings.llm_batch_size,
                max_new_tokens=settings.llm_max_new_tokens,
                do_sample=False,
                pad_token_id=tokenizer.eos_token_id,
                return_full_text=False,
            )
            
            for idx, output, prompt_str in zip(indices, outputs, batch_prompts):
                try:
                    # Extract text generated by the model
                    generated_text = output[0]["generated_text"]
                    response_content = generated_text.strip()
                    
                    parsed = parse_metadata_response(response_content)
                    results[idx] = json.dumps({
                        "core_method": parsed["core_method"],
                        "dataset_used": parsed["dataset_used"],
                        "key_findings": parsed["key_findings"]
                    }, ensure_ascii=False)
                    
                except Exception as error:
                    print(
                        f"⚠️ Metadata parse warning for row {idx}: {error}; "
                        f"raw output={generated_text[:300]!r}"
                    )
                    results[idx] = json.dumps({
                        "core_method": "Extraction Error",
                        "dataset_used": "Extraction Error",
                        "key_findings": "Extraction Error"
                    }, ensure_ascii=False)
                
        except Exception as e:
            print(f"⚠️ Batch Generation Error: {str(e)}")
            for idx, _ in prompts:
                results[idx] = json.dumps({
                    "core_method": "Extraction Error",
                    "dataset_used": "Extraction Error",
                    "key_findings": "Extraction Error"
                }, ensure_ascii=False)

    return results