# ArXiv Data Insights: Automated ETL & RAG Platform

A containerized, production-ready data pipeline and retrieval system that extracts artificial intelligence research papers from ArXiv, enforces an upstream data service-level agreements (SLAs), and exposes an advanced RAG (Retrieval-Augmented Generation) query interface featuring multi-model evaluation and deterministic hybrid search.

---

## 🏗️ System Architecture & Engineering Highlights

The application is split into two decoupled layers coordinated via Docker Compose inside an isolated bridge network:
1. **The ETL Pipeline (`data-pipeline`)**: A batch-processing engine powered by **Apache Spark** that extracts XML/RSS feeds, transforms raw typography, runs a fault-tolerant quality audit, and performs atomic upserts into PostgreSQL.
2. **The RAG Query Service (`backend`)**: A **FastAPI** application that executes production-grade hybrid retrieval and routes contextual records dynamically to LLM endpoints for synthesized answers.

### 🌟 Key Engineering Features
* **1. Batch ETL & Data Quality Gates (SLA):** Distributed processing with automated error-rate thresholds and dead-letter queue (DLQ) partitioning for high data reliability.
* **2. Distributed Vector & Metadata Extraction Pipeline:** Leverages PySpark Pandas UDFs to perform parallel, offline extraction of structured metadata (core methods, datasets used, key findings) using lightweight local LLMs, shifting complexity upstream to eliminate online hallucination risks.
* **3. Rich Text Embedding & Hybrid Storage:** Fuses unstructured abstracts with structured metadata into high-dimensional vector spaces (`BAAI/bge-small-en-v1.5`), backed by **PostgreSQL & `pgvector`** utilizing **HNSW** and **GIN** indexes.
* **4. Production Hybrid Search with RRF:** Combines Dense Vector Search (semantic similarity) and Sparse Keyword Search (BM25 full-text match via PostgreSQL expressions) fused via **Reciprocal Rank Fusion (RRF)** for high-precision information retrieval.

---

## 🛠️ Tech Stack

* **Language:** Python 3.11+
* **Distributed Processing:** Apache Spark 3.5+
* **Database:** PostgreSQL 16+ with `pgvector` extension
* **API Framework:** FastAPI, Uvicorn
* **Orchestration:** Docker, Docker Compose
* **Client Packages:** Psycopg2-binary, Requests, OpenAI SDK, Python-Dotenv, Sentence-Transformers

---

## 🚀 Getting Started

### 1. Prerequisites
* Docker and Docker Compose installed.
* A valid OpenRouter API Key with access to model endpoints.

### 2. Environment Configuration
Create a `.env` file under the root directory with the following parameters:

```ini
# ==========================================
# Database Configuration (PostgreSQL)
# ==========================================
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_NAME=ai_etl_db
DB_HOST=postgres
DB_PORT=5432

# ==========================================
# LLM & API Configuration (OpenRouter / OpenAI)
# ==========================================
LLM_API_KEY=your_openrouter_or_openai_api_key
LLM_BASE_URL=https://openrouter.ai/api/v1
MODEL_NAME=gpt-4o-mini
ARXIV_RSS_URL=https://export.arxiv.org/rss/cs

# ==========================================
# Local ML Models & Inference Tuning
# ==========================================
LLM_MODEL_ID=Qwen/Qwen2.5-0.5B-Instruct
LLM_BATCH_SIZE=16
LLM_MAX_NEW_TOKENS=256
EMBEDDING_MODEL_ID=BAAI/bge-small-en-v1.5
EMBEDDING_BATCH_SIZE=32

# CPU Threading Controls for PyTorch/Transformers
OMP_NUM_THREADS=2
MKL_NUM_THREADS=2
TORCH_NUM_THREADS=2

# ==========================================
# Data Lake & Storage Paths
# ==========================================
RAW_DATA_PATH=/app/data/part_of_arxiv_history_01.json
DATALAKE_PATH=/app/datalake/silver/arxiv
DLQ_PATH=/app/datalake/dlq/arxiv
INTERMEDIATE_METADATA_PATH=/app/datalake/intermediate/arxiv_metadata
INTERMEDIATE_EMBEDDINGS_PATH=/app/datalake/intermediate/arxiv_embeddings

# ==========================================
# Apache Spark Configuration & Tuning
# ==========================================
POSTGRES_JAR_MAVEN="org.postgresql:postgresql:42.7.3"
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_MASTER_METADATA=local[8]
SPARK_MASTER_EMBEDDINGS=local[*]
SPARK_MASTER_LOADING=local[*]
SPARK_METADATA_REPARTITIONS=5
SPARK_EMBEDDINGS_REPARTITIONS=10
SPARK_LOADING_REPARTITIONS=24
SPARK_METADATA_MAX_RECORDS_PER_BATCH=200
SPARK_EMBEDDINGS_MAX_RECORDS_PER_BATCH=200
SPARK_LOADING_MAX_RECORDS_PER_BATCH=200

# ==========================================
# Data Quality & Target Sinks
# ==========================================
TARGET_TABLE=arxiv_documents
MAX_ERROR_RATE_DAILY=0.10
MAX_ERROR_RATE_BACKFILL=0.01
```

## Execution
docker-compose down
docker-compose up -d --build
docker-compose run --rm data-pipeline backfill
docker-compose run --rm data-pipeline metadata
docker-compose run --rm data-pipeline embeddings
docker-compose run --rm data-pipeline loading

## 📊 API Usage & Endpoints

Once the backend service container is active, the interactive API documentation (Swagger UI) is accessible at `http://localhost:8000/docs`. 

The service exposes a unified interface designed for contextual knowledge retrieval and real-time LLM inference benchmarking.

---

### 1. Contextual Query Ingestion (`POST /ask`)

Retrieves relevant paper abstracts matching the user's query from the specific model's database partition, constructs an isolated context prompt, and routes the payload to the GitHub Models API for a synthesized answer.

#### **URL Query Parameters**
| Parameter | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `model_tag` | `string` | No | `gpt-4o-mini` | The targeted model partition to isolate context from and route inference to (e.g., `gpt-4o-mini`, `DeepSeek-V3`). |


#### **Request Body (JSON payload)**
```json
{
  "question": "How do researchers study the interior pressure and temperature of giant planets like Jupiter?"
}
```

#### **Sample CURL Command**
```bash
curl -X 'POST' \
  'http://localhost:8000/ask?model_tag=gpt-4o-mini' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "question": "How do researchers study the interior pressure and temperature of giant planets like Jupiter?"
}'
```

#### **Expected JSON Response**
```json
{
  "status": "success",
  "model_requested": "gpt-4o-mini",
  "model_used": "openai/gpt-4o-mini",
  "question": "How do researchers study the interior pressure and temperature of giant planets like Jupiter?",
  "answer": "The context provided does not specifically address how researchers study the interior pressure and temperature of giant planets like Jupiter. However, from my general knowledge, researchers typically study the interior pressure and temperature through methods such as seismology, gravitational measurement, and the analysis of equations of state (EOS) derived from first-principles simulations.\n\nFor example, in the context of Jupiter, first-principles computer simulations can be used to model the behavior of hydrogen-helium mixtures under extreme conditions, which helps in understanding the pressures and temperatures within the planet's interior. Additionally, missions like the Galileo Entry Probe provide direct measurements that can inform models of the planet's composition and internal structure. \n\nFor more specific insights, you may refer to the paper titled \"A Massive Core in Jupiter Predicted From First-Principles Simulations\" available at https://arxiv.org/abs/0807.4264, which discusses the use of simulations to derive properties of Jupiter's interior.",
  "matched_papers": [
    {
      "paper_id": "0710.2930",
      "title": "Atmospheric Circulation of Hot Jupiters: A Review of Current Understanding",
      "published_date": "2007-10-17",
      "metadata": {
        "core_method": "Atmospheric Dynamics",
        "dataset_used": "Hot Jupiter",
        "key_findings": "Understanding atmospheric circulation is crucial for accurate"
      }
    },
    {
      "paper_id": "0704.3269",
      "title": "Atmospheric Dynamics of Short-period Extra Solar Gas Giant Planets I: Dependence of Night-Side Temperature on Opacity",
      "published_date": "2009-11-13",
      "metadata": {
        "core_method": "Numerical simulation",
        "dataset_used": "Jupiter-mass gas giant planets",
        "key_findings": "A clear temperature"
      }
    },
    {
      "paper_id": "0807.4264",
      "title": "A Massive Core in Jupiter Predicted From First-Principles Simulations",
      "published_date": "2009-11-13",
      "metadata": {
        "core_method": "First-principles",
        "dataset_used": "Jupiter's interior",
        "key_findings": "Core accretion supported"
      }
    }
  ]
}
```

### 2. Pure Metadata Retrieval (`GET /search`)

Performs a low-latency relational database look-up against the PostgreSQL backend using optimized case-insensitive `ILIKE` pattern matching. This endpoint bypasses the LLM inference layer entirely, returning structured records containing raw cleansed titles, URLs, abstracts, and metadata tracking fields.

#### **URL Query Parameters**

| Parameter | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `q` | `string` | **Yes** | N/A | The search keyword query matched against paper titles and abstracts. Must be at least 1 character long. |
| `model_tag` | `string` | No | `gpt-4o-mini` | Filters results to match the specific LLM dynamic partition variant batch ingested during the ETL phase. |
| `limit` | `integer` | No | `5` | Pagination limit regulating the maximum number of structured article records to return (Min: 1, Max: 50). |

#### **Sample q**
```
How do researchers study the interior pressure and temperature of giant planets like Jupiter?
```

#### **Sample CURL Command**
```bash
curl -X 'GET' \
  'http://localhost:8000/search?q=How%20do%20researchers%20study%20the%20interior%20pressure%20and%20temperature%20of%20giant%20planets%20like%20Jupiter%3F&model_tag=gpt-4o-mini&limit=3' \
  -H 'accept: application/json'
```
#### **Expected JSON Response**
```json
{
  "status": "success",
  "query": "ow do researchers study the interior pressure and temperature of giant planets like Jupiter?",
  "count": 3,
  "results": [
    {
      "id": "0710.2930",
      "title": "Atmospheric Circulation of Hot Jupiters: A Review of Current Understanding",
      "abstract": "Hot Jupiters are new laboratories for the physics of giant planet atmospheres. Subject to unusual forcing conditions, the circulation regime on these planets may be unlike anything known in the Solar System. Characterizing the atmospheric circulation of hot Jupiters is necessary for reliable interpretation of the multifaceted data currently being collected on these planets. We discuss several fundamental concepts of atmospheric dynamics that are likely central to obtaining a solid understanding of these fascinating atmospheres. A particular effort is made to compare the various modeling approaches employed so far to address this challenging problem.",
      "url": "https://arxiv.org/abs/0710.2930",
      "published_date": "2007-10-17",
      "rank_dense": 1,
      "rank_sparse": 999,
      "rrf_score": 0.017337729686218054
    },
    {
      "id": "0704.3269",
      "title": "Atmospheric Dynamics of Short-period Extra Solar Gas Giant Planets I: Dependence of Night-Side Temperature on Opacity",
      "abstract": "More than two dozen short-period Jupiter-mass gas giant planets have been discovered around nearby solar-type stars in recent years, several of which undergo transits, making them ideal for the detection and characterization of their atmospheres. Here we adopt a three-dimensional radiative hydrodynamical numerical scheme to simulate atmospheric circulation on close-in gas giant planets. In contrast to the conventional GCM and shallow water algorithms, this method does not assume quasi hydrostatic equilibrium and it approximates radiation transfer from optically thin to thick regions with flux-limited diffusion. In the first paper of this series, we consider synchronously-spinning gas giants. We show that a full three-dimensional treatment, coupled with rotationally modified flows and an accurate treatment of radiation, yields a clear temperature transition at the terminator. Based on a series of numerical simulations with varying opacities, we show that the night-side temperature is a strong indicator of the opacity of the planetary atmosphere. Planetary atmospheres that maintain large, interstellar opacities will exhibit large day-night temperature differences, while planets with reduced atmospheric opacities due to extensive grain growth and sedimentation will exhibit much more uniform temperatures throughout their photosphere's. In addition to numerical results, we present a four-zone analytic approximation to explain this dependence.",
      "url": "https://arxiv.org/abs/0704.3269",
      "published_date": "2009-11-13",
      "rank_dense": 2,
      "rank_sparse": 999,
      "rrf_score": 0.01707331932133175
    },
    {
      "id": "0807.4264",
      "title": "A Massive Core in Jupiter Predicted From First-Principles Simulations",
      "abstract": "Hydrogen-helium mixtures at conditions of Jupiter's interior are studied with first-principles computer simulations. The resulting equation of state (EOS) implies that Jupiter possesses a central core of 14-18 Earth masses of heavier elements, a result that supports core accretion as standard model for the formation of hydrogen-rich giant planets. Our nominal model has about 2 Earth masses of planetary ices in the H-He-rich mantle, a result that is, within modeling errors, consistent with abundances measured by the 1995 Galileo Entry Probe mission (equivalent to about 5 Earth masses of planetary ices when extrapolated to the mantle), suggesting that the composition found by the probe may be representative of the entire planet. Interior models derived from this first-principles EOS do not give a match to Jupiter's gravity moment J4 unless one invokes interior differential rotation, implying that jovian interior dynamics has an observable effect on the measured gravity field.",
      "url": "https://arxiv.org/abs/0807.4264",
      "published_date": "2009-11-13",
      "rank_dense": 3,
      "rank_sparse": 999,
      "rrf_score": 0.016817302936283106
    }
  ]
}
```