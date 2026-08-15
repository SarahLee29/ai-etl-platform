# ArXiv Data Insights: Automated ETL & RAG Platform

A containerized, production-ready data pipeline and retrieval system that extracts artificial intelligence research papers from ArXiv, enforces an upstream data service-level agreements (SLAs), and exposes a RAG (Retrieval-Augmented Generation) query interface supporting multi-model evaluation.

## 🏗️ System Architecture

The application is split into two decoupled layers coordinated via Docker Compose inside an isolated bridge network:
1. **The ETL Pipeline (`data-pipeline`)**: A batch-processing engine that extracts XML/RSS feeds, transforms raw typography, runs a fault-tolerant quality audit, and performs atomic upserts into PostgreSQL.
2. **The RAG Query Service (`backend`)**: A FastAPI application that retrieves contextual records based on user queries and routes them dynamically to the GitHub Models API for synthesized answers.

---

## 🛠️ Tech Stack

* **Language:** Python 3.11+
* **Database:** PostgreSQL 16+ with `pgvector` extension
* **API Framework:** FastAPI, Uvicorn
* **Orchestration:** Docker, Docker Compose
* **Client Packages:** Psycopg2-binary, Requests, OpenAI SDK, Python-Dotenv

---

## 🚀 Getting Started

### 1. Prerequisites
* Docker and Docker Compose installed.
* A valid GitHub Personal Access Token (PAT) with access to GitHub Models API.

### 2. Environment Configuration
Create a `.env` file inside the `data-pipeline/` directory with the following parameters:

```ini
DB_HOST=postgres
DB_PORT=5432
DB_NAME=ai_etl_db
DB_USER=postgres
DB_PASSWORD=your_secure_password
GITHUB_TOKEN=your_github_personal_access_token
```

## Execution
docker-compose down -v

docker-compose up -d --build

docker-compose run --rm data-pipeline

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
  "question": "What are the latest breakthroughs in incremental learning?"
}
```

#### **Sample CURL Command**
```bash
curl -X 'POST' \
  'http://localhost:8000/ask?model_tag=DeepSeek-V3' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "question": "What are the latest breakthroughs in incremental learning?"
}'
```

#### **Expected JSON Response**
```json
{
  "status": "success",
  "model_evaluated": "DeepSeek-V3",
  "context_count": 3,
  "answer": "Based on the retrieved ArXiv abstracts, recent breakthroughs focus on stabilizing weights in neural networks during sequential training phases. Key methodologies utilize custom loss functions to minimize catastrophic forgetting while allowing the architecture to adapt to novel data streams continuously."
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

#### **Sample CURL Command**
```bash
curl -X 'GET' \
  '[http://127.0.0.1:8000/search?q=incremental%20learning&model_tag=DeepSeek-V3&limit=1](http://127.0.0.1:8000/search?q=incremental%20learning&model_tag=DeepSeek-V3&limit=1)' \
  -H 'accept: application/json'
```
#### **Expected JSON Response**
```json
{
  "status": "success",
  "query": "incremental learning",
  "model_partition": "DeepSeek-V3",
  "results_returned": 1,
  "records": [
    {
      "id": 142,
      "title": "Mitigating Catastrophic Forgetting via Dynamic Weights",
      "url": "[https://arxiv.org/abs/2606.12345](https://arxiv.org/abs/2606.12345)",
      "abstract": "We introduce an explicit gating architecture that regularizes network activations during sequential training...",
      "data_source_version": "v1.0.2",
      "cleaned_at": "2026-06-15T13:10:34Z",
      "llm_model_used": "DeepSeek-V3"
    }
  ]
}
```