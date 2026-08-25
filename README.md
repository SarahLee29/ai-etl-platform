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
* A valid OpenRounter API Key with access to OpenRounter Models API.

### 2. Environment Configuration
Create a `.env` file under the root directory with the following parameters:

```ini
DB_HOST=postgres
DB_PORT=5432
DB_NAME=ai_etl_db
DB_USER=postgres
DB_PASSWORD=your_secure_password
GITHUB_TOKEN=your_github_personal_access_token
```

## Execution
docker-compose down

docker-compose up -d --build

docker-compose run --rm data-pipeline daily (or backfill, or embeddings)

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
  "answer": "The provided context does not contain information on how researchers study the interior pressure and temperature of giant planets like Jupiter. Therefore, I cannot give a specific answer based on the local database. Generally, such studies are typically conducted using methods like gravitational field measurements, atmospheric composition analysis, and theoretical models of planetary formation and structure.",
  "matched_papers": [
    {
      "paper_id": "0704.0448",
      "title": "Giant Planet Migration in Viscous Power-Law Discs",
      "published_date": "2009-06-23"
    },
    {
      "paper_id": "0704.1210",
      "title": "The dynamics of Jupiter and Saturn in the gaseous proto-planetary disk",
      "published_date": "2007-05-23"
    },
    {
      "paper_id": "0704.1138",
      "title": "Testing Disk Instability Models for Giant Planet Formation",
      "published_date": "2009-11-13"
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
  "query": "How do researchers study the interior pressure and temperature of giant planets like Jupiter?",
  "count": 3,
  "results": [
    {
      "id": "0704.0448",
      "title": "Giant Planet Migration in Viscous Power-Law Discs",
      "abstract": "Many extra-solar planets discovered over the past decade are gas giants in tight orbits around their host stars. Due to the difficulties of forming these `hot Jupiters' in situ, they are generally assumed to have migrated to their present orbits through interactions with their nascent discs. In this paper, we present a systematic study of giant planet migration in power law discs. We find that the planetary migration rate is proportional to the disc surface density. This is inconsistent with the assumption that the migration rate is simply the viscous drift speed of the disc. However, this result can be obtained by balancing the angular momentum of the planet with the viscous torque in the disc. We have verified that this result is not affected by adjusting the resolution of the grid, the smoothing length used, or the time at which the planet is released to migrate.",
      "url": "https://arxiv.org/abs/0704.0448",
      "published_date": "2009-06-23",
      "rank_dense": 1,
      "rank_sparse": 999,
      "rrf_score": 0.017337729686218054
    },
    {
      "id": "0704.1210",
      "title": "The dynamics of Jupiter and Saturn in the gaseous proto-planetary disk",
      "abstract": "We study the possibility that the mutual interactions between Jupiter and Saturn prevented Type II migration from driving these planets much closer to the Sun. Our work extends previous results by Masset and Snellgrove (2001), by exploring a wider set of initial conditions and disk parameters, and by using a new hydrodynamical code that properly describes for the global viscous evolution of the disk. Initially both planets migrate towards the Sun, and Saturn's migration tends to be faster. As a consequence, they eventually end up locked in a mean motion resonance. If this happens in the 2:3 resonance, the resonant motion is particularly stable, and the gaps opened by the planets in the disk may overlap. This causes a drastic change in the torque balance for the two planets, which substantially slows down the planets' inward migration. If the gap overlap is substantial, planet migration may even be stopped or reversed. As the widths of the gaps depend on disk viscosity and scale height, this mechanism is particularly efficient in low viscosity, cool disks. We discuss the compatibility of our results with the initial conditions adopted in Tsiganis et al. (2005) and Gomes et al. (2005) to explain the current orbital architecture of the giant planets and the origin of the Late Heavy Bombardment of the Moon.",
      "url": "https://arxiv.org/abs/0704.1210",
      "published_date": "2007-05-23",
      "rank_dense": 2,
      "rank_sparse": 999,
      "rrf_score": 0.01707331932133175
    },
    {
      "id": "0704.1138",
      "title": "Testing Disk Instability Models for Giant Planet Formation",
      "abstract": "Disk instability is an attractive yet controversial means for the rapid formation of giant planets in our solar system and elsewhere. Recent concerns regarding the first adiabatic exponent of molecular hydrogen gas are addressed and shown not to lead to spurious clump formation in the author's disk instability models. A number of disk instability models have been calculated in order to further test the robustness of the mechanism, exploring the effects of changing the pressure equation of state, the vertical temperature profile, and other parameters affecting the temperature distribution. Possible reasons for differences in results obtained by other workers are discussed. Disk instability remains as a plausible formation mechanism for giant planets.",
      "url": "https://arxiv.org/abs/0704.1138",
      "published_date": "2009-11-13",
      "rank_dense": 3,
      "rank_sparse": 999,
      "rrf_score": 0.016817302936283106
    }
  ]
}
```