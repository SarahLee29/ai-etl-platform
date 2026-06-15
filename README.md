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
docker-compose up -d postgres
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
| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `question` | `string` | Yes | Natural language query or research topic you want to analyze. |

#### **Sample Payload**
```json
{
  "question": "ai agents"
}
```

#### **Expected JSON Response**
```json
{
    "question": "ai agents",
    "context_used": [
        {
            "title": "Hybrid Open-Ended Tri-Evolution Makes Better Deep Researcher",
            "abstract": "arXiv:2606.13710v1 Announce Type: new  Abstract: Deep research and agent evolution serve as de-facto tasks for AI agents in real-world applications toward artificial general intelligence. The former enables autonomous retrieval and integration of information in open-ended environments to tackle open-ended research tasks, yet it is constrained by the static parametric deep research capabilities of agent systems. The latter allows agents to autonomously interact with the environment to gain experiences that evolve model capabilities. However, its effectiveness has been widely validated only on verifiable tasks with standard answers, leaving a gap with open-ended research tasks. To bridge these two critical tasks, we propose the Hybrid Open-Ended Tri-Evolution (HOTE) framework, which leverages hybrid-mode reinforcement learning to facilitate the collaborative evolution of a proposer, solver and judge based on web-scale knowledge, moving toward autonomous evolving agents in open-ended tasks and environments. Extensive experiments on three long-form deep research benchmarks demonstrate that the 8B model trained via HOTE surpasses the strongest static open 8-32B models as well as those trained by state-of-the-art deep research training methods with less time overhead, and further verify that the evolution of all three modules in HOTE is indispensable.",
            "url": "https://arxiv.org/abs/2606.13710",
            "data_source_version": "v1.0.0-arxiv-raw",
            "cleaned_at": "2026-06-15T18:28:03.958805+00:00",
            "llm_model_used": "deepseek-gpt-3.5-mini"
        },
        {
            "title": "Same-Origin Policy for Agentic Browsers",
            "abstract": "arXiv:2606.14027v1 Announce Type: cross  Abstract: Agentic browsers integrate autonomous AI agents into web browsers, enabling users to accomplish web tasks through natural-language instructions. The same-origin policy (SOP) is a fundamental browser security mechanism that prevents unauthorized automated cross-origin data flows induced by scripts. However, whether SOP remains effective in agentic browsers is an open question that has not been systematically studied. In this work, we bridge this gap. We first observe that an agentic browser can itself serve as an automated channel for cross-origin data flows, potentially leading to SOP violations. To investigate this phenomenon, we construct SOPBench, a benchmark for evaluating SOP violations in agentic browsers. Our evaluation shows that existing agentic browsers frequently violate SOP, both in benign settings and under attacks. To address this problem, we propose SOPGuard, an SOP enforcement mechanism tailored to agentic browsers. We implement SOPGuard in BrowserOS, an open-source agentic browser. Extensive evaluations demonstrate that SOPGuard effectively enforces SOP while preserving utility and incurring only a small runtime overhead. Our code and data are available at https://github.com/wxl-lxw/BrowserOS-SOPGuard.",
            "url": "https://arxiv.org/abs/2606.14027",
            "data_source_version": "v1.0.0-arxiv-raw",
            "cleaned_at": "2026-06-15T18:28:03.958805+00:00",
            "llm_model_used": "deepseek-gpt-3.5-mini"
        },
        {
            "title": "Will AI Agents Free Us From Meaningless Work? A Human-Centered Analysis",
            "abstract": "arXiv:2606.12430v2 Announce Type: replace-cross  Abstract: Some claim that AI agents will free workers from the boring parts of their jobs, yet little is known about how workers themselves identify which tasks should be automated. Prior research focuses on occupations, overlooking that workers experience varying levels of meaning across tasks within the same role. We address this gap with a task-level analysis grounded in Graeber's theory of bullshit jobs. Using ratings from 202 workers on 171 workplace tasks, we (1) validate a five-item scale of perceived bullshitness, (2) show that perceived bullshitness strongly predicts desire for AI delegation, and (3) find that such tasks are also seen as requiring less human oversight. Together, these findings suggest that tasks perceived as bullshit are natural candidates for AI delegation, aligning worker preferences with perceived feasibility.",
            "url": "https://arxiv.org/abs/2606.12430",
            "data_source_version": "v1.0.0-arxiv-raw",
            "cleaned_at": "2026-06-15T18:28:03.958805+00:00",
            "llm_model_used": "deepseek-gpt-3.5-mini"
        }
    ],
    "answer": "The provided context contains insights about AI agents in different applications:\n\n1. In the paper \"Hybrid Open-Ended Tri-Evolution Makes Better Deep Researcher\" (https://arxiv.org/abs/2606.13710), AI agents are discussed in the context of deep research and their evolution in open-ended environments. The proposed Hybrid Open-Ended Tri-Evolution (HOTE) framework allows agents to autonomously retrieve and integrate information, facilitating their evolution towards achieving artificial general intelligence.\n\n2. The paper \"Same-Origin Policy for Agentic Browsers\" (https://arxiv.org/abs/2606.14027) describes the integration of autonomous AI agents within web browsers to perform tasks based on natural-language instructions. It raises concerns regarding the effectiveness of the same-origin policy in preventing unauthorized cross-origin data flows by these agents.\n\n3. The study \"Will AI Agents Free Us From Meaningless Work? A Human-Centered Analysis\" (https://arxiv.org/abs/2606.12430) analyzes how AI agents could automate tasks that workers find pointless or \"bullshitty,\" suggesting a potential alignment between worker preferences and the tasks suitable for AI delegation.\n\nIf you have a specific question regarding AI agents, feel free to ask!"
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

#### **Sample Query**
```bash
http://127.0.0.1:8000/search?q=model&limit=2
```
#### **Expected JSON Response**
```json
[
    {
        "id": 267,
        "title": "When Sample Selection Bias Precipitates Model Collapse",
        "url": "https://arxiv.org/abs/2606.13732",
        "abstract": "arXiv:2606.13732v1 Announce Type: new  Abstract: The proliferation of recursive training on synthetic data can alleviate data scarcity but risks model collapse, where repeated training erodes distributional tails and homogenizes outputs. Data selection is widely viewed as a remedy, yet its reliability depends critically on the reference distribution used by the verifier. We show that in low-resource verification regimes, where each verifier observes only a small, fragmented, and biased slice of the target manifold, selection itself becomes biased. This situation naturally arises in low-resource data silos such as healthcare consortia or proprietary financial institutions, where raw data cannot be pooled and local references are inherently incomplete. As a result, selection preferentially retains samples aligned with the local manifold while pruning globally relevant tail modes, turning from a safeguard against collapse into a mechanism that precipitates it. We theoretically prove that such siloed selection accelerates collapse and induces power-law diversity decay. As an initial mitigation, we construct Wasserstein proxy references from multiple silos without sharing raw data. Empirical results confirm that local-reference selection fails on skewed distributions, whereas collaborative proxy references mitigate diversity degradation, suggesting that recursive synthetic-data pipelines require particular caution when real-data coverage is fragmented or scarce.",
        "data_source_version": "v1.0.0-arxiv-raw",
        "cleaned_at": "2026-06-15T20:58:05.276257+00:00",
        "llm_model_used": "gpt-4o-mini"
    },
    {
        "id": 269,
        "title": "MA-ProofBench: A Two-Tiered Evaluation of LLMs for Theorem Proving in Mathematical Analysis",
        "url": "https://arxiv.org/abs/2606.13782",
        "abstract": "arXiv:2606.13782v1 Announce Type: new  Abstract: Large Language Models (LLMs) have made notable progress in automated theorem proving, yet existing formal benchmarks remain limited in both mathematical coverage and difficulty. Most are concentrated in areas that are easier to formalize, such as algebra and elementary number theory, and provide limited coverage of subfields that require deeper reasoning, including mathematical analysis. To address this gap, we introduce MA-ProofBench, to the best of our knowledge, the first formal theorem-proving benchmark dedicated to Mathematical Analysis. The benchmark contains 200 formalized theorems covering 6 core topics and 27 subcategories, including measure and integration theory, complex analysis, and functional analysis. The problems are divided into two difficulty levels, an undergraduate level (Level I, 100 problems) and a Ph.D. qualifying level (Level II, 100 problems), to evaluate how well LLMs perform formal reasoning at different mathematical depths. Each problem is constructed through a human-led, LLM-assisted formalization pipeline followed by independent expert review, ensuring that the formal statements remain faithful to the original mathematics. We evaluate a range of recent general-purpose reasoning models and formal theorem provers on MA-ProofBench. However, most models perform poorly: even the best-performing model, GPT-5.5, achieves only 16% Pass@8 on Level I and 5% on Level II, while most models stay close to 0% on Level II. Further analysis identifies Mathlib hallucinations and incomplete proofs as the two dominant failure modes, while an evaluation on the natural-language version of the benchmark exposes a clear gap between informal and formal reasoning. MA-ProofBench is intended to serve as a reliable reference for tracking progress in formal mathematical reasoning in advanced domains.",
        "data_source_version": "v1.0.0-arxiv-raw",
        "cleaned_at": "2026-06-15T20:58:05.276257+00:00",
        "llm_model_used": "gpt-4o-mini"
    }
]
```