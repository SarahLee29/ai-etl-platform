import os
from typing import List, Dict, Any, Optional, cast
from fastapi import FastAPI, HTTPException, Query, status
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from openai import APIConnectionError, APIStatusError, AuthenticationError, OpenAI
import uvicorn
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

# Load environment variables pointing to the root .env file
load_dotenv()

# 1. Database Configurations
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") 

# 2. LLM Configurations
LLM_API_KEY = os.getenv("LLM_API_KEY")
LLM_BASE_URL = os.getenv("LLM_BASE_URL")
LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME")


app = FastAPI(
    title="AI & ETL Platform Backend MVP",
    description="A RAG backend using PostgreSQL keyword search and OpenRounter Modlels.",
    version="1.1.0"
)

embedding_model = SentenceTransformer("BAAI/bge-small-en-v1.5")

# Initialize the OpenAI Client, routing it to OpenRouter Models
ai_client = OpenAI(
    base_url=LLM_BASE_URL,
    api_key=LLM_API_KEY
)

def get_db_connection():
    """Establishes and returns a synchronous connection to the PostgreSQL instance."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        print(f"Database Connection Error: {e}")
        raise HTTPException(status_code=500, detail="Database unreachable.")

MODEL_MAP = {
    "gpt-4o-mini": "openai/gpt-4o-mini",
    "gpt-4o": "openai/gpt-4o",
    "DeepSeek-V3": "deepseek/deepseek-chat",
    "Claude-3.5": "anthropic/claude-3.5-sonnet",
    "gemini-3.7-flash": "google/gemini-3.7-flash"
}

@app.get("/")
def home():
    return {"message": "Welcome to the AI & ETL Backend API Gateway"}


@app.get("/search")
def search_articles(
    q: str = Query(..., min_length=1, description="The search query (keyword or natural language)"),
    model_tag: Optional[str] = Query(default=LLM_MODEL_NAME, description="Filter results or track model tag"),
    limit: Optional[int] = Query(default=5, ge=1, le=50, description="Number of records to return")
):
    """
    Executes an advanced Hybrid Search (Dense HNSW + Sparse GIN with RRF) 
    instead of traditional slow ILIKE queries.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        query_vector = embedding_model.encode(q).tolist()
        vector_str = "[" + ",".join(map(str, query_vector)) + "]"

        hybrid_sql = """
        WITH 
        vector_search AS (
            SELECT 
                paper_id, title, abstract, url, published_date,
                ROW_NUMBER() OVER (ORDER BY embedding <=> %s::vector) AS rank_dense
            FROM arxiv_documents
            ORDER BY embedding <=> %s::vector
            LIMIT 20
        ),
        keyword_search AS (
            SELECT 
                paper_id, title, abstract, url, published_date,
                ROW_NUMBER() OVER (ORDER BY ts_rank(fts_vector, websearch_to_tsquery('english', %s)) DESC) AS rank_sparse
            FROM arxiv_documents
            WHERE fts_vector @@ websearch_to_tsquery('english', %s)
            LIMIT 20
        ),
        combined_candidates AS (
            SELECT COALESCE(v.paper_id, k.paper_id) AS paper_id FROM vector_search v
            FULL OUTER JOIN keyword_search k ON v.paper_id = k.paper_id
        )
        SELECT 
            c.paper_id AS id,
            COALESCE(v.title, k.title) AS title,
            COALESCE(v.abstract, k.abstract) AS abstract,
            COALESCE(v.url, k.url) AS url,
            COALESCE(v.published_date, k.published_date) AS published_date,
            COALESCE(v.rank_dense, 999) AS rank_dense,
            COALESCE(k.rank_sparse, 999) AS rank_sparse,
            (
                1.0 / (60.0 + COALESCE(v.rank_dense, 999)) + 
                1.0 / (60.0 + COALESCE(k.rank_sparse, 999))
            ) AS rrf_score
        FROM combined_candidates c
        LEFT JOIN vector_search v ON c.paper_id = v.paper_id
        LEFT JOIN keyword_search k ON c.paper_id = k.paper_id
        ORDER BY rrf_score DESC
        LIMIT %s;
        """

        cursor.execute(hybrid_sql, (vector_str, vector_str, q, q, limit))
        results = cursor.fetchall()
        
        return {
            "status": "success",
            "query": q,
            "count": len(results),
            "results": results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Hybrid search query failure: {str(e)}")
    finally:
        cursor.close()
        conn.close()
        
class QueryRequest(BaseModel):
    question: str

@app.post("/ask")
def ask_rag(
    payload: QueryRequest, 
    model_tag: Optional[str] = Query(
        default=LLM_MODEL_NAME, description="Target model for AB testing"
        )
    ):
    """
    RAG Endpoint: 
    1. Extracts keywords from user question to query PostgreSQL.
    2. Injects the fetched papers into the prompt as Context.
    3. Calls the selected LLM to generate an answer based ONLY on the context.
    """

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    question = payload.question 
    target_model = MODEL_MAP.get(model_tag, MODEL_MAP["gemini-3.7-flash"]) # type: ignore

    try:
        query_vector = embedding_model.encode(question).tolist()
        vector_str = "[" + ",".join(map(str, query_vector)) + "]"

        hybrid_sql = """
        WITH 
        vector_search AS (
            SELECT 
                paper_id, title, abstract, url, published_date, metadata
                ROW_NUMBER() OVER (ORDER BY embedding <=> %s::vector) AS rank_dense
            FROM arxiv_documents
            ORDER BY embedding <=> %s::vector
            LIMIT 20
        ),
        keyword_search AS (
            SELECT 
                paper_id, title, abstract, url, published_date, metadata
                ROW_NUMBER() OVER (ORDER BY ts_rank(fts_vector, websearch_to_tsquery('english', %s)) DESC) AS rank_sparse
            FROM arxiv_documents
            WHERE fts_vector @@ websearch_to_tsquery('english', %s)
            LIMIT 20
        ),
        combined_candidates AS (
            SELECT COALESCE(v.paper_id, k.paper_id) AS paper_id FROM vector_search v
            FULL OUTER JOIN keyword_search k ON v.paper_id = k.paper_id
        )
        SELECT 
            c.paper_id,
            COALESCE(v.title, k.title) AS title,
            COALESCE(v.abstract, k.abstract) AS abstract,
            COALESCE(v.url, k.url) AS url,
            COALESCE(v.published_date, k.published_date) AS published_date,
            COALESCE(v.metadata, k.metadata) AS metadata,
            COALESCE(v.rank_dense, 999) AS rank_dense,
            COALESCE(k.rank_sparse, 999) AS rank_sparse,
            (
                1.0 / (60.0 + COALESCE(v.rank_dense, 999)) + 
                1.0 / (60.0 + COALESCE(k.rank_sparse, 999))
            ) AS rrf_score
        FROM combined_candidates c
        LEFT JOIN vector_search v ON c.paper_id = v.paper_id
        LEFT JOIN keyword_search k ON c.paper_id = k.paper_id
        ORDER BY rrf_score DESC
        LIMIT 3;
        """

        cursor.execute(hybrid_sql, (vector_str, vector_str, question, question))
        matched_papers = cursor.fetchall()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch context from database: {str(e)}")
    finally:
        cursor.close()
        conn.close()

    context_text = ""
    papers_metadata = []
    
    if matched_papers:
        for i, paper in enumerate(matched_papers, 1):
            meta_str = f"\nStructured Metadata: {paper['metadata']}" if paper['metadata'] else ""
            context_text += f"[{i}] Title: {paper['title']}\nAbstract: {paper['abstract']}\nURL: {paper['url']}\n{meta_str}\n\n"
            papers_metadata.append({
                "paper_id": paper["paper_id"],
                "title": paper["title"],
                "published_date": str(paper["published_date"]) if paper["published_date"] else None,
                "metadata": paper["metadata"]
            })
    else:
        context_text = "No specific matching papers found in the local database."

    system_prompt = (
        "You are an advanced AI Research Assistant. Your task is to answer the user's question "
        "using ONLY the provided Context from recent ArXiv papers. If the context doesn't contain "
        "the answer, rely on your general knowledge but state clearly that it wasn't found in the local database. "
        "Always cite the paper titles or URLs when referencing facts."
    )
    
    user_prompt = f"Context:\n{context_text}\n\nQuestion: {question}"

    # Call the OpenRouter Models API
    try:
        response = ai_client.chat.completions.create(
            model=target_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=1000
        )
        
        answer = response.choices[0].message.content
        
        return {
            "status": "success",
            "model_requested": model_tag,
            "model_used": target_model,
            "question": question,
            "answer": answer,
            "matched_papers": papers_metadata
        }
        
    # Catch API Key or authentication issues (500: Server configuration defect)
    except AuthenticationError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server configuration error: Invalid upstream LLM API Key."
        )

    # Catch network timeouts and connection drops (503: Upstream service unavailable)
    except APIConnectionError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Upstream LLM Provider (OpenRouter) is unreachable or timed out."
        )

    # Catch HTTP error responses returned by OpenRouter (502: Bad gateway)
    except APIStatusError as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Upstream LLM Provider error [{e.status_code}]: {e.message}"
        )

    # Fallback for unexpected local application bugs (500: Internal server error)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unhandled internal server error: {str(e)}"
        )
    
if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)