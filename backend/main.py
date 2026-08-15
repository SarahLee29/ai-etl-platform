import os
from typing import List, Dict, Any, Optional, cast
from fastapi import FastAPI, HTTPException, Query, status
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from openai import APIConnectionError, APIStatusError, AuthenticationError, OpenAI
import uvicorn
from pydantic import BaseModel

# Load environment variables pointing to the root .env file
load_dotenv()

app = FastAPI(
    title="AI & ETL Platform Backend MVP",
    description="A RAG backend using PostgreSQL keyword search and OpenRounter Modlels.",
    version="1.1.0"
)

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
    q: str = Query(..., min_length=1, description="The search keyword query"),
    model_tag: Optional[str] = Query(default=LLM_MODEL_NAME, description="Filter results by the LLM dynamic partition"),
    limit: Optional[int] = Query(default=5, ge=1, le=50, description="Number of records to return")
    ):
    """Executes a traditional SQL case-insensitive keyword search."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    search_query = f"%{q}%"
    target_model = MODEL_MAP.get(model_tag, MODEL_MAP["gemini-3.7-flash"]) # type: ignore
    '''if model_tag:
        sql = """
            SELECT id, title, url, abstract, fetched_at, data_source_version, cleaned_at, llm_model_used
            FROM test_articles
            WHERE (title ILIKE %s OR abstract ILIKE %s)
            LIMIT %s;
        """
        params = (search_query, search_query, limit)
    else:'''

    sql = """
        SELECT id, title, url, abstract, fetched_at, data_source_version, cleaned_at, llm_model_used
        FROM test_articles
        WHERE llm_model_used IS NULL
        AND (title ILIKE %s OR abstract ILIKE %s)
        LIMIT %s;
    """
    params = (search_query, search_query, limit)
    try:
        cursor.execute(sql, params)
        return cursor.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failure: {e}")
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
    # Simple Keyword Extraction: Use the first few words or the whole question as a search term
    # For MVP, pass the question directly into the ILIKE query.
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    question = payload.question 
    search_term = f"%{question}%"
    target_model = MODEL_MAP.get(model_tag, MODEL_MAP["gemini-3.7-flash"]) # type: ignore

    # If the question is long, also split words to try a broader match if needed, 
    # but for simplicity, search the full string first.
    try:
        cursor.execute("""
            SELECT title, abstract, url, data_source_version, cleaned_at, llm_model_used
            FROM test_articles 
            WHERE (title ILIKE %s OR abstract ILIKE %s)
            LIMIT 3;
        """, (search_term, search_term))
        matched_papers = cursor.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to fetch context from database.")
    finally:
        cursor.close()
        conn.close()

    # Build the context string from database records
    context_text = ""
    if matched_papers:
        for i, paper in enumerate(matched_papers, 1):
            context_text += f"[{i}] Title: {paper['title']}\nAbstract: {paper['abstract']}\nURL: {paper['url']}\n\n"
    else:
        context_text = "No specific matching papers found in the local database."

    # Construct the System and User Prompts
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
            "answer": answer
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