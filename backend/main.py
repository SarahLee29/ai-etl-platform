import os
from typing import List, Dict, Any, cast
from fastapi import FastAPI, HTTPException, Query
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from openai import OpenAI
import uvicorn

# Load environment variables pointing to the root .env file
load_dotenv()

app = FastAPI(
    title="AI & ETL Platform Backend MVP",
    description="A RAG backend using PostgreSQL keyword search and free GitHub Models GPT.",
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

# Initialize the OpenAI Client, routing it to GitHub Models
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


@app.get("/")
def home():
    return {"message": "Welcome to the AI & ETL Backend API Gateway"}


@app.get("/search")
def search_articles(q: str = Query(..., min_length=1, description="The search keyword query")):
    """Executes a traditional SQL case-insensitive keyword search."""
    conn = get_db_connection()
    cursor = conn.cursor()
    search_query = f"%{q}%"
    try:
        cursor.execute("""
            SELECT id, title, url, abstract, data_source_version, cleaned_at, llm_model_used
            FROM test_articles 
            WHERE title ILIKE %s OR abstract ILIKE %s
            LIMIT 5;
        """, (search_query, search_query))
        return cursor.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failure: {e}")
    finally:
        cursor.close()
        conn.close()


@app.post("/ask")
def ask_rag(question: str = Query(..., description="Ask a question about AI papers")):
    """
    RAG Endpoint: 
    1. Extracts keywords from user question to query PostgreSQL.
    2. Injects the fetched papers into the prompt as Context.
    3. Calls the free GPT-4o-mini to generate an answer based ONLY on the context.
    """
    # Simple Keyword Extraction: Use the first few words or the whole question as a search term
    # For MVP, we pass the question directly into the ILIKE query.
    conn = get_db_connection()
    cursor = conn.cursor()
    search_term = f"%{question}%"
    
    # If the question is long, let's also split words to try a broader match if needed, 
    # but for simplicity, we'll search the full string first.
    try:
        cursor.execute("""
            SELECT title, abstract, url, data_source_version, cleaned_at, llm_model_used
            FROM test_articles 
            WHERE title ILIKE %s OR abstract ILIKE %s
            LIMIT 3;
        """, (search_term, search_term))
        matched_papers = cursor.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to fetch context from database.")
    finally:
        cursor.close()
        conn.close()

    # Build the context string from our database records
    context_text = ""
    if matched_papers:
        for i, paper in enumerate(matched_papers, 1):
            context_text += f"[{i}] Title: {paper['title']}\nAbstract: {paper['abstract']}\nURL: {paper['url']}\n\n"
    else:
        context_text = "No specific matching papers found in the local database."

    # Construct the System and User Prompts (The core of RAG)
    system_prompt = (
        "You are an advanced AI Research Assistant. Your task is to answer the user's question "
        "using ONLY the provided Context from recent ArXiv papers. If the context doesn't contain "
        "the answer, rely on your general knowledge but state clearly that it wasn't found in the local database. "
        "Always cite the paper titles or URLs when referencing facts."
    )
    
    user_prompt = f"Context:\n{context_text}\n\nQuestion: {question}"

    # Call the GitHub Models API
    try:
        response = ai_client.chat.completions.create(
            model=LLM_MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=1,
            max_tokens=4096,
            top_p=1
        )
        
        answer = response.choices[0].message.content
        
        return {
            "question": question,
            "context_used": matched_papers,
            "answer": answer
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM Generation Failed: {e}")
    
if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)