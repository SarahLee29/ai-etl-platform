CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS arxiv_documents (
    paper_id VARCHAR(64) PRIMARY KEY,
    title TEXT NOT NULL,
    abstract TEXT NOT NULL,
    authors TEXT,
    categories VARCHAR(255),
    url TEXT NOT NULL,
    published_date DATE,
    published_year INT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    embedding vector(384),
    metadata JSONB
);
