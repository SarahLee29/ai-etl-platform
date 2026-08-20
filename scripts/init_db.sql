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
    embedding vector(768)  
);

CREATE INDEX IF NOT EXISTS idx_published_year ON arxiv_documents(published_year);
CREATE INDEX IF NOT EXISTS idx_categories ON arxiv_documents(categories);