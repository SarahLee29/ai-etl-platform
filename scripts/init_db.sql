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
    fts_vector tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(abstract, '')), 'B')
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_published_year ON arxiv_documents(published_year);
CREATE INDEX IF NOT EXISTS idx_categories ON arxiv_documents(categories);

'''CREATE INDEX IF NOT EXISTS articles_fts_idx 
ON arxiv_documents USING gin (fts_vector);

CREATE INDEX IF NOT EXISTS articles_embedding_hnsw_idx 
ON arxiv_documents USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);'''