import os
import re
import xml.etree.ElementTree as ET
import requests
import psycopg2
from dotenv import load_dotenv
import datetime
from psycopg2.extras import execute_values
import time

#uoad environment variables from .env file
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") 


def init_database():
    """create a test table"""
    print("initializing database...")
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            break 
        except psycopg2.OperationalError as e:
            retries -= 1
            print(f"⏳ Database is waking up... waiting. (Retries left: {retries})")
            if retries == 0:
                raise e
            time.sleep(2)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_articles (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            abstract TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data_source_version VARCHAR(50) DEFAULT 'v1.0.0',
            cleaned_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            llm_model_used VARCHAR(50) DEFAULT 'gpt-4o-mini',
            CONSTRAINT unique_url_per_model UNIQUE (url, llm_model_used)
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("table `test_articles` initialized successfully!")


def extract_data():
    """E: Extract - extract AI articles from ArXiv RSS"""
    print("initializing data extraction...")
    url = "https://rss.arxiv.org/rss/cs.AI"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    response = requests.get(url, headers=headers, timeout=10)
    if response.status_code != 200:
        raise Exception(f"failing, status code: {response.status_code}")
    
    return response.content


def transform_data(xml_content):
    """T: Transform - parse XML text, clean and extract core fields"""
    print("cleaning and transforming data...")
    root = ET.fromstring(xml_content)
    
    articles = []
    # interesting fields: title, link, description (abstract) are nested under <item> tags
    for item in root.findall('.//item'):
        title_node = item.find('title')
        title = title_node.text if title_node is not None else "Unknown Title"
        
        link_node = item.find('link')
        link = link_node.text if link_node is not None else ""
        
        desc_node = item.find('description')
        description = desc_node.text if desc_node is not None else ""
        
        # Clean text: Strip HTML tags and clean up erratic line breaks or extra spaces
        if description:
            clean_abstract = re.sub(r'<[^>]+>', '', description).strip()
            clean_abstract = clean_abstract.replace('\n', ' ')

        if link:
            articles.append({
                "title": title,
                "url": link,
                "abstract": clean_abstract
            })
            
    print(f"Successfully processed and cleansed {len(articles)} articles.")
    return articles


def load_data(articles):
    """L: Load - Executes batch upserts into the target PostgreSQL relation."""
    print("Loading cleansed datasets into PostgreSQL...")
    DATA_SOURCE_VERSION = "v1.0.0-arxiv-raw"
    LLM_MODEL_USED = "gpt-4o-mini" 
    current_time_utc = datetime.datetime.now(datetime.timezone.utc)
    
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()

    upsert_query = """
        INSERT INTO test_articles (
            title, url, abstract, data_source_version, cleaned_at, llm_model_used
        ) VALUES %s
        ON CONFLICT (url, llm_model_used) DO UPDATE SET
            title = EXCLUDED.title,
            abstract = EXCLUDED.abstract,
            data_source_version = EXCLUDED.data_source_version,
            cleaned_at = EXCLUDED.cleaned_at;
    """

    data_to_insert = []
    for article in articles:
        data_tuple = (
            article['title'],
            article['url'],
            article['abstract'],
            DATA_SOURCE_VERSION,
            current_time_utc,
            LLM_MODEL_USED
        )
        data_to_insert.append(data_tuple)
    
    inserted_count = 0
    try:
        execute_values(cursor, upsert_query, data_to_insert)
        inserted_count = len(data_to_insert)
        conn.commit()
    except Exception as e:
        print(f"❌ Batch ingestion failed: {e}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

    print(f"ETL pipeline executed successfully! Upserted {inserted_count} new records.")

def validate_and_filter_data(articles):
    """
    Data Quality Assertion Layer (Data SLA Gate)
    Audits the transformed batch before loading. Triggers a hard fail-fast melt if rules are breached.
    """
    print("Executing Data Quality Audit against Data SLA standards...")
    
    if not articles:
        raise ValueError("Melt Triggered: Extracted batch contains 0 records. Upstream source may be down.")

    total_count = len(articles)
    clean_articles = []
    corrupt_articles = []

    for article in articles:
        is_abstract_valid = article['abstract'] and article['abstract'].strip() != ""
        is_url_valid = article['url'].startswith("http")

        if is_abstract_valid and is_url_valid:
            clean_articles.append(article) 
        else:
            corrupt_articles.append(article)

    # Calculate metrics for alerting
    corrupt_count = len(corrupt_articles)
    empty_ratio = corrupt_count / total_count
    print(f"[Data Audit Report] Total: {total_count} | Clean: {len(clean_articles)} | Corrupt: {corrupt_count}")

    # SLA Hard Gate: If MORE THAN 10% of data is corrupt, something is fundamentally wrong.
    # freeze the entire pipeline.
    if empty_ratio > 0.10:
        raise RuntimeError(f"Pipeline Circuit Breaker! Corruption rate at {empty_ratio:.2%}, exceeding 10% SLA limit.")

    if corrupt_count > 0:
        print(f"⚠️ Warning: Filtered out {corrupt_count} corrupt rows. Proceeding with remaining {len(clean_articles)} rows.")

    return clean_articles

if __name__ == "__main__":
    try:
        init_database()
        raw_data = extract_data() 
        cleansed_batch = transform_data(raw_data)
        cleansed_batch = validate_and_filter_data(cleansed_batch)
        load_data(cleansed_batch)
        
    except Exception as error:
        print(f"💥 ETL Execution Pipeline Failed: {error}")
