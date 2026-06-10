import os
import re
import xml.etree.ElementTree as ET
import requests
import psycopg2
from dotenv import load_dotenv

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
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_articles (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            url TEXT UNIQUE NOT NULL,
            abstract TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()
    
    inserted_count = 0
    for article in articles:
        try:
            # Handle idempotency using ON CONFLICT DO NOTHING against unique URLs
            cursor.execute("""
                INSERT INTO test_articles (title, url, abstract)
                VALUES (%s, %s, %s)
                ON CONFLICT (url) DO NOTHING;
            """, (article['title'], article['url'], article['abstract']))
            inserted_count += cursor.rowcount
        except Exception as e:
            print(f"Failed to insert single record: {e}")
            conn.rollback()
            
    conn.commit()
    cursor.close()
    conn.close()
    print(f"ETL pipeline executed successfully! Upserted {inserted_count} new records.")


if __name__ == "__main__":
    try:
        init_database()
        raw_data = extract_data() 
        cleansed_datasets = transform_data(raw_data)
        load_data(cleansed_datasets)
        
    except Exception as error:
        print(f"💥 ETL Execution Pipeline Failed: {error}")
