import os
import re
import xml.etree.ElementTree as ET
import requests
import psycopg2
from dotenv import load_dotenv
import datetime
from psycopg2.extras import execute_values
import time
from email.utils import parsedate_to_datetime
from psycopg2.extensions import connection as PostgreSQLConnection

load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") 


def get_db_connection()-> PostgreSQLConnection:
    """Establishes and returns a connection to PostgreSQL with retry logic."""
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
            return conn
        except psycopg2.OperationalError as e:
            retries -= 1
            print(f"⏳ Waiting for PostgreSQL database... (Retries left: {retries})")
            if retries == 0:
                raise e
            time.sleep(2)
    raise RuntimeError("Unable to establish PostgreSQL connection.")


def extract_data():
    """E: Extract - extract AI articles from ArXiv RSS"""
    print("[Extract] Fetching daily papers from ArXiv RSS (cs)...")
    url = "https://rss.arxiv.org/rss/cs"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    response = requests.get(url, headers=headers, timeout=10)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data, status code: {response.status_code}")
    
    return response.content

def parse_paper_id(guid_text: str, link_text: str) -> str:
    """
    Extracts versioned paper ID (e.g., '2608.18078v1') from <guid> or <link>.
    Sample GUID: 'oai:arXiv.org:2608.18078v1'
    """
    raw_id = ""

    if guid_text:
        match = re.search(r"arXiv\.org:([\w\.-]+)", guid_text)
        if match:
            raw_id = match.group(1)

    if not raw_id and link_text:
        match = re.search(r"/abs/([\w\.-]+)", link_text)
        if match:
            raw_id = match.group(1)

    return re.sub(r"v\d+$", "", raw_id)

def parse_pub_date(pub_date_str: str):
    """
    Parses RFC 822 date string (e.g., 'Thu, 20 Aug 2026 00:00:00 -0400')
    Returns tuple: (date_object, year_int)
    """
    if not pub_date_str:
        return None, None
        
    try:
        parsed_date = parsedate_to_datetime(pub_date_str).date()
        return parsed_date, parsed_date.year
    except (TypeError, ValueError):
        return None, None

def transform_data(xml_content):
    """
    T: Transform - Parse RSS XML sample, strip metadata headers,
    and convert to production `arxiv_documents` schema format.
    """
    print("[Transform] Cleaning RSS XML based on ArXiv sample schema...")
    root = ET.fromstring(xml_content)
    
    articles = []
    namespaces = {
        'dc': 'http://purl.org/dc/elements/1.1/',
        'arxiv': 'http://arxiv.org/schemas/atom'
    }

    for item in root.findall('.//item'):

        title_node = item.find('title')
        clean_title = (title_node.text or "").strip() if title_node is not None else ""

        link_node = item.find("link")
        link = (link_node.text or "").strip() if link_node is not None else ""

        guid_node = item.find("guid")
        guid_text = (guid_node.text or "").strip() if guid_node is not None else ""
        
        paper_id = parse_paper_id(guid_text, link)

        desc_node = item.find("description")
        raw_desc = (desc_node.text or "") if desc_node is not None else ""
        
        clean_abstract = re.sub(
            r"^arXiv:.*?\bAbstract:\s*",
            "",
            raw_desc,
            flags=re.DOTALL,
        )
        clean_abstract = re.sub(r"<[^>]+>", "", clean_abstract)
        clean_abstract = re.sub(r"\s+", " ", clean_abstract).strip()

        creator_node = item.find('dc:creator', namespaces)
        authors = (creator_node.text or "").strip() if creator_node is not None else ""

        categories = ", ".join(
            category.text.strip()
            for category in item.findall("category")
            if category.text and category.text.strip()
        )[:255]

        # 6. Parse Published Date & Year (<pubDate>)
        pub_date_node = item.find('pubDate')
        pub_date_str = (pub_date_node.text or "").strip() if pub_date_node is not None else "" # type: ignore
        published_date, published_year = parse_pub_date(pub_date_str)

        articles.append({
            "paper_id": paper_id,
            "title": clean_title,
            "abstract": clean_abstract,
            "authors": authors or None,
            "categories": categories or None,
            "url": link,
            "published_date": published_date,
            "published_year": published_year,
        })
            
    print(f"✅ [Transform] Successfully transformed {len(articles)} daily paper records.")
    return articles

def validate_and_filter_data(articles):
    """
    Data SLA Gate: Validate transformed records before database write.
    """
    print("[SLA Gate] Performing Data Quality Audits...")

    if not articles:
        raise ValueError(
            "SLA Gate Violation: Extracted daily batch contains 0 records."
        )

    total_count = len(articles)
    clean_articles = []
    corrupt_count = 0

    for article in articles:
        paper_id = str(article.get("paper_id") or "").strip()
        title = str(article.get("title") or "").strip()
        abstract = str(article.get("abstract") or "").strip()
        url = str(article.get("url") or "").strip()

        is_id_valid = bool(paper_id) and len(paper_id) <= 64
        is_title_valid = bool(title)
        is_abstract_valid = bool(abstract) and len(abstract) >= 20
        is_url_valid = url.startswith("https://arxiv.org/")

        if (
            is_id_valid
            and is_title_valid
            and is_abstract_valid
            and is_url_valid
        ):
            clean_article = article.copy()
            clean_article["paper_id"] = paper_id
            clean_article["title"] = title
            clean_article["abstract"] = abstract
            clean_article["url"] = url
            clean_articles.append(clean_article)
        else:
            corrupt_count += 1

    corruption_rate = corrupt_count / total_count

    print(
        f"[SLA Audit Report] Total: {total_count} | "
        f"Clean: {len(clean_articles)} | "
        f"Corrupt: {corrupt_count}"
    )

    if corruption_rate > 0.10:
        raise RuntimeError(
            f"Pipeline Circuit Breaker: corruption rate "
            f"{corruption_rate:.2%} exceeds 10% SLA limit."
        )

    if corrupt_count:
        print(
            f"[SLA Warning] Filtered out "
            f"{corrupt_count} corrupt records."
        )

    return clean_articles

def load_data(articles):
    """
    L: Load - Executes Idempotent Batch Upserts into `arxiv_documents`.
    """
    print("[Load] Streaming ingested records into `arxiv_documents` table...")

    if not articles:
        print("[Load] No valid articles to load.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    upsert_query = """
        INSERT INTO arxiv_documents (
            paper_id, title, abstract, authors, categories, url, published_date, published_year
        ) VALUES %s
        ON CONFLICT (paper_id) DO UPDATE SET
            title = EXCLUDED.title,
            abstract = EXCLUDED.abstract,
            authors = EXCLUDED.authors,
            categories = EXCLUDED.categories,
            url = EXCLUDED.url,
            published_date = EXCLUDED.published_date,
            published_year = EXCLUDED.published_year,
            ingested_at = CURRENT_TIMESTAMP;
    """

    data_to_insert = [
        (
            article['paper_id'],
            article['title'],
            article['abstract'],
            article['authors'],
            article['categories'],
            article['url'],
            article['published_date'],
            article['published_year']
        )
        for article in articles
    ]

    try:
        execute_values(cursor, upsert_query, data_to_insert)
        conn.commit()
        print(f"🎉 [Load] Pipeline Executed Successfully! Idempotently upserted {len(data_to_insert)} records.")
    except Exception as e:
        conn.rollback()
        print(f"❌ [Load] Batch Ingestion to `arxiv_documents` failed: {e}")
        raise 
    finally:
        cursor.close()
        conn.close()

def run_daily_pipeline() -> None:
    """Run the daily ArXiv RSS ingestion pipeline."""
    print("[Pipeline] Starting daily ingestion...")

    raw_data = extract_data()
    transformed_articles = transform_data(raw_data)
    clean_articles = validate_and_filter_data(transformed_articles)
    load_data(clean_articles)

    print("[Pipeline] Daily ingestion completed successfully.")

if __name__ == "__main__":
    run_daily_pipeline()