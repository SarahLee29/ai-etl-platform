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

load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") 


def get_db_connection():
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
        # 1. Parse Title
        title_node = item.find('title')
        raw_title = title_node.text.strip() if title_node is not None else "" # type: ignore
        clean_title = raw_title

        # 2. Parse Link, GUID, and paper_id
        link_node = item.find('link')
        link = link_node.text.strip() if link_node is not None else "" # type: ignore
        
        guid_node = item.find('guid')
        guid_text = guid_node.text.strip() if guid_node is not None else "" # type: ignore
        
        paper_id = parse_paper_id(guid_text, link)

        # 3. Clean Abstract (Remove "arXiv:2608.18078v1 Announce Type: new \nAbstract: ")
        desc_node = item.find('description')
        raw_desc = desc_node.text if desc_node is not None else "" # type: ignore
        
        # Regex removes the ArXiv RSS header prefix up to 'Abstract:'
        clean_abstract = re.sub(r'^arXiv:.*?\bAbstract:\s*', '', raw_desc, flags=re.DOTALL) # type: ignore
        clean_abstract = re.sub(r'<[^>]+>', '', clean_abstract).strip()
        clean_abstract = clean_abstract.replace('\n', ' ')

        # 4. Parse Authors (<dc:creator>)
        creator_node = item.find('dc:creator', namespaces)
        authors = creator_node.text.strip() if creator_node is not None else "" # type: ignore

        # 5. Parse Primary Category
        cat_node = item.find('category')
        categories = ", ".join(
            category.text.strip()
            for category in item.findall("category")
            if category.text and category.text.strip()
        )[:255]

        # 6. Parse Published Date & Year (<pubDate>)
        pub_date_node = item.find('pubDate')
        pub_date_str = pub_date_node.text.strip() if pub_date_node is not None else "" # type: ignore
        published_date, published_year = parse_pub_date(pub_date_str)

        if paper_id and link and clean_title and clean_abstract:
            articles.append({
                "paper_id": paper_id,
                "title": clean_title,
                "abstract": clean_abstract,
                "authors": authors,
                "categories": categories,
                "url": link,
                "published_date": published_date,
                "published_year": published_year
            })
            
    print(f"✅ [Transform] Successfully transformed {len(articles)} daily paper records.")
    return articles