import os
import re
import xml.etree.ElementTree as ET
import requests
import psycopg2
from dotenv import load_dotenv
import datetime
from psycopg2.extras import execute_values
import time

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



