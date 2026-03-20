import asyncio
import json
import os
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from google.cloud import bigquery
from groq import Groq
from pydantic import BaseModel, ValidationError
from typing import Optional, List

# 1. Authenticate with GCP and Groq
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"

GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("CRITICAL ERROR: GROQ_API_KEY environment variable is missing!")

# Connect to APIs
bq_client = bigquery.Client()
PROJECT_ID = bq_client.project
groq_client = Groq(api_key=GROQ_API_KEY)

# 2. Update to the New Database Architecture
FRONTIER_TABLE = f"{PROJECT_ID}.book_scraping.crawl_frontier"
DESTINATION_TABLE = f"{PROJECT_ID}.book_scraping.library_database"

# ---------------------------------------------------------
# MODULE 4: THE DATA BOUNCER (PYDANTIC)
# ---------------------------------------------------------
class BookData(BaseModel):
    title: Optional[str] = None
    author: Optional[str] = None
    publisher: Optional[str] = None
    publish_date: Optional[str] = None
    cover_type: Optional[str] = None
    page_count: Optional[int] = None
    standard_price_vnd: Optional[int] = None
    current_price_vnd: Optional[int] = None
    overview: Optional[str] = None
    keywords: Optional[List[str]] = None

# ---------------------------------------------------------
# STATE MANAGEMENT (Free-Tier Append Method)
# ---------------------------------------------------------
def update_link_status(url, domain, status, new_retry_count=0):
    """Appends a new status row instead of updating, bypassing DML limits."""
    timestamp = datetime.now(timezone.utc).isoformat()
    row = [{
        "url": url,
        "domain": domain,
        "status": status,
        "discovered_at": timestamp, 
        "last_visited_at": timestamp,
        "retry_count": new_retry_count
    }]
    bq_client.load_table_from_json(row, FRONTIER_TABLE).result()

def get_unvisited_link():
    """Finds the most recent status for each URL and grabs one UNVISITED link."""
    query = f"""
        WITH LatestStatus AS (
            SELECT url, domain, status, retry_count,
            ROW_NUMBER() OVER(PARTITION BY url ORDER BY discovered_at DESC) as rn
            FROM `{FRONTIER_TABLE}`
        )
        SELECT url, domain, retry_count FROM LatestStatus 
        WHERE rn = 1 AND status = 'UNVISITED' 
        LIMIT 1
    """
    results = list(bq_client.query(query).result())
    
    if not results:
        return None
        
    row = results[0]
    # Immediately append an IN_PROGRESS row to lock it
    update_link_status(row.url, row.domain, 'IN_PROGRESS', row.retry_count)
    return row

# ---------------------------------------------------------
# CORE FUNCTIONS
# ---------------------------------------------------------
async def scrape_dynamic_text(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0")
        page = await context.new_page()
        
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            
            for _ in range(3):
                await page.mouse.wheel(0, 1500) 
                await page.wait_for_timeout(1000) 
            try:
                await page.click("text='Xem Thêm'", timeout=1000)
                await page.wait_for_timeout(1000)
            except:
                pass 
                
            await page.evaluate("document.querySelectorAll('script, style, nav, footer, img').forEach(el => el.remove())")
            raw_text = await page.locator("body").inner_text()
            
        except Exception as e:
            print(f"      [!] Playwright failed to load page: {e}")
            raw_text = ""
            
        await browser.close()
        return raw_text[:30000]

def clean_data_with_ai(raw_text):
    prompt = f"""
    You are an expert librarian data assistant. Extract the following information from the raw Vietnamese text and return ONLY a valid JSON object. 
    If a piece of information is missing, use null. Preserve all Vietnamese accents perfectly.
    
    Required JSON Schema:
    {{
      "title": "Book Title", "author": "Author Name", "publisher": "Publisher Name",
      "publish_date": "YYYY-MM-DD or MM/YYYY", "cover_type": "Hardcover or Paperback",
      "page_count": 300, "standard_price_vnd": 150000, "current_price_vnd": 120000,
      "overview": "Summary...", "keywords": ["keyword1", "keyword2"]
    }}
    
    Raw text: {raw_text}
    """
    try:
        chat_completion = groq_client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You output strict JSON only."},
                {"role": "user", "content": prompt}
            ],
            model="llama-3.1-8b-instant", 
            temperature=0, 
            response_format={"type": "json_object"} 
        )
        return chat_completion.choices[0].message.content
    except Exception as e:
        print(f"      [!] Groq API Error: {e}")
        return "{}"

# ---------------------------------------------------------
# THE WORKER LOOP
# ---------------------------------------------------------
async def run_extractor_worker():
    print("==================================================")
    print("  STARTING LAYER 3: EXTRACTOR WORKER (CONSUMER) ")
    print("==================================================")
    
    max_empty_retries = 6  
    empty_retries = 0

    while True:
        target = get_unvisited_link()
        
        if not target:
            if empty_retries < max_empty_retries:
                print(f"[WORKER] Queue empty. Waiting 10s for new links... (Attempt {empty_retries+1}/{max_empty_retries})")
                await asyncio.sleep(10)
                empty_retries += 1
                continue
            else:
                print("[WORKER] Queue has been empty for 60 seconds. All books processed. Shutting down.")
                break
                
        empty_retries = 0
            
        target_url = target.url
        target_domain = target.domain
        retry_count = target.retry_count
        print(f"\n[WORKER] Picked up job from queue: {target_url}")
        
        raw_html_text = await scrape_dynamic_text(target_url)
        
        if not raw_html_text:
            print("-> SKIPPED: Could not extract text from page.")
            update_link_status(target_url, target_domain, "FAILED", retry_count + 1)
            continue
            
        print("-> Text scraped. Sending to Llama 3.1...")
        clean_json_str = clean_data_with_ai(raw_html_text)
        
        try:
            raw_record = json.loads(clean_json_str)
            clean_record = BookData(**raw_record)
            book_dict = clean_record.model_dump()
            
            if book_dict.get("title") is None:
                print("-> SKIPPED: AI found no valid title. Marking as FAILED.")
                update_link_status(target_url, target_domain, 'FAILED', retry_count + 1)
                continue
                
            try:
                job = bq_client.load_table_from_json([book_dict], DESTINATION_TABLE)
                job.result()
                print("-> Successfully saved book to BigQuery library_database!")
                update_link_status(target_url, target_domain, 'VISITED', retry_count)
                
            except Exception as e:
                print(f"-> [!] Database Insert Error: {e}")
                update_link_status(target_url, target_domain, 'FAILED', retry_count + 1)
                
        except ValidationError as e:
            print(f"-> [!] Pydantic rejected the AI's data format. Marking as FAILED.")
            update_link_status(target_url, target_domain, 'FAILED', retry_count + 1)
            
        except json.JSONDecodeError:
            print("-> [!] JSON Error from Groq. Marking as FAILED in queue.")
            update_link_status(target_url, target_domain, 'FAILED', retry_count + 1)

        print("-> Pausing 2 seconds for API limits...")
        await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(run_extractor_worker())
