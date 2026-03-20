import asyncio
import json
import os
from datetime import datetime, timezone
from playwright.async_api import async_playwright
from google.cloud import bigquery

# 1. Authenticate with GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"
bq_client = bigquery.Client()
PROJECT_ID = bq_client.project

# Point to the new memory bank
FRONTIER_TABLE = f"{PROJECT_ID}.book_scraping.crawl_frontier"

def load_config():
    with open("sites_config.json", "r") as f:
        return json.load(f)

async def run_discovery():
    print("==================================================")
    print("  STARTING LAYER 1: DISCOVERY BOT (SPIDER)")
    print("==================================================")
    
    config = load_config()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0")
        page = await context.new_page()
        
        # Loop through each bookstore in the config file
        for site_name, site_data in config.items():
            print(f"\n[*] Scanning site: {site_name.upper()}")
            domain = site_data["domain"]
            book_link_selector = site_data["selectors"]["book_link"]
            
            for seed_url in site_data["seed_urls"]:
                print(f" -> Visiting seed: {seed_url}")
                try:
                    await page.goto(seed_url, wait_until="domcontentloaded", timeout=60000)
                    
                    # Scroll to trigger lazy-loaded images/links
                    for _ in range(3):
                        await page.mouse.wheel(0, 1000)
                        await page.wait_for_timeout(1000)
                    
                    # Extract links using the selector from the JSON file
                    elements = await page.locator(book_link_selector).element_handles()
                    
                    found_links = []
                    for el in elements:
                        href = await el.get_attribute("href")
                        if href:
                            # Handle relative URLs (e.g., /book-name -> https://tiki.vn/book-name)
                            if href.startswith("/"):
                                href = f"https://{domain}{href}"
                            # Remove tracking parameters like ?spid=123
                            href = href.split("?")[0] 
                            
                            if href not in found_links:
                                found_links.append(href)
                                
                    print(f" -> Found {len(found_links)} unique book links.")
                    
                    # 2. Push the UNVISITED links to BigQuery
                    if found_links:
                        rows_to_insert = []
                        timestamp = datetime.now(timezone.utc).isoformat()
                        
                        for link in found_links:
                            rows_to_insert.append({
                                "url": link,
                                "domain": domain,
                                "status": "UNVISITED",
                                "discovered_at": timestamp,
                                "last_visited_at": None,
                                "retry_count": 0
                            })
                        
                        # Load data into the table
                        job = bq_client.load_table_from_json(rows_to_insert, FRONTIER_TABLE)
                        job.result() 
                        print(f" -> Successfully saved {len(found_links)} links to crawl_frontier!")
                            
                except Exception as e:
                    print(f" -> [!] Error scanning {seed_url}: {e}")
                    
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_discovery())
