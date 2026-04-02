-- ==========================================
-- SMART VIRTUAL LIBRARY: BIGQUERY SCHEMA
-- Run these queries in your BigQuery Workspace after replacing 'YOUR_PROJECT_ID' with your actual project ID.
-- ==========================================

-- 1. Category gap level table
CREATE VIEW `book-scraper-db-490703.book_scraping.v_gap_category_level`
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-25T04:09:12.335Z"
)
AS SELECT 
  m.category,
  COUNT(m.book_id) AS total_market_supply,
  COUNT(l.title) AS library_owned_count,
  -- The Mathematical Gap: Market Supply minus Library Inventory
  (COUNT(m.book_id) - COUNT(l.title)) AS collection_gap_volume
FROM `book-scraper-db-490703.book_scraping.v_library_master_catalog` m
-- THE LEFT JOIN: Matches market books to your library inventory
LEFT JOIN `book-scraper-db-490703.book_scraping.library_owned_books` l 
  ON m.title = l.title
WHERE m.category IS NOT NULL
GROUP BY m.category
ORDER BY collection_gap_volume DESC;

-- 2. Publisher table
CREATE VIEW `book-scraper-db-490703.book_scraping.dim_publishers`
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-22T09:57:18.830Z"
)
AS SELECT DISTINCT
  CAST(FARM_FINGERPRINT(publisher) AS STRING) AS publisher_id,
  publisher AS publisher_name
FROM `book-scraper-db-490703.book_scraping.v_library_cleaned`;

-- 3. Master data table
CREATE VIEW `book-scraper-db-490703.book_scraping.v_library_master_catalog`
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-24T07:53:43.016Z"
)
AS SELECT
  b.book_id,
  b.title,
  a.author_name,
  p.publisher_name,
  c.udc_code,               
  c.udc_name AS category,   
  c.categorized_at,         
  b.publish_date,
  b.standard_price_vnd,     
  b.current_price_vnd,
  -- Making them available to the Master View
  IFNULL(b.rating_score, 0.0) AS rating_score,
  IFNULL(b.review_count, 0) AS review_count,
  IFNULL(b.is_bestseller, FALSE) AS is_bestseller,
  (SELECT STRING_AGG(k.keyword_name, ', ') 
   FROM `book-scraper-db-490703.book_scraping.bridge_book_keywords` br
   JOIN `book-scraper-db-490703.book_scraping.dim_keywords` k ON br.keyword_id = k.keyword_id
   WHERE br.book_id = b.book_id) AS all_keywords
FROM `book-scraper-db-490703.book_scraping.fact_book_inventory` b
LEFT JOIN `book-scraper-db-490703.book_scraping.dim_authors` a ON b.author_id = a.author_id
LEFT JOIN `book-scraper-db-490703.book_scraping.dim_publishers` p ON b.publisher_id = p.publisher_id
LEFT JOIN `book-scraper-db-490703.book_scraping.ai_book_categories` c ON b.book_id = c.book_id;

-- 4. Owned books table
CREATE TABLE `book-scraper-db-490703.book_scraping.library_owned_books`
(
  title STRING,
  owned_copies INT64
)
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-25T04:04:16.899Z"
);

-- 5. AI Specialist's analysis table
CREATE TABLE `book-scraper-db-490703.book_scraping.ai_market_insights`
(
  trending_categories STRING,
  business_recommendation STRING,
  analyzed_at TIMESTAMP,
  insight_id STRING
)
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-25T05:33:57.309Z"
);

-- 6. Price alert table
CREATE VIEW `book-scraper-db-490703.book_scraping.v_procurement_alerts`
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-24T06:53:13.631Z"
)
AS SELECT
  title,
  author_name,
  category,
  standard_price_vnd,
  current_price_vnd,
  -- Calculate exactly how much money is saved per book
  (standard_price_vnd - current_price_vnd) AS savings_vnd,
  -- Calculate the exact percentage of the discount (e.g., 25.5%)
  ROUND(((standard_price_vnd - current_price_vnd) / standard_price_vnd) * 100, 1) AS discount_percentage,
  -- Pull in the timestamp so we know exactly when this deal was spotted
  categorized_at AS deal_spotted_at
FROM `book-scraper-db-490703.book_scraping.v_library_master_catalog`
-- 1. Ensure the math doesn't break (no division by zero)
WHERE standard_price_vnd > 0 
  AND current_price_vnd > 0
-- 2. Ensure it's actually on sale
  AND current_price_vnd < standard_price_vnd
-- 3. THE TRIGGER: Only show books where the discount is 20% (0.20) or higher
  AND ((standard_price_vnd - current_price_vnd) / standard_price_vnd) >= 0.20
-- Put the biggest discounts right at the top of the inbox
ORDER BY discount_percentage DESC;

-- 7. Keywords table (book's keywords)
CREATE VIEW `book-scraper-db-490703.book_scraping.dim_keywords`
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-22T09:57:20.088Z"
)
AS SELECT DISTINCT
  CAST(FARM_FINGERPRINT(keyword) AS STRING) AS keyword_id,
  keyword AS keyword_name
FROM `book-scraper-db-490703.book_scraping.v_library_cleaned`,
UNNEST(keywords) AS keyword;

-- 8. Book categories table
CREATE TABLE `book-scraper-db-490703.book_scraping.ai_book_categories`
(
  book_id STRING,
  udc_code STRING,
  udc_name STRING,
  categorized_at TIMESTAMP
)
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-23T08:45:52.666Z"
);

-- 9. UDC book indexing table
CREATE VIEW `book-scraper-db-490703.book_scraping.dim_udc_categories`
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-24T03:11:26.990Z"
)
AS SELECT * FROM UNNEST([
  STRUCT('0' AS udc_code, 'Science and Knowledge. Computer Science' AS udc_name),
  STRUCT('1' AS udc_code, 'Philosophy. Psychology' AS udc_name),
  STRUCT('2' AS udc_code, 'Religion. Theology' AS udc_name),
  STRUCT('3' AS udc_code, 'Social Sciences' AS udc_name),
  STRUCT('5' AS udc_code, 'Mathematics. Natural Sciences' AS udc_name),
  STRUCT('6' AS udc_code, 'Applied Sciences. Medicine. Technology' AS udc_name),
  STRUCT('7' AS udc_code, 'The Arts. Recreation. Entertainment. Sport' AS udc_name),
  STRUCT('8' AS udc_code, 'Language. Linguistics. Literature' AS udc_name),
  STRUCT('9' AS udc_code, 'Geography. Biography. History' AS udc_name)
]);

-- 10. Purchased books table
CREATE TABLE `book-scraper-db-490703.book_scraping.purchased_books`
(
  title STRING,
  action STRING,
  action_at TIMESTAMP
)
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-30T03:03:38.240Z"
);

-- 11. Book inventory table
CREATE VIEW `book-scraper-db-490703.book_scraping.fact_book_inventory`
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-24T07:49:20.817Z"
)
AS SELECT 
  CAST(FARM_FINGERPRINT(c.title) AS STRING) AS book_id,
  c.title,
  a.author_id,
  p.publisher_id,
  c.publish_date,
  c.cover_type,
  c.page_count,
  c.standard_price_vnd,
  c.current_price_vnd,
  c.overview,
  -- Passing the columns from the clean view
  c.rating_score,
  c.review_count,
  c.is_bestseller
FROM `book-scraper-db-490703.book_scraping.v_library_cleaned` c
LEFT JOIN `book-scraper-db-490703.book_scraping.dim_authors` a ON c.author = a.author_name
LEFT JOIN `book-scraper-db-490703.book_scraping.dim_publishers` p ON c.publisher = p.publisher_name;

-- 12. Storing book links table
CREATE TABLE `book-scraper-db-490703.book_scraping.crawl_frontier`
(
  url STRING NOT NULL,
  domain STRING,
  status STRING,
  discovered_at TIMESTAMP,
  last_visited_at TIMESTAMP,
  retry_count INT64
)
CLUSTER BY status
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-19T09:32:42.175Z"
);

-- 13. Cleaned data table (exclude duplicates)
CREATE VIEW `book-scraper-db-490703.book_scraping.v_library_cleaned`
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-24T07:45:06.623Z"
)
AS WITH DeduplicatedBooks AS (
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY title ORDER BY extracted_at DESC NULLS LAST, current_price_vnd DESC) as rn
  FROM `book-scraper-db-490703.book_scraping.library_database`
  WHERE title IS NOT NULL 
)
SELECT
  title,
  IFNULL(author, 'Unknown Author') AS author,
  IFNULL(publisher, 'Unknown Publisher') AS publisher,
  IFNULL(publish_date, 'Unknown Date') AS publish_date,
  IFNULL(cover_type, 'Unknown Type') AS cover_type,
  IFNULL(page_count, 0) AS page_count,
  IFNULL(standard_price_vnd, 0) AS standard_price_vnd,
  IFNULL(current_price_vnd, 0) AS current_price_vnd,
  overview,
  keywords,
  extracted_at,
  -- Pulling the new columns from the raw table
  rating_score,
  review_count,
  is_bestseller
FROM DeduplicatedBooks
WHERE rn = 1;

-- 14. Book database
CREATE TABLE `book-scraper-db-490703.book_scraping.library_database`
(
  title STRING,
  author STRING,
  publisher STRING,
  publish_date STRING,
  cover_type STRING,
  page_count INT64,
  standard_price_vnd INT64,
  current_price_vnd INT64,
  overview STRING,
  keywords ARRAY<STRING>,
  extracted_at STRING,
  rating_score FLOAT64,
  review_count INT64,
  is_bestseller BOOL
)
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-19T09:33:11.427Z"
);

-- 15. Book gap level table (analyze inventory gap to book level instead of just category level)
CREATE VIEW `book-scraper-db-490703.book_scraping.v_gap_book_level`
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-25T04:15:41.328Z"
)
AS SELECT 
  m.title,
  m.author_name,
  m.category,
  m.publish_date,
  m.current_price_vnd,
  -- We include the proxies just for context, even if they are null
  m.rating_score,
  m.review_count,
  m.is_bestseller
FROM `book-scraper-db-490703.book_scraping.v_library_master_catalog` m
LEFT JOIN `book-scraper-db-490703.book_scraping.library_owned_books` l 
  ON m.title = l.title
-- THE CRITICAL FILTER: Only show books the library DOES NOT own
WHERE l.title IS NULL 
ORDER BY 
  m.is_bestseller DESC,                  -- Priority 1: Proven Bestsellers
  m.review_count DESC NULLS LAST,        -- Priority 2: Proven Demand
  m.publish_date DESC NULLS LAST         -- Priority 3: Freshness / New Releases (The Null Savior!)
LIMIT 100;

-- 17. Top books table
CREATE VIEW `book-scraper-db-490703.book_scraping.v_market_top_books`
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-25T03:53:34.389Z"
)
AS SELECT 
  title,
  author_name,
  category,
  all_keywords,
  rating_score,
  review_count,
  is_bestseller,
  current_price_vnd
FROM `book-scraper-db-490703.book_scraping.v_library_master_catalog`
-- Filter out the noise: Only look at books that have proven reader engagement
WHERE review_count > 0 OR is_bestseller = TRUE
-- Rank them heavily by demand proxies
ORDER BY 
  is_bestseller DESC, 
  review_count DESC, 
  rating_score DESC
-- Limit to the top 100 highest-demand books in the entire market to fit the AI's context window
LIMIT 100;

-- 18. Bridge keyword table
CREATE VIEW `book-scraper-db-490703.book_scraping.bridge_book_keywords`
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-22T09:57:20.523Z"
)
AS SELECT
  CAST(FARM_FINGERPRINT(title) AS STRING) AS book_id,
  CAST(FARM_FINGERPRINT(keyword) AS STRING) AS keyword_id
FROM `book-scraper-db-490703.book_scraping.v_library_cleaned`,
UNNEST(keywords) AS keyword;

-- 19. Author table
CREATE VIEW `book-scraper-db-490703.book_scraping.dim_authors`
OPTIONS(
  expiration_timestamp=TIMESTAMP "2026-05-22T09:57:17.934Z"
)
AS SELECT DISTINCT
  CAST(FARM_FINGERPRINT(author) AS STRING) AS author_id,
  author AS author_name
FROM `book-scraper-db-490703.book_scraping.v_library_cleaned`;
