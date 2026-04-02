-- ==========================================
-- SMART VIRTUAL LIBRARY: BIGQUERY SCHEMA
-- Find & Replace 'YOUR_PROJECT_ID' with your actual GCP Project ID before running!
-- ==========================================

-- ==========================================
-- PHASE 1: BASE TABLES (No Dependencies)
-- ==========================================

-- 1. Storing book links table (The Spider's Memory)
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.book_scraping.crawl_frontier` (
  url STRING NOT NULL,
  domain STRING,
  status STRING,
  discovered_at TIMESTAMP,
  last_visited_at TIMESTAMP,
  retry_count INT64
) CLUSTER BY status;

-- 2. Book database (Raw Extracted Data)
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.book_scraping.library_database` (
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
  extracted_at TIMESTAMP, 
  rating_score FLOAT64,
  review_count INT64,
  is_bestseller BOOL
);

-- 3. Book categories table (AI Librarian Output)
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.book_scraping.ai_book_categories` (
  book_id STRING,
  udc_code STRING,
  udc_name STRING,
  categorized_at TIMESTAMP
);

-- 4. Purchased books table (Interactive Dashboard Ledger)
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.book_scraping.purchased_books` (
  title STRING,
  action STRING,
  action_at TIMESTAMP
);

-- 5. Owned books table (Legacy/Static Inventory)
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.book_scraping.library_owned_books` (
  title STRING,
  owned_copies INT64
);

-- 6. AI Specialist's analysis table
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.book_scraping.ai_market_insights` (
  trending_categories STRING,
  business_recommendation STRING,
  analyzed_at TIMESTAMP,
  insight_id STRING
);


-- ==========================================
-- PHASE 2: LEVEL 1 VIEWS (Read from Base Tables)
-- ==========================================

-- 7. Cleaned data table (exclude duplicates)
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.book_scraping.v_library_cleaned` AS 
WITH DeduplicatedBooks AS (
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY title ORDER BY extracted_at DESC NULLS LAST, current_price_vnd DESC) as rn
  FROM `YOUR_PROJECT_ID.book_scraping.library_database`
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
  rating_score,
  review_count,
  is_bestseller
FROM DeduplicatedBooks
WHERE rn = 1;

-- 8. UDC book indexing table (Standalone)
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.book_scraping.dim_udc_categories` AS 
SELECT * FROM UNNEST([
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


-- ==========================================
-- PHASE 3: LEVEL 2 VIEWS (Dimensional Models)
-- ==========================================

-- 9. Author table
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.book_scraping.dim_authors` AS 
SELECT DISTINCT
  CAST(FARM_FINGERPRINT(author) AS STRING) AS author_id,
  author AS author_name
FROM `YOUR_PROJECT_ID.book_scraping.v_library_cleaned`;

-- 10. Publisher table
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.book_scraping.dim_publishers` AS 
SELECT DISTINCT
  CAST(FARM_FINGERPRINT(publisher) AS STRING) AS publisher_id,
  publisher AS publisher_name
FROM `YOUR_PROJECT_ID.book_scraping.v_library_cleaned`;

-- 11. Keywords table
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.book_scraping.dim_keywords` AS 
SELECT DISTINCT
  CAST(FARM_FINGERPRINT(keyword) AS STRING) AS keyword_id,
  keyword AS keyword_name
FROM `YOUR_PROJECT_ID.book_scraping.v_library_cleaned`,
UNNEST(keywords) AS keyword;

-- 12. Bridge keyword table
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.book_scraping.bridge_book_keywords` AS 
SELECT
  CAST(FARM_FINGERPRINT(title) AS STRING) AS book_id,
  CAST(FARM_FINGERPRINT(keyword) AS STRING) AS keyword_id
FROM `YOUR_PROJECT_ID.book_scraping.v_library_cleaned`,
UNNEST(keywords) AS keyword;


-- ==========================================
-- PHASE 4: LEVEL 3 VIEWS (Fact Models)
-- ==========================================

-- 13. Book inventory table
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.book_scraping.fact_book_inventory` AS 
SELECT 
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
  c.rating_score,
  c.review_count,
  c.is_bestseller
FROM `YOUR_PROJECT_ID.book_scraping.v_library_cleaned` c
LEFT JOIN `YOUR_PROJECT_ID.book_scraping.dim_authors` a ON c.author = a.author_name
LEFT JOIN `YOUR_PROJECT_ID.book_scraping.dim_publishers` p ON c.publisher = p.publisher_name;


-- ==========================================
-- PHASE 5: LEVEL 4 VIEWS (The Master Hub)
-- ==========================================

-- 14. Master data table
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.book_scraping.v_library_master_catalog` AS 
SELECT
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
  IFNULL(b.rating_score, 0.0) AS rating_score,
  IFNULL(b.review_count, 0) AS review_count,
  IFNULL(b.is_bestseller, FALSE) AS is_bestseller,
  (SELECT STRING_AGG(k.keyword_name, ', ') 
   FROM `YOUR_PROJECT_ID.book_scraping.bridge_book_keywords` br
   JOIN `YOUR_PROJECT_ID.book_scraping.dim_keywords` k ON br.keyword_id = k.keyword_id
   WHERE br.book_id = b.book_id) AS all_keywords
FROM `YOUR_PROJECT_ID.book_scraping.fact_book_inventory` b
LEFT JOIN `YOUR_PROJECT_ID.book_scraping.dim_authors` a ON b.author_id = a.author_id
LEFT JOIN `YOUR_PROJECT_ID.book_scraping.dim_publishers` p ON b.publisher_id = p.publisher_id
LEFT JOIN `YOUR_PROJECT_ID.book_scraping.ai_book_categories` c ON b.book_id = c.book_id;


-- ==========================================
-- PHASE 6: LEVEL 5 VIEWS (Analytics & Dashboards)
-- ==========================================

-- 15. Category gap level table
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.book_scraping.v_gap_category_level` AS 
SELECT 
  m.category,
  COUNT(m.book_id) AS total_market_supply,
  COUNT(l.title) AS library_owned_count,
  (COUNT(m.book_id) - COUNT(l.title)) AS collection_gap_volume
FROM `YOUR_PROJECT_ID.book_scraping.v_library_master_catalog` m
LEFT JOIN `YOUR_PROJECT_ID.book_scraping.library_owned_books` l ON m.title = l.title
WHERE m.category IS NOT NULL
GROUP BY m.category
ORDER BY collection_gap_volume DESC;

-- 16. Book gap level table
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.book_scraping.v_gap_book_level` AS 
SELECT 
  m.title,
  m.author_name,
  m.category,
  m.publish_date,
  m.current_price_vnd,
  m.rating_score,
  m.review_count,
  m.is_bestseller
FROM `YOUR_PROJECT_ID.book_scraping.v_library_master_catalog` m
LEFT JOIN `YOUR_PROJECT_ID.book_scraping.library_owned_books` l ON m.title = l.title
WHERE l.title IS NULL 
ORDER BY 
  m.is_bestseller DESC, 
  m.review_count DESC NULLS LAST, 
  m.publish_date DESC NULLS LAST         
LIMIT 100;

-- 17. Top books table
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.book_scraping.v_market_top_books` AS 
SELECT 
  title, author_name, category, all_keywords, rating_score, review_count, is_bestseller, current_price_vnd
FROM `YOUR_PROJECT_ID.book_scraping.v_library_master_catalog`
WHERE review_count > 0 OR is_bestseller = TRUE
ORDER BY is_bestseller DESC, review_count DESC, rating_score DESC
LIMIT 100;

-- 18. Price alert table
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.book_scraping.v_procurement_alerts` AS 
SELECT
  title, author_name, category, standard_price_vnd, current_price_vnd,
  (standard_price_vnd - current_price_vnd) AS savings_vnd,
  ROUND(((standard_price_vnd - current_price_vnd) / standard_price_vnd) * 100, 1) AS discount_percentage,
  categorized_at AS deal_spotted_at
FROM `YOUR_PROJECT_ID.book_scraping.v_library_master_catalog`
WHERE standard_price_vnd > 0 
  AND current_price_vnd > 0
  AND current_price_vnd < standard_price_vnd
  AND ((standard_price_vnd - current_price_vnd) / standard_price_vnd) >= 0.20
ORDER BY discount_percentage DESC;
