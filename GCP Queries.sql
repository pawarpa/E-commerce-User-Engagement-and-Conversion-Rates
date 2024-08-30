-- Count of Visitors in a Specific Month
SELECT COUNT(fullVisitorId) AS row_num
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`

/* Purpose: This query counts the total number of unique visitors (fullVisitorId) in the dataset for July 2017.
Details:
COUNT(fullVisitorId): Counts the number of unique visitor IDs.
The dataset being queried is specifically for July 2017, as indicated by ga_sessions_201707*.
*/

-- Count of Visitors for All of 2017
SELECT COUNT(fullVisitorId) AS row_num
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`

/*
Purpose: This query counts the total number of unique visitors for the entire year of 2017.
Details:
COUNT(fullVisitorId): Counts the number of unique visitor IDs across all sessions for 2017.
The dataset includes data for the whole year of 2017, as indicated by ga_sessions_2017*.
*/

-- Monthly User Activity Analysis
SELECT EXTRACT(MONTH FROM PARSE_DATE("%Y%m%d", date)) AS month,
       COUNT(*) AS counts,
       ROUND((COUNT(*) / (SELECT COUNT(*) 
                          FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`)) * 100, 1) AS pct
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
GROUP BY EXTRACT(MONTH FROM PARSE_DATE("%Y%m%d", date))

/*
Purpose: This query provides a monthly breakdown of user sessions for the year 2017, including the percentage of total sessions for each month.
Details:
EXTRACT(MONTH FROM PARSE_DATE("%Y%m%d", date)) AS month: Extracts the month from the session date.
COUNT(*) AS counts: Counts the number of sessions for each month.
ROUND((COUNT(*) / (SELECT COUNT(*) FROM bigquery-public-data.google_analytics_sample.ga_sessions_2017*)) * 100, 1) AS pct: Calculates the percentage of total sessions for each month. This is done by dividing the monthly session count by the total number of sessions and then rounding to one decimal place.
GROUP BY EXTRACT(MONTH FROM PARSE_DATE("%Y%m%d", date)): Groups the results by month.
*/

-- Detailed Session and Product Information for July 2017
SELECT date, 
       fullVisitorId,
       eCommerceAction.action_type,
       product.v2ProductName,
       product.productRevenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
     UNNEST(hits) AS hits,
     UNNEST(hits.product) AS product

/* Purpose: This query retrieves detailed session and product-level information for July 2017.
Details:
date: The date of the session.
fullVisitorId: The unique identifier for the visitor.
eCommerceAction.action_type: The type of eCommerce action (e.g., view, add to cart, purchase).
product.v2ProductName: The name of the product.
product.productRevenue: The revenue associated with the product.
UNNEST(hits) AS hits: Expands the hits record to allow access to nested fields.
UNNEST(hits.product) AS product: Expands the product record to access product-specific fields within each hit.
*/

-- calculate total visits, pageview, transaction, and revenue for Jan, Feb, and March 2017
   SELECT 
    FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) month_extract
    ,SUM(totals.visits) visits
    ,SUM(totals.pageviews) pageviews
    ,SUM(totals.transactions) transactions
    ,ROUND(SUM(totals.totalTransactionRevenue)/POW(10,6),2) revenue
   FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
   WHERE _table_suffix BETWEEN '0101' AND '0331'
   GROUP BY month_extract

-- Bounce rate per traffic source in July 2017
SELECT trafficSource.source
       ,COUNT(visitNumber) total_visits
       ,SUM(totals.bounces) total_no_of_bounces
       ,ROUND((SUM(totals.bounces)/COUNT(visitNumber))*100,2) bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY trafficSource.source
ORDER BY total_visits DESC;

-- Revenue by traffic source by week, by month in June 2017
WITH GET_RE_MONTH AS 
(
    SELECT DISTINCT
        CASE WHEN 1=1 THEN "Month" END time_type,
        FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) AS time ,
        trafficSource.source AS source,
        ROUND(SUM(totals.totalTransactionRevenue/1000000) OVER(PARTITION BY trafficSource.source),2) revenue
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
),

GET_RE_WEEK AS 
(
    SELECT
        CASE WHEN 1=1 THEN "WEEK" END time_type,
        FORMAT_DATE("%Y%W", PARSE_DATE("%Y%m%d", date)) AS time,
        trafficSource.source AS source,
        SUM(totals.totalTransactionRevenue)/1000000 revenue
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    WHERE _table_suffix BETWEEN '0601' AND '0630'
    GROUP BY 1,2,3
)

SELECT * FROM GET_RE_MONTH
UNION ALL 
SELECT * FROM GET_RE_WEEK
ORDER BY revenue DESC;

-- Average number of pageviews by purchaser type
WITH GET_AVG_6_MONTH AS (SELECT
CASE WHEN 1 = 1 THEN "201706" END AS Month,
SUM(CASE WHEN totals.transactions >=1 THEN totals.transactions END ) AS total_transactions,
COUNT(DISTINCT(CASE WHEN totals.transactions >=1 THEN fullVisitorId END )) AS NUM_USER
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`)

SELECT 
Month,
ROUND(total_transactions/NUM_USER,2) as Avg_total_transactions_per_user
FROM GET_AVG_6_MONTH;

-- Average number of transactions per user that purchased in July 2017
WITH GET_AVG_7_MONTH AS (SELECT
CASE WHEN 1 = 1 THEN "201707" END AS Month,
SUM(CASE WHEN totals.transactions >=1 THEN totals.transactions END ) AS total_transactions,
COUNT(DISTINCT(CASE WHEN totals.transactions >=1 THEN fullVisitorId END )) AS NUM_USER
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`)

SELECT 
Month,
ROUND(total_transactions/NUM_USER,2) as Avg_total_transactions_per_user
FROM GET_AVG_7_MONTH;

-- Average amount of money spent per session. Only include purchaser data in 2017

SELECT 
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
  ROUND((SUM(product.productRevenue) / SUM(totals.visits))/1000000,2) AS Avg_revenue_by_user_per_visit
FROM 
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`, 
  UNNEST(hits) AS hits, 
  UNNEST(hits.product) AS product
WHERE 
  _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
  AND product.productRevenue IS NOT NULL
  AND totals.transactions IS NOT NULL
GROUP BY month;

-- Other products purchased by customers who purchased product” Youtube Men’s Vintage Henley” in July 2017
WITH GET_CUS_ID AS (SELECT DISTINCT fullVisitorId as Henley_CUSTOMER_ID
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST(hits) AS hits,
UNNEST(hits.product) as product
WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
AND product.productRevenue IS NOT NULL)

SELECT product.v2ProductName AS other_purchased_products,
       SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` TAB_A 
RIGHT JOIN GET_CUS_ID
ON GET_CUS_ID.Henley_CUSTOMER_ID=TAB_A.fullVisitorId,
UNNEST(hits) AS hits,
UNNEST(hits.product) as product
WHERE TAB_A.fullVisitorId IN (SELECT * FROM GET_CUS_ID)
    AND product.v2ProductName <> "YouTube Men's Vintage Henley"
    AND product.productRevenue IS NOT NULL
GROUP BY product.v2ProductName
ORDER BY QUANTITY DESC;

-- Calculate cohort map from product view to add_to_cart/number_product_view
WITH addtocart AS
(
       SELECT
       FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month
       ,COUNT(eCommerceAction.action_type) AS num_addtocart
       FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`   
               ,UNNEST (hits) AS hits
       WHERE _table_suffix BETWEEN '0101' AND '0331'
               AND eCommerceAction.action_type = '3'
       GROUP BY month 
)
   , productview AS
(
       SELECT
       FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month
       ,COUNT(eCommerceAction.action_type) AS num_product_view
       FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`   
               ,UNNEST (hits) AS hits
       WHERE _table_suffix BETWEEN '0101' AND '0331'
               AND eCommerceAction.action_type = '2'
       GROUP BY month 
)
   , id_purchase_revenue AS -- this is the first step to inspect the purchase step
(
               SELECT
       FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month
       ,fullVisitorId
       ,eCommerceAction.action_type
       ,product.productRevenue -- notice that not every purchase step that an ID made that the revenue was recorded (maybe refund?).
       FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`   
               ,UNNEST (hits) AS hits
               ,UNNEST (hits.product) AS product -- productrevenue 
       WHERE _table_suffix BETWEEN '0101' AND '0331'
               AND eCommerceAction.action_type = '6'
)
   , purchase AS
(
       SELECT 
           month
           ,COUNT(action_type) AS num_purchase
       FROM id_purchase_revenue 
       WHERE productRevenue IS NOT NULL
       GROUP BY month
)
SELECT 
       month
       ,num_product_view
       ,num_addtocart
       ,num_purchase
       ,ROUND(num_addtocart / num_product_view * 100.0, 2) AS add_to_cart_rate
       ,ROUND(num_purchase / num_product_view * 100.0, 2) AS purchase_rate
FROM productview
JOIN addtocart
USING (month)
JOIN purchase
USING (month)
ORDER BY month;





