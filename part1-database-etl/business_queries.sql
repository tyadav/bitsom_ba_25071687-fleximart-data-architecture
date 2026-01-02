-- Business Queries for FlexiMart
-- Location: sql/business_queries.sql
-- Contains: Query 1 (Customer Purchase History), 
-- Query 2 (Product Sales Analysis)
-- Query 3 (Monthly Sales Trend with Window Function)

-- Query 1: Customer Purchase History
-- Business question: For each customer, show name, email, total_orders, total_spent.
-- Include only customers with at least 2 orders and total_spent > 5000. Order by total_spent desc.

SELECT
  CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
  c.email,
  COUNT(DISTINCT o.order_id)        AS total_orders,
  ROUND(SUM(oi.subtotal), 2)        AS total_spent
FROM customers c
JOIN orders o     ON o.customer_id = c.customer_id
JOIN order_items oi ON oi.order_id = o.order_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email
HAVING COUNT(DISTINCT o.order_id) >= 2
   AND SUM(oi.subtotal) > 5000
ORDER BY total_spent DESC;

-- =====================================================================

-- Query 2: Product Sales Analysis
-- Business question: For each product category, show category name, number of distinct products sold,
-- total quantity sold, and total revenue. Include only categories with revenue > 10000. Order by revenue desc.

SELECT
  p.category                                     AS category,
  COUNT(DISTINCT p.product_id)                   AS num_products,
  SUM(oi.quantity)                               AS total_quantity_sold,
  ROUND(SUM(CAST(oi.subtotal AS DECIMAL(14,2))), 2) AS total_revenue
FROM products p
JOIN order_items oi
  ON oi.product_id = p.product_id
GROUP BY p.category
HAVING SUM(CAST(oi.subtotal AS DECIMAL(14,2))) > 10000
ORDER BY total_revenue DESC;

-- Query 3: Monthly Sales Trend (2024)
-- Business question: Show monthly sales trends for the year 2024. For each month, display the month name,
-- total number of orders, total revenue (monthly_revenue), and the running total of revenue (cumulative_revenue).
-- Uses window function (SUM() OVER) to compute cumulative revenue and extracts month from order_date.

-- MySQL (8+) / compatible:
SELECT
  DATE_FORMAT(o.order_date, '%M')                                      AS month_name,
  COUNT(DISTINCT o.order_id)                                            AS total_orders,
  ROUND(SUM(oi.subtotal), 2)                                            AS monthly_revenue,
  ROUND(SUM(SUM(oi.subtotal)) OVER (ORDER BY MONTH(o.order_date) ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS cumulative_revenue
FROM orders o
JOIN order_items oi ON oi.order_id = o.order_id
WHERE YEAR(o.order_date) = 2024
GROUP BY MONTH(o.order_date), DATE_FORMAT(o.order_date, '%M')
ORDER BY MONTH(o.order_date) ASC;

-- In MySQL (use TO_CHAR for month names and EXTRACT for month number):
-- SELECT
--   TO_CHAR(o.order_date, 'Month') AS month_name,
--   COUNT(DISTINCT o.order_id)      AS total_orders,
--   ROUND(SUM(oi.subtotal)::numeric, 2) AS monthly_revenue,
--   ROUND(SUM(SUM(oi.subtotal)) OVER (ORDER BY EXTRACT(MONTH FROM o.order_date) ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::numeric, 2) AS cumulative_revenue
-- FROM orders o
-- JOIN order_items oi ON oi.order_id = o.order_id
-- WHERE EXTRACT(YEAR FROM o.order_date) = 2024
-- GROUP BY EXTRACT(MONTH FROM o.order_date), TO_CHAR(o.order_date, 'Month')
-- ORDER BY EXTRACT(MONTH FROM o.order_date) ASC;

-- Notes:
-- - These queries are written to work on both MySQL and Postgres broadly; 
-- - To run: connect to your DB (e.g., mysql or psql) and source this file or paste the query.
