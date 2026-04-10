-- ============================================================
-- SQL Analytics Views — Microsoft Fabric Retail Sales Lakehouse
-- Run in: Fabric SQL Analytics Endpoint
-- ============================================================

-- ── View 1: Executive KPIs ────────────────────────────────────
CREATE OR ALTER VIEW vw_executive_kpis AS
SELECT
    COUNT(DISTINCT transaction_id)              AS total_transactions,
    COUNT(DISTINCT customer_id)                 AS unique_customers,
    ROUND(SUM(total_amount), 2)                 AS total_revenue,
    ROUND(SUM(profit_amount), 2)                AS total_profit,
    ROUND(AVG(total_amount), 2)                 AS avg_order_value,
    SUM(quantity)                               AS total_units_sold,
    ROUND(SUM(profit_amount)/SUM(total_amount)*100, 2) AS profit_margin_pct,
    SUM(CAST(return_flag AS INT))               AS total_returns,
    ROUND(SUM(CAST(return_flag AS INT))*100.0/COUNT(*), 2) AS return_rate_pct
FROM gold_fact_sales;

-- ── View 2: Revenue by category ───────────────────────────────
CREATE OR ALTER VIEW vw_revenue_by_category AS
SELECT
    category,
    ROUND(SUM(total_amount), 2)             AS total_revenue,
    ROUND(SUM(profit_amount), 2)            AS total_profit,
    SUM(quantity)                           AS units_sold,
    COUNT(DISTINCT transaction_id)          AS transactions,
    COUNT(DISTINCT customer_id)             AS unique_customers,
    ROUND(AVG(discount_pct)*100, 1)         AS avg_discount_pct,
    ROUND(SUM(profit_amount)/SUM(total_amount)*100, 2) AS margin_pct
FROM gold_fact_sales
GROUP BY category;

-- ── View 3: Monthly revenue trend ─────────────────────────────
CREATE OR ALTER VIEW vw_monthly_trend AS
SELECT
    year, month, month_name,
    ROUND(SUM(total_amount), 2)             AS monthly_revenue,
    ROUND(SUM(profit_amount), 2)            AS monthly_profit,
    COUNT(DISTINCT transaction_id)          AS order_count,
    COUNT(DISTINCT customer_id)             AS active_customers,
    ROUND(AVG(total_amount), 2)             AS avg_order_value,
    SUM(quantity)                           AS units_sold
FROM gold_fact_sales
GROUP BY year, month, month_name;

-- ── View 4: Top customers ──────────────────────────────────────
CREATE OR ALTER VIEW vw_top_customers AS
SELECT TOP 50
    customer_id, full_name, city, state,
    loyalty_tier, age_band,
    ROUND(SUM(total_amount), 2)             AS total_spend,
    COUNT(DISTINCT transaction_id)          AS total_orders,
    ROUND(AVG(total_amount), 2)             AS avg_order_value,
    MAX(full_date)                          AS last_purchase_date
FROM gold_fact_sales
GROUP BY customer_id, full_name, city, state, loyalty_tier, age_band
ORDER BY total_spend DESC;

-- ── View 5: Store performance ──────────────────────────────────
CREATE OR ALTER VIEW vw_store_performance AS
SELECT
    store_id, store_name, store_type, region,
    ROUND(SUM(total_amount), 2)             AS total_revenue,
    ROUND(SUM(profit_amount), 2)            AS total_profit,
    COUNT(DISTINCT transaction_id)          AS transaction_count,
    COUNT(DISTINCT customer_id)             AS unique_customers,
    ROUND(AVG(total_amount), 2)             AS avg_order_value,
    ROUND(SUM(profit_amount)/SUM(total_amount)*100, 2) AS margin_pct
FROM gold_fact_sales
GROUP BY store_id, store_name, store_type, region;

-- ============================================================
-- Analytics Queries — for interview demos
-- ============================================================

-- Q1: Total revenue and profit summary
SELECT * FROM vw_executive_kpis;

-- Q2: Top 5 revenue-generating categories
SELECT TOP 5 category, total_revenue, margin_pct, units_sold
FROM vw_revenue_by_category
ORDER BY total_revenue DESC;

-- Q3: Month-over-month revenue growth
SELECT
    year, month, month_name,
    monthly_revenue,
    LAG(monthly_revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
    ROUND(
        (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY year, month))
        / LAG(monthly_revenue) OVER (ORDER BY year, month) * 100, 1
    ) AS mom_growth_pct
FROM vw_monthly_trend
ORDER BY year, month;

-- Q4: Revenue by channel
SELECT
    channel,
    COUNT(DISTINCT transaction_id)      AS orders,
    ROUND(SUM(total_amount), 2)         AS revenue,
    ROUND(AVG(total_amount), 2)         AS avg_order_value
FROM gold_fact_sales
GROUP BY channel
ORDER BY revenue DESC;

-- Q5: Weekend vs weekday sales
SELECT
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(DISTINCT transaction_id)      AS transactions,
    ROUND(SUM(total_amount), 2)         AS total_revenue,
    ROUND(AVG(total_amount), 2)         AS avg_order_value
FROM gold_fact_sales
GROUP BY is_weekend;

-- Q6: Customer loyalty tier breakdown
SELECT
    loyalty_tier,
    COUNT(DISTINCT customer_id)         AS customers,
    ROUND(SUM(total_amount), 2)         AS revenue,
    ROUND(AVG(total_amount), 2)         AS avg_order_value,
    ROUND(SUM(total_amount)*100.0
        / SUM(SUM(total_amount)) OVER(), 1) AS revenue_pct
FROM gold_fact_sales
GROUP BY loyalty_tier
ORDER BY revenue DESC;

-- Q7: Top 10 best-selling items
SELECT TOP 10
    item_name, category,
    SUM(quantity)                       AS units_sold,
    ROUND(SUM(total_amount), 2)         AS revenue,
    ROUND(AVG(discount_pct)*100, 1)     AS avg_discount_pct
FROM gold_fact_sales
GROUP BY item_name, category
ORDER BY revenue DESC;

-- Q8: Regional performance
SELECT
    region,
    COUNT(DISTINCT store_id)            AS store_count,
    ROUND(SUM(total_amount), 2)         AS total_revenue,
    COUNT(DISTINCT customer_id)         AS customers,
    ROUND(AVG(total_amount), 2)         AS avg_order_value
FROM gold_fact_sales
GROUP BY region
ORDER BY total_revenue DESC;
