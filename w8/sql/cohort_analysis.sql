-- =====================================================
-- Query 12: Year-over-Year Revenue Comparison
-- =====================================================

WITH monthly_revenue AS (

    SELECT
        CAST(strftime('%Y', o.order_date) AS INTEGER) AS year,
        CAST(strftime('%m', o.order_date) AS INTEGER) AS month,
        ROUND(
            SUM(
                oi.quantity *
                oi.unit_price *
                (1 - oi.discount_percent / 100.0)
            ),
            2
        ) AS revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    GROUP BY year, month

)

SELECT
    current.year,
    current.month,
    current.revenue,
    previous.revenue AS prev_year_revenue,
    CASE
        WHEN previous.revenue IS NULL OR previous.revenue = 0
            THEN NULL
        ELSE ROUND(
            ((current.revenue - previous.revenue) * 100.0)
            / previous.revenue,
            2
        )
    END AS yoy_growth_percent
FROM monthly_revenue current
LEFT JOIN monthly_revenue previous
ON current.month = previous.month
AND current.year = previous.year + 1
ORDER BY current.year, current.month;


-- =====================================================
-- Query 13: First Purchased Category vs Latest Purchased Category
-- =====================================================

WITH customer_categories AS (

    SELECT
        o.customer_id,
        p.category,
        o.order_date,
        ROW_NUMBER() OVER (
            PARTITION BY o.customer_id
            ORDER BY o.order_date
        ) AS first_order,
        ROW_NUMBER() OVER (
            PARTITION BY o.customer_id
            ORDER BY o.order_date DESC
        ) AS last_order
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    JOIN products p
        ON oi.product_id = p.product_id
    WHERE o.customer_id != 'UNKNOWN'

)

SELECT
    first.customer_id,
    first.category AS first_category,
    last.category AS latest_category,
    CASE
        WHEN first.category = last.category
            THEN 'No'
        ELSE 'Yes'
    END AS category_shift
FROM customer_categories first
JOIN customer_categories last
ON first.customer_id = last.customer_id
WHERE first.first_order = 1
AND last.last_order = 1;


-- =====================================================
-- Query 14: Cumulative Revenue Distribution
-- =====================================================

WITH customer_revenue AS (

    SELECT
        o.customer_id,
        ROUND(
            SUM(
                oi.quantity *
                oi.unit_price *
                (1 - oi.discount_percent / 100.0)
            ),
            2
        ) AS revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    WHERE o.customer_id != 'UNKNOWN'
    GROUP BY o.customer_id

)

SELECT
    customer_id,
    revenue,
    SUM(revenue) OVER (
        ORDER BY revenue DESC
    ) AS cumulative_revenue,
    ROUND(
        100.0 *
        SUM(revenue) OVER (
            ORDER BY revenue DESC
        ) /
        SUM(revenue) OVER (),
        2
    ) AS cumulative_percent
FROM customer_revenue
ORDER BY revenue DESC;


-- =====================================================
-- Query 15: Cohort Analysis
-- =====================================================

WITH first_purchase AS (

    SELECT
        customer_id,
        MIN(DATE(order_date)) AS first_order_date
    FROM orders
    WHERE customer_id != 'UNKNOWN'
    GROUP BY customer_id

),

cohort_orders AS (

    SELECT
        o.customer_id,
        strftime('%Y-%m', fp.first_order_date) AS cohort_month,

        (
            (CAST(strftime('%Y', o.order_date) AS INTEGER) -
             CAST(strftime('%Y', fp.first_order_date) AS INTEGER)) * 12
            +
            (CAST(strftime('%m', o.order_date) AS INTEGER) -
             CAST(strftime('%m', fp.first_order_date) AS INTEGER))
        ) AS month_number

    FROM orders o
    JOIN first_purchase fp
        ON o.customer_id = fp.customer_id

)

SELECT
    cohort_month,

    COUNT(DISTINCT CASE WHEN month_number = 0 THEN customer_id END) AS month0,
    COUNT(DISTINCT CASE WHEN month_number = 1 THEN customer_id END) AS month1,
    COUNT(DISTINCT CASE WHEN month_number = 2 THEN customer_id END) AS month2,
    COUNT(DISTINCT CASE WHEN month_number = 3 THEN customer_id END) AS month3,

    ROUND(
        100.0 *
        COUNT(DISTINCT CASE WHEN month_number = 1 THEN customer_id END)
        /
        NULLIF(
            COUNT(DISTINCT CASE WHEN month_number = 0 THEN customer_id END),
            0
        ),
        2
    ) AS retention_month1

FROM cohort_orders
GROUP BY cohort_month
ORDER BY cohort_month;


-- =====================================================
-- Query 16: Customer Revenue Comparison using LAG
-- =====================================================

WITH customer_sales AS (

    SELECT
        o.customer_id,
        ROUND(
            SUM(
                oi.quantity *
                oi.unit_price *
                (1 - oi.discount_percent / 100.0)
            ),
            2
        ) AS revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    WHERE o.customer_id != 'UNKNOWN'
    GROUP BY o.customer_id

)

SELECT
    customer_id,
    revenue,
    LAG(revenue) OVER (
        ORDER BY revenue DESC
    ) AS previous_customer_revenue,
    revenue -
    LAG(revenue) OVER (
        ORDER BY revenue DESC
    ) AS revenue_difference
FROM customer_sales
ORDER BY revenue DESC;