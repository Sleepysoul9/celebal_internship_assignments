-- =====================================================
-- Query 7: Running Revenue per Region
-- =====================================================

SELECT
    o.region,
    DATE(o.order_date) AS order_date,
    ROUND(
        SUM(
            oi.quantity *
            oi.unit_price *
            (1 - oi.discount_percent / 100.0)
        ),
        2
    ) AS daily_revenue,
    ROUND(
        SUM(
            SUM(
                oi.quantity *
                oi.unit_price *
                (1 - oi.discount_percent / 100.0)
            )
        ) OVER (
            PARTITION BY o.region
            ORDER BY DATE(o.order_date)
        ),
        2
    ) AS running_total
FROM orders o
JOIN order_items oi
    ON o.order_id = oi.order_id
GROUP BY
    o.region,
    DATE(o.order_date)
ORDER BY
    o.region,
    DATE(o.order_date);


-- =====================================================
-- Query 8: Rank Products by Revenue within Category
-- =====================================================

SELECT
    category,
    product_name,
    total_revenue,
    DENSE_RANK() OVER (
        PARTITION BY category
        ORDER BY total_revenue DESC
    ) AS rank_in_category
FROM (
    SELECT
        p.category,
        p.product_name,
        ROUND(
            SUM(
                oi.quantity *
                oi.unit_price *
                (1 - oi.discount_percent / 100.0)
            ),
            2
        ) AS total_revenue
    FROM products p
    JOIN order_items oi
        ON p.product_id = oi.product_id
    GROUP BY
        p.category,
        p.product_name
);


-- =====================================================
-- Query 9: Days Between Consecutive Orders
-- =====================================================

SELECT
    customer_id,
    order_date,
    LAG(order_date) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    ) AS previous_order_date,
    ROUND(
        julianday(order_date) -
        julianday(
            LAG(order_date) OVER (
                PARTITION BY customer_id
                ORDER BY order_date
            )
        ),
        2
    ) AS days_gap
FROM orders
WHERE customer_id != 'UNKNOWN';


-- =====================================================
-- Query 10: Monthly Revenue Customer Segmentation (CTE)
-- =====================================================

WITH monthly_revenue AS (

    SELECT
        o.customer_id,
        strftime('%Y-%m', o.order_date) AS month,
        SUM(
            oi.quantity *
            oi.unit_price *
            (1 - oi.discount_percent / 100.0)
        ) AS revenue
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    WHERE o.customer_id != 'UNKNOWN'
    GROUP BY
        o.customer_id,
        month

),

customer_segment AS (

    SELECT
        customer_id,
        month,
        CASE
            WHEN revenue > 10000 THEN 'High'
            WHEN revenue >= 5000 THEN 'Medium'
            ELSE 'Low'
        END AS segment
    FROM monthly_revenue

)

SELECT
    month,
    segment,
    COUNT(*) AS customers
FROM customer_segment
GROUP BY
    month,
    segment
ORDER BY
    month,
    segment;


-- =====================================================
-- Query 11: Customer Segmentation using NTILE
-- =====================================================

WITH customer_value AS (

    SELECT
        o.customer_id,
        ROUND(
            SUM(
                oi.quantity *
                oi.unit_price *
                (1 - oi.discount_percent / 100.0)
            ),
            2
        ) AS total_value
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    WHERE o.customer_id != 'UNKNOWN'
    GROUP BY o.customer_id

)

SELECT
    customer_id,
    total_value,
    NTILE(4) OVER (
        ORDER BY total_value DESC
    ) AS quartile,
    CASE
        WHEN NTILE(4) OVER (ORDER BY total_value DESC) = 1 THEN 'Platinum'
        WHEN NTILE(4) OVER (ORDER BY total_value DESC) = 2 THEN 'Gold'
        WHEN NTILE(4) OVER (ORDER BY total_value DESC) = 3 THEN 'Silver'
        ELSE 'Bronze'
    END AS quartile_label
FROM customer_value;