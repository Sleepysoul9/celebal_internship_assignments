-- =====================================================
-- Query 1: Total Revenue per Category
-- =====================================================

SELECT
    p.category,
    ROUND(
        SUM(
            oi.quantity *
            oi.unit_price *
            (1 - oi.discount_percent / 100.0)
        ),
        2
    ) AS total_revenue
FROM order_items oi
JOIN products p
    ON oi.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;


-- =====================================================
-- Query 2: Top 10 Customers by Total Order Value
-- =====================================================

SELECT
    c.customer_id,
    c.customer_name,
    ROUND(
        SUM(
            oi.quantity *
            oi.unit_price *
            (1 - oi.discount_percent / 100.0)
        ),
        2
    ) AS total_order_value
FROM customers c
JOIN orders o
    ON c.customer_id = o.customer_id
JOIN order_items oi
    ON o.order_id = oi.order_id
GROUP BY
    c.customer_id,
    c.customer_name
ORDER BY total_order_value DESC
LIMIT 10;


-- =====================================================
-- Query 3: Month-wise Order Count (Last 12 Months)
-- =====================================================

SELECT
    strftime('%Y-%m', order_date) AS month,
    COUNT(order_id) AS total_orders
FROM orders
GROUP BY month
ORDER BY month DESC
LIMIT 12;


-- =====================================================
-- Query 4: Customers Who Placed Orders but Never Had Any Delivered Order
-- =====================================================

SELECT DISTINCT
    c.customer_id,
    c.customer_name
FROM customers c
JOIN orders o
    ON c.customer_id = o.customer_id
WHERE NOT EXISTS (
    SELECT 1
    FROM orders o2
    WHERE o2.customer_id = c.customer_id
      AND o2.status = 'DELIVERED'
);


-- =====================================================
-- Query 5: Products Having More Returns Than Purchases
-- =====================================================

SELECT
    p.product_name,
    SUM(
        CASE
            WHEN oi.quantity > 0 THEN oi.quantity
            ELSE 0
        END
    ) AS purchased,
    ABS(
        SUM(
            CASE
                WHEN oi.quantity < 0 THEN oi.quantity
                ELSE 0
            END
        )
    ) AS returned
FROM order_items oi
JOIN products p
    ON oi.product_id = p.product_id
GROUP BY p.product_name
HAVING returned > purchased;


-- =====================================================
-- Query 6: Return Rate Per Category
-- =====================================================

SELECT
    p.category,
    ROUND(
        100.0 *
        ABS(
            SUM(
                CASE
                    WHEN oi.quantity < 0 THEN oi.quantity
                    ELSE 0
                END
            )
        ) /
        SUM(ABS(oi.quantity)),
        2
    ) AS return_rate
FROM order_items oi
JOIN products p
    ON oi.product_id = p.product_id
GROUP BY p.category;