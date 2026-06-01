/*
Q1. Write a query to display all columns and 
rows from the customer's table. 
*/

SELECT * 
FROM customers

/*
Q2. Retrieve only the first_name, last_name, 
and city of all customers.
*/

SELECT 
	first_name ,
	last_name ,
	city
FROM customers

/*
Q3. List all unique categories available in the products table. 
*/

SELECT DISTINCT 
	category
FROM products

/*
Q6. Try inserting a product with unit_price = -50. 
What happens and which constraint prevents it? 
Write both the INSERT statement and explain the error. 
*/
INSERT INTO products
VALUES 
(201, 'Hair Dryer', 'Electronics', 'Dyson', -50 , 250)


/*Q7. Retrieve all orders with status = 'Delivered'. */
SELECT *
FROM orders
WHERE status = 'Delivered'

/*Q8. Find all products in the 'Electronics' category 
with a unit_price greater than ₹2000. */
SELECT *
FROM products
WHERE category = 'Electronics' AND unit_price > 2000

/*Q9. List all customers who joined in the year 2024 and 
belong to the state 'Maharashtra'.*/

SELECT *
FROM customers
WHERE YEAR(join_date) = 2024 AND state = 'Maharashtra'

/*Q10. Find all orders placed between '2024-08-10' and 
'2024-08-25' (inclusive) that are NOT cancelled.*/
SELECT * 
FROM orders
WHERE order_date BETWEEN '2024-08-10' AND '2024-08-25'
	AND status != 'Cancelled'

/*Q11. Explain what the index idx_orders_date does. 
How would it improve the performance of a query that 
filters orders by order_date? Write a sample query that 
would benefit from this index.*/
SELECT order_id, customer_id, total_amount, status , order_date
FROM orders
WHERE order_date >= '2024-08-01' AND order_date <= '2024-08-08'
ORDER BY order_date DESC;

/*Q12. If you run: SELECT * FROM customers WHERE YEAR(join_date)
= 2024; — would the index on join_date be used? Explain why or why 
not, and rewrite the query to be index-friendly (SARGable).*/
CREATE INDEX idx_customers_join_date ON customers(join_date);

/*Q13. Count the total number of orders in the orders table.*/
SELECT COUNT(*)
FROM orders

/*Q14. Find the total revenue (SUM of total_amount) 
from all 'Delivered' orders.*/
SELECT 
	SUM(total_amount) AS total_revenue
FROM orders
WHERE status = 'Delivered'

/*Q15. Calculate the average unit_price of products in each category.*/
SELECT 
	category , 
	AVG(unit_price)
FROM products
GROUP BY category

/*Q16. For each order status, find the count of orders and the 
total revenue. Sort the result by total revenue in descending order.*/
SELECT 
	status , 
	COUNT(*) AS order_count , 
	SUM(total_amount) AS status_total_revenue 
FROM orders
GROUP BY status
ORDER BY SUM(total_amount) DESC

/*Q17. Find the most expensive (MAX) and cheapest (MIN) 
product in each category. */
SELECT
	category ,
	MAX(unit_price) as most_expensive ,
	MIN(unit_price) as cheapest
FROM products
GROUP BY category

/*Q18. List all product categories where the average 
unit_price is greater than ₹2000. (Hint: Use HAVING clause) */
SELECT 
	category
FROM products
GROUP BY category
HAVING AVG(unit_price) > 2000

/*Q19. Write an INNER JOIN query to display each order 
along with the customer's first_name and last_name. 
Show: order_id, order_date, first_name, last_name, total_amount. */
SELECT 
	order_id ,
	order_date ,
	first_name ,
	last_name ,
	total_amount
FROM customers
INNER JOIN orders
ON customers.customer_id = orders.customer_id

/*Q20. Using a LEFT JOIN, list ALL customers and their 
orders (if any). Customers with no orders should still 
appear with NULL values for order columns.*/
SELECT *
FROM customers AS cu
LEFT JOIN orders AS ord
ON cu.customer_id = ord.customer_id

/*Q24. Write a query using CASE to classify products into price tiers: 
  • 'Budget'    → unit_price < 1000 
  • 'Mid-Range' → unit_price BETWEEN 1000 AND 3000 
  • 'Premium'   → unit_price > 3000 
Display: product_name, unit_price, price_tier. */
SELECT 
    product_name, 
    unit_price,
    CASE 
        WHEN unit_price < 1000 THEN 'Budget'
        WHEN unit_price BETWEEN 1000 AND 3000 THEN 'Mid-Range'
        WHEN unit_price > 3000 THEN 'Premium'
        ELSE 'Unpriced' -- Catch-all safety net (e.g., if a price is NULL)
    END AS price_tier
FROM products;

/*Q25. Using a CASE statement inside an aggregate function, count how 
many orders are 'Delivered' vs 'Not Delivered' (all other statuses). 
Display the result in a single row. */
SELECT 
    COUNT(CASE WHEN status = 'Delivered' THEN 1 END) AS Delivered_Count,
    COUNT(CASE WHEN status <> 'Delivered' THEN 1 END) AS Not_Delivered_Count
FROM orders;

/*Q27. Write a SQL transaction that does the following atomically: 
  1. Insert a new order (order_id=1011, customer_id=102, today's date, 'Pending', 1598.00) 
  2. Insert two order items for that order 
  3. Update the stock_qty of the purchased products 
  4. If any step fails, ROLLBACK the entire transaction. Otherwise, COMMIT. 
Write the complete BEGIN...COMMIT/ROLLBACK block. */
BEGIN TRANSACTION;

BEGIN TRY
    -- 1. Create the parent order record
    INSERT INTO orders (order_id, customer_id, order_date, status, total_amount)
    VALUES (1011, 102, '2026-06-01', 'Pending', 1598.00);

    -- 2. Attach the line items to that order
    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
    VALUES 
        (1011, 501, 1, 1099.00),
        (1011, 502, 2, 249.50);

    -- 3. Adjust inventory levels for what was bought
    UPDATE products
    SET stock_qty = stock_qty - 1
    WHERE product_id = 501;

    UPDATE products
    SET stock_qty = stock_qty - 2
    WHERE product_id = 502;

    -- If the code made it here without tripping any errors, lock it in!
    COMMIT TRANSACTION;
    PRINT 'Order processed perfectly and stock updated.';

END TRY
BEGIN CATCH
    -- If anything broke above, wipe the slate completely clean
    IF @@TRANCOUNT > 0
    BEGIN
        ROLLBACK TRANSACTION;
    END
    
    -- Let us know exactly what went wrong for troubleshooting
    PRINT 'Something went wrong. Transaction rolled back safely.';
    PRINT ERROR_MESSAGE();
END CATCH;