import sqlite3
from datetime import datetime, timedelta

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DB_PATH = BASE_DIR / "database" / "ecommerce.db"

def get_report(period, start_date, end_date):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    query = """
    SELECT
        COUNT(DISTINCT o.order_id) AS total_orders,
        ROUND(
            SUM(
                oi.quantity *
                oi.unit_price *
                (1 - oi.discount_percent/100.0)
            ),
        2) AS total_revenue,
        COUNT(DISTINCT o.customer_id) AS unique_customers
    FROM orders o
    JOIN order_items oi
    ON o.order_id = oi.order_id
    WHERE DATE(o.order_date)
    BETWEEN ? AND ?;
    """

    cursor.execute(query, (start_date, end_date))

    summary = cursor.fetchone()

    print("\n========== REPORT ==========\n")
    print("Report Type :", period)
    print("Date Range  :", start_date, "to", end_date)
    print("-------------------------------")
    print("Total Orders      :", summary[0])
    print("Total Revenue     :", summary[1])
    print("Unique Customers  :", summary[2])

    print("\nTop 3 Products\n")

    top_products_query = """
    SELECT
        p.product_name,
        ROUND(
            SUM(
                oi.quantity *
                oi.unit_price *
                (1 - oi.discount_percent/100.0)
            ),
        2) AS revenue
    FROM order_items oi
    JOIN products p
    ON oi.product_id = p.product_id
    JOIN orders o
    ON oi.order_id = o.order_id
    WHERE DATE(o.order_date)
    BETWEEN ? AND ?
    GROUP BY p.product_name
    ORDER BY revenue DESC
    LIMIT 3;
    """

    cursor.execute(top_products_query, (start_date, end_date))

    rows = cursor.fetchall()

    for row in rows:
        print(row[0], ":", row[1])

    conn.close()


def previous_period(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    days = (end - start).days + 1

    previous_end = start - timedelta(days=1)
    previous_start = previous_end - timedelta(days=days - 1)

    return previous_start.strftime("%Y-%m-%d"), previous_end.strftime("%Y-%m-%d")


def revenue_between(start_date, end_date):
    conn = sqlite3.connect(DB_PATH)

    cursor = conn.cursor()

    query = """
    SELECT
        IFNULL(
            SUM(
                oi.quantity *
                oi.unit_price *
                (1 - oi.discount_percent/100.0)
            ),
        0)
    FROM orders o
    JOIN order_items oi
    ON o.order_id = oi.order_id
    WHERE DATE(o.order_date)
    BETWEEN ? AND ?;
    """

    cursor.execute(query, (start_date, end_date))

    revenue = cursor.fetchone()[0]

    conn.close()

    return revenue


print("\nE-Commerce Reporting Tool\n")

report_type = input("Enter report type (daily/weekly/monthly): ")

start_date = input("Enter start date (YYYY-MM-DD): ")

end_date = input("Enter end date (YYYY-MM-DD): ")

get_report(report_type, start_date, end_date)

prev_start, prev_end = previous_period(start_date, end_date)

current = revenue_between(start_date, end_date)

previous = revenue_between(prev_start, prev_end)

if previous == 0:
    print("\nPrevious period has no revenue.")
else:

    change = ((current - previous) / previous) * 100

    print("\nRevenue Change from Previous Period : {:.2f}%".format(change))