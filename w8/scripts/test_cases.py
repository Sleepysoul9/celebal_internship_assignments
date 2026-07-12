import sqlite3

DB_PATH = "../database/ecommerce.db"


def test_invalid_order_reference():
    print("\nTest 1: Invalid Order Reference")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT COUNT(*)
    FROM order_items
    WHERE order_id NOT IN (
        SELECT order_id
        FROM orders
    );
    """)

    count = cursor.fetchone()[0]

    print("Invalid References Found:", count)

    conn.close()


def test_discount_greater_than_100():
    print("\nTest 2: Discount > 100")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT COUNT(*)
    FROM order_items
    WHERE discount_percent > 100;
    """)

    count = cursor.fetchone()[0]

    print("Rows Found:", count)

    conn.close()


def test_zero_quantity():
    print("\nTest 3: Quantity = 0")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT COUNT(*)
    FROM order_items
    WHERE quantity = 0;
    """)

    count = cursor.fetchone()[0]

    print("Rows Found:", count)

    conn.close()


def test_future_orders():
    print("\nTest 4: Future Order Dates")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT COUNT(*)
    FROM orders
    WHERE DATE(order_date) > DATE('now');
    """)

    count = cursor.fetchone()[0]

    print("Future Orders:", count)

    conn.close()


if __name__ == "__main__":

    test_invalid_order_reference()

    test_discount_greater_than_100()

    test_zero_quantity()

    test_future_orders()

    print("\nAll Edge Case Tests Completed Successfully!")