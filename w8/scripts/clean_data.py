import pandas as pd
import re
from datetime import datetime

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DIR = BASE_DIR / "data" / "raw"
CLEAN_DIR = BASE_DIR / "data" / "cleaned"
OUTPUT_DIR = BASE_DIR / "output"

customers_df = pd.read_csv(RAW_DIR / "customers.csv")

products_df = pd.read_csv(RAW_DIR / "products.csv")

orders_df = pd.read_csv(RAW_DIR / "orders.csv")

order_items_df = pd.read_csv(RAW_DIR / "order_items.csv")
print(customers_df.shape)
print(products_df.shape)
print(orders_df.shape)
print(order_items_df.shape)
def clean_orders(df):
    """
    Cleans the orders dataset by:
    - Standardizing date formats.
    - Filling missing customer IDs.
    """

    cleaned = df.copy()

    # Standardize date formats
    cleaned["order_date"] = pd.to_datetime(
        cleaned["order_date"],
        format="mixed",
        dayfirst=True,
        errors="coerce"
    )

    # Replace missing customer IDs
    cleaned["customer_id"] = cleaned["customer_id"].fillna("UNKNOWN")

    return cleaned
orders_clean = clean_orders(orders_df)

orders_clean.head()
orders_clean.info()
def clean_products(df):
    """
    Cleans product names by removing extra spaces
    and converting them to Title Case.
    """

    cleaned = df.copy()

    cleaned["product_name"] = (
        cleaned["product_name"]
        .str.strip()
        .str.title()
    )

    return cleaned
products_clean = clean_products(products_df)

products_clean.head()
products_df.sample(10)[["product_name"]]
products_clean.loc[
    products_df.sample(10).index,
    ["product_name"]
]
import re

def validate_emails(df):
    """
    Returns customer IDs with invalid email addresses.
    """

    pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'

    invalid = df[
        ~df["email"].str.match(pattern, na=False)
    ]

    return invalid["customer_id"].tolist()
invalid_customers = validate_emails(customers_df)

print(invalid_customers)
len(invalid_customers)
def check_referential_integrity(orders_df, order_items_df):
    """
    Returns order_items whose order_id
    does not exist in orders.
    """

    valid_orders = set(orders_df["order_id"])

    invalid_rows = order_items_df[
        ~order_items_df["order_id"].isin(valid_orders)
    ]

    return invalid_rows
invalid_order_items = check_referential_integrity(
    orders_df,
    order_items_df
)

invalid_order_items
orders_clean = clean_orders(orders_df)

products_clean = clean_products(products_df)

customers_clean = customers_df.copy()

order_items_clean = order_items_df.copy()
customers_clean.to_csv(
    CLEAN_DIR / "customers_clean.csv" ,
    index=False
)

products_clean.to_csv(
    CLEAN_DIR / "products_clean.csv",
    index=False
)

orders_clean.to_csv(
    CLEAN_DIR / "orders_clean.csv",
    index=False
)

order_items_clean.to_csv(
    CLEAN_DIR / "order_items_clean.csv",
    index=False
)

print("All cleaned datasets saved successfully!")
null_customer_ids = orders_df["customer_id"].isna().sum()

invalid_emails = len(validate_emails(customers_df))

negative_quantities = (order_items_df["quantity"] < 0).sum()

wrong_dates = (
    orders_df["order_date"]
    .str.match(r"\d{2}-\d{2}-\d{4}")
    .sum()
)

invalid_order_refs = len(
    check_referential_integrity(
        orders_df,
        order_items_df
    )
)

report = f"""
DATA QUALITY REPORT
===================

Null Customer IDs      : {null_customer_ids}
Invalid Emails         : {invalid_emails}
Negative Quantities    : {negative_quantities}
Wrong Date Formats     : {wrong_dates}
Broken Order References: {invalid_order_refs}
"""
with open(OUTPUT_DIR / "issues_report.txt", "w") as file:
    file.write(report)

print(report)
