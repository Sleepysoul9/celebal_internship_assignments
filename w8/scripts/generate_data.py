import sys
print(sys.executable)
import pandas as pd
from faker import Faker

print("Python environment initialized successfully.")
import random

from datetime import datetime
fake = Faker("en_IN")

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

random.seed(42)
Faker.seed(42)
NUM_CUSTOMERS = 600
NUM_PRODUCTS = 500
NUM_ORDERS = 700
NUM_ORDER_ITEMS = 2500
customer_types = [
    "REGULAR",
    "PREMIUM",
    "VIP"
]

categories = {
    "Electronics": [
        "Mobile",
        "Laptop",
        "Accessories"
    ],
    "Clothing": [
        "Men",
        "Women",
        "Kids"
    ],
    "Home": [
        "Kitchen",
        "Furniture",
        "Decor"
    ],
    "Books": [
        "Fiction",
        "Education",
        "Comics"
    ]
}

regions = [
    "North",
    "South",
    "East",
    "West"
]

order_status = [
    "PLACED",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED"
]
customers = []

for i in range(1, NUM_CUSTOMERS + 1):

    customer_id = f"CUST{i:04d}"

    customer_name = fake.name()

    email = fake.email()

    registration_date = fake.date_between(
        start_date="-3y",
        end_date="today"
    )

    customer_type = random.choices(
        customer_types,
        weights=[70, 20, 10],
        k=1
    )[0]

    customers.append({
        "customer_id": customer_id,
        "customer_name": customer_name,
        "email": email,
        "registration_date": registration_date,
        "customer_type": customer_type
    })
num_invalid = int(NUM_CUSTOMERS * 0.02)

invalid_indices = random.sample(range(NUM_CUSTOMERS), num_invalid)

for idx in invalid_indices:

    email = customers[idx]["email"]

    if random.choice([True, False]):
        customers[idx]["email"] = email.replace("@", "")
    else:
        customers[idx]["email"] = email.split("@")[0] + "@"
customers_df = pd.DataFrame(customers)

customers_df.to_csv(
    RAW_DIR / "customers.csv",
    index=False
)

print("customers.csv generated successfully!")
product_catalog = {
    "Electronics": {
        "Mobile": [
            "iPhone 15",
            "Galaxy S24",
            "OnePlus 13",
            "Pixel 9",
            "Nothing Phone"
        ],
        "Laptop": [
            "MacBook Air",
            "Dell XPS",
            "HP Pavilion",
            "Lenovo ThinkPad",
            "Asus Vivobook"
        ],
        "Accessories": [
            "Wireless Mouse",
            "Bluetooth Speaker",
            "Power Bank",
            "USB Hub",
            "Keyboard"
        ]
    },

    "Clothing": {
        "Men": [
            "T-Shirt",
            "Jeans",
            "Shirt",
            "Jacket",
            "Hoodie"
        ],
        "Women": [
            "Kurti",
            "Dress",
            "Top",
            "Jeans",
            "Sweater"
        ],
        "Kids": [
            "Kids T-Shirt",
            "Kids Shorts",
            "Kids Jacket",
            "Kids Shoes"
        ]
    },

    "Home": {
        "Kitchen": [
            "Mixer",
            "Pressure Cooker",
            "Knife Set",
            "Frying Pan"
        ],
        "Furniture": [
            "Office Chair",
            "Dining Table",
            "Bookshelf",
            "Sofa"
        ],
        "Decor": [
            "Wall Clock",
            "Flower Vase",
            "Lamp",
            "Mirror"
        ]
    },

    "Books": {
        "Education": [
            "Python Programming",
            "Data Structures",
            "Operating Systems",
            "DBMS"
        ],
        "Fiction": [
            "Mystery Novel",
            "Fantasy Book",
            "Thriller Novel"
        ],
        "Comics": [
            "Marvel Comic",
            "Batman Comic",
            "Manga Volume"
        ]
    }
}
products = []

for i in range(1, NUM_PRODUCTS + 1):

    category = random.choice(list(product_catalog.keys()))

    subcategory = random.choice(
        list(product_catalog[category].keys())
    )

    product_name = random.choice(
        product_catalog[category][subcategory]
    )

    cost_price = round(
        random.uniform(100, 5000),
        2
    )

    products.append({
        "product_id": f"PROD{i:04d}",
        "product_name": product_name,
        "category": category,
        "subcategory": subcategory,
        "cost_price": cost_price
    })
indices = random.sample(range(NUM_PRODUCTS), int(NUM_PRODUCTS * 0.08))

for idx in indices:

    name = products[idx]["product_name"]

    if random.choice([True, False]):
        products[idx]["product_name"] = "  " + name + "  "
    else:
        products[idx]["product_name"] = name.swapcase()
products_df = pd.DataFrame(products)

products_df.to_csv(
    RAW_DIR / "products.csv",
    index=False
)

print("products.csv generated successfully!")
orders = []

for i in range(1, NUM_ORDERS + 1):

    order_id = f"ORD{i:05d}"

    customer_id = random.choice(customers_df["customer_id"].tolist())

    region = random.choice(regions)

    status = random.choice(order_status)

    order_datetime = fake.date_time_between(
        start_date="-2y",
        end_date="now"
    )

    orders.append({
        "order_id": order_id,
        "customer_id": customer_id,
        "region": region,
        "status": status,
        "order_date": order_datetime.strftime("%Y-%m-%d %H:%M:%S")
    })
num_null = int(NUM_ORDERS * 0.05)

indices = random.sample(range(NUM_ORDERS), num_null)

for idx in indices:
    orders[idx]["customer_id"] = None
wrong_date_indices = random.sample(range(NUM_ORDERS), int(NUM_ORDERS * 0.05))

for idx in wrong_date_indices:

    dt = datetime.strptime(
        orders[idx]["order_date"],
        "%Y-%m-%d %H:%M:%S"
    )

    orders[idx]["order_date"] = dt.strftime("%d-%m-%Y %H:%M:%S")
orders_df = pd.DataFrame(orders)

orders_df.to_csv(
    RAW_DIR / "orders.csv",
    index=False
)

print("orders.csv generated successfully!")
order_items = []

order_ids = orders_df["order_id"].tolist()
product_ids = products_df["product_id"].tolist()

# Create a lookup for product cost prices
price_lookup = products_df.set_index("product_id")["cost_price"].to_dict()

for i in range(1, NUM_ORDER_ITEMS + 1):

    order_id = random.choice(order_ids)

    product_id = random.choice(product_ids)

    quantity = random.randint(1, 5)

    cost_price = price_lookup[product_id]

    # Selling price is 20–60% above cost
    unit_price = round(cost_price * random.uniform(1.2, 1.6), 2)

    discount_percent = random.randint(0, 50)

    order_items.append({
        "order_item_id": f"ITEM{i:05d}",
        "order_id": order_id,
        "product_id": product_id,
        "quantity": quantity,
        "unit_price": unit_price,
        "discount_percent": discount_percent
    })
negative_indices = random.sample(
    range(NUM_ORDER_ITEMS),
    int(NUM_ORDER_ITEMS * 0.03)
)

for idx in negative_indices:

    order_items[idx]["quantity"] *= -1
order_items_df = pd.DataFrame(order_items)

order_items_df.to_csv(
    RAW_DIR / "order_items.csv",
    index=False
)

print("order_items.csv generated successfully!")
print("Customers:", len(customers_df))
print("Products:", len(products_df))
print("Orders:", len(orders_df))
print("Order Items:", len(order_items_df))

print("\nVerification")
print("--------------------------")
print("Invalid emails:",
      (~customers_df["email"].str.contains(
          r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$',
          regex=True,
          na=False
      )).sum())

print("Null customer IDs:",
      orders_df["customer_id"].isna().sum())

print("Negative quantities:",
      (order_items_df["quantity"] < 0).sum())

print("Wrong date formats:",
      orders_df["order_date"].str.match(r"\d{2}-\d{2}-\d{4}").sum())

print("\nData generation completed successfully.")