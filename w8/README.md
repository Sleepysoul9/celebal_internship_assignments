# 🛒 E-Commerce Order Analytics System

## 📌 Project Overview

This project is an end-to-end E-Commerce Order Analytics System developed as part of the Celebal Technologies Internship (Week 8 Mini Project).

The project demonstrates the complete data analytics pipeline using Python and SQL, starting from synthetic data generation, data cleaning, SQL-based business analysis, and ending with a command-line reporting tool.

---

## 🎯 Objectives

- Generate realistic e-commerce datasets
- Introduce intentional data quality issues
- Clean and validate data using Pandas
- Load cleaned data into SQLite
- Perform business analytics using SQL
- Implement Window Functions and Common Table Expressions (CTEs)
- Perform Cohort and Customer Segmentation Analysis
- Build a CLI-based reporting tool
- Handle common edge cases through automated tests

---

# 🛠️ Technologies Used

- Python 3.x
- Pandas
- Faker
- SQLite3
- Jupyter Notebook
- VS Code

---

# 📂 Project Structure

```
ecommerce-analytics-system/

├── data/
│   ├── raw/
│   └── cleaned/
│
├── database/
│   └── ecommerce.db
│
├── notebooks/
│   ├── 01_generate_data.ipynb
│   ├── 02_clean_data.ipynb
│   └── 03_load_sqlite.ipynb
│
├── output/
│   ├── issues_report.txt
│   └── sample_reports/
│
├── scripts/
│   ├── generate_data.py
│   ├── clean_data.py
│   ├── report_cli.py
│   └── test_cases.py
│
├── sql/
│   ├── schema.sql
│   ├── aggregations.sql
│   ├── window_functions.sql
│   └── cohort_analysis.sql
│
├── README.md
└── requirements.txt
```

---

# 📊 Dataset

The project generates four datasets.

## customers.csv

- Customer ID
- Customer Name
- Email
- Registration Date
- Customer Type

## products.csv

- Product ID
- Product Name
- Category
- Subcategory
- Cost Price

## orders.csv

- Order ID
- Customer ID
- Region
- Status
- Order Date

## order_items.csv

- Order Item ID
- Order ID
- Product ID
- Quantity
- Unit Price
- Discount Percentage

---

# ⚠️ Intentional Data Issues

The generated datasets include realistic inconsistencies.

- 5% Missing Customer IDs
- 3% Negative Quantities (Returns)
- Invalid Date Formats
- Product Name Formatting Issues
- Invalid Email Addresses
- Mixed Case Product Names

---

# 🧹 Data Cleaning

The cleaning pipeline performs:

- Date Standardization
- Missing Value Handling
- Product Name Normalization
- Email Validation
- Referential Integrity Validation

---

# 📈 SQL Analysis

The project includes:

## Basic Analytics

- Revenue by Category
- Top Customers
- Monthly Orders
- Return Rate
- Products with Highest Returns

## Advanced Analytics

- Running Totals
- Window Functions
- DENSE_RANK()
- LAG()
- NTILE()
- Cohort Analysis
- Customer Segmentation
- Year-over-Year Analysis
- Cumulative Revenue Distribution

---

# 💻 CLI Reporting Tool

The reporting tool allows users to:

- Generate Daily Reports
- Generate Weekly Reports
- Generate Monthly Reports

Each report displays:

- Total Orders
- Total Revenue
- Unique Customers
- Top 3 Products
- Revenue Comparison with Previous Period

---

# 🧪 Edge Case Testing

The project validates:

- Invalid Order References
- Discount > 100%
- Zero Quantity
- Future Order Dates

---

# ▶️ How to Run

### Install dependencies

```bash
pip install pandas faker
```

### Generate Data

```bash
python scripts/generate_data.py
```

### Clean Data

```bash
python scripts/clean_data.py
```

### Execute SQL Analysis

Run the notebooks inside the `notebooks` folder.

### Run CLI Tool

```bash
python scripts/report_cli.py
```

### Execute Test Cases

```bash
python scripts/test_cases.py
```

---

# 📌 Learning Outcomes

Through this project, the following concepts were implemented:

- Data Generation
- Data Cleaning
- Data Validation
- SQL Joins
- Aggregations
- Window Functions
- Common Table Expressions
- Cohort Analysis
- Customer Segmentation
- SQLite Integration
- Python Automation

---

# 👨‍💻 Author

**Kushagra Sanghi**

B.Tech Information Technology

Celebal Technologies Internship – Week 8 Mini Project