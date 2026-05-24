# 📊 Week 1 Assignment: Data Exploration & Cleaning

## 📋 Project Overview
This project involved a comprehensive data exploration, cleaning, and feature engineering workflow on a raw product dataset. The goal was to inspect data structure, handle missing values, clean inconsistent formatting, and engineer multiple variations of revenue metrics (`total_amount`).

---

## 🛠️ Technical Steps Performed

### 1. Data Ingestion & Initial Inspection
* **Environment Setup:** Imported essential data science libraries (`pandas`, `numpy`).
* **Data Ingestion:** Loaded the raw dataset using `pd.read_csv()` and created a deep copy (`df`) to keep the original data intact.
* **Structural Analysis:** Inspected the layout using `.head(10)` and `.tail()`, evaluated dimensions with `.shape`, and generated summaries using `.describe()` and `.info()`.

### 2. Data Cleansing & Data Type Alignment
* **Duplicate Check:** Verified identical rows using `.duplicated().sum()` and confirmed 0 duplicates.
* **Missing Value Imputation:** Identified missing data in the numerical `discount` column and seamlessly imputed them using the column's mean via `.fillna()`.
* **Dimensionality Reduction:** Dropped irrelevant and unwanted columns to optimize memory usage.
* **String Parsing & Type Casting:** Cleaned formatting anomalies in the `final_price` column (removing currencies, commas, and formatting strings) and cast it into an Integer datatype.
* **Column Validation:** Compared `final_price` against the initial price column and elected to prioritize the cleaned `final_price`.

### 3. Feature Engineering & Permutation Modeling
* **Quantity Assumptions:** Kept `original_rating_quantity` and engineered a proxy `assumed_quantity` feature (modeled as 2 × number of ratings).
* **Revenue Permutations:** Engineered 6 unique revenue columns using cross-multiplication permutations:
  * `total_amount1` = Initial Price × Original Rating Quantity
  * `total_amount2` = Initial Price × Assumed Quantity
  * `total_amount3` = Final Price × Original Rating Quantity
  * `total_amount4` = Final Price × Assumed Quantity
  * `total_amount5` = Discounted Price × Original Rating Quantity
  * `total_amount6` = Discounted Price × Assumed Quantity

### 4. Data Export
* Exported the fully transformed dataset into a clean CSV file using `.to_csv(index=False)` for final submission.

---

## 📁 Repository Structure
* `week1/assignment_week1.ipynb` — Jupyter Notebook containing code execution and data outputs.
* `week1/week1_assignment_submission_data.csv` — Cleaned dataset containing the engineered price and quantity permutation models.