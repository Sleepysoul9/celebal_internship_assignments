# 🏥 Healthcare Data Platform using Medallion Architecture

> An end-to-end healthcare data engineering pipeline built on **Databricks** using the **Medallion Architecture (Bronze → Silver → Gold)** to transform raw healthcare data into trusted analytical datasets.

![Databricks](https://img.shields.io/badge/Databricks-EF3E42?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Healthcare](https://img.shields.io/badge/Domain-Healthcare-2E8B57?style=for-the-badge)

---

This project demonstrates a modern **Lakehouse-based Data Engineering Pipeline** that ingests raw healthcare datasets, applies data quality validation and business transformations, implements **Slowly Changing Dimension (SCD Type 2)** for historical tracking, generates business-ready KPIs, and maintains comprehensive audit logs for pipeline monitoring.

Designed using the **Medallion Architecture**, the pipeline progressively refines data through the **Bronze**, **Silver**, and **Gold** layers, ensuring scalability, reliability, and analytical readiness.

## 📌 Project Overview

Healthcare organizations generate vast amounts of data from patient records, appointments, treatments, billing, and clinical operations. However, raw data is often incomplete, inconsistent, and not directly suitable for analytics or decision-making.

This project demonstrates an end-to-end **Lakehouse Data Engineering Pipeline** that transforms raw healthcare datasets into trusted, analytics-ready data using the **Medallion Architecture** on **Databricks**.

The pipeline leverages **Apache Spark** and **Delta Lake** to ingest, cleanse, validate, and enrich data across Bronze, Silver, and Gold layers. It also implements **Slowly Changing Dimension (SCD Type 2)** for historical tracking, generates business KPIs, and maintains audit logs to monitor pipeline execution.

### Objectives

- Build a scalable healthcare data pipeline using Medallion Architecture.
- Ingest raw healthcare datasets into the Bronze layer.
- Clean, standardize, and validate data in the Silver layer.
- Implement SCD Type 2 for historical data tracking.
- Generate business-ready KPIs in the Gold layer.
- Track pipeline execution using audit logging.


## ✨ Key Features

- 🏗️ **Medallion Architecture** implementation with Bronze, Silver, and Gold data layers.
- ⚡ **Apache Spark**-based distributed data processing on Databricks.
- 📥 **Metadata-driven ingestion** for standardized and scalable data loading.
- 🧹 **Data validation and cleansing** to improve data quality and consistency.
- 🔄 **Slowly Changing Dimension (SCD Type 2)** implementation for historical data tracking.
- 💾 **Delta Lake** storage for reliable, ACID-compliant data management.
- 📊 **Business KPI generation** for patient, appointment, billing, and treatment analytics.
- 📋 **Pipeline audit logging** to monitor execution status, processing metrics, and errors.
- 📈 **Analytics-ready Gold layer** optimized for reporting and BI tools.
- 🧩 **Modular notebook-based design** for maintainability and scalability.

## 🏗️ Solution Architecture

<p align="center">
  <img src="images\architecture.png" alt="Healthcare Data Platform Architecture" width="1000">
</p>

The project follows the **Medallion Architecture**, where healthcare data is progressively refined through multiple layers to improve quality, reliability, and analytical value.

### Data Flow

```text
Raw CSV Files
       │
       ▼
Bronze Layer
(Data Ingestion)
       │
       ▼
Silver Layer
(Data Validation, Cleansing & SCD Type 2)
       │
       ▼
Gold Layer
(Business KPIs & Analytics)
       │
       ▼
Reporting & BI Tools
(Power BI / Dashboards)
```

### Pipeline Components

| Layer | Purpose |
|--------|---------|
| **Bronze** | Ingest raw healthcare datasets without modifying the source data. |
| **Silver** | Clean, validate, standardize, and enrich data while implementing business rules and SCD Type 2. |
| **Gold** | Generate business-ready KPIs and analytical datasets for reporting and decision-making. |
| **Audit Layer** | Monitor pipeline execution, processing metrics, execution time, and failures across all stages. |


## 🛠️ Tech Stack

<p align="left">
  <img src="https://img.shields.io/badge/Databricks-EF3E42?style=for-the-badge&logo=databricks&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge"/>
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white"/>
  <img src="https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white"/>
</p>

| Category | Technology |
|-----------|------------|
| **Platform** | Databricks |
| **Processing Engine** | Apache Spark (PySpark) |
| **Storage Layer** | Delta Lake |
| **Programming Language** | Python |
| **Data Source** | CSV Files |
| **Architecture** | Medallion Architecture (Bronze → Silver → Gold) |
| **Data Modeling** | Slowly Changing Dimension (SCD Type 2) |
| **Pipeline Type** | ETL / ELT |
| **Monitoring** | Audit Logging |
| **Version Control** | Git & GitHub |
| **Future Visualization** | Power BI |