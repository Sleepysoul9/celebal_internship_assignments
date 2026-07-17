# Databricks notebook source
# ==========================================================
# Project : Healthcare Data Platform (Medallion Architecture)
# Notebook: 00_setup_config
# Author  : Kushagra Sanghi
# Purpose : Initialize project configuration, metadata tables,
#           audit framework and global settings.
# ==========================================================

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import uuid
from datetime import datetime

# COMMAND ----------

spark.sql("SELECT current_catalog()").show()

# COMMAND ----------

spark.sql("SELECT current_schema()").show()

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS healthcare
""")

# COMMAND ----------

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA healthcare")

# COMMAND ----------

spark.sql("SELECT current_schema()").show()

# COMMAND ----------

PROJECT_NAME = "Healthcare Data Platform"

PIPELINE_VERSION = "1.0"

BATCH_ID = str(uuid.uuid4())

PIPELINE_START_TIME = datetime.now()

# COMMAND ----------

print("=" * 50)
print(f"Project          : {PROJECT_NAME}")
print(f"Version          : {PIPELINE_VERSION}")
print(f"Batch ID         : {BATCH_ID}")
print(f"Pipeline Started : {PIPELINE_START_TIME}")
print("=" * 50)

# COMMAND ----------

spark.sql("""

CREATE TABLE IF NOT EXISTS metadata_config (

source_file STRING,

bronze_table STRING,

silver_table STRING,

primary_key STRING,

load_type STRING,

is_active BOOLEAN,

created_at TIMESTAMP

)

USING DELTA

""")

# COMMAND ----------

spark.sql("TRUNCATE TABLE metadata_config")

# COMMAND ----------

spark.sql("""
INSERT INTO metadata_config VALUES

('patients.csv',
 'bronze_patients',
 'silver_patients',
 'patient_id',
 'FULL',
 TRUE,
 current_timestamp()),

('doctors.csv',
 'bronze_doctors',
 'silver_doctors',
 'doctor_id',
 'FULL',
 TRUE,
 current_timestamp()),

('appointments.csv',
 'bronze_appointments',
 'silver_appointments',
 'appointment_id',
 'FULL',
 TRUE,
 current_timestamp()),

('treatments.csv',
 'bronze_treatments',
 'silver_treatments',
 'treatment_id',
 'FULL',
 TRUE,
 current_timestamp()),

('billing.csv',
 'bronze_billing',
 'silver_billing',
 'bill_id',
 'FULL',
 TRUE,
 current_timestamp())
""")

# COMMAND ----------

display(spark.table("metadata_config"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS audit_log (
# MAGIC
# MAGIC batch_id STRING,
# MAGIC
# MAGIC pipeline_stage STRING,
# MAGIC
# MAGIC source_file STRING,
# MAGIC
# MAGIC table_name STRING,
# MAGIC
# MAGIC status STRING,
# MAGIC
# MAGIC records_processed BIGINT,
# MAGIC
# MAGIC start_time TIMESTAMP,
# MAGIC
# MAGIC end_time TIMESTAMP,
# MAGIC
# MAGIC remarks STRING,
# MAGIC
# MAGIC error_message STRING
# MAGIC
# MAGIC )
# MAGIC
# MAGIC USING DELTA

# COMMAND ----------

spark.sql("DESCRIBE TABLE audit_log").show(truncate=False)

# COMMAND ----------

# Final Validation

print("=" * 60)
print("Healthcare Data Platform - Setup Validation")
print("=" * 60)

# Validate Metadata Table

metadata_count = spark.table("metadata_config").count()

assert metadata_count == 5, \
    f"Expected 5 metadata rows, found {metadata_count}"

print(f"Metadata table validated ({metadata_count} rows).")

# Validate Audit Table Schema

audit_columns = set(spark.table("audit_log").columns)

expected_columns = {
    "batch_id",
    "pipeline_stage",
    "source_file",
    "table_name",
    "status",
    "records_processed",
    "start_time",
    "end_time",
    "remarks",
    "error_message"
}

missing = expected_columns - audit_columns
extra = audit_columns - expected_columns

assert len(missing) == 0, f"Missing columns: {missing}"
assert len(extra) == 0, f"Unexpected columns: {extra}"

print(f"Audit table validated ({len(audit_columns)} columns).")

# Success

print("Setup completed successfully.")
print("=" * 60)

# COMMAND ----------

