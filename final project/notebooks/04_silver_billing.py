# Databricks notebook source
# ==========================================================
# Project : Healthcare Data Platform (Medallion Architecture)
# Notebook: 04_silver_billing
# Author  : Kushagra Sanghi
#
# Purpose :
#     Transform Bronze Billing data into the Silver Layer by
#     applying data quality rules, business validation,
#     and SCD Type 2.
# ==========================================================

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    lit,
    trim,
    upper,
    lower,
    current_timestamp,
    when,
    sha2,
    concat_ws,
    coalesce,
    max
)

from delta.tables import DeltaTable
from pyspark.sql import Row

import uuid
from datetime import datetime

# COMMAND ----------

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA healthcare")

# COMMAND ----------

PROJECT_NAME = "Healthcare Data Platform"

PIPELINE_VERSION = "1.0"

BATCH_ID = str(uuid.uuid4())

PIPELINE_START_TIME = datetime.now()

SOURCE_TABLE = "bronze_billing"

TARGET_TABLE = "silver_billing"

# COMMAND ----------

# Read Bronze Billing Data

print("=" * 60)
print("Loading Bronze Billing Data")
print("=" * 60)

bronze_df = (

    spark.table(SOURCE_TABLE)

)

print(f"Source Table   : {SOURCE_TABLE}")

print(f"Records Loaded : {bronze_df.count()}")

print("=" * 60)

display(bronze_df)

# COMMAND ----------

# Source Validation

print("=" * 60)
print("Validating Bronze Billing Data")
print("=" * 60)

# Row Count Validation

row_count = bronze_df.count()

if row_count == 0:

    raise Exception(f"{SOURCE_TABLE} is empty.")

print(f"✓ Source Record Count : {row_count}")

# Mandatory Column Validation

expected_columns = [

    "bill_id",
    "patient_id",
    "treatment_id",
    "bill_date",
    "amount",
    "payment_method",
    "payment_status",
    "batch_id",
    "ingestion_timestamp",
    "source_file"

]

actual_columns = bronze_df.columns

missing_columns = [

    column

    for column in expected_columns

    if column not in actual_columns

]

if len(missing_columns) > 0:

    raise Exception(

        f"Missing Required Columns : {missing_columns}"

    )

print("✓ Mandatory Columns Present.")

# Schema Validation

unexpected_columns = [

    column

    for column in actual_columns

    if column not in expected_columns

]

if len(unexpected_columns) > 0:

    print(f"Warning : Unexpected Columns Found : {unexpected_columns}")

else:

    print("✓ Schema Validation Passed.")

print("=" * 60)

# COMMAND ----------

# Cleaning and Standardizing Billing Data

print("=" * 60)
print("Cleaning and Standardizing Billing Data")
print("=" * 60)

silver_df = (

    bronze_df

    # Trim String Columns
    
    .withColumn(
        "bill_id",
        trim(col("bill_id"))
    )

    .withColumn(
        "patient_id",
        trim(col("patient_id"))
    )

    .withColumn(
        "treatment_id",
        trim(col("treatment_id"))
    )

    .withColumn(
        "payment_method",
        upper(trim(col("payment_method")))
    )

    .withColumn(
        "payment_status",
        upper(trim(col("payment_status")))
    )

)

# Remove Records with NULL Bill ID

silver_df = (

    silver_df

    .filter(col("bill_id").isNotNull())

)

# Remove Duplicate Bill Records

silver_df = (

    silver_df

    .dropDuplicates(["bill_id"])

)

print("✓ String Cleaning Completed.")

print("✓ Payment Method Standardized.")

print("✓ Payment Status Standardized.")

print("✓ Duplicate Billing Records Removed.")

print(f"Records After Cleaning : {silver_df.count()}")

print("=" * 60)

display(silver_df)

# COMMAND ----------

# Business Rule Validation

print("=" * 60)
print("Applying Business Rule Validation")
print("=" * 60)

silver_df = (

    silver_df

    .withColumn(

        "billing_valid",

        (

            col("bill_id").isNotNull()

            &

            col("patient_id").isNotNull()

            &

            col("treatment_id").isNotNull()

            &

            col("bill_date").isNotNull()

            &

            (col("amount") > 0)

            &

            col("payment_method").isin(

                "CASH",
                "CREDIT CARD",
                "INSURANCE"

            )

            &

            col("payment_status").isin(

                "PAID",
                "PENDING",
                "FAILED"

            )

        )

    )

)

valid_records = (

    silver_df

    .filter(col("billing_valid") == True)

    .count()

)

invalid_records = (

    silver_df

    .filter(col("billing_valid") == False)

    .count()

)

print(f"Valid Billing Records   : {valid_records}")

print(f"Invalid Billing Records : {invalid_records}")

print("=" * 60)

display(silver_df)

# COMMAND ----------

# SCD Type 2 Preparation

print("=" * 60)
print("Preparing SCD Type 2 Dataset")
print("=" * 60)

# Single Timestamp for Entire Batch

SCD_TIMESTAMP = datetime.now()

# Generate Business Hash

prepared_df = (

    silver_df

    .withColumn(

        "business_hash",

        sha2(

            concat_ws(

                "||",

                coalesce(col("patient_id"), lit("")),

                coalesce(col("treatment_id"), lit("")),

                coalesce(col("bill_date").cast("string"), lit("")),

                coalesce(col("amount").cast("string"), lit("")),

                coalesce(col("payment_method"), lit("")),

                coalesce(col("payment_status"), lit(""))

            ),

            256

        )

    )

)

# Add SCD Metadata

prepared_df = (

    prepared_df

    .withColumn(
        "effective_from",
        lit(SCD_TIMESTAMP)
    )

    .withColumn(
        "effective_to",
        lit(None).cast("timestamp")
    )

    .withColumn(
        "is_current",
        lit(True)
    )

    .withColumn(
        "record_version",
        lit(1)
    )

    .withColumn(
        "silver_batch_id",
        lit(BATCH_ID)
    )

)

print(f"Prepared Records : {prepared_df.count()}")

print("✓ Business Hash Generated.")

print("✓ SCD Metadata Columns Added.")

print("=" * 60)

display(

    prepared_df.select(

        "bill_id",
        "business_hash",
        "record_version",
        "is_current",
        "effective_from"

    )

)

# COMMAND ----------

# SCD Type 2 Change Detection

print("=" * 60)
print("SCD Type 2 Change Detection")
print("=" * 60)

# Check Whether Silver Table Exists

table_exists = spark.catalog.tableExists(TARGET_TABLE)

if not table_exists:

    print("First Execution Detected.")
    print(f"{TARGET_TABLE} table does not exist.")

    current_df = spark.createDataFrame([], prepared_df.schema)

    new_df = prepared_df

    changed_df = spark.createDataFrame([], prepared_df.schema)

    unchanged_df = spark.createDataFrame([], prepared_df.schema)

    expired_df = spark.createDataFrame([], prepared_df.schema)

else:

    print("Incremental Execution Detected.")

    # Read Current Active Records
    
    current_df = (

        spark.table(TARGET_TABLE)

        .filter(col("is_current") == True)

        .withColumn(

            "business_hash",

            sha2(

                concat_ws(

                    "||",

                    coalesce(col("patient_id"), lit("")),

                    coalesce(col("treatment_id"), lit("")),

                    coalesce(col("bill_date").cast("string"), lit("")),

                    coalesce(col("amount").cast("string"), lit("")),

                    coalesce(col("payment_method"), lit("")),

                    coalesce(col("payment_status"), lit(""))

                ),

                256

            )

        )

    )

    # Join Prepared Data with Current Silver
    
    joined_df = (

        prepared_df.alias("new")

        .join(

            current_df.alias("old"),

            on="bill_id",

            how="left"

        )

    )

    # New Bills
    
    new_df = (

        joined_df

        .filter(col("old.bill_id").isNull())

        .select("new.*")

    )

    # Changed Bills
    
    changed_df = (

        joined_df

        .filter(

            (col("old.bill_id").isNotNull())

            &

            (col("new.business_hash") != col("old.business_hash"))

        )

        .select("new.*")

    )

    # Unchanged Bills
    
    unchanged_df = (

        joined_df

        .filter(

            (col("old.bill_id").isNotNull())

            &

            (col("new.business_hash") == col("old.business_hash"))

        )

        .select("new.*")

    )

    # Records to Expire
    
    expired_df = (

        current_df.alias("old")

        .join(

            changed_df

            .select("bill_id")

            .alias("chg"),

            on="bill_id",

            how="inner"

        )

        .withColumn(

            "effective_to",

            lit(SCD_TIMESTAMP)

        )

        .withColumn(

            "is_current",

            lit(False)

        )

    )

# Execution Summary

print()

print(f"Current Records      : {current_df.count()}")

print(f"New Records          : {new_df.count()}")

print(f"Changed Records      : {changed_df.count()}")

print(f"Unchanged Records    : {unchanged_df.count()}")

print(f"Expired Records      : {expired_df.count()}")

print()

print("✓ Change Detection Completed.")

print("=" * 60)

# COMMAND ----------

# Persist SCD Type 2 (Part A)

print("=" * 60)
print("Persisting Silver Billing Table")
print("=" * 60)

# First Execution

if not table_exists:

    print("Creating Silver Billing Table...")

    final_silver_df = (

        prepared_df

        .drop("business_hash")

    )

# Incremental Execution

else:

    print("Applying SCD Type 2...")

    # Historical Records
    
    historical_df = (

        spark.table(TARGET_TABLE)

        .filter(col("is_current") == False)

    )

    # Current Active Records
    
    active_df = (

        spark.table(TARGET_TABLE)

        .filter(col("is_current") == True)

    )

    # Keep Unchanged Records
    
    active_unchanged_df = (

        active_df.alias("old")

        .join(

            unchanged_df

            .select("bill_id")

            .alias("same"),

            "bill_id",

            "inner"

        )

        .select("old.*")

    )

    # Expire Changed Records
    
    expired_records_df = (

        expired_df

        .drop("business_hash")

    )

    # Determine Next Record Version
    
    version_df = (

        active_df

        .groupBy("bill_id")

        .agg(

            max("record_version").alias("max_version")

        )

    )

    # Build Version 2 Records
    
    changed_version_df = (

        changed_df.alias("new")

        .join(

            version_df.alias("ver"),

            "bill_id",

            "left"

        )

        .withColumn(

            "record_version",

            coalesce(col("max_version"), lit(0)) + lit(1)

        )

        .drop("max_version")

        .drop("business_hash")

    )

    # New Billing Records
    
    new_records_df = (

        new_df

        .drop("business_hash")

    )

    # Build Final Silver Dataset
    
    final_silver_df = (

        historical_df

        .unionByName(expired_records_df)

        .unionByName(active_unchanged_df)

        .unionByName(changed_version_df)

        .unionByName(new_records_df)

    )

# Persist SCD Type 2 (Part B)

# Persist Final Silver Table

(

    final_silver_df

    .write

    .format("delta")

    .mode("overwrite")

    .option("overwriteSchema", "true")

    .saveAsTable(TARGET_TABLE)

)

# Execution Summary

rows_written = final_silver_df.count()

print()

print(f"Rows Written : {rows_written}")

print()

print("✓ Silver Billing Table Persisted Successfully.")

print("=" * 60)

display(final_silver_df)

# COMMAND ----------

# Audit Logging

print("=" * 60)
print("Writing Audit Log")
print("=" * 60)

PIPELINE_END_TIME = datetime.now()

audit_entry = Row(

    batch_id = BATCH_ID,

    pipeline_stage = "Silver",

    table_name = TARGET_TABLE,

    status = "SUCCESS",

    records_processed = final_silver_df.count(),

    start_time = PIPELINE_START_TIME,

    end_time = PIPELINE_END_TIME,

    remarks = "Silver Billing Pipeline Completed Successfully.",

    source_file = SOURCE_TABLE,

    error_message = "No Error"

)

audit_df = spark.createDataFrame([audit_entry])

(

    audit_df

    .write

    .mode("append")

    .saveAsTable("audit_log")

)

print("✓ Audit Log Written Successfully.")

print("=" * 60)

display(audit_df)

# COMMAND ----------

# Silver Table Validation

print("=" * 60)
print("Running Silver Table Validation")
print("=" * 60)

silver_df = spark.table(TARGET_TABLE)

# Duplicate Bill IDs

duplicate_count = (

    silver_df

    .groupBy("bill_id")

    .count()

    .filter(col("count") > 1)

    .count()

)

# Current Records

current_record_count = (

    silver_df

    .filter(col("is_current") == True)

    .count()

)

# Historical Records

historical_record_count = (

    silver_df

    .filter(col("is_current") == False)

    .count()

)

# Invalid Record Versions

invalid_version_count = (

    silver_df

    .filter(col("record_version") < 1)

    .count()

)

# NULL Business Keys

null_key_count = (

    silver_df

    .filter(col("bill_id").isNull())

    .count()

)

# Current Records with Effective To

invalid_current_records = (

    silver_df

    .filter(

        (col("is_current") == True)

        &

        (col("effective_to").isNotNull())

    )

    .count()

)

print(f"Duplicate Bill IDs                     : {duplicate_count}")

print(f"Current Records                        : {current_record_count}")

print(f"Historical Records                     : {historical_record_count}")

print(f"Invalid Record Versions                : {invalid_version_count}")

print(f"NULL Bill IDs                          : {null_key_count}")

print(f"Current Records with effective_to Set  : {invalid_current_records}")

validation_passed = (

    duplicate_count == 0

    and

    invalid_version_count == 0

    and

    null_key_count == 0

    and

    invalid_current_records == 0

)

print()

if validation_passed:

    print("✓ Silver Table Validation Passed.")

else:

    print("✗ Silver Table Validation Failed.")

print("=" * 60)

# COMMAND ----------

# Pipeline Execution Summary

print("=" * 60)
print("Healthcare Data Platform - Pipeline Summary")
print("=" * 60)

PIPELINE_END_TIME = datetime.now()

pipeline_duration = PIPELINE_END_TIME - PIPELINE_START_TIME

print(f"Project Name        : {PROJECT_NAME}")
print(f"Pipeline Version    : {PIPELINE_VERSION}")
print(f"Pipeline Stage      : Silver")
print(f"Source Table        : {SOURCE_TABLE}")
print(f"Target Table        : {TARGET_TABLE}")

print()

print(f"Batch ID            : {BATCH_ID}")

print()

print(f"Source Records      : {prepared_df.count()}")
print(f"Records Written     : {final_silver_df.count()}")

print()

print(f"New Records         : {new_df.count()}")
print(f"Changed Records     : {changed_df.count()}")
print(f"Expired Records     : {expired_df.count()}")
print(f"Unchanged Records   : {unchanged_df.count()}")

print()

print(f"Validation Status   : {'PASSED' if validation_passed else 'FAILED'}")

print()

print(f"Pipeline Duration   : {pipeline_duration}")

print("=" * 60)
print("Silver Billing Pipeline Completed Successfully.")
print("=" * 60)

# COMMAND ----------

