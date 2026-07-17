# Databricks notebook source
# ==========================================================
# Project : Healthcare Data Platform (Medallion Architecture)
# Notebook: 03_silver_appointments
# Author  : Kushagra Sanghi
#
# Purpose :
#     Transform Bronze Appointment data into Silver Layer by
#     applying data quality rules, business validation,
#     appointment enrichment and SCD Type 2.
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
    regexp_extract,
    regexp_replace,
    sha2,
    concat,
    concat_ws,
    substring,
    length,
    coalesce,
    to_date,
    current_date,
    row_number,
    max
)

import uuid
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import Row
from pyspark.sql.window import Window

# COMMAND ----------

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA healthcare")

# COMMAND ----------

# ==========================================================
# Pipeline Configuration
# ==========================================================

PROJECT_NAME = "Healthcare Data Platform"

PIPELINE_VERSION = "1.0"

BATCH_ID = str(uuid.uuid4())

PIPELINE_START_TIME = datetime.now()

SOURCE_TABLE = "bronze_appointments"

TARGET_TABLE = "silver_appointments"

# COMMAND ----------

# Read Bronze Appointment Data

bronze_df = spark.table(SOURCE_TABLE)

source_record_count = bronze_df.count()

print("=" * 60)
print("Bronze Appointment Data Loaded")
print("=" * 60)

print(f"Source Table   : {SOURCE_TABLE}")
print(f"Records Loaded : {source_record_count}")

display(bronze_df)

# COMMAND ----------

# Source Data Validation

print("=" * 60)
print("Validating Bronze Appointment Data")
print("=" * 60)

# Row Count Validation

if source_record_count == 0:
    raise Exception("Source table is empty. Pipeline aborted.")

print(f"✓ Row Count Validation Passed : {source_record_count} records found.")

# Mandatory Column Validation

required_columns = [
    "appointment_id",
    "patient_id",
    "doctor_id",
    "appointment_date",
    "appointment_time",
    "reason_for_visit",
    "status",
    "batch_id",
    "ingestion_timestamp",
    "source_file"
]

missing_columns = [
    column
    for column in required_columns
    if column not in bronze_df.columns
]

if missing_columns:
    raise Exception(
        f"Missing Required Columns: {', '.join(missing_columns)}"
    )

print("✓ Mandatory Column Validation Passed.")

print("=" * 60)
print("Source Validation Completed Successfully.")
print("=" * 60)

# COMMAND ----------

# Data Cleaning & Standardization

print("=" * 60)
print("Cleaning and Standardizing Appointment Data")
print("=" * 60)

prepared_df = (

    bronze_df

    # Trim String Columns
    
    .withColumn("appointment_id", trim(col("appointment_id")))
    .withColumn("patient_id", trim(col("patient_id")))
    .withColumn("doctor_id", trim(col("doctor_id")))
    .withColumn("reason_for_visit", trim(col("reason_for_visit")))
    .withColumn("status", trim(col("status")))

    # Standardize Status
    
    .withColumn(
        "status",
        upper(col("status"))
    )

)

# Remove Duplicate Appointment Records

window_spec = Window.partitionBy("appointment_id") \
                    .orderBy(col("ingestion_timestamp").desc())

prepared_df = (

    prepared_df

    .withColumn(
        "row_num",
        row_number().over(window_spec)
    )

    .filter(col("row_num") == 1)

    .drop("row_num")

)

print("✓ String Cleaning Completed.")

print("✓ Appointment Status Standardized.")

print("✓ Duplicate Appointment Records Removed.")

print(f"Records After Cleaning : {prepared_df.count()}")

display(prepared_df)

# COMMAND ----------

# Business Rule Validation

print("=" * 60)
print("Applying Business Rule Validation")
print("=" * 60)

# Valid Appointment Status

valid_status = [
    "SCHEDULED",
    "COMPLETED",
    "CANCELLED",
    "NO SHOW"
]

prepared_df = (

    prepared_df

    .withColumn(

        "appointment_valid",

        when(
            col("appointment_id").isNull(), False
        )

        .when(
            col("patient_id").isNull(), False
        )

        .when(
            col("doctor_id").isNull(), False
        )

        .when(
            col("appointment_date").isNull(), False
        )

        .when(
            ~col("status").isin(valid_status), False
        )

        .otherwise(True)

    )

)

valid_records = prepared_df.filter(col("appointment_valid")).count()

invalid_records = prepared_df.filter(~col("appointment_valid")).count()

print(f"Valid Appointment Records   : {valid_records}")

print(f"Invalid Appointment Records : {invalid_records}")

display(prepared_df)

# COMMAND ----------

# SCD Type 2 Preparation

print("=" * 60)
print("Preparing SCD Type 2 Dataset")
print("=" * 60)

# Single Timestamp for Entire Batch

SCD_TIMESTAMP = datetime.now()

# Generate Business Hash

prepared_df = (

    prepared_df

    .withColumn(

        "business_hash",

        sha2(

            concat_ws(

                "||",

                coalesce(col("patient_id"), lit("")),

                coalesce(col("doctor_id"), lit("")),

                coalesce(col("appointment_date").cast("string"), lit("")),

                coalesce(col("appointment_time").cast("string"), lit("")),

                coalesce(col("reason_for_visit"), lit("")),

                coalesce(col("status"), lit(""))

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

        "appointment_id",
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

                    coalesce(col("doctor_id"), lit("")),

                    coalesce(col("appointment_date").cast("string"), lit("")),

                    coalesce(col("appointment_time").cast("string"), lit("")),

                    coalesce(col("reason_for_visit"), lit("")),

                    coalesce(col("status"), lit(""))

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

            on="appointment_id",

            how="left"

        )

    )

    # New Appointments
    
    new_df = (

        joined_df

        .filter(col("old.appointment_id").isNull())

        .select("new.*")

    )

    # Changed Appointments
    
    changed_df = (

        joined_df

        .filter(

            (col("old.appointment_id").isNotNull())

            &

            (col("new.business_hash") != col("old.business_hash"))

        )

        .select("new.*")

    )

    # Unchanged Appointments
    
    unchanged_df = (

        joined_df

        .filter(

            (col("old.appointment_id").isNotNull())

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

            .select("appointment_id")

            .alias("chg"),

            on="appointment_id",

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

# ==========================================================
# Persist SCD Type 2 (Part A)
# ==========================================================

print("=" * 60)
print("Persisting Silver Appointment Table")
print("=" * 60)

# ----------------------------------------------------------
# First Execution
# ----------------------------------------------------------

if not table_exists:

    print("Creating Silver Appointment Table...")

    final_silver_df = (

        prepared_df

        .drop("business_hash")

    )

# ----------------------------------------------------------
# Incremental Execution
# ----------------------------------------------------------

else:

    print("Applying SCD Type 2...")

    # ------------------------------------------------------
    # Historical Records
    # ------------------------------------------------------

    historical_df = (

        spark.table(TARGET_TABLE)

        .filter(col("is_current") == False)

    )

    # ------------------------------------------------------
    # Current Active Records
    # ------------------------------------------------------

    active_df = (

        spark.table(TARGET_TABLE)

        .filter(col("is_current") == True)

    )

    # ------------------------------------------------------
    # Keep Unchanged Records
    # ------------------------------------------------------

    active_unchanged_df = (

        active_df.alias("old")

        .join(

            unchanged_df

            .select("appointment_id")

            .alias("same"),

            "appointment_id",

            "inner"

        )

        .select("old.*")

    )

    # ------------------------------------------------------
    # Expire Changed Records
    # ------------------------------------------------------

    expired_records_df = (

        expired_df

        .drop("business_hash")

    )

    # ------------------------------------------------------
    # Determine Next Record Version
    # ------------------------------------------------------

    version_df = (

        active_df

        .groupBy("appointment_id")

        .agg(

            max("record_version").alias("max_version")

        )

    )

    # ------------------------------------------------------
    # Build Version 2 Records
    # ------------------------------------------------------

    changed_version_df = (

        changed_df.alias("new")

        .join(

            version_df.alias("ver"),

            "appointment_id",

            "left"

        )

        .withColumn(

            "record_version",

            coalesce(col("max_version"), lit(0)) + lit(1)

        )

        .drop("max_version")

        .drop("business_hash")

    )

    # ------------------------------------------------------
    # New Appointment Records
    # ------------------------------------------------------

    new_records_df = (

        new_df

        .drop("business_hash")

    )

    # ------------------------------------------------------
    # Build Final Silver Dataset
    # ------------------------------------------------------

    final_silver_df = (

        historical_df

        .unionByName(expired_records_df)

        .unionByName(active_unchanged_df)

        .unionByName(changed_version_df)

        .unionByName(new_records_df)

    )

# ==========================================================
# Persist SCD Type 2 (Part B)
# ==========================================================

# ----------------------------------------------------------
# Persist Final Silver Table
# ----------------------------------------------------------

(

    final_silver_df

    .write

    .format("delta")

    .mode("overwrite")

    .option("overwriteSchema", "true")

    .saveAsTable(TARGET_TABLE)

)

# ----------------------------------------------------------
# Execution Summary
# ----------------------------------------------------------

rows_written = final_silver_df.count()

print()

print(f"Rows Written : {rows_written}")

print()

print("✓ Silver Appointment Table Persisted Successfully.")

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

    remarks = "Silver Appointment Pipeline Completed Successfully.",

    source_file = "bronze_appointments",

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

# Duplicate Appointment IDs

duplicate_count = (

    silver_df

    .groupBy("appointment_id")

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

    .filter(col("appointment_id").isNull())

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

# Validation Results

print(f"Duplicate Appointment IDs              : {duplicate_count}")

print(f"Current Records                        : {current_record_count}")

print(f"Historical Records                     : {historical_record_count}")

print(f"Invalid Record Versions                : {invalid_version_count}")

print(f"NULL Appointment IDs                   : {null_key_count}")

print(f"Current Records with effective_to Set  : {invalid_current_records}")

# Overall Validation Status

validation_passed = (

    duplicate_count == 0

    and invalid_version_count == 0

    and null_key_count == 0

    and invalid_current_records == 0

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
print("Silver Appointment Pipeline Completed Successfully.")
print("=" * 60)

# COMMAND ----------

