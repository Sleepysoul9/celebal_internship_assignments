# Databricks notebook source
# ==========================================================
# Project : Healthcare Data Platform (Medallion Architecture)
# Notebook: 02_silver_patients
# Author  : Kushagra Sanghi
#
# Purpose :
#     Transform Bronze Patient data into Silver Layer by
#     applying data quality rules, HIPAA masking,
#     insurance validation and SCD Type 2.
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
    substring,
    length,
    coalesce
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

SOURCE_TABLE = "bronze_patients"

TARGET_TABLE = "silver_patients"

# COMMAND ----------

# Read Bronze Patient Data

bronze_df = spark.table(SOURCE_TABLE)

display(bronze_df)

# COMMAND ----------

# Source Validation

print("=" * 60)
print("Validating Bronze Patient Table")
print("=" * 60)

source_count = bronze_df.count()

assert source_count > 0, \
    "Bronze Patient table is empty."

expected_columns = {
    "patient_id",
    "first_name",
    "last_name",
    "gender",
    "date_of_birth",
    "contact_number",
    "address",
    "registration_date",
    "insurance_provider",
    "insurance_number",
    "email",
    "batch_id",
    "ingestion_timestamp",
    "source_file"
}

actual_columns = set(bronze_df.columns)

missing_columns = expected_columns - actual_columns

assert len(missing_columns) == 0, \
    f"Missing columns : {missing_columns}"

print(f"Source Rows : {source_count}")

print("✓ Source validation successful.")

print("=" * 60)

# COMMAND ----------

# Data Cleaning & Standardization

print("=" * 60)
print("Cleaning Patient Data")
print("=" * 60)

initial_count = bronze_df.count()

silver_df = (
    bronze_df

    # Remove leading/trailing spaces
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn("address", trim(col("address")))
    .withColumn("insurance_provider", trim(col("insurance_provider")))
    .withColumn("insurance_number", trim(col("insurance_number")))
    .withColumn("email", trim(col("email")))

    # Standardization
    .withColumn("gender", upper(col("gender")))
    .withColumn("email", lower(col("email")))

    # Remove duplicate patients
    .dropDuplicates(["patient_id"])

    # Remove rows with mandatory fields missing
    .dropna(
        subset=[
            "patient_id",
            "first_name",
            "last_name",
            "gender",
            "date_of_birth"
        ]
    )
)

cleaned_count = silver_df.count()

duplicates_removed = initial_count - cleaned_count

print(f"Rows before cleaning      : {initial_count}")
print(f"Rows after cleaning       : {cleaned_count}")
print(f"Duplicates/Invalid removed: {duplicates_removed}")

print("✓ Data cleaning completed.")
print("=" * 60)

# COMMAND ----------

# Insurance Validation

print("=" * 60)
print("Insurance Validation")
print("=" * 60)

silver_df = (

    silver_df

    .withColumn(

        "insurance_valid",

        when(

            (col("insurance_provider").isNotNull()) &
            (regexp_extract(
                col("insurance_number"),
                r"^INS\d{6}$",
                0
            ) != ""),

            lit(True)

        ).otherwise(lit(False))

    )

)

valid_count = silver_df.filter(col("insurance_valid")).count()

invalid_count = silver_df.filter(~col("insurance_valid")).count()

print(f"Valid Insurance Records   : {valid_count}")
print(f"Invalid Insurance Records : {invalid_count}")

print("✓ Insurance validation completed.")

print("=" * 60)

# COMMAND ----------

# HIPAA PII Masking

print("=" * 60)
print("Applying HIPAA PII Masking")
print("=" * 60)

silver_df = (

    silver_df

    # Mask Contact Number
    .withColumn(
        "contact_number",
        concat(
            lit("XXXXXXX"),
            substring(col("contact_number").cast("string"), -3, 3)
        )
    )

    # Mask Email
    .withColumn(
        "email",
        concat(
            substring(col("email"), 1, 1),
            lit("********"),
            regexp_extract(col("email"), "@.*", 0)
        )
    )

    # Hash Address using SHA-256
    .withColumn(
        "address",
        sha2(col("address"), 256)
    )

)

print("✓ Contact Number masked")

print("✓ Email masked")

print("✓ Address hashed using SHA-256")

print("=" * 60)

display(
    silver_df.select(
        "patient_id",
        "contact_number",
        "email",
        "address"
    )
)

# COMMAND ----------

# SCD Type 2 Preparation

from datetime import datetime

from pyspark.sql.functions import (
    col,
    concat_ws,
    sha2,
    lit
)

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

                col("patient_id"),
                col("first_name"),
                col("last_name"),
                col("gender"),
                col("date_of_birth").cast("string"),
                col("contact_number"),
                col("address"),
                col("registration_date").cast("string"),
                col("insurance_provider"),
                col("insurance_number"),
                col("email")

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

print("✓ Business hash generated.")

print("✓ SCD metadata columns added.")

print("=" * 60)

display(

    prepared_df.select(

        "patient_id",
        "business_hash",
        "record_version",
        "is_current",
        "effective_from"

    )

)

# COMMAND ----------

# SCD Type 2 Change Detection

from pyspark.sql.functions import (
    col,
    lit
)

print("=" * 60)
print("SCD Type 2 Change Detection")
print("=" * 60)

# Check whether Silver table exists

table_exists = spark.catalog.tableExists("silver_patients")

if not table_exists:

    print("First execution detected.")
    print("silver_patients table does not exist.")

    current_df = spark.createDataFrame([], prepared_df.schema)

    new_df = prepared_df

    changed_df = spark.createDataFrame([], prepared_df.schema)

    unchanged_df = spark.createDataFrame([], prepared_df.schema)

    expired_df = spark.createDataFrame([], prepared_df.schema)

else:

    print("Incremental execution detected.")

    # Read Current Active Records
    
    current_df = (
        spark.table("silver_patients")
        .filter(col("is_current") == True)
        .withColumn(
            "business_hash",
            sha2(
                concat_ws(
                    "||",
                    col("patient_id"),
                    col("first_name"),
                    col("last_name"),
                    col("gender"),
                    col("date_of_birth").cast("string"),
                    col("contact_number"),
                    col("address"),
                    col("registration_date").cast("string"),
                    col("insurance_provider"),
                    col("insurance_number"),
                    col("email")
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

            on="patient_id",

            how="left"

        )

    )

    # New Patients
    
    new_df = (

        joined_df

        .filter(col("old.patient_id").isNull())

        .select("new.*")

    )

    # Changed Patients
    
    changed_df = (

        joined_df

        .filter(

            (col("old.patient_id").isNotNull())

            &

            (col("new.business_hash") != col("old.business_hash"))

        )

        .select("new.*")

    )

    # Unchanged Patients
    
    unchanged_df = (

        joined_df

        .filter(

            (col("old.patient_id").isNotNull())

            &

            (col("new.business_hash") == col("old.business_hash"))

        )

        .select("new.*")

    )

    # Records to Expire
    
    expired_df = (

        current_df.alias("old")

        .join(

            changed_df.select("patient_id").alias("chg"),

            on="patient_id",

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

print("✓ Change detection completed.")

print("=" * 60)

# COMMAND ----------

# Persist SCD Type 2 (Part A)

print("=" * 60)
print("Persisting Silver Patient Table")
print("=" * 60)

# FIRST EXECUTION

if not table_exists:

    print("Creating Silver Patient table...")

    final_silver_df = (

        prepared_df

        .drop("business_hash")

    )

# INCREMENTAL EXECUTION

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

            unchanged_df.select("patient_id").alias("same"),

            "patient_id",

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

        .groupBy("patient_id")

        .agg(

            {"record_version": "max"}

        )

        .withColumnRenamed(

            "max(record_version)",

            "max_version"

        )

    )

    # Build Version 2 Records
    
    changed_version_df = (

        changed_df.alias("new")

        .join(

            version_df.alias("ver"),

            "patient_id",

            "left"

        )

        .withColumn(

            "record_version",

            coalesce(col("max_version"), lit(0)) + lit(1)

        )

        .drop("max_version")

        .drop("business_hash")

    )

    # New Patients
    
    new_records_df = (

        new_df

        .drop("business_hash")

    )

    # Final Silver Dataset
    
    final_silver_df = (

        historical_df

        .unionByName(expired_records_df)

        .unionByName(active_unchanged_df)

        .unionByName(changed_version_df)

        .unionByName(new_records_df)

    )

# Persist Silver Table

(

    final_silver_df

    .write

    .format("delta")

    .mode("overwrite")

    .option("overwriteSchema", "true")

    .saveAsTable(TARGET_TABLE)

)

print()

print(f"Rows Written : {final_silver_df.count()}")

print()

print("✓ Silver Patient table persisted successfully.")

print("=" * 60)

# COMMAND ----------

# Audit Logging

from datetime import datetime

print("=" * 60)
print("Writing Audit Log")
print("=" * 60)

PIPELINE_END_TIME = datetime.now()

# Pipeline Metrics

records_written = final_silver_df.count()

new_count = new_df.count()

changed_count = changed_df.count()

unchanged_count = unchanged_df.count()

expired_count = expired_df.count()

remarks = (
    f"New={new_count}, "
    f"Changed={changed_count}, "
    f"Unchanged={unchanged_count}, "
    f"Expired={expired_count}"
)

# Audit Record

audit_df = spark.createDataFrame(

    [(
        BATCH_ID,
        "Silver",
        TARGET_TABLE,
        "SUCCESS",
        records_written,
        PIPELINE_START_TIME,
        PIPELINE_END_TIME,
        remarks,
        "patients.csv",
        "No Error"
    )],

    [
        "batch_id",
        "pipeline_stage",
        "table_name",
        "status",
        "records_processed",
        "start_time",
        "end_time",
        "remarks",
        "source_file",
        "error_message"
    ]

)

# Append Audit Record

(
    audit_df
    .write
    .mode("append")
    .saveAsTable("audit_log")
)

print(f"Records Written : {records_written}")

print("✓ Audit Log Updated Successfully.")

print("=" * 60)

display(

    spark.table("audit_log")

    .orderBy(col("end_time").desc())

    .limit(5)

)

# COMMAND ----------

# Silver Layer Validation

print("=" * 60)
print("Validating Silver Patient Table")
print("=" * 60)

silver_table = spark.table(TARGET_TABLE)

# Validation Metrics

total_records = silver_table.count()

current_records = silver_table.filter(
    col("is_current") == True
).count()

historical_records = silver_table.filter(
    col("is_current") == False
).count()

duplicate_patients = (

    silver_table

    .groupBy("patient_id")

    .count()

    .filter(col("count") > 1)

    .count()

)

null_patient_ids = (

    silver_table

    .filter(col("patient_id").isNull())

    .count()

)

invalid_versions = (

    silver_table

    .filter(col("record_version") < 1)

    .count()

)

invalid_current_records = (

    silver_table

    .filter(

        (col("is_current") == True) &

        (col("effective_to").isNotNull())

    )

    .count()

)

# Assertions

assert total_records == records_written, \
    "Row count mismatch."

assert duplicate_patients == 0, \
    "Duplicate patient IDs found."

assert null_patient_ids == 0, \
    "NULL patient IDs found."

assert invalid_versions == 0, \
    "Invalid record versions found."

assert invalid_current_records == 0, \
    "Current records cannot have effective_to populated."

# Validation Summary

print(f"Total Records        : {total_records}")

print(f"Current Records      : {current_records}")

print(f"Historical Records   : {historical_records}")

print(f"Duplicate Patients   : {duplicate_patients}")

print(f"NULL Patient IDs     : {null_patient_ids}")

print(f"Invalid Versions     : {invalid_versions}")

print(f"Invalid Current Rows : {invalid_current_records}")

print()

print("✓ Silver Layer Validation Passed")

print("=" * 60)

# COMMAND ----------

# Pipeline Execution Summary

from datetime import datetime

print("=" * 70)
print("        HEALTHCARE DATA PLATFORM - SILVER PIPELINE SUMMARY")
print("=" * 70)

pipeline_end = datetime.now()

pipeline_duration = round(
    (pipeline_end - PIPELINE_START_TIME).total_seconds(),
    2
)

print(f"Project Name          : {PROJECT_NAME}")
print(f"Pipeline Version      : {PIPELINE_VERSION}")
print(f"Pipeline Stage        : Silver Layer")
print(f"Source Table          : {SOURCE_TABLE}")
print(f"Target Table          : {TARGET_TABLE}")
print()

print(f"Batch ID              : {BATCH_ID}")
print(f"Pipeline Start Time   : {PIPELINE_START_TIME}")
print(f"Pipeline End Time     : {pipeline_end}")
print(f"Execution Time (sec)  : {pipeline_duration}")
print()

print(f"Source Records        : {source_count}")
print(f"Records Written       : {records_written}")
print()

print(f"New Records           : {new_count}")
print(f"Changed Records       : {changed_count}")
print(f"Unchanged Records     : {unchanged_count}")
print(f"Expired Records       : {expired_count}")
print()

print(f"Current Records       : {current_records}")
print(f"Historical Records    : {historical_records}")
print()

print("Pipeline Status       : SUCCESS")
print("Validation Status     : PASSED")
print("Audit Status          : LOGGED")

print("=" * 70)
print("Silver Patient Pipeline Completed Successfully.")
print("=" * 70)