# Databricks notebook source
# ==========================================================
# Project : Healthcare Data Platform (Medallion Architecture)
# Notebook: 01_ingest_bronze
# Author  : Kushagra Sanghi
# Purpose :
#     Read raw CSV files from Landing Volume and
#     create Bronze Delta tables.
# ==========================================================

# COMMAND ----------

from pyspark.sql.functions import (
    current_timestamp,
    lit,
    col
)

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

LANDING_PATH = "/Volumes/workspace/healthcare/landing/"

# COMMAND ----------

# Read Metadata Configuration

metadata_df = (
    spark.table("metadata_config")
         .filter(col("is_active") == True)
)

display(metadata_df)

print(f"Active datasets : {metadata_df.count()}")


# COMMAND ----------

# Validate Landing Volume

landing_files = [
    file.name
    for file in dbutils.fs.ls(LANDING_PATH)
]

print("Files available in Landing Volume:")

for file in landing_files:
    print(file)

# COMMAND ----------

# Validate Metadata Against Landing Files

metadata_files = [
    row["source_file"]
    for row in metadata_df.collect()
]

missing_files = [
    file
    for file in metadata_files
    if file not in landing_files
]

if len(missing_files) == 0:
    print("Metadata validation successful.")
    print("All configured source files are available.")
else:
    raise Exception(
        f"Missing source files : {missing_files}"
    )

# COMMAND ----------

# Audit Logging Function

def log_audit(
    batch_id: str,
    pipeline_stage: str,
    source_file: str,
    table_name: str,
    status: str,
    records_processed: int,
    start_time,
    end_time,
    remarks: str,
    error_message=""
):
    """
    Logs each pipeline execution into the audit_log table.

    Parameters:
        batch_id           : Unique execution batch identifier.
        pipeline_stage     : Pipeline layer (e.g., BRONZE, SILVER, GOLD).
        source_file        : Input file processed.
        table_name         : Output Delta table.
        status             : SUCCESS / FAILED.
        records_processed  : Number of records processed.
        start_time         : Processing start timestamp.
        end_time           : Processing end timestamp.
        remarks            : Additional execution remarks.
        error_message      : Error details (None if execution succeeds).
    """


    audit_data = [{
        "batch_id": batch_id,
        "pipeline_stage": pipeline_stage,
        "source_file": source_file,
        "table_name": table_name,
        "status": status,
        "records_processed": int(records_processed),
        "start_time": start_time,
        "end_time": end_time,
        "remarks": remarks,
        "error_message": error_message if error_message else ""
    }]

    audit_df = spark.createDataFrame(audit_data)

    (
        audit_df.write
                .mode("append")
                .saveAsTable("audit_log")
    )

# COMMAND ----------

# Bronze Layer Ingestion

metadata_rows = metadata_df.collect()

processed_tables = []

for row in metadata_rows:

    source_file = row["source_file"]
    bronze_table = row["bronze_table"]

    print(f"\nProcessing : {source_file}")

    start_time = datetime.now()

    try:

        # Read Source CSV

        df = (
            spark.read
                 .option("header", True)
                 .option("inferSchema", "true")
                 .csv(LANDING_PATH + source_file)
        )

        source_count = df.count()

        assert source_count > 0, \
            f"{source_file} contains no records."


        # Add Bronze Technical Columns
        
        bronze_df = (
            df
            .withColumn("batch_id", lit(BATCH_ID))
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", lit(source_file))
        )


        # Write Bronze Delta Table
        
        (
            bronze_df.write
                     .format("delta")
                     .mode("overwrite")
                     .saveAsTable(bronze_table)
        )


        # Validate Load
        
        target_count = (
            spark.table(bronze_table)
                 .count()
        )


        end_time = datetime.now()


        if source_count == target_count:
            status = "SUCCESS"
            remarks = "Row count validated successfully."

        else:
            status = "WARNING"
            remarks = (
                f"Source Count={source_count}, "
                f"Target Count={target_count}"
            )


        # Audit Success
        
        log_audit(
            batch_id=BATCH_ID,
            pipeline_stage="Bronze",
            source_file=source_file,
            table_name=bronze_table,
            status=status,
            records_processed=target_count,
            start_time=start_time,
            end_time=end_time,
            remarks=remarks,
            error_message=""
        )


        processed_tables.append(bronze_table)

        print(
            f"✓ {bronze_table} loaded successfully "
            f"({target_count} rows)"
        )


    except Exception as e:

        end_time = datetime.now()

        # Audit Failure
        
        log_audit(
            batch_id=BATCH_ID,
            pipeline_stage="Bronze",
            source_file=source_file,
            table_name=bronze_table,
            status="FAILED",
            records_processed=0,
            start_time=start_time,
            end_time=end_time,
            remarks="Bronze ingestion failed.",
            error_message=str(e)
        )


        print(f"✗ Failed : {source_file}")
        print(f"Reason : {str(e)}")

# COMMAND ----------

print("=" * 60)
print("Bronze Layer Validation")
print("=" * 60)

expected_tables = {
    "bronze_patients",
    "bronze_doctors",
    "bronze_appointments",
    "bronze_treatments",
    "bronze_billing"
}

actual_tables = {
    row.tableName
    for row in spark.sql("SHOW TABLES").collect()
    if row.tableName.startswith("bronze_")
}

assert actual_tables == expected_tables, \
    f"Bronze tables mismatch.\nExpected: {expected_tables}\nFound: {actual_tables}"

print("✓ All Bronze tables created.")

# COMMAND ----------

audit_count = (
    spark.table("audit_log")
         .filter(col("batch_id") == BATCH_ID)
         .count()
)

assert audit_count == 5, \
    f"Expected 5 audit records, found {audit_count}"

print("✓ Audit logging validated.")

# COMMAND ----------

print("=" * 60)
print("Bronze Layer Execution Summary")
print("=" * 60)

print(f"Batch ID : {BATCH_ID}")
print(f"Datasets Processed : {len(processed_tables)}")

for table in processed_tables:
    rows = spark.table(table).count()
    print(f"{table:<25} {rows} rows")

print()

print("Bronze ingestion completed successfully.")
print("=" * 60)

# COMMAND ----------

spark.table("bronze_patients").printSchema()

display(spark.table("bronze_patients"))

display(spark.table("bronze_patients").summary())

# COMMAND ----------

