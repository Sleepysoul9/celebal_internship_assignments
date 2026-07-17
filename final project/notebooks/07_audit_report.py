# Databricks notebook source
# ==========================================================
# Project : Healthcare Data Platform (Medallion Architecture)
# Notebook: 07_audit_report
# Author  : Kushagra Sanghi
#
# Purpose :
# Query the centralized audit log to generate a pipeline
# execution report, summarize processing statistics,
# monitor execution status, identify failures, and provide
# operational insights for the Medallion Architecture.
# ==========================================================

# COMMAND ----------

# Import Required Libraries

from datetime import datetime

from pyspark.sql.functions import (
    col,
    count,
    sum,
    max,
    min,
    avg,
    desc
)

# COMMAND ----------

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA healthcare")

# COMMAND ----------

# Configuration

PIPELINE_START_TIME = datetime.now()

print("=" * 60)
print("Initializing Audit Report")
print("=" * 60)

print(f"Report Generated At : {PIPELINE_START_TIME}")

print("=" * 60)

# COMMAND ----------

# Load Audit Log

audit_df = spark.table("audit_log")

print("=" * 60)
print("Audit Log Loaded")
print("=" * 60)

print(f"Total Pipeline Runs : {audit_df.count()}")

print()

display(audit_df)

# COMMAND ----------

# Latest Pipeline Execution

print("=" * 60)
print("Latest Pipeline Execution")
print("=" * 60)

latest_execution = (

    audit_df

    .orderBy(desc("end_time"))

)

display(latest_execution)

print()

print(f"Latest Pipeline Stage : {latest_execution.first()['pipeline_stage']}")

print(f"Latest Status         : {latest_execution.first()['status']}")

print(f"Latest Execution Time : {latest_execution.first()['end_time']}")

print("=" * 60)

# COMMAND ----------

# Pipeline Status Summary

print("=" * 60)
print("Pipeline Status Summary")
print("=" * 60)

status_summary = (

    audit_df

    .groupBy("pipeline_stage", "status")

    .agg(

        count("*").alias("pipeline_runs")

    )

    .orderBy("pipeline_stage")

)

display(status_summary)

# COMMAND ----------

# Execution Time Analysis

from pyspark.sql.functions import (
    unix_timestamp,
    round,
    avg,
    max,
    min
)

print("=" * 60)
print("Pipeline Execution Time Analysis")
print("=" * 60)

# Calculate execution duration in seconds
execution_df = (

    audit_df

    .withColumn(
        "execution_time_seconds",
        round(
            unix_timestamp(col("end_time")) -
            unix_timestamp(col("start_time")),
            2
        )
    )

)

# Display execution time for each pipeline step
display(

    execution_df.select(
        "pipeline_stage",
        "table_name",
        "status",
        "execution_time_seconds"
    )

)

# Stage-wise execution statistics
execution_summary = (

    execution_df

    .groupBy("pipeline_stage")

    .agg(
        round(avg("execution_time_seconds"), 2).alias("avg_execution_time_sec"),
        round(min("execution_time_seconds"), 2).alias("min_execution_time_sec"),
        round(max("execution_time_seconds"), 2).alias("max_execution_time_sec")
    )

    .orderBy("pipeline_stage")

)

print("\nExecution Time Summary")

display(execution_summary)

# COMMAND ----------

# Pipeline Failure Analysis

print("=" * 60)
print("Pipeline Failure Analysis")
print("=" * 60)

failed_runs = (

    audit_df

    .filter(col("status") == "FAILED")

)

failure_count = failed_runs.count()

print(f"Total Failed Pipeline Executions : {failure_count}")

print()

if failure_count == 0:

    print("✓ No pipeline failures detected.")

else:

    print("Failed Pipeline Details")

    display(

        failed_runs.select(
            "pipeline_stage",
            "table_name",
            "source_file",
            "start_time",
            "end_time",
            "error_message"
        )

    )

# COMMAND ----------

# Records Processed Summary

print("=" * 60)
print("Records Processed Summary")
print("=" * 60)

records_summary = (

    audit_df

    .groupBy("pipeline_stage")

    .agg(
        sum("records_processed").alias("total_records_processed"),
        avg("records_processed").alias("average_records_processed"),
        max("records_processed").alias("maximum_records_processed"),
        min("records_processed").alias("minimum_records_processed")
    )

    .orderBy("pipeline_stage")

)

display(records_summary)

print()

total_records = (

    audit_df

    .agg(
        sum("records_processed").alias("total")
    )

    .first()["total"]

)

print(f"Overall Records Processed : {total_records}")

# COMMAND ----------

# Pipeline Execution Summary

print("=" * 60)
print("Healthcare Data Platform")
print("Pipeline Execution Summary")
print("=" * 60)

total_runs = audit_df.count()

successful_runs = (
    audit_df
    .filter(col("status") == "SUCCESS")
    .count()
)

failed_runs = (
    audit_df
    .filter(col("status") == "FAILED")
    .count()
)

total_records = (
    audit_df
    .agg(
        sum("records_processed").alias("total_records")
    )
    .first()["total_records"]
)

pipeline_stages = (
    audit_df
    .select("pipeline_stage")
    .distinct()
    .count()
)

print(f"Project Name             : Healthcare Data Platform")
print(f"Pipeline Stages          : {pipeline_stages}")
print(f"Total Pipeline Executions: {total_runs}")
print(f"Successful Executions    : {successful_runs}")
print(f"Failed Executions        : {failed_runs}")
print(f"Total Records Processed  : {total_records}")
print(f"Report Generated At      : {PIPELINE_START_TIME}")

print()

if failed_runs == 0:
    print("Pipeline Health Status   : HEALTHY")
    print("All pipeline stages executed successfully.")
else:
    print("Pipeline Health Status   : ATTENTION REQUIRED")
    print("One or more pipeline stages failed.")

print("=" * 60)
print("Audit Report Completed Successfully")
print("=" * 60)

# COMMAND ----------

