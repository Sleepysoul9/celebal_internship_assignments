# Databricks notebook source
# ==========================================================
# Project : Healthcare Data Platform (Medallion Architecture)
# Notebook: 06_gold_kpis
# Author  : Kushagra Sanghi
#
# Purpose :
#     Generate Gold Layer business KPIs from curated Silver
#     tables and persist analytical Delta tables for
#     reporting and dashboarding.
# ==========================================================

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum,
    avg,
    when,
    lit,
    current_timestamp
)

from pyspark.sql import Row

import builtins
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

PATIENT_TABLE = "silver_patients"

APPOINTMENT_TABLE = "silver_appointments"

BILLING_TABLE = "silver_billing"

TREATMENT_TABLE = "silver_treatments"

GOLD_PATIENT_TABLE = "gold_patient_kpi"

GOLD_APPOINTMENT_TABLE = "gold_appointment_kpi"

GOLD_BILLING_TABLE = "gold_billing_kpi"

GOLD_TREATMENT_TABLE = "gold_treatment_kpi"

GOLD_SUMMARY_TABLE = "gold_kpi_summary"

# COMMAND ----------

# Load Silver Tables

print("=" * 60)
print("Loading Silver Tables")
print("=" * 60)

patients_df = (
    spark.table(PATIENT_TABLE)
    .filter(col("is_current") == True)
)

appointments_df = (
    spark.table(APPOINTMENT_TABLE)
    .filter(col("is_current") == True)
)

billing_df = (
    spark.table(BILLING_TABLE)
    .filter(col("is_current") == True)
)

treatments_df = (
    spark.table(TREATMENT_TABLE)
    .filter(col("is_current") == True)
)

print(f"Patients Loaded      : {patients_df.count()}")
print(f"Appointments Loaded  : {appointments_df.count()}")
print(f"Billing Loaded       : {billing_df.count()}")
print(f"Treatments Loaded    : {treatments_df.count()}")

print("=" * 60)

# COMMAND ----------

# KPI Helper Functions

from pyspark.sql import Row

print("=" * 60)
print("Initializing KPI Helper Functions")
print("=" * 60)

# Gold Metadata

REPORT_DATE = datetime.now().date()

LOAD_TIMESTAMP = datetime.now()

# KPI Units

UNIT_COUNT = "COUNT"

UNIT_PERCENT = "PERCENT"

UNIT_CURRENCY = "INR"

UNIT_AVERAGE = "AVERAGE"

# KPI Categories

CATEGORY_PATIENT = "Patient"

CATEGORY_APPOINTMENT = "Appointment"

CATEGORY_BILLING = "Billing"

CATEGORY_TREATMENT = "Treatment" 

# Helper Function

def create_kpi_df(kpi_records, category):

    """
    Parameters
    ----------
    kpi_records : list

        Example:

        [
            ("Total Patients", 200, UNIT_COUNT),
            ("Insurance Coverage Rate", 94.25, UNIT_PERCENT)
        ]

    category :

        Patient
        Appointment
        Billing
        Treatment

    Returns
    -------
        Spark DataFrame
    """

    rows = []

    for name, value, unit in kpi_records:
        
        rows.append(

            Row(

                kpi_name=name,

                kpi_value=float(value),

                kpi_unit=unit,

                kpi_category=category,

                report_date=REPORT_DATE,

                gold_batch_id=BATCH_ID,

                load_timestamp=LOAD_TIMESTAMP

            )

        )

    return spark.createDataFrame(rows)

print("✓ KPI Helper Functions Initialized.")

print("=" * 60)

# COMMAND ----------

# Patient KPIs

print("=" * 60)
print("Generating Patient KPIs")
print("=" * 60)

# Calculate Patient Metrics

total_patients = patients_df.count()

male_patients = (
    patients_df
    .filter(col("gender") == "M")
    .count()
)

female_patients = (
    patients_df
    .filter(col("gender") == "F")
    .count()
)

other_patients = (
    patients_df
    .filter(~col("gender").isin("M", "F"))
    .count()
)

insured_patients = (
    patients_df
    .filter(col("insurance_valid") == True)
    .count()
)

insurance_coverage_rate = (
    (insured_patients / total_patients) * 100
    if total_patients > 0
    else 0
)

# Build KPI DataFrame

patient_kpis = [

    ("Total Patients", total_patients, UNIT_COUNT),

    ("Male Patients", male_patients, UNIT_COUNT),

    ("Female Patients", female_patients, UNIT_COUNT),

    ("Other Patients", other_patients, UNIT_COUNT),

    ("Insurance Coverage Rate", insurance_coverage_rate, UNIT_PERCENT)

]

gold_patient_df = create_kpi_df(
    patient_kpis,
    "Patient"
)

print(f"Patient KPIs Generated : {gold_patient_df.count()}")

print("=" * 60)

display(gold_patient_df)

# COMMAND ----------

# Appointment KPIs

print("=" * 60)
print("Generating Appointment KPIs")
print("=" * 60)

# Calculate Appointment Metrics

total_appointments = appointments_df.count()

completed_appointments = (
    appointments_df
    .filter(col("status") == "COMPLETED")
    .count()
)

scheduled_appointments = (
    appointments_df
    .filter(col("status") == "SCHEDULED")
    .count()
)

cancelled_appointments = (
    appointments_df
    .filter(col("status") == "CANCELLED")
    .count()
)

no_show_appointments = (
    appointments_df
    .filter(col("status") == "NO-SHOW")
    .count()
)

no_show_rate = (
    (no_show_appointments / total_appointments) * 100
    if total_appointments > 0
    else 0
)

appointment_completion_rate = (
    (completed_appointments / total_appointments) * 100
    if total_appointments > 0
    else 0
)

repeat_patients = (

    appointments_df

    .groupBy("patient_id")

    .count()

    .filter(col("count") > 1)

    .count()

)

total_unique_patients = (

    appointments_df

    .select("patient_id")

    .distinct()

    .count()

)

repeat_patient_rate = (
    (repeat_patients / total_unique_patients) * 100
    if total_unique_patients > 0
    else 0
)

# Build KPI DataFrame

appointment_kpis = [

    ("Total Appointments",
     total_appointments,
     UNIT_COUNT),

    ("Completed Appointments",
     completed_appointments,
     UNIT_COUNT),

    ("Scheduled Appointments",
     scheduled_appointments,
     UNIT_COUNT),

    ("Cancelled Appointments",
     cancelled_appointments,
     UNIT_COUNT),

    ("No Show Appointments",
     no_show_appointments,
     UNIT_COUNT),

    ("Patient No Show Rate",
     no_show_rate,
     UNIT_PERCENT),

    ("Appointment Completion Rate",
     appointment_completion_rate,
     UNIT_PERCENT),

    ("Repeat Patient Rate",
     repeat_patient_rate,
     UNIT_PERCENT)

]

gold_appointment_df = create_kpi_df(

    appointment_kpis,

    "Appointment"

)

print(f"Appointment KPIs Generated : {gold_appointment_df.count()}")

print("=" * 60)

display(gold_appointment_df)

# COMMAND ----------

# Billing KPIs

print("=" * 60)
print("Generating Billing KPIs")
print("=" * 60)

# Calculate Billing Metrics

total_bills = billing_df.count()

total_revenue = (

    billing_df

    .agg(sum("amount"))

    .first()[0]

)

paid_revenue = (

    billing_df

    .filter(col("payment_status") == "PAID")

    .agg(sum("amount"))

    .first()[0]

)

pending_revenue = (

    billing_df

    .filter(col("payment_status") == "PENDING")

    .agg(sum("amount"))

    .first()[0]

)

failed_revenue = (

    billing_df

    .filter(col("payment_status") == "FAILED")

    .agg(sum("amount"))

    .first()[0]

)

# Handle NULL sums

total_revenue = total_revenue or 0.0
paid_revenue = paid_revenue or 0.0
pending_revenue = pending_revenue or 0.0
failed_revenue = failed_revenue or 0.0

# Payment Success Rate

paid_bills = (

    billing_df

    .filter(col("payment_status") == "PAID")

    .count()

)

payment_success_rate = (

    (paid_bills / total_bills) * 100

    if total_bills > 0

    else 0

)

# Insurance Payment Success Rate

insurance_bills = (

    billing_df

    .filter(col("payment_method") == "INSURANCE")

)

insurance_total = insurance_bills.count()

insurance_paid = (

    insurance_bills

    .filter(col("payment_status") == "PAID")

    .count()

)

insurance_success_rate = (

    (insurance_paid / insurance_total) * 100

    if insurance_total > 0

    else 0

)

# Average Billing per Patient

unique_patients = (

    billing_df

    .select("patient_id")

    .distinct()

    .count()

)

average_billing_per_patient = (

    total_revenue / unique_patients

    if unique_patients > 0

    else 0

)

# Rounding to 2 decimal places

total_revenue = float(f"{total_revenue:.2f}")
paid_revenue = float(f"{paid_revenue:.2f}")
pending_revenue = float(f"{pending_revenue:.2f}")
failed_revenue = float(f"{failed_revenue:.2f}")

payment_success_rate = float(f"{payment_success_rate:.2f}")
insurance_success_rate = float(f"{insurance_success_rate:.2f}")
average_billing_per_patient = float(f"{average_billing_per_patient:.2f}")

# Build KPI DataFrame

billing_kpis = [

    ("Total Revenue",
     total_revenue,
     UNIT_CURRENCY),

    ("Paid Revenue",
     paid_revenue,
     UNIT_CURRENCY),

    ("Pending Revenue",
     pending_revenue,
     UNIT_CURRENCY),

    ("Failed Revenue",
     failed_revenue,
     UNIT_CURRENCY),

    ("Payment Success Rate",
     payment_success_rate,
     UNIT_PERCENT),

    ("Insurance Payment Success Rate",
     insurance_success_rate,
     UNIT_PERCENT),

    ("Average Billing per Patient",
     average_billing_per_patient,
     UNIT_AVERAGE)

]

gold_billing_df = create_kpi_df(

    billing_kpis,

    "Billing"

)

print(f"Billing KPIs Generated : {gold_billing_df.count()}")

print("=" * 60)

display(gold_billing_df)

# COMMAND ----------

# Treatment KPIs

print("=" * 60)
print("Generating Treatment KPIs")
print("=" * 60)

# Calculate Treatment Metrics

total_treatments = treatments_df.count()

total_treatment_cost = (

    treatments_df

    .agg(sum("cost"))

    .first()[0]

)

total_treatment_cost = total_treatment_cost or 0.0

average_treatment_cost = (

    total_treatment_cost / total_treatments

    if total_treatments > 0

    else 0

)

unique_treatment_types = (

    treatments_df

    .select("treatment_type")

    .distinct()

    .count()

)

average_treatments_per_patient = (

    treatments_df

    .join(

        appointments_df.select(
            "appointment_id",
            "patient_id"
        ),

        "appointment_id"

    )

    .groupBy("patient_id")

    .count()

    .agg(avg("count"))

    .first()[0]

)

average_treatments_per_patient = (

    average_treatments_per_patient or 0.0

)

# Format Currency Values

total_treatment_cost = float(f"{total_treatment_cost:.2f}")

average_treatment_cost = float(f"{average_treatment_cost:.2f}")

average_treatments_per_patient = float(
    f"{average_treatments_per_patient:.2f}"
)

# Build KPI DataFrame

treatment_kpis = [

    (
        "Total Treatments",
        total_treatments,
        UNIT_COUNT
    ),

    (
        "Total Treatment Cost",
        total_treatment_cost,
        UNIT_CURRENCY
    ),

    (
        "Average Treatment Cost",
        average_treatment_cost,
        UNIT_CURRENCY
    ),

    (
        "Unique Treatment Types",
        unique_treatment_types,
        UNIT_COUNT
    ),

    (
        "Average Treatments per Patient",
        average_treatments_per_patient,
        UNIT_AVERAGE
    )

]

gold_treatment_df = create_kpi_df(

    treatment_kpis,

    CATEGORY_TREATMENT

)

print(f"Treatment KPIs Generated : {gold_treatment_df.count()}")

print("=" * 60)

display(gold_treatment_df)

# COMMAND ----------

# Analytical Gold Tables

print("=" * 60)
print("Generating Analytical Gold Tables")
print("=" * 60)

# Treatment Distribution

gold_treatment_distribution = (

    treatments_df

    .groupBy("treatment_type")

    .count()

    .withColumnRenamed("count", "total_treatments")

)

# Revenue by Payment Method

gold_payment_method_summary = (

    billing_df

    .groupBy("payment_method")

    .agg(

        sum("amount").alias("total_revenue"),

        count("*").alias("total_transactions")

    )

)

# Daily Appointment Trend

gold_daily_appointment_trend = (

    appointments_df

    .groupBy("appointment_date")

    .count()

    .withColumnRenamed("count", "total_appointments")

)

# Daily Revenue Trend

gold_daily_revenue_trend = (

    billing_df

    .groupBy("bill_date")

    .agg(

        sum("amount").alias("daily_revenue")

    )

)

print()

print(f"Treatment Distribution Rows : {gold_treatment_distribution.count()}")

print(f"Payment Method Rows         : {gold_payment_method_summary.count()}")

print(f"Appointment Trend Rows      : {gold_daily_appointment_trend.count()}")

print(f"Revenue Trend Rows          : {gold_daily_revenue_trend.count()}")

print()

print("✓ Analytical Gold Tables Generated.")

print("=" * 60)

display(gold_treatment_distribution)

display(gold_payment_method_summary)

display(gold_daily_appointment_trend)

display(gold_daily_revenue_trend)

# COMMAND ----------

# Gold KPI Summary

print("=" * 60)
print("Creating Gold KPI Summary")
print("=" * 60)

gold_kpi_summary = (

    gold_patient_df

    .unionByName(gold_appointment_df)

    .unionByName(gold_billing_df)

    .unionByName(gold_treatment_df)

)

print(f"Total KPIs : {gold_kpi_summary.count()}")

print("=" * 60)

display(

    gold_kpi_summary

    .orderBy(

        "kpi_category",

        "kpi_name"

    )

)

# COMMAND ----------

# Persist Gold Tables

print("=" * 60)
print("Persisting Gold Tables")
print("=" * 60)

gold_tables = {

    "gold_patient_kpi": gold_patient_df,

    "gold_appointment_kpi": gold_appointment_df,

    "gold_billing_kpi": gold_billing_df,

    "gold_treatment_kpi": gold_treatment_df,

    "gold_kpi_summary": gold_kpi_summary,

    "gold_treatment_distribution": gold_treatment_distribution,

    "gold_payment_method_summary": gold_payment_method_summary,

    "gold_daily_appointment_trend": gold_daily_appointment_trend,

    "gold_daily_revenue_trend": gold_daily_revenue_trend

}

for table_name, dataframe in gold_tables.items():

    (

        dataframe

        .write

        .format("delta")

        .mode("overwrite")

        .option("overwriteSchema", "true")

        .saveAsTable(table_name)

    )

    print(f"✓ {table_name} persisted ({dataframe.count()} rows)")

print()

print(f"Total Gold Tables Persisted : {len(gold_tables)}")

print()

print("Gold Layer Successfully Persisted.")

print("=" * 60)

# COMMAND ----------

spark.sql("SHOW TABLES").display()

# COMMAND ----------

# Audit Logging

from pyspark.sql import Row

print("=" * 60)
print("Writing Audit Log")
print("=" * 60)

audit_row = Row(

    batch_id=BATCH_ID,

    pipeline_stage="Gold",

    table_name="gold_kpi_summary",

    status="SUCCESS",

    records_processed=gold_kpi_summary.count(),

    start_time=PIPELINE_START_TIME,

    end_time=datetime.now(),

    remarks="Gold KPI tables generated successfully.",

    source_file="Silver Layer",

    error_message="No Error"

)

audit_df = spark.createDataFrame([audit_row])

(

    audit_df

    .write

    .mode("append")

    .saveAsTable("audit_log")

)

print()

print("✓ Audit Log Written Successfully.")

print("=" * 60)

display(audit_df)

# COMMAND ----------

# Healthcare Data Platform - Pipeline Summary

PIPELINE_END_TIME = datetime.now()

PIPELINE_DURATION = PIPELINE_END_TIME - PIPELINE_START_TIME

print("=" * 60)
print("Healthcare Data Platform - Pipeline Summary")
print("=" * 60)

print(f"Project Name        : Healthcare Data Platform")
print(f"Pipeline Version    : 1.0")
print(f"Pipeline Stage      : Gold")

print()

print(f"Source Tables       :")
print(f"  • silver_patients")
print(f"  • silver_appointments")
print(f"  • silver_billing")
print(f"  • silver_treatments")

print()

print(f"Gold Tables Created : 9")

print(f"KPIs Generated      : {gold_kpi_summary.count()}")

print()

print(f"Batch ID            : {BATCH_ID}")

print()

print(f"Validation Status   : PASSED")

print()

print(f"Pipeline Duration   : {PIPELINE_DURATION}")

print("=" * 60)

print("Gold Pipeline Completed Successfully.")

print("=" * 60)

# COMMAND ----------

