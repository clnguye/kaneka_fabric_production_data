# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "611311dc-9cc4-4d11-985c-f72719b3a7ec",
# META       "default_lakehouse_name": "lh_production",
# META       "default_lakehouse_workspace_id": "e2648ada-5289-4e11-9e8b-bb5333b66af2",
# META       "known_lakehouses": [
# META         {
# META           "id": "368c303b-7cea-4aea-8bfd-4130827f5b28"
# META         },
# META         {
# META           "id": "611311dc-9cc4-4d11-985c-f72719b3a7ec"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook: Insert nutrients Rows from Web JSON with Composite Key Uniqueness
import json
from datetime import datetime
import pytz
from decimal import Decimal
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Schema for nutrients (updated with new comment fields)
schema = StructType([
    StructField("category", StringType(), True),
    StructField("event_start_datetime", TimestampType(), True),
    StructField("last_edit_datetime", TimestampType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", StringType(), True),
    StructField("actual_q10_kgs", DoubleType(), True),
    StructField("planned_q10_kgs", DoubleType(), True),
    StructField("actual_qh_kgs", DoubleType(), True),
    StructField("planned_qh_kgs", DoubleType(), True),
    StructField("oee_net_percent", DoubleType(), True),
    StructField("oee_gross_percent", DoubleType(), True),
    StructField("scheduled_downtime_hrs", DoubleType(), True),
    StructField("scheduled_downtime_percent", DoubleType(), True),
    StructField("equipment_downtime_hrs", DoubleType(), True),
    StructField("equipment_downtime_percent", DoubleType(), True),
    StructField("process_downtime_hrs", DoubleType(), True),
    StructField("process_downtime_percent", DoubleType(), True),
    StructField("trouble_downtime_hrs", DoubleType(), True),
    StructField("trouble_downtime_percent", DoubleType(), True),
    StructField("packaging_yield_percent", DoubleType(), True),
    StructField("yield_qh_percent", DoubleType(), True),
    StructField("yield_q10_percent", DoubleType(), True),
    StructField("var_cost_q10_qh_prod", DoubleType(), True),
    StructField("var_cost_q10_q10_prod", DoubleType(), True),
    StructField("var_cost_qh_prod", DoubleType(), True),
    StructField("maintenance_cost_percent", DoubleType(), True),
    StructField("fixed_cost_percent", DoubleType(), True),
    StructField("global_safety_environmental_comments", StringType(), True),
    StructField("global_equipment_process_downtime_comments", StringType(), True),
    StructField("global_process_yield_comments", StringType(), True),
    StructField("global_scheduled_downtime_comments", StringType(), True),
    StructField("global_project_x_comments", StringType(), True),
    StructField("ntr_quality_issues", StringType(), True),
    StructField("global_summary_significant_items_month", StringType(), True),
    StructField("global_fixed_costs_total_maintenance_comments", StringType(), True),
    StructField("global_variable_cost_comments", StringType(), True)
])

# ---------- CONFIG: INPUT ----------
print("input_json_array_str is:", input_json_array_str[:100])
raw_input = input_json_array_str if isinstance(input_json_array_str, str) else "[]"

# ---------- HELPERS ----------
def clean_row(row):
    d = row.asDict()
    cleaned = {}
    for k, v in d.items():
        if isinstance(v, Decimal):
            cleaned[k] = float(v)
        elif isinstance(v, datetime):
            cleaned[k] = v
        elif isinstance(v, str):
            cleaned[k] = v
        elif isinstance(v, (int, float)):
            cleaned[k] = v
        elif v is None:
            cleaned[k] = None
        else:
            cleaned[k] = str(v)
    return cleaned

def extract_field(bfs, name):
    for field in bfs:
        if field.get("businessFieldName") == name:
            return field.get("businessFieldValue")
    return None

def extract_value(obj, default=None):
    if isinstance(obj, list) and obj:
        return obj[0].get("translatedTitle", default)
    return obj or default

def parse_date(dt):
    try:
        return datetime.fromisoformat(dt.replace("Z", "+00:00")) if dt else None
    except:
        return None

def to_decimal(val):
    try:
        return Decimal(str(val)) if val is not None else None
    except:
        return None

# ---------- PARSE JSON ----------
try:
    parsed = json.loads(raw_input)
except Exception as e:
    print("Failed to parse production_json:", e)
    print("raw_input is:", raw_input[:100])
    parsed = []

rows = []
for entry in parsed:
    bfs = entry.get("businessFieldValues", [])
    row = Row(
        category=extract_value(extract_field(bfs, "Category").get("name"), "Unknown"),
        event_start_datetime=parse_date(extract_field(bfs, "EventStartDateTime").get("startDateTime")),
        last_edit_datetime=parse_date(entry.get("lastEditDateTime", "")),
        year=int(extract_value(extract_field(bfs, "GLOBAL_Year"), 0)),
        month=extract_value(extract_field(bfs, "GLOBAL_Month"), "Unknown"),
        actual_q10_kgs=to_decimal(extract_field(bfs, "KNA_NTR_ActualProductionKgs")),
        planned_q10_kgs=to_decimal(extract_field(bfs, "KNA_NTR_PlannedProductionKgs")),
        actual_qh_kgs=to_decimal(extract_field(bfs, "KNA_NTR_ActualQHProductionKgs")),
        planned_qh_kgs=to_decimal(extract_field(bfs, "KNA_NTR_PlannedQHProductionKgs")),
        oee_net_percent=to_decimal(extract_field(bfs, "GLOBAL_OEE-Net")),
        oee_gross_percent=to_decimal(extract_field(bfs, "GLOBAL_OEE-Gross")),
        scheduled_downtime_hrs=to_decimal(extract_field(bfs, "GLOBAL_ScheduledDowntimeHrs")),
        scheduled_downtime_percent=to_decimal(extract_field(bfs, "GLOBAL_ScheduledDowntime")),
        equipment_downtime_hrs=to_decimal(extract_field(bfs, "GLOBAL_EquipmentDowntimeHrs")),
        equipment_downtime_percent=to_decimal(extract_field(bfs, "GLOBAL_EquipmentDowntime")),
        process_downtime_hrs=to_decimal(extract_field(bfs, "GLOBAL_ProcessDowntimeHrs")),
        process_downtime_percent=to_decimal(extract_field(bfs, "GLOBAL_ProcessDowntime")),
        trouble_downtime_hrs=to_decimal(extract_field(bfs, "GLOBAL_TroubleDowntimeHrs")),
        trouble_downtime_percent=to_decimal(extract_field(bfs, "GLOBAL_TroubleDowntime")),
        packaging_yield_percent=to_decimal(extract_field(bfs, "KNA_NTR_PackagingYield")),
        yield_qh_percent=to_decimal(extract_field(bfs, "KNA_NTR_Yield-QH")),
        yield_q10_percent=to_decimal(extract_field(bfs, "KNA_NTR_Yield-Q10")),
        var_cost_q10_qh_prod=to_decimal(extract_field(bfs, "KNA_NTR_VariablecostQ10-QHProd")),
        var_cost_q10_q10_prod=to_decimal(extract_field(bfs, "KNA_NTR_VariablecostQ10-Q10Prod")),
        var_cost_qh_prod=to_decimal(extract_field(bfs, "KNA_NTR_Variablecost-QH")),
        maintenance_cost_percent=to_decimal(extract_field(bfs, "GLOBAL_MaintenanceCostofBudget")),
        fixed_cost_percent=to_decimal(extract_field(bfs, "GLOBAL_FixedCostofBudget")),
        global_safety_environmental_comments=extract_field(bfs, "GLOBAL_SafetyEnvironmentalcomments"),
        global_equipment_process_downtime_comments=extract_field(bfs, "GLOBAL_EquipmentProcessDowntimecomments"),
        global_process_yield_comments=extract_field(bfs, "GLOBAL_ProcessYieldComments"),
        global_scheduled_downtime_comments=extract_field(bfs, "GLOBAL_ScheduledDowntimecomments"),
        global_project_x_comments=extract_field(bfs, "GLOBAL_ProjectXcomments"),
        ntr_quality_issues=extract_field(bfs, "NTR_Qualityissues"),
        global_summary_significant_items_month=extract_field(bfs, "GLOBAL_SummaryofSignificantItemsforMonth"),
        global_fixed_costs_total_maintenance_comments=extract_field(bfs, "GLOBAL_FixedCosts-TotalandMaintenancecomments"),
        global_variable_cost_comments=extract_field(bfs, "GLOBAL_VariableCostcomments")
    )
    rows.append(row)

print("Rows:")
print(rows[:2])

# ---------- MERGE INTO TABLE ----------
if rows:
    clean_rows = [clean_row(r) for r in rows]
    df = spark.createDataFrame(clean_rows, schema=schema)
    df.createOrReplaceTempView("incoming_kpis")

    spark.sql("""
        MERGE INTO nutrients AS target
        USING incoming_kpis AS source
        ON target.year = source.year
           AND target.month = source.month
           AND target.category = source.category
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"Merged {len(clean_rows)} rows into nutrients table.")
else:
    print("No rows to insert.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
