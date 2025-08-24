# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "368c303b-7cea-4aea-8bfd-4130827f5b28",
# META       "default_lakehouse_name": "lh_production",
# META       "default_lakehouse_workspace_id": "35e0b5a5-904f-46bc-b35c-0f6b5c993a5c",
# META       "known_lakehouses": [
# META         {
# META           "id": "368c303b-7cea-4aea-8bfd-4130827f5b28"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Standalone Run Notebook 2: Create nutrients Table and Insert Metadata Descriptions

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql import Row

# -----------------------------------------------
# Drop and recreate nutrients table
# -----------------------------------------------
spark.sql("DROP TABLE IF EXISTS nutrients")

spark.sql("""
CREATE TABLE nutrients (
    category STRING,
    event_start_datetime TIMESTAMP,
    last_edit_datetime TIMESTAMP,
    year INT,
    month STRING,
    actual_q10_kgs DOUBLE,
    planned_q10_kgs DOUBLE,
    actual_qh_kgs DOUBLE,
    planned_qh_kgs DOUBLE,
    oee_net_percent DOUBLE,
    oee_gross_percent DOUBLE,
    scheduled_downtime_hrs DOUBLE,
    scheduled_downtime_percent DOUBLE,
    equipment_downtime_hrs DOUBLE,
    equipment_downtime_percent DOUBLE,
    process_downtime_hrs DOUBLE,
    process_downtime_percent DOUBLE,
    trouble_downtime_hrs DOUBLE,
    trouble_downtime_percent DOUBLE,
    packaging_yield_percent DOUBLE,
    yield_qh_percent DOUBLE,
    yield_q10_percent DOUBLE,
    var_cost_q10_qh_prod DOUBLE,
    var_cost_q10_q10_prod DOUBLE,
    var_cost_qh_prod DOUBLE,
    maintenance_cost_percent DOUBLE,
    fixed_cost_percent DOUBLE,
    global_safety_environmental_comments STRING,
    global_equipment_process_downtime_comments STRING,
    global_process_yield_comments STRING,
    global_scheduled_downtime_comments STRING,
    global_project_x_comments STRING,
    ntr_quality_issues STRING,
    global_summary_significant_items_month STRING,
    global_fixed_costs_total_maintenance_comments STRING,
    global_variable_cost_comments STRING
)
USING DELTA
""")

# -----------------------------------------------
# Insert metadata descriptions into table_column_metadata
# -----------------------------------------------
metadata = [
    ("nutrients", "category", "KPI category, e.g., Nutrients Monthly KPIs, Process, Safety"),
    ("nutrients", "event_start_datetime", "Date and time when the KPI event started"),
    ("nutrients", "last_edit_datetime", "Date and time when the KPI event was last edited"),
    ("nutrients", "year", "Reporting year (e.g., 2023)"),
    ("nutrients", "month", "Reporting month (e.g., July)"),
    ("nutrients", "actual_q10_kgs", "Actual Q10 production in kilograms"),
    ("nutrients", "planned_q10_kgs", "Planned Q10 production in kilograms"),
    ("nutrients", "actual_qh_kgs", "Actual QH production in kilograms"),
    ("nutrients", "planned_qh_kgs", "Planned QH production in kilograms"),
    ("nutrients", "oee_net_percent", "Overall Equipment Effectiveness (Net %)"),
    ("nutrients", "oee_gross_percent", "Overall Equipment Effectiveness (Gross %)"),
    ("nutrients", "scheduled_downtime_hrs", "Scheduled downtime in hours"),
    ("nutrients", "scheduled_downtime_percent", "Scheduled downtime as a percentage"),
    ("nutrients", "equipment_downtime_hrs", "Equipment downtime in hours"),
    ("nutrients", "equipment_downtime_percent", "Equipment downtime as a percentage"),
    ("nutrients", "process_downtime_hrs", "Process downtime in hours"),
    ("nutrients", "process_downtime_percent", "Process downtime as a percentage"),
    ("nutrients", "trouble_downtime_hrs", "Trouble downtime in hours"),
    ("nutrients", "trouble_downtime_percent", "Trouble downtime as a percentage"),
    ("nutrients", "packaging_yield_percent", "Packaging yield as a percentage"),
    ("nutrients", "yield_qh_percent", "Yield for QH as a percentage"),
    ("nutrients", "yield_q10_percent", "Yield for Q10 as a percentage"),
    ("nutrients", "var_cost_q10_qh_prod", "Variable cost of Q10 - QH production in dollars"),
    ("nutrients", "var_cost_q10_q10_prod", "Variable cost of Q10 - Q10 production in dollars"),
    ("nutrients", "var_cost_qh_prod", "Variable cost of QH production in dollars"),
    ("nutrients", "maintenance_cost_percent", "Maintenance cost as a percentage of budget"),
    ("nutrients", "fixed_cost_percent", "Fixed cost as a percentage of budget"),
    ("nutrients", "global_safety_environmental_comments", "Safety & Environmental comments"),
    ("nutrients", "global_equipment_process_downtime_comments", "Equipment / Process Downtime comments (> 12 hours)"),
    ("nutrients", "global_process_yield_comments", "Process Yield Comments"),
    ("nutrients", "global_scheduled_downtime_comments", "Scheduled Downtime comments"),
    ("nutrients", "global_project_x_comments", "Project X Roadmap Comments"),
    ("nutrients", "ntr_quality_issues", "Quality issues"),
    ("nutrients", "global_summary_significant_items_month", "Summary of Significant Items for Month"),
    ("nutrients", "global_fixed_costs_total_maintenance_comments", "Fixed Costs - Total and Maintenance comments"),
    ("nutrients", "global_variable_cost_comments", "Variable Cost comments")
]

meta_df = spark.createDataFrame(metadata, ["table", "column", "description"])
meta_df.write.format("delta").mode("append").saveAsTable("table_column_metadata")
print("nutrients table and metadata inserted into table_column_metadata.")

# -----------------------------------------------
# Table : search_config - insert entry
# -----------------------------------------------
search_config_data = [
    ("Nutrients Monthly KPIs", "2943fcac-9418-487c-aa70-a33773d8673a")
]
search_config_schema = StructType([
    StructField("category", StringType(), True),
    StructField("mandatorGuid", StringType(), True)
])
search_config_df = spark.createDataFrame(search_config_data, schema=search_config_schema)
search_config_df.write.format("delta").mode("append").saveAsTable("search_config")
print("Inserted data into table: search_config")

# -----------------------------------------------
# Table : last_run_times - insert entry
# -----------------------------------------------
default_time = "2023-12-31T23:59:59.9999Z"
last_run_data = [Row(category="Nutrients Monthly KPIs", lastRunEndTime=default_time)]
last_run_schema = StructType([
    StructField("category", StringType(), True),
    StructField("lastRunEndTime", StringType(), True)
])
last_run_df = spark.createDataFrame(last_run_data)
last_run_df.write.format("delta").mode("append").saveAsTable("last_run_times")
print("Inserted data into table: last_run_times")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
