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

# Standalone Run Notebook 1: Drop and Create Config Tables - table_column_metadata (empty), search_config (empty), last_run_times (empty)

from pyspark.sql.types import StructType, StructField, StringType

# -----------------------------------------------
# Empty table_column_metadata (no entries)
# -----------------------------------------------
spark.sql("DROP TABLE IF EXISTS table_column_metadata")
schema = StructType([
    StructField("table", StringType(), True),
    StructField("column", StringType(), True),
    StructField("description", StringType(), True)
])
empty_df = spark.createDataFrame([], schema)
empty_df.write.format("delta").mode("overwrite").saveAsTable("table_column_metadata")
print("Created empty table: table_column_metadata")

# ----------------------------------------------------------------------------------------
# Table : search_config (empty) for Shift Connector -> Production KPIs data fetch
# ----------------------------------------------------------------------------------------
spark.sql("DROP TABLE IF EXISTS search_config")
search_config_schema = StructType([
    StructField("category", StringType(), True),
    StructField("mandatorGuid", StringType(), True)
])
empty_search_df = spark.createDataFrame([], schema=search_config_schema)
empty_search_df.write.format("delta").mode("overwrite").saveAsTable("search_config")
print("Created empty table: search_config")

# ----------------------------------------------------------------------------------------
# Table : last_run_times (empty) for Shift Connector -> Production KPIs data pipeline runs
# ----------------------------------------------------------------------------------------
spark.sql("DROP TABLE IF EXISTS last_run_times")
last_run_schema = StructType([
    StructField("category", StringType(), True),
    StructField("lastRunEndTime", StringType(), True)
])
empty_last_run_df = spark.createDataFrame([], schema=last_run_schema)
empty_last_run_df.write.format("delta").mode("overwrite").saveAsTable("last_run_times")
print("Created empty table: last_run_times")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
