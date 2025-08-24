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

# Update the last edit datetime of shiftconnector production kpis in table: last_run_times for a data pipeline

from datetime import datetime
from pyspark.sql import Row

# Parameters from pipeline
category = category if isinstance(category, str) else "Nutrients Monthly KPIs"
last_run = last_run if isinstance(last_run, str) else datetime.utcnow().isoformat() + "Z"


# Convert to timestamp
row = Row(category=category, lastRunEndTime=last_run)

df = spark.createDataFrame([row])
df.createOrReplaceTempView("new_run_time")

spark.sql("""
MERGE INTO last_run_times AS target
USING new_run_time AS source
ON target.category = source.category
WHEN MATCHED THEN UPDATE SET target.lastRunEndTime = source.lastRunEndTime
WHEN NOT MATCHED THEN INSERT (category, lastRunEndTime) VALUES (source.category, source.lastRunEndTime)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
