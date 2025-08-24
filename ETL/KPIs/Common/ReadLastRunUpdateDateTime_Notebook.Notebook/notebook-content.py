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

# Fabric Notebook: Export all last_run_times records to last_run.json

from pyspark.sql.functions import col
import json
import os

# Read the entire last_run_times table
df = spark.read.table("last_run_times")

# Collect all rows as list of dictionaries
records = [row.asDict() for row in df.collect()]

# Debug print
print(f"Found {len(records)} records in last_run_times")
for r in records:
    print(r)

# Convert to JSON string
json_string = json.dumps(records, default=str)  # default=str for datetime safety

# Ensure output folder exists
output_dir = "/lakehouse/default/Files/pipeline_output"
os.makedirs(output_dir, exist_ok=True)

# Save as single file
output_path = os.path.join(output_dir, "last_run.json")
with open(output_path, "w") as f:
    f.write(json_string)

print(f"✅ last_run.json written with {len(records)} records at {output_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_production.dbo.nutrients LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
