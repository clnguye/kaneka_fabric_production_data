# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e514fcc4-32b2-4760-8376-596ef804dd2c",
# META       "default_lakehouse_name": "CN_LH",
# META       "default_lakehouse_workspace_id": "23ad811c-4534-4c1d-a2f4-7004bb07d91e",
# META       "known_lakehouses": [
# META         {
# META           "id": "e514fcc4-32b2-4760-8376-596ef804dd2c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

import pandas as pd

abfs_path = "abfss://Playground@onelake.dfs.fabric.microsoft.com/CN_LH.Lakehouse/Files/Staging/data_sample.csv"
relative_path = "Files/Staging/data_sample.csv"
api_path = "/lakehouse/default/Files/Staging/data_sample.csv"

df = spark.read.option("header", "true").option("delimiter", ",").csv(f"{abfs_path}")
# display(df)

df_pd = df.toPandas()
# df_pd.describe()
df_pd.columns


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pd.loc[:5, ["Carrier", "SCAC"]]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pd.iloc[:5,:4]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pd.iloc[-3:,-4:]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df[(df['col1'] > 2) & (df['col3'] < 10)]
df_where = df_pd[(df_pd['Carrier'] == 'ZIM') & (df_pd['MasterIdentifier'] == '098-97376086')]
df_where.iloc[:,:2]

print(type(df))
print(type(df_pd))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
