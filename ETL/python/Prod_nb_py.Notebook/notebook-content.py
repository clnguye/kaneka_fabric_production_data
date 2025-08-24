# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
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

abfs_path = "abfss://Playground@onelake.dfs.fabric.microsoft.com/CN_LH.Lakehouse/Files/Staging/data_sample.xlsx"
relative_path = "Files/Staging/data_sample.xlsx"
api_path = "/lakehouse/default/Files/Staging/data_sample.xlsx"

df = pd.read_excel(abfs_path)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
