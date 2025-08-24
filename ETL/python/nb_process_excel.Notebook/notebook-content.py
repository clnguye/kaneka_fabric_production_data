# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4ad52bb8-607a-4af8-913c-a26a2c894bac",
# META       "default_lakehouse_name": "lh_production",
# META       "default_lakehouse_workspace_id": "31daa085-1aee-4a35-ba19-ccb939677fef",
# META       "known_lakehouses": [
# META         {
# META           "id": "4ad52bb8-607a-4af8-913c-a26a2c894bac"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Library Imports

# CELL ********************

import pandas as pd
from pyspark.sql import DataFrame as ps
from notebookutils import mssparkutils
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Combine duplicate KPIs with different monthly target

# CELL ********************

def combine_rows(df, kpi_col, target_col, kpi_name, target1, target2, new_target1=0, new_target2=0):
    df_focus = df[df[kpi_col] == kpi_name]
    idx1, idx2 = df_focus.index
    row1 = df.loc[idx1]
    row2 = df.loc[idx2]
    value_cols = [col for col in df_focus.columns if col not in [target_col, kpi_col]]
    new_value_row = {}
    new_target_row = {}
    for col in value_cols:
        if pd.notnull(row1[col]):
            new_value_row[col] = row1[col]
            new_target_row[col] = target1
        elif pd.notnull(row2[col]): 
            new_value_row[col] = row2[col]
            new_target_row[col] = target2

    new_value_row[kpi_col] = kpi_name
    new_target_row[kpi_col] = kpi_name
    new_value_row[target_col] = new_target1
    new_target_row[target_col] = new_target2

    df.loc[idx1] = new_value_row
    df.loc[idx2] = new_target_row
    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Nutrient Scorecard FY2024

# CELL ********************

df_nutrient_kpi = pd.read_excel(
            "abfss://Production_Data@onelake.dfs.fabric.microsoft.com/lh_production.Lakehouse/Files/PRD/OneDrive_1_5-22-2025/Nutrients Scorecard FY2024 KPIs.xlsx",
            sheet_name="2024 KPI Dashboard",
            usecols="A:B, D:P",      # Specify columns like "A:P" or a list [0, 1, ..., 15]
            skiprows=1,              # Skip rows *before* your desired start_row
            nrows=20                 # Number of rows to read from the start_row
        )

df_nutrient_kpi.columns = df_nutrient_kpi.columns.str.replace(' ', '_')
df_nutrient_kpi['Nutrients_FY2024'] = df_nutrient_kpi['Nutrients_FY2024'].ffill() # Use when two rows are merged
df_nutrient_kpi = combine_rows(df_nutrient_kpi, 'Nutrients_FY2024', 'Target', 'Variable cost Q10', 259, 262, 'Q10 prod. Actual', 'Q10 prod. Target')
df_nutrient_kpi = df_nutrient_kpi.fillna(value=0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

id_vars=[]

df_nutrient_kpi_melt = pd.melt(df_nutrient_kpi, 
            id_vars=['Nutrients_FY2024', 'Target'], 
            value_vars=[col for col in df_nutrient_kpi.columns if col not in id_vars], 
            var_name='Month', 
            value_name='Value'
)

file_path = 'Files/PRD/OneDrive_1_5-22-2025/Nutrients Scorecard FY2024 KPIs.xlsx'

file_info = mssparkutils.fs.ls(file_path)

modified_time = datetime.fromtimestamp(file_info[0].modifyTime / 1000)
df_nutrient_kpi_melt['Timestamp'] = modified_time
df_nutrient_kpi_melt['Timestamp'] = pd.to_datetime(df_nutrient_kpi_melt['Timestamp'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_nutrient_kpi_melt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df_nutrient_kpi = spark.createDataFrame(df_nutrient_kpi_melt)
# spark_df_nutrient_kpi.createOrReplaceTempView("table")
# result = spark.sql("SELECT * FROM table WHERE Nutrients_FY2024 = 'Variable cost Q10'")
# result.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tf_nutrient_kpi = spark_df_nutrient_kpi.select('*')
tf_nutrient_kpi.write.format("delta").mode("overwrite").saveAsTable("nutrient_kpi")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2024 Goals Running Total

# CELL ********************

df_oee = pd.read_excel(
    "abfss://Production_Data@onelake.dfs.fabric.microsoft.com/lh_production.Lakehouse/Files/PRD/OneDrive_1_5-22-2025/2024FY Goals Running Total .xlsx",
    sheet_name="OEE calculation",
    usecols="A:O", 
    skiprows=3, 
    nrows=16
)

df_oee = df_oee.ffill(axis=0)

df_oee_melt =pd.melt(df_oee, 
    id_vars=[df_oee.columns[0], df_oee.columns[1], df_oee.columns[2]],
    value_vars=[col for col in df_oee.columns if col not in id_vars],
    var_name='Month',
    value_name='Value'
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_oee_melt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dash = pd.read_excel(
    "abfss://Production_Data@onelake.dfs.fabric.microsoft.com/lh_production.Lakehouse/Files/PRD/OneDrive_1_5-22-2025/2024FY Goals Running Total .xlsx",
    sheet_name="Dashboard",
    usecols="B:Q", 
    skiprows=3, 
    nrows=20
)

# remove downstream row?

df_dash_melt =pd.melt(df_dash, 
    id_vars=[df_dash.columns[0], df_dash.columns[1]],
    value_vars=[col for col in df_dash.columns if col not in id_vars],
    var_name='Month',
    value_name='Value'
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_dash_melt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Apical Shift Report Apr 2025

# CELL ********************

df_prod_roll = pd.read_excel(
    "abfss://Production_Data@onelake.dfs.fabric.microsoft.com/lh_production.Lakehouse/Files/PRD/OneDrive_1_5-22-2025/Apical Shift Report April 2025.xlsm",
    sheet_name="Production Rolls",
    usecols="B:AD", 
    skiprows=0, 
    nrows=23
)

df_off_roll = pd.read_excel(
    "abfss://Production_Data@onelake.dfs.fabric.microsoft.com/lh_production.Lakehouse/Files/PRD/OneDrive_1_5-22-2025/Apical Shift Report April 2025.xlsm",
    sheet_name="Off Mill Roll",
    usecols="A:N", 
    skiprows=0, 
    nrows=17
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_prod_roll)
display(df_off_roll)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# ## CPVC OEE FY2024

# CELL ********************

df_line1 = pd.read_excel(
    "abfss://Production_Data@onelake.dfs.fabric.microsoft.com/lh_production.Lakehouse/Files/PRD/OneDrive_1_5-22-2025/CPVC OEE FY2024.xlsx",
    sheet_name="Input",
    usecols="C:W", 
    skiprows=23, 
    nrows=15
)

df_line2 = pd.read_excel(
    "abfss://Production_Data@onelake.dfs.fabric.microsoft.com/lh_production.Lakehouse/Files/PRD/OneDrive_1_5-22-2025/CPVC OEE FY2024.xlsx",
    sheet_name="Input",
    usecols="C:W", 
    skiprows=43, 
    nrows=15
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_line1)
display(df_line2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## FY2024 MSA OEE Report

# CELL ********************

df_line1 = pd.read_excel(
    "abfss://Production_Data@onelake.dfs.fabric.microsoft.com/lh_production.Lakehouse/Files/PRD/OneDrive_1_5-22-2025/FY2024 MSA OEE Report.xlsx",
    sheet_name="Input",
    usecols="C:W", 
    skiprows=23, 
    nrows=15
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 
