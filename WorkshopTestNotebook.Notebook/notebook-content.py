# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c725455a-d7c6-40b2-b508-807049e8df30",
# META       "default_lakehouse_name": "RawLakehouse",
# META       "default_lakehouse_workspace_id": "c815bfee-333b-4393-ad24-1e2a43b4e3bb",
# META       "known_lakehouses": [
# META         {
# META           "id": "c725455a-d7c6-40b2-b508-807049e8df30"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/dimension_customer.csv")
# df now is a Spark DataFrame containing CSV data from "Files/dimension_customer.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/flights/flights.csv")
# df now is a Spark DataFrame containing CSV data from "Files/flights/flights.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
