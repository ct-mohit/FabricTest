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

# Create multi column DataFrame from RDD
employees = sc.parallelize(
                            [
                                (1, "John", 10000),
                                (2, "Fred", 20000),
                                (3, "Anna", 30000),
                                (4, "James", 40000),
                                (5, "Mohit", 50000)
                            ]
                          )

employeesDF = employees.toDF()

employeesDF.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employeesDF = employeesDF.toDF("id", "name", "salary")

employeesDF.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter data

newdf = (
            employeesDF
                .where("salary > 20000")
        )

display(
  newdf
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Working with DataFrames

# CELL ********************

greenTaxisFilePath = "Files/Raw/GreenTaxis_201911.csv"

taxiZonesFilePath = "Files/Raw/TaxiZones.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read csv file
greenTaxiDF = (
                  spark
                    .read                     
                    .csv(greenTaxisFilePath)
              )

display(greenTaxiDF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read csv file
greenTaxiDF = (
                  spark
                    .read                     
                    .option("header", "true")
                    .csv(greenTaxisFilePath)
              )

display(greenTaxiDF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read csv file
greenTaxiDF = (
                  spark
                    .read                     
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(greenTaxisFilePath)
              )

display(greenTaxiDF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Applying Schemas

# CELL ********************

# Create schema for Green Taxi Data

from pyspark.sql.functions import *
from pyspark.sql.types import *
  
greenTaxiSchema = (
            StructType()               
               .add("VendorId", "integer")
               .add("lpep_pickup_datetime", "timestamp")
               .add("lpep_dropoff_datetime", "timestamp")
               .add("store_and_fwd_flag", "string")
               .add("RatecodeID", "integer")
               .add("PULocationID", "integer")
               .add("DOLocationID", "integer")
  
              .add("passenger_count", "integer")
              .add("trip_distance", "double")
              .add("fare_amount", "double")
              .add("extra", "double")
              .add("mta_tax", "double")
              .add("tip_amount", "double")
  
              .add("tolls_amount", "double")
              .add("ehail_fee", "double")
              .add("improvement_surcharge", "double")
              .add("total_amount", "double")
              .add("payment_type", "integer")
              .add("trip_type", "integer")
         )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read csv file
greenTaxiDF = (
                  spark
                    .read                     
                    .option("header", "true")
                    .schema(greenTaxiSchema)
                    .csv(greenTaxisFilePath)
              )

display(greenTaxiDF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Analyzing Data

# CELL ********************

display(
    greenTaxiDF.describe(
                             "passenger_count",                                     
                             "trip_distance"                                     
                        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cleaning Data

# CELL ********************

# Display the count before filtering
print("Before filter = " + str(greenTaxiDF.count()))

# Filter inaccurate data - ACCURACY CHECK
greenTaxiDF = (
                  greenTaxiDF
                          .where("passenger_count > 0")
                          .filter(col("trip_distance") > 0.0)
)

# Display the count after filtering
print("After filter = " + str(greenTaxiDF.count()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display the count before filtering
print("Before filter = " + str(greenTaxiDF.count()))

# Drop rows with nulls - COMPLETENESS CHECK
greenTaxiDF = (
                  greenTaxiDF
                          .na.drop('all')
              )

# Display the count after filtering
print("After filter = " + str(greenTaxiDF.count()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Map of default values
defaultValueMap = {'payment_type': 5, 'RateCodeID': 1}

# Replace nulls with default values - COMPLETENESS CHECK
greenTaxiDF = (
                  greenTaxiDF
                      .na.fill(defaultValueMap)
              )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display the count before filtering
print("Before filter = " + str(greenTaxiDF.count()))

# Drop duplicate rows - UNIQUENESS CHECK
greenTaxiDF = (
                  greenTaxiDF
                          .dropDuplicates()
              )

# Display the count after filtering
print("After filter = " + str(greenTaxiDF.count()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display the count before filtering
print("Before filter = " + str(greenTaxiDF.count()))

# Drop duplicate rows - TIMELINESS CHECK
greenTaxiDF = (
                  greenTaxiDF
                          .where("lpep_pickup_datetime >= '2019-11-01' AND lpep_dropoff_datetime < '2019-12-01'")
              )

# Display the count after filtering
print("After filter = " + str(greenTaxiDF.count()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Chain all operations together

greenTaxiDF = (
                  spark
                    .read                     
                    .option("header", "true")
                    .schema(greenTaxiSchema)
                    .csv(greenTaxisFilePath)
              )

greenTaxiDF = (
                  greenTaxiDF
                          .where("passenger_count > 0")
                          .filter(col("trip_distance") > 0.0)
  
                          .na.drop(how='all')
  
                          .na.fill(defaultValueMap)
  
                          .dropDuplicates()
  
                          .where("lpep_pickup_datetime >= '2019-11-01' AND lpep_dropoff_datetime < '2019-12-01'")
              )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Applying Transformations

# CELL ********************

greenTaxiDF = (
                  greenTaxiDF

                        # Select only limited columns
                        .select(
                                  col("VendorID"),
                                  col("passenger_count").alias("PassengerCount"),
                                  col("trip_distance").alias("TripDistance"),
                                  col("lpep_pickup_datetime").alias("PickupTime"),                          
                                  col("lpep_dropoff_datetime").alias("DropTime"), 
                                  col("PUlocationID").alias("PickupLocationId"), 
                                  col("DOlocationID").alias("DropLocationId"), 
                                  col("RatecodeID"), 
                                  col("total_amount").alias("TotalAmount"),
                                  col("payment_type").alias("PaymentType")
                               )
              )

greenTaxiDF.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# OPTION 1: Create a derived column - Trip time in minutes

greenTaxiDF = (
                  greenTaxiDF
                        .withColumn("TripTimeInMinutes", 
                                        round(
                                                (unix_timestamp(col("DropTime")) - unix_timestamp(col("PickupTime"))) 
                                                    / 60
                                             )
                                   )
              )

greenTaxiDF.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# OPTION 2: Create a derived column - Trip time in minutes

tripTimeInMinutes = round(
                            (unix_timestamp(col("DropTime")) - unix_timestamp(col("PickupTime"))) 
                              / 60
                         )                         

greenTaxiDF = (
                  greenTaxiDF
                        .withColumn("TripTimeInMinutes", tripTimeInMinutes)
              )

greenTaxiDF.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a derived column - Trip type, and drop SR_Flag column

tripTypeColumn = (
                    when(
                        col("RatecodeID") == 6,
                            "SharedTrip"
                     )
                    .otherwise("SoloTrip")
                 )

greenTaxiDF = (
                  greenTaxiDF
                        .withColumn("TripType", tripTypeColumn)
                        #.drop("RatecodeID")
              )

greenTaxiDF.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create derived columns for year, month and day
greenTaxiDF = (
                  greenTaxiDF
                        .withColumn("TripYear", year(col("PickupTime")))
                        .withColumn("TripMonth", month(col("PickupTime")))
                        .withColumn("TripDay", dayofmonth(col("PickupTime")))
              )

greenTaxiDF.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EXERCISES

# 1. Create 3 columns (TripYear, TripMonth, TripDay) by extracting year, month & day values from PickupTime column
# 2. Find a method to rename column RatecodeID to RateCodeId
# 3. Find distinct number of PickupLocationId
# 4. Based on PickupTime, add a new column with day text = Monday to Sunday
# 5. Based on PickupTime, add a new column with month text = January to December
# 6. Based on PickupTime, add a new column with value being the last day of month

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(greenTaxiDF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Group the data by TripDay and aggregate the TotalAmount

greenTaxiGroupedDF = (
                          greenTaxiDF
                            .groupBy("TripDay")
                            .agg(sum("TotalAmount").alias("total"))
                            .orderBy(col("TripDay").desc())
                     )
    
display(greenTaxiGroupedDF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EXERCISES

# 1. Round the TotalAmountPerDay to 2 decimal places
# 2. Find out the total number of trips per day

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read payment types json file
taxiZonesDF = (
                  spark
                      .read
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .csv(taxiZonesFilePath)
              )

display(taxiZonesDF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a dataframe joining trip data with zones

greenTaxiWithZonesDF = (
                          greenTaxiDF.alias("g")
                                     .join(taxiZonesDF.alias("t"),                                               
                                               col("t.LocationId") == col("g.PickupLocationId"),
                                              "inner"
                                          )
                       )

display(greenTaxiWithZonesDF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EXERCISES

# 1. Change the below code to read TaxiZones by defining schema, and not by inferring schema
# 2. JOIN greenTaxiWithZonesDF with TaxiZones on DropLocationId. And group by PickupZone and DropZone, and provide average of TotalAmount.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Working with Spark SQL

# CELL ********************

# Create a local temp view
greenTaxiDF.createOrReplaceTempView("GreenTaxiTripData")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
  spark.sql("SELECT PassengerCount, PickupTime FROM GreenTaxiTripData WHERE PickupLocationID = 1")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT PassengerCount, PickupTime 
# MAGIC FROM GreenTaxiTripData 
# MAGIC WHERE PickupLocationID = 1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EXERCISES 

# 1. Create views on greenTaxiDF and taxiZonesDF
# 2.a. Write SQL query - JOIN greentaxis view  with taxizones view on PickupLocationId
#   b. Join again with taxizones view on DropLocationId
#   c. Group by pickupzone and dropzone, and get average of amount

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Writing Output to Data Lake

# CELL ********************

# Load the dataframe as CSV to storage
(
    greenTaxiDF  
        .write
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
        .mode("overwrite")
        .csv("Files/Output/Facts/GreenTaxiFact.csv")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EXERCISES

# 1. Check out the number of output files.
# 2. Verify number of files against number of tasks by using Spark UI
# 3. Do you understand correlation between tasks and output files?

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the dataframe as parquet to storage
(
    greenTaxiDF  
      .write
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
      .mode("overwrite")
      .parquet("Files/Output/Facts/GreenTaxiFact.parquet")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EXERCISE

# Compare size of Parquet files with CSV files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Writing Output to Tables

# CELL ********************

(
  greenTaxiDF
    .write
    .mode("overwrite")
    .saveAsTable("FactGreenTaxiTrip")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM FactGreenTaxiTrip
# MAGIC LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED FactGreenTaxiTrip

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EXERCISES

# 1. Save GreenTaxis and TaxiZones as tables
# 2. Run JOIN query on both tables

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
