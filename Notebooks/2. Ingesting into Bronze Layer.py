# Databricks notebook source
# MAGIC %md
# MAGIC ####Executing the common notebook to reuse shared functions and variables

# COMMAND ----------

# MAGIC %run "/Workspace/Users/user1@devineedisrilekhagmail.onmicrosoft.com/Project_Workspace/4. Common Notebook"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Taking the environment name as input from the widget

# COMMAND ----------

dbutils.widgets.text(name="environment",defaultValue="",label='Enter the environment in lower case')
env = dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating a function to read the traffic data from the external location

# COMMAND ----------

def readTrafficData():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    print("Reading the Raw Traffic Data :  ", end='\n')
    schema1 = StructType([
    StructField("Record_ID",IntegerType()),
    StructField("Count_point_id",IntegerType()),
    StructField("Direction_of_travel",StringType()),
    StructField("Year",IntegerType()),
    StructField("Count_date",StringType()),
    StructField("hour",IntegerType()),
    StructField("Region_id",IntegerType()),
    StructField("Region_name",StringType()),
    StructField("Local_authority_name",StringType()),
    StructField("Road_name",StringType()),
    StructField("Road_Category_ID",IntegerType()),
    StructField("Start_junction_road_name",StringType()),
    StructField("End_junction_road_name",StringType()),
    StructField("Latitude",DoubleType()),
    StructField("Longitude",DoubleType()),
    StructField("Link_length_km",DoubleType()),
    StructField("Pedal_cycles",IntegerType()),
    StructField("Two_wheeled_motor_vehicles",IntegerType()),
    StructField("Cars_and_taxis",IntegerType()),
    StructField("Buses_and_coaches",IntegerType()),
    StructField("LGV_Type",IntegerType()),
    StructField("HGV_Type",IntegerType()),
    StructField("EV_Car",IntegerType()),
    StructField("EV_Bike",IntegerType())
    ])

    rawTrafficStream = (
        spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFile.schemaLocation",f'{checkpoint}/rawTrafficLoad/schemaInfer').option("header",True).schema(schema1).load(landing+'/raw_traffic/').withColumn("Extract_Time",current_timestamp())
    )
    print("Succeeded reading the Traffic data")
    print("---------------------------------------------------------------")

    return rawTrafficStream


# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating a function to write the traffic data in the bronze schema

# COMMAND ----------

def writeTrafficData(writedf,env):
    writeStream = writedf.writeStream.format("delta").option("checkPointLocation",f'{checkpoint}/rawTrafficLoad/checkpoint').outputMode("append").queryName("rawTrafficWriteStream").trigger(availableNow=True).toTable(f'`{env}_Catalog`.`bronze`.`raw_traffic`')

    writeStream.awaitTermination()
    print("Succeeded writing the traffic data")
    print("---------------------------------------------------------------")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating a function to read the raw_roads data from the external location

# COMMAND ----------

def readRoadsData():
    print("Reading the Roads Data:",end="\n")
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    schema2 = StructType([
        StructField('Road_ID',IntegerType()),
        StructField('Road_Category_Id',IntegerType()),
        StructField('Road_Category',StringType()),
        StructField('Region_ID',IntegerType()),
        StructField('Region_Name',StringType()),
        StructField('Total_Link_Length_Km',DoubleType()),
        StructField('Total_Link_Length_Miles',DoubleType()),
        StructField('All_Motor_Vehicles',DoubleType())
    ])

    read_df1 = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemaLocation",f'{checkpoint}/rawRoadsLoad/schemaInfer').option("header",True).schema(schema2).load(landing+'/raw_roads/')

    print("Suceeded reading the traffic data")
    print("---------------------------------------------------------------")
    return read_df1

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating a function to write the raw_roads data into the bronze schema

# COMMAND ----------

def writeRawRoadsData(writedf1,env):
    print(f'Writing data to {env}_catalog raw_roads table', end='' )
    writeStream1 = (writedf1.writeStream.format("delta").option("checkPointLocation",f'{checkpoint}/rawRoadsLoad/checkpoint').outputMode('append').queryName('rawRoadsWriteStream').trigger(availableNow=True).toTable(f"`{env}_catalog`.`bronze`.`raw_roads`"))
    writeStream1.awaitTermination()
    print('Succeeded writing the Traffic data')
    print("-------------------------------------------------------------------")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Calling all the functions

# COMMAND ----------

read_df = readTrafficData()
writeTrafficData(read_df,env)
read_df1 = readRoadsData()
writeRawRoadsData(read_df1,env)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Displaying all the data in the bronze layer

# COMMAND ----------

spark.sql(f'select * from `{env}_Catalog`.`bronze`.`raw_traffic`').display()

# COMMAND ----------

spark.sql(f'select * from `{env}_Catalog`.`bronze`.`raw_roads`').display()