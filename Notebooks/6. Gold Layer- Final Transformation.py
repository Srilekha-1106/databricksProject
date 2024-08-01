# Databricks notebook source
# MAGIC %md
# MAGIC ####Executing the common notebook to reuse shared functions and variables

# COMMAND ----------

# MAGIC %run "/Workspace/Users/user1@devineedisrilekhagmail.onmicrosoft.com/Project_Workspace/4. Common Notebook"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Taking the environment name as input from the text widget

# COMMAND ----------

dbutils.widgets.text(name="environment",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading the traffic data from the silver layer

# COMMAND ----------

def readSilverTrafficTable(environment):
    dft = spark.readStream.table(f"`{environment}_Catalog`.`silver`.silver_traffic")
    print(f"Successfully read {environment}_Catalog`.`silver`.silver_traffic")
    return dft

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading the roads data from the silver layer

# COMMAND ----------

def readSilverRoadsTable(environment):
    dfr = spark.readStream.table(f"`{environment}_Catalog`.`silver`.`silver_roads`")
    print(f"Successfully read {environment}_Catalog`.`silver`.`silver_roads`")
    return dfr

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating a new column - Vehicle_Intensity

# COMMAND ----------

def createVehicleIntensity(df):
    from pyspark.sql.functions import col
    dfv = df.withColumn("Vehicle_Intensity",col('Motor_Vehicles_Count')/col('Link_Length_km'))
    print("Successfully created Vehicle Intensity Column")
    return dfv

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating a new column - Load_Time

# COMMAND ----------

def createLoadTimeColumn(df):
    from pyspark.sql.functions import current_timestamp
    dfl = df.withColumn("Load_Time",current_timestamp())
    print("Successfully created Load Time Column")
    return dfl

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing the transformed traffic data to the gold layer

# COMMAND ----------

def writeTrafficGoldTable(dft,environment):
    writestream = dft.writeStream.format('delta').option('checkpointLocation',f"{checkpoint}/GoldTrafficLoad/Checkpoint/").outputMode('append').queryName('GoldTrafficWriteStream').trigger(availableNow =True).toTable(f"`{environment}_Catalog`.`gold`.`gold_traffic`")
    writestream.awaitTermination()
    print(f"Successfully written `{environment}_Catalog`.`gold`.`gold_traffic`")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing the transformed roads data to the gold layer

# COMMAND ----------

def writeRoadsGoldTable(dft,environment):
    writestream = dft.writeStream.format('delta').option('checkpointLocation',f"{checkpoint}/GoldRoadsLoad/Checkpoint/").outputMode('append').queryName('GoldRoadsWriteStream').trigger(availableNow =True).toTable(f"`{environment}_Catalog`.`gold`.`gold_roads`")
    writestream.awaitTermination()
    print(f"Successfully written `{environment}_Catalog`.`gold`.`gold_roads`")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Invoking all the functions

# COMMAND ----------

silverTraffic = readSilverTrafficTable(env)
silverRoads = readSilverRoadsTable(env)
vehicleIntensity = createVehicleIntensity(silverTraffic)
df_finalTraffic = createLoadTimeColumn(vehicleIntensity)
df_finalRoads = createLoadTimeColumn(silverRoads)
writeTrafficGoldTable(df_finalTraffic,env)
writeRoadsGoldTable(df_finalRoads,env)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Displaying all the gold layer data

# COMMAND ----------

spark.sql(f"select * from `{env}_Catalog`.`gold`.`gold_traffic` order by Record_ID").display()

# COMMAND ----------

spark.sql(f"select * from `{env}_Catalog`.`gold`.`gold_roads` order by Record_ID").display()