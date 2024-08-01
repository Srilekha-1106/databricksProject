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
# MAGIC ####Reading the raw_traffic data from bronze layer

# COMMAND ----------

def readBronzeData(environment):
    df = spark.readStream.table(f"`{environment}_Catalog`.`bronze`.raw_traffic")
    print(f"Successfully reading `{environment}_Catalog`.`bronze`.raw_traffic")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating a new column to count electric vehicles

# COMMAND ----------

def electricVehiclesCount(ev):
    from pyspark.sql.functions import col
    df_ev = ev.withColumn('Electric_Vehicles_Count',col('EV_Car')+col('EV_Bike'))
    print("Created_electric_vehicles_count column")
    return df_ev

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating a new column to count motor vehicles

# COMMAND ----------

def motorVehiclesCount(dm):
    from pyspark.sql.functions import col
    df_motor = dm.withColumn('Motor_Vehicles_Count',col('Two_wheeled_motor_vehicles')+col('Cars_and_taxis')+col('Buses_and_coaches')+col('LGV_Type')+col('HGV_Type')+col('Electric_Vehicles_Count'))
    print("Created motor_vehicles_count column")
    return df_motor

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating a new column to get the timestamp

# COMMAND ----------

def timeStampColumn(dt):
    from pyspark.sql.functions import current_timestamp
    df_timestamp = dt.withColumn('Timestamp',current_timestamp())
    print("Created Timestamp column")
    return df_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing the transformed data to silver layer

# COMMAND ----------

def writeTrafficSilverTable(env,df):
    df.writeStream.format('delta').option('checkpointLocation',checkpoint+"/SilverTrafficRoad/checkpoint").outputMode('append').queryName('SilverTrafficWriteStream').trigger(availableNow=True).toTable(f"`{env}_Catalog`.`silver`.`silver_traffic`")
    print("Successfully written the traffic data into silver layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Invoking all functions, including those from the common notebook

# COMMAND ----------

df = readBronzeData(env)
dp = removeDuplicated(df)
allColumns = dp.schema.names
dn = handlingNullValues(dp,allColumns)
ev = electricVehiclesCount(dn)
dm = motorVehiclesCount(ev)
dt = timeStampColumn(dm)
writeTrafficSilverTable(env,dt)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Displaying the data transferred to the silver_traffic

# COMMAND ----------

spark.sql(f"select * from `{env}_Catalog`.`silver`.`silver_traffic` order by Record_ID").display()