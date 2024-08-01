# Databricks notebook source
# MAGIC %md ####Executing the common notebook to reuse shared functions and variables

# COMMAND ----------

# MAGIC %run "/Workspace/Users/user1@devineedisrilekhagmail.onmicrosoft.com/Project_Workspace/4. Common Notebook"

# COMMAND ----------

# MAGIC %md ####Take environment name as input from widget

# COMMAND ----------

dbutils.widgets.text(name="environment",defaultValue="",label="Enter the environment in lower case")
env = dbutils.widgets.get(name="environment")

# COMMAND ----------

# MAGIC %md ####Creating bronze, gold and silver schemas

# COMMAND ----------

def createBronzeSchema(environment,path):
    print(f'Using {environment}_Catalog')
    spark.sql(f"""Use catalog '{environment}_Catalog'""")
    print(f"Creating Bronze Schema in {environment}_Catalog")
    spark.sql(f"""create schema if not exists `bronze` managed location '{path}/bronze'""")
    print("---------------------------------------------------------------")

# COMMAND ----------

def createSilverSchema(environment,path):
    print(f'Using {environment}_Catalog')
    spark.sql(f"""Use Catalog '{environment}_Catalog'""")
    print(f"Creating Silver Schema in {environment}_Catalog")
    spark.sql(f"""create schema if not exists `silver` managed location '{path}/silver'""")
    print("---------------------------------------------------------------")

# COMMAND ----------

def createGoldSchema(environment,path):
    print(f'Using {environment}_Catalog')
    spark.sql(f"""Use Catalog '{environment}_Catalog'""")
    print(f"Creating Gold Schema in {environment}_Catalog")
    spark.sql(f"""create schema if not exists `gold` managed location '{path}/gold'""")
    print("---------------------------------------------------------------")

# COMMAND ----------

# MAGIC %md ####Creating raw_traffic table

# COMMAND ----------

def createTableRawTraffic(environment):
    print(f'Creating raw_traffic table in {environment}_catalog')
    spark.sql(f"""create table if not exists `{environment}_catalog`.`bronze`.`raw_traffic`
              (
                Record_ID INT,
                Count_point_id INT,
                Direction_of_travel VARCHAR(255),
                Year INT,
                Count_date VARCHAR(255),
                hour INT,
                Region_id INT,
                Region_name VARCHAR(255),
                Local_authority_name VARCHAR(255),
                Road_name VARCHAR(255),
                Road_Category_ID INT,
                Start_junction_road_name VARCHAR(255),
                End_junction_road_name VARCHAR(255),
                Latitude DOUBLE,
                Longitude DOUBLE,
                Link_length_km DOUBLE,
                Pedal_cycles INT,
                Two_wheeled_motor_vehicles INT,
                Cars_and_taxis INT,
                Buses_and_coaches INT,
                LGV_Type INT,
                HGV_Type INT,
                EV_Car INT,
                EV_Bike INT,
                Extract_Time TIMESTAMP
              );""")
    print("---------------------------------------------------------------")

# COMMAND ----------

# MAGIC %md ####Creating raw_roads table

# COMMAND ----------

def createTableRawRoad(environment):
    print(f'Creating table raw_road in {environment}_catalog')
    spark.sql(f"""create table if not exists `{environment}_catalog`.`bronze`.`raw_roads`(
        Road_ID INT,
        Road_Category_Id INT,
        Road_Category VARCHAR(255),
        Region_ID INT,
        Region_Name VARCHAR(255),
        Total_Link_Length_Km DOUBLE,
        Total_Link_Length_Miles DOUBLE,
        All_Motor_Vehicles DOUBLE
    );""")
    print("---------------------------------------------------------------")

# COMMAND ----------

# MAGIC %md ####Calling all functions

# COMMAND ----------

createBronzeSchema(env,bronze)
createSilverSchema(env,silver)
createGoldSchema(env,gold)
createTableRawTraffic(env)
createTableRawRoad(env)