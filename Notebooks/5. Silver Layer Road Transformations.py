# Databricks notebook source
# MAGIC %md
# MAGIC ####Reading the common notebook to access the variables and functions

# COMMAND ----------

# MAGIC %run "/Workspace/Users/user1@devineedisrilekhagmail.onmicrosoft.com/Project_Workspace/4. Common Notebook"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Taking the environment name as input from widget

# COMMAND ----------

dbutils.widgets.text(name="environment",defaultValue="",label="Enter the environment in lower case")
env = dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading the raw_roads data from bronze layer

# COMMAND ----------

def bronzeData(environment):
    df = spark.readStream.table(f"`{environment}_catalog`.`bronze`.raw_roads")
    print(f"Successfully read the {environment}_catalog.bronze.raw_roads")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating a new column - road_category

# COMMAND ----------

def road_Category(df):
    from pyspark.sql.functions import when,col
    dfc = df.withColumn("Road_Category_Name",
        when(col('Road_Category') == 'TA', 'Class A Trunk Road')
        .when(col('Road_Category') == 'TM', 'Class A Trunk Motor')
        .when(col('Road_Category') == 'PA','Class A Principal road')
        .when(col('Road_Category') == 'PM','Class A Principal Motorway')
        .when(col('Road_Category') == 'M','Class B road')
        .otherwise('NA')
          )
    print("Successfully created Road_Category_Name column")
    return dfc

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating an new column - road_type

# COMMAND ----------

def road_type(df):
    from pyspark.sql.functions import when,col
    dfr = df.withColumn('Road_Type',
            when(col('Road_Category_Name').contains('A'),'Major')
            .when(col('Road_Category_Name').contains('B'),'Minor')
            .otherwise('NA')
            )
    print("Successfully created Road Type Name column")
    return dfr

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing the transformed raw_roads data into silver layer

# COMMAND ----------

def writeRoadsSilverTable(df):
    writestream = df.writeStream.format('delta').option("checkpointLocation",f'{checkpoint}/SilverRoadsLoad/checkpoint').outputMode('append').trigger(availableNow=True).toTable(f"`{env}_Catalog`.`silver`.`silver_roads`")
    writestream.awaitTermination()
    print(f"Successfully written `{env}_Catalog`.`silver`.`silver_roads`")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Invoking all the functions

# COMMAND ----------

df_roads = bronzeData(env)
dr = removeDuplicated(df_roads)
allColumns = df_roads.schema.names
dn = handlingNullValues(dr,allColumns)
dfc = road_Category(dn)
dfr = road_type(dfc)
writeRoadsSilverTable(dfr)