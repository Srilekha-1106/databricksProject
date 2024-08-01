# Databricks notebook source
# MAGIC %md
# MAGIC ####Extracting the external locations - Checkpoint, landing, bronze, silver, gold containers URL's

# COMMAND ----------

checkpoint = spark.sql("describe external location `checkpoints`").select("url").collect()[0][0]
landing = spark.sql("describe external location `landing`").select("url").collect()[0][0]
bronze = spark.sql("describe external location `bronze`").select("url").collect()[0][0]
silver = spark.sql("describe external location `silver`").select("url").collect()[0][0]
gold = spark.sql("describe external location `gold`").select("url").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ####Handling duplicate rows

# COMMAND ----------

def removeDuplicated(dp):
    dp_dup = dp.dropDuplicates()
    print("Removed duplicate values")
    return dp_dup

# COMMAND ----------

# MAGIC %md
# MAGIC ####Handling null values by replacing them with 0/Unknown

# COMMAND ----------

def handlingNullValues(dn,columns):
    print('Replacing NULL values on String Columns with "Unknown" ' , end='')
    dn1=dn.fillna('unknown',subset=columns)
    print('Replacing NULL values on Integer Columns with "0" ')
    dn2=dn1.fillna(0,subset=columns)
    return dn2