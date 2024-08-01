-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Verifying the count of all tables in each layer

-- COMMAND ----------

select count(*) from `dev_catalog`.`bronze`.raw_traffic

-- COMMAND ----------

select count(*) from `dev_catalog`.`bronze`.raw_roads

-- COMMAND ----------

select count(*) from `dev_catalog`.`silver`.silver_traffic

-- COMMAND ----------

select count(*) from `dev_catalog`.`silver`.silver_roads

-- COMMAND ----------

select count(*) from `dev_catalog`.`gold`.gold_traffic

-- COMMAND ----------

select count(*) from `dev_catalog`.`gold`.gold_roads