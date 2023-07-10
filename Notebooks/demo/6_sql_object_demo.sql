-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ##### Lesson Objectives
-- MAGIC 1. Spark SQL Documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. Show Command
-- MAGIC 5. Describe Command
-- MAGIC 6. Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

USE demo 

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Learning Objectives
-- MAGIC
-- MAGIC 1. Create managed tables using Python
-- MAGIC 2. Cretae managed tables using SQL
-- MAGIC 3. Create managed tables using table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT * FROM demo.race_results_python WHERE race_year=2020

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS 
SELECT * FROM demo.race_results_python WHERE race_year=2020

-- COMMAND ----------

DESCRIBE EXTENDED race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

#Dropping the table will also remove the files from DBFS 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Learning Objectives
-- MAGIC 1. Create external tables using Python
-- MAGIC 2. Cretae external tables using SQL
-- MAGIC 3. Effects of dropping external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write \
-- MAGIC     .format("parquet") \
-- MAGIC     .option("path", f"{presentation_folder_path}/race_results_ext_py") \
-- MAGIC     .saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
  race_year int,
  race_name string,
  race_date timestamp,
  circuit_location string,
  driver_name string,
  driver_number int,
  driver_nationality string,
  team string,
  grid int,
  fastest_lap int,
  race_time string,
  points float,
  position int,
  created_date timestamp
)
USING PARQUET
LOCATION "abfss://presentation@pkformula1dl.dfs.core.windows.net/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Views on Tables
-- MAGIC
-- MAGIC ##### Learning Objectives
-- MAGIC
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permanent View 

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results AS
SELECT 
  * 
  FROM demo.race_results_python
  WHERE race_year = 2018

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results AS
SELECT 
  * 
  FROM demo.race_results_python
  WHERE race_year = 2012

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results AS
SELECT 
  * 
  FROM demo.race_results_python
  WHERE race_year = 2000

-- COMMAND ----------

SHOW TABLES in demo

-- COMMAND ----------

SELECT * FROM pv_race_results

-- COMMAND ----------


