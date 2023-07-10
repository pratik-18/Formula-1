-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

DESC DATABASE f1_presentation

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE f1_processed

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC TABLE EXTENDED drivers

-- COMMAND ----------

SELECT * FROM drivers LIMIT 100

-- COMMAND ----------

SELECT 
  * 
FROM drivers 
WHERE 
  nationality = 'British'AND
  dob > '1990-01-01'

-- COMMAND ----------

SELECT 
  nationality, name, dob,
  RANK() OVER (PARTITION BY nationality, ORDER BY dob DESC) AS age_rank
FROM
  drivers
ORDER BY
  nationality, age_rank
