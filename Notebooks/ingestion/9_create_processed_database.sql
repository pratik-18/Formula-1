-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed 
LOCATION "abfss://processed@pkformula1dl.dfs.core.windows.net/"

-- COMMAND ----------

DESC DATABASE f1_processed

-- COMMAND ----------

-- DROP SCHEMA f1_processed CASCADE

-- COMMAND ----------


