# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Storage using keys Cluster Scoped Credentials
# MAGIC 1. Set Spark confing fs.azure.account.key in cluster
# MAGIC 2. List file from demo cotainer
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@pkformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@pkformula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


