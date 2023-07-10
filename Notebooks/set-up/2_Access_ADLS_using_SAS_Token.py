# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Storage using SAS (Shared Access Signatures) Token
# MAGIC 1. Set Spark confing SAS Token
# MAGIC 2. List file from demo cotainer
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.pkformula1dl.dfs.core.windows.net", "SAS")

spark.conf.set("fs.azure.sas.token.provider.type.pkformula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

spark.conf.set("fs.azure.sas.fixed.token.pkformula1dl.dfs.core.windows.net", "TOKEN")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@pkformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@pkformula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

print("Hello")

# COMMAND ----------


