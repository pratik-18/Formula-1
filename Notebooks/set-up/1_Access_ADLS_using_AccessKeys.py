# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Storage using keys
# MAGIC 1. Set Spark confing fs.azure.account.key
# MAGIC 2. List file from demo cotainer
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get('formula1-scope', 'formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.pkformula1dl.dfs.core.windows.net",
    formula1dl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@pkformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@pkformula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


