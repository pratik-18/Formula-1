# Databricks notebook source
# MAGIC %md
# MAGIC ##### Explore the capabilities of dbutil.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

dbutils.secrets.get('formula1-scope', 'formula1dl-account-key')

# COMMAND ----------


