# Databricks notebook source
# MAGIC %md
# MAGIC ### Global Temporary Views
# MAGIC ##### Objectives
# MAGIC
# MAGIC 1. Create global temporary view on Dataframe
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from python cell
# MAGIC 3. Access the view from another cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

#race_results_df.createTempView("v_race_results")
race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM global_temp.gv_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql("SELECT * FROM global_temp.gv_race_results")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------


