# Databricks notebook source
dbutils.widgets.text("p_data_source", "", "Data Source")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef String, name String, nationality String, url String "

# COMMAND ----------

constructor_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/constructors.json", schema=constructor_schema)

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

constructor_final_df = constructor_dropped_df \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('constructorRef', 'constructor_ref') \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

#constructor_final_df.write.parquet(f"{processed_folder_path}/constructors", mode='overwrite')
#constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors LIMIT 10

# COMMAND ----------

 dbutils.notebook.exit("Success")
