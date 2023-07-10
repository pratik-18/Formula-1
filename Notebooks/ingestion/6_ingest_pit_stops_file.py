# Databricks notebook source
dbutils.widgets.text("p_data_source", "", "Data Source")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pit_stops_schema = StructType(
    fields=[
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), True),
        StructField("stop", StringType(), True),
        StructField("lap", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("milliseconds", IntegerType(), True)
    ])

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option("multiLine", True) \
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

final_df = pit_stops_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#final_df.write.parquet(f"{processed_folder_path}/pit_stops", mode="overwrite")
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")


#### Incermental Load ####

#overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Delta Lake

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df, "f1_processed", "pit_stops", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

#display(spark.read.parquet("abfss://processed@pkformula1dl.dfs.core.windows.net/pit_stops"))

# COMMAND ----------

 dbutils.notebook.exit("Success")
