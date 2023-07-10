# Databricks notebook source
dbutils.widgets.text("p_data_source", "", "Data Source")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp, to_timestamp, lit, concat

# COMMAND ----------

races_schema = StructType(fields = [
    StructField("raceID", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

races_selected_df = races_df.drop('url')

# COMMAND ----------

races_renamed_df = races_selected_df \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('year', 'race_year') \
    .withColumnRenamed('circuitId', 'circuit_id')

# COMMAND ----------

races_final_df = races_renamed_df \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("ingestion_date", current_timestamp()) \
    .drop(col('date'), col('time')) \
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#races_final_df.write.partitionBy('race_year').parquet(f"{processed_folder_path}/races", mode='overwrite')
#races_final_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races LIMIT 10

# COMMAND ----------

 dbutils.notebook.exit("Success")
