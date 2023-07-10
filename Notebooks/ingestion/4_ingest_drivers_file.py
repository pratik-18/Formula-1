# Databricks notebook source
dbutils.widgets.text("p_data_source", "", "Data Source")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

name_schema = StructType(
    fields=[
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True)
    ]
)

# COMMAND ----------

driver_schema = StructType(
    fields=[
        StructField("driverId", IntegerType(), False),
        StructField("driverRef", StringType(), True),
        StructField("number", IntegerType(), False),
        StructField("code", StringType(), True),
        StructField("name", name_schema),
        StructField("dob", DateType(), True),
        StructField("nationality", StringType(), True),
        StructField("url", StringType(), True)]
    )

# COMMAND ----------

drivers_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/drivers.json", schema=driver_schema)

# COMMAND ----------

drivers_with_column_df = drivers_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) 

# COMMAND ----------

drivers_final_df = drivers_with_column_df \
    .drop(col("url")) \
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#drivers_final_df.write.parquet(f"{processed_folder_path}/drivers", mode='overwrite')
#drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_processed.drivers LIMIT 10

# COMMAND ----------

 dbutils.notebook.exit("Success")
