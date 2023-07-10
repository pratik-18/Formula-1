# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Window Functions

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import countDistinct, sum

# COMMAND ----------

demo_groupped_df = demo_df \
    .groupBy("race_year", "driver_name") \
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_groupped_df.withColumn("rank", rank().over(driverRankSpec)).show(100)

# COMMAND ----------


