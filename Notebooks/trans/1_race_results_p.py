# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
    .select(col("race_id"), col("race_year"), col("name").alias("race_name"), col("race_timestamp").alias("race_date"), col("circuit_id") )

# COMMAND ----------

display(races_df)

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .select(col("circuit_id"), col("location").alias("circuit_location"))

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
    .select(col("driver_id"), col("name").alias("driver_name"), col("number").alias("driver_number"), col("nationality").alias("driver_nationality"))

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
    .select(col("constructor_id"), col("name").alias("team"))

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
    .select(col("race_id"), col("driver_id"), col("constructor_id"), col("grid"), col("fastest_lap"), col("time").alias("race_time"), col("points"))

# COMMAND ----------

results_df.columns

# COMMAND ----------

display(results_df)

# COMMAND ----------

result_races_df = results_df.join(races_df, races_df.race_id == results_df.race_id, "inner") \
    .drop(races_df.race_id)

# COMMAND ----------

result_races_circuit_df = result_races_df.join(circuits_df, circuits_df.circuit_id == result_races_df.circuit_id, "inner") \
    .drop(circuits_df.circuit_id, result_races_df.circuit_id) \
    .drop(col("race_id"))

# COMMAND ----------

result_races_circuit_driver_df = result_races_circuit_df.join(drivers_df, result_races_circuit_df.driver_id == drivers_df.driver_id, "inner") \
    .drop(result_races_circuit_df.driver_id, drivers_df.driver_id)

# COMMAND ----------

final_df = result_races_circuit_driver_df.join(constructors_df, constructors_df.constructor_id == result_races_circuit_driver_df.constructor_id, "inner") \
    .drop(constructors_df.constructor_id, result_races_circuit_driver_df.constructor_id)

# COMMAND ----------

final_df.columns

# COMMAND ----------

display(final_df.filter((col("race_year") == 2020) & (col("race_name") == "Abu Dhabi Grand Prix")))

# COMMAND ----------

display(final_df.count())

# COMMAND ----------


