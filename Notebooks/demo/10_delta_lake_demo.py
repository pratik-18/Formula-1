# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed tables)
# MAGIC 2. Write data to delta lake (external tables)
# MAGIC 3. Read data from delta lake (table)
# MAGIC 4. Read data from delta lake (file)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "abfss://demo@pkformula1dl.dfs.core.windows.net/"

# COMMAND ----------

results_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/2021-03-28/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Python way to Create managed table

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC Python way to Save data as file & Create External Table

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save(f"{demo_folder_path}/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION  "abfss://demo@pkformula1dl.dfs.core.windows.net/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

# MAGIC %md
# MAGIC Same as before we can also read file directly in our Python Dataframe

# COMMAND ----------

results_external_df = spark.read.format("delta").load(f"{demo_folder_path}/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Partition the data while writing

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete from Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, f"{demo_folder_path}/results_managed")

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "position <= 10",
  set = { "points": "21 - position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC Deletes

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed WHERE position > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, f"{demo_folder_path}/results_managed")

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using Merge

# COMMAND ----------

drivers_day1_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/2021-03-28/drivers.json") \
    .filter("driverId <= 10") \
    .select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 6 AND 15") \
    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))    

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING, 
# MAGIC   surname STRING,
# MAGIC   createdDate DATE, 
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC Deltal is default

# COMMAND ----------

# MAGIC %md
# MAGIC Day 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Day 3

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, f"{demo_folder_path}/drivers_merge")

deltaTable.alias("tgt") \
    .merge(drivers_day3_df.alias("upd"),"tgt.driverId = upd.driverId") \
    .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
    .whenNotMatchedInsert(values =
        {
        "driverId": "upd.driverId",
        "dob": "upd.dob",
        "forename" : "upd.forename", 
        "surname" : "upd.surname", 
        "createdDate": "current_timestamp()"
        }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2023-06-18T08:26:26.000+0000'

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2023-06-18T08:26:26.000+0000').load(f"{demo_folder_path}/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2023-06-18T08:26:26.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2023-06-18T08:26:26.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Restoring to previous version

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge' WHERE driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 3

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 3 src
# MAGIC    ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC    INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------


