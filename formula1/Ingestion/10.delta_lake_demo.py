# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1dbp/demo'

# COMMAND ----------

result_df = spark.read\
.option("inferSchema", True)\
.json("/mnt/formula1dbp/raw/2021-03-28/results.json")

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").save("/mnt/formula1dbp/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/formula1dbp/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

result_external_df = spark.read.format("delta").load("/mnt/formula1dbp/demo/results_external")

# COMMAND ----------

display(result_external_df)

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partition")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partition

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Update Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dbp/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(condition = "position <= 10",
  set = { "points": "21 - position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Delete from Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC --DELETE FROM people10m WHERE birthDate < '1955-01-01'
# MAGIC --DELETE FROM delta.`/tmp/delta/people-10m` WHERE birthDate < '1955-01-01'

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

#deltaTable = DeltaTable.forPath(spark, '/tmp/delta/people-10m')

# Declare the predicate by using a SQL-formatted string.
#deltaTable.delete("birthDate < '1955-01-01'")

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Upsert using merge to Delta Table

# COMMAND ----------

drivers_day1_df = spark.read\
.option("inferSchema", True)\
.json("/mnt/formula1dbp/raw/2021-03-28/drivers.json")\
.filter("driverId <= 10")\
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read\
.option("inferSchema", True)\
.json("/mnt/formula1dbp/raw/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 6 AND 15")\
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------


