# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")\
.withColumnRenamed("name", "driver_name")\
.withColumnRenamed("number", "driver_number")\
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")\
.withColumnRenamed("name", "team")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")\
.withColumnRenamed("name", "race_name")\
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")\
.withColumnRenamed("time", "race_time") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### join circuits to races

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### join result to all other dataframs

# COMMAND ----------

race_result_df = results_df.join(races_circuits_df, results_df.race_id == races_circuits_df.race_id)\
                            .join(drivers_df, drivers_df.driver_id == results_df.driver_id)\
                            .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, current_timestamp, to_timestamp

# COMMAND ----------

final_df = race_result_df\
.select("race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time", "points")\
.withColumn("crated_date", current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/race_results"))
