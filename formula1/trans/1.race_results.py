# Databricks notebook source
dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_funtions"

# COMMAND ----------

# drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")\
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers")\
.withColumnRenamed("name", "driver_name")\
.withColumnRenamed("number", "driver_number")\
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

#display(drivers_df)

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")\
.withColumnRenamed("name", "team")

# COMMAND ----------

#display(constructors_df)

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

#display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")\
.withColumnRenamed("name", "race_name")\
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

#display(races_df)

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")\
.filter(f"file_date = '{v_file_date}'")\
.withColumnRenamed("time", "race_time")\
.withColumnRenamed("race_id", "result_race_id")\
.withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

#display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### join circuits to races

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### join result to all other dataframs

# COMMAND ----------

race_result_df = results_df.join(races_circuits_df, results_df.result_race_id == races_circuits_df.race_id)\
                            .join(drivers_df, drivers_df.driver_id == results_df.driver_id)\
                            .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id)

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, current_timestamp, to_timestamp

# COMMAND ----------

final_df = race_result_df\
.select("race_id","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time", "points", "position", "result_file_date")\
.withColumn("crated_date", current_timestamp())\
.withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_presentation.race_results

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
overwrite_partition(final_df, "f1_presentation", "race_results", "race_id")

# COMMAND ----------

#display(spark.read.parquet(f"{presentation_folder_path}/race_results"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results

# COMMAND ----------

dbutils.notebook.exit('success')
