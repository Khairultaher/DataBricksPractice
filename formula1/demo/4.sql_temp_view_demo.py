# Databricks notebook source
# MAGIC %md
# MAGIC ####Accessing dataframe using SQL

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_result_df.createOrReplaceTempView("v_race_results")
#race_result_df.createOrReplaceGlobalTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results

# COMMAND ----------

races_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = 2019")

# COMMAND ----------

display(races_2019_df)
