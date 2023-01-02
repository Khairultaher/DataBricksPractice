# Databricks notebook source
dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')


# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_funtions"

# COMMAND ----------

rece_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")\
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

display(rece_result_df)

# COMMAND ----------

race_year_list = df_column_to_list(rece_result_df, 'race_year') 

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col


# COMMAND ----------

rece_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructor_standing_df = rece_result_df\
.groupBy("race_year", "team")\
.agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standing_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
overwrite_partition(final_df, "f1_presentation", "constructor_standings", "race_year")

# COMMAND ----------

#display(spark.read.parquet(f"{presentation_folder_path}/constructor_standings"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings

# COMMAND ----------

dbutils.notebook.exit('success')
