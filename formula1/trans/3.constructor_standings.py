# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

rece_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(rece_result_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col


# COMMAND ----------

constructor_standing_df = rece_result_df\
.groupBy("race_year", "team")\
.agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standing_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standing_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/constructor_standings"))
