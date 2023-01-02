# Databricks notebook source
dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_funtions"

# COMMAND ----------

rece_result_list = spark.read.parquet(f"{presentation_folder_path}/race_results")\
.filter(f"file_date = '{v_file_date}'")\
.select("race_year").distinct().collect()

# COMMAND ----------

display(rece_result_list)

# COMMAND ----------

race_year_list = []
for race_year in rece_result_list:
    #print(race_rear)
    race_year_list.append(race_year.race_year)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

rece_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(rece_result_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

driver_standing_df = rece_result_df\
.groupBy("race_year", "driver_name", "driver_nationality", "team")\
.agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standing_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_presentation.driver_standings
# MAGIC --SELECT * FROM f1_presentation.driver_standings

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
overwrite_partition(final_df, "f1_presentation", "driver_standings", "race_year")

# COMMAND ----------

#display(spark.read.parquet(f"{presentation_folder_path}/driver_standings"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings

# COMMAND ----------

dbutils.notebook.exit('success')
