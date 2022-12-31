# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregation functions demo

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

demo_df = race_result_df.filter("race_year == 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()


# COMMAND ----------

demo_df.groupBy("driver_name").sum("points").show()

# COMMAND ----------

demo_df.groupBy("driver_name").agg(sum("points").alias("total_points"), countDistinct("race_name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### window funtion

# COMMAND ----------

demo_df = race_result_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("race_year", "driver_name").agg(sum("points").alias("total_points"), countDistinct("race_name"))

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window 
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driver_randk_spec = Window .partitionBy("race_year").orderBy(desc("total_points")) 
demo_grouped_df.withColumn("rank", rank().over(driver_randk_spec)).show(100)
