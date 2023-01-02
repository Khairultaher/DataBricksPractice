# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.Json file (incremental load)

# COMMAND ----------

spark.read.json(f"/mnt/formula1dbp/raw/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId, COUNT(1)
# MAGIC FROM results_cutover
# MAGIC GROUP BY raceId
# MAGIC ORDER BY RaceId DESC

# COMMAND ----------

spark.read.json(f"/mnt/formula1dbp/raw/2021-04-18/results.json").createOrReplaceTempView("results_w2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId, COUNT(1)
# MAGIC FROM results_w2
# MAGIC GROUP BY raceId
# MAGIC ORDER BY RaceId DESC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Read Json file using the spark dataframe reader.API

# COMMAND ----------

dbutils.widgets.text('p_data_source', 'testing')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_funtions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType, DateType


# COMMAND ----------

results_schema = StructType(fields = [
    StructField('resultId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('raceId', IntegerType(), True),
    StructField('constructorId', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('grid', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('positionText', StringType(), True),
    StructField('positionOrder', IntegerType(), True),
    StructField('points', FloatType(), True),
    StructField('laps', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True),
    StructField('fastestLap', IntegerType(), True),
    StructField('rank', IntegerType(), True),
    StructField('fastestLapTime', StringType(), True),
    StructField('fastestLapSpeed', FloatType(), True),
    StructField('statusId', StringType(), True),
])

# COMMAND ----------

results_df =  spark.read\
.schema(results_schema)\
.json(f'dbfs:{row_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Drop unwanted columns

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, current_timestamp, to_timestamp

# COMMAND ----------

results_droped_df = results_df.drop(col('statusId'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Rename and add new column

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, current_timestamp, to_timestamp

# COMMAND ----------

result_final_df = results_droped_df.withColumn('ingestion_date', current_timestamp())\
.withColumnRenamed('resultId', 'result_id')\
.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')\
.withColumnRenamed('constructorId', 'constructor_id')\
.withColumnRenamed('positionText', 'position_text')\
.withColumnRenamed('positionOrder', 'position_order')\
.withColumnRenamed('fastestLap', 'fastest_lap')\
.withColumnRenamed('fastestLapTime', 'fastest_lap_time')\
.withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')\
.withColumn('data_source', lit(v_data_source))\
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(result_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Write data to datalake as parquet

# COMMAND ----------

# MAGIC %md
# MAGIC incremental load method 1

# COMMAND ----------

# for race_id_list in result_final_df.select("race_id").distinct().collect():
#     #print(race_id_list.race_id)
#     if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# result_final_df.write.mode("overwrite").parquet("/mnt/formula1dbp/processed/results")
# result_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC incremental load method 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE f1_processed.results

# COMMAND ----------

# output_df = re_arrange_partition_column(result_final_df, "race_id")

# COMMAND ----------

overwrite_partition(result_final_df, "f1_processed", "results", "race_id")

# COMMAND ----------

# spark.conf.set("spark.sql.source.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# result_final_df = result_final_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order", 
#                                          "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed",
#                                          "data_source", "file_date", "race_id")

# COMMAND ----------

# if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#      result_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#      result_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dbp/processed/results

# COMMAND ----------

#display(spark.read.parquet('/mnt/formula1dbp/processed/results'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

dbutils.notebook.exit('success')
