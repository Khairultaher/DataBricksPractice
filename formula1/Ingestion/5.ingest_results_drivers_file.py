# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.Json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Read Json file using the spark dataframe reader.API

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
.json('dbfs:/mnt/formula1dbp/raw/results.json')

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
.withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')

# COMMAND ----------

display(result_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Write data to datalake as parquet

# COMMAND ----------

#result_final_df.write.mode("overwrite").parquet("/mnt/formula1dbp/processed/results")
result_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dbp/processed/results

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dbp/processed/results'))
