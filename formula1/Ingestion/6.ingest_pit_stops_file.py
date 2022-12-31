# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.Json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Read Json file using the spark dataframe reader.API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType, DateType


# COMMAND ----------

pit_stops_schema = StructType(fields = [
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('stop', StringType(), True),
    StructField('lap', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('duration', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

pit_stops_df =  spark.read\
.schema(pit_stops_schema)\
.option('multiLine', True)\
.json('dbfs:/mnt/formula1dbp/raw/pit_stops.json')

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Drop unwanted columns

# COMMAND ----------

#from pyspark.sql.functions import col, lit, concat, current_timestamp, to_timestamp

# COMMAND ----------

#results_droped_df = results_df.drop(col('statusId'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Rename and add new column

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, current_timestamp, to_timestamp

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumn('ingestion_date', current_timestamp())\
.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')

# COMMAND ----------

display(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Write data to datalake as parquet

# COMMAND ----------

#pit_stops_final_df.write.mode("overwrite").parquet("/mnt/formula1dbp/processed/pit_stops")
pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dbp/processed/pit_stops

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dbp/processed/pit_stops'))
