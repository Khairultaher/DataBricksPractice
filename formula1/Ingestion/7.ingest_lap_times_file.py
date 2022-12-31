# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times.Json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Read Json file using the spark dataframe reader.API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType, DateType


# COMMAND ----------

lap_times_schema = StructType(fields = [
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('lap', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

lap_times_df =  spark.read\
.schema(lap_times_schema)\
.csv('dbfs:/mnt/formula1dbp/raw/lap_times/lap_times_split*.csv')

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

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

lap_times_final_df = lap_times_df.withColumn('ingestion_date', current_timestamp())\
.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Write data to datalake as parquet

# COMMAND ----------

#lap_times_final_df.write.mode("overwrite").parquet("/mnt/formula1dbp/processed/lap_times")
lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dbp/processed/lap_times

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dbp/processed/lap_times'))
