# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying.Json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Read Json file using the spark dataframe reader.API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType, DateType


# COMMAND ----------

qualifying_schema = StructType(fields = [
    StructField('qualifyId', IntegerType(), False),
    StructField('raceId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('constructorId', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('q1', StringType(), True),
    StructField('q2', StringType(), True),
    StructField('q3', StringType(), True)
])

# COMMAND ----------

qualifying_df =  spark.read\
.schema(qualifying_schema)\
.option('multiLine', True)\
.json('dbfs:/mnt/formula1dbp/raw/qualifying/qualifying_split*.json')

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

qualifying_df.count()

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

qualifying_final_df = qualifying_df.withColumn('ingestion_date', current_timestamp())\
.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')\
.withColumnRenamed('constructorId', 'contsructor_id')\
.withColumnRenamed('qualifyId', 'qualify_id')

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Write data to datalake as parquet

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet("/mnt/formula1dbp/processed/qualifying")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dbp/processed/qualifying

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dbp/processed/qualifying'))
