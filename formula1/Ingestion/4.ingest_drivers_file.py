# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.Json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Read Json file using the spark dataframe reader.API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType


# COMMAND ----------

name_schema = StructType(fields = [
    StructField('forename', StringType(), True),
    StructField('surname', StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields = [
    StructField('driverId', IntegerType(), False),
    StructField('driverRef', StringType(), True),
    StructField('number', IntegerType(), True),
    StructField('code', StringType(), True),
    StructField('name', name_schema, True),
    StructField('dob', DateType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True),
])

# COMMAND ----------

drivers_df =  spark.read\
.schema(drivers_schema)\
.json('dbfs:/mnt/formula1dbp/raw/drivers.json')

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Drop unwanted cloumns

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, current_timestamp, to_timestamp

# COMMAND ----------

drivers_droped_df = drivers_df.drop(col('url'))

# COMMAND ----------

display(drivers_droped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Rename and add new column

# COMMAND ----------

driveders_with_columns_df = drivers_droped_df.withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))\
                                               .withColumn('ingestion_date', current_timestamp())\
                                               .withColumnRenamed('driverId', 'driver_id')\
                                               .withColumnRenamed('driverRef', 'driver_ref')

# COMMAND ----------

display(driveders_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Write data to datalake as parquet

# COMMAND ----------

#driveders_with_columns_df.write.mode("overwrite").parquet("/mnt/formula1dbp/processed/drivers")
driveders_with_columns_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dbp/processed/drivers

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dbp/processed/drivers'))
