# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_funtions"

# COMMAND ----------

dbutils.widgets.text('p_data_source', 'testing')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Read CSV file using spark dataframe reader.API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), False),
                                     StructField("circuitId", IntegerType(), False),
                                     StructField("name", StringType(), False),
                                     StructField("date", DateType(), False),
                                     StructField("time", StringType(), False),
                                     StructField("url", StringType(), False)
    
])

# COMMAND ----------

races_df = spark.read\
.option("header", True)\
.schema(races_schema)\
.csv(f'dbfs:{row_folder_path}/{v_file_date}/races.csv')

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Add ingestion date and race_timestamp to the data frame

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, current_timestamp, to_timestamp

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss'))\
.withColumn('data_source', lit(v_data_source))\
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Select only required columns

# COMMAND ----------

races_selected_df = races_with_timestamp_df\
.select(col("raceId").alias('race_id'), col("year").alias('race_year'), col("round"), col('circuitId').alias('circuit_id'), col("name").alias("name"), col("time"), col("ingestion_date"), col("race_timestamp"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Write data to datalake as parquet

# COMMAND ----------

#races_selected_df.write.mode("overwrite").partitionBy('race_year').parquet("/mnt/formula1dbp/processed/races")
races_selected_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dbp/processed/races

# COMMAND ----------

#display(spark.read.parquet('/mnt/formula1dbp/processed/races'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races

# COMMAND ----------

dbutils.notebook.exit('success')
