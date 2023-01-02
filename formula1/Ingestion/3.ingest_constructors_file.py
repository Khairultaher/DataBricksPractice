# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.Json file

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

# MAGIC %md
# MAGIC #### Step 1: Read Json file using the spark dataframe reader.API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType


# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df =  spark.read\
.schema(constructors_schema)\
.json(f'dbfs:{row_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Drop unwanted cloumns

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, current_timestamp, to_timestamp

# COMMAND ----------

constructors_droped_df = constructors_df.drop(col('url'))

# COMMAND ----------

display(constructors_droped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Rename and add ingestion date

# COMMAND ----------

constructors_final_df = constructors_droped_df.withColumn('ingestion_date', current_timestamp())\
                                               .withColumnRenamed('constructorId', 'constructor_id')\
                                               .withColumnRenamed('constructorRef', 'constructor_ref')\
.withColumn('data_source', lit(v_data_source))\
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Write data to datalake as parquet

# COMMAND ----------

#constructors_final_df.write.mode("overwrite").parquet("/mnt/formula1dbp/processed/constructors")
constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dbp/processed/constructors

# COMMAND ----------

#display(spark.read.parquet('/mnt/formula1dbp/processed/constructors'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit('success')
