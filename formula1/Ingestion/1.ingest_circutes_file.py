# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1: Read the CSV file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dbp/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), False),
                                     StructField("location", StringType(), False),
                                     StructField("country", StringType(), False),
                                     StructField("lat", DoubleType(), False),
                                     StructField("lng", DoubleType(), False),
                                     StructField("alt", IntegerType(), False),
                                     StructField("url", StringType(), False)
    
])

# COMMAND ----------

##.option("infoSchema", True)
circuits_df = spark.read\
.option("header", True)\
.schema(circuits_schema)\
.csv('dbfs:/mnt/formula1dbp/raw/circuits.csv')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Select only required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#circuits_select_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "lat", "lng", "alt")
#circuits_select_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.lat, circuits_df.lng, circuits_df.alt)
#circuits_select_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])
circuits_select_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("country").alias("race_contry"), col("location"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Rename the columns as required 

# COMMAND ----------

circuits_rename_df = circuits_select_df.withColumnRenamed("race_contry", "country")

# COMMAND ----------

display(circuits_rename_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Adding new column to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_rename_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("override").parquet("/mnt/formula1dbp/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dbp/processed/circuits

# COMMAND ----------

