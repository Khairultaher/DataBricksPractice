-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE DEMO

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

USE demo

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rece_resluts_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rece_resluts_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python 

-- COMMAND ----------

SELECT * 
FROM demo.race_results_python
WHERE race_year = 2019

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT * 
FROM demo.race_results_python

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DROP TABLE race_results_python

-- COMMAND ----------

USE default

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DROP TABLE circuits_1_csv
