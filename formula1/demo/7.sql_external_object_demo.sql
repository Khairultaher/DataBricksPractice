-- Databricks notebook source
-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rece_resluts_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rece_resluts_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_ext_py 

-- COMMAND ----------

SELECT * 
FROM demo.race_results_ext_py
WHERE race_year = 2019

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
race_year INT,
race_name STRING
)
USING parquet
LOCATION "/mnt/formula1dbp/presentation/demo.race_results_ext_sql"

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql VALUES(1, "Test")

-- COMMAND ----------

SELECT * FROM demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DROP TABLE circuits_1_csv
