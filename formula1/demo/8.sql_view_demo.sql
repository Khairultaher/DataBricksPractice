-- Databricks notebook source
-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

USE demo

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tv_reace_results
AS
SELECT *
FROM demo.race_results_sql
WHERE race_year = 2018

-- COMMAND ----------

SELECT * FROM tv_reace_results

-- COMMAND ----------



-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_reace_results
AS
SELECT *
FROM demo.race_results_sql
WHERE race_year = 2018

-- COMMAND ----------

SELECT * FROM global_temp.gv_reace_results

-- COMMAND ----------

SHOW TABLES IN global_temp

-- COMMAND ----------

CREATE OR REPLACE VIEW v_reace_results
AS
SELECT *
FROM demo.race_results_sql
WHERE race_year = 2019

-- COMMAND ----------

SELECT * FROM v_reace_results
