-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_resluts
USING parquet 
AS
SELECT
      races.race_year,
      constructors.name as team_name,
      drivers.name as driver_name,
      results.position,
      results.points,
      11 - results.position as calculated_points
FROM results
JOIN drivers ON drivers.driver_id = results.driver_id
JOIN constructors ON constructors.constructor_id = results.constructor_id
JOIN races ON races.race_id = results.race_id
WHERE results.position <= 10

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_resluts
