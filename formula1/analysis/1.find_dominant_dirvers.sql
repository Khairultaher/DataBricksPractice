-- Databricks notebook source
SELECT 
      driver_name,
      COUNT(1) as total_races,
      SUM(calculated_points) as total_points,
      AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_resluts
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT 
      driver_name,
      COUNT(1) as total_races,
      SUM(calculated_points) as total_points,
      AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_resluts
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC
