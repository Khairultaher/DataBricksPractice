-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####creates circuit table from csv

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circutes

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circutes(
circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS(path "/mnt/formula1dbp/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circutes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####creates drivers table from json

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING json
OPTIONS(path "/mnt/formula1dbp/raw/drivers.json", multiLine true, header true);

-- COMMAND ----------

SELECT * FROM f1_raw.drivers
