-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dbp/processed"

-- COMMAND ----------

DESC  DATABASE EXTENDED f1_processed
