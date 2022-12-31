# Databricks notebook source
v_result = dbutils.notebook.run('1.ingest_circutes_file', 0, {'p_data_source': 'Ergast API'})

# COMMAND ----------

v_result
