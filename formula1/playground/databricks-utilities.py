# Databricks notebook source
# MAGIC %fs
# MAGIC ls

# COMMAND ----------

for folder_name in dbutils.fs.ls('/'):
    print(folder_name)

# COMMAND ----------


