# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### inner join

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits')

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,'inner').select(races_df.circuit_id)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### left outer join

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,'left').select(circuits_df.name, races_df.name)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### right outer join

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,'right').select(circuits_df.name, races_df.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### full outer join

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,'full').select(circuits_df.name, races_df.name)

# COMMAND ----------

display(races_circuits_df)
