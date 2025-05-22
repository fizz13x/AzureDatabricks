# Databricks notebook source
# MAGIC %md
# MAGIC ###Access ADLS through Cluster Scoped Credntial

# COMMAND ----------

dbutils.fs.ls("abfss://demo@adbsa11.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@adbsa11.dfs.core.windows.net",header=True))

# COMMAND ----------

