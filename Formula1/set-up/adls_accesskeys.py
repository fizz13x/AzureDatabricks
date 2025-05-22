# Databricks notebook source
# MAGIC %md
# MAGIC ###Access adls through Access Keys

# COMMAND ----------

formula_access_key = dbutils.secrets.get(scope = "formula-1 Scope", key = "formulaAccessKey")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.adbsa11.dfs.core.windows.net",formula_access_key)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@adbsa11.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@adbsa11.dfs.core.windows.net",header=True))

# COMMAND ----------

