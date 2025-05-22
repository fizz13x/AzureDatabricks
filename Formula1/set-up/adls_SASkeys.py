# Databricks notebook source
# MAGIC %md
# MAGIC ###Access ADLS through SAS Keys

# COMMAND ----------

formula_sas_token = dbutils.secrets.get(scope="formula-1 Scope",key="formula1SaStoken")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adbsa11.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.adbsa11.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.adbsa11.dfs.core.windows.net",formula_sas_token)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@adbsa11.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@adbsa11.dfs.core.windows.net",header=True))

# COMMAND ----------

