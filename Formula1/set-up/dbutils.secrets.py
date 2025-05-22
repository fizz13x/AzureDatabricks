# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = "formula-1 Scope")

# COMMAND ----------

dbutils.secrets.get(scope = "formula-1 Scope", key = "formulaAccessKey")