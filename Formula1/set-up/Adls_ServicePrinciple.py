# Databricks notebook source
# MAGIC %md
# MAGIC ###Access ADLS through Service Principle

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula-1 Scope",key="clientidServicePrinciple")
tenant_id = dbutils.secrets.get(scope="formula-1 Scope",key="tenantIDservicePrinciple")
client_secret = dbutils.secrets.get(scope="formula-1 Scope",key="clientSecretServicePrinciple")

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.adbsa11.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adbsa11.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adbsa11.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.adbsa11.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adbsa11.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@adbsa11.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@adbsa11.dfs.core.windows.net",header=True))

# COMMAND ----------

