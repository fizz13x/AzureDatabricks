# Databricks notebook source
# MAGIC %md
# MAGIC ###Mount ADLS through Service Principle

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula-1 Scope",key="clientidServicePrinciple")
tenant_id = dbutils.secrets.get(scope="formula-1 Scope",key="tenantIDservicePrinciple")
client_secret = dbutils.secrets.get(scope="formula-1 Scope",key="clientSecretServicePrinciple")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@adbsa11.dfs.core.windows.net/",
  mount_point = "/mnt/adbsa11/demo",
  extra_configs = configs)

# COMMAND ----------

display(spark.read.csv("/mnt/adbsa11/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/adbsa11/demo")
display(dbutils.fs.mounts())