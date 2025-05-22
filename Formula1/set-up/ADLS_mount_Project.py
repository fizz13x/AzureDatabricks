# Databricks notebook source
# MAGIC %md
# MAGIC ###Mount ADLS through Service Principle

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #Secrets from key Vault
    client_id = dbutils.secrets.get(scope="formula-1 Scope",key="clientidServicePrinciple")
    tenant_id = dbutils.secrets.get(scope="formula-1 Scope",key="tenantIDservicePrinciple")
    client_secret = dbutils.secrets.get(scope="formula-1 Scope",key="clientSecretServicePrinciple")

    #Set Sparks Configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": client_id,
               "fs.azure.account.oauth2.client.secret": client_secret,
               "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #Unmount if already mounted
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    #Mount storage account Container
    dbutils.fs.mount(source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
                     mount_point = f"/mnt/{storage_account_name}/{container_name}",
                     extra_configs = configs)
    display(dbutils.fs.mounts())


# COMMAND ----------

mount_adls("adbsa11","raw")
mount_adls("adbsa11","process")
mount_adls("adbsa11","presentation")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#dbutils.fs.unmount("/mnt/adbsa11/demo")
#display(dbutils.fs.mounts())