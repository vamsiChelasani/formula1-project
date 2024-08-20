# Databricks notebook source
# MAGIC %md
# MAGIC #### Mounting adls using service principle

# COMMAND ----------

application_id = dbutils.secrets.get(scope='formula1_scope',key='formula1-app-application-id')
directory_id = dbutils.secrets.get(scope='formula1_scope',key='formula1-app-directory-id')
service_credential = dbutils.secrets.get(scope='formula1_scope',key='formula1-app-service-credential')

# COMMAND ----------

def mount_container(storage_account_name, container_name):
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": application_id,
              "fs.azure.account.oauth2.client.secret": service_credential,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/formula1/{container_name}",
      extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_container('formula1datalakeacc', 'rawdata')
mount_container('formula1datalakeacc', 'processed')

# COMMAND ----------

mount_container('formula1datalakeacc', 'cleaned')
