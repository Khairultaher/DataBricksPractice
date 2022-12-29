# Databricks notebook source
print('started...')

# COMMAND ----------

storage_account_name = 'formula1dbp '
client_id = '344090bc-2afa-4436-b5d3-0f56f0919f49'
tenant_id = '6af2cc5d-f999-4f33-a36d-a9c1acb7e9f9'
client_secret = 'e4a8Q~lAxTbUMQxHDWwKULt_zvbfsPrwfA2t2bMH'

# COMMAND ----------

configs = {
	"fs.azure.account.auth.type":"OAuth",
	"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
	"fs.azure.account.oauth2.client.id": f"{client_id}",
	"fs.azure.account.oauth2.client.secret": f"{client_secret}",
	"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

storage_account_name = 'formula1dbp '
container_name = "raw"
dbutils.fs.mount(
  source = f"wasbs://raw@formula1dbp.blob.core.windows.net/",
  mount_point = f"/mnt/raw",
  extra_configs = {'fs.azure.account.key.formula1dbp.blob.core.windows.net': 'G0cg94Fq6KCMS+fTuM9TO4fh/h6LSuZMxwLeWMmwJ0xC23UDwotJNq/gH7us7M8Ll7LbuHAh7hO/+AStOs5lFA=='}
)

# COMMAND ----------

dbutils.fs.ls('/mnt/raw')

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
      source = f"wasbs://{container_name}@formula1dbp.blob.core.windows.net/",
      mount_point = f"/mnt/formula1dbp/{container_name}",
      extra_configs = {'fs.azure.account.key.formula1dbp.blob.core.windows.net': 'G0cg94Fq6KCMS+fTuM9TO4fh/h6LSuZMxwLeWMmwJ0xC23UDwotJNq/gH7us7M8Ll7LbuHAh7hO/+AStOs5lFA=='}
    )

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('formula1dbp-scope')

# COMMAND ----------

dbutils.fs.unmount('/mnt/raw')

# COMMAND ----------

dbutils.fs.unmount('/mnt/processed')

# COMMAND ----------


