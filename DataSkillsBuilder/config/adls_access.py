# Databricks notebook source
dsb_access_key = dbutils.secrets.get(scope="dsb-secrets", key='dsb-access-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.dataskillsbuilder.dfs.core.windows.net",
    dsb_access_key
)

# COMMAND ----------

raw_path = "abfss://raw@dataskillsbuilder.dfs.core.windows.net"
processed_path = "abfss://processed@dataskillsbuilder.dfs.core.windows.net"
presentation_path = "abfss://presentation@dataskillsbuilder.dfs.core.windows.net"


# COMMAND ----------

developer = 'Mike'

# COMMAND ----------


