# Databricks notebook source
# MAGIC %md
# MAGIC ### The capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'dsb-secrets')

# COMMAND ----------

dbutils.secrets.get(scope = 'dsb-secrets', key = 'dsb-access-key')

# COMMAND ----------


