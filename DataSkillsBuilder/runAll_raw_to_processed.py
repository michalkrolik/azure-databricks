# Databricks notebook source
jobs = [
    'circuits',
    'constructors',
    'drivers',
    'laptimes',
    'pitstops',
    'qualifying',
    'races',
    'results',
]

# COMMAND ----------

results = {}

# COMMAND ----------

for job in jobs:
    results[job] = 'Failed'
    result = dbutils.notebook.run(f'./raw_to_processed/{job}', 3600)
    results[job] = result

# COMMAND ----------

for k,v in results.items():
    print(f'{v}: {k}')

# COMMAND ----------


