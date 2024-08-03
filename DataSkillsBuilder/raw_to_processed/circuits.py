# Databricks notebook source
# MAGIC %run ../config/adls_access

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

df_circuit_raw = spark.read.csv(f'{raw_path}/circuits.csv', schema=schema, header=True)

# COMMAND ----------

df_circuit = df_circuit_raw \
    .withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("developer", lit(developer))


# COMMAND ----------

df_circuit.write.mode("overwrite").parquet(f'{processed_path}/circuits')

# COMMAND ----------

dbutils.notebook.exit("Completed")
