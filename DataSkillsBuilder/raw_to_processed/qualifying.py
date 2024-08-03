# Databricks notebook source
# MAGIC %run ../config/adls_access

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_raw_df = spark.read.json(f'{raw_path}/qualifying/qualifying_split_*.json',schema=qualifying_schema , multiLine=True)

# COMMAND ----------

qualifying_df = qualifying_raw_df \
    .withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("developer", lit(developer))

# COMMAND ----------

qualifying_df.write.mode("overwrite").parquet(f'{processed_path}/qualifying')

# COMMAND ----------

dbutils.notebook.exit("Completed")
