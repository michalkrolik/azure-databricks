# Databricks notebook source
# MAGIC %run ../config/adls_access

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

laptimes_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

laptimes_raw_df = spark.read.csv(f'{raw_path}/lap_times/lap_times_split*csv', schema=laptimes_schema)

# COMMAND ----------

laptimes_df = laptimes_raw_df \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("developer", lit(developer))

# COMMAND ----------

laptimes_df.write.mode("overwrite").parquet(f'{processed_path}/lap_times')

# COMMAND ----------

dbutils.notebook.exit("Completed")
