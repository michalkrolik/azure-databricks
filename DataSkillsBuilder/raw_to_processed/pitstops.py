# Databricks notebook source
# MAGIC %run ../config/adls_access

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, DoubleType, FloatType
from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

pitstops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_raw_df = spark.read.json(f'{raw_path}/pit_stops.json', schema=pitstops_schema, multiLine=True)

# COMMAND ----------

pit_stops_df = pit_stops_raw_df \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("developer", lit(developer))

# COMMAND ----------

pit_stops_df.write.mode("overwrite").parquet(f'{processed_path}/pit_stops')

# COMMAND ----------

dbutils.notebook.exit("Completed")
