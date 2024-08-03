# Databricks notebook source
# MAGIC %run ../config/adls_access

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, FloatType, DoubleType
from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), True),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), False),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

results_raw_df = spark.read.json(f'{raw_path}/results.json', schema=results_schema)

# COMMAND ----------

results_df = results_raw_df \
    .withColumnRenamed("constructorId","constructor_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("fastestLap","fastest_lap") \
    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
    .withColumnRenamed("fastestLapTime","fastest_lap_time") \
    .withColumnRenamed("positionOrder","position_oder") \
    .withColumnRenamed("positionText","positio_text") \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("resultId","result_id") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("developer", lit(developer)) \
    .drop("statusId")

# COMMAND ----------

results_df.write.mode("overwrite").partitionBy('race_id').parquet(f'{processed_path}/results')

# COMMAND ----------

dbutils.notebook.exit("Completed")
