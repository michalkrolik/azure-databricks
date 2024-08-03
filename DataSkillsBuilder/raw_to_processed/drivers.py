# Databricks notebook source
# MAGIC %run ../config/adls_access

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
from pyspark.sql.functions import current_timestamp, lit, concat, col

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), True),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", StructType(fields=[
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True)
    ])),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_raw_df = spark.read.json(f'{raw_path}/drivers.json', schema=drivers_schema)

# COMMAND ----------

drivers_df = drivers_raw_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("developer", lit(developer)) \
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
    .drop("url")

# COMMAND ----------

drivers_df.write.mode("overwrite").parquet(f'{processed_path}/drivers')

# COMMAND ----------

dbutils.notebook.exit("Completed")
