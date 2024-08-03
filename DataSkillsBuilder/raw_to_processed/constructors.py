# Databricks notebook source
# MAGIC %run ../config/adls_access

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructors_schema = StructType(fields=[
    StructField("constructorId", IntegerType(), True),
    StructField("constructorRef", StringType(), False),
    StructField("name", StringType(), False),
    StructField("nationality", StringType(), False),
    StructField("url", StringType(), False)
])

# COMMAND ----------

constructors_raw_df = spark.read.json(f'{raw_path}/constructors.json', schema=constructors_schema)

# COMMAND ----------

constructors_df = constructors_raw_df \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("developer", lit(developer)) \
    .drop("url")

# COMMAND ----------

constructors_df.write.mode("overwrite").parquet(f'{processed_path}/constructors')

# COMMAND ----------

dbutils.notebook.exit("Completed")
