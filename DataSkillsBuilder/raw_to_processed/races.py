# Databricks notebook source
# MAGIC %run ../config/adls_access

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df_raw = spark.read.csv(f'{raw_path}/races.csv', header="True", schema=races_schema)

# COMMAND ----------

races_tech_df = races_df_raw \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("developer", lit(developer))

# COMMAND ----------

races_df = races_tech_df.select(\
    races_tech_df.raceId.alias("race_id"), \
    races_tech_df.year.alias("race_year"), \
    races_tech_df.round, \
    races_tech_df.circuitId.alias("circuit_id"), \
    races_tech_df.name, \
    races_tech_df.ingestion_date, \
    races_tech_df.race_timestamp, \
    races_tech_df.developer
)

# COMMAND ----------

races_df.write.mode("overwrite").partitionBy("race_year").parquet(f'{processed_path}/races')

# COMMAND ----------

dbutils.notebook.exit("Completed")
