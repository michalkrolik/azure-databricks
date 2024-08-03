# Databricks notebook source
# MAGIC %run ../config/adls_access

# COMMAND ----------

from pyspark.sql.functions import to_date, current_timestamp, lit, col

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_path}/races') \
    .withColumnRenamed("name","race_name") \
    .withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_path}/circuits') \
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

drivers_df = spark.read.parquet(f'{processed_path}/drivers') \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.parquet(f'{processed_path}/constructors') \
    .withColumnRenamed("name", "team")

# COMMAND ----------

results_df = spark.read.parquet(f'{processed_path}/results') \
    .withColumnRenamed("time", "race_time")

# COMMAND ----------

races_results_df = races_df \
    .join(circuits_df, circuits_df.circuit_id == races_df.circuit_id) \
    .join(results_df, races_df.race_id == results_df.race_id) \
    .join(drivers_df, drivers_df.driver_id == results_df.driver_id) \
    .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id) \
    .select( \
        "race_year", \
        "race_name", \
        "race_date", \
        "circuit_location", \
        "driver_name", \
        "driver_number", \
        "driver_nationality",
        "team", \
        "grid", \
        "fastest_lap", \
        "race_time", \
        "points" \
        ) \
    .withColumn("created_date", current_timestamp())

# COMMAND ----------

#display(races_results.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(races_results.points.desc()))

# COMMAND ----------

#display(races_results.filter((races_results["race_year"] == 2020) & (races_results["race_name"] == 'Abu Dhabi Grand Prix')).orderBy(races_results["points"].desc()))

# COMMAND ----------

#display(races_results.filter(races_results["driver_name"] == 'Robert Kubica').orderBy(races_results["points"].desc()))

# COMMAND ----------

races_results_df.write.mode("overwrite").parquet(f'{presentation_path}/races_results')

# COMMAND ----------

# MAGIC %md
# MAGIC Testing...

# COMMAND ----------

#%run ../config/adls_access

# COMMAND ----------

#test_df = spark.read.parquet(f'{presentation_path}/races_results')

# COMMAND ----------

#from pyspark.sql.functions import count, countDistinct, sum, desc

# COMMAND ----------

#points = test_df.groupBy("race_year", "driver_name").sum("points")

# COMMAND ----------

#display(points.filter((points["driver_name"] == 'Robert Kubica') | (points["driver_name"] == 'Lewis Hamilton')).orderBy(desc("race_year"),"driver_name"))

# COMMAND ----------

#points_2 = test_df.filter(test_df["driver_name"] == 'Robert Kubica').groupBy("race_year", "driver_name").sum("points")

# COMMAND ----------

#display(points_2)

# COMMAND ----------


