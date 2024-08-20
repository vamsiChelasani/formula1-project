# Databricks notebook source
# MAGIC %run "../utility/variables"

# COMMAND ----------

# MAGIC %run "../utility/common_functions"

# COMMAND ----------

dbutils.widgets.text('file_date_parameter','2024-08-17')
file_date = dbutils.widgets.get('file_date_parameter')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.option('nullValue', '\\N') \
.csv(f'{raw_data_path}/{file_date}/lap_times.csv',header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn('file_date',lit(file_date))

# COMMAND ----------

final_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the output to delta table

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id and tgt.driver_id = src.driver_id and tgt.lap = src.lap"
merge_data_to_table(final_df, 'f1_cleaned.lap_times', merge_condition)

# COMMAND ----------


