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

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option('nullValue', '\\N') \
.csv(f"{raw_data_path}/{file_date}/pit_stops.csv",header=True)

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn('file_date',lit(file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the output to delta table

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id and tgt.driver_id = src.driver_id and tgt.stop = src.stop"
merge_data_to_table(final_df, 'f1_cleaned.pit_stops', merge_condition)

# COMMAND ----------

# final_df.createOrReplaceTempView('pit_stops_tmp_tb')

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS f1_cleaned.pit_stops
# (
#   race_id integer,
#   driver_id integer,
#   stop string,
#   lap integer,
#   time string,
#   duration string,
#   milliseconds integer,
#   ingestion_date timestamp,
#   file_date string
# )

# COMMAND ----------

# columnsList  = final_df.schema.names

# update_columns = ", ".join([f"tgt.{col} = src.{col}" for col in columnsList])
# insert_columns = ", ".join(columnsList)
# insert_values = ", ".join([f"src.{col}" for col in columnsList])

# spark.sql(f"""
# MERGE INTO f1_cleaned.pit_stops tgt
# USING pit_stops_tmp_tb src
# ON tgt.race_id = src.race_id and tgt.driver_id = src.driver_id and tgt.stop = src.stop
# WHEN MATCHED THEN
#     UPDATE SET {update_columns}
# WHEN NOT MATCHED THEN
#     INSERT ({insert_columns}) VALUES ({insert_values})
# """
# )

# COMMAND ----------

spark.conf.get("spark.sql.optimizer.dynamicPartitionPruning.enabled") 
