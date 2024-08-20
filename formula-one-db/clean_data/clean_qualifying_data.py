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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option('nullValue', '\\N') \
.csv(f"{raw_data_path}/{file_date}/qualifying.csv",header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename qualifyingId, driverId, constructorId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn('file_date',lit(file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the output to delta lake

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id"
merge_data_to_table(final_df, 'f1_cleaned.qualifying', merge_condition)

# COMMAND ----------

# final_df.createOrReplaceTempView('qualifying_tmp_tb')

# COMMAND ----------

# columnsList  = final_df.schema.names

# update_columns = ", ".join([f"tgt.{col} = src.{col}" for col in columnsList])
# insert_columns = ", ".join(columnsList)
# insert_values = ", ".join([f"src.{col}" for col in columnsList])

# spark.sql(f"""
# MERGE INTO f1_cleaned.qualifying tgt
# USING qualifying_tmp_tb src
# ON tgt.qualify_id = src.qualify_id
# WHEN MATCHED THEN
#     UPDATE SET {update_columns}
# WHEN NOT MATCHED THEN
#     INSERT ({insert_columns}) VALUES ({insert_values})
# """
# )

# COMMAND ----------


