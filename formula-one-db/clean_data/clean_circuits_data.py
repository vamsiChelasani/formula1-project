# Databricks notebook source
from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

# MAGIC %run "../utility/variables"

# COMMAND ----------

dbutils.widgets.text('file_date_parameter','2024-08-17')
file_date = dbutils.widgets.get('file_date_parameter')

# COMMAND ----------

circuits_schema = "circuit_id int,circuit_ref string,name string,location string,country string,latitude double,longitude double,altitude int"

# COMMAND ----------

circuits_df = spark.read \
    .format('csv') \
    .option('header',True) \
    .schema(circuits_schema) \
    .option('nullValue', '\\N') \
    .load(f'{raw_data_path}/{file_date}/circuits.csv')

# COMMAND ----------

circuits_timestamp_df = circuits_df.withColumn('ingestion_date',current_timestamp()) \
                                   .withColumn('file_date',lit(file_date))

# COMMAND ----------

circuits_timestamp_df.show()

# COMMAND ----------

circuits_timestamp_df.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('f1_cleaned.circuits')
