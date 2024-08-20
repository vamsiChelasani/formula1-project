# Databricks notebook source
# MAGIC %run "../utility/variables"

# COMMAND ----------

# MAGIC %run "../utility/common_functions"

# COMMAND ----------

dbutils.widgets.text('file_date_parameter','2024-08-17')
file_date = dbutils.widgets.get('file_date_parameter')

# COMMAND ----------

races_schema = "race_id int,year int ,round int ,circuit_id int,name string,date string,time string"

# COMMAND ----------

races_df = spark.read \
    .format('csv') \
    .option('header',True) \
    .schema(races_schema) \
    .option('nullValue', '\\N') \
    .load(f'{raw_data_path}/{file_date}/races.csv')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,concat_ws,to_timestamp,lit

# COMMAND ----------

final_df = races_df.withColumn(
    'race_timestamp',
    to_timestamp(
        concat_ws(" ",col('date'), col('time')),
        'yyyy-MM-dd HH:mm:ss'
    )) \
.withColumn('ingestion_date',current_timestamp()) \
.withColumn('file_date',lit(file_date))

# COMMAND ----------

final_df.show()

# COMMAND ----------

final_df.write.mode('overwrite').format('delta').saveAsTable('f1_cleaned.races')
