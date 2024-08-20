# Databricks notebook source
# MAGIC %run "../utility/variables"

# COMMAND ----------

dbutils.widgets.text('file_date_parameter','2024-08-17')
file_date = dbutils.widgets.get('file_date_parameter')

# COMMAND ----------

drivers_schema = "driver_id INT,driver_ref STRING,number INT,code STRING,forename STRING,surname STRING,dob STRING,nationality STRING"

# COMMAND ----------

drivers_df = spark.read \
    .format('csv') \
    .option('header',True) \
    .schema(drivers_schema) \
    .option('nullValue', '\\N') \
    .load(f'{raw_data_path}/{file_date}/drivers.csv')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,concat_ws,col,lit

# COMMAND ----------

drivers_timestamp_df = drivers_df.withColumn('name',concat_ws(' ',col('forename'),col('surname'))) \
                                  .withColumn('ingestion_date',current_timestamp())\
                                  .withColumn('file_date',lit(file_date))

# COMMAND ----------

drivers_final_df = drivers_timestamp_df.drop(col('forename'),col('surname'))

# COMMAND ----------

drivers_final_df.show()

# COMMAND ----------

drivers_final_df.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('f1_cleaned.drivers')
