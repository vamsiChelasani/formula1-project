# Databricks notebook source
# MAGIC %run "../utility/variables"

# COMMAND ----------

dbutils.widgets.text('file_date_parameter','2024-08-17')
file_date = dbutils.widgets.get('file_date_parameter')

# COMMAND ----------

constructors_schema = "constructorId int,constructorRef string,name string,nationality string,url string"

# COMMAND ----------

constructors_df = spark.read \
    .format('csv') \
    .option('header',True) \
    .schema(constructors_schema) \
    .option('nullValue', '\\N') \
    .load(f'{raw_data_path}/{file_date}/constructors.csv')

# COMMAND ----------

constructors_df.show()

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit

# COMMAND ----------

constructors_renamed_df = constructors_df.select(col('constructorId').alias('constructor_id'),col('constructorRef').alias('constructor_ref'),
                                    col('name'), col('nationality'))

# COMMAND ----------

constructors_final_df = constructors_renamed_df.withColumn('ingestion_time',current_timestamp()) \
                                               .withColumn('file_date',lit(file_date))

# COMMAND ----------

constructors_final_df.write.format('delta').mode('overwrite').saveAsTable('f1_cleaned.constructors')

# COMMAND ----------

constructors_final_df.show()

# COMMAND ----------


