# Databricks notebook source
# MAGIC %run "../utility/variables"

# COMMAND ----------

# MAGIC %run "../utility/common_functions"

# COMMAND ----------

dbutils.widgets.text('file_date_parameter','2024-08-19')
file_date_v = dbutils.widgets.get('file_date_parameter')

# COMMAND ----------

results_df = spark.read \
             .format('delta') \
             .load(f'{cleaned_data_path}/results') \
             .filter(f"file_date = '{file_date_v}'")

# COMMAND ----------

drivers_df = spark.read.format('delta').load(f'{cleaned_data_path}/drivers')

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed('name','driver_name') \
                               .withColumnRenamed('number','driver_number') \
                               .withColumnRenamed('nationality','driver_nationality')

# COMMAND ----------

constructors_df = spark.read.format('delta').load(f'{cleaned_data_path}/constructors')

# COMMAND ----------

constructors_renamed_df = constructors_df.withColumnRenamed('name','team_name') \
                                         .withColumnRenamed('nationality','team_nationality')

# COMMAND ----------

races_df = spark.read.format('delta').load(f'{cleaned_data_path}/races')

# COMMAND ----------

races_renamed_df = races_df.withColumnRenamed('name','race_name') \
                           .withColumnRenamed('race_timestamp','race_time')

# COMMAND ----------

circuits_df = spark.read.format('delta').load(f'{cleaned_data_path}/circuits')

# COMMAND ----------

circuit_renamed_df = circuits_df.withColumnRenamed('name','circuit_name') \
                                .withColumnRenamed('location','circuit_location')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join operations

# COMMAND ----------

races_circuits_df = races_renamed_df \
                        .join(circuit_renamed_df, circuit_renamed_df.circuit_id == races_renamed_df.circuit_id) \
                        .select(races_renamed_df.race_id,
                                races_renamed_df.year,
                                races_renamed_df.race_name,
                                races_renamed_df.race_time,
                                circuit_renamed_df.circuit_name,
                                circuit_renamed_df.circuit_location)

# COMMAND ----------

race_results_df = results_df.join(races_circuits_df,races_circuits_df.race_id == results_df.race_id) \
                            .join(constructors_renamed_df,constructors_renamed_df.constructor_id == results_df.constructor_id) \
                            .join(drivers_renamed_df,drivers_renamed_df.driver_id == results_df.driver_id)

# COMMAND ----------

final_df = race_results_df.select(results_df.race_id,'year','race_name','race_time','circuit_name',
                                  'circuit_location','driver_name','grid',
                                  'driver_nationality','team_name','position',
                                  'fastest_lap_time', 'points') \
                          .withColumn('ingestion_time',current_timestamp()) \
                          .withColumn('file_date',lit(file_date_v))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data to delta table

# COMMAND ----------

merge_condition = 'tgt.race_id = src.race_id and tgt.driver_name = src.driver_name'
merge_data_to_table(final_df,'f1_processed.race_results',merge_condition)

# COMMAND ----------

final_df.show()

# COMMAND ----------


