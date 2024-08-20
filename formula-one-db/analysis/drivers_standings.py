# Databricks notebook source
# MAGIC %run "../utility/variables"

# COMMAND ----------

# MAGIC %run "../utility/common_functions"

# COMMAND ----------

dbutils.widgets.text('file_date_parameter','2024-08-19')
file_date_v = dbutils.widgets.get('file_date_parameter')

# COMMAND ----------

race_results_df = spark.read \
    .format('delta') \
    .load(f'{processed_data_path}/race_results') \
    .filter(f"file_date = '{file_date_v}'")

# COMMAND ----------

from pyspark.sql.functions import countDistinct,sum,count,when,col

# COMMAND ----------

drivers_standings_df = race_results_df.groupBy('year','driver_name','driver_nationality','team_name') \
                                      .agg(sum('points').alias('total_points'),
                                           countDistinct('race_name').alias('nr_of_races'),
                                           count(when(col('position') == 1,True)).alias('wins'))


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc,lit

# COMMAND ----------

rankingWindow = Window.partitionBy('year').orderBy(desc('total_points'))
standings_df = drivers_standings_df.withColumn('position', rank().over(rankingWindow)) \
                                    .withColumn('file_date',lit(file_date_v))

# COMMAND ----------

display(standings_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_processed.drivers_standings 
# MAGIC (
# MAGIC     year INTEGER,
# MAGIC     driver_name STRING,
# MAGIC     driver_nationality STRING,
# MAGIC     team_name STRING,
# MAGIC     total_points DOUBLE,
# MAGIC     nr_of_races LONG,
# MAGIC     wins INTEGER,
# MAGIC     position INTEGER,
# MAGIC     file_date STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (year);

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name and tgt.year = src.year"
merge_data_to_table(standings_df, 'f1_processed.drivers_standings', merge_condition)
