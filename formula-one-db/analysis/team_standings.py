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

from pyspark.sql.functions import col,count,sum,countDistinct,when

# COMMAND ----------

team_standings_df = race_results_df.groupBy('year','team_name') \
                                   .agg(sum('points').alias('total_points'),
                                        countDistinct('race_name').alias('total_races'),
                                        count(when(col('position')==1,True)).alias('wins'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc,lit

# COMMAND ----------

standingsRank = Window.partitionBy('year').orderBy(desc('total_points'))
standings_df = team_standings_df.withColumn('rankings',rank().over(standingsRank)) \
                                .withColumn('file_date',lit(file_date_v)) \
                                .orderBy(desc('year'),('rankings'))

# COMMAND ----------

merge_condition = "tgt.team_name = src.team_name and tgt.year = src.year"
merge_data_to_table(standings_df, 'f1_processed.teams_standings', merge_condition)

# COMMAND ----------

display(standings_df)
