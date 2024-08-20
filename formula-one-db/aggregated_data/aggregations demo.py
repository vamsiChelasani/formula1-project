# Databricks notebook source
# MAGIC %run "../utility/variables"

# COMMAND ----------

race_results_df = spark.read.parquet(f'{processed_data_path}/race_results')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter('year = 2020')

# COMMAND ----------

from pyspark.sql.functions import col,countDistinct,sum

# COMMAND ----------

demo_df.groupBy('driver_name').sum('points').show()
# in this way only a single aggregation is possible, this is not valid incase of multiple aggregations

# COMMAND ----------

demo_df.groupBy('driver_name') \
       .agg(
            countDistinct("race_name").alias('total_races'),
            sum("points").alias('total_points')
            ) \
       .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### window functions

# COMMAND ----------

demo_df2 = race_results_df.filter('year in (2020,2019)')

# COMMAND ----------

grouped_df = demo_df2.groupBy('year','driver_name') \
                     .agg(
                            countDistinct("race_name").alias('total_races'),
                            sum("points").alias('total_points')
                         )

# COMMAND ----------

from pyspark.sql.functions import desc,rank
from pyspark.sql.window import Window

# COMMAND ----------

driverRank = Window.partitionBy('year').orderBy(desc('total_points'))
window_df = grouped_df.withColumn('rank',rank().over(driverRank))

# COMMAND ----------

display(window_df)

# COMMAND ----------

demo_df2.groupBy('year','driver_name') \
        .agg(sum('points').alias('total_points'))
