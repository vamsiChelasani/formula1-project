# Databricks notebook source
dbutils.widgets.text('file_date_parameter','2024-08-17')
file_date_v = dbutils.widgets.get('file_date_parameter')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_processed.calculated_results
# MAGIC (
# MAGIC     year INT,
# MAGIC     driver_name STRING,
# MAGIC     driver_id INT,
# MAGIC     constructor_name STRING,
# MAGIC     race_id INT,
# MAGIC     position INT,
# MAGIC     POINTS INT,
# MAGIC     adjusted_points INT,
# MAGIC     created_date TIMESTAMP,
# MAGIC     updated_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

spark.sql(f"""

        CREATE OR REPLACE TEMP VIEW results_updated
        AS
        SELECT
            races.year,
            drivers.name as driver_name,
            drivers.driver_id as driver_id,
            constructors.name as constructor_name,
            races.race_id as race_id,
            results.position,
            results.points,
            11 - results.position as adjusted_points
        FROM f1_cleaned.results
        JOIN f1_cleaned.drivers ON (results.driver_id = drivers.driver_id)
        JOIN f1_cleaned.constructors ON (results.constructor_id = constructors.constructor_id)
        JOIN f1_cleaned.races ON (races.race_id = results.race_id)
        WHERE (results.position <= 10) AND (results.file_date = '{file_date_v}')

""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_processed.calculated_results tgt
# MAGIC USING results_updated src
# MAGIC ON tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET tgt.position = src.position,
# MAGIC                tgt.points = src.points,
# MAGIC                tgt.adjusted_points = src.adjusted_points,
# MAGIC                tgt.updated_date = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN
# MAGIC     INSERT (year,driver_name,driver_id,constructor_name, race_id, position, points,adjusted_points,created_date)
# MAGIC     VALUES (year,driver_name,driver_id,constructor_name, race_id, position, points,adjusted_points, CURRENT_TIMESTAMP)
