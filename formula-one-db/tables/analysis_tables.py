# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_processed.teams_standings (
# MAGIC     year INTEGER,
# MAGIC     team_name STRING,
# MAGIC     total_points DOUBLE,
# MAGIC     total_races LONG,
# MAGIC     wins LONG,
# MAGIC     rankings INTEGER,
# MAGIC     file_date STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (year);

# COMMAND ----------


