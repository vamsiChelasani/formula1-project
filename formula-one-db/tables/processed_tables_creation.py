# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_processed.race_results
# MAGIC (
# MAGIC   race_id INTEGER,
# MAGIC   year INTEGER,
# MAGIC   race_name STRING,
# MAGIC   race_time TIMESTAMP,
# MAGIC   circuit_name STRING,
# MAGIC   circuit_location STRING,
# MAGIC   driver_name STRING,
# MAGIC   grid INTEGER,
# MAGIC   driver_nationality STRING,
# MAGIC   team_name STRING,
# MAGIC   position INTEGER,
# MAGIC   fastest_lap_time STRING,
# MAGIC   points FLOAT,
# MAGIC   ingestion_time TIMESTAMP,
# MAGIC   file_date STRING
# MAGIC )
