-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Drop tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_cleaned CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_cleaned
LOCATION "/mnt/formula1/cleaned/databricks"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1/processed/"
