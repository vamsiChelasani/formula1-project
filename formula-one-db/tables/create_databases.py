# Databricks notebook source
# MAGIC %run "../utility/variables/"

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE f1_processed CASCADE;
# MAGIC DROP DATABASE f1_cleaned CASCADE;

# COMMAND ----------

spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS f1_cleaned
            LOCATION '{cleaned_data_path}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc database f1_cleaned

# COMMAND ----------

spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS f1_processed
            LOCATION '{processed_data_path}'
""")
