-- Databricks notebook source
CREATE TABLE IF NOT EXISTS f1_cleaned.lap_times
(
  race_id INTEGER,
  driver_id INTEGER,
  lap INTEGER,
  position INTEGER,
  time STRING,
  milliseconds INTEGER,
  ingestion_date TIMESTAMP,
  file_date STRING
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_cleaned.pit_stops
(
  race_id integer,
  driver_id integer,
  stop string,
  lap integer,
  time string,
  duration string,
  milliseconds integer,
  ingestion_date timestamp,
  file_date string
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_cleaned.qualifying
(
  qualify_id INTEGER,
  race_id INTEGER,
  driver_id INTEGER,
  constructor_id INTEGER,
  number INTEGER,
  position INTEGER,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  ingestion_date TIMESTAMP,
  file_date STRING
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_cleaned.results
(
  result_id integer,
  race_id integer,
  driver_id integer,
  constructor_id integer,
  number integer,
  grid integer,
  position integer,
  position_text string,
  position_order integer,
  points float,
  laps integer,
  time string,
  milliseconds integer,
  fastest_lap integer,
  rank integer,
  fastest_lap_time string,
  fastest_lap_speed float,
  ingestion_date timestamp,
  file_date string
)
