-- Databricks notebook source


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circuits
(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING CSV
OPTIONS (header 'true', path 'dbfs:/mnt/formula1/rawdata/raw/circuits.csv');

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races
(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "dbfs:/mnt/formula1/rawdata/raw/races.csv", header true, nullValue '\\N')

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors
(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING csv
OPTIONS(path "dbfs:/mnt/formula1/rawdata/raw/constructors.csv",header 'true')

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers
(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  forename STRING,
  surname STRING,
  dob DATE,
  nationality STRING,
  url STRING
)
USING csv
OPTIONS (path "dbfs:/mnt/formula1/rawdata/raw/drivers.csv",header 'true', nullValue '\\N')

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING csv
OPTIONS (
  path 'dbfs:/mnt/formula1/rawdata/raw/results.csv',
  header 'true',
  nullValue '\\N'
);

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops
(
  raceId INT,
  driverId INT,
  stop INT,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING csv
OPTIONS(path "dbfs:/mnt/formula1/rawdata/raw/pit_stops.csv", header true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "dbfs:/mnt/formula1/rawdata/raw/lap_times.csv", header 'true')

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying
(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING csv
OPTIONS (path "dbfs:/mnt/formula1/rawdata/raw/qualifying.csv", header true, nullValue '\\N')

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying
