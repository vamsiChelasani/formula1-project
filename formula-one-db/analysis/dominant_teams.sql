-- Databricks notebook source

SELECT
    constructor_name,
    COUNT(*) AS total_races,
    SUM(adjusted_points) AS total_points,
    ROUND(AVG(adjusted_points),3) AS avg_points,
    RANK() OVER(ORDER BY AVG(adjusted_points) DESC) AS rank
FROM f1_processed.adjusted_race_points
GROUP BY constructor_name
HAVING COUNT(*) > 100
ORDER BY avg_points DESC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW top10_teams_v
AS
SELECT
    constructor_name,
    COUNT(*) AS total_races,
    SUM(adjusted_points) AS total_points,
    ROUND(AVG(adjusted_points),3) AS avg_points,
    RANK() OVER(ORDER BY AVG(adjusted_points) DESC) AS rank
FROM f1_processed.adjusted_race_points
GROUP BY constructor_name
HAVING COUNT(*) > 100

-- COMMAND ----------

SELECT
    year,
    constructor_name,
    COUNT(*) AS total_races,
    SUM(adjusted_points) AS total_points,
    ROUND(AVG(adjusted_points),3) AS avg_points
FROM f1_processed.adjusted_race_points
WHERE constructor_name in (SELECT constructor_name from top10_teams_v WHERE rank <= 10)
GROUP BY year, constructor_name
ORDER BY year, avg_points DESC

-- COMMAND ----------

SELECT
    year,
    constructor_name,
    COUNT(*) AS total_races,
    SUM(adjusted_points) AS total_points,
    ROUND(AVG(adjusted_points),3) AS avg_points
FROM f1_processed.adjusted_race_points
WHERE constructor_name in (SELECT constructor_name from top10_teams_v WHERE rank <= 5)
GROUP BY year, constructor_name
ORDER BY year, avg_points DESC

-- COMMAND ----------

SELECT
    year,
    constructor_name,
    COUNT(*) AS total_races,
    SUM(adjusted_points) AS total_points,
    ROUND(AVG(adjusted_points),3) AS avg_points
FROM f1_processed.adjusted_race_points
WHERE constructor_name in (SELECT constructor_name from top10_teams_v WHERE rank <= 5)
GROUP BY year, constructor_name
