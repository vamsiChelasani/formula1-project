-- Databricks notebook source
SELECT
    driver_name,
    COUNT(*) AS total_races,
    sum(adjusted_points) AS total_points,
    ROUND(AVG(adjusted_points),3) AS avg_points,
    RANK() OVER(ORDER BY AVG(adjusted_points) DESC) AS rank
FROM f1_processed.adjusted_race_points
GROUP BY driver_name
HAVING COUNT(*) > 50
ORDER BY avg_points DESC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW top10_drivers_v
AS
SELECT
    driver_name,
    COUNT(*) AS total_races,
    sum(adjusted_points) AS total_points,
    ROUND(AVG(adjusted_points),3) AS avg_points,
    RANK() OVER(ORDER BY AVG(adjusted_points) DESC) AS rank
FROM f1_processed.adjusted_race_points
GROUP BY driver_name
HAVING COUNT(*) > 50
ORDER BY avg_points DESC

-- COMMAND ----------

REFRESH TABLE f1_processed.adjusted_race_points;

-- COMMAND ----------

SELECT
    year,
    driver_name,
    COUNT(*) AS total_races,
    sum(adjusted_points) AS total_points,
    ROUND(AVG(adjusted_points),3) AS avg_points
FROM f1_processed.adjusted_race_points
WHERE driver_name IN (SELECT driver_name FROM top10_drivers_v WHERE rank <= 10)
GROUP BY year, driver_name
ORDER BY year, avg_points DESC
