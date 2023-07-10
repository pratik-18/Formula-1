-- Databricks notebook source
SELECT * FROM f1_presentation.calculated_race_results LIMIT 10

-- COMMAND ----------

SELECT
  team_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING total_races >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

 
