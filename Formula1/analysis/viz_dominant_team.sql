-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_dominant_team
AS
SELECT 
  team_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points,
  RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT 
  race_year, 
  team_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE team_name IN (
  SELECT team_name 
  FROM v_dominant_team 
  WHERE team_rank <= 5
)
GROUP BY race_year, team_name
ORDER BY AVG(calculated_points) DESC;
