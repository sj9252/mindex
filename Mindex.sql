WITH img AS (
  SELECT 
    COUNT(*) FILTER (WHERE "result" = 'Win') AS Number_of_Wins,
    COUNT(*) FILTER (WHERE "result" = 'Loss') AS Number_of_Losses
  FROM shakti_jagadish
),
rd AS (
  SELECT 
    COALESCE(SUM(CASE WHEN boyd_yards <> '' AND boyd_yards ~ '^[-+]?[0-9]+(\.[0-9]+)?$' THEN boyd_yards::numeric ELSE 0 END), 0) AS "Boyd Yards",
    COALESCE(SUM(CASE WHEN higgins_yards <> '' AND higgins_yards ~ '^[-+]?[0-9]+(\.[0-9]+)?$' THEN higgins_yards::numeric ELSE 0 END), 0) AS "Higgins Yards",
    COALESCE(SUM(CASE WHEN chase_yards <> '' AND chase_yards ~ '^[-+]?[0-9]+(\.[0-9]+)?$' THEN chase_yards::numeric ELSE 0 END), 0) AS "Chase Yards"
  FROM shakti_jagadish
  WHERE boyd_yards <> '' OR higgins_yards <> '' OR chase_yards <> ''
)
SELECT 
  rd."Boyd Yards",
  rd."Higgins Yards",
  rd."Chase Yards",
  CONCAT(img.Number_of_Wins, '-', img.Number_of_Losses) AS "Win/Loss"
FROM img
CROSS JOIN rd;
