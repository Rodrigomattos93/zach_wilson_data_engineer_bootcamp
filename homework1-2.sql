-- answer to assignment task 3
CREATE TABLE actors_history_scd (
	actor TEXT,
	quality_class quality_class,
	is_active BOOLEAN,
	start_date INTEGER,
	end_date INTEGER	
);

INSERT INTO actors_history_scd
WITH CTE1 AS (
select 
	actor, 
	quality_class, 
	LAG(quality_class) OVER(PARTITION BY actor ORDER BY current_year) AS previous_quality_class,
	is_active, 
	LAG(is_active) OVER(PARTITION BY actor ORDER BY current_year) AS previous_is_active,
	current_year 
from actors),

	CTE2 AS (
SELECT 
	*,
	CASE WHEN 
	(previous_quality_class <> quality_class) OR (previous_is_active <> is_active) THEN 1
	ELSE 0 END AS change_indicator
	FROM CTE1),

	CTE3 AS (
SELECT 
actor, quality_class, is_active, current_year,
SUM(change_indicator) OVER(PARTITION BY actor ORDER BY current_year) AS streak_identifier
FROM CTE2
	),

	CTE4 AS (
SELECT
actor, quality_class, is_active,
MIN(current_year) AS start_date,
MAX(current_year) AS end_date
FROM CTE3
GROUP BY actor, streak_identifier, quality_class, is_active
	)

	SELECT * FROM CTE4
