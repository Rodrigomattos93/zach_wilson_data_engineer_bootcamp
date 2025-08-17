--this query answers question 7 and 8

CREATE TABLE IF NOT EXISTS host_activity_reduced(
	current_date_ DATE,
	month_date DATE,
	host TEXT,
	hit_array INTEGER[],
	unique_visitors_array INTEGER[],
	PRIMARY KEY(current_date_, host)
);

INSERT INTO host_activity_reduced
WITH dedup AS(
	SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY user_id, device_id, event_time) AS dedup_wf
	FROM events
	),
	
	yesterday AS (
	SELECT * FROM host_activity_reduced WHERE current_date_ = '2023-01-30'
	),

	today AS (
	SELECT
		DATE(event_time) AS current_date_,
		DATE(DATE_TRUNC('month', DATE(event_time))) AS month_date,
		host,
		COUNT(1) AS hit_array,
		COUNT(DISTINCT user_id) AS unique_visitors_array
	FROM dedup
	WHERE DATE(event_time) = '2023-01-31' AND (dedup_wf = 1) AND (user_id IS NOT NULL) AND (host IS NOT NULL)
	GROUP BY 1,2,3		
) 

	SELECT 
	COALESCE(t.current_date_, DATE(y.current_date_ + INTERVAL '1 day')),
	COALESCE(t.month_date, DATE(DATE_TRUNC('month', y.current_date_ + INTERVAL '1 day'))) AS month_date,
	COALESCE(t.host, y.host) AS host,
	
	CASE WHEN y.hit_array IS NULL THEN ARRAY[t.hit_array]
		 WHEN t.hit_array IS NULL THEN y.hit_array || ARRAY[0]
	ELSE y.hit_array || ARRAY[t.hit_array]
	END AS hit_array,
	
	CASE WHEN y.unique_visitors_array IS NULL THEN ARRAY[t.unique_visitors_array]
		 WHEN t.unique_visitors_array IS NULL THEN y.unique_visitors_array || ARRAY[0]
	ELSE y.unique_visitors_array || ARRAY[t.unique_visitors_array]
	END AS unique_visitors_array
	FROM today t FULL OUTER JOIN yesterday y
	ON y.host = t.host