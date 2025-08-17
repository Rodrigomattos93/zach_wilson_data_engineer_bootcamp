--this query answers question 5 and 6

CREATE TABLE hosts_cumulated (
	host TEXT,
	host_activity_datelist DATE[],
	current_date_ DATE,
	PRIMARY KEY (host, current_date_)
);


INSERT INTO hosts_cumulated
WITH dedup AS(
	SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY host, event_time) AS dedup_wf
	FROM events),

	yesterday AS (
	SELECT
		host,
		host_activity_datelist,
		current_date_
	FROM hosts_cumulated
	WHERE current_date_ = '2022-12-31'
),

	today AS (
	SELECT
		host,
		DATE(event_time) AS event_time
	FROM dedup
	WHERE (DATE(event_time) = '2023-01-01') AND (dedup_wf = 1) AND (host IS NOT NULL)
	GROUP BY host, DATE(event_time)
)
	
	SELECT
	COALESCE(t.host, y.host) AS host,
	CASE
		WHEN t.event_time IS NULL THEN y.host_activity_datelist
		WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.event_time]
		ELSE ARRAY[t.event_time] || y.host_activity_datelist
		END AS host_activity_datelist,
	COALESCE(t.event_time, (current_date_ + INTERVAL '1 day')::DATE) AS current_date_
	FROM yesterday y FULL OUTER JOIN today t
	ON y.host = t.host

