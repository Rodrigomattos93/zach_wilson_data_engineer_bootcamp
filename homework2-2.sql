-- A DDL for an user_devices_cumulated table that has:
--a device_activity_datelist which tracks a users active days by browser_type
--data type here should look similar to MAP<STRING, ARRAY[DATE]>
--or you could have browser_type as a column with multiple rows for each user 
--(either way works, just be consistent!)

CREATE TABLE user_devices_cumulated (
	user_id NUMERIC,
	device_id NUMERIC,
	browser_type TEXT,
	device_activity_datelist DATE[],
	current_date_ DATE,
	PRIMARY KEY (user_id, device_id, current_date_)
);

INSERT INTO user_devices_cumulated
WITH dedup AS(
	SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY user_id, device_id, event_time) AS dedup_wf
	FROM events),

	yesterday AS (
	SELECT
		user_id,
		device_id,
		device_activity_datelist,
		current_date_
	FROM user_devices_cumulated
	WHERE current_date_ = '2022-12-31'
),

	today AS (
	SELECT
		user_id,
		device_id,
		DATE(event_time) AS event_time
	FROM dedup
	WHERE (DATE(event_time) = '2023-01-01') AND (dedup_wf = 1) AND (user_id IS NOT NULL) AND (device_id IS NOT NULL)
	GROUP BY user_id, device_id, DATE(event_time)
),
	
	joined AS (
	SELECT
	COALESCE(t.user_id, y.user_id) AS user_id,
	COALESCE(t.device_id, y.device_id) AS device_id,
	CASE
		WHEN t.event_time IS NULL THEN y.device_activity_datelist
		WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.event_time]
		ELSE ARRAY[t.event_time] || y.device_activity_datelist
		END AS device_activity_datelist,
	COALESCE(t.event_time, (current_date_ + INTERVAL '1 day')::DATE) AS current_date_
	FROM yesterday y FULL OUTER JOIN today t
	ON y.user_id = t.user_id AND y.device_id = t.device_id
) 

SELECT
	j.user_id,
	j.device_id,
	d.browser_type,
	j.device_activity_datelist,
	j.current_date_
FROM joined j 
LEFT JOIN (SELECT DISTINCT device_id, browser_type FROM devices) d 
ON j.device_id = d.device_id
