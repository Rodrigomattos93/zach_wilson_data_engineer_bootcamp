--this query answers question 4

WITH users_and_device AS (
	SELECT * FROM user_devices_cumulated WHERE current_date_ = '2023-01-31'
),

	series AS (
	SELECT 
	* FROM GENERATE_SERIES('2023-01-01', '2023-01-31', INTERVAL '1 day') AS series_date
	),
	
	place_holder_ints AS (
	SELECT
	CASE
	WHEN device_activity_datelist @> ARRAY[DATE(series_date)]
	THEN CAST(POW(2,32-(current_date_-DATE(series_date))) AS BIGINT)
	ELSE 0 
	END AS placeholder_int_value,
	*
	FROM users_and_device CROSS JOIN series
	)

	SELECT 
	user_id,
	device_id,
	CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)),
	BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_montly_active
	FROM place_holder_ints
	GROUP BY user_id, device_id
