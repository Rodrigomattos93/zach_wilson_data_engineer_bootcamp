--answer to assignment tasks 1 and 2

CREATE TYPE films AS (
	year INTEGER,
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT	
);

CREATE TYPE quality_class AS ENUM(
	'star', 'good', 'average', 'bad'
);

CREATE TABLE actors (
	actor TEXT,
	quality_class quality_class,
	films films[],
	is_active BOOLEAN,
	current_year INTEGER,
	PRIMARY KEY(actor, current_year)
);

INSERT INTO actors

WITH yesterday as (
	SELECT * 
		   FROM actors WHERE current_year = 1969
),
	
	today as (
	SELECT 
		*,
	CASE
		WHEN AVG(rating) OVER(PARTITION BY actor, year) > 8 THEN 'star'
		WHEN AVG(rating) OVER(PARTITION BY actor, year) BETWEEN 7 AND 8 THEN 'good'
		WHEN AVG(rating) OVER(PARTITION BY actor, year) BETWEEN 6 AND 7 THEN 'average'
		WHEN AVG(rating) OVER(PARTITION BY actor, year) < 6 THEN 'bad'
		ELSE NULL::quality_class
	END AS quality_class		
	FROM actor_films WHERE year = 1970
	),

	today2 AS (
	SELECT 
		actor,
		quality_class,
		year,
		ARRAY_AGG(ARRAY[ROW(
			year,
			film,
			votes,
			rating,
			filmid
		)::films]) AS films	
	FROM today
	GROUP BY actor, quality_class, year
	)

SELECT 
	COALESCE(t.actor, y.actor) as actor,
	COALESCE(t.quality_class, y.quality_class) as quality_class,
	CASE WHEN y.films IS NULL THEN t.films		
		 WHEN t.films IS NOT NULL THEN y.films || t.films
		 ELSE y.films
	END AS films,	
	CASE WHEN t.actor IS NULL THEN FALSE ELSE TRUE END AS is_active,
	COALESCE(t.year, y.current_year+1) as current_year
	
	FROM yesterday y
	FULL OUTER JOIN today2 t
	ON y.actor = t.actor
