-- assignment task 5

CREATE TYPE scd_type AS (
	quality_class quality_class,
	is_active BOOLEAN,
	start_date INTEGER,
	end_date INTEGER
);

WITH last_year_scd AS (
	SELECT * FROM actors_history_scd WHERE current_year = 2020 and end_date = 2020
),
with_historical_scd AS (
	SELECT actor, quality_class, is_active, start_date, end_date
	FROM actors_history_scd WHERE current_year = 2020 and end_date < 2020
	),
with_current_year_scd AS (
	SELECT * FROM actors WHERE current_year = 2021
	),
with_unchanged AS (
	SELECT ly.actor, ly.quality_class, ly.is_active, ly.start_date, cy.current_year AS end_date 
	FROM with_current_year_scd cy JOIN last_year_scd ly ON ly.actor = cy.actor
	WHERE (ly.is_active = cy.is_active) AND (ly.quality_class = cy.quality_class)
),
with_changed AS (
	SELECT ly.actor,
	ARRAY[ROW(ly.quality_class, ly.is_active, ly.start_date, ly.end_date)::scd_type,
		  ROW(cy.quality_class, cy.is_active, cy.current_year, cy.current_year)::scd_type] AS actor_array
	FROM with_current_year_scd cy JOIN last_year_scd ly ON ly.actor = cy.actor
	WHERE (ly.is_active <> cy.is_active) OR (ly.quality_class <> cy.quality_class)
),
with_unnested_changed AS (
	SELECT actor, (UNNEST(actor_array)::scd_type).* FROM with_changed
),
with_new_records AS (
	SELECT cy.actor, cy.quality_class, cy.is_active, cy.current_year AS start_date, cy.current_year AS end_date 
	FROM with_current_year_scd cy JOIN last_year_scd ly ON ly.actor = cy.actor
	WHERE ly.actor IS NULL
)

SELECT * FROM with_new_records
UNION ALL
SELECT * FROM with_unnested_changed
UNION ALL
SELECT * FROM with_unchanged
UNION ALL
SELECT * FROM with_historical_scd
