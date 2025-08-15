-- A query to deduplicate game_details from Day 1 so there's no duplicates
WITH joined AS (
	SELECT 
		gd.game_id,
		game_date_est AS game_date,
		team_id, 
		player_id, 
		player_name, 
		start_position, 
		comment, 
		min,
		fgm,
		fga,
		fg_pct,
		fg3m,
		fg3a,
		fg3_pct,
		ftm,
		fta,
		ft_pct,
		oreb,
		dreb,
		reb,
		ast,
		stl,
		blk,
		"TO" AS turnovers,
		pf,
		pts,
		plus_minus
		FROM game_details gd JOIN games g ON gd.game_id = g.game_id
		WHERE game_date_est = (SELECT MIN(game_date_est) FROM games)
),
	deduplicated AS (
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) AS dedup_wf	
	FROM joined
)

SELECT * FROM deduplicated WHERE dedup_wf = 1
