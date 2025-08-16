-- this query answers question 1
WITH deduplicated AS (
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) AS dedup_wf	
	FROM game_details
)

SELECT * FROM deduplicated WHERE dedup_wf = 1
