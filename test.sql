SELECT
  *,
  CASE
    WHEN team = winning_team THEN 'WIN'
    ELSE 'LOSE'
  END as redskins_result
FROM (
  SELECT 
    *,
    MAX_BY(team, final_score) OVER(PARTITION BY game_date) as winning_team,
    MAX_BY(final_score, final_score) OVER(PARTITION BY game_date) as winning_team_score
  FROM (
    SELECT
      game_date,
      team,
      final as final_score
    FROM 
        nfl
  )
)
WHERE
  team = 'Washington'