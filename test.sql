SELECT
  *,
  CASE
    WHEN id = winning_team_id THEN 'WIN'
    ELSE 'LOSE'
  END as redskins_result
FROM (
  SELECT 
    *,
    MAX_BY(id, value) OVER(PARTITION BY date) as winning_team_id,
    MAX_BY(value, value) OVER(PARTITION BY date) as winning_team_score
  FROM 
    nfl_df
)
WHERE
  id = '28'