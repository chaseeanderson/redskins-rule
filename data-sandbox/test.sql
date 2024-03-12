SELECT
  *,
  CASE WHEN (pres_winning_party = pop_incumbent_party) THEN 'WIN'
  ELSE 'LOSE'
  END as pop_incumbent_elec_result
FROM (
  SELECT
    *,
    LAG(pres_winning_party, 1) OVER (ORDER BY elec_date) as incumbent_pres_party,
    LAG(pop_winning_party, 1) OVER (ORDER BY elec_date) as pop_incumbent_party
  FROM (
    SELECT
      elec_date,
      MAX_BY(political_party, electoral_votes) as pres_winning_party,
      MAX_BY(candidate, electoral_votes) as pres_winning_candidate,
      MAX_BY(electoral_votes, electoral_votes) as count_electoral_votes,
      MAX_BY(popular_votes, popular_votes) as count_popular_votes,
      pop_winning_candidate,
      pop_winning_party,
      electoral_rank_desc,
      popular_rank_desc,
      challenger_pres_party
    FROM (
      SELECT 
        foo.*,
        bar.challenger_pres_party,
        RANK() OVER (PARTITION BY foo.elec_date ORDER BY electoral_votes DESC) as electoral_rank_desc,
        RANK() OVER (PARTITION BY foo.elec_date ORDER BY popular_votes DESC) as popular_rank_desc
      FROM (
        SELECT
          elec_date,
          candidate,
          political_party,
          electoral_votes,
          popular_votes,
          MAX_BY(candidate, popular_votes) OVER (PARTITION BY elec_date) as pop_winning_candidate,
          MAX_BY(political_party, popular_votes) OVER (PARTITION BY elec_date) as pop_winning_party
        FROM
          elec_df
      ) foo
      LEFT JOIN (
        SELECT 
          elec_date,
          political_party as challenger_pres_party
        FROM (
          SELECT 
            *,
            RANK() OVER (PARTITION BY elec_date ORDER BY electoral_votes DESC) as electoral_rank_desc
          FROM (
            SELECT
              *,
              LAG(pres_winning_party, 1) OVER (ORDER BY elec_date) as prev_winning_party
            FROM (
              SELECT
              elec_date,
              political_party,
              electoral_votes,
              MAX_BY(political_party, electoral_votes) OVER(PARTITION BY elec_date) as pres_winning_party
            FROM 
              elec_df
            )
          )
          WHERE 
            political_party <> prev_winning_party
        )
        WHERE 
          electoral_rank_desc = 1
      ) bar
      ON foo.elec_date = bar.elec_date
    )
    WHERE 
      electoral_rank_desc = 1
    GROUP BY 
      elec_date,
      electoral_rank_desc,
      popular_rank_desc,
      pop_winning_candidate,
      pop_winning_party,
      challenger_pres_party
  )
)