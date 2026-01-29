
  create or replace   view MOVIELENS.DEV.src_genome_score
  
   as (
    WITH genome_scores AS(
    SELECT * FROM MOVIELENS.RAW.RAW_GENOME_SCORES
)
SELECT
    movieId AS movie_id,
    tagId AS tag_id,
    relevance
FROM genome_scores
  );

