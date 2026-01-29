
  create or replace   view MOVIELENS.DEV.src_movies
  
   as (
    WITH raw_movies AS(
  -- SELECT * FROM MOVIELENS.RAW.RAW_MOVIES
  SELECT * FROM MOVIELENS.raw.raw_movies
)
SELECT
    movieId AS movie_id,
    title,
    genres,
FROM raw_movies
  );

