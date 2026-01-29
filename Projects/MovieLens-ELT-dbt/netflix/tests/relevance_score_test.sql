-- SELECT
--     movie_id,
--     tag_id,
--     relevance_score
-- FROM {{ ref('fct_genome_scores') }}
-- WHERE relevance_score <= 0

-- use below when we used macros
{{ no_nulls_in_columns(ref('fct_genome_scores'))}}
