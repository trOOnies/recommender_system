SELECT
    index as id,
    cast(Date as datetime) as fecha_hora,
    user_id,
    movie_id,
    rating
FROM {{ source("public", "scores_raw") }}
