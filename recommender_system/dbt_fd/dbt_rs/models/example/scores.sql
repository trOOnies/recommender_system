SELECT
    cast(index as int) as id,
    cast("Date" as timestamp) as fecha_hora,
    cast(user_id as int),
    cast(movie_id as int),
    cast(rating as int)
FROM {{ source("recommmender_system_raw", "scores_raw") }}
