SELECT *
FROM {{ ref('scores') }} sc 
INNER JOIN {{ ref('movies') }} using (movie_id)
INNER JOIN {{ ref('users') }} using (user_id)
