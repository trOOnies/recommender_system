SELECT
    sc.id as id,
    sc.user_id,
    sc.movie_id,
    sc.rating,
    sc.fecha_hora,
    -- (mv.year - us.year_birth) as year_diff,
    us.is_female as u_is_female,
    us.year_birth as u_year_birth,
    cast(
        {{ datediff("cast(concat(us.year_birth, '-1-1') as date)", "sc.fecha_hora", "year") }}
        as int
    ) as u_age,
    us.zip_code as u_zip_code,
    -- mv.year as m_year,
    mv.release_date as m_release_date,
    action as m_genre_action,
    adventure as m_genre_adventure,
    animation as m_genre_animation,
    childrens as m_genre_childrens,
    comedy as m_genre_comedy,
    crime as m_genre_crime,
    documentary as m_genre_documentary,
    drama as m_genre_drama,
    fantasy as m_genre_fantasy,
    film_noir as m_genre_film_noir,
    horror as m_genre_horror,
    musical as m_genre_musical,
    mystery as m_genre_mystery,
    romance as m_genre_romance,
    sci_fi as m_genre_sci_fi,
    thriller as m_genre_thriller,
    war as m_genre_war,
    western as m_genre_western
FROM {{ ref('scores') }} sc 
INNER JOIN {{ ref('users') }} us
    ON sc.user_id = us.id
INNER JOIN {{ ref('movies') }} mv
    ON sc.movie_id = mv.id
