WITH t_0 as (
    SELECT
        id,
        strip(Name) as name,
        "Release date" as release_date,
        year("Release date") as year,
        substring("IMDB URL", 19) as imdb_url,
        ...  -- genres
    FROM {{ source("public", "movies_raw") }}
)
SELECT * FROM t_0
