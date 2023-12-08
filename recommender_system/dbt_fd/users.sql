SELECT
    id,
    "Full Name" as full_name,
    (gender = "F") as is_female,
    cast("year of birth" as int) as year_birth,
    "Zip Code" as zip_code
FROM {{ source("public", "users_raw") }}
