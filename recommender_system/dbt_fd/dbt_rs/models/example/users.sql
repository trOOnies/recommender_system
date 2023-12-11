SELECT
    cast("id" as int),
    "Full Name" as full_name,
    ("gender" = 'F') as is_female,
    cast("year of birth" as int) as year_birth,
    "Zip Code" as zip_code
FROM {{ source("recommender_system_raw", "users_raw") }}
