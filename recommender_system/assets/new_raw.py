from dagster import with_resources
from dagster_airbyte import airbyte_resource, build_airbyte_assets
from recommender_system.constants import AIRBYTE_CONNECTION_IDS

META_COLS = ["name", "year", "release_date", "imdb_url"]
GENRE_RAW_COLS = [
    'unknown', 'Action', 'Adventure', 'Animation',
    "Children's", 'Comedy', 'Crime', 'Documentary', 'Drama',
    'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery',
    'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'
]

airbyte_instance = airbyte_resource.configured(
    {
        "host": {"env": "AIRBYTE_HOST"},
        "port": {"env": "AIRBYTE_PORT"},
        "username": {"env": "AIRBYTE_USERNAME"},
        "password": {"env": "AIRBYTE_PASSWORD"},
    }
)

ab_assets = {
    k: with_resources(
        build_airbyte_assets(
            connection_id=conn_id,
            destination_tables=[k],
            group_name="raw"
        ),
        {"airbyte": airbyte_instance}
    )
    for k, conn_id in AIRBYTE_CONNECTION_IDS.items()
}

# @repository
# def repo():
#     return [ ab_assets ]
