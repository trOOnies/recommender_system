from dagster import Definitions, load_assets_from_modules, define_asset_job

from recommender_system.assets import sources, transformed, training

all_assets = load_assets_from_modules([sources, transformed, training])

all_assets_job_config = {
    "resources": {
        "mlflow": {
            "config": {
                "experiment_name": "recommender_system"
            }
        }
    },
    "ops": {
        "movies": {
            "config": {
                "uri": 'https://raw.githubusercontent.com/mlops-itba/Datos-RS/main/data/peliculas_0.csv'
            }
        },
        "users": {
            "config": {
                "uri": "https://raw.githubusercontent.com/mlops-itba/Datos-RS/main/data/usuarios_0.csv"
            }
        },
        "scores": {
            "config": {
                "uri": 'https://raw.githubusercontent.com/mlops-itba/Datos-RS/main/data/scores_0.csv'
            }
        }
    }
}

defs = Definitions(
    assets=all_assets,
    jobs=[
        define_asset_job(
            "all_assets",
            config=all_assets_job_config
        )
    ]
)
