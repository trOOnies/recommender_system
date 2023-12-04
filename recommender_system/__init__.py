from dagster import (
    Definitions, load_assets_from_modules, define_asset_job,
)
from recommender_system.assets import (
    raw, transformed, staging, splitting, tuning, training
)

all_assets = load_assets_from_modules(
    [transformed, staging, splitting, tuning, training]
)
for a in raw.ab_assets.values():
    all_assets += a

# all_assets_job_config = {
#     "resources": {
#         # "mlflow": {
#         #     "config": {
#         #         "experiment_name": "recommender_system"
#         #     }
#         # }
#     }
# }

defs = Definitions(
    assets=all_assets,
    jobs=[
        define_asset_job(
            "all_assets",
            # config=all_assets_job_config
        )
    ],
)
