# import os
from dagster import (
    Definitions, load_assets_from_modules, define_asset_job,
)
# from dagster_mlflow import mlflow_tracking
from recommender_system.assets import (
    raw,
    dbt_transform,
    # transformed,
    # staging,
    splitting,
    tuning,
    training
)

# dbt_assets = [dbt_transform.dbt_project_assets]
all_assets = load_assets_from_modules(
    [dbt_transform, splitting, tuning, training]
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
    resources={"dbt": dbt_transform.dbt_resource}
    # resources={"mlflow": mlflow}
)
