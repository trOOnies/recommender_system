import os
from dagster import Definitions, load_assets_from_modules, define_asset_job
from dagster_mlflow import mlflow_tracking
from recommender_system.assets import (
    raw,
    dbt_transform,
    splitting,
    tuning,
    training,
    predict
)

mlf_tuning = mlflow_tracking.configured({
    "experiment_name": "rs_tuning",
    "mlflow_tracking_uri": os.environ["MLFLOW_TRACKING_URI"],
    "env": {k: os.environ[k] for k in ["MLFLOW_ARTIFACTS_PATH"]},
})
mlf_training = mlflow_tracking.configured({
    "experiment_name": "rs_training",
    "mlflow_tracking_uri": os.environ["MLFLOW_TRACKING_URI"],
    "env": {k: os.environ[k] for k in ["MLFLOW_ARTIFACTS_PATH"]},
})

all_assets = load_assets_from_modules(
    [dbt_transform, splitting, tuning, training, predict]
)
for a in raw.ab_assets.values():
    all_assets += a

defs = Definitions(
    assets=all_assets,
    jobs=[
        define_asset_job("all_assets")
    ],
    resources={
        "dbt": dbt_transform.dbt_resource,
        "mlflow_tuning": mlf_tuning,
        "mlflow_training": mlf_training
    }
)
