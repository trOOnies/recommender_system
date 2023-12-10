import os
import numpy as np
import pandas as pd
# import pickle
from dagster import asset, AssetIn, Output
from dagster_mlflow import mlflow_tracking
from recommender_system.constants import OBJ_METRIC
from recommender_system.assets.code.metrics import calc_metrics
from p2_ml.model_src.baseline_models import RS_baseline_usr_mov

P_STEPS = 10

mlflow = mlflow_tracking.configured({
    "experiment_name": "recommender_system",
    "mlflow_tracking_uri": os.environ["MLFLOW_TRACKING_URI"],
    # "parent_run_id": "an_existing_mlflow_run_id",  # if want to run a nested run, provide parent_run_id
    "env": {k: os.environ[k] for k in ["MLFLOW_ARTIFACTS_PATH"]},  # env variables to pass to mlflow
    # "env_to_tag": ["DOCKER_IMAGE_TAG"],  # env variables you want to log as mlflow tags
    # "extra_tags": {"super": "experiment"},  # key-value tags to add to your experiment
})


@asset(
    ins={
        "X_train": AssetIn(),
        "X_val": AssetIn(),
        "y_train": AssetIn(),
        "y_val": AssetIn()
    },
    description="Mejor parametro de tuning",
    # resource_defs={"mlflow": mlflow_tracking},
    group_name="tuning"
)
def tuning_baseline_model_best_param(
    X_train: pd.DataFrame,
    X_val: pd.DataFrame,
    y_train: np.ndarray,
    y_val: np.ndarray
) -> Output[float]:
    metadata = {}

    all_perf = np.zeros(P_STEPS + 1, dtype=float)
    p_width = 1.0 / float(P_STEPS)
    ps = np.arange(0.0, 1.0 + p_width, p_width)

    for i, p in enumerate(ps):
        mlflow.start_run()

        mlflow.log_params({"p": p})
        model = RS_baseline_usr_mov(p=p)

        model.fit(X_train, y_train)

        y_pred = model.predict(X_val)
        md = calc_metrics(y_val, y_pred)
        all_perf[i] = md[OBJ_METRIC["name"]]
        mlflow.log_metrics(md)

        # path = os.path.join(MODELS_TUNING_FD, f"baseline__{int(p * 100)}pp.pkl")
        # with open(path, "wb") as f:
        #     pickle.dump(model, f)
        del model
        mlflow.end_run()

    if OBJ_METRIC["lower_is_better"]:
        best_ix = np.argmin(all_perf)
    else:
        best_ix = np.argmax(all_perf)
    best_val = ps[best_ix]

    metadata["P_STEPS"] = P_STEPS
    metadata["metric_name"] = OBJ_METRIC["name"]
    metadata["best_p"] = best_val
    metadata["best_perf"] = all_perf[best_ix]
    metadata["lower_is_better"] = OBJ_METRIC["lower_is_better"]

    return Output(best_val, metadata=metadata)
