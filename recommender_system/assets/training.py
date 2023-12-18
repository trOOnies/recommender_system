import numpy as np
import pandas as pd
from typing import Dict
from dagster import asset, AssetIn, Output
from recommender_system.assets.code.metrics import calc_metrics
from p2_ml.model_src.baseline_models import RS_baseline_usr_mov


@asset(
    compute_kind="python",
    ins={
        "X_train_val": AssetIn(),
        "y_train_val": AssetIn(),
        "tuning_baseline_model_best_param": AssetIn()
    },
    description="Mejor modelo de training",
    required_resource_keys={"mlflow_training"},
    group_name="training"
)
def training_run_info(
    context,
    X_train_val: pd.DataFrame,
    y_train_val: np.ndarray,
    tuning_baseline_model_best_param: float
) -> Output[Dict[str, str]]:
    mlflow = context.resources.mlflow_training

    run = mlflow.active_run()
    mlflow.end_run()
    mlflow.delete_run(run.info.run_id)

    metadata = {}

    p = tuning_baseline_model_best_param

    with mlflow.start_run():
        mlflow.log_params({"p": p})

        model = RS_baseline_usr_mov(p=p)
        model.fit(X_train_val, y_train_val)

        y_pred = model.predict(X_train_val)
        md = calc_metrics(y_train_val, y_pred, prefix="train_val")
        mlflow.log_metrics(md)
        metadata.update(md)

        model_info = mlflow.pyfunc.log_model(
            "model",
            python_model=model,
            signature=model.sig
        )
        run = mlflow.active_run()

        run_info = {
            "model_uri": model_info.model_uri,
            "run_id": run.info.run_id
        }

    return Output(run_info, metadata=metadata)
