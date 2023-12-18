import numpy as np
import pandas as pd
from typing import Dict
from dagster import asset, AssetIn, Output
from recommender_system.assets.code.metrics import calc_metrics
from p2_ml.model_src.baseline_models import RS_baseline_usr_mov


@asset(
    compute_kind="python",
    ins={
        "training_run_info": AssetIn(),
        "X_test": AssetIn(),
        "y_test": AssetIn()
    },
    description="Mejor modelo de training",
    required_resource_keys={"mlflow_training"},
    group_name="training"
)
def trained_baseline_model(
    context,
    training_run_info: Dict[str, str],
    X_test: pd.DataFrame,
    y_test: np.ndarray
) -> Output[RS_baseline_usr_mov]:
    mlflow = context.resources.mlflow_training

    run = mlflow.active_run()
    mlflow.end_run()
    mlflow.delete_run(run.info.run_id)

    metadata = {}

    with mlflow.start_run(run_id=training_run_info["run_id"]):
        model = mlflow.pyfunc.load_model(model_uri=training_run_info["model_uri"])
        model = model.unwrap_python_model()

        y_pred = model.predict(X_test)
        md = calc_metrics(y_test, y_pred, prefix="test")
        mlflow.log_metrics(md)
        metadata.update(md)

    return Output(model, metadata=metadata)
