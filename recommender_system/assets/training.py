import os
# import pickle
import numpy as np
import pandas as pd
from dagster import asset, AssetIn, Output
from dagster_mlflow import mlflow_tracking
# from recommender_system.constants import MODELS_TRAINING_FD
from recommender_system.assets.code.metrics import calc_metrics
from p2_ml.model_src.baseline_models import RS_baseline_usr_mov

mlf_cfg = mlflow_tracking.configured({
    "experiment_name": "rs_training",
    "mlflow_tracking_uri": os.environ["MLFLOW_TRACKING_URI"],
    # "parent_run_id": "an_existing_mlflow_run_id",  # if want to run a nested run, provide parent_run_id
    "env": {k: os.environ[k] for k in ["MLFLOW_ARTIFACTS_PATH"]},  # env variables to pass to mlflow
    # "env_to_tag": ["DOCKER_IMAGE_TAG"],  # env variables you want to log as mlflow tags
    # "extra_tags": {"super": "experiment"},  # key-value tags to add to your experiment
})


@asset(
    compute_kind="python",
    ins={
        "X_train_val": AssetIn(),
        "X_test": AssetIn(),
        "y_train_val": AssetIn(),
        "y_test": AssetIn(),
        "tuning_baseline_model_best_param": AssetIn()
    },
    description="Mejor modelo de training",
    group_name="training"
)
def trained_baseline_model(
    context,
    X_train_val: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train_val: np.ndarray,
    y_test: np.ndarray,
    tuning_baseline_model_best_param: float
) -> Output[RS_baseline_usr_mov]:
    run = context.resources.mlflow.active_run()
    context.resources.mlflow.end_run()
    context.resources.mlflow.delete_run(run.info.run_id)

    metadata = {}

    p = tuning_baseline_model_best_param

    with context.resources.mlflow.start_run():
        context.resources.mlflow.log_params({"p": p})

        model = RS_baseline_usr_mov(p=p)
        model.fit(X_train_val, y_train_val)

        y_pred = model.predict(X_test)
        md = calc_metrics(y_test, y_pred)

        context.resources.mlflow.log_metrics(md)

        # class MyModel(mlflow.pyfunc.PythonModel):
        #     def predict(self, context, model_input: List[str], params=None) -> List[str]:
        #         return [i.upper() for i in model_input]

        # with mlflow.start_run():
        #     model_info = mlflow.pyfunc.log_model(artifact_path="model", python_model=MyModel())

        # loaded_model = mlflow.pyfunc.load_model(model_uri=model_info.model_uri)
        # print(loaded_model.predict(["a", "b", "c"]))  # -> ["A", "B", "C"]


        # path = os.path.join(MODELS_TRAINING_FD, f"baseline__{int(p * 100)}pp.pkl")
        # with open(path, "wb") as f:
        #     pickle.dump(model, f)

        metadata.update(md)

    return Output(model, metadata=metadata)
