import os
import pickle
import numpy as np
import pandas as pd
from dagster import asset, AssetIn, Output
from recommender_system.constants import MODELS_TRAINING_FD
from recommender_system.assets.code.metrics import calc_metrics
from p2_ml.model_src.baseline_models import RS_baseline_usr_mov


@asset(
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
    X_train_val: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train_val: np.ndarray,
    y_test: np.ndarray,
    tuning_baseline_model_best_param: float
) -> Output[RS_baseline_usr_mov]:
    metadata = {}

    p = tuning_baseline_model_best_param
    model = RS_baseline_usr_mov(p=p)
    model.fit(X_train_val, y_train_val)

    y_pred = model.predict(X_test)
    md = calc_metrics(y_test, y_pred)

    # path = os.path.join(MODELS_TRAINING_FD, f"baseline__{int(p * 100)}pp.pkl")
    # with open(path, "wb") as f:
    #     pickle.dump(model, f)

    metadata.update(md)

    return Output(model, metadata=metadata)
