import os
import numpy as np
import pandas as pd
import pickle
from dagster import asset, AssetIn, Output
from recommender_system.constants import MODELS_TUNING_FD, OBJ_METRIC
from recommender_system.assets.code.metrics import calc_metrics
from p2_ml.model_src.baseline_models import RS_baseline_usr_mov

P_STEPS = 10


@asset(
    ins={
        "X_train": AssetIn(),
        "X_val": AssetIn(),
        "y_train": AssetIn(),
        "y_val": AssetIn()
    },
    group_name="tuning"
)
def tuning_result_baseline_model(
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
        model = RS_baseline_usr_mov(p=p)

        model.fit(X_train, y_train)

        y_pred = model.predict(X_val)
        md = calc_metrics(y_val, y_pred)
        all_perf[i] = md[OBJ_METRIC["name"]]

        # path = os.path.join(MODELS_TUNING_FD, f"baseline__{int(p * 100)}pp.pkl")
        # with open(path, "wb") as f:
        #     pickle.dump(model, f)
        del model

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
