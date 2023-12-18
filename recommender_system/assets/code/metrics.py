from math import sqrt
from typing import TYPE_CHECKING, Dict, Union, Optional
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
if TYPE_CHECKING:
    from numpy import ndarray


def calc_metrics(
    y_real: "ndarray",
    y_pred: "ndarray",
    prefix: Optional[str] = None
) -> Dict[str, Union[float, int]]:
    md = {
        "avg_score_real": y_real.mean(),
        "avg_score_pred": y_pred.mean(),
        "mae": mean_absolute_error(y_real, y_pred),
        "mse": mean_squared_error(y_real, y_pred),
        "r2": r2_score(y_real, y_pred),
        "n_obs": int(y_real.size)
    }
    md["avg_score_diff"] = md["avg_score_pred"] - md["avg_score_real"]
    md["rmse"] = sqrt(md["mse"])
    if prefix is not None:
        md = {f"{prefix}_{k}": v for k, v in md.items()}
    return md
