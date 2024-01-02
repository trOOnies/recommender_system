import pickle
import numpy as np
import pandas as pd
from mlflow.pyfunc import PythonModel
from mlflow.models.signature import infer_signature
from typing import Tuple


class RS_baseline_usr_mov(PythonModel):
    def __init__(self, p: float) -> None:
        self.p = p

    def fit(self, X_train: pd.DataFrame, y_train: np.ndarray) -> None:
        df = pd.concat((X_train, pd.DataFrame(y_train, columns=["rating"])), axis=1)
        self.mean_usr = df.groupby("user_id").rating.mean()
        self.mean_mov = df.groupby("movie_id").rating.mean()
        self.mean = y_train.mean()
        self.sig = infer_signature(X_train[["user_id", "movie_id"]], self.predict(X_train))

    def get_usr_mov(self, row) -> Tuple[float, float]:
        if row.user_id in self.mean_usr:
            usr = self.mean_usr[row.user_id]
        else:
            usr = self.mean
        if row.movie_id in self.mean_mov:
            mov = self.mean_mov[row.movie_id]
        else:
            mov = self.mean
        return usr, mov

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        # usr, mov
        scores = np.array(
            [self.get_usr_mov(row) for _, row in X[["user_id", "movie_id"]].iterrows()],
            dtype=float
        )
        assert scores.shape == (X.shape[0], 2), f"Tamano de scores erroneo: {scores.shape} vs {(X.shape[0], 2)}"
        return self.p * scores[:, 0] + (1.0 - self.p) * scores[:, 1]

    @staticmethod
    def get_metric(y: np.ndarray, y_pred: np.ndarray) -> float:
        return np.sqrt((y-y_pred)**2).mean()

    def score(self, X: pd.DataFrame, y: np.ndarray) -> float:
        y_pred = self.predict(X)
        return self.get_metric(y, y_pred)

    def save_model(self, filename: str) -> None:
        with open(filename, "wb") as f:
            pickle.dump([self.mean_usr, self.mean_mov, self.mean, self.p], f)

    @classmethod
    def load_model(cls, filename: str):
        with open(filename, "rb") as f:
            mean_usr, mean_mov, mean, p = pickle.load(f)
        model = RS_baseline_usr_mov(p)
        model.mean_usr = mean_usr
        model.mean_mov = mean_mov
        model.mean = mean
        model.p = p
        return model


class RS_baseline:
    def fit(self, X_train: pd.DataFrame, y_train: np.ndarray) -> None:
        df = pd.concat((X_train, pd.DataFrame(y_train, columns=["rating"])), axis=1)
        self.mean_usr = df.groupby("user_id").rating.mean()
        self.mean_mov = df.groupby("movie_id").rating.mean()
        self.mean = y_train.mean()

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        # usr, mov
        return np.fromiter((self.mean for _ in X), dtype=float)

    def score(self, X: pd.DataFrame, y: np.ndarray) -> float:
        y_pred = self.predict(X)
        return np.sqrt((y-y_pred)**2).mean()

    def save_model(self, filename: str) -> None:
        with open(filename, "wb") as f:
            pickle.dump(self.mean, f)

    @classmethod
    def load_model(cls, filename: str):
        with open(filename, "rb") as f:
            mean, p = pickle.load(f)
        model = RS_baseline(p)
        model.mean_usr = mean
        model.mean_mov = mean
        model.mean = mean
        model.p = p
        return model
