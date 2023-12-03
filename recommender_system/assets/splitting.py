import pandas as pd
from typing import Tuple
from numpy import ndarray
from sklearn.model_selection import train_test_split
from dagster import multi_asset, AssetIn, AssetOut, Output
from recommender_system.constants import LABEL_COL, USER_COL, MOVIE_COL

STRATIFY_COL = "stratify_col"


@multi_asset(
    ins={
        "staged_data": AssetIn()
    },
    outs={
        "X_train": AssetOut(),
        "X_val": AssetOut(),
        "X_test": AssetOut(),
        "y_train": AssetOut(),
        "y_val": AssetOut(),
        "y_test": AssetOut(),
        "ord_train_val": AssetOut()
    },
    group_name="splitting"
)
def splitted_data(
    staged_data: pd.DataFrame
) -> Tuple[
    Output[pd.DataFrame], Output[pd.DataFrame], Output[pd.DataFrame],
    Output[ndarray], Output[ndarray], Output[ndarray], Output[ndarray]
]:
    staged_data = staged_data.sample(
        n=staged_data.shape[0],
        random_state=42,
        replace=False,
        ignore_index=True
    )

    y = staged_data[LABEL_COL]
    X = staged_data[[c for c in staged_data.columns if c != LABEL_COL]]
    X["stratify_col"] = X[[USER_COL, MOVIE_COL]].apply(
        lambda row: f"{row[USER_COL]}-{row[MOVIE_COL]}",
        axis=1
    )

    X_train_val, X_test, y_train_val, y_test = train_test_split(
        X,
        y,
        test_size=0.10,
        stratify=X.stratify_col,
        random_state=42
    )
    X_train, X_val, y_train, y_val = train_test_split(
        X_train_val,
        y_train_val,
        test_size=8/9,
        stratify=X.stratify_col,
        random_state=42
    )
    del X_train_val, y_train_val

    X_train = X_train.drop(STRATIFY_COL, axis=1)
    X_val = X_val.drop(STRATIFY_COL, axis=1)
    X_test = X_test.drop(STRATIFY_COL, axis=1)

    X_train_val = pd.concat((X_train, X_val), ignore_index=True)
    ord_train_val = X_train_val.index.sample(
        n=X_train_val.shape[0],
        replace=False,
        random_state=42
    ).values

    return X_train, X_val, X_test, y_train, y_val, y_test, ord_train_val
