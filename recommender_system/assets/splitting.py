import os
import numpy as np
import pandas as pd
import psycopg2 as pg
from dagster import multi_asset, AssetIn, AssetOut, Output
# from dagster_dbt import get_asset_key_for_model
from recommender_system.constants import LABEL_COL, USER_COL, MOVIE_COL
from recommender_system.assets.dbt_transform import staged_data_asset_key
from recommender_system.assets.code.splitting import two_cols_split

CONN = pg.connect(
    f"host={os.environ['POSTGRES_HOST']} dbname=itba_mlops user={os.environ['POSTGRES_USER']} password={os.environ['POSTGRES_PASSWORD']}"
)


@multi_asset(
    compute_kind="python",
    deps=staged_data_asset_key,
    # ins={
    #     "staged_data": AssetIn(key=staged_data_asset_key)
    # },
    outs={
        "X_train": AssetOut(
            dagster_type=Output[pd.DataFrame],
            description="Training features sin validation",
            group_name="train_data"
        ),
        "X_val": AssetOut(
            dagster_type=Output[pd.DataFrame],
            description="Validation features",
            group_name="val_data"
        ),
        "X_test": AssetOut(
            dagster_type=Output[pd.DataFrame],
            description="Testing features",
            group_name="test_data"
        ),
        "y_train": AssetOut(
            dagster_type=Output[np.ndarray],
            description="Training labels",
            group_name="train_data"
        ),
        "y_val": AssetOut(
            dagster_type=Output[np.ndarray],
            description="Validation labels",
            group_name="val_data"
        ),
        "y_test": AssetOut(
            dagster_type=Output[np.ndarray],
            description="Testing labels",
            group_name="test_data"
        ),
        "ord_train_val": AssetOut(
            dagster_type=Output[np.ndarray],
            description="Indices para mezclar train y val",
            group_name="train_data"
        )
    }
)
def splitted_data():
    staged_data = pd.read_sql_query(
        """select * from public.staged_data""",
        con=CONN
    )
    staged_data = staged_data.sample(
        n=staged_data.shape[0],
        random_state=42,
        replace=False,
        ignore_index=True
    )

    y = staged_data[LABEL_COL]
    X = staged_data[[c for c in staged_data.columns if c != LABEL_COL]]

    X_train_val, X_test, y_train_val, y_test = two_cols_split(
        X, y,
        cols=[USER_COL, MOVIE_COL],
        test_size=0.10
    )
    X_train, X_val, y_train, y_val = two_cols_split(
        X_train_val, y_train_val,
        cols=[USER_COL, MOVIE_COL],
        test_size=0.10
    )
    del X_train_val, y_train_val

    X_train_val = pd.concat((X_train, X_val), ignore_index=True)
    train_val_ix = X_train_val.reset_index(drop=False)["index"]
    ord_train_val = train_val_ix.sample(
        n=X_train_val.shape[0],
        replace=False,
        random_state=42
    ).values

    return X_train, X_val, X_test, y_train, y_val, y_test, ord_train_val


@multi_asset(
    compute_kind="python",
    ins={
        "X_train": AssetIn(),
        "X_val": AssetIn(),
        "y_train": AssetIn(),
        "y_val": AssetIn(),
        "ord_train_val": AssetIn()
    },
    outs={
        "X_train_val": AssetOut(
            dagster_type=Output[pd.DataFrame],
            description="Train+Val features",
            group_name="train_val_data"
        ),
        "y_train_val": AssetOut(
            dagster_type=Output[np.ndarray],
            description="Train+Val labels",
            group_name="train_val_data"
        )
    }
)
def train_val_outputs(
    X_train: pd.DataFrame,
    X_val: pd.DataFrame,
    y_train: np.ndarray,
    y_val: np.ndarray,
    ord_train_val: np.ndarray
):
    X_train_val = pd.concat((X_train, X_val), ignore_index=True)
    X_train_val = X_train_val.iloc[ord_train_val]

    y_train_val = np.hstack((y_train, y_val))
    assert y_train_val.shape[0] == y_train.shape[0] + y_val.shape[0]

    return X_train_val, y_train_val
