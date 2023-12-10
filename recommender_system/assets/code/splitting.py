import numpy as np
import pandas as pd
from typing import Tuple, List
from math import sqrt
from sklearn.model_selection import train_test_split


def get_unique_vals(ser: pd.Series) -> set:
    vc = ser.value_counts()
    vc = vc[vc==1]
    return set(vc.index.to_list())


def split_unique(
    X: pd.DataFrame,
    y: np.ndarray,
    mask_unique,
    test_size: float
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, np.ndarray, np.ndarray, np.ndarray]:
    mask_w_test_size = (np.random.random_sample(X.shape[0]) < test_size)
    mask_unique_test = np.logical_and(mask_unique, mask_w_test_size)
    mask_unique_train = np.logical_and(mask_unique, ~mask_w_test_size)
    del mask_w_test_size

    X_train_unique = X[mask_unique_train]
    X_test_unique = X[mask_unique_test]
    X_res = X[~mask_unique]

    y_train_unique = y[mask_unique_train]
    y_test_unique = y[mask_unique_test]
    y_res = y[~mask_unique]
    del mask_unique, mask_unique_train, mask_unique_test

    return X_train_unique, X_test_unique, X_res, y_train_unique, y_test_unique, y_res


def two_cols_split(
    X: pd.DataFrame,
    y: np.ndarray,
    cols: List[str],
    test_size: float
) -> Tuple[pd.DataFrame, pd.DataFrame, np.ndarray, np.ndarray]:
    n_rows_orig = X.shape[0]
    n_cols_orig = X.shape[1]

    c0_unique = get_unique_vals(X[cols[0]])
    c1_unique = get_unique_vals(X[cols[1]])

    mask_unique = (X[cols[0]].isin(c0_unique)) | (X[cols[1]].isin(c1_unique))
    del c0_unique, c1_unique
    X_train_unique, X_test_unique, X_res, y_train_unique, y_test_unique, y_res = split_unique(
        X, y, mask_unique, test_size=test_size
    )
    del mask_unique

    # Calculo: a * a = a**2 = test_size --> a = sqrt(test_size)
    ts_sqrt = sqrt(test_size)
    X_train_0, X_test_0, y_train_0, y_test_0 = train_test_split(
        X_res, y_res,
        test_size=ts_sqrt,
        stratify=X_res[cols[0]],
        random_state=42
    )
    del X_res, y_res

    # Ahora me quedaron peliculas unicas tambien en X_test_0
    c1_unique = get_unique_vals(X_test_0[cols[1]])
    mask_unique = X_test_0[cols[1]].isin(c1_unique)
    del c1_unique
    X_train_unique_0, X_test_unique_0, X_test_0, y_train_unique_0, y_test_unique_0, y_test_0 = split_unique(
        X_test_0,
        y_test_0,
        mask_unique,
        test_size=ts_sqrt
    )
    del mask_unique

    X_train_1, X_test, y_train_1, y_test = train_test_split(
        X_test_0, y_test_0,
        test_size=ts_sqrt,
        stratify=X_test_0[cols[1]],
        random_state=42
    )
    del X_test_0, y_test_0

    X_train = pd.concat((X_train_1, X_train_unique_0, X_train_0, X_train_unique), ignore_index=True)
    X_test = pd.concat((X_test, X_test_unique_0, X_test_unique), ignore_index=True)
    assert X_train.shape[0] + X_test.shape[0] == n_rows_orig
    assert X_train.shape[1] == n_cols_orig
    assert X_test.shape[1] == n_cols_orig

    y_train = np.hstack((y_train_1, y_train_unique_0, y_train_0, y_train_unique))
    y_test = np.hstack((y_test, y_test_unique_0, y_test_unique))
    assert y_train.shape[0] + y_test.shape[0] == n_rows_orig

    X_train = X_train.sample(frac=1.0)
    X_test = X_test.sample(frac=1.0)
    np.random.shuffle(y_train)  # inplace
    np.random.shuffle(y_test)  # inplace

    print("test_size resultante:", float(X_test.shape[0]) / float(n_rows_orig))

    return X_train, X_test, y_train, y_test
