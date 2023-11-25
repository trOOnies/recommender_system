import re
import numpy as np
import pandas as pd
from typing import List
from dagster import asset, Output, AssetIn
from recommender_system.assets.sources import scores, movies, users, META_COLS, GENRE_RAW_COLS
# from dagster_mlflow import mlflow_tracking

replace_dict = {"children's": "childrens", "film-noir": "film_noir", "sci-fi": "sci_fi"}
GENRE_COLS_TRANSFORM = {g: g.lower() for g in GENRE_RAW_COLS}
GENRE_COLS_TRANSFORM = {
    g: (replace_dict[g_new] if g_new in replace_dict else g_new)
    for g, g_new in GENRE_COLS_TRANSFORM.items()
}
del replace_dict
GENRE_COLS = [c for c in GENRE_COLS_TRANSFORM.values() if c != "unknown"]


def order_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    return df[cols + [c for c in df.columns if c not in cols]]


@asset(
    ins={"scores": AssetIn()},
    group_name='transformed',
)
def transformed_scores(
    scores: pd.DataFrame
) -> Output[pd.DataFrame]:
    metadata = {}
    metadata["rows_in"] = scores.shape[0]

    new_scores = scores.copy()

    assert (new_scores["index"] == new_scores["Unnamed: 0"]).all()
    new_scores = new_scores.drop("Unnamed: 0", axis=1)
    new_scores = new_scores.set_index("index", drop=True, verify_integrity=True)
    new_scores = new_scores.rename({"Date": "fecha_hora"}, axis=1)
    metadata["rows_out"] = new_scores.shape[0]

    return Output(
        new_scores,
        metadata=metadata,
    )


@asset(
    ins={"movies": AssetIn()},
    group_name='transformed',
)
def transformed_movies(
    movies: pd.DataFrame
) -> Output[pd.DataFrame]:
    metadata = {}
    metadata["rows_in"] = new_movies.shape[0]

    new_movies = movies.copy()

    # Chequeos
    assert (new_movies.index == new_movies["index"]).all()
    assert (new_movies.index + 1 == new_movies.id).all()

    aux = new_movies.copy()
    aux = aux.drop(["index", "id", "Name", "Release Date", "IMDB URL"], axis=1)
    for c in aux.columns:
        assert set(aux[c].unique()) == {0, 1}  # para bools

    # Cambios al df
    new_movies = new_movies.drop("index", axis=1)
    new_movies = new_movies.set_index("id", drop=True, verify_integrity=True)

    new_movies["release_date"] = pd.to_datetime(new_movies["Release Date"])

    new_movies["Name"] = new_movies.Name.str.strip()

    YEAR_PATT = re.compile(r"[1-2]\d{3}$")

    new_movies["year"] = new_movies.Name.str[-5:-1]
    new_movies["year_ok"] = new_movies.year.map(lambda v: YEAR_PATT.match(v))
    new_movies["year"] = new_movies.apply(lambda row: float(row.year) if row.year_ok else np.nan, axis=1)

    new_movies["name"] = new_movies.apply(lambda row: row.Name[:-7] if row.year_ok else row.Name, axis=1)
    new_movies = new_movies.drop("year_ok", axis=1)

    new_movies["imdb_url"] = new_movies["IMDB URL"].str[19:]

    new_movies = new_movies.drop(["Release Date", "Name", "IMDB URL"], axis=1)

    new_movies = order_cols(new_movies, META_COLS)

    new_movies = new_movies.rename(GENRE_COLS_TRANSFORM, axis=1)
    new_movies = new_movies.astype({c: bool for c in GENRE_COLS + ["unkwown"]})

    assert (~new_movies[GENRE_COLS][new_movies.unknown].any(axis=1)).all()
    new_movies = new_movies.drop("unknown", axis=1)

    metadata["rows_out"] = new_movies.shape[0]

    return Output(
        new_movies,
        metadata=metadata,
    )


@asset(
    ins={"users": AssetIn()},
    group_name='transformed',
)
def transformed_users(
    users: pd.DataFrame
) -> Output[pd.DataFrame]:
    metadata = {}
    metadata["rows_in"] = users.shape[0]

    new_users = users.copy()

    assert (new_users.index == new_users["index"]).all()
    assert (new_users.index + 1 == new_users.id).all()

    new_users = new_users.drop("index", axis=1)
    new_users = new_users.set_index("id", drop=True, verify_integrity=True)

    new_users = new_users.rename(
        {"year of birth": "year_birth", "Gender": "gender", "Zip Code": "zip_code"},
        axis=1
    )
    new_users = new_users.astype({"year_birth": int})

    assert new_users.gender.nunique() == 2
    new_users["is_female"] = new_users.gender == "F"
    new_users = new_users.drop("gender", axis=1)

    assert (new_users.zip_code.str.len() == 5).all()  # aunque usan letras y numeros a piacere

    return Output(
        new_users,
        metadata=metadata,
    )
