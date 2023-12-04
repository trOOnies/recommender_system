import os
import re
import numpy as np
import pandas as pd
import psycopg2 as pg
from typing import List
from dagster import asset, Output, AssetIn, MetadataValue
# import pandas.io.sql as psql
from recommender_system.assets.raw import META_COLS, GENRE_RAW_COLS
from recommender_system.assets.code.funcs import order_cols
# from dagster_mlflow import mlflow_tracking
# from dagster_dbt import load_assets_from_dbt_project

# dbt_assets = load_assets_from_dbt_project(project_dir=DBT_PROJECT_DIR)

replace_dict = {"children's": "childrens", "film-noir": "film_noir", "sci-fi": "sci_fi"}
GENRE_COLS_TRANSFORM = {g: g.lower() for g in GENRE_RAW_COLS}
GENRE_COLS_TRANSFORM = {
    g: (replace_dict[g_new] if g_new in replace_dict else g_new)
    for g, g_new in GENRE_COLS_TRANSFORM.items()
}
del replace_dict
GENRE_COLS = [c for c in GENRE_COLS_TRANSFORM.values() if c != "unknown"]
GENRE_COLS_W_UNK = [c for c in GENRE_COLS] + ["unknown"]

AIRBYTE_META_COLS = ["_airbyte_ab_id", "_airbyte_emitted_at", "_airbyte_normalized_at", "_airbyte_{table}_hashid"]
CONN = pg.connect(
    f"host={os.environ['POSTGRES_HOST']} dbname=itba_mlops user={os.environ['POSTGRES_USER']} password={os.environ['POSTGRES_PASSWORD']}"
)


def get_airbyte_cols(table: str) -> List[str]:
    cols = [c for c in AIRBYTE_META_COLS]
    cols[-1] = cols[-1].format(table=table)
    return cols


# -----------------


@asset(
    ins={"scores_raw": AssetIn("scores_raw")},
    group_name='transformed',
)
def scores(scores_raw) -> Output[pd.DataFrame]:
    df = pd.read_sql_query(
        """select * from public.scores_raw""",
        con=CONN
    )
    df = df.drop(get_airbyte_cols("scores_raw"), axis=1)

    metadata = {}
    metadata["rows_in"] = df.shape[0]

    assert (df["index"] == df["Unnamed: 0"]).all()
    df = df.drop("Unnamed: 0", axis=1)
    df = df.rename({"index": "id"}, axis=1)
    df = df.set_index("id", drop=True, verify_integrity=True)
    df = df.rename({"Date": "fecha_hora"}, axis=1)

    metadata["rows_out"] = df.shape[0]
    metadata["df"] = MetadataValue.md(df.head(5).to_markdown())

    return Output(
        df,
        metadata=metadata,
    )


@asset(
    ins={"movies_raw": AssetIn("movies_raw")},
    group_name='transformed',
)
def movies(movies_raw) -> Output[pd.DataFrame]:
    df = pd.read_sql_query(
        """select * from public.movies_raw""",
        con=CONN
    )
    df = df.drop(get_airbyte_cols("movies_raw"), axis=1)

    metadata = {}
    metadata["rows_in"] = df.shape[0]

    # Chequeos
    assert (df.index == df["index"]).all()
    assert (df.index + 1 == df.id).all()

    # Cambios al df
    df = df.drop("index", axis=1)
    df = df.set_index("id", drop=True, verify_integrity=True)

    # Generos
    df = df.rename(GENRE_COLS_TRANSFORM, axis=1)
    for c in GENRE_COLS_W_UNK:
        assert set(df[c].unique()) == {0, 1}
    df = df.astype({c: bool for c in GENRE_COLS_W_UNK})
    assert (~df[GENRE_COLS][df.unknown].any(axis=1)).all()
    df = df.drop("unknown", axis=1)

    # Otros cambios

    df["release_date"] = pd.to_datetime(df["Release Date"])

    df["Name"] = df.Name.str.strip()

    YEAR_PATT = re.compile(r"[1-2]\d{3}$")

    df["year"] = df.Name.str[-5:-1]
    df["year_ok"] = df.year.map(lambda v: YEAR_PATT.match(v))
    df["year"] = df.apply(lambda row: float(row.year) if row.year_ok else np.nan, axis=1)

    df["name"] = df.apply(lambda row: row.Name[:-7] if row.year_ok else row.Name, axis=1)
    df = df.drop("year_ok", axis=1)

    df["imdb_url"] = df["IMDB URL"].str[19:]

    df = df.drop(["Release Date", "Name", "IMDB URL"], axis=1)

    df = order_cols(df, META_COLS)

    metadata["rows_out"] = df.shape[0]
    metadata["df"] = MetadataValue.md(df.head(5).to_markdown())

    return Output(
        df,
        metadata=metadata,
    )


@asset(
    ins={"users_raw": AssetIn("users_raw")},
    group_name='transformed',
)
def users(users_raw) -> Output[pd.DataFrame]:
    df = pd.read_sql_query(
        """select * from public.users_raw""",
        con=CONN
    )
    df = df.drop(get_airbyte_cols("users_raw"), axis=1)

    metadata = {}
    metadata["rows_in"] = df.shape[0]

    assert (df.index == df["index"]).all()
    assert (df.index + 1 == df.id).all()

    df = df.drop("index", axis=1)
    df = df.set_index("id", drop=True, verify_integrity=True)

    df = df.rename(
        {"year of birth": "year_birth", "Zip Code": "zip_code"},
        axis=1,
        errors="raise"
    )
    df = df.astype({"year_birth": int})

    assert df.gender.nunique() == 2
    df["is_female"] = df.gender == "F"
    df = df.drop("gender", axis=1)

    assert (df.zip_code.str.len() == 5).all()  # aunque usan letras y numeros a piacere

    metadata["rows_out"] = df.shape[0]
    metadata["df"] = MetadataValue.md(df.head(5).to_markdown())

    return Output(
        df,
        metadata=metadata,
    )
