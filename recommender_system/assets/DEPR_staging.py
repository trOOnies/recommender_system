import pandas as pd
from dagster import asset, AssetIn, Output, MetadataValue
from recommender_system.assets.DEPR_transformed import GENRE_COLS
from recommender_system.assets.code.funcs import order_cols


@asset(
    ins={
        "movies": AssetIn(),
        "users": AssetIn(),
        "scores": AssetIn()
    },
    group_name="staging"
)
def staged_data(
    movies: pd.DataFrame,
    users: pd.DataFrame,
    scores: pd.DataFrame
) -> Output[pd.DataFrame]:
    metadata = {
        "rows_scores_in": scores.shape[0],
        "rows_movies_in": movies.shape[0],
        "rows_users_in": users.shape[0],
    }

    assert "id" not in scores.columns
    df = scores.copy()

    df = df.reset_index(drop=False)
    df = df.rename({"id": "score_id"}, axis=1)

    users_aux = users.reset_index().drop("Full Name", axis=1)
    users_aux = users_aux.rename(
        {
            "year_birth": "u_year_birth",
            "zip_code": "u_zip_code",
            "is_female": "u_is_female"
        },
        axis=1
    )
    df = df.merge(users_aux, how="inner", left_on="user_id", right_on="id")
    df = df.drop("id", axis=1)

    movies_aux = movies.reset_index().drop(["name", "imdb_url"], axis=1)
    movies_aux = movies_aux.rename(
        {
            "year": "m_year",
            "release_date": "m_release_date"
        },
        axis=1
    )
    df = df.merge(movies_aux, how="inner", left_on="movie_id", right_on="id")
    df = df.drop("id", axis=1)

    df = df.rename({"score_id": "id"}, axis=1).set_index("id", drop=True, verify_integrity=True)

    df = df.rename({c: f"m_genre_{c}" for c in GENRE_COLS}, axis=1)

    df["fecha_hora"] = pd.to_datetime(df.fecha_hora)
    df["u_age"] = df.fecha_hora - df.u_year_birth.map(lambda v: pd.Timestamp(f"{v}-01-01"))
    df["u_age"] = (df.u_age.dt.days / 365.25).round(0).astype(int)
    df["year_diff"] = df.m_year - df.u_year_birth

    left_cols = ["user_id", "movie_id", "rating"] + [c for c in df.columns if c.startswith("u_")] + [c for c in df.columns if c.startswith("m_")]
    df = order_cols(df, left_cols)

    metadata["rows_out"] = df.shape[0]
    metadata["df"] = MetadataValue.md(df.head(5).to_markdown())

    return Output(df, metadata=metadata)
