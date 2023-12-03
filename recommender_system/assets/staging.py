import pandas as pd
from dagster import asset, AssetIn, Output
from recommender_system.assets.transformed import GENRE_COLS


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

    df = scores.copy()

    df = df.reset_index(drop=False)

    df = df.merge(movies.reset_index(), how="inner", left_on="movie_id", right_on="id")
    df = df.merge(users.reset_index(), how="inner", left_on="user_id", right_on="id")

    df = df.drop(["id_x", "id_y"], axis=1)
    df = df.rename({"index": "id"}, axis=1).set_index("id", drop=True, verify_integrity=True)
    df = df.rename(
        {
            "name": "m_name", "imdb_url": "m_imdb_url",
            "year": "m_year", "release_date": "m_release_date"
        },
        axis=1
    )
    df = df.rename(
        {
            "year_birth": "u_year_birth", "Full Name": "u_full_name",
            "zip_code": "u_zip_code", "is_female": "u_is_female"
        },
        axis=1
    )

    df = df.rename({c: f"m_genre_{c}" for c in GENRE_COLS}, axis=1)

    df["fecha_hora"] = pd.to_datetime(df.fecha_hora)
    df["u_age"] = df.fecha_hora - df.u_year_birth.map(lambda v: pd.Timestamp(f"{v}-01-01"))
    df["u_age"] = (df.u_age.dt.days / 365.25).round(0).astype(int)
    df["year_diff"] = df.m_year - df.u_year_birth

    metadata["rows_out"] = df.shape[0]

    return Output(df, metadata=metadata)
