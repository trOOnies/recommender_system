import pandas as pd
from typing import List
from dagster import asset, Output, String, FreshnessPolicy, MetadataValue
from dagster_mlflow import mlflow_tracking

META_COLS = ["name", "year", "release_date", "imdb_url"]
GENRE_RAW_COLS = [
    'unknown', 'Action', 'Adventure', 'Animation',
    "Children's", 'Comedy', 'Crime', 'Documentary', 'Drama',
    'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery',
    'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'
]


def order_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    return df[cols + [c for c in df.columns if c not in cols]]


@asset(
    resource_defs={"mlflow": mlflow_tracking},
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=5),
    group_name='sources',
    code_version="2",
    config_schema={
        'uri': String
    },
)
def movies(context) -> Output[pd.DataFrame]:
    uri = context.op_config["uri"]
    mlflow = context.resources.mlflow
    mlflow.log_params(context.op_config)
    result = pd.read_csv(uri)
    return Output(
        result,
        metadata={
            "Total rows": len(result),
            **result[GENRE_RAW_COLS].sum().to_dict(),
            "preview": MetadataValue.md(result.head().to_markdown()),
        },
    )


@asset(
    group_name='sources',
    # io_manager_key="parquet_io_manager",
    # partitions_def=hourly_partitions,
    # key_prefix=["s3", "core"],
    # config_schema={
    #     'uri': String
    # }
)
def users(context) -> Output[pd.DataFrame]:
    uri = context.op_config["uri"]
    result = pd.read_csv(uri)
    return Output(
        result,
        metadata={
            "Total rows": len(result),
            **result.groupby('Occupation').count()['id'].to_dict()
        },
    )


@asset(
    group_name='sources',
    # io_manager_key="parquet_io_manager",
    # partitions_def=hourly_partitions,
    # key_prefix=["s3", "core"],
    # config_schema={
    #     'uri': String
    # }
)
def scores(context) -> Output[pd.DataFrame]:
    uri = context.op_config["uri"]
    result = pd.read_csv(uri)
    return Output(
        result,
        metadata={
            "Total rows": len(result)
        },
    )
