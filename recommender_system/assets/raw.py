# # import pandas as pd
# # from dagster import resource, op, pipeline, ModeDefinition
# # from dagster import GraphDefinition
# from dagster_airbyte import (
#     airbyte_resource,
#     load_assets_from_airbyte_instance,
#     build_airbyte_assets
# )
# from recommender_system.constants import AIRBYTE_CONNECTION_ID
# # from dagster_mlflow import mlflow_tracking

# airbyte_instance = airbyte_resource.configured(
#     {
#         "host": {"env": "AIRBYTE_HOST"},
#         "port": {"env": "AIRBYTE_PORT"},
#         "username": {"env": "AIRBYTE_USERNAME"},
#         "password": {"env": "AIRBYTE_PASSWORD"},
#     }
# )
# # sync_foobar = airbyte_sync_op.configured({"connection_id": AIRBYTE_CONNECTION_ID}, name="sync_foobar")
# airbyte_assets = load_assets_from_airbyte_instance(
#     airbyte_instance,
#     io_manager_key="connection_to_io_manager_key_fn"
# )

# airbyte_assets = build_airbyte_assets(
#     connection_id=AIRBYTE_CONNECTION_ID,
#     destination_tables=["movies_raw", "users_raw", "scores_raw"],
#     # asset_key_prefix=["postgres_replica"]
# )
# # graph = GraphDefinition(name="my-graph", node_defs=airbyte_assets)
# # job = graph.to_job()

# META_COLS = ["name", "year", "release_date", "imdb_url"]
# GENRE_RAW_COLS = [
#     'unknown', 'Action', 'Adventure', 'Animation',
#     "Children's", 'Comedy', 'Crime', 'Documentary', 'Drama',
#     'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery',
#     'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'
# ]


# # @asset(
# #     resource_defs={
# #         "airbyte": airbyte_assets,
# #         "mlflow": mlflow_tracking
# #     },
# #     # freshness_policy=FreshnessPolicy(maximum_lag_minutes=5),
# #     group_name="raw",
# #     # code_version="2",
# #     # config_schema={
# #     #     'uri': String
# #     # },
# # )
# # def movies_raw(context) -> Output[pd.DataFrame]:
# #     uri = context.op_config["uri"]
# #     mlflow = context.resources.mlflow
# #     mlflow.log_params(context.op_config)
# #     result = pd.read_csv(uri)
# #     return Output(
# #         result,
# #         metadata={
# #             "Total rows": len(result),
# #             **result[GENRE_RAW_COLS].sum().to_dict(),
# #             "preview": MetadataValue.md(result.head().to_markdown()),
# #         },
# #     )


# # @asset(
# #     resource_defs={"mlflow": mlflow_tracking},
# #     freshness_policy=FreshnessPolicy(maximum_lag_minutes=5),
# #     group_name="raw",
# #     code_version="2",
# #     config_schema={
# #         'uri': String
# #     },
# # )
# # def movies_raw(context) -> Output[pd.DataFrame]:
# #     uri = context.op_config["uri"]
# #     mlflow = context.resources.mlflow
# #     mlflow.log_params(context.op_config)
# #     result = pd.read_csv(uri)
# #     return Output(
# #         result,
# #         metadata={
# #             "Total rows": len(result),
# #             **result[GENRE_RAW_COLS].sum().to_dict(),
# #             "preview": MetadataValue.md(result.head().to_markdown()),
# #         },
# #     )


# # @asset(
# #     group_name="raw",
# #     # io_manager_key="parquet_io_manager",
# #     # partitions_def=hourly_partitions,
# #     # key_prefix=["s3", "core"],
# #     config_schema={
# #         'uri': String
# #     }
# # )
# # def users_raw(context) -> Output[pd.DataFrame]:
# #     uri = context.op_config["uri"]
# #     result = pd.read_csv(uri)
# #     return Output(
# #         result,
# #         metadata={
# #             "Total rows": len(result),
# #             **result.groupby('Occupation').count()['id'].to_dict()
# #         },
# #     )


# # @asset(
# #     group_name="raw",
# #     # io_manager_key="parquet_io_manager",
# #     # partitions_def=hourly_partitions,
# #     # key_prefix=["s3", "core"],
# #     config_schema={
# #         'uri': String
# #     }
# # )
# # def scores_raw(context) -> Output[pd.DataFrame]:
# #     uri = context.op_config["uri"]
# #     result = pd.read_csv(uri)
# #     return Output(
# #         result,
# #         metadata={
# #             "Total rows": len(result)
# #         },
# #     )


# # from dagster import job, op
# # from dagster_airbyte import airbyte_resource, load_assets_from_airbyte_instance

# # @op(required_resource_keys={"airbyte"})
# # def load_assets(context):
# #     airbyte = context.resources.airbyte
# #     # Specify the Airbyte connection IDs for your sources
# #     source_ids = ["movies_id", "scores_id", "users_id"]
# #     for source_id in source_ids:
# #         load_assets_from_airbyte_instance(airbyte, source_id)

# # @job(resource_defs={"airbyte": airbyte_resource})
# # def airbyte_job():
# #     load_assets()
