from dagster import AssetKey, AssetExecutionContext, AssetOut
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    dbt_assets,
    get_asset_key_for_model,
)
from typing import Mapping, Any
from recommender_system.constants import DBT_PROJECT_DIR

dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    target="dev",
)
dbt_parse_invocation = dbt_resource.cli(["parse"]).wait()
dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")

# DBT_MANIFEST_PATH = r"C:\Users\FacundoScasso\Documents\projects\recommender_system\recommender_system\dbt_fd\manifest.json"


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        asset_key = super().get_asset_key(dbt_resource_props)

        # if dbt_resource_props["resource_type"] == "model":
        #     asset_key = asset_key.with_prefix(["postgres", "public"])
        # elif dbt_resource_props["resource_type"] == "source":
        #     asset_key = asset_key.with_prefix(["postgres", "public"])

        return asset_key


@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource) -> AssetOut(group_name="transform"):
    yield from dbt.cli(["build"], context=context).stream()


# users_asset_key = get_asset_key_for_model([dbt_project_assets], "users")
# movies_asset_key = get_asset_key_for_model([dbt_project_assets], "movies")
# scores_asset_key = get_asset_key_for_model([dbt_project_assets], "scores")
staged_data_asset_key = get_asset_key_for_model([dbt_project_assets], "staged_data")

# @dbt_assets(manifest=DBT_MANIFEST_PATH)
# def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
#     yield from dbt.cli(["build"], context=context).stream()


# @assets(
#     ins={"movies_raw": AssetIn("movies_raw")},
#     group_name='transformed',
# )
# def movies(movies_raw) -> Output[DataFrame]:
#     return movies


# @assets(
#     ins={"scores_raw": AssetIn("scores_raw")},
#     group_name='transformed',
# )
# def scores(scores_raw) -> Output[DataFrame]:
#     return scores


# @assets(
#     ins={"users_raw": AssetIn("users_raw")},
#     group_name='transformed',
# )
# def users(users_raw) -> Output[DataFrame]:
#     return users
