from dagster_dbt import DbtCliResource
from recommender_system.constants import DBT_PROJECT_DIR

dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    target="local",
)
