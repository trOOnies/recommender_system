from setuptools import find_packages, setup

import os
DAGSTER_VERSION=os.getenv('DAGSTER_VERSION', '1.5.9')
DAGSTER_LIBS_VERSION=os.getenv('DAGSTER_LIBS_VERSION', '0.21.9')
MLFLOW_VERSION=os.getenv('MLFLOW_VERSION', '2.8.0')

setup(
    name="recommender_system",
    packages=find_packages(exclude=["recommender_system_tests"]),
    install_requires=[
        f"dagster=={DAGSTER_VERSION}",
        f"dagster-airbyte=={DAGSTER_LIBS_VERSION}",
        f"dagster-dbt=={DAGSTER_LIBS_VERSION}",
        f"dagster-mlflow=={DAGSTER_LIBS_VERSION}",
        f"mlflow=={MLFLOW_VERSION}",
        "dbt-postgres"
        # f"tensorflow==2.14.0",
        # dbt-duckdb
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "jupyter"]},
)
