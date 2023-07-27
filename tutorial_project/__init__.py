from dagster import Definitions, load_assets_from_modules
from dagstermill import ConfigurableLocalOutputNotebookIOManager

# from dagster_duckdb_pandas import DuckDBPandasIOManager
from .geopandas_io import (
    PostgreSQLPandasIOManager,
    PostGISGeoPandasIOManager,
    TrajectoryCollectionIOManager,
)

from . import assets

from .assets.MG91 import assets_mg91
from .assets.LH52 import assets_lh52
from tutorial_project.jobs import hello_job
import os

# from dagster_dbt import load_assets_from_dbt_project
from dagster import file_relative_path

from dagster_dbt import DbtCliClientResource
from dagster import Definitions, load_assets_from_modules
from .assets.dbt import dbt_assets
from .assets.dbt import DBT_PROJECT_PATH
from .assets.dbt import DBT_PROFILES

# import dbt project

# DBT_PROJECT_PATH = file_relative_path(__file__, "../dbt_project")
# DBT_PROFILES = file_relative_path(__file__, "../dbt_project")


# dbt_assets = load_assets_from_dbt_project(
#     project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROFILES, key_prefix=["dbt"]
# )

# print("=====", dbt_assets)
all_assets = load_assets_from_modules([assets_mg91, assets_lh52])

defs = Definitions(
    assets=all_assets + dbt_assets,
    resources={
        "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager(),
        "mobilityDb_manager": PostGISGeoPandasIOManager(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
        ),
        "trajectory_collection_manager": TrajectoryCollectionIOManager(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
            trackIdColumn="track_id",
            timeColumn="time",
        ),
        # "trajectory_manager": TrajectoryIOManager(
        #     user=os.getenv("POSTGRES_USER"),
        #     password=os.getenv("POSTGRES_PASSWORD"),
        #     host=os.getenv("POSTGRES_HOST"),
        #     port=os.getenv("POSTGRES_PORT"),
        #     database=os.getenv("POSTGRES_DB"),
        #     trackIdColumn="track_id",
        #     timeColumn="time",
        # ),
        "dbt": DbtCliClientResource(
            project_dir=DBT_PROJECT_PATH,
            profiles_dir=DBT_PROFILES,
        ),
    },
    # jobs=[hello_job],
)

# defs = Definitions(
#     assets=all_assets,
#     resources={
#         "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager(),
#         "io_manager": DuckDBPandasIOManager(
#             database="data/database.duckdb",
#             schema="IRIS",
#         )
#     },
# )


# defs = Definitions(
#     assets=[MG91_persona_reloj_20230428_02],
#     resources={
#         "io_manager": DuckDBPandasIOManager(
#             database="data/database.duckdb",
#             schema="IRIS",
#         )
#     },
# )
