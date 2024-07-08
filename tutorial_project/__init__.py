from dagster import Definitions, load_assets_from_modules
from dagstermill import ConfigurableLocalOutputNotebookIOManager

# from dagster_duckdb_pandas import DuckDBPandasIOManager
from .geopandas_io import (
    PostgreSQLPandasIOManager,
    PostGISGeoPandasIOManager,
    TrajectoryCollectionIOManager,
)

from . import assets

from .assets.gps_assets import gps_assets
from .assets.gps_assets import positionfixes_clean
from .assets.gps_assets import aggregations
import os

# from dagster_dbt import load_assets_from_dbt_project
from dagster import file_relative_path

from dagster_dbt import DbtCliClientResource
from dagster import Definitions, load_assets_from_modules
from .assets.dbt import dbt_assets
from .assets.dbt import DBT_PROJECT_PATH
from .assets.dbt import DBT_PROFILES


all_assets = load_assets_from_modules([gps_assets, aggregations])
# all_assets = load_assets_from_modules([gps_assets])

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
        "dbt": DbtCliClientResource(
            project_dir=DBT_PROJECT_PATH,
            profiles_dir=DBT_PROFILES,
        ),
    },
)

