from dagster import Definitions, load_assets_from_modules
from dagstermill import ConfigurableLocalOutputNotebookIOManager
from dagster_duckdb_pandas import DuckDBPandasIOManager
from .geopandas_io import (
    PostgreSQLPandasIOManager,
    PostGISGeoPandasIOManager,
    TrajectoryIOManager,
)

from . import assets

from .assets.MG91 import assets_mg91
from .assets.LH52 import assets_lh52
from tutorial_project.jobs import hello_job
import os

all_assets = load_assets_from_modules([assets_mg91, assets_lh52])

defs = Definitions(
    assets=all_assets,
    resources={
        "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager(),
        "mobilityDb_manager": PostGISGeoPandasIOManager(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
        ),
        "trajectory_manager": TrajectoryIOManager(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
        ),
    },
    jobs=[hello_job],
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
