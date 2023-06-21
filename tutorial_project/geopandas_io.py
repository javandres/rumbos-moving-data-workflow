# Copyright 2023 Holger Bruch (hb@mfdz.de)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from contextlib import contextmanager
import geopandas
import pandas
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Connection
from typing import Iterator, Optional, Sequence
import movingpandas as mpd
import pandas as pd


from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)


@contextmanager
def connect_postgresql(config, schema="public") -> Iterator[Connection]:
    url = URL.create(
        "postgresql+psycopg2",
        username=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
        database=config["database"],
    )
    conn = None
    try:
        conn = create_engine(url).connect()
        yield conn
    finally:
        if conn:
            conn.close()


class PostgreSQLPandasIOManager(ConfigurableIOManager):
    """This IOManager will take in a pandas dataframe and store it in postgresql."""

    host: Optional[str] = "localhost"
    port: Optional[int] = 5432
    user: Optional[str] = "postgres"
    password: Optional[str]
    database: Optional[str]

    @property
    def _config(self):
        return self.dict()

    def handle_output(self, context: OutputContext, obj: pandas.DataFrame):
        schema, table = self._get_schema_table(context.asset_key)

        if isinstance(obj, pandas.DataFrame):
            row_count = len(obj)
            context.log.info(f"Row count: {row_count}")
            # TODO make chunksize configurable
            with connect_postgresql(config=self._config) as con:
                obj.to_sql(
                    con=con,
                    name=table,
                    schema=schema,
                    if_exists="replace",
                    chunksize=500,
                )
        else:
            raise Exception(f"Outputs of type {type(obj)} not supported.")

    def load_input(self, context: InputContext) -> geopandas.GeoDataFrame:
        schema, table = self._get_schema_table(context.asset_key)
        with connect_postgresql(config=self._config) as con:
            columns = (context.metadata or {}).get("columns")
            return self._load_input(con, table, schema, columns, context)

    def _load_input(
        self,
        con: Connection,
        table: str,
        schema: str,
        columns: Optional[Sequence[str]],
        context: InputContext,
    ) -> pandas.DataFrame:
        df = pandas.read_sql(
            sql=self._get_select_statement(
                table,
                schema,
                columns,
            ),
            con=con,
        )
        return df

    def _get_schema_table(self, asset_key):
        return (
            asset_key.path[-2] if len(asset_key.path) > 1 else "public",
            asset_key.path[-1],
        )

    def _get_select_statement(
        self,
        table: str,
        schema: str,
        columns: Optional[Sequence[str]],
    ):
        col_str = ", ".join(columns) if columns else "*"
        return f"""SELECT {col_str} FROM {schema}.\"{table}\""""


class PostGISGeoPandasIOManager(PostgreSQLPandasIOManager):
    """This IOManager will take in a geopandas dataframe and store it in postgis."""

    def handle_output(self, context: OutputContext, obj: geopandas.GeoDataFrame):
        schema, table = self._get_schema_table(context.asset_key)

        if isinstance(obj, geopandas.GeoDataFrame):
            row_count = len(obj)
            context.log.info(f"Row count: {row_count}")
            # TODO make chunksize configurable
            with connect_postgresql(config=self._config) as con:
                obj.to_postgis(
                    con=con,
                    name=table,
                    schema=schema,
                    if_exists="replace",
                    chunksize=500,
                )
        else:
            super().handle_output(context, obj)

    def _load_input(
        self,
        con: Connection,
        table: str,
        schema: str,
        columns: Optional[Sequence[str]],
        context: InputContext,
    ) -> geopandas.GeoDataFrame:
        df = geopandas.read_postgis(
            sql=self._get_select_statement(
                table,
                schema,
                columns,
            ),
            geom_col=(context.metadata or {}).get("geom_col", "geometry"),
            con=con,
        )
        return df


class TrajectoryIOManager(PostGISGeoPandasIOManager):
    """This IOManager will take in a geopandas dataframe and store it in postgis."""

    def handle_output(self, context, obj: mpd.Trajectory):
        if isinstance(obj, mpd.Trajectory):
            # write df to table
            # obj.to_sql(name=context.asset_key.path[-1], con=self._con, if_exists="replace")
            pass
        elif obj is None:
            # dbt has already written the data to this table
            pass
        else:
            raise ValueError(f"Unsupported object type {type(obj)} for DbIOManager.")

    def _load_input(
        self,
        con: Connection,
        table: str,
        schema: str,
        columns: Optional[Sequence[str]],
        context: InputContext,
    ) -> geopandas.GeoDataFrame:
        df = geopandas.read_postgis(
            sql=self._get_select_statement(
                table,
                schema,
                columns,
            ),
            geom_col=(context.metadata or {}).get("geom_col", "geometry"),
            con=con,
        )
        return df

        """Load the contents of a table as a pandas DataFrame."""
        # model_name = context.asset_key.path[-1]
        # return pd.read_sql(f"SELECT * FROM {model_name}", con=self.con_string)

    # def handle_output(self, context: OutputContext, obj: geopandas.GeoDataFrame):
    # 	schema, table = self._get_schema_table(context.asset_key)

    # 	if isinstance(obj, geopandas.GeoDataFrame):
    # 		traj = mpd.Trajectory(obj, traj_id='id', t='time')
    # 	# 	row_count = len(obj)
    # 	# 	context.log.info(f"Row count: {row_count}")
    # 	# 	# TODO make chunksize configurable
    # 	# 	with connect_postgresql(config=self._config) as con:
    # 	# 		obj.to_postgis(con=con,
    # 	# 			name=table,
    # 	# 			schema=schema,
    # 	# 			if_exists='replace',
    # 	# 			chunksize=500
    # 	# 		)
    # 		return traj
    # 	else:
    # 		super().handle_output(context, obj)

    # def _load_input(
    # 	self,
    # 	con: Connection,
    # 	table: str,
    # 	schema: str,
    # 	columns: Optional[Sequence[str]],
    # 	context: InputContext
    # ) -> geopandas.GeoDataFrame:
    # 	df = geopandas.read_postgis(
    # 		sql=self._get_select_statement(
    # 			table,
    # 			schema,
    # 			columns,
    # 		),
    # 		geom_col=(context.metadata or {}).get("geom_col", "geometry"),
    # 		con=con,
    # 	)

    # 	traj = mpd.Trajectory(df, traj_id='id', t='time')

    # 	return traj


# from dagster import ConfigurableIOManager, InputContext, OutputContext


# class DataframeTableIOManager(ConfigurableIOManager):
#     def handle_output(self, context, obj):
#         # name is the name given to the Out that we're storing for
#         table_name = context.name
#         write_dataframe_to_table(name=table_name, dataframe=obj)

#     def load_input(self, context):
#         # upstream_output.name is the name given to the Out that we're loading for
#         table_name = context.upstream_output.name
#         return read_dataframe_from_table(name=table_name)

# # import geopandas as gpd
# # from dagster import IOManager, OutOfProcessIOManager, OutputContext, TypeCheck, check


# # class GeoPandasIOManager(IOManager):
# #     def _compute_path(self, context):
# #         """Compute the path for the output file."""
# #         output_name = context.name
# #         output_dir = context.step_context.pipeline_run.run_id
# #         return f'{output_dir}/{output_name}.gpkg'

# #     def handle_output(self, context: OutputContext, obj: gpd.GeoDataFrame):
# #         """Handle the output of the step by saving the GeoDataFrame to a file."""
# #         path = self._compute_path(context)
# #         obj.to_file(path, driver='GPKG')

# #         # Return metadata about the output file
# #         return {'path': path}

# #     def load_input(self, context: OutputContext) -> gpd.GeoDataFrame:
# #         """Loading input is not supported."""
# #         check.failed('Loading input is not supported.')

# #     def can_load_input(self, context: OutputContext) -> bool:
# #         """Check if loading input is supported."""
# #         return False

# #     def handle_output_types(self) -> TypeCheck:
# #         """Specify the type of the output object."""
# #         return TypeCheck(gpd.GeoDataFrame)

# #     def get_output_asset_key(self, context: OutputContext) -> str:
# #         """Return the asset key for the output file."""
# #         path = self._compute_path(context)
# #         return path
