import pandas as pd  # Add new imports to the top of `assets.py`
import requests

import geopandas as gpd
from dagstermill import define_dagstermill_asset
from dagster import asset, AssetIn, AssetKey
import geopandas
import movingpandas as mpd
import movingpandas 


from ...utils.gpx import load_gpx_file

from dagster import (
    AssetKey,
    DagsterInstance,
    MetadataValue,
    Output,
    asset,
    get_dagster_logger,
    Definitions
)

from ...utils.gpx import load_gpx_file

#### LH52
# @asset(group_name="LH52", compute_kind="gpx", key_prefix=["LH52", "gpx"])
# def LH52_persona_reloj_20230425_gpx():
#     gdf = load_gpx_file ("data/LH52/LH52_persona_reloj_20230425.gpx")
#     print(gdf.head())
#     return Output( 
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="LH52", io_manager_key="mobilityDb_manager", compute_kind="postgres")
# def LH52_persona_reloj_20230425(LH52_persona_reloj_20230425_gpx):
#     gdf = LH52_persona_reloj_20230425_gpx
#     print(gdf.head())
#     return Output( 
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# LH52_persona_reloj_20230425_explore = define_dagstermill_asset(
#     name="LH52_persona_reloj_20230425_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("LH52_persona_reloj_20230425")),
#     },
#     group_name="LH52",
#     key_prefix=["LH52", "raw_explore"]
# )

# @asset(group_name="LH52", compute_kind="trajectory", key_prefix=["LH52", "trajectories"])
# def LH52_persona_reloj_20230425_traj(LH52_persona_reloj_20230425: geopandas.GeoDataFrame):
#     gdf = LH52_persona_reloj_20230425
    
#     print("========")
#     print(gdf)
    
#     print(type(gdf))
    
#     traj = mpd.Trajectory(gdf, traj_id='id', t='time')

#     print("+++++++")
#     print(traj)
    
#     print("-------")
#     return Output( 
#             value=traj,
#             metadata={
#                 "description": "LH52_persona_reloj_20230425_clean---",
#                 "num_records": 0,  # Metadata can be any key-value pair
#             },
#         )
    
# @asset(group_name="LH52", compute_kind="trajectory", key_prefix=["LH52", "trajectories"])
# def LH52_persona_reloj_20230425_clean(LH52_persona_reloj_20230425_traj):
#     gdf = LH52_persona_reloj_20230425
    
#     print("========")
#     print(gdf)
    
#     print(type(gdf))
    
#     traj = LH52_persona_reloj_20230425_traj

#     print("+++++++")
#     print(traj)
    
#     print("-------")
#     return Output( 
#             value=traj,
#             metadata={
#                 "description": "LH52_persona_reloj_20230425_clean---",
#                 "num_records": 0,  # Metadata can be any key-value pair
#             },
#         )    

            

