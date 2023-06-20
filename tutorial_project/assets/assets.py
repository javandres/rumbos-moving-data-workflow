# import pandas as pd  # Add new imports to the top of `assets.py`
# import requests

# import gpxpy
# import gpxpy.gpx
# import geopandas as gpd
# from dagstermill import define_dagstermill_asset
# from dagster import asset, AssetIn, AssetKey
# from dagster_duckdb_pandas import DuckDBPandasIOManager


# from .utils.gpx import load_gpx_file

# from dagster import (
#     AssetKey,
#     DagsterInstance,
#     MetadataValue,
#     Output,
#     asset,
#     get_dagster_logger,
#     Definitions
# )

# # @asset # add the asset decorator to tell Dagster this is an asset
# # @asset(io_manager_key="mobilityDb_manager")
# # def topstory_ids():
# #     data = [10,20,30,40,50,60]
  
# #     # Create the pandas DataFrame with column name is provided explicitly
# #     df = pd.DataFrame(data, columns=['Numbers'])
# #     return df
#     # return top_new_story_ids


# # @asset
# # def topstories(topstory_ids) -> pd.DataFrame:
# #     logger = get_dagster_logger()

# #     results = []
# #     for item_id in topstory_ids:
# #         item = requests.get(
# #             f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
# #         ).json()
# #         results.append(item)

# #         if len(results) % 20 == 0:
# #             logger.info(f"Got {len(results)} items so far.")

# #     df = pd.DataFrame(results)

# #     return Output(  # The return value is updated to wrap it in `Output` class
# #         value=df,  # The original df is passed in with the `value` parameter
# #         metadata={
# #             "num_records": len(df),  # Metadata can be any key-value pair
# #             "preview": MetadataValue.md(df.head().to_markdown()),
# #             # The `MetadataValue` class has useful static methods to build Metadata
# #         },
# #     )

# # @asset
# # def most_frequent_words(topstories) -> pd.DataFrame:
# #     stopwords = ["a", "the", "an", "of", "to", "in", "for", "and", "with", "on", "is"]

# #     # loop through the titles and count the frequency of each word
# #     word_counts = {}
# #     for raw_title in topstories["title"]:
# #         title = raw_title.lower()
# #         for word in title.split():
# #             cleaned_word = word.strip(".,-!?:;()[]'\"-")
# #             if cleaned_word not in stopwords and len(cleaned_word) > 0:
# #                 word_counts[cleaned_word] = word_counts.get(cleaned_word, 0) + 1

# #     # Get the top 25 most frequent words
# #     top_words = {
# #         pair[0]: pair[1]
# #         for pair in sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:25]
# #     }

# #     return top_words



# # def load_gpx_file(file_path):

# #     with open(file_path, "r") as gpx_file:
# #         gpx = gpxpy.parse(gpx_file)
    
# #     file_name = file_path.split("/")[-1]
# #     track_id = file_name.split(".")[0]
# #     route_info = []

# #     codigo = track_id.split("_")[0]


# #     for track in gpx.tracks:
# #         for segment in track.segments:
# #             for point in segment.points:
# #                 route_info.append({
# #                     'id': uuid.uuid4(),
# #                     'lat': point.latitude,
# #                     'lon': point.longitude,
# #                     'elevation': point.elevation,
# #                     'time': point.time,
# #                     'file_path': file_path,
# #                     'fila_name': file_name,
# #                     'track_id': track_id,
# #                     'codigo': codigo,
# #                 })
# #     route_df = pd.DataFrame(route_info)
# #     gdf = gpd.GeoDataFrame(
# #     route_df, geometry=gpd.points_from_xy(route_df.lon, route_df.lat), crs=from_epsg(4326))

# #     return gdf

# #### LH52
# @asset(group_name="LH52", compute_kind="gpx")
# def LH52_persona_reloj_20230425_gpx():
#     gdf = load_gpx_file("data/LH52/LH52_persona_reloj_20230425.gpx")
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
#     notebook_path="tutorial_project/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("LH52_persona_reloj_20230425")),
#     },
#     group_name="LH52"
# )

# #### MG91_artefacto_reloj_20230428_01
# @asset(group_name="MG91", compute_kind="gpx", metadata={"owner": "Emilia Acurio"})
# def MG91_artefacto_reloj_20230428_01_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_artefacto_reloj_20230428_01.gpx")
#     print(gdf.head())
#     return Output( 
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91", io_manager_key="mobilityDb_manager", compute_kind="postgres")
# def MG91_artefacto_reloj_20230428_01(MG91_artefacto_reloj_20230428_01_gpx):
#     gdf = MG91_artefacto_reloj_20230428_01_gpx
#     print(gdf.head())
#     return Output( 
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_persona_reloj_20230428_explore = define_dagstermill_asset(
#     name="MG91_persona_reloj_20230428_explore",
#     notebook_path="tutorial_project/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_artefacto_reloj_20230428_01")),
#     },
#     group_name="MG91"
# )

# #### MG91_artefacto_reloj_20230428_02
# @asset(group_name="MG91", compute_kind="gpx")
# def MG91_artefacto_reloj_20230428_02_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_artefacto_reloj_20230428_02.gpx")
#     print(gdf.head())
#     return Output( 
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_artefacto_reloj_20230428_02(MG91_artefacto_reloj_20230428_02_gpx):
#     gdf = MG91_artefacto_reloj_20230428_02_gpx
#     print(gdf.head())
#     return Output( 
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_artefacto_reloj_20230428_02_explore = define_dagstermill_asset(
#     name="MG91_artefacto_reloj_20230428_02_explore",
#     notebook_path="tutorial_project/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_artefacto_reloj_20230428_02")),
#     },
#     group_name="MG91"
# )
# #### MG91_persona_reloj_20230428_01
# @asset(group_name="MG91", compute_kind="gpx")
# def MG91_persona_reloj_20230428_01_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_persona_reloj_20230428_01.gpx")
#     print(gdf.head())
#     return Output( 
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_persona_reloj_20230428_01(MG91_persona_reloj_20230428_01_gpx):
#     gdf = MG91_persona_reloj_20230428_01_gpx
#     print(gdf.head())
#     return Output( 
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_persona_reloj_20230428_01_explore = define_dagstermill_asset(
#     name="MG91_persona_reloj_20230428_01_explore",
#     notebook_path="tutorial_project/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_persona_reloj_20230428_01")),
#     },
#     group_name="MG91"
# )

# ####  MG91_persona_reloj_20230428_02
# @asset(group_name="MG91", compute_kind="gpx")
# def MG91_persona_reloj_20230428_02_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_persona_reloj_20220428_02.gpx")
#     print(gdf.head())
#     return Output( 
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_persona_reloj_20230428_02(MG91_persona_reloj_20230428_02_gpx):
#     gdf = MG91_persona_reloj_20230428_02_gpx
#     print(gdf.head())
#     return Output( 
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_persona_reloj_20230428_02_explore = define_dagstermill_asset(
#     name="MG91_persona_reloj_20230428_02_explore",
#     notebook_path="tutorial_project/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_persona_reloj_20230428_02")),
#     },
#     group_name="MG91"
# )

# # MG91_notebook = define_dagstermill_asset(
# #     name="MG91_notebook",
# #     notebook_path="tutorial_project/gpx_explore.ipynb",
# #     ins={
# #         "MG91_persona_reloj_20230428_01": AssetIn(key=AssetKey("MG91_persona_reloj_20230428_01")),
# #         "MG91_persona_reloj_20230428_02": AssetIn(key=AssetKey("MG91_persona_reloj_20230428_02"))
# #     },
# #     group_name="MG91"
# # )


# # defs = Definitions(
# #     assets=[topstory_ids],
# #     resources={
# #         "io_manager": DuckDBPandasIOManager(
# #             database="data/database.duckdb",
# #             schema="IRIS",
# #         )
# #     },
# # )

      
# # iris_notebook2 = define_dagstermill_asset(
# #     name="iris_notebook2",
# #     notebook_path="tutorial_project/iris.ipynb",
# #     ins={
# #         "iris": AssetIn(key=AssetKey("iris_dataset"))
# #     }
# # )
            

