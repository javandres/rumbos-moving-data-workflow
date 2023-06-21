import pandas as pd  # Add new imports to the top of `assets.py`
import requests

import gpxpy
import gpxpy.gpx
import geopandas as gpd

from dagstermill import define_dagstermill_asset

from dagster import (
    AssetKey,
    AssetIn,
    DagsterInstance,
    MetadataValue,
    Output,
    asset,
    get_dagster_logger,
    Definitions,
    multi_asset,
    AssetOut,
)

from ...utils.gpx import load_gpx_file

import movingpandas as mpd
import movingpandas
import json

from shapely.geometry import Point

with open("tutorial_project/assets/MG91/gpx_files.json", "r") as read_file:
    data = json.load(read_file)

assets_df = pd.DataFrame(data)
print(assets_df)


def make_gpx_assets(asset_to_make):
    @asset(
        name=asset_to_make["asset_name"] + "_gpx",
        group_name=asset_to_make["group"],
        compute_kind="gpx",
        key_prefix=["workdir", asset_to_make["code"], asset_to_make["group"], "gpx"],
    )
    def asset_template():
        gdf = load_gpx_file(
            "data"
            + "/"
            + asset_to_make["folder_name"]
            + "/"
            + asset_to_make["file_name"]
        )
        return Output(
            value=gdf,
            metadata={
                "num_records": len(gdf),
                "preview": MetadataValue.md(gdf.head().to_markdown()),
            },
        )

    return asset_template


def make_postgres_assets(asset_to_make):
    @asset(
        name=asset_to_make["asset_name"],
        group_name=asset_to_make["group"],
        compute_kind="postgres",
        ins={"asset_gpx": AssetIn(asset_to_make["asset_name"] + "_gpx")},
        io_manager_key="mobilityDb_manager",
    )
    def asset_template(asset_gpx):
        gdf = asset_gpx
        print(asset_gpx.head())
        return Output(
            value=gdf,
            metadata={
                "num_records": len(gdf),
                "preview": MetadataValue.md(gdf.head().to_markdown()),
            },
        )

    return asset_template


def make_jupyter_explore_assets(asset_to_make):
    asset_template = define_dagstermill_asset(
        name=asset_to_make["asset_name"] + "_explore",
        notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
        ins={
            "data": AssetIn(key=AssetKey(asset_to_make["asset_name"])),
        },
        group_name=asset_to_make["group"],
        key_prefix=[asset_to_make["group"], "raw_explore"],
    )
    return asset_template


def make_trajectory_assets(asset_to_make):
    @asset(
        name=asset_to_make["asset_name"] + "_traj",
        group_name=asset_to_make["group"],
        compute_kind="trajectory",
        ins={"asset_gpx": AssetIn(asset_to_make["asset_name"])},
        key_prefix=["workdir"],
    )
    def asset_template(asset_gpx):
        gdf = asset_gpx
        traj = mpd.Trajectory(gdf, traj_id="track_id", t="time")
        traj_gdf = traj.to_point_gdf()

        return Output(
            value=traj,
            metadata={
                "description": "",
                "rows": traj.size(),
                "duration": "{}".format(gdf.time.max() - gdf.time.min()),
                "start_time": MetadataValue.text(
                    traj.get_start_time().strftime("%m/%d/%Y, %H:%M:%S")
                ),
                "end_time": traj.get_end_time().strftime(("%m/%d/%Y, %H:%M:%S")),
                "preview": MetadataValue.md(traj_gdf.head().to_markdown()),
            },
        )

    return asset_template


def make_trajectory_clean_assets(asset_to_make):
    @asset(
        name=asset_to_make["asset_name"] + "_traj_clean",
        group_name=asset_to_make["group"],
        compute_kind="trajectory",
        ins={"traj": AssetIn(asset_to_make["asset_name"] + "_traj")},
        key_prefix=["workdir"],
    )
    def asset_template(traj):
        cleaned = traj.copy()

        cleaned.add_speed(overwrite=True)
        for i in range(0, 10):
            cleaned = mpd.OutlierCleaner(cleaned).clean({"speed": 0.5})

        traj_gdf = cleaned.to_point_gdf()

        return Output(
            value=cleaned,
            metadata={
                "description": "",
                "rows": cleaned.size(),
                "start_time": MetadataValue.text(
                    cleaned.get_start_time().strftime("%m/%d/%Y, %H:%M:%S")
                ),
                "end_time": cleaned.get_end_time().strftime(("%m/%d/%Y, %H:%M:%S")),
                "preview": MetadataValue.md(traj_gdf.head().to_markdown()),
            },
        )

    return asset_template


# def make_trajectory_clean_assets_db(asset_to_make):
#     @asset(
#         name=asset_to_make["asset_name"]+"_traj_clean_db",
#         group_name=asset_to_make["group"],
#         compute_kind="postgres",
#         ins={"traj": AssetIn(asset_to_make["asset_name"]+"_traj_clean")},
#     )
#     def asset_template(traj):
#             traj_gdf = traj.to_traj_gdf()
#             print(traj_gdf.head())
#             return Output(
#                     value=traj,
#                     metadata={
#                         "description": "",
#                         "rows": traj_gdf.size(),
#                         # "duration": "{}".format(traj_gdf.time.max() - traj_gdf.time.min()),
#                         # "start_time": MetadataValue.text(cleaned.get_start_time().strftime("%m/%d/%Y, %H:%M:%S")),
#                         # "end_time": cleaned.get_end_time().strftime(("%m/%d/%Y, %H:%M:%S")),
#                         # "preview": MetadataValue.md(traj_gdf.head().to_markdown()),
#                     },

#         )
#     return asset_template


def make_trajectory_smooth_assets(asset_to_make):
    @asset(
        name=asset_to_make["asset_name"] + "_traj_smooth",
        group_name=asset_to_make["group"],
        compute_kind="trajectory",
        ins={"traj": AssetIn(asset_to_make["asset_name"] + "_traj_clean")},
        key_prefix=["workdir"],
    )
    def asset_template(traj):
        smoothed = mpd.KalmanSmootherCV(traj).smooth(
            process_noise_std=0.1, measurement_noise_std=10
        )
        traj_gdf = smoothed.to_point_gdf()
        return Output(
            value=smoothed,
            metadata={
                "description": "",
                "rows": smoothed.size(),
                "start_time": MetadataValue.text(
                    smoothed.get_start_time().strftime("%m/%d/%Y, %H:%M:%S")
                ),
                "end_time": smoothed.get_end_time().strftime(("%m/%d/%Y, %H:%M:%S")),
                "preview": MetadataValue.md(traj_gdf.head().to_markdown()),
            },
        )

    return asset_template


def make_trajectory_smooth_assets_db(asset_to_make):
    @asset(
        name=asset_to_make["asset_name"] + "_traj_smooth_db",
        group_name=asset_to_make["group"],
        compute_kind="postgres",
        ins={"traj": AssetIn(asset_to_make["asset_name"] + "_traj_smooth")},
        io_manager_key="mobilityDb_manager",
    )
    def asset_template(traj):
        traj_gdf = traj.to_line_gdf()
        return Output(
            value=traj_gdf,
            metadata={
                "description": "",
                "rows": traj.size(),
                "start_time": MetadataValue.text(
                    traj.get_start_time().strftime("%m/%d/%Y, %H:%M:%S")
                ),
                "end_time": traj.get_end_time().strftime(("%m/%d/%Y, %H:%M:%S")),
                "preview": MetadataValue.md(traj_gdf.head().to_markdown()),
            },
        )

    return asset_template


def make_trajectory_clean_smooth_jupyter(asset_to_make):
    ins = {
        "traj": AssetIn(asset_to_make["asset_name"] + "_traj"),
        "cleaned": AssetIn(asset_to_make["asset_name"] + "_traj_clean"),
        "smoothed": AssetIn(asset_to_make["asset_name"] + "_traj_smooth"),
    }
    asset_template = define_dagstermill_asset(
        name=asset_to_make["asset_name"] + "_traj_clean_jupyter",
        notebook_path="tutorial_project/notebooks_templates/trajectory_clean.ipynb",
        ins=ins,
        group_name=asset_to_make["group"],
        key_prefix=[asset_to_make["group"], "trajectory_clean"],
    )
    return asset_template


factory_assets_gpx = [make_gpx_assets(asset) for asset in data]
factory_assets_postgres = [make_postgres_assets(asset) for asset in data]
factory_assets_jupyter_explore = [make_jupyter_explore_assets(asset) for asset in data]
factory_assets_trajectory = [make_trajectory_assets(asset) for asset in data]
factory_assets_trajectory_clean = [
    make_trajectory_clean_assets(asset) for asset in data
]
factory_assets_trajectory_smooth = [
    make_trajectory_smooth_assets(asset) for asset in data
]
factory_assets_trajectory_smooth_db = [
    make_trajectory_smooth_assets_db(asset) for asset in data
]
# factory_assets_trajectory_clean_db = [make_trajectory_clean_assets_db(asset) for asset in data]

# factory_assets_trajectory_smooth = []

# for index, row in assets_df.iterrows():
#     row_dict = row.to_dict()
#     # Perform operations with the row dictionary
#     print(row_dict)  # Replace this with your desired logic
# smooth_result =  make_trajectory_clean_assets(row_dict)
# print("===>", asset, smooth_result)
# factory_assets_trajectory_smooth.append( smooth_result)

# for asset in assets_df:
#     print("===>", asset)
# smooth_result =  make_trajectory_clean_assets(asset)
# print("===>", asset, smooth_result)
# factory_assets_trajectory_smooth.append( smooth_result)


# factory_assets_trajectory_clean_jupyter = [make_trajectory_clean_smooth_jupyter(asset) for asset in data]


def make_trajectory_tipo_dia(code, group, date, type, asset_inputs):
    ins = {
        f"asset_gpx{i+1}": AssetIn(asset_input["asset_name"] + "_traj_smooth")
        for i, asset_input in enumerate(asset_inputs)
    }

    @asset(
        name=group + "_traj",
        group_name=code + "_" + date + "_" + type,
        compute_kind="trajectory_collection",
        ins=ins,
        key_prefix=["workdir"],
    )
    def asset_template(**kargs):
        lst = []
        for arg in kargs:
            lst.append(kargs[arg])

        traj_collection = mpd.TrajectoryCollection(lst, "track_id", t="time")

        traj_gdf = traj_collection.to_point_gdf()
        traj_gdf["time"] = traj_gdf.index

        return Output(
            value=traj_collection,
            metadata={
                "description": "",
                "rows": len(traj_gdf),
                "start_time": MetadataValue.text(
                    traj_gdf["time"].min().strftime("%m/%d/%Y, %H:%M:%S")
                ),
                "end_time": MetadataValue.text(
                    traj_gdf["time"].max().strftime("%m/%d/%Y, %H:%M:%S")
                ),
                "preview": MetadataValue.md(traj_gdf.head().to_markdown()),
            },
        )

    return asset_template


def make_assets_db(
    parent_asset_name, asset_name, group_name, compute_kind, geometry_type="point"
):
    @asset(
        name=asset_name,
        group_name=group_name,
        compute_kind=compute_kind,
        ins={"sourceTrajectory": AssetIn(parent_asset_name)},
        io_manager_key="mobilityDb_manager",
    )
    def asset_template(sourceTrajectory):
        if geometry_type == "point":
            gdf = sourceTrajectory.to_point_gdf()
        else:
            gdf = sourceTrajectory.to_line_gdf()

        gdf["time"] = gdf.index

        return Output(
            value=gdf,
            metadata={
                "description": "",
                "rows": len(gdf),
                # "start_time": MetadataValue.text(gdf['time'].min().strftime("%m/%d/%Y, %H:%M:%S")),
                # "end_time": MetadataValue.text(gdf['time'].max().strftime("%m/%d/%Y, %H:%M:%S")),
                "preview": MetadataValue.md(gdf.head().to_markdown()),
            },
        )

    return asset_template


factory_assets_trajectory_by_date_type = []
factory_assets_trajectory_by_date_type_db = []
factory_assets_trajectory_by_date_type_db_track = []

grouped = assets_df.groupby(["code", "group", "date", "type"])
for (code, group, date, type), group_data in grouped:
    result = make_trajectory_tipo_dia(
        code, group, date, type, group_data.to_dict("records")
    )
    factory_assets_trajectory_by_date_type.append(result)
    print("RESULT", result)
    result = make_assets_db(
        group + "_traj", group + "_traj_db", code + "_" + date + "_" + type, "postgres"
    )
    factory_assets_trajectory_by_date_type_db.append(result)
    result_track = make_assets_db(
        group + "_traj",
        group + "_traj_db_track",
        code + "_" + date + "_" + type,
        "postgres",
        "line",
    )
    factory_assets_trajectory_by_date_type_db_track.append(result_track)


# print("+_+_+_+_+", factory_assets_trajectory_by_date_type)

# for te in factory_assets_trajectory_by_date_type:
#     print (te)

# factory_assets_ins = dict(zip(data, [AssetIn(i) for i in data]))


# @asset
# def downstream_of_factory_one(MG91):
#     pass

# @asset(
#     ins=factory_assets_ins
# )
# def downstream_of_factory_all(**kwargs):
#     pass

#################################### MG91_20230428
#### MG91_artefacto_reloj_20230428_01
# @asset(group_name="MG91_20230428", compute_kind="gpx", key_prefix=["MG91", "gpx"], metadata={"owner": "Emilia Acurio"})
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

# @asset(group_name="MG91_20230428", io_manager_key="mobilityDb_manager", compute_kind="postgres")
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
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_artefacto_reloj_20230428_01")),
#     },
#     group_name="MG91_20230428",
#     key_prefix=["MG91", "raw_explore"],
# )

# @asset(group_name="MG91_20230428", compute_kind="trajectory", key_prefix=["MG91", "trajectories"])
# def MG91_persona_reloj_20230428_01_traj(MG91_artefacto_reloj_20230428_01: gpd.GeoDataFrame):
#     gdf = MG91_artefacto_reloj_20230428_01
#     traj = mpd.Trajectory(gdf, traj_id='id', t='time')

#     traj_gdf = traj.to_traj_gdf()
#     traj_gdf.drop('geometry', axis='columns', inplace=True)

#     return Output(
#             value=traj,
#             metadata={
#                 "description": "",
#                 "rows": traj.size(),
#                 "duration": "{}".format(gdf.time.max() - gdf.time.min()),
#                 "start_time": MetadataValue.text(traj.get_start_time().strftime("%m/%d/%Y, %H:%M:%S")),
#                 "end_time": traj.get_end_time().strftime(("%m/%d/%Y, %H:%M:%S")),
#                 "preview": MetadataValue.md(traj_gdf.head().to_markdown()),
#             },

#         )

# #### MG91_artefacto_reloj_20230428_02
# @asset(group_name="MG91_20230428", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
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

# @asset(group_name="MG91_20230428", io_manager_key="mobilityDb_manager", compute_kind="postgres")
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
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_artefacto_reloj_20230428_02")),
#     },
#     group_name="MG91_20230428",
#     key_prefix=["MG91", "raw_explore"],
# )

# #### MG91_persona_reloj_20230428_01
# @asset(group_name="MG91_20230428", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
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

# @asset(group_name="MG91_20230428", io_manager_key="mobilityDb_manager",compute_kind="postgres")
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
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_persona_reloj_20230428_01")),
#     },
#     group_name="MG91_20230428",
#     key_prefix=["MG91", "raw_explore"],
# )

# ####  MG91_persona_reloj_20230428_02
# @asset(group_name="MG91_20230428", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
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

# @asset(group_name="MG91_20230428", io_manager_key="mobilityDb_manager",compute_kind="postgres")
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
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_persona_reloj_20230428_02")),
#     },
#     group_name="MG91_20230428",
#     key_prefix=["MG91", "raw_explore"],
# )

# #################################### MG91_20230503
# ####  MG91_persona_reloj_20230503_01
# @asset(group_name="MG91_20230503", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
# def MG91_persona_reloj_20230503_01_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_persona_reloj_20230503_01.gpx")
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91_20230503", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_persona_reloj_20230503_01(MG91_persona_reloj_20230503_01_gpx):
#     gdf = MG91_persona_reloj_20230503_01_gpx
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_persona_reloj_20230503_01_explore = define_dagstermill_asset(
#     name="MG91_persona_reloj_20230503_01_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_persona_reloj_20230503_01")),
#     },
#     group_name="MG91_20230503",
#     key_prefix=["MG91", "raw_explore"],
# )

# ####  MG91_persona_reloj_20230503_02
# @asset(group_name="MG91_20230503", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
# def MG91_persona_reloj_20230503_02_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_persona_reloj_20230503_02.gpx")
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91_20230503", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_persona_reloj_20230503_02(MG91_persona_reloj_20230503_02_gpx):
#     gdf = MG91_persona_reloj_20230503_02_gpx
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_persona_reloj_20230503_02_explore = define_dagstermill_asset(
#     name="MG91_persona_reloj_20230503_02_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_persona_reloj_20230503_02")),
#     },
#     group_name="MG91_20230503",
#     key_prefix=["MG91", "raw_explore"],
# )

# #### MG91_artefacto_reloj_20230503_01
# @asset(group_name="MG91_20230503", compute_kind="gpx", key_prefix=["MG91", "gpx"], metadata={"owner": "Emilia Acurio"})
# def MG91_artefacto_reloj_20230503_01_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_artefacto_reloj_20230503_01.gpx")
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91_20230503", io_manager_key="mobilityDb_manager", compute_kind="postgres")
# def MG91_artefacto_reloj_20230503_01(MG91_artefacto_reloj_20230503_01_gpx):
#     gdf = MG91_artefacto_reloj_20230503_01_gpx
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_artefacto_reloj_20230503_01_explore = define_dagstermill_asset(
#     name="MG91_artefacto_reloj_20230503_01_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_artefacto_reloj_20230503_01")),
#     },
#     group_name="MG91_20230503",
#     key_prefix=["MG91", "raw_explore"],
# )

# #### MG91_artefacto_reloj_20230503_02
# @asset(group_name="MG91_20230503", compute_kind="gpx", key_prefix=["MG91", "gpx"], metadata={"owner": "Emilia Acurio"})
# def MG91_artefacto_reloj_20230503_02_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_artefacto_reloj_20230503_02.gpx")
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91_20230503", io_manager_key="mobilityDb_manager", compute_kind="postgres")
# def MG91_artefacto_reloj_20230503_02(MG91_artefacto_reloj_20230503_02_gpx):
#     gdf = MG91_artefacto_reloj_20230503_02_gpx
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_artefacto_reloj_20230503_02_explore = define_dagstermill_asset(
#     name="MG91_artefacto_reloj_20230503_02_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_artefacto_reloj_20230503_02")),
#     },
#     group_name="MG91_20230503",
#     key_prefix=["MG91", "raw_explore"],
# )

# #################################### MG91_20230505
# ####  MG91_persona_reloj_20230505_01
# @asset(group_name="MG91_20230505", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
# def MG91_persona_reloj_20230505_01_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_persona_reloj_20230505_01.gpx")
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91_20230505", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_persona_reloj_20230505_01(MG91_persona_reloj_20230505_01_gpx):
#     gdf = MG91_persona_reloj_20230505_01_gpx
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_persona_reloj_20230505_01_explore = define_dagstermill_asset(
#     name="MG91_persona_reloj_20230505_01_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_persona_reloj_20230505_01")),
#     },
#     group_name="MG91_20230505",
#     key_prefix=["MG91", "raw_explore"],
# )

# ####  MG91_persona_reloj_20230505_02
# @asset(group_name="MG91_20230505", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
# def MG91_persona_reloj_20230505_02_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_persona_reloj_20230505_02.gpx")
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91_20230505", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_persona_reloj_20230505_02(MG91_persona_reloj_20230505_02_gpx):
#     gdf = MG91_persona_reloj_20230505_02_gpx
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_persona_reloj_20230505_02_explore = define_dagstermill_asset(
#     name="MG91_persona_reloj_20230505_02_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_persona_reloj_20230505_02")),
#     },
#     group_name="MG91_20230505",
#     key_prefix=["MG91", "raw_explore"],
# )

# ####  MG91_artefacto_reloj_20230505_01
# @asset(group_name="MG91_20230505", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
# def MG91_artefacto_reloj_20230505_01_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_artefacto_reloj_20230505_01.gpx")
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91_20230505", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_artefacto_reloj_20230505_01(MG91_artefacto_reloj_20230505_01_gpx):
#     gdf = MG91_artefacto_reloj_20230505_01_gpx
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_artefacto_reloj_20230505_01_explore = define_dagstermill_asset(
#     name="MG91_artefacto_reloj_20230505_01_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_artefacto_reloj_20230505_01")),
#     },
#     group_name="MG91_20230505",
#     key_prefix=["MG91", "raw_explore"],
# )

# ####  MG91_artefacto_reloj_20230505_02
# @asset(group_name="MG91_20230505", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
# def MG91_artefacto_reloj_20230505_02_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_artefacto_reloj_20230505_02.gpx")
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91_20230505", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_artefacto_reloj_20230505_02(MG91_artefacto_reloj_20230505_02_gpx):
#     gdf = MG91_artefacto_reloj_20230505_02_gpx
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_artefacto_reloj_20230505_02_explore = define_dagstermill_asset(
#     name="MG91_artefacto_reloj_20230505_02_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_artefacto_reloj_20230505_02")),
#     },
#     group_name="MG91_20230505",
#     key_prefix=["MG91", "raw_explore"],
# )

# #################################### MG91_20230508
# ####  MG91_persona_reloj_20230508_01
# @asset(group_name="MG91_20230508", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
# def MG91_persona_reloj_20230508_01_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_persona_reloj_20230508_1.gpx")
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91_20230508", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_persona_reloj_20230508_01(MG91_persona_reloj_20230508_01_gpx):
#     gdf = MG91_persona_reloj_20230508_01_gpx
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_persona_reloj_20230508_01_explore = define_dagstermill_asset(
#     name="MG91_persona_reloj_20230508_01_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_persona_reloj_20230508_01")),
#     },
#     group_name="MG91_20230508",
#     key_prefix=["MG91", "raw_explore"],
# )

# ####  MG91_persona_reloj_20230508_02
# @asset(group_name="MG91_20230508", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
# def MG91_persona_reloj_20230508_02_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_persona_reloj_20230508_2.gpx")
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91_20230508", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_persona_reloj_20230508_02(MG91_persona_reloj_20230508_02_gpx):
#     gdf = MG91_persona_reloj_20230508_02_gpx
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_persona_reloj_20230508_02_explore = define_dagstermill_asset(
#     name="MG91_persona_reloj_20230508_02_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_persona_reloj_20230508_02")),
#     },
#     group_name="MG91_20230508",
#     key_prefix=["MG91", "raw_explore"],
# )

# ####  MG91_artefacto_reloj_20230508_01
# @asset(group_name="MG91_20230508", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
# def MG91_artefacto_reloj_20230508_01_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_artefacto_reloj_20230508_1.gpx")
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91_20230508", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_artefacto_reloj_20230508_01(MG91_artefacto_reloj_20230508_01_gpx):
#     gdf = MG91_artefacto_reloj_20230508_01_gpx
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_artefacto_reloj_20230508_01_explore = define_dagstermill_asset(
#     name="MG91_artefacto_reloj_20230508_01_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_artefacto_reloj_20230508_01")),
#     },
#     group_name="MG91_20230508",
#     key_prefix=["MG91", "raw_explore"],
# )

# ####  MG91_artefacto_reloj_20230508_02
# @asset(group_name="MG91_20230508", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
# def MG91_artefacto_reloj_20230508_02_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_artefacto_reloj_20230508_2.gpx")
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91_20230508", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_artefacto_reloj_20230508_02(MG91_artefacto_reloj_20230508_02_gpx):
#     gdf = MG91_artefacto_reloj_20230508_02_gpx
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_artefacto_reloj_20230508_02_explore = define_dagstermill_asset(
#     name="MG91_artefacto_reloj_20230508_02_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_artefacto_reloj_20230508_02")),
#     },
#     group_name="MG91_20230508",
#     key_prefix=["MG91", "raw_explore"],
# )

# #################################### MG91_20230510
# ####  MG91_persona_reloj_20230510_01
# @asset(group_name="MG91_20230510", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
# def MG91_persona_reloj_20230510_01_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_persona_reloj_20230510_1.gpx")
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91_20230510", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_persona_reloj_20230510_01(MG91_persona_reloj_20230510_01_gpx):
#     gdf = MG91_persona_reloj_20230510_01_gpx
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_persona_reloj_20230510_01_explore = define_dagstermill_asset(
#     name="MG91_persona_reloj_20230510_01_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_persona_reloj_20230510_01")),
#     },
#     group_name="MG91_20230510",
#     key_prefix=["MG91", "raw_explore"],
# )

# ####  MG91_artefacto_reloj_20230510_01
# @asset(group_name="MG91_20230510", compute_kind="gpx", key_prefix=["MG91", "gpx"],)
# def MG91_artefacto_reloj_20230510_01_gpx():
#     gdf = load_gpx_file("data/MG91/MG91_artefacto_reloj_20230510_1.gpx")
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# @asset(group_name="MG91_20230510", io_manager_key="mobilityDb_manager",compute_kind="postgres")
# def MG91_artefacto_reloj_20230510_01(MG91_artefacto_reloj_20230510_01_gpx):
#     gdf = MG91_artefacto_reloj_20230510_01_gpx
#     print(gdf.head())
#     return Output(
#             value=gdf,
#             metadata={
#                 "num_records": len(gdf),  # Metadata can be any key-value pair
#                 "preview": MetadataValue.md(gdf.head().to_markdown()),
#             },
#         )

# MG91_artefacto_reloj_20230510_01_explore = define_dagstermill_asset(
#     name="MG91_artefacto_reloj_20230510_01_explore",
#     notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
#     ins={
#         "data": AssetIn(key=AssetKey("MG91_artefacto_reloj_20230510_01")),
#     },
#     group_name="MG91_20230510",
#     key_prefix=["MG91", "raw_explore"],
# )
