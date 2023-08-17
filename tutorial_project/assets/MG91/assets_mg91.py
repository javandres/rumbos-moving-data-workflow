import pandas as pd
import requests

import gpxpy
import gpxpy.gpx
import geopandas as gpd
import hashlib

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
    SourceAsset,
)

from ...utils.gpx import load_gpx_file
from ...utils.gpx import load_gpkg_file

import movingpandas as mpd
import movingpandas
import json

from shapely.geometry import Point
from pyproj import CRS
from skmob.preprocessing import filtering
import skmob
from fiona.crs import from_epsg
from datetime import datetime, timedelta
import os


data = []
# with open("tutorial_project/assets/MG91/gpx_AF79.json", "r") as read_file:
#     data_AF79 = json.load(read_file)
# for x in data_AF79:
#     data.append(x)


# with open("tutorial_project/assets/MG91/gpx_MG91.json", "r") as read_file:
#     data_MG91 = json.load(read_file)
# for x in data_AF79:
#     data.append(x)


# with open("tutorial_project/assets/MG91/gpx_files.json", "r") as read_file:
#     data = json.load(read_file)


file_paths = [
    # "tutorial_project/assets/MG91/diarios_viaje/gpx_AF79.json",
    # "tutorial_project/assets/MG91/diarios_viaje/gpx_AT87.json",
    # "tutorial_project/assets/MG91/diarios_viaje/gpx_CD87.json",
    # "tutorial_project/assets/MG91/diarios_viaje/gpx_LH52.json",
    # "tutorial_project/assets/MG91/diarios_viaje/gpx_MG91.json",
    # "tutorial_project/assets/MG91/diarios_viaje/gpx_MC59.json",
    # "tutorial_project/assets/MG91/diarios_viaje/gpx_ML43.json",
    # "tutorial_project/assets/MG91/diarios_viaje/gpx_MQ70.json",
    # "tutorial_project/assets/MG91/diarios_viaje/gpx_MV43.json",
    # "tutorial_project/assets/MG91/diarios_viaje/gpx_MZ49.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_BC51.json",
    "tutorial_project/assets/MG91/solo_gps/gpx_BZ14.json",
    "tutorial_project/assets/MG91/solo_gps/gpx_CL74.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_CN83.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_JC73.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_LO71.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_LQ02.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_LQ07.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_MC30.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_MG17.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_ML09.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_ML24.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_ML72.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_MP88.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_MV79.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_RC57.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_RV07.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_RY43.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_RZ63.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_TQ38.json",
    # "tutorial_project/assets/MG91/solo_gps/gpx_TV55.json",
]
for file_path in file_paths:
    with open(file_path, "r") as read_file:
        json_data = json.load(read_file)
        for x in json_data:
            data.append(x)

# data.append(data_AF79)
assets_df = pd.DataFrame(data)
print(data)


config = {
    "clean": {"speed": 4},
    "smooth": {
        "process_noise_std": 0.5,
        "measurement_noise_std": 1,
    },
    "make_notebooks": False,
    "stop_detection_artefacto": [
        {"max_diameter_meters": 20, "min_duration_seconds": 20},
        {"max_diameter_meters": 40, "min_duration_seconds": 40},
    ],
    "stop_detection_persona": [
        {"max_diameter_meters": 5, "min_duration_seconds": 20},
        {"max_diameter_meters": 10, "min_duration_seconds": 30},
        {"max_diameter_meters": 20, "min_duration_seconds": 60},
        {"max_diameter_meters": 40, "min_duration_seconds": 900},
    ],
}


def make_raw_assets(asset_to_make):
    @asset(
        name=asset_to_make["asset_name"] + "_gpx",
        group_name=asset_to_make["code"],
        compute_kind="gpx",
        key_prefix=["workdir", asset_to_make["code"], asset_to_make["group"], "gpx"],
    )
    def asset_template():
        file_name = (
            "data"
            + "/"
            + asset_to_make["folder_name"]
            + "/"
            + asset_to_make["file_name"]
        )
        file_extension = os.path.splitext(asset_to_make["file_name"])[1][1:]

        if file_extension == "gpx":
            gdf = load_gpx_file(
                file_name,
                asset_to_make,
            )
        elif file_extension == "gpkg":
            gdf = load_gpkg_file(
                file_name,
                asset_to_make,
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
        group_name=asset_to_make["code"],
        compute_kind="postgres",
        ins={"asset_gpx": AssetIn(asset_to_make["asset_name"] + "_gpx")},
        io_manager_key="mobilityDb_manager",
        key_prefix=["public"],
    )
    def asset_template(asset_gpx):
        gdf = asset_gpx
        gdf["day_of_week"] = gdf["time"].dt.day_name(locale="es_EC.utf8")
        print(asset_gpx.head())

        return Output(
            value=gdf,
            metadata={
                "num_records": len(gdf),
                "preview": MetadataValue.md(gdf.head().to_markdown()),
            },
        )

    return asset_template


# def make_project_asset(parent_asset_name, asset_name, group_name, compute_kind, to_crs):
#     print("=======>", parent_asset_name, asset_name, group_name, compute_kind)

#     @asset(
#         name=asset_name,
#         group_name=group_name,
#         compute_kind=compute_kind,
#         ins={
#             "input_asset": AssetIn(
#                 key=["public", parent_asset_name],
#                 input_manager_key="mobilityDb_manager",
#             )
#         },
#         io_manager_key="mobilityDb_manager",
#         key_prefix=["public"],
#     )
#     def asset_template(input_asset):
#         # output = input_asset.to_crs("epsg:" + to_crs)
#         outpput = input.to_crs(CRS(32717))

#         output["time"] = output.index

#         return Output(
#             value=output,
#             metadata={
#                 "description": "",
#                 "rows": len(output),
#                 "preview": MetadataValue.md(output.head().to_markdown()),
#             },
#         )

#     return asset_template


def make_jupyter_explore_assets(asset_to_make):
    asset_template = define_dagstermill_asset(
        name=asset_to_make["asset_name"] + "_explore",
        notebook_path="tutorial_project/notebooks_templates/gpx_explore.ipynb",
        ins={
            "data": AssetIn(key=AssetKey(asset_to_make["asset_name"])),
        },
        group_name=asset_to_make["group"],
        key_prefix=[asset_to_make["code"], asset_to_make["group"], "raw_explore"],
    )
    return asset_template


def make_trajectory_assets(asset_to_make):
    @asset(
        name=asset_to_make["asset_name"] + "_traj",
        group_name=asset_to_make["code"],
        compute_kind="trajectory",
        ins={"asset_gpx": AssetIn(asset_to_make["asset_name"])},
        # key_prefix=["workdir"],
        io_manager_key="trajectory_collection_manager",
        key_prefix=["public"],
        metadata={"trajectory_type": "Trajectory"},
    )
    def asset_template(asset_gpx):
        gdf = asset_gpx
        traj = mpd.Trajectory(gdf, traj_id="track_id", t="time")

        traj.add_speed()
        traj.add_speed(overwrite=True, name="speed_kmh", units="km")
        traj.add_distance(overwrite=True)

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
        group_name=asset_to_make["code"],
        compute_kind="trajectory",
        ins={"traj": AssetIn(asset_to_make["asset_name"] + "_traj")},
        # key_prefix=["workdir"],
        io_manager_key="trajectory_collection_manager",
        key_prefix=["public"],
        metadata={"trajectory_type": "Trajectory"},
    )
    def asset_template(traj):
        cleaned = traj.copy()

        cleaned.add_speed(overwrite=True)

        # cleaned = mpd.OutlierCleaner(cleaned).clean({"speed_kmh": 3})
        # cleaned = mpd.OutlierCleaner(cleaned).clean({"speed": 20})

        cleaned = mpd.MinDistanceGeneralizer(cleaned).generalize(tolerance=0.0001)
        # cleaned = mpd.OutlierCleaner(cleaned).clean({"distance": 40})

        # cleaned = mpd.DouglasPeuckerGeneralizer(cleaned).generalize(tolerance=0.001)

        # cleaned = mpd.TopDownTimeRatioGeneralizer(cleaned).generalize(
        #     tolerance=0.00000001
        # )

        # for i in range(0, 10):
        #     cleaned = mpd.OutlierCleaner(cleaned).clean({"speed": 12})

        # traj_gdf = cleaned.to_point_gdf()

        print("=======>", 1)
        gdf = cleaned.to_point_gdf()
        gdf["time"] = gdf.index

        print("=======>", 2)

        df = pd.DataFrame(gdf)
        print("=======>", df.head())
        # df["datetime"] = df["time"]

        print(df.head())

        tdf = skmob.TrajDataFrame(df, latitude="lat", longitude="lon", datetime="time")
        print("=======>", 3)

        print(tdf.head())
        # ftdf = filtering.filter(
        #     tdf, max_speed_kmh=50, include_loops=True, max_loop=10, ratio_max=0.7
        # )

        # ftdf = filtering.filter(
        #     tdf,
        #     max_speed_kmh=200,
        # )

        # ftdf = tdf

        max_speed_kmh = 40
        max_loop = 10
        ratio_max = 0.5

        ftdf = filtering.filter(
            tdf,
            max_speed_kmh=max_speed_kmh,
            include_loops=True,
            max_loop=max_loop,
            ratio_max=ratio_max,
        )

        # ftdf = tdf
        # ftdf = filtering.filter(ftdf, max_speed_kmh=50, include_loops=True)
        # print("=======>", 4)

        n_deleted_points = len(tdf) - len(ftdf)  # number of deleted points
        print(n_deleted_points)
        print("=======>", 5)

        # gdf = gpd.GeoDataFrame(
        #     ftdf,
        #     geometry=gpd.points_from_xy(ftdf.lon, ftdf.lat),
        #     crs=from_epsg(4326),
        # )

        gdf2 = ftdf.to_geodataframe()

        gdf2["time"] = gdf2["datetime"]
        print("GDF", gdf2.head())

        print("GDF LEN", len(gdf2))

        traj2 = mpd.Trajectory(gdf2, traj_id="track_id", t="time")

        traj2.add_speed(overwrite=True)
        traj2.add_distance(overwrite=True)
        print("=======>", 6)

        # traj2 = mpd.OutlierCleaner(traj2).clean({"speed": 15})

        # traj2 = mpd.DouglasPeuckerGeneralizer(traj2).generalize(tolerance=0.000001)

        # cleaned = mpd.TopDownTimeRatioGeneralizer(cleaned).generalize(
        #     tolerance=0.00000001
        # )

        # cleaned = mpd.MinDistanceGeneralizer(cleaned).generalize(tolerance=0.001)

        return Output(
            value=traj2,
            metadata={
                "description": "",
                "rows": traj2.size(),
                "deleted_points": n_deleted_points,
                "max_speed_kmh": max_speed_kmh,
                "max_loop": max_loop,
                "ratio_max": ratio_max
                # "start_time": MetadataValue.text(
                #     cleaned.get_start_time().strftime("%m/%d/%Y, %H:%M:%S")
                # ),
                # "end_time": cleaned.get_end_time().strftime(("%m/%d/%Y, %H:%M:%S")),
                # "preview": MetadataValue.md(traj_gdf.head().to_markdown()),
            },
        )

    return asset_template


def make_trajectory_smooth_assets(asset_to_make):
    @asset(
        name=asset_to_make["asset_name"] + "_traj_smooth",
        group_name=asset_to_make["code"],
        compute_kind="trajectory",
        ins={"traj": AssetIn(asset_to_make["asset_name"] + "_traj_clean")},
        io_manager_key="trajectory_collection_manager",
        key_prefix=["public"],
        metadata={"trajectory_type": "Trajectory"},
    )
    def asset_template(traj):
        smoothed = mpd.KalmanSmootherCV(traj).smooth(
            process_noise_std=config["smooth"]["process_noise_std"],
            measurement_noise_std=config["smooth"]["measurement_noise_std"],
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


factory_assets_gpx = [make_raw_assets(asset) for asset in data]
factory_assets_postgres = [make_postgres_assets(asset) for asset in data]
if config["make_notebooks"]:
    factory_assets_jupyter_explore = [
        make_jupyter_explore_assets(asset) for asset in data
    ]

# factory_assets_projected = []
# for index, row in assets_df.iterrows():
#     print(row["code"], row["group"])
#     a = make_project_asset(
#         row["asset_name"], row["asset_name"] + "_proj", row["group"], "postgres", 32717
#     )
#     factory_assets_projected.append(a)


factory_assets_trajectory = [make_trajectory_assets(asset) for asset in data]
factory_assets_trajectory_clean = [
    make_trajectory_clean_assets(asset) for asset in data
]
factory_assets_trajectory_smooth = [
    make_trajectory_smooth_assets(asset) for asset in data
]

if config["make_notebooks"]:
    factory_assets_trajectory_clean_jupyter = [
        make_trajectory_clean_smooth_jupyter(asset) for asset in data
    ]


def make_trajectory_collection_asset(
    name, group_name, asset_inputs, geometry_type="point"
):
    ins = {
        f"asset{i+1}": AssetIn(key=["public", asset_input])
        for i, asset_input in enumerate(asset_inputs)
    }

    @asset(
        name=name,
        group_name=group_name,
        compute_kind="trajectory_collection",
        ins=ins,
        io_manager_key="trajectory_collection_manager",
        key_prefix=["public"],
        metadata={
            "trajectory_type": "TrajectoryCollection",
            "to_gdf_type": geometry_type,
        },
    )
    def asset_template(**kargs):
        trajectoriesList = []
        trajectoriesCollectionList = []
        for arg in kargs:
            if isinstance(kargs[arg], mpd.Trajectory):
                trajectoriesList.append(kargs[arg])
            else:
                trajectoriesCollectionList.append(kargs[arg])

        for trajectoryCollection in trajectoriesCollectionList:
            for i in range(len(trajectoryCollection)):
                trajectoriesList.append(trajectoryCollection.trajectories[i])

        traj_collection = mpd.TrajectoryCollection(
            trajectoriesList, "track_id", t="time"
        )

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
        key_prefix=["public"],
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
factory_assets_trajectory_by_date_type_db_track = []

factory_assets_trajectory_by_date_type_names = []
grouped = assets_df.groupby(["code", "group", "date", "type"])
for (code, group, date, type), group_data in grouped:
    name = group + "_traj"
    # group_name = code + "_" + date + "_" + type
    group_name = code
    inputs = []

    for index, row in group_data.iterrows():
        inputs.append(row["asset_name"] + "_traj_smooth")

    result = make_trajectory_collection_asset(
        name,
        group_name,
        inputs,
    )

    factory_assets_trajectory_by_date_type.append(result)
    # result_track = make_assets_db(
    #     group + "_traj",
    #     group + "_traj_line",
    #     code + "_" + date + "_" + type,
    #     "postgres",
    #     "line",
    # )
    # factory_assets_trajectory_by_date_type_db_track.append(result_track)


factory_assets_trajectory_by_code_type = []


all_inputs = []
factory_assets_trajectory_by_code_type = []
grouped = assets_df.groupby(["code", "type"])
for (code, type), group_data in grouped:
    grouped2 = group_data.groupby(["group"])
    inputs = []
    for (group2), group_data2 in grouped2:
        inputs.append(group2 + "_traj")

    name = code + "_" + type + "_traj"
    # group_name = code + "_" + type
    group_name = code
    result = make_trajectory_collection_asset(name, group_name, inputs)
    factory_assets_trajectory_by_code_type.append(result)

    # name_line = code + "_" + type + "_traj_line"
    # result_line = make_trajectory_collection_asset(name_line, group_name, [name])
    # result_line = make_assets_db(name, name_line, group_name, "postgres", "line")
    # factory_assets_trajectory_by_code_type_line.append(result_line)

    all_inputs.append(name)

all_assets = make_trajectory_collection_asset(
    "trajectories", "trajectories", all_inputs
)


def make_assets_by_code(codigo):
    @asset(
        name=codigo + "_line",
        group_name=codigo,
        compute_kind="postgres",
        ins={
            "trajectories_line": AssetIn(
                key=["public", "trajectories_line"],
                input_manager_key="mobilityDb_manager",
                metadata={"where": f""" \"codigo\" = \'{codigo}\'"""},
            )
        },
        io_manager_key="mobilityDb_manager",
        key_prefix=["public"],
    )
    def asset_template(trajectories_line):
        print("======>>>>", trajectories_line)
        return Output(
            value=trajectories_line,
            metadata={
                "description": "",
                "rows": len(trajectories_line),
                "preview": MetadataValue.md(trajectories_line.head().to_markdown()),
            },
        )

    return asset_template


# by_code = []
# grouped = assets_df.groupby(["code"])
# for (code), group_data in grouped:
#     r = make_assets_by_code(code)
#     by_code.append(r)


# Stop detection


def make_assets_stops(
    output_name, input_name, max_diameter_meters, min_duration_seconds, type
):
    @asset(
        name=output_name,
        group_name="trajectories",
        compute_kind="postgres",
        # ins={
        #     "trajectories": AssetIn("trajectories"),
        # },
        ins={
            "trajectories": AssetIn(
                key=["public", input_name],
                input_manager_key="trajectory_collection_manager",
            ),
            "trajectories_line": AssetIn(
                key=["public", "trajectories_line"],
                input_manager_key="mobilityDb_manager",
            ),
        },
        key_prefix=["public"],
        io_manager_key="mobilityDb_manager",
    )
    def asset_template(trajectories, trajectories_line):
        detector = mpd.TrajectoryStopDetector(trajectories)
        stops = detector.get_stop_points(
            min_duration=timedelta(seconds=min_duration_seconds),
            max_diameter=max_diameter_meters,
        )

        stops["track_id"] = stops["traj_id"]

        # gdf_traj = trajectories.to_point_gdf()
        # traj_row = gdf.loc[gdf['track_id'] == 'x']

        print("=============", trajectories_line)
        trajectories_line.index(["modalidad"])

        # merged_stops = stops.merge(
        #     trajectories_line[["track_id", "modalidad", "codigo", "fecha", "tipo"]],
        #     left_on="track_id",
        #     right_on="track_id",
        #     how="left",
        # )

        merged_stops = stops.merge(
            trajectories_line[["track_id", "modalidad"]], on="track_id", how="left"
        )

        return Output(
            value=merged_stops,
            metadata={
                "description": "",
                "rows": len(stops),
                "preview": MetadataValue.md(stops.head().to_markdown()),
                "max_diameter_meters": max_diameter_meters,
                "min_duration_seconds": min_duration_seconds,
            },
        )

    return asset_template


# stops = make_assets_stops(
#     output_name="trajectories_stops_10_10",
#     max_diameter_meters=20,
#     min_duration_seconds=120,
# )

# stops = []

# for item in config.get("stop_detection_artefacto", []):
#     stop_asset = make_assets_stops(
#         output_name="trajectories_stops_artefacto_"
#         + str(item["max_diameter_meters"])
#         + "_"
#         + str(item["min_duration_seconds"]),
#         input_name="trajectories_artefacto",
#         max_diameter_meters=item["max_diameter_meters"],
#         min_duration_seconds=item["min_duration_seconds"],
#         type="artefacto",
#     )
#     stops.append(stop_asset)
#     # stopsInputs.append()

# for item in config.get("stop_detection_persona", []):
#     stop_asset = make_assets_stops(
#         output_name="trajectories_stops_persona_"
#         + str(item["max_diameter_meters"])
#         + "_"
#         + str(item["min_duration_seconds"]),
#         input_name="trajectories_persona",
#         max_diameter_meters=item["max_diameter_meters"],
#         min_duration_seconds=item["min_duration_seconds"],
#         type="artefacto",
#     )
#     stops.append(stop_asset)
