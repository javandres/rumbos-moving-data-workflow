from dagster import AssetOut, multi_asset

import pandas as pd
import requests

import gpxpy
import gpxpy.gpx
import geopandas as gpd
import hashlib
import numpy as np

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
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Connection
from datetime import datetime, time, timedelta

import trackintel as ti


data = []

file_paths_test = [
    "tutorial_project/assets/gps_assets/solo_gps/gpx_LQ02.json",
]

file_paths_diagnostico = [
    "tutorial_project/assets/gps_assets/diarios_viaje/gpx_AF79.json",
    "tutorial_project/assets/gps_assets/diarios_viaje/gpx_AT87.json",
    "tutorial_project/assets/gps_assets/diarios_viaje/gpx_CD87.json",
    "tutorial_project/assets/gps_assets/diarios_viaje/gpx_DS76.json",
    "tutorial_project/assets/gps_assets/diarios_viaje/gpx_LH52.json",
    "tutorial_project/assets/gps_assets/diarios_viaje/gpx_MG91.json",
    "tutorial_project/assets/gps_assets/diarios_viaje/gpx_MJ63.json",
    "tutorial_project/assets/gps_assets/diarios_viaje/gpx_MC59.json",
    "tutorial_project/assets/gps_assets/diarios_viaje/gpx_ML43.json",
    "tutorial_project/assets/gps_assets/diarios_viaje/gpx_MQ70.json",
    "tutorial_project/assets/gps_assets/diarios_viaje/gpx_MV43.json",
    "tutorial_project/assets/gps_assets/diarios_viaje/gpx_MZ49.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_BZ14.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_CL74.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_CN83.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_JC73.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_LO71.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_LQ02.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_LQ07.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_MC30.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_MG17.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_ML09.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_ML24.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_ML72.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_MP88.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_MV79.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_RC57.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_RV07.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_RY43.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_RZ63.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_TQ38.json",
    "tutorial_project/assets/gps_assets/solo_gps/gpx_TV55.json",
]

file_paths_piloto1 = [
    "tutorial_project/assets/gps_assets/piloto/piloto_1/gpx_AT87.json",
    "tutorial_project/assets/gps_assets/piloto/piloto_1/gpx_JA21.json",
    "tutorial_project/assets/gps_assets/piloto/piloto_1/gpx_LH52.json",
]

file_paths_piloto2 = [
    "tutorial_project/assets/gps_assets/piloto/piloto_2/gpx_AT87.json",
    "tutorial_project/assets/gps_assets/piloto/piloto_2/gpx_JA21.json",
    "tutorial_project/assets/gps_assets/piloto/piloto_2/gpx_LH52.json",
]

file_paths_piloto3 = [
    "tutorial_project/assets/gps_assets/piloto/piloto_3/gpx_AT87.json",
    "tutorial_project/assets/gps_assets/piloto/piloto_3/gpx_JA21.json",
    "tutorial_project/assets/gps_assets/piloto/piloto_3/gpx_LH52.json",
]

file_paths = file_paths_diagnostico

for file_path in file_paths:
    with open(file_path, "r") as read_file:
        json_data = json.load(read_file)
        for x in json_data["assets"]:
            if not "code" in x:
                x["code"] = json_data["code"]
            if not "folder_name" in x:
                x["folder_name"] = json_data["folder_name"]
            if not "owner" in x:
                x["owner"] = json_data["owner"]
            if not "modalidad" in x:
                x["modalidad"] = json_data["modalidad"]
            if not "hora_inicio" in x:
                x["hora_inicio"] = json_data["hora_inicio"]
            if not "hora_fin" in x:
                x["hora_fin"] = json_data["hora_fin"]
            if not "ciudad" in x:
                x["ciudad"] = json_data["ciudad"]

            data.append(x)

assets_df = pd.DataFrame(data)


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
        print(1)
        file_name = (
            "data"
            + "/"
            + asset_to_make["folder_name"]
            + "/"
            + asset_to_make["file_name"]
        )
        print(2, file_name)
        file_extension = os.path.splitext(asset_to_make["file_name"])[1][1:]
        print(3, file_extension)
        if file_extension == "gpx":
            gdf = load_gpx_file(file_name, asset_to_make)
        elif file_extension == "gpkg":
            gdf = load_gpkg_file(file_name, asset_to_make)

        print(4, gdf.head())
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
        print("=======>", asset_gpx["time"])
        gdf = asset_gpx
        # gdf["time"] = gdf["time"].dt.tz_localize("UTC")
        # gdf["time"] = pd.to_datetime(gdf["time"]).dt.tz_localize("UTC")
        # gdf = gdf.tz_convert("UTC")
        # gdf.to_datetime(gdf, unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
        # gdf["time"] = gdf["time"].dt.tz_convert("US/Eastern")
        # gdf["time"] = gdf["time"].dt.tz_convert("UTC")
        # gdf["time"] = (
        #     gdf["time"]
        #     .dt.tz_convert("Etc/GMT+5")
        #     .dt.strftime("%Y-%m-%d %H:%M:%S-05:00")
        # )
        gdf["time"] = gdf["time"].dt.tz_localize(None)
        gdf["time"] = gdf["time"].dt.tz_localize("America/Guayaquil")

        gdf["day_of_week"] = gdf["time"].dt.day_name(locale="es_EC.utf8")
        print(gdf["time"])

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
        key_prefix=[asset_to_make["code"], asset_to_make["group"], "raw_explore"],
    )
    return asset_template


def filter_by_time_assets(asset_to_make):
    @asset(
        name=asset_to_make["asset_name"] + "_filtered",
        group_name=asset_to_make["code"],
        compute_kind="postgres",
        ins={"asset_gpx": AssetIn(asset_to_make["asset_name"])},
        io_manager_key="mobilityDb_manager",
        key_prefix=["public"],
    )
    def asset_template(asset_gpx):
        gdf = asset_gpx
        gdf["time"] = pd.to_datetime(gdf["time"])
        if gdf["time"].dt.tz is None:
            gdf["time"] = gdf["time"].dt.tz_localize("UTC")
        gdf["time"] = gdf["time"].dt.tz_convert("America/Guayaquil")

        first_row = gdf.iloc[0]
        start_time = first_row["hora_inicio"] or "00:00:00"
        end_time = first_row["hora_fin"] or "23:59:59"
        track_id = first_row["track_id"]
        group = first_row["group"]

        filtered_gdf = (
            gdf.set_index("time").between_time(start_time, end_time).reset_index()
        )

        filters_df = pd.read_csv("./data/filters_v2.csv")
        filters_df["desde"] = pd.to_datetime(
            filters_df["desde"], format="%H:%M:%S"
        ).dt.time
        filters_df["hasta"] = pd.to_datetime(
            filters_df["hasta"], format="%H:%M:%S"
        ).dt.time

        filters_df = filters_df[filters_df["track_id"] == group]

        for index, row in filters_df.iterrows():
            to_filter = (
                filtered_gdf.set_index("time")
                .between_time(row["desde"], row["hasta"])
                .reset_index()
            )

            filtered_gdf = filtered_gdf[~filtered_gdf["time"].isin(to_filter["time"])]

        return Output(
            value=filtered_gdf,
            metadata={
                "description": f"Filtered asset for {asset_to_make['asset_name']}",
                "rows": len(filtered_gdf),
                "duration": "{}".format(
                    filtered_gdf.time.max() - filtered_gdf.time.min()
                ),
                "preview": MetadataValue.md(filtered_gdf.head().to_markdown()),
            },
        )

    return asset_template


def make_trajectory_assets(asset_to_make):
    @asset(
        name=asset_to_make["asset_name"] + "_traj",
        group_name=asset_to_make["code"],
        compute_kind="trajectory",
        ins={"asset_gpx": AssetIn(asset_to_make["asset_name"] + "_filtered")},
        io_manager_key="trajectory_collection_manager",
        key_prefix=["public"],
        metadata={"trajectory_type": "Trajectory"},
    )
    def asset_template(asset_gpx):
        gdf = asset_gpx
        gdf["time"] = gdf["time"].dt.tz_convert("America/Guayaquil")

        traj = mpd.Trajectory(gdf, traj_id="track_id", t="time")
        traj.add_speed()
        traj.add_speed(overwrite=True, name="speed_kmh", units=("km", "h"))
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

        cleaned = mpd.MinDistanceGeneralizer(cleaned).generalize(tolerance=0.0001)
        gdf = cleaned.to_point_gdf()
        gdf["time"] = gdf.index
        df = pd.DataFrame(gdf)
        tdf = skmob.TrajDataFrame(df, latitude="lat", longitude="lon", datetime="time")

        max_speed_kmh = 100
        max_loop = 150
        ratio_max = 0.99
        speed_kmh = 4

        ftdf = filtering.filter(
            tdf,
            max_speed_kmh=max_speed_kmh,
            include_loops=True,
            max_loop=max_loop,
            ratio_max=ratio_max,
            speed_kmh=speed_kmh,
        )

        n_deleted_points = len(tdf) - len(ftdf)  # number of deleted points

        gdf2 = ftdf.to_geodataframe()

        gdf2["time"] = gdf2["datetime"]
        traj2 = mpd.Trajectory(gdf2, traj_id="track_id", t="time")

        traj2.add_speed(overwrite=True)
        traj2.add_distance(overwrite=True)

        return Output(
            value=traj2,
            metadata={
                "description": "",
                "rows": traj2.size(),
                "deleted_points": n_deleted_points,
                "max_speed_kmh": max_speed_kmh,
                "max_loop": max_loop,
                "ratio_max": ratio_max,
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


factory_assets_filter_time = [filter_by_time_assets(asset) for asset in data]

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


def make_positionfixes(code, input, output, type):
    @multi_asset(
        name=output,
        group_name=code,
        compute_kind="trackintel",
        ins={
            "trajectories": AssetIn(
                key=["public", input],
                input_manager_key="mobilityDb_manager",
            )
        },
        outs={
            output
            + "_positionfixes": AssetOut(
                key_prefix=["public"],
                io_manager_key="mobilityDb_manager",
            ),
            output
            + "_staypoints": AssetOut(
                key_prefix=["public"],
                io_manager_key="mobilityDb_manager",
            ),
            output
            + "_locations": AssetOut(
                key_prefix=["public"],
                io_manager_key="mobilityDb_manager",
            ),
            output
            + "_triplegs": AssetOut(
                key_prefix=["public"],
                io_manager_key="mobilityDb_manager",
            ),
        },
    )
    def asset_template(trajectories):
        # Create positionfixes
        pfs = ti.io.read_positionfixes_gpd(
            trajectories,
            geom_col="geometry",
            tracked_at="time",
            user_id="group_num",
        )

        print("======", pfs)

        STAYPOINTS_TIME_THRESHOLD = 0.25

        # Create staypoints
        pfs, staypoints = pfs.as_positionfixes.generate_staypoints(
            method="sliding",
            dist_threshold=5,
            time_threshold=STAYPOINTS_TIME_THRESHOLD,
            include_last=False,
        )

        # Add a flag whether or not a staypoint is considered an activity.
        staypoints = ti.analysis.labelling.create_activity_flag(
            staypoints,
            method="time_threshold",
            time_threshold=0.25,
            activity_column_name="is_activity_15s",
        )

        staypoints = ti.analysis.labelling.create_activity_flag(
            staypoints,
            method="time_threshold",
            time_threshold=0.5,
            activity_column_name="is_activity_30s",
        )

        staypoints = ti.analysis.labelling.create_activity_flag(
            staypoints,
            method="time_threshold",
            time_threshold=1,
            activity_column_name="is_activity_60s",
        )

        staypoints["duration"] = staypoints["finished_at"] - staypoints["started_at"]

        staypoints["duration_seconds"] = (
            staypoints["finished_at"] - staypoints["started_at"]
        ).dt.total_seconds()

        staypoints["duration_minutes"] = staypoints["duration_seconds"] / 60

        # Generate locations https://trackintel.readthedocs.io/en/latest/modules/model.html#trackintel.model.staypoints.Staypoints.generate_locations
        staypoints, locs = staypoints.as_staypoints.generate_locations(
            method="dbscan", epsilon=100, num_samples=10
        )

        # Create triplegs
        pfs, triplegs = pfs.as_positionfixes.generate_triplegs(
            staypoints=staypoints,
            method="between_staypoints",
            gap_threshold=5,  # minutes
        )

        custom_categories = {
            2.2: "slow_mobility",  # ~8 km/h as slow_mobility
            np.inf: "motorized_mobility",  # >8 km/h as motorized_mobility
        }
        triplegs = triplegs.as_triplegs.predict_transport_mode(
            method="simple-coarse", categories=custom_categories
        )
        staypoints["type"] = type
        staypoints["codigo"] = code

        locs["type"] = type
        locs["codigo"] = code

        triplegs["type"] = type
        triplegs["codigo"] = code
        triplegs["duration"] = triplegs["finished_at"] - triplegs["started_at"]
        triplegs["duration_seconds"] = (
            triplegs["finished_at"] - triplegs["started_at"]
        ).dt.total_seconds()
        triplegs["duration_minutes"] = triplegs["duration_seconds"] / 60

        staypoints["id"] = staypoints.index
        triplegs["id"] = triplegs.index

        return (
            Output(
                value=pfs,
                metadata={
                    "description": "",
                    "rows": len(pfs),
                    "preview": MetadataValue.md(pfs.head().to_markdown()),
                },
            ),
            Output(
                value=staypoints,
                metadata={
                    "description": "",
                    "rows": len(staypoints),
                    "preview": MetadataValue.md(staypoints.head().to_markdown()),
                },
            ),
            Output(
                value=locs,
                metadata={
                    "description": "",
                    "rows": len(locs),
                    "preview": MetadataValue.md(locs.head().to_markdown()),
                },
            ),
            Output(
                value=triplegs,
                metadata={
                    "description": "Trackintel triplegs",
                    "rows": len(triplegs),
                    "preview": MetadataValue.md(triplegs.head().to_markdown()),
                },
            ),
        )

    return asset_template


def make_asset_union_tables(name, group_name, asset_inputs):
    ins = {
        f"asset{i+1}": AssetIn(key=["public", asset_input])
        for i, asset_input in enumerate(asset_inputs)
    }

    @asset(
        name=name,
        group_name=group_name,
        compute_kind="postgres",
        key_prefix=["public"],
    )
    def asset_template(**kargs):
        select = ""
        count = 0
        for arg in asset_inputs:
            union = "UNION"
            if count == len(asset_inputs) - 1 or len(asset_inputs) == 1:
                union = ""
            select = select + f' SELECT * FROM public."{arg}" {union} '
            count = count + 1

        url = URL.create(
            "postgresql+psycopg2",
            username=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
        )
        conn = create_engine(url).connect()

        output = conn.execute(
            f"""
                DROP TABLE IF EXISTS {name};
                CREATE TABLE {name} as
                    {select}
            """
        )

        return Output(
            value=[],
            metadata={
                "description": "",
            },
        )

    return asset_template


factory_assets_trajectory_by_code_type = []


all_inputs = []
factory_assets_trajectory_by_code_type = []
trackintel_positionfixes = []
positionfixes_inputs = []
staypoints_inputs = []
locations_inputs = []
triplegs_inputs = []

grouped = assets_df.groupby(["code", "type"])
for (code, type), group_data in grouped:
    grouped2 = group_data.groupby(["group"])
    inputs = []
    for (group2), group_data2 in grouped2:
        inputs.append(group2 + "_traj")

    name = code + "_" + type + "_traj"
    group_name = code
    result = make_trajectory_collection_asset(name, group_name, inputs)
    factory_assets_trajectory_by_code_type.append(result)

    positionfixes = make_positionfixes(code, name, name + "_trackintel", type)
    trackintel_positionfixes.append(positionfixes)
    positionfixes_inputs.append(name + "_trackintel_positionfixes")
    staypoints_inputs.append(name + "_trackintel_staypoints")
    locations_inputs.append(name + "_trackintel_locations")
    triplegs_inputs.append(name + "_trackintel_triplegs")

    all_inputs.append(name)

all_assets = make_trajectory_collection_asset(
    "trajectories", "trajectories", all_inputs
)

positionfixes_asset = make_asset_union_tables(
    "_positionfixes", "trackintel", positionfixes_inputs
)

staypoints_asset = make_asset_union_tables(
    "_staypoints", "trackintel", staypoints_inputs
)

locations_asset = make_asset_union_tables("_locations", "trackintel", locations_inputs)

triplegs_asset = make_asset_union_tables("_triplegs", "trackintel", triplegs_inputs)


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
        return Output(
            value=trajectories_line,
            metadata={
                "description": "",
                "rows": len(trajectories_line),
                "preview": MetadataValue.md(trajectories_line.head().to_markdown()),
            },
        )

    return asset_template


def make_assets_stops(
    output_name, input_name, max_diameter_meters, min_duration_seconds, type
):
    @asset(
        name=output_name,
        group_name="trajectories",
        compute_kind="postgres",
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
        trajectories_line.index(["modalidad"])
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
