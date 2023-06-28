import pandas as pd
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


config = {
    "clean": {"speed": 0.5},
    "smooth": {
        "process_noise_std": 0.1,
        "measurement_noise_std": 10,
        "measurement_noise_std": 10,
    },
}

print("CONFIG=====", config["smooth"]["process_noise_std"])


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
            + asset_to_make["file_name"],
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
            cleaned = mpd.OutlierCleaner(cleaned).clean(
                {"speed": config["clean"]["speed"]}
            )

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


def make_trajectory_smooth_assets(asset_to_make):
    @asset(
        name=asset_to_make["asset_name"] + "_traj_smooth",
        group_name=asset_to_make["group"],
        compute_kind="trajectory",
        ins={"traj": AssetIn(asset_to_make["asset_name"] + "_traj_clean")},
        io_manager_key="trajectory_manager",
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


factory_assets_trajectory_clean_jupyter = [
    make_trajectory_clean_smooth_jupyter(asset) for asset in data
]


def make_trajectory_collection_asset(name, group_name, asset_inputs):
    ins = {
        f"asset{i+1}": AssetIn(key=[asset_input])
        for i, asset_input in enumerate(asset_inputs)
    }

    @asset(
        name=name,
        group_name=group_name,
        compute_kind="trajectory_collection",
        ins=ins,
        io_manager_key="trajectory_collection_manager",
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
    group_name = code + "_" + date + "_" + type
    inputs = []

    for index, row in group_data.iterrows():
        inputs.append(row["asset_name"] + "_traj_smooth")

    result = make_trajectory_collection_asset(name, group_name, inputs)

    factory_assets_trajectory_by_date_type.append(result)
    result_track = make_assets_db(
        group + "_traj",
        group + "_traj_line",
        code + "_" + date + "_" + type,
        "postgres",
        "line",
    )
    factory_assets_trajectory_by_date_type_db_track.append(result_track)


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
    group_name = code + "_" + type
    result = make_trajectory_collection_asset(name, group_name, inputs)
    factory_assets_trajectory_by_code_type.append(result)

    all_inputs.append(name)

all_assets = make_trajectory_collection_asset(
    "trajectories", "trajectories", all_inputs
)
