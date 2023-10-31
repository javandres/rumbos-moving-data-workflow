from dagster import (
    AssetKey,
    AssetIn,
    Output,
    asset,
    Definitions,
    multi_asset,
    AssetOut,
    SourceAsset,
)
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Connection
import os
import pandas as pd
import geopandas as gpd
from skmob.preprocessing import filtering
import skmob

url = URL.create(
    "postgresql+psycopg2",
    username=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    database=os.getenv("POSTGRES_DB"),
)
conn = create_engine(url).connect()
positionfixes_table = "_positionfixes_with_mode"


def process_track(track):
    codigo = track[0]
    track_id = track[1]

    sql = f"""
        SELECT * 
        FROM {positionfixes_table} 
        where mode = 'slow_mobility' 
        and track_id='{track_id}' 
        order by tracked_at
    """
    gdf = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col="geometry")
    gdf = gdf.set_index("tracked_at")
    gdf["time"] = gdf.index

    df = pd.DataFrame(gdf)

    tdf = skmob.TrajDataFrame(
        df,
        latitude="lat",
        longitude="lng",
        datetime="tracked_at",
        # user_id="codigo_num",
        # trajectory_id="track_id_num",
    )

    max_speed_kmh = 40
    max_loop = 50
    ratio_max = 0.5
    speed_kmh = 5

    ftdf = filtering.filter(
        tdf,
        max_speed_kmh=max_speed_kmh,
        include_loops=True,
        max_loop=max_loop,
        ratio_max=ratio_max,
        speed_kmh=speed_kmh,
    )

    n_deleted_points = len(tdf) - len(ftdf)  # number of deleted points
    print("=======> points deleted", n_deleted_points)

    cleaned_gdf = gpd.GeoDataFrame(
        ftdf, geometry=gpd.points_from_xy(ftdf.lng, ftdf.lat), crs="EPSG:4326"
    )
    cleaned_gdf.set_crs(epsg=4326, inplace=True)
    print("=======> cleaned_gdf", cleaned_gdf.head(2))

    print(1)

    print(2)

    cleaned_gdf.to_postgis(
        positionfixes_table + "_slowmobility_cleaned",
        conn,
        if_exists="append",
        schema="public",
    )

    print("Done")


def positionfixes_clean():
    @asset(
        name="test_traj",
        group_name="positionfixes_clean",
        compute_kind="trajectory",
        ins={},
        # key_prefix=["workdir"],
        # io_manager_key="trajectory_collection_manager",
        # key_prefix=["public"],
        # metadata={"trajectory_type": "Trajectory"},
    )
    def asset_template():
        tracksList = conn.execute(
            f"""
                select codigo, track_id, count(*)
                from {positionfixes_table}
                group by codigo, track_id  
            """
        )
        tracks = list(tracksList)
        size = len(tracks)
        print("====== Start processing", size, "tracks ======")
        count = 0

        conn.execute(
            f"""
                DROP TABLE IF EXISTS _positionfixes_with_mode_slowmobility_cleaned;
         """
        )

        for track in tracks:
            count += 1
            print("Processing track", track[1], "count", count, "of", size)
            process_track(track)

        print("All tracks processed.")
        return Output(
            value=[],
            metadata={
                "description": "",
            },
        )

    return asset_template


positionfixes_clean = positionfixes_clean()
