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
import movingpandas as mpd
from datetime import datetime, time, timedelta



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

@multi_asset(
    name="all_aggregations",
    group_name= "aggregations",
    compute_kind="movingpandas_agg",
    outs={
        "agg_flows": AssetOut(
            key_prefix=["public"],
            io_manager_key="mobilityDb_manager",
        ),
        "agg_clusters": AssetOut(
            key_prefix=["public"],
            io_manager_key="mobilityDb_manager",
        )
    },
)
def asset_template():

    sql = f"""
        SELECT * 
        FROM {positionfixes_table} 
        where type = 'persona'
        order by tracked_at
    """
    gdf = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col="geometry")
    gdf = gdf.set_index("tracked_at")
    gdf["time"] = gdf.index

    traj_collection = mpd.TrajectoryCollection(gdf, 'track_id', t="time")
    aggregator = mpd.TrajectoryCollectionAggregator(traj_collection, max_distance=100, min_distance=10, min_stop_duration=timedelta(minutes=5))
    clusters = aggregator.get_clusters_gdf()
    print(clusters.head())
    flows = aggregator.get_flows_gdf()
    print(flows.head())    
    return (
        Output(
            value=flows,
            metadata={
                "description": "",
                "rows": 0,
                "preview": "",
            },
        ),
        Output(
            value=clusters,
            metadata={
                "description": "",
                "rows": 0,
                "preview": "",
            },
        ),
    )


def make_aggregations_by_code(code):
    @multi_asset(
        name=code + "_aggregations",
        group_name= "aggregations",
        compute_kind="movingpandas_agg",
        outs={
            code
            + "_agg_flows": AssetOut(
                key_prefix=["public"],
                io_manager_key="mobilityDb_manager",
            ),
            code
            + "_agg_clusters": AssetOut(
                key_prefix=["public"],
                io_manager_key="mobilityDb_manager",
            )
        },
    )
    def asset_template():

        # sql = f"""
        #     SELECT * 
        #     FROM {positionfixes_table} 
        #     where 
        #     codigo ='{code}' and type = 'persona'
        #     order by tracked_at
        # """


        positionfixes_with_changes_query = f"""
            SELECT 
                *,
                SUM(change_flag) OVER (ORDER BY id) AS change
            FROM (
                SELECT 
                    *,
                    CASE 
                        WHEN mode != lag(mode) OVER (ORDER BY id) THEN 1 
                        ELSE 0 
                    END AS change_flag 
                FROM { positionfixes_table} 
                    where codigo = '{code}'
                    order by tracked_at
            ) AS subquery
        """

        change_groups_query = f"""
            select change, mode, count(*) as count
            from (
                { positionfixes_with_changes_query }
            )a
            group by change, mode
            order by change
        """

        change_groups = pd.read_sql(change_groups_query, conn)
        print(change_groups)

        clusters = None
        flows = None

        count = 0
        for index, row in change_groups.iterrows():
            print("Change group", row["change"])
            sql = f"""
                select * 
                from (
                    { positionfixes_with_changes_query }
                )a
                where change = {row["change"]}
            """

            gdf = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col="geometry")
            gdf = gdf.set_index("tracked_at")
            gdf["time"] = gdf.index
            

            print("??????????????????", gdf.length)

            traj_collection = mpd.TrajectoryCollection(gdf, 'track_id', t="time")
            aggregator = mpd.TrajectoryCollectionAggregator(traj_collection, max_distance=100, min_distance=10, min_stop_duration=timedelta(minutes=5))
            clusters_by_change = aggregator.get_clusters_gdf()
            clusters_by_change["mode"] = row["mode"]
            print("CLUSTERS:", clusters_by_change.head())
            flows_by_change = aggregator.get_flows_gdf()
            flows_by_change["mode"] = row["mode"]
            print("FLOWS", flows_by_change.head())    

            clusters = clusters_by_change if clusters is None else clusters.append(clusters_by_change)
            flows = flows_by_change if flows is None else flows.append(flows_by_change)

            count += 1
            # if count > 3:
            #     break

        # raise Exception("Stop here")

        # gdf = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col="geometry")
        # gdf = gdf.set_index("tracked_at")
        # gdf["time"] = gdf.index

        # traj_collection = mpd.TrajectoryCollection(gdf, 'track_id', t="time")
        # aggregator = mpd.TrajectoryCollectionAggregator(traj_collection, max_distance=100, min_distance=10, min_stop_duration=timedelta(minutes=5))
        # clusters = aggregator.get_clusters_gdf()
        # print(clusters.head())
        # flows = aggregator.get_flows_gdf()
        # print(flows.head())    
        return (
            Output(
                value=flows,
                metadata={
                    "description": "",
                    "rows": 0,
                    "preview": "",
                },
            ),
            Output(
                value=clusters,
                metadata={
                    "description": "",
                    "rows": 0,
                    "preview": "",
                },
            ),
        )
    return asset_template


tracksList = conn.execute(
    f"""
        select codigo, count(*)
        from {positionfixes_table}
        where codigo = 'LH52'
        group by codigo  
    """
)
tracks = list(tracksList)
size = len(tracks)
print("====== Start processing", size, "tracks ======")
count = 0

aggregations_assets = []

for track in tracks:
    count += 1
    print("Processing track", track[0], "count", count, "of", size)
    agg_assets = make_aggregations_by_code(track[0])
    aggregations_assets.append(agg_assets)


print("All aggregation assets loaded.")


