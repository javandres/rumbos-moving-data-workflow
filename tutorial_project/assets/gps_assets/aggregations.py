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
            + "_agg_flows_slow": AssetOut(
                key_prefix=["public"],
                io_manager_key="mobilityDb_manager",
            ),
            code
            + "_agg_clusters_slow_temp": AssetOut(
                key_prefix=["public"],
                io_manager_key="mobilityDb_manager",
            ),
            code
            + "_agg_flows_motorized": AssetOut(
                key_prefix=["public"],
                io_manager_key="mobilityDb_manager",
            ),
            code
            + "_agg_clusters_motorized_temp": AssetOut(
                key_prefix=["public"],
                io_manager_key="mobilityDb_manager",
            )
        },
    )
    def asset_template():

        sql_slow = f"""
            SELECT * 
            FROM {positionfixes_table} 
            where 
            codigo ='{code}' and type = 'persona' and mode = 'slow_mobility'
            order by tracked_at
        """

        gdf_slow = gpd.GeoDataFrame.from_postgis(sql_slow, conn, geom_col="geometry")
        gdf_slow = gdf_slow.set_index("tracked_at")
        gdf_slow["time"] = gdf_slow.index

        traj_collection_slow = mpd.TrajectoryCollection(gdf_slow, 'track_id', t="time")
        aggregator_slow = mpd.TrajectoryCollectionAggregator(traj_collection_slow, max_distance=100, min_distance=10, min_stop_duration=timedelta(minutes=5))
        clusters_slow = aggregator_slow.get_clusters_gdf()
        clusters_slow["id"] = clusters_slow.index
        print("clusters_slow", clusters_slow.head())


        flows_slow = aggregator_slow.get_flows_gdf()
        if flows_slow.empty:
            print("==================== flows_slow empty =====================")
            flows_slow = gpd.GeoDataFrame(columns=['id', 'length', 'weight', 'geometry'], geometry='geometry')
            print("flows_motorized empty", flows_slow.head())
        else:
            flows_slow['length'] = flows_slow['geometry'].length
            flows_slow = flows_slow[flows_slow['length'] < 0.005]
            flows_slow["id"] = flows_slow.index
            print("flows_slow", flows_slow.head())    

        sql_motorized = f"""
            SELECT * 
            FROM {positionfixes_table} 
            where 
            codigo ='{code}' and type = 'persona' and mode != 'slow_mobility'
            order by tracked_at
        """

        try:
            gdf_motorized = gpd.GeoDataFrame.from_postgis(sql_motorized, conn, geom_col="geometry")
            gdf_motorized = gdf_motorized.set_index("tracked_at")
            gdf_motorized["time"] = gdf_motorized.index

        
            traj_collection_motorized = mpd.TrajectoryCollection(gdf_motorized, 'track_id', t="time")
            aggregator_motorized = mpd.TrajectoryCollectionAggregator(traj_collection_motorized, max_distance=100, min_distance=10, min_stop_duration=timedelta(minutes=5))
            clusters_motorized = aggregator_motorized.get_clusters_gdf()
            clusters_motorized["id"] = clusters_motorized.index
            print("clusters_motorized", clusters_motorized.head())
            
            flows_motorized = aggregator_motorized.get_flows_gdf()
            print(flows_motorized.head())

            if flows_motorized.empty:
                print("==================== flows_motorized empty =====================")
                flows_motorized = gpd.GeoDataFrame(columns=['id', 'length', 'weight', 'geometry'], geometry='geometry')
                print("flows_motorized empty", flows_motorized.head())
            else:
                flows_motorized['length'] = flows_motorized['geometry'].length
                flows_motorized = flows_motorized[flows_motorized['length'] < 0.003]
                flows_motorized["id"] = flows_motorized.index
                print("flows_motorized", flows_motorized.head())    
        except:
            print("Error")
            flows_motorized = gpd.GeoDataFrame(columns=['id', 'length', 'weight', 'geometry'], geometry='geometry')    
            clusters_motorized = gpd.GeoDataFrame(columns=['id', 'n', 'geometry'], geometry='geometry')    

        return (
            Output(
                value=flows_slow,
                metadata={
                    "description": "",
                    "rows": len(flows_slow),
                    "preview": "",
                },
            ),
            Output(
                value=clusters_slow,
                metadata={
                    "description": "",
                    "rows": len(clusters_slow),
                    "preview": "",
                },
            ),
            Output(
                value=flows_motorized,
                metadata={
                    "description": "",
                    "rows": len(flows_motorized),
                    "preview": "",
                },
            ),
            Output(
                value=clusters_motorized,
                metadata={
                    "description": "",
                    "rows": len(clusters_motorized),
                    "preview": "",
                },
            ),
        )
    return asset_template


def make_cluster_slow_cleaned_by_code(code):
    @multi_asset(
        name=code + "_cluster_slow_cleaned",
        group_name= "aggregations",
        compute_kind="movingpandas_agg",
        ins={
            "agg_clusters": AssetIn(
                key=["public", code + "_agg_clusters_slow_temp"],
                input_manager_key="mobilityDb_manager",
            )
        },
        outs={
            code
            + "_agg_clusters_slow": AssetOut(
                key_prefix=["public"],
                io_manager_key="mobilityDb_manager",
            ),
        },
    )
    def asset_template(agg_clusters):
        gdf = clean_cluster("slow", code)

        return (
            Output(
                value=gdf,
                metadata={
                    "description": "",
                    "rows": len(gdf),
                    "preview": "",
                },
            )
           
        )
    return asset_template

def make_cluster_motorized_cleaned_by_code(code):
    @multi_asset(
        name=code + "_cluster_motorized_cleaned",
        group_name= "aggregations",
        compute_kind="movingpandas_agg",
        ins={
            "agg_clusters": AssetIn(
                key=["public", code + "_agg_clusters_motorized_temp"],
                input_manager_key="mobilityDb_manager",
            )
        },
        outs={
            code
            + "_agg_clusters_motorized": AssetOut(
                key_prefix=["public"],
                io_manager_key="mobilityDb_manager",
            ),
        },
    )
    def asset_template(agg_clusters):
 
        gdf = clean_cluster("motorized", code)

        return (
            Output(
                value=gdf,
                metadata={
                    "description": "",
                    "rows": len(gdf),
                    "preview": "",
                },
            )
           
        )
    return asset_template

def clean_cluster(type, code):
    flow_table = code + "_agg_flows_" + type
    cluster_table = code + "_agg_clusters_" + type + "_temp" 
    sql = f"""
        select c.*  
        from (
            select distinct c.id 
            from "{flow_table}" f join "{cluster_table}" c
            on ST_intersects(c.geometry, f.geometry ) 
        ) a, "{cluster_table}" c
        where a.id = c.id
    """

    gdf = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col="geometry")
    # gdf = gdf.set_index("tracked_at")
    # gdf["time"] = gdf.index

    return gdf

tracksList = conn.execute(
    f"""
        select codigo, count(*)
        from {positionfixes_table}
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
    agg_clusters_slow_cleaned = make_cluster_slow_cleaned_by_code(track[0])
    aggregations_assets.append(agg_clusters_slow_cleaned)
    agg_clusters_motorized_cleaned = make_cluster_motorized_cleaned_by_code(track[0])
    aggregations_assets.append(agg_clusters_motorized_cleaned)


print("All aggregation assets loaded.")


