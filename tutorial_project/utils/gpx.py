import pandas as pd
import geopandas as gpd
import gpxpy
import gpxpy.gpx
import uuid
from fiona.crs import from_epsg


def load_gpx_file(file_path, asset_to_make):
    with open(file_path, "r") as gpx_file:
        gpx = gpxpy.parse(gpx_file)

    file_name = file_path.split("/")[-1]
    track_id = file_name.split(".")[0]
    route_info = []

    codigo = track_id.split("_")[0]

    for track in gpx.tracks:
        for segment in track.segments:
            for point in segment.points:
                route_info.append(
                    {
                        # 'id': uuid.uuid4(),
                        "lat": point.latitude,
                        "lon": point.longitude,
                        "elevation": point.elevation,
                        "time": point.time,
                        "file_path": file_path,
                        "file_name": file_name,
                        "track_id": asset_to_make["asset_name"],
                        "codigo": asset_to_make["code"],
                        "type": asset_to_make["type"],
                        "date": asset_to_make["date"],
                        "group": asset_to_make["group"],
                        "modalidad": asset_to_make["modalidad"],
                        "hora_inicio": asset_to_make["hora_inicio"],
                        "hora_fin": asset_to_make["hora_fin"],
                        "owner": asset_to_make["owner"],
                        "id": 1,
                    }
                )
    route_df = pd.DataFrame(route_info)
    gdf = gpd.GeoDataFrame(
        route_df,
        geometry=gpd.points_from_xy(route_df.lon, route_df.lat),
        crs=from_epsg(4326),
    )

    return gdf


def load_gpkg_file(file_path, asset_to_make):
    file_name = file_path.split("/")[-1]
    track_id = file_name.split(".")[0]
    codigo = track_id.split("_")[0]

    gdf = gpd.read_file(file_path)

    gdf = gdf[["time", "ele", "geometry"]].copy()

    gdf.rename(columns={"ele": "elevation"}, inplace=True)

    gdf["lon"] = gdf.geometry.x
    gdf["lat"] = gdf.geometry.y

    gdf["file_path"] = file_path
    gdf["file_name"] = file_name
    gdf["track_id"] = asset_to_make["asset_name"]
    gdf["codigo"] = asset_to_make["code"]
    gdf["type"] = asset_to_make["type"]
    gdf["date"] = asset_to_make["date"]
    gdf["group"] = asset_to_make["group"]
    gdf["modalidad"] = asset_to_make["modalidad"]
    gdf["hora_inicio"] = asset_to_make["hora_inicio"]
    gdf["hora_fin"] = asset_to_make["hora_fin"]
    gdf["owner"] = asset_to_make["owner"]
    gdf["id"] = 1

    if not pd.core.dtypes.common.is_datetime_or_timedelta_dtype(gdf["time"]):
        gdf["time"] = pd.to_datetime(gdf["time"], format="%Y/%m/%d %H:%M:%S.%f")

    print(gdf)

    return gdf
