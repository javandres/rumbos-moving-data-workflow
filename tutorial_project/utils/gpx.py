import pandas as pd
import geopandas as gpd
import gpxpy
import gpxpy.gpx
import uuid
from fiona.crs import from_epsg


def load_gpx_file(file_path):
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
                        "fila_name": file_name,
                        "track_id": track_id,
                        "codigo": codigo,
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
