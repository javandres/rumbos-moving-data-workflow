import pandas as pd
import geopandas as gpd
import gpxpy
import gpxpy.gpx
import uuid
from fiona.crs import from_epsg
import rasterio
from rasterio.transform import from_origin
from pyproj import Proj, transform

dem_path_cuenca = "tutorial_project/utils/ecu_dem/dem.tif"
dem_path_ec = "tutorial_project/utils/ecu_dem/dem_ec.tif"

dem_cuenca_dataset = rasterio.open(dem_path_cuenca)
print(
    "====================================",
    dem_cuenca_dataset,
    dem_cuenca_dataset.crs,
    dem_cuenca_dataset.bounds,
)
dem_ec_dataset = rasterio.open(dem_path_ec)
in_proj = Proj(init="epsg:4326")  # WGS 84 4326
out_proj_cuenca = Proj(dem_cuenca_dataset.crs)
out_proj_ec = Proj(dem_ec_dataset.crs)
# out_proj = Proj(init="epsg:32717")  # 32717


def get_elevation(raster_dataset, out_proj, longitude, latitude):
    # Reproject the input coordinates to match the DEM's CRS
    x, y = transform(in_proj, out_proj, longitude, latitude)
    row, col = raster_dataset.index(x, y)

    # Read the elevation value from the DEM
    elevation = raster_dataset.read(1, window=((row, row + 1), (col, col + 1)))

    elevation = elevation[0][0]
    if elevation == 0.0:
        raise Exception("No elevation data provided")
    return elevation


def get_elevation_by_coordinates(ciudad, latitude, longitude):
    if ciudad == "Cuenca":
        raster_dataset = dem_cuenca_dataset
        out_proj = out_proj_cuenca
        dem_path = dem_path_cuenca
    else:
        raster_dataset = dem_ec_dataset
        out_proj = out_proj_ec
        dem_path = dem_path_ec
    try:
        elevation = get_elevation(raster_dataset, out_proj, longitude, latitude)
        return elevation

    except:
        elevation = get_elevation(dem_ec_dataset, out_proj_ec, longitude, latitude)
        return elevation


def load_gpx_file(file_path, asset_to_make):
    print(3.1, file_path)
    with open(file_path, "r") as gpx_file:
        gpx = gpxpy.parse(gpx_file)

    print(3.2, gpx)
    file_name = file_path.split("/")[-1]
    print(3.3, file_name)
    track_id = file_name.split(".")[0]
    print(3.4, track_id)
    route_info = []


    codigo = track_id.split("_")[0]
    print(3.5, codigo)

    for track in gpx.tracks:
        for segment in track.segments:
            for point in segment.points:
                elevation = get_elevation_by_coordinates(
                    asset_to_make["ciudad"], point.latitude, point.longitude
                )

                route_info.append(
                    {
                        "lat": point.latitude,
                        "lon": point.longitude,
                        "elevation": elevation,
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
                        "ciudad": asset_to_make["ciudad"],
                        "id": 1,
                    }
                )
    route_df = pd.DataFrame(route_info)
    print(3.6, route_df)
    gdf = gpd.GeoDataFrame(
        route_df,
        geometry=gpd.points_from_xy(route_df.lon, route_df.lat),
        crs=from_epsg(4326),
    )
    print(3.7, gdf)

    return gdf


def load_gpkg_file(file_path, asset_to_make):
    file_name = file_path.split("/")[-1]
    track_id = file_name.split(".")[0]
    codigo = track_id.split("_")[0]

    gdf = gpd.read_file(file_path)

    gdf = gdf[["time", "geometry"]].copy()

    gdf["lon"] = gdf.geometry.x
    gdf["lat"] = gdf.geometry.y

    gdf["elevation"] = gdf.apply(
        lambda row: get_elevation_by_coordinates(
            asset_to_make["ciudad"], row["lat"], row["lon"]
        ),
        axis=1,
    )
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
    gdf["ciudad"] = asset_to_make["ciudad"]
    gdf["id"] = 1

    if not pd.core.dtypes.common.is_datetime_or_timedelta_dtype(gdf["time"]):
        gdf["time"] = pd.to_datetime(gdf["time"], format="%Y/%m/%d %H:%M:%S.%f")

    new_gdf = gdf[
        [
            "lat",
            "lon",
            "elevation",
            "time",
            "file_path",
            "file_name",
            "track_id",
            "codigo",
            "type",
            "date",
            "group",
            "modalidad",
            "hora_inicio",
            "hora_fin",
            "owner",
            "ciudad",
            "id",
            "geometry",
        ]
    ].copy()

    return new_gdf
