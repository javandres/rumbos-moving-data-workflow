# Foursquare Studio Panel

The Foursquare Studio Panel Plugin enables you to add Foursquare Studio Maps to your Grafana dashboard.

![Plugin Preview](https://storage.googleapis.com/unfolded_public/grafana-plugin-images/plugin-preview.png)

## Features

- Visualize your geospatial data in your Grafana dashboard as:

  - Points on the map
  - A line (LineString) that connects the points based on the passed timestamp
  - [TripLayer](https://location.foursquare.com/studio/docs/layer-trip) that allows you to animate the received data based on the passed timestamp
  - A GeoJSON layer

- [Save your map configuration](#save-your-map-configuration) in the Grafana instance so you don't lose your map settings when the data refreshes.
- [Save your map in the Foursquare Cloud](#save-your-map-to-foursquare-cloud) at any time with the data from the Grafana queries

## Installation

See https://grafana.com/docs/grafana/latest/administration/plugin-management/#install-a-plugin for help with the installation process.

> Supported version of Grafana is `7.0+`

## Usage

### Displaying data as Points on the map

Visualizing the data as Points on the map doesn't require any `time` field only the location. You need to provide the values for the `latitude` and `longitude` values and you need to name these values exactly like that in the query. The order of the properties isn't important.

An example of a query for getting the latitude and longitude against a PostgreSQL (with PostGIS) database:

```sql
SELECT long AS longitude,
    lat AS latitude
FROM table_name
```

For example it would be a similar query if you are using the [geometry data type](https://postgis.net/docs/geometry.html) in PostGIS:

```sql
SELECT ST_X (ST_Transform (geom, 4326)) AS longitude,
   ST_Y (ST_Transform (geom, 4326)) AS latitude
FROM table_name
```

![Create points based on lat/lng](https://storage.googleapis.com/unfolded_public/grafana-plugin-images/points-geom-latlng.png)

If you want to visualize more properties in the tooltip of the created points, you just need to add that data to the query:

```sql
SELECT ST_X (ST_Transform (geom, 4326)) AS longitude,
       ST_Y (ST_Transform (geom, 4326)) AS latitude,
       name,
       borough,
       date_created as time
FROM nyc_subway_stations
```

![Create points based on lat/lng and show additional tooltip values](https://storage.googleapis.com/unfolded_public/grafana-plugin-images/points-latlng-tooltip.png)

### Display a line that connects the points based on the passed timestamp

Similar to the creation of Points, to create a line that connects the points you need to provide the `time` value (as Unix timestamp) in the ascending order for the respective row. Keep in mind to name the properties in the query as `latitude`, `longitude` and `time`.

To show the line you need to toggle the `Create line path from points` to `true` in the panel options.

```sql
SELECT ST_X (ST_Transform (geom, 4326)) AS longitude,
       ST_Y (ST_Transform (geom, 4326)) AS latitude,
       date_created as time
FROM nyc_subway_stations
ORDER BY time
```

![Create a line based on lat/lng and time](https://storage.googleapis.com/unfolded_public/grafana-plugin-images/line-timestamp.png)

### Animate the data using the TripLayer

If you want to leverage more advanced features of Foursquare Studio you can animate the time-based data by toggling the `Enable animation` to `true` in the panel options. To enable this option, `Create line path from points` must be set to `true` in the panel options. Same query as in the line creation should be provided.

This will create a `TripLayer` on the map and add a timeline animation control on the map. To customize this control see our [official documentation](https://location.foursquare.com/studio/docs/layer-trip).

```sql
SELECT long AS longitude,
   lat AS latitude,
   date_created as time
FROM nyc_subway_stations
ORDER BY table_name
```

![Animate a line using TripLayer](https://storage.googleapis.com/unfolded_public/grafana-plugin-images/line-trips-animate.gif)

### Display a GeoJSON object

If you are able to leverage your database to get a **single** GeoJSON object, you can provide it to the plugin to be displayed on the map. Keep in mind to name the value as `geojson_layer`.

An example of a query for getting the GeoJSON `LineString` object against a PostgreSQL (with PostGIS) database:

```sql
SELECT ST_AsGeoJSON(ST_MakeLine(ST_Transform(geom, 4326) ORDER BY date_created)) AS geojson_layer
FROM table_name
```

An example of a query for getting the custom GeoJSON feature object with selected properties against a PostgreSQL (with PostGIS) database. This query will include all properties except for `gid`, `geom` values. Keep in mind to name the value as `geojson_layer`.

```sql
SELECT jsonb_build_object(
 'type',     'FeatureCollection',
 'features', jsonb_agg(features.feature)
) as geojson_layer
FROM (
SELECT jsonb_build_object(
 'type',       'Feature',
 'id',         gid,
 'geometry',   ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
 'properties', to_jsonb(inputs) - 'gid' - 'geom'
) AS feature
FROM (SELECT * FROM nyc_subway_stations) inputs) features;
```

In case you have a GeoJSON object for each individual Point in the row, you can just create a simple query as when you are creating a regular point, just provide the `geojson` instead of the `latitude` and `longitude` values. In this case name the property value as `geojson` and **not** as `geojson_layer`.

```sql
SELECT ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson
FROM nyc_subway_stations
```

### Save your Map Configuration

Any changes made in Foursquare Studio (layers, filters, interactions, base map settings) can be saved in your Grafana instance, and they will be applied when the plugin is initialized.
For example if you changed the `Base Map Style` from `Dark` to `Light` and you want to save that setting you need to:

1.  Click the `Save` button under the `Save map configuration` section

![Save panel configuration](https://storage.googleapis.com/unfolded_public/grafana-plugin-images/grafana-panel-settings.png)

2.  Apply that change with clicking `Save` in the top right corner of your Grafana panel settings to confirm your changes

![Save map configuration](https://storage.googleapis.com/unfolded_public/grafana-plugin-images/grafana-studio-settings.png)

### Save Grafana Maps to your Foursquare Cloud Account

In case you want to save and share maps visible in your Grafana Panel, you have the ability to save a map (along with any data from the panel's queries) to your [Foursquare Studio](https://studio.foursquare.com/home) account.

Required steps to save your map:

1. Log in to your account in [Foursquare Studio](https://studio.foursquare.com/home)
2. Copy your [Access Token](https://studio.foursquare.com/tokens.html)

![Get Access Token](https://storage.googleapis.com/unfolded_public/grafana-plugin-images/auth-token.jpg)

3. Go to your Foursquare Studio Panel Plugin settings
4. Click on `Save map to Foursquare Cloud` button
5. Paste the token in the `Add Access Token` field
6. Optionally you can also add map name and description values

![Save To Cloud Config](https://storage.googleapis.com/unfolded_public/grafana-plugin-images/save-to-cloud-config.png)

If the request is successful you will get the saved map URL at the bottom of the section.

Keep in mind that the created map is still private and you would have to [publish it in Foursquare Studio](https://location.foursquare.com/studio/docs/maps-publish) to be able to share it publicly.

### Add multiple queries

Plugin allows you to add multiple queries where the result of each query will be added as a separate dataset and layer in the Foursquare Studio Panel.

![Create multiple queries](https://storage.googleapis.com/unfolded_public/grafana-plugin-images/grafana-multiple-queries.jpg)

## Learn more

- [Website](https://location.foursquare.com/products/studio/)
- [Documentation](https://location.foursquare.com/studio/docs)
