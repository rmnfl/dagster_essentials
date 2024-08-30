from dagster import asset

import plotly.express as px
import plotly.io as pio
import geopandas as gpd
import pandas as pd
import datetime

import duckdb
import os

from . import constants

@asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats() -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

@asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig = px.choropleth_mapbox(trips_by_zone,
        geojson=trips_by_zone.geometry.__geo_interface__,
        locations=trips_by_zone.index,
        color='num_trips',
        color_continuous_scale='Plasma',
        mapbox_style='carto-positron',
        center={'lat': 40.758, 'lon': -73.985},
        zoom=11,
        opacity=0.7,
        labels={'num_trips': 'Number of Trips'}
    )

    pio.write_image(fig, constants.MANHATTAN_MAP_FILE_PATH)

@asset(
    deps=["taxi_trips", "taxi_zones"]
)
def trips_by_week() -> None:
    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))

    min_date = conn.execute("select min(pickup_datetime::date) from trips").fetchone()[0]
    max_date = conn.execute("select max(pickup_datetime::date) from trips").fetchone()[0]

    result = pd.DataFrame()

    for current_date in [min_date + datetime.timedelta(days=x) for x in range(0, (max_date-min_date).days, 7)]:
        current_date_str = current_date.strftime(constants.DATE_FORMAT)
        query = f"""
            select 
                date_trunc('week', pickup_datetime) + interval '6 day' as period,
                count(vendor_id) as num_trips,
                sum(passenger_count) as passenger_count,
                sum(trip_distance) as trip_distance,
                sum(total_amount) as total_amount
            from trips
            where date_trunc('week', pickup_datetime) = date_trunc('week', '{current_date_str}'::date)
            group by period
        """
        week_data = conn.execute(query).fetch_df()

        result = pd.concat([result, week_data])

    result['num_trips'] = result['num_trips'].astype('int')
    result['passenger_count'] = result['passenger_count'].astype('int')
    result['total_amount'] = result['total_amount'].round(2).astype(float)
    result['trip_distance'] = result['trip_distance'].round(2).astype(float)
    result = result.sort_values(by="period")

    result.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
