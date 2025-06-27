import dagster as dg
import matplotlib.pyplot as plt
import geopandas as gpd
from dagster_essentials.assets import constants
from dagster_essentials.partitions import weekly_partition
from dagster_gcp import BigQueryResource
import pandas as pd


@dg.asset(
    deps=["yellow_taxi_trips", "taxi_zones"]
)
def manhattan_stats(bqr: BigQueryResource) -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.the_geom,
            count(*) as num_trips,
        from taxi_trips.yellow_trips as trips
        left join taxi_trips.zones as zones on trips.PULocationID = zones.objectid
        where borough = 'Manhattan' and the_geom is not null
        group by zone, borough, the_geom
    """

    with bqr.get_client() as client:
        trips_by_zone = client.query(query).to_dataframe()

    trips_by_zone["the_geom"] = gpd.GeoSeries.from_wkt(
        trips_by_zone["the_geom"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone, geometry="the_geom")

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())


@dg.asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="num_trips", cmap="plasma",
                       legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim([-74.05, -73.90])  # Adjust longitude range
    ax.set_ylim([40.70, 40.82])  # Adjust latitude range

    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH,
                format="png", bbox_inches="tight")
    plt.close(fig)


@dg.asset(
    deps=["yellow_taxi_trips"],
    partitions_def=weekly_partition
)
def trips_by_week(context: dg.AssetExecutionContext, bqr: BigQueryResource) -> None:
    """
      The number of trips per week, aggregated by week.
    """

    period_to_fetch = context.partition_key

    # get all trips for the week
    query = f"""
        SELECT VendorID, total_amount, trip_distance, passenger_count
        FROM taxi_trips.yellow_trips
        WHERE tpep_pickup_datetime >= TIMESTAMP('{period_to_fetch}')
          AND tpep_pickup_datetime < TIMESTAMP(DATE_ADD('{period_to_fetch}', INTERVAL 7 DAY))
    """

    with bqr.get_client() as client:
        data_for_month = client.query(query).to_dataframe()

    aggregate = data_for_month.agg({
        "VendorID": "count",
        "total_amount": "sum",
        "trip_distance": "sum",
        "passenger_count": "sum"
    }).rename({"VendorID": "num_trips"}).to_frame().T  # type: ignore

    # clean up the formatting of the dataframe
    aggregate["period"] = period_to_fetch
    aggregate['num_trips'] = aggregate['num_trips'].astype(int)
    aggregate['passenger_count'] = aggregate['passenger_count'].astype(int)
    aggregate['total_amount'] = aggregate['total_amount'].round(
        2).astype(float)
    aggregate['trip_distance'] = aggregate['trip_distance'].round(
        2).astype(float)
    aggregate = aggregate[["period", "num_trips",
                           "total_amount", "trip_distance", "passenger_count"]]

    try:
        # If the file already exists, append to it, but replace the existing month's data
        existing = pd.read_csv(constants.TRIPS_BY_WEEK_FILE_PATH)
        existing = existing[existing["period"] != period_to_fetch]
        existing = pd.concat([existing, aggregate]).sort_values(by="period")
        existing.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
    except FileNotFoundError:
        aggregate.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
