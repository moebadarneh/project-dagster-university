import requests
from dagster_gcp import BigQueryResource
from dagster_essentials.assets import constants
import dagster as dg
import os
import pandas as pd
from google.cloud import bigquery as bq
from dagster_essentials.partitions import monthly_partition
from io import BytesIO
from google.cloud import storage


@dg.asset(
    partitions_def=monthly_partition
)
def green_taxi_trips_file(context: dg.AssetExecutionContext) -> None:
    """
      The raw parquet files for the green taxi trips dataset. Sourced from the NYC Open Data portal.
    """

    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    taxi_type = "green"
    destination_blob_name = f"{taxi_type}_taxi_trips_{month_to_fetch}.parquet"

    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{month_to_fetch}.parquet", stream=True
    )
    raw_trips.raise_for_status()

    client = storage.Client()
    bucket = client.bucket(constants.GCP_BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)

    # Upload directly from the HTTP response content
    blob.upload_from_file(BytesIO(raw_trips.content),
                          content_type='application/octet-stream')


@dg.asset(
    partitions_def=monthly_partition
)
def yellow_taxi_trips_file(context: dg.AssetExecutionContext) -> None:
    """
      The raw parquet files for the yellow taxi trips dataset. Sourced from the NYC Open Data portal.
    """

    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    taxi_type = "yellow"
    destination_blob_name = f"{taxi_type}_taxi_trips_{month_to_fetch}.parquet"

    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{month_to_fetch}.parquet", stream=True
    )
    raw_trips.raise_for_status()

    client = storage.Client()
    bucket = client.bucket(constants.GCP_BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)

    # Upload directly from the HTTP response content
    blob.upload_from_file(BytesIO(raw_trips.content),
                          content_type='application/octet-stream')


@dg.asset
def taxi_zones_file() -> None:
    """
      The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv", stream=True)

    raw_zones.raise_for_status()

    destination_blob_name = "taxi_zones.csv"
    client = storage.Client()
    bucket = client.bucket(constants.GCP_BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)

    # Upload directly from the HTTP response content
    blob.upload_from_file(BytesIO(raw_zones.content),
                          content_type='application/octet-stream')


@dg.asset
def taxi_trip_bq_dataset(bqr: BigQueryResource) -> None:

    project_id = os.environ["GCP_PROJECT"]
    dataset_id = f"{project_id}.taxi_trips"

    dataset = bq.Dataset(dataset_id)

    with bqr.get_client() as client:
        try:
            # won't raise error if exists
            client.create_dataset(dataset, exists_ok=True)

        except Exception as e:
            raise RuntimeError(f"Failed to create dataset: {e}")


@dg.asset(
    deps=["green_taxi_trips_file", "taxi_trip_bq_dataset"],
    partitions_def=monthly_partition,
)
def green_taxi_trips(context: dg.AssetExecutionContext, bqr: BigQueryResource) -> None:
    """
      The raw green taxi trips dataset, loaded into BigQuery, partitioned by month.
    """

    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    client = storage.Client()
    bucket = client.bucket(constants.GCP_BUCKET_NAME)
    blob_name = f"green_taxi_trips_{month_to_fetch}.parquet"
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
    df = pd.read_parquet(BytesIO(data))

    # enforce types (to help BQ schema inference)
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    df["partition_date"] = month_to_fetch  # Add partition column

    project_id = os.environ["GCP_PROJECT"]
    table_id = f"{project_id}.taxi_trips.green_trips"

    # Ensure table exists
    try:
        with bqr.get_client() as client:
            client.get_table(table_id)
        context.log.info("Table exists.")
    except Exception as e:
        context.log.info("Creating table...")
        job = client.load_table_from_dataframe(df, table_id)
        job.result()
        context.log.info("Table created and data inserted.")
        return

    # Delete existing rows for this partition
    delete_query = f"""
    DELETE FROM `{table_id}`
    WHERE partition_date = '{month_to_fetch}'
    """
    client.query(delete_query).result()
    context.log.info(f"Deleted rows for partition: {month_to_fetch}")

    # Insert new data
    job = client.load_table_from_dataframe(
        df,
        destination=table_id,
        job_config=bq.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    context.log.info(f"Inserted {len(df)} rows for partition {month_to_fetch}")


@dg.asset(
    deps=["yellow_taxi_trips_file", "taxi_trip_bq_dataset"],
    partitions_def=monthly_partition,
)
def yellow_taxi_trips(context: dg.AssetExecutionContext, bqr: BigQueryResource) -> None:
    """
      The raw yellow taxi trips dataset, loaded into BigQuery, partitioned by month.
    """

    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    client = storage.Client()
    bucket = client.bucket(constants.GCP_BUCKET_NAME)
    blob_name = f"yellow_taxi_trips_{month_to_fetch}.parquet"
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
    df = pd.read_parquet(BytesIO(data))

    # enforce types (to help BQ schema inference)
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df["partition_date"] = month_to_fetch  # Add partition column

    project_id = os.environ["GCP_PROJECT"]
    table_id = f"{project_id}.taxi_trips.yellow_trips"

    # Ensure table exists
    try:
        with bqr.get_client() as client:
            client.get_table(table_id)
        context.log.info("Table exists.")
    except Exception as e:
        context.log.info("Creating table...")
        job = client.load_table_from_dataframe(df, table_id)
        job.result()
        context.log.info("Table created and data inserted.")
        return

    # Delete existing rows for this partition
    delete_query = f"""
    DELETE FROM `{table_id}`
    WHERE partition_date = '{month_to_fetch}'
    """
    client.query(delete_query).result()
    context.log.info(f"Deleted rows for partition: {month_to_fetch}")

    # Insert new data
    job = client.load_table_from_dataframe(
        df,
        destination=table_id,
        job_config=bq.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    context.log.info(f"Inserted {len(df)} rows for partition {month_to_fetch}")


@dg.asset(
    deps=["taxi_zones_file"]
)
def taxi_zones(bqr: BigQueryResource) -> None:
    """
      The raw taxi zones dataset, loaded into a BigQuery database from GCP bucket.
    """
    project_id = os.environ["GCP_PROJECT"]
    table_id = f"{project_id}.taxi_trips.zones"
    gcs_uri = f"gs://{constants.GCP_BUCKET_NAME}/taxi_zones.csv"

    job_config = bq.LoadJobConfig(
        source_format=bq.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    with bqr.get_client() as client:
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )
        load_job.result()
