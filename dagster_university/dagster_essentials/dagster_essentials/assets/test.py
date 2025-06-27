import dagster as dg
from dagster import materialize, define_asset_job, Definitions, execute_job, DagsterInstance
from dagster_essentials.assets.trips import taxi_trips_file
from dagster_essentials.jobs import taxi_trips_file_job


def materialize_taxi_trips_file(taxi_type: str):

    taxi_trips_file_job = define_asset_job(
        "taxi_trips_file_job", selection=["taxi_trips_file"])

    defs = Definitions(
        assets=[taxi_trips_file],
        jobs=[taxi_trips_file_job],
    )

    resolved_job = defs.get_job_def("taxi_trips_file_job")

    # Get the partition definition
    partitions_def = taxi_trips_file.partitions_def
    partition_keys = partitions_def.get_partition_keys()

    # Loop through all partition keys and execute the job
    for partition_key in partition_keys:
        run_config = {
            "ops": {
                "taxi_trips_file": {
                    "config": {
                        "taxi_type": taxi_type
                    }
                }
            }
        }

        print(f"Materializing partition: {partition_key}")
        result = resolved_job.execute_in_process(
            run_config=run_config,
            partition_key=partition_key,
        )

        print(f"✅ Partition {partition_key} success = {result.success}")


materialize_taxi_trips_file("green")
materialize_taxi_trips_file("yellow")
