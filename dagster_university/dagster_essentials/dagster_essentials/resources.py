from dagster_duckdb import DuckDBResource
from dagster_gcp import BigQueryResource
import dagster as dg

database_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE")      # replaced with environment variable
)

bq_resource = BigQueryResource(
    project=dg.EnvVar("GCP_PROJECT")
    ) 