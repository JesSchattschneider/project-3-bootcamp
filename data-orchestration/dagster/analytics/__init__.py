from dagster import Definitions
from analytics.jobs import my_snowflake_job, update_wfs_job
from analytics.resources import snowflake_resource

defs = Definitions(
    jobs=[my_snowflake_job, update_wfs_job],
    resources={
        "snowflake_resource": snowflake_resource
    }
)