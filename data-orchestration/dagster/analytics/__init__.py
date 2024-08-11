from dagster import Definitions
from analytics.jobs import my_snowflake_job, update_wfs_job, pull_environ_data
from analytics.resources import snowflake_resource

defs = Definitions(
    jobs=[my_snowflake_job, update_wfs_job, pull_environ_data],
    resources={
        "snowflake_resource": snowflake_resource
    }
)