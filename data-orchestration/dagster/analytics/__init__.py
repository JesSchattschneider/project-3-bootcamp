from dagster import Definitions
from analytics.jobs import my_snowflake_job, update_wfs_job
from analytics.resources import snowflake_resource
from analytics.assets.dbt.dbt import dbt_warehouse, dbt_warehouse_resource

defs = Definitions(
    jobs=[my_snowflake_job, update_wfs_job],
    resources={
        "snowflake_resource": snowflake_resource,
        "dbt_warehouse_resource": dbt_warehouse_resource  # Add the dbt resource

    },
    assets=[dbt_warehouse]  # Add the dbt assets
)