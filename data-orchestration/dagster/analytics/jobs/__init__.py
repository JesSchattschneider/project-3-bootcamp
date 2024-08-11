from dagster import job, static_partitioned_config, daily_partitioned_config
from analytics.resources import snowflake_resource
from analytics.ops import get_one
from analytics.ops.site_list import process_wfs_data
from analytics.ops.environ_data import pull_lwq_data
from datetime import datetime


@job(resource_defs={"snowflake_resource": snowflake_resource})
def my_snowflake_job():
    get_one()

# Partition configuration
COUNCILS = [
    "ecan",
    "gdc",
]

# Define any additional modules or variables if needed
MODULES = [
    "lwq", 
    "swq", 
    "mac"
]

# Define static partitioned config
@static_partitioned_config(partition_keys=COUNCILS)
def council_config(partition_key: str):
    return {
        "ops": {
            "process_wfs_data": {
                "inputs": {
                    "modules": MODULES, 
                }
            }
        }
    }

# Define daily partitioned job
@daily_partitioned_config(start_date=datetime(2024, 1, 1))
def env_data_etl_daily_partition(start: datetime, _end: datetime):
    return {
        "ops": {
            "pull_lwq_data": {
                "config": {
                    "date_start": start.strftime("%Y-%m-%d")
                }
            }
        }
    }

@job(config=council_config, resource_defs={"snowflake_resource": snowflake_resource})
def update_wfs_job():
    process_wfs_data()

@job(config=env_data_etl_daily_partition, resource_defs={"snowflake_resource": snowflake_resource})
def pull_environ_data():
    pull_lwq_data()