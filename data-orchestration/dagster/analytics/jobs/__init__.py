from dagster import job, static_partitioned_config
from analytics.resources import snowflake_resource
from analytics.ops import get_one
from analytics.ops.site_list import process_wfs_data

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

@job(config=council_config, resource_defs={"snowflake_resource": snowflake_resource})
def update_wfs_job():
    process_wfs_data()