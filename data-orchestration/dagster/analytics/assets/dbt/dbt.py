import os
from pathlib import Path
from dagster_dbt import DbtCliResource, dbt_assets
from dagster import AssetExecutionContext

# Corrected path to point to the correct dbt project directory
dbt_project_dir = Path(__file__).joinpath("..", "..", "..", "..", "warehouse_snowflake").resolve()
print(f"dbt_project_dir: {dbt_project_dir}")

# Initialize the DbtCliResource with the dbt project directory
dbt_warehouse_resource = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

# Path to the manifest.json file in the dbt target directory
dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")
print(f"dbt_manifest_path: {dbt_manifest_path}")

# Check if the manifest.json file exists and raise an error if not
if not dbt_manifest_path.exists():
    raise FileNotFoundError(f"manifest.json does not exist at {dbt_manifest_path}. Please check your dbt run.")

# Load the manifest to produce asset definitions
@dbt_assets(manifest=dbt_manifest_path)
def dbt_warehouse(context: AssetExecutionContext, dbt_warehouse_resource: DbtCliResource):
    yield from dbt_warehouse_resource.cli(["run"], context=context).stream()
