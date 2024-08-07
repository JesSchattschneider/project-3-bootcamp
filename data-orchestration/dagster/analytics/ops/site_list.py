import pandas as pd
from dagster import OpExecutionContext, op
import requests
import csv
import datetime
import numpy as np
from analytics.ops import parse_wfs_data, insert_data_to_snowflake
from sqlalchemy import MetaData
from sqlalchemy.engine.url import URL

# Define columns to be used in the WFS data
VARS = ["councilsiteid", "siteid", "lawasiteid",
        "lfenzid", "ltype", "geomorphicltype",
        "region", "agency", "catchment", "lwquality", "macro", "swquality"]

URL_LIST = "analytics/data/list_of_urls.csv"

@op(required_resource_keys={"snowflake_resource"})
def process_wfs_data(context: OpExecutionContext,
                     modules: list[str],
                     vars: list[str] = VARS,
                     url_path: str = URL_LIST) -> pd.DataFrame:
    """Processes WFS data for the given council partition."""

    context.log.info(modules)
    context.log.info(vars)

    context.log.info("Opening file with URLs")
    councils_wfs = []
    with open(url_path, "r") as fp:
        context.log.info("Reading WFS data")
        csv_reader = csv.reader(fp)
        for row in csv_reader:
            councils_wfs.append({"council": row[0], "wfs": row[1]})

    context.log.info("Filtering data based on partition")
    selected_councils = [council for council in councils_wfs if council["council"] == context.partition_key]

    results = []
    for council in selected_councils:
        context.log.info(f"Getting WFS data for: {council.get('council')}")
        response = requests.get(url=council.get("wfs"))
        data = {
            "council": council.get("council"),
            "url": council.get("wfs"),
            "status": "success" if response.status_code == 200 else "failed",
            "partition_key": context.partition_key,
            "creation_date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "raw_data": response.content if response.status_code == 200 else response.text
        }
        results.append(data)

    context.log.info("Parsing WFS data")
    df = pd.concat([parse_wfs_data(result) for result in results if result['status'] == 'success'], ignore_index=True)
    
    context.log.info("Wrangling WFS data")
    df.columns = map(str.lower, df.columns)
    df = df[df.columns.intersection(vars)]

    for var in vars:
        if var not in df.columns:
            context.log.info(f"Adding column for {var} to WFS dataframe")
            df[var] = np.nan

    df['partition_key'] = context.partition_key
    data_raw = df.copy()

    context.log.info("Processing modules")
    for module in modules:
        context.log.info(f"Processing module: {module}")

        if module == "mac":
            var_module = "macro"
        elif module in ["lwq", "swq"]:
            var_module = f"{module}uality"
        else:
            context.log.error(f"Unknown module: {module}")
            continue  # Skip processing for unknown module

        if var_module not in df.columns:
            context.log.error(f"Column {var_module} not in the data")
            continue  # Skip processing if column is missing

        # Filter data based on module
        # print(df.head())
        df = data_raw[data_raw[var_module].str.lower().isin(["y", "yes", "true"])]

        if len(df) == 0:
            context.log.info(f"No sites found for {module} module {var_module}")
            # print the df to see if it is empty - columns 'councilsiteid', 'siteid', 'lawasiteid' and var_module
            print(df[['councilsiteid', 'siteid', 'lawasiteid', var_module]])

            continue  # Skip processing if no sites are found
        else:
            # Replace np.nan with None
            context.log.info("Replace np.nan with None and add lawa_site column")
            df = df.replace({np.nan: None})
            df['lawa_site'] = "yes"

            context.log.info(f"Add to DB: Found sites for {module} module")
            snowflake_resource_con = context.resources.snowflake_resource
            insert_data_to_snowflake(snowflake_resource_con, df, f"{module}_wfs_table", context.log)
    return df
