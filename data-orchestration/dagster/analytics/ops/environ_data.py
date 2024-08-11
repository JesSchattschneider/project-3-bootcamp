
import xml.etree.ElementTree as ET
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from dagster import op, OpExecutionContext, Config
from analytics.ops import load_data_to_snowflake 
import numpy as np

URL_LIST = "analytics/data/list_of_urls.csv"
URL_VARIABLES_LWQ = "analytics/data/transfer_table_LWQ.csv"

class EnvironDataConfig(Config):
    date_start: str
    date_end: str = "2030-01-01"
    councils: list[str] = ["ecan"]

def fetch_status_code(url_info, logger):
    url = url_info["url"]
    site = url_info.get("site")
    variable = url_info.get("variable")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            # Parse XML content if status code is 200
            content = response.content
            root = ET.fromstring(content)

            # Process the XML data
            data_list = []
            for data_element in root.iter("Data"):
                for e_element in data_element.findall(".//E"):
                    data_dict = {
                        "url": url,
                        "status_code": response.status_code,
                        "error": None,
                        "site": site,
                        "variable": variable
                    }
                    for element in e_element:
                        if element.tag == "Parameter":
                            param_name = element.get("Name")
                            param_value = element.get("Value")
                            data_dict[param_name] = param_value
                        else:
                            data_dict[element.tag] = element.text
                    data_list.append(data_dict)

            # Check if data_list is empty
            if not data_list:
                logger.info(f"No data found for {url}")
                return {
                    "url": url,
                    "status_code": response.status_code,
                    "data": None,
                    "error": "No data found",
                    "site": site,
                    "variable": variable
                }

            # Convert to DataFrame
            df = pd.DataFrame(data_list)
            return {
                "url": url,
                "status_code": response.status_code,
                "data": df.to_json(orient="records"),
                "error": None,
                "site": site,
                "variable": variable
            }
        else:
            logger.error(f"Failed to fetch {url}, status code: {response.status_code}")
            return {
                "url": url,
                "status_code": response.status_code,
                "data": None,
                "error": f"HTTP error: {response.status_code}",
                "site": site,
                "variable": variable
            }
    except Exception as e:
        logger.error(f"Exception for {url}: {e}")
        return {
            "url": url,
            "status_code": None,
            "data": None,
            "error": str(e),
            "site": site,
            "variable": variable
        }
    
@op(required_resource_keys={"snowflake_resource"})
def pull_lwq_data(context: OpExecutionContext,
                  config: EnvironDataConfig,
                  sites: list[str] = [],
                  variables: list[str] = [],
                  url_path: str = URL_LIST,
                  limit: int = 5) -> pd.DataFrame:
    context.log.info("Opening file with URLs")
    root_urls = []

    # Read date range from config
    date_start = config.date_start
    date_end = config.date_end
    context.log.info(f"Date range: {date_start} to {date_end}")
    
    with open(url_path, "r") as fp:
        context.log.info("Reading URL list data")
        csv_reader = csv.reader(fp)
        urls_all = [row for row in csv_reader]
        
    context.log.info(f"Councils selected: {config.councils}")

    if not variables:
        with open(URL_VARIABLES_LWQ, "r") as tp:
            context.log.info("Reading transfer table data")
            csv_transfer = csv.reader(tp)
            vars_all = [row for row in csv_transfer]

    urls = []
    for council in config.councils:
        root_urls = [url[3] for url in urls_all if url[0] == council][0]
        context.log.info(f"Retrieved root URLs: {root_urls}")
        if vars_all:
            variables = [var[1] for var in vars_all if var[0] == council]
            context.log.info(f"Retrieved variables: {variables}")

        if not sites:
            try:
                snowflake_resource_con = context.resources.snowflake_resource
                with snowflake_resource_con.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(f"SELECT COUNCILSITEID, LAWASITEID, COUNCIL FROM LWQ_WFS_TABLE_LATEST WHERE council = '{council}'")
                           # Fetch all rows from the executed query
                        rows = cursor.fetchall()
                        
                        # Get the column names from the cursor description
                        column_names = [desc[0] for desc in cursor.description]
                        
                        # Create a DataFrame from the fetched data
                        sites_metadata = pd.DataFrame(rows, columns=column_names)
                        
                        # Extract specific columns if needed (optional)
                        sites = sites_metadata['COUNCILSITEID'].tolist()
                        context.log.info(f"Retrieved site IDs: {sites}")
            except Exception as e:
                context.log.error(f"Error getting sites from snowflake: {e}")
                return pd.DataFrame()  # Return an empty DataFrame in case of error
        
        # select only the first 5 sites for testing
        sites = sites[:limit]

        for site in sites:
            for variable in variables:
                url = f"{root_urls}&From={date_start}&To={date_end}&Site={site}&Measurement={variable}"
                urls.append({"url": url, "site": site, "variable": variable})

    context.log.info(f"Generated URLs for {len(urls)} sites and variables")

    # Use ThreadPoolExecutor to fetch URLs concurrently
    url_status_data = []
    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers as needed
        future_to_url = {executor.submit(fetch_status_code, url_info, context.log): url_info for url_info in urls}
        for future in as_completed(future_to_url):
            url_status_data.append(future.result())

    # Process DataFrames from URL status data
    all_data_frames = []
    failed_data = []
    for result in url_status_data:
        if result["data"]:
            df = pd.read_json(result["data"])
            df["url"] = result["url"]
            df["status_code"] = result["status_code"]
            df["error"] = result["error"]
            df["site"] = result["site"]
            df["variable"] = result["variable"]
            all_data_frames.append(df)
        else:
            # Include failed URLs in the DataFrame
            failed_data.append({
                "url": result["url"],
                "status_code": result["status_code"],
                "error": result["error"],
                "site": result["site"],
                "variable": result["variable"]
            })
            # Log the URL and error if data is None
            context.log.error(f"Data retrieval failed for URL: {result['url']}. Error: {result['error']}")

    # Concatenate all DataFrames into a single DataFrame
    if all_data_frames:
        final_df = pd.concat(all_data_frames, ignore_index=True)
    else:
        final_df = pd.DataFrame()

    # Convert failed_data to DataFrame and concatenate with final_df
    failed_df = pd.DataFrame(failed_data)
    if not failed_df.empty:
        final_df = pd.concat([final_df, failed_df], ignore_index=True)

    context.log.info("DataFrame with URLs, status codes, sites, and variables created.")

    if 't' not in final_df.columns.str.lower():
        context.log.info("No data retrieved. Returning DataFrame with only errors.")
        final_df['t'] = "missing"
        final_df['value'] = None
    
    number_of_urls = len(urls)
    failed_urls = len(final_df[final_df["error"].notnull()])

    context.log.info(f"Number of URLs: {number_of_urls}\nNumber of failed URLs: {failed_urls}")

    # convert vars_all to DataFrame
    vars_all = pd.DataFrame(vars_all[1:], columns=vars_all[0])

    # merge to get lawa site id
    final_df = final_df.merge(sites_metadata, how='left', left_on='site', right_on='COUNCILSITEID')
    # merge with transfer table to get variable name - merge based on Agency and CallName for transfer and variable and COUNCIL for final_df
    final_df = final_df.merge(vars_all, how='left', left_on=['variable', 'COUNCIL'], right_on=['CallName', 'Agency'])
    # convert all columns to lowercase
    final_df.columns = final_df.columns.str.lower()

    # create column for primary key
    final_df['id'] = final_df['lawasiteid'] + "_" + final_df['councilsiteid'] + "_" + final_df['lawaname'] + "_" + final_df['t']

    # select data df, following columns: id, date, variable, value,  url, status_code,	error, site, variable, T, Value
    df_columns = ['id', 't', 'site', 'variable', 'value', 'error', 'status_code', 'url']
    df_data = final_df[df_columns]

    # select all other columns that are not in df_columns, plus id
    df_columns_meta = final_df.columns
    df_columns_meta = df_columns_meta.drop(df_columns)
    df_columns_meta = df_columns_meta.insert(0, 'id')

    df_metadata = final_df[df_columns_meta]

        # Save the final_df to a CSV file
    df_data.to_csv("analytics/data/final_df.csv", index=False)
    df_metadata.to_csv("analytics/data/final_df_metadata.csv", index=False)
    
    context.log.info("Final DataFrame saved to 'analytics/data/final_df.csv'.")

    context.log.info("Add to DB: data and metadata")

    # Replace np.nan with None
    context.log.info("Replace np.nan with None and add lawa_site and council column")
    df_data = df_data.replace({np.nan: None})
    if not df_data.empty:
        snowflake_resource_con = context.resources.snowflake_resource
        load_data_to_snowflake(snowflake_resource_con = snowflake_resource_con, 
                                df = df_data, 
                                table_name =  "lwq_data",
                                method='upsert',
                                logger = context.log)
    


    return final_df


