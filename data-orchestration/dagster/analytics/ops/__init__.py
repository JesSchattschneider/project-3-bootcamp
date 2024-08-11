from dagster import op, OpExecutionContext
import xml.etree.ElementTree as ET
import pandas as pd
import datetime
from sqlalchemy import Table, MetaData, Column, String, Integer, Float, DateTime
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Dict, Optional, Any
import numpy as np
# Sample data handling function
def parse_wfs_data(data: dict) -> pd.DataFrame:
    """Parses the wfs data."""
    root = ET.fromstring(data['raw_data'])
    records = []
    for feature in root.iter("{http://www.opengis.net/gml}featureMember"):
        record = {}
        for property in feature.iter():
            if property.tag != feature.tag:
                property_name = property.tag.split("}")[1]
                property_value = property.text
                record[property_name] = property_value
        records.append(record)
    df = pd.DataFrame(records)
    df = df.fillna(-99999)
    return df

@op(required_resource_keys={"snowflake_resource"})
def get_one(context):
    snowflake_resource_con = context.resources.snowflake_resource
    with snowflake_resource_con.get_connection() as conn:
        with conn.cursor() as cursor:
            # select from dim_temperature table where temperature = 11.83 limit 1
            cursor.execute("SELECT * FROM dim_temperature WHERE temperature = 11.83 LIMIT 1")
            result = cursor.fetchone()
            print(result)
            context.log.info(f"Result: {result}")

def _create_table_if_not_exists(cursor, table_name, df, logger, datetime_columns=None):
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            if datetime_columns:
                # get all columns from the dataframe that are not datetime columns
                columns = df.columns.difference(datetime_columns)
                column_definitions = ", ".join([f"{col} STRING" for col in columns])  # Assuming all columns are strings
                column_definitions += ", created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"  # Add created_at column with default current timestamp
                for col in datetime_columns:
                    column_definitions += f", {col} TIMESTAMP"
            else:
                columns = df.columns
                column_definitions = ", ".join([f"{col} STRING" for col in columns])  # Assuming all columns are strings
                column_definitions += ", created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"  # Add created_at column with default current timestamp            
            create_stmt = f"CREATE TABLE {table_name} ({column_definitions})"
            cursor.execute(create_stmt)
            logger.info(f"Table {table_name} created.")
            print(f"Table {table_name} created.")

def _insert_data(cursor, table_name, data_to_insert):
        columns = data_to_insert[0].keys()
        insert_cols = ', '.join(columns)
        insert_vals = ', '.join([f'%({col})s' for col in columns])
        insert_stmt = f"""
        INSERT INTO {table_name} ({insert_cols})
        VALUES ({insert_vals})
        """
        for row in data_to_insert:
            cursor.execute(insert_stmt, row)

def _upsert_data(cursor, table_name, data_to_insert, primary_key):
    if not data_to_insert:
        return  # Exit if there's no data to insert

    # Extract columns from data
    columns = data_to_insert[0].keys()
    columns_list = ', '.join(columns)
    
    # Handle values and convert None to NULL
    values_list = ', '.join([
        f"({', '.join([f'NULL' if value is None else repr(value) for value in row.values()])})"
        for row in data_to_insert
    ])
    
    # Define the INSERT SQL statement with WHERE NOT EXISTS clause
    insert_stmt = f"""
    INSERT INTO {table_name} ({columns_list})
    SELECT {', '.join([f'source.{col}' for col in columns])}
    FROM (VALUES {values_list}) AS source ({columns_list})
    WHERE NOT EXISTS (
        SELECT 1
        FROM {table_name} target
        WHERE target.{primary_key} = source.{primary_key}
    );
    """
    
    # Print SQL statement for debugging
    print(f"Executing SQL: {insert_stmt}")
    
    try:
        # Execute the INSERT statement
        cursor.execute(insert_stmt)
    except Exception as e:
        print(f"Error executing upsert: {e}")
        raise

def _insert_data_snowflake(snowflake_resource_con: Any, df: pd.DataFrame, table_name: str, logger: Any, council: Optional[str] = None) -> None:
    """Insert data into a Snowflake table, creating the table if it does not exist.

    Args:
        snowflake_resource_con (Any): The Snowflake connection resource.
        df (pd.DataFrame): The DataFrame containing data to insert.
        table_name (str): The name of the target table.
        logger (Any): Logger for logging information and errors.
    """
    try:
            with snowflake_resource_con.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Insert into main table
                    _create_table_if_not_exists(cursor, table_name, df, logger)
                    
                    data_to_insert = df.to_dict(orient='records')
                    _insert_data(cursor, table_name, data_to_insert)
                    
                    # Insert into latest table
                    latest_table_name = f"{table_name}_latest"
                    _create_table_if_not_exists(cursor, latest_table_name, df, logger)

                    # Delete existing rows for the council in the latest table
                    delete_stmt = f"DELETE FROM {latest_table_name} WHERE council = %s"
                    cursor.execute(delete_stmt, (council,))
                    
                    # Insert new rows into the latest table
                    _insert_data(cursor, latest_table_name, data_to_insert)
                    
                    logger.info("Data successfully inserted into the database and latest table.")
        
    except Exception as e:
        logger.error(f"Failed to append to database: {e}")

def load_data_to_snowflake(snowflake_resource_con: Any, 
                             df: pd.DataFrame, 
                             table_name: str,
                             logger: Any,
                             method: str = "insert",
                             council: str = None
                             ) -> None:
    """Insert data into a Snowflake table, creating the table if it does not exist.

    Args:
        snowflake_resource_con (Any): The Snowflake connection resource.
        df (pd.DataFrame): The DataFrame containing data to insert.
        table_name (str): The name of the target table.
        logger (Any): Logger for logging information and errors.
    """
    
    if df.empty:
        logger.info("No data to insert.")
        return
    
    if method == "insert":
        logger.info("Inserting data into the database.")
        _insert_data_snowflake(snowflake_resource_con, df, table_name, logger, council)
    
    if method == "upsert":
        logger.info("Upserting data into the database.")

        try:
            with snowflake_resource_con.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Insert into main table
                    print(df.columns)
                    _create_table_if_not_exists(cursor, table_name, df, logger)
                    
                    test = df.head(2)
                    
                    data_to_upsert = test.to_dict(orient='records')
                    _upsert_data(cursor, table_name, data_to_upsert, primary_key = 'id')
                    
                    logger.info("Data successfully inserted into the database and latest table.")
        
        except Exception as e:
            logger.error(f"Failed to upsert to database: {e}")


@op(required_resource_keys={"snowflake_resource"})
def wfs_db_table(context: OpExecutionContext, 
                        transformed_df: pd.DataFrame, vars: list[str]) -> None:
    """Produces wfs table and upserts to Snowflake.

    Args:
        snowflake_conn: a SnowflakeResource object
        transformed_df: the transformed DataFrame
        vars: list of column names
    """

    # Add an additional column for the primary key
    transformed_df["pk_lawa_and_council_id"] = transformed_df["lawasiteid"].astype(str) + "__" + transformed_df["councilsiteid"].astype(str)
    # Add partition_key columns
    transformed_df['partition_key'] = context.partition_key

    # Transform the DataFrame to a list of dictionaries
    data_to_db = transformed_df.to_dict(orient="records") 

    # Define table metadata
    metadata = MetaData()
    table = Table(
        "wfs_table",
        metadata,
        Column("pk_lawa_and_council_id", String, primary_key=True),
        *(Column(var, String) for var in vars),
        Column("partition_key", String)
    )

    print(table)

    # Upsert data to Snowflake
    # append_to_database(context, snowflake_conn, data_to_db, table, metadata)
    