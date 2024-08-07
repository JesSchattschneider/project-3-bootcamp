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

# TODO: add primary key to the table
def insert_data_to_snowflake(snowflake_resource_con: Any, 
                             df: pd.DataFrame, 
                             table_name: str, 
                             logger: Any) -> None:
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
        
    try:
        with snowflake_resource_con.get_connection() as conn:
            with conn.cursor() as cursor:
                # Check if table exists
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                table_exists = cursor.fetchone() is not None
                
                if not table_exists:
                    # Create table if it does not exist
                    columns = df.columns
                    column_definitions = ", ".join([f"{col} STRING" for col in columns])  # Assuming all columns are strings
                    # add created_at column as datetime
                    column_definitions += ", created_at TIMESTAMP"

                    create_stmt = f"CREATE TABLE {table_name} ({column_definitions})"
                    cursor.execute(create_stmt)
                    logger.info(f"Table {table_name} created.")

                # Add created_at column with the current timestamp to each row
                df['created_at'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Prepare data for insertion
                data_to_insert = df.to_dict(orient='records')
                columns = list(data_to_insert[0].keys())
                insert_cols = ', '.join(columns)
                insert_vals = ', '.join([f'%({col})s' for col in columns])
                insert_stmt = f"""
                INSERT INTO {table_name} ({insert_cols})
                VALUES ({insert_vals})
                """

                # Insert data
                for row in data_to_insert:
                    cursor.execute(insert_stmt, row)
                logger.info("Data successfully inserted into the database.")
    
    except Exception as e:
        logger.error(f"Failed to append to database: {e}")

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
    # Add created_at and partition_key columns
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
    

# def append_to_database(
#         context: OpExecutionContext,
#         snowflake_conn: SnowflakeResource,
#         data: list[dict],
#         table: Table,
#         metadata: MetaData
#     ) -> None:
#     """Appends data into the Snowflake database.

#     Args:
#         snowflake_conn: a SnowflakeResource object
#         data: the transformed data
#     """
#     with snowflake_conn.get_connection() as conn:
#         account = snowflake_conn.account
#         user = snowflake_conn.user
#         password = snowflake_conn.password
#         database = snowflake_conn.database
#         schema = snowflake_conn.schema
#         warehouse = snowflake_conn.warehouse

#         connection_url = URL.create(
#             drivername="snowflake",
#             username=user,
#             password=password,
#             host=account,
#             database=database,
#             schema=schema,
#             query={
#                 "warehouse": warehouse
#             }
#         )

#         engine = create_engine(connection_url)
        
#         context.log.info("Creating database table if not exist")
#         metadata.create_all(engine)

#         # Convert data to Snowflake-compatible format
#         data_to_insert = [
#             {col: val for col, val in row.items()} for row in data
#         ]

#         # Generate the column list and value placeholders for the insert statement
#         columns = list(data_to_insert[0].keys())
#         insert_cols = ', '.join(columns)
#         insert_vals = ', '.join([f':{col}' for col in columns])

#         # Create the INSERT statement
#         insert_stmt = f"""
#         INSERT INTO {table.name} ({insert_cols})
#         VALUES ({insert_vals})
#         """

#         context.log.info("Appending records to database table")
#         with engine.begin() as connection:
#             try:
#                 for row in data_to_insert:
#                     connection.execute(text(insert_stmt), **row)
#             except Exception as e:
#                 raise Exception(f"Failed to append to database, {e}")
#         context.log.info(f"Completed append to database table.")

# def insert_to_database(
#         context: OpExecutionContext,
#         postgres_conn: PostgresqlDatabaseResource,
#         data: list[dict],
#         table: Table,
#         metadata: MetaData
#     ) -> None:
#     """Inserts data into the target database.

#     Args:
#         context: Dagster execution context.
#         postgres_conn: a PostgresqlDatabaseResource object with connection details.
#         data: the data to be inserted.
#         table: a SQLAlchemy Table object representing the target table.
#         metadata: a SQLAlchemy MetaData object.
#     """
#     # Create the connection URL
#     connection_url = URL.create(
#         drivername="postgresql+pg8000",
#         username=postgres_conn.username,
#         password=postgres_conn.password,
#         host=postgres_conn.host_name,
#         port=postgres_conn.port,
#         database=postgres_conn.database_name,
#     )
#     engine = create_engine(connection_url)
    
#     # Create the table if it does not exist
#     context.log.info("Creating database table if not exist")
#     metadata.create_all(engine)
    
#     # Prepare the insert statement
#     insert_statement = table.insert().values(data)
    
#     # Insert the data into the database
#     context.log.info("Inserting records into database table")
#     with engine.begin() as connection:
#         try:
#             result = connection.execute(insert_statement)
#         except SQLAlchemyError as e:
#             raise Exception(f"Failed to insert into database: {e}")
    
#     context.log.info(f"Completed insert to database table. Rows affected: {result.rowcount}")

# def upsert_to_database(
#         context: OpExecutionContext,
#         snowflake_conn: SnowflakeResource,
#         data: list[dict],
#         table: Table,
#         metadata: MetaData
#     ) -> None:
#     """Upserts data into the Snowflake database.

#     Args:
#         snowflake_conn: a SnowflakeResource object
#         data: the transformed data
#     """
#     # Create SQLAlchemy engine using the Snowflake connection
#     connection_url = snowflake_conn.get_connection_url()
#     engine = create_engine(connection_url)
    
#     context.log.info("Creating database table if not exist")
#     metadata.create_all(engine)

#     key_columns = [
#         pk_column.name for pk_column in table.primary_key.columns.values()
#     ]

#     # Convert data to Snowflake-compatible format
#     data_to_insert = [
#         {col: val for col, val in row.items()} for row in data
#     ]

#     # Generate the column list and value placeholders for the insert statement
#     columns = list(data_to_insert[0].keys())
#     insert_cols = ', '.join(columns)
#     insert_vals = ', '.join([f':{col}' for col in columns])
    
#     # Generate the SET clause for the update part of the MERGE statement
#     update_clause = ', '.join([f'{col} = :{col}' for col in columns if col not in key_columns])

#     # Create the MERGE statement
#     merge_stmt = f"""
#     MERGE INTO {table.name} AS target
#     USING (SELECT {insert_vals}) AS source ({insert_cols})
#     ON ({' AND '.join([f'target.{col} = source.{col}' for col in key_columns])})
#     WHEN MATCHED THEN UPDATE SET {update_clause}
#     WHEN NOT MATCHED THEN
#     INSERT ({insert_cols}) VALUES ({insert_vals})
#     """

#     context.log.info("Upserting records to database table")
#     with engine.begin() as connection:
#         try:
#             for row in data_to_insert:
#                 connection.execute(text(merge_stmt), **row)
#         except Exception as e:
#             raise Exception(f"Failed to upsert to database, {e}")
#     context.log.info(f"Completed upsert to database table.")
