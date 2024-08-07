from dagster import resource
from dagster_snowflake import SnowflakeResource
import os
from dotenv import load_dotenv
load_dotenv()

@resource
def snowflake_resource():
    return SnowflakeResource(
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),	
        user= os.environ.get('SNOWFLAKE_USER'),
        password= os.environ.get('SNOWFLAKE_PASSWORD'),
        database=os.environ.get('SNOWFLAKE_DATABASE'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA'),
        warehouse="XSMALL_WH"
    )