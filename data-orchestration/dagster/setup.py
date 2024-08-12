from setuptools import find_packages, setup

setup(
    name="analytics",
    packages=find_packages(exclude=["analytics_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "pg8000",
        "elementpath",
        "dagster_snowflake",
        "snowflake-connector-python",
        "dagster-dbt", 
        "dagster-webserver"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
