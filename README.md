# project-3-bootcamp

## DAGSTER

# Instruction

## Step-by-step

### Step 1: Create new virtual environment

Create a new environment

```
py -3.11 -m venv .venv
source .venv/Scripts/activate
```

### Step 2: Install dagster

```
pip install dagster==1.6.8
```

### Step 3: Bootstrap a new project

Scaffold new dagster project ([taken from here](https://docs.dagster.io/getting-started/create-new-project#step-1-bootstrap-a-new-project)).

```
cd unsolved
dagster project scaffold --name analytics
```

Rename top level `analytics` folder to `dagster` to avoid confusion.


The following structure has been scaffolded for you

```
dagster/
|__ analytics/
    |__ __init__.py
```

The root `analytics/__init__.py` file contains a [Definitions](https://docs.dagster.io/concepts/code-locations#defining-code-locations) object which defines all the code logic of this project.

### Step 4: Run dagster

Install other dependencies required for this dagster project

```
cd dagster
pip install -e ".[dev]"
```

Run dagster

```
dagster dev
```

Open dagster web UI in your browser: http://127.0.0.1:3000

![](./images/ui.png)

### Step 5: Create ops

Create new folder and files

```
dagster/
|__ analytics/
    |__ __init__.py
    |__ ops/            # create new folder
        |__ __init__.py     # create new file
        |__ weather.py      # create new file
```

In `analytics/ops/weather.py`, create ops that replicate's functions from the following code: `01-python-etl/3/11-ins-scheduling/solved/etl_project/assets/weather.py`.

For examples on how to create ops, see: https://docs.dagster.io/concepts/ops-jobs-graphs/ops

Make sure to use op config to expose configurations to the user of dagster: https://docs.dagster.io/concepts/configuration/config-schema#using-ops

Use `EnvVar('secret_name')` to fetch environment variables: https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets#from-dagster-configuration

Use [Resources](https://docs.dagster.io/concepts/resources) to make connection string details and secrets reusable by other ops and jobs.

![](./images/config.png)

### Step 6: Create jobs

After creating ops, stitch together the ops using a job.

Create the following folder and files

```
dagster/
|__ analytics/
    |__ __init__.py
    |__ ops/
        |__ __init__.py
        |__ weather.py
    |__ jobs/               # create new folder
        |__ __init__.py     # create new file
```

In `analytics/jobs/__init__.py`, import the previously created ops and create a job.

For more details on how to create a job, see: https://docs.dagster.io/concepts/ops-jobs-graphs/op-jobs

### Step 7: Add jobs and resources to Defintions object

```python
from dagster import Definitions, EnvVar

from analytics.jobs import run_weather_etl
from analytics.resources import PostgresqlDatabaseResource

defs = Definitions(
    jobs=[run_weather_etl],
    resources={
        "postgres_conn": PostgresqlDatabaseResource(
            host_name=EnvVar("postgres_host_name"),
            database_name=EnvVar("postgres_database_name"),
            username=EnvVar("postgres_username"),
            password=EnvVar("postgres_password"),
            port=EnvVar("postgres_port")
        )
    }
)
```

### Step 8: Manually trigger job

In the dagster UI, go to "Overview" > "Jobs".

![](./images/jobs.png)

Select the `run_weather_etl` job.

![](./images/launchpad.png)

Scaffold the missing and default config. You shouldn't need to replace them.

Click on "Launch Run".
