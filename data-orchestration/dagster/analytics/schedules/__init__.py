from dagster import ScheduleDefinition, build_schedule_from_partitioned_job

from analytics.jobs import run_weather_etl, run_alpaca_etl

# weather_etl_schedule = ScheduleDefinition(job=run_weather_etl, cron_schedule="* * * * *")
weather_etl_schedule = build_schedule_from_partitioned_job(job=run_weather_etl)

# alpaca_etl_schedule = ScheduleDefinition(job=run_alpaca_etl, cron_schedule="* * * * *")
alpaca_etl_schedule = build_schedule_from_partitioned_job(job=run_alpaca_etl)
