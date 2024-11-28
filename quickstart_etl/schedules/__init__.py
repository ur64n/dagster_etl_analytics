from dagster import ScheduleDefinition
from ..jobs import correlation_job, db_pipeline_job, seasonity_job, sentiment_job, trading_volume_job

db_pipeline_schedule = ScheduleDefinition(
    job=db_pipeline_job,
    cron_schedule="0 12 1 * *",
)

correlation_schedule = ScheduleDefinition(
    job=correlation_job,
    cron_schedule="0 12 1 * *",
)

seasonity_schedule = ScheduleDefinition(
    job=seasonity_job,
    cron_schedule="0 12 1 1,4,7,10 *",
) 

sentiment_schedule = ScheduleDefinition(
    job=sentiment_job,
    cron_schedule="0 12 1 * *",
)

trading_volume_schedule = ScheduleDefinition(
    job=trading_volume_job,
    cron_schedule="0 12 1 * *",
)
