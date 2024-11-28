from dagster import Definitions, EnvVar, load_assets_from_modules
from quickstart_etl.resources import postgres_db_resource
from quickstart_etl.assets import db_pipeline, correlation, sentiment, seasonity, trading_volume, sensorequest
from quickstart_etl.jobs import correlation_job, sentiment_job, seasonity_job, trading_volume_job, tech_sentiment_job, price_change_sensor_job, db_pipeline_job
from quickstart_etl.schedules import correlation_schedule, db_pipeline_schedule, seasonity_schedule, sentiment_schedule, trading_volume_schedule
from quickstart_etl.sensors import price_change_sensor

extract_all_data = load_assets_from_modules([db_pipeline])
clean_and_structure_data = load_assets_from_modules ([db_pipeline])
process_combined_data = load_assets_from_modules([correlation])
analyze_and_visualize_correlation = load_assets_from_modules([correlation])
load_and_prepare_sentiment_data = load_assets_from_modules([sentiment])#pipeline1
analyze_sentiment = load_assets_from_modules([sentiment])#pipeline1
visualize_sentiment_impact = load_assets_from_modules([sentiment])#pipeline1
load_and_prepare_seasonity_data = load_assets_from_modules([seasonity])
analyze_seasonality = load_assets_from_modules([seasonity])
visualize_seasonality = load_assets_from_modules([seasonity])
load_and_filter_volume_data = load_assets_from_modules([trading_volume])
aggregate_volume_data = load_assets_from_modules([trading_volume])
detect_volume_anomalies = load_assets_from_modules([trading_volume])
analyze_price_impact_of_anomalies = load_assets_from_modules([trading_volume])
visualize_price_impact = load_assets_from_modules([trading_volume])
load_and_prepare_sentiment2_data = load_assets_from_modules ([sentiment])#pipeline2
analyze_technology_sentiment2 = load_assets_from_modules ([sentiment])#pipeline2
analyze_tech_sentiment_impact = load_assets_from_modules([sentiment])#pipeline2
visualize_tech_sentiment2_impact = load_assets_from_modules([sentiment])#pipeline2
visualize_price_change_outliers = load_assets_from_modules([sensorequest])

all_jobs = [correlation_job, sentiment_job, seasonity_job, trading_volume_job,
            tech_sentiment_job,price_change_sensor_job, db_pipeline_job]

all_schedules = [correlation_schedule, db_pipeline_schedule, seasonity_schedule, 
                 sentiment_schedule, trading_volume_schedule]

all_sensors = [price_change_sensor]

defs = Definitions(
    assets=[*extract_all_data, *clean_and_structure_data, 
    *process_combined_data, *analyze_and_visualize_correlation, 
    *load_and_prepare_sentiment_data, *analyze_sentiment,
    *visualize_sentiment_impact, *load_and_prepare_seasonity_data,
    *analyze_seasonality, *visualize_seasonality, *load_and_filter_volume_data,
    *aggregate_volume_data, *detect_volume_anomalies, *analyze_price_impact_of_anomalies,
    *load_and_prepare_sentiment2_data,*analyze_technology_sentiment2,*analyze_tech_sentiment_impact,
    *visualize_tech_sentiment2_impact, *visualize_price_change_outliers],

    resources={
        "postgres_db": postgres_db_resource.configured({
            "username": EnvVar("PG_USERNAME").get_value(),
            "password": EnvVar("PG_PASSWORD").get_value(),
            "hostname": EnvVar("PG_HOSTNAME").get_value(),
            "db_name": EnvVar("PG_DB").get_value()
        }),
    },
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
)
