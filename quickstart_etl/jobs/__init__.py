from dagster import define_asset_job,AssetSelection
from quickstart_etl.partitions import monthly_partition, weekly_partition, daily_partition

db_pipeline_job = define_asset_job(
    name="db_pipeline_job",
    partitions_def=monthly_partition,
    selection=AssetSelection.all() - AssetSelection.assets(["process_combined_data"]) - AssetSelection.assets(["analyze_and_visualize_correlation"])
    - AssetSelection.assets(["load_and_prepare_sentiment_data"]) - AssetSelection.assets(["analyze_sentiment"]) - AssetSelection.assets(["visualize_sentiment_impact"])
    - AssetSelection.assets(["load_and_prepare_seasonity_data"]) - AssetSelection.assets(["analyze_seasonality"]) - AssetSelection.assets(["visualize_seasonality"])
    - AssetSelection.assets(["load_and_filter_volume_data"]) - AssetSelection.assets(["aggregate_volume_data"]) - AssetSelection.assets(["detect_volume_anomalies"])
    - AssetSelection.assets(["analyze_price_impact_of_anomalies"]) - AssetSelection.assets(["visualize_price_impact"]) - AssetSelection.assets(["load_and_prepare_sentiment2_data"])
    - AssetSelection.assets(["analyze_technology_sentiment2"]) - AssetSelection.assets(["analyze_tech_sentiment_impact"]) - AssetSelection.assets(["visualize_tech_sentiment2_impact"])
    - AssetSelection.assets(["visualize_price_change_outliers"])
)

correlation_job = define_asset_job(
    name="correlation_job",
    selection=AssetSelection.all() - AssetSelection.assets(["analyze_sentiment"]) - AssetSelection.assets(["visualize_sentiment_impact"])
    - AssetSelection.assets(["load_and_filter_volume_data"]) - AssetSelection.assets(["aggregate_volume_data"]) - AssetSelection.assets(["detect_volume_anomalies"]) 
    - AssetSelection.assets(["analyze_price_impact_of_anomalies"])- AssetSelection.assets(["visualize_price_impact"]) - AssetSelection.assets(["load_and_prepare_sentiment2_data"])
    - AssetSelection.assets(["analyze_technology_sentiment2"]) - AssetSelection.assets(["analyze_tech_sentiment_impact"]) - AssetSelection.assets(["visualize_tech_sentiment2_impact"])
    - AssetSelection.assets(["visualize_price_change_outliers"])- AssetSelection.assets(["load_and_prepare_seasonity_data"])- AssetSelection.assets(["load_and_prepare_sentiment_data"])
    - AssetSelection.assets(["analyze_seasonality"])- AssetSelection.assets(["visualize_seasonality"])
    )

sentiment_job = define_asset_job(
    name="sentiment_job",
    selection=AssetSelection.all() - AssetSelection.assets(["process_combined_data"]) - AssetSelection.assets(["analyze_and_visualize_correlation"])
    - AssetSelection.assets(["load_and_prepare_seasonity_data"]) - AssetSelection.assets(["analyze_seasonality"]) - AssetSelection.assets(["visualize_seasonality"])
    - AssetSelection.assets(["load_and_filter_volume_data"]) - AssetSelection.assets(["aggregate_volume_data"]) - AssetSelection.assets(["detect_volume_anomalies"]) 
    - AssetSelection.assets(["analyze_price_impact_of_anomalies"]) - AssetSelection.assets(["visualize_price_impact"]) - AssetSelection.assets(["load_and_prepare_sentiment2_data"])
    - AssetSelection.assets(["analyze_technology_sentiment2"]) - AssetSelection.assets(["analyze_tech_sentiment_impact"]) - AssetSelection.assets(["visualize_tech_sentiment2_impact"])
    - AssetSelection.assets(["visualize_price_change_outliers"])
)

tech_sentiment_job = define_asset_job(
    name="tech_sentiment_job",
    selection=AssetSelection.all() - AssetSelection.assets(["process_combined_data"]) - AssetSelection.assets(["analyze_and_visualize_correlation"]) 
    - AssetSelection.assets(["load_and_prepare_sentiment_data"]) - AssetSelection.assets(["analyze_sentiment"]) - AssetSelection.assets(["visualize_sentiment_impact"]) 
    - AssetSelection.assets(["load_and_prepare_seasonity_data"]) - AssetSelection.assets(["analyze_seasonality"]) - AssetSelection.assets(["visualize_seasonality"]) 
    - AssetSelection.assets(["load_and_filter_volume_data"]) - AssetSelection.assets(["aggregate_volume_data"]) - AssetSelection.assets(["detect_volume_anomalies"])
    - AssetSelection.assets(["analyze_price_impact_of_anomalies"]) - AssetSelection.assets(["visualize_price_impact"])- AssetSelection.assets(["visualize_price_change_outliers"])
)

seasonity_job = define_asset_job(
    name="seasonity_job",
    selection=AssetSelection.all() - AssetSelection.assets(["process_combined_data"]) - AssetSelection.assets(["analyze_and_visualize_correlation"])
    - AssetSelection.assets(["load_and_prepare_sentiment_data"])- AssetSelection.assets(["analyze_sentiment"]) - AssetSelection.assets(["visualize_sentiment_impact"])
    - AssetSelection.assets(["load_and_filter_volume_data"]) - AssetSelection.assets(["aggregate_volume_data"]) - AssetSelection.assets(["detect_volume_anomalies"]) 
    - AssetSelection.assets(["analyze_price_impact_of_anomalies"]) - AssetSelection.assets(["visualize_price_impact"]) - AssetSelection.assets(["load_and_prepare_sentiment2_data"])
    - AssetSelection.assets(["analyze_technology_sentiment2"]) - AssetSelection.assets(["analyze_tech_sentiment_impact"]) - AssetSelection.assets(["visualize_tech_sentiment2_impact"])
    - AssetSelection.assets(["visualize_price_change_outliers"])
)

trading_volume_job = define_asset_job(
    name="trading_volume_job",
    selection=AssetSelection.all() - AssetSelection.assets(["process_combined_data"]) - AssetSelection.assets(["analyze_and_visualize_correlation"])
    - AssetSelection.assets(["load_and_prepare_sentiment_data"]) - AssetSelection.assets(["analyze_sentiment"]) - AssetSelection.assets(["visualize_sentiment_impact"])
    - AssetSelection.assets(["load_and_prepare_seasonity_data"]) - AssetSelection.assets(["analyze_seasonality"]) - AssetSelection.assets(["visualize_seasonality"])
    - AssetSelection.assets(["load_and_prepare_sentiment2_data"]) - AssetSelection.assets(["analyze_technology_sentiment2"]) - AssetSelection.assets(["analyze_tech_sentiment_impact"])
    - AssetSelection.assets(["visualize_tech_sentiment2_impact"]) - AssetSelection.assets(["visualize_price_change_outliers"])
)

price_change_sensor_job = define_asset_job(
    name="price_change_sensor_job",
    selection=AssetSelection.all() - AssetSelection.assets(["extract_all_data"]) - AssetSelection.assets(["clean_and_structure_data"])
    - AssetSelection.assets(["process_combined_data"]) - AssetSelection.assets(["analyze_and_visualize_correlation"])
    - AssetSelection.assets(["load_and_prepare_sentiment_data"]) - AssetSelection.assets(["analyze_sentiment"]) - AssetSelection.assets(["visualize_sentiment_impact"])
    - AssetSelection.assets(["load_and_prepare_seasonity_data"]) - AssetSelection.assets(["analyze_seasonality"]) - AssetSelection.assets(["visualize_seasonality"])
    - AssetSelection.assets(["load_and_filter_volume_data"]) - AssetSelection.assets(["aggregate_volume_data"]) - AssetSelection.assets(["detect_volume_anomalies"])
    - AssetSelection.assets(["analyze_price_impact_of_anomalies"]) - AssetSelection.assets(["visualize_price_impact"]) - AssetSelection.assets(["load_and_prepare_sentiment2_data"])
    - AssetSelection.assets(["analyze_technology_sentiment2"]) - AssetSelection.assets(["analyze_tech_sentiment_impact"]) - AssetSelection.assets(["visualize_tech_sentiment2_impact"])
)

