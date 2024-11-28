# Project Structure

## my-dagster-project

### data
- **monitoring**: Directory for monitoring data.
- **raw**: Contains raw data files.
- **results**: Stores results and visualizations.
- **structured**: Holds structured and cleaned data.

### quickstart_etl

#### assets
- **constants.py**: Defines constants used across the project.
- **correlation.py**: Handles correlation analysis between macroeconomic indicators and stock prices.
  - **process_combined_data**: Merges macroeconomic data with stock data.
  - **analyze_and_visualize_correlation**: Analyzes and visualizes the correlation matrix.
- **db_pipeline.py**: Manages data extraction and structuring from the database.
  - **extract_all_data**: Extracts data from a PostgreSQL database and saves it to CSV files.
  - **clean_and_structure_data**: Cleans and structures the data.
- **seasonity.py**: Analyzes and visualizes seasonal patterns in stock prices.
  - **load_and_prepare_seasonity_data**: Loads and prepares data for seasonality analysis.
  - **analyze_seasonality**: Analyzes seasonality of stock prices in different sectors.
  - **visualize_seasonality**: Visualizes seasonal patterns.
- **sensorequest.py**: Contains functions for storing and visualizing outliers.
- **sentiment.py**: Performs sentiment analysis on news data.
  - **load_and_prepare_sentiment_data**: Prepares data for sentiment analysis.
  - **analyze_sentiment**: Analyzes news sentiment.
  - **visualize_sentiment_impact**: Visualizes the impact of sentiment on stock prices.
- **trading_volume.py**: Analyzes trading volume and detects anomalies.
  - **load_and_filter_volume_data**: Loads and filters trading volume data.
  - **detect_volume_anomalies**: Detects anomalies in trading volume.
  - **analyze_price_impact_of_anomalies**: Analyzes the impact of volume anomalies on price changes.
  - **visualize_price_impact**: Visualizes the impact of volume anomalies on prices.

#### jobs
- **\_\_init\_\_.py**: Initializes job configurations.

#### partitions
- **\_\_init\_\_.py**: Initializes partition configurations.

#### resources
- **resource.py**: Defines resources for database connections.

#### schedules
- **\_\_init\_\_.py**: Initializes schedule configurations.

#### sensors
- **definitions.py**: Contains sensor definitions for monitoring data changes.
  - **Price Change Sensor**: Monitors significant changes in stock prices based on sentiment analysis.

### quickstart_etl_tests
- Directory for test cases related to the ETL pipeline.

### Configuration Files
- **.env**: Environment variables for the project.
- **.gitignore**: Specifies files and directories to be ignored by Git.
- **dagster_cloud.yaml**: Configuration for Dagster Cloud.
- **dagster.yaml**: Dagster project configuration.
- **pyproject.toml**: Python project configuration.
- **README.md**: Project documentation.
