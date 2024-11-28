# ETL Project with Macroeconomic and Stock Price Analysis

## Project based on PySpark-FAKER Data

## Project Description

This project is (Extract, Transform, Load) system built using Dagster, designed to analyze macroeconomic data and stock prices. The goal is to understand the impact of macroeconomic indicators, trading volume, and news sentiment on stock prices across various sectors.

## Project Structure

### Main Modules

1. **ETL Pipeline**:
   - **extract_all_data**: Extracts data from a PostgreSQL database and saves it to CSV files.
   - **clean_and_structure_data**: Cleans and structures the data.

2. **Seasonality Analysis**:
   - **load_and_prepare_seasonity_data**: Loads and prepares data for seasonality analysis.
   - **analyze_seasonality**: Analyzes seasonality of stock prices in different sectors.
   - **visualize_seasonality**: Visualizes seasonal patterns.

3. **Correlation Analysis**:
   - **process_combined_data**: Merges macroeconomic data with stock data.
   - **analyze_and_visualize_correlation**: Analyzes and visualizes the correlation matrix.

4. **Trading Volume Analysis**:
   - **load_and_filter_volume_data**: Loads and filters trading volume data.
   - **detect_volume_anomalies**: Detects anomalies in trading volume.
   - **analyze_price_impact_of_anomalies**: Analyzes the impact of volume anomalies on price changes.
   - **visualize_price_impact**: Visualizes the impact of volume anomalies on prices.

5. **Sentiment Analysis**:
   - **load_and_prepare_sentiment_data**: Prepares data for sentiment analysis.
   - **analyze_sentiment**: Analyzes news sentiment.
   - **visualize_sentiment_impact**: Visualizes the impact of sentiment on stock prices.

# Sensor: Price Change Sensor

## Description

The **Price Change Sensor** is designed to monitor significant changes in stock prices based on sentiment analysis. It is part of the ETL pipeline and is specifically linked to the `tech_sentiment_job`. This sensor continuously checks for anomalies in price changes and triggers further actions if certain conditions are met.

## Functionality

- **Data Monitoring**: The sensor reads data from a CSV file containing the results of sentiment analysis on stock prices.
- **Change Detection**: It calculates a hash of the current data to detect any changes since the last evaluation.
- **Threshold Evaluation**: If the data has changed, the sensor checks for any price changes that exceed a predefined threshold (e.g., 10%).
- **Outlier Handling**: If outliers are detected, they are stored for further analysis.
- **Visualization Trigger**: The sensor triggers a visualization process to create a boxplot of the detected price change outliers.
- **Run Request**: If conditions are met, a new run request is generated to handle the detected changes.

## Key Features

- **Integration with tech_sentiment_job**: The sensor is integrated with the `tech_sentiment_job`, ensuring that it operates within the context of the sentiment analysis pipeline.
- **Efficient Monitoring**: By using hash calculations, the sensor efficiently detects changes without reprocessing unchanged data.
- **Automated Visualization**: Automatically generates visualizations for detected anomalies, aiding in quick analysis and decision-making.
