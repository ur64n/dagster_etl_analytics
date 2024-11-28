# ETL Project with Macroeconomic and Stock Price Analysis

## Project based on PySpark-FAKER Data

## Project Description

This project is (Extract, Transform, Load) system built using Dagster, designed to analyze macroeconomic data and stock prices. The goal is to understand the impact of macroeconomic indicators, trading volume, and news sentiment on stock prices across various sectors.

## Project Structure

### Main Modules

1. **db_pipeline**:
   - **extract_all_data**: Extracts data from a PostgreSQL database and saves it to CSV files.
   - **clean_and_structure_data**: Cleans and structures the extracted data for analysis.

2. **seasonity**:
   - **load_and_prepare_seasonity_data**: Loads and prepares data for seasonality analysis.
   - **analyze_seasonality**: Identifies months with the highest and lowest average stock prices for each sector.
   - **visualize_seasonality**: Generates visualizations of seasonal patterns in stock prices.

3. **Correlation**:
   - **process_combined_data**: Merges macroeconomic data with stock data for correlation analysis.
   - **analyze_and_visualize_correlation**: Calculates and visualizes the correlation matrix between macroeconomic indicators and stock prices.

4. **trading volume**:
   - **load_and_filter_volume_data**: Loads and filters stock data for volume analysis.
   - **detect_volume_anomalies**: Detects significant changes in trading volume.
   - **analyze_price_impact_of_anomalies**: Analyzes the impact of volume anomalies on subsequent price changes.
   - **visualize_price_impact**: Visualizes the impact of volume anomalies on future price changes.

5. **sentiment**:
   - **load_and_prepare_sentiment_data**: Prepares news and stock data for sentiment analysis.
   - **analyze_sentiment**: Performs sentiment analysis on news data.
   - **visualize_sentiment_impact**: Visualizes the impact of news sentiment on stock prices.
   - **Price Change Sensor**: Monitors significant changes in stock prices based on sentiment analysis.
   - It is part of the ETL pipeline and is specifically linked to the `tech_sentiment_job`. This sensor continuously checks for anomalies in price changes and triggers further actions if certain conditions are met.
   The **Price Change Sensor** is designed to monitor significant changes in stock prices based on sentiment analysis.

6. **Sensorequest**:
   - **Data Monitoring**: Reads data from a CSV file containing the results of sentiment analysis on stock prices.
   - **Change Detection**: Calculates a hash of the current data to detect any changes since the last evaluation.
   - **Threshold Evaluation**: Checks for any price changes that exceed a predefined threshold (e.g., 10%).
   - **Outlier Handling**: Stores outliers for further analysis.
   - **Visualization Trigger**: Initiates the visualization process to create a boxplot of the detected price change outliers.
   - **Run Request**: Generates a new run request to handle the detected changes if conditions are met.
