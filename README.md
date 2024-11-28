# ETL Project with Macroeconomic and Stock Price Analysis

## Project Description

This project based on PySpark-FAKER Data. (Extract, Transform, Load) system built using Dagster, designed to analyze macroeconomic data and stock prices. The goal is to understand the impact of macroeconomic indicators, trading volume, and news sentiment on stock prices across various sectors.

  ## Functionalities
This project offers:
- Automated data extraction and transformation using Dagster.
- Macroeconomic and stock price correlation analysis.
- Seasonality detection and visualization.
- Trading volume anomaly detection and analysis.
- News sentiment analysis and its impact on stock prices.
- Real-time monitoring for price changes and anomaly detection.

## Installation

2. **Install Dependencies**:
   After cloning the repository, navigate to the project directory and install all necessary dependencies using the following command:
   ```bash
   pip install -r requirements.txt
   ```

   Ensure you have Python and `pip` installed in your environment.

## Usage

Follow these steps to run the project:

1. **Navigate to the Project Directory**:
   Change your directory to the project folder:
   ```bash
   cd /home/urban/Projekty/dagster2/my-dagster-project
   ```

2. **Start Dagster**:
   Launch the Dagster development server to access the ETL pipeline and monitor the jobs:
   ```bash
   dagster dev
   ```

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
  

1. **Clone the Repository**:
   To get started, clone the repository using the following command:
   ```bash
   git clone https://github.com/ur64n/dagster_etl_analytics
