from dagster import asset
import pandas as pd
import os
from . import constants
import matplotlib.pyplot as plt
import seaborn as sns

@asset(deps=["clean_and_structure_data"])
def load_and_filter_volume_data() -> pd.DataFrame:
    """
    Load stock data and filter necessary columns for volume analysis.
    """
    # Define the file path
    stock_data_path = os.path.join(constants.STRUCTURED_DATA_PATH, "structured_stock_data.csv")
    
    # Load data from CSV file using usecols and dtype
    stock_data_df = pd.read_csv(
        stock_data_path,
        usecols=['date', 'symbol', 'volume', 'close_price'],
        dtype={'symbol': 'category', 'volume': 'float32', 'close_price': 'float32'}
    )
    
    # Convert 'date' column to date format
    stock_data_df['date'] = pd.to_datetime(stock_data_df['date'], format=constants.DATE_FORMAT)
    
    return stock_data_df

@asset()
def aggregate_volume_data(load_and_filter_volume_data: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate stock data to calculate total volume and average close price per month for each symbol.
    """
    # Transform 'date' column to month
    load_and_filter_volume_data['month'] = load_and_filter_volume_data['date'].dt.to_period("M")

    # Aggregate data: sum volume and average close price
    monthly_aggregation = load_and_filter_volume_data.groupby(['symbol', 'month']).agg({
        'volume': 'sum',        # Total volume per month
        'close_price': 'mean'   # Average close price per month
    }).reset_index()

    # Convert 'month' column back to datetime for clarity
    monthly_aggregation['month'] = monthly_aggregation['month'].dt.to_timestamp()

    return monthly_aggregation

@asset()
def detect_volume_anomalies(aggregate_volume_data: pd.DataFrame) -> pd.DataFrame:
    """
    Detect anomalies in trading volume patterns by identifying significant changes month-over-month.
    """
    # Calculate percentage change in volume for each symbol month-over-month
    aggregate_volume_data['volume_change'] = aggregate_volume_data.groupby('symbol')['volume'].pct_change() * 100

    # Set anomaly threshold - for example, if volume change exceeds 50%
    anomaly_threshold = 50
    aggregate_volume_data['is_anomaly'] = aggregate_volume_data['volume_change'].abs() > anomaly_threshold

    # Filter only months that show anomalies
    anomalies_df = aggregate_volume_data[aggregate_volume_data['is_anomaly']].reset_index(drop=True)

    return anomalies_df

@asset()
def analyze_price_impact_of_anomalies(detect_volume_anomalies: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze the impact of volume anomalies on subsequent price changes.
    """
    # Calculate monthly change in close price
    detect_volume_anomalies['price_change'] = detect_volume_anomalies.groupby('symbol')['close_price'].pct_change() * 100

    # Shift price change by one month forward to see the impact of anomalies on subsequent price changes
    detect_volume_anomalies['future_price_change'] = detect_volume_anomalies.groupby('symbol')['price_change'].shift(-1)

    # Filter only records that show volume anomaly
    impact_analysis_df = detect_volume_anomalies[detect_volume_anomalies['is_anomaly']].reset_index(drop=True)

    return impact_analysis_df

@asset()
def visualize_price_impact(analyze_price_impact_of_anomalies: pd.DataFrame) -> None:
    """
    Generate and save a visualization of the impact of volume anomalies on subsequent price changes.
    """
    # Path to save the plot
    results_path = os.path.join(constants.RESULTS_DATA_PATH, "volume_anomalies_price_impact.png")
    os.makedirs(constants.RESULTS_DATA_PATH, exist_ok=True)

    # Create plot with gray background
    fig, ax = plt.subplots(figsize=(12, 6))
    fig.patch.set_facecolor('#e6e6e6')  # Set the background color of the entire figure
    ax.set_facecolor('#7a7a7a')  # Set the background color of the plot interior
    plt.scatter(analyze_price_impact_of_anomalies['month'], analyze_price_impact_of_anomalies['future_price_change'], c='cyan', label="Price Change After Anomaly")
    
    # Axis and title settings
    plt.xlabel("Date")
    plt.ylabel("Future Price Change (%)")
    plt.title("Impact of Volume Anomalies on Future Price Changes")
    plt.legend()

    # Add white grid
    plt.grid(color='white', linestyle='--', linewidth=0.5)  # White grid lines

    # X-axis settings
    plt.xticks(rotation=0)

    # Save plot
    plt.tight_layout()
    plt.savefig(results_path)
    plt.close()

    print(f"Plot saved to file: {results_path}")