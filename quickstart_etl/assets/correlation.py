from dagster import asset
import pandas as pd
import os
from . import constants
import seaborn as sns
import matplotlib.pyplot as plt

@asset(
    deps=["clean_and_structure_data"]
)
def process_combined_data(context) -> pd.DataFrame:
    """
    Asset to load, transform, and merge macro and stock data.
    """

    # Paths to CSV files
    macro_data_path = os.path.join(constants.STRUCTURED_DATA_PATH, "structured_macro_data.csv")
    stock_data_path = os.path.join(constants.STRUCTURED_DATA_PATH, "structured_stock_data.csv")

    # Loading macroeconomic data with parse_dates and usecols
    macro_data_df = pd.read_csv(
        macro_data_path,
        parse_dates=['date'],
        usecols=['date', 'indicator', 'value']
    )

    # Filtering and aggregating
    macro_data_filtered = macro_data_df[macro_data_df['indicator'].isin(["Inflation", "Unemployment", "Interest Rates"])]

    # Monthly grouping and pivoting of macroeconomic data
    macro_data_monthly = (
        macro_data_filtered
        .groupby([macro_data_filtered['date'].dt.to_period("M"), 'indicator'])['value']
        .mean()
        .unstack('indicator')
        .reset_index()
    )
    macro_data_monthly['date'] = macro_data_monthly['date'].dt.to_timestamp()
    macro_data_monthly.columns = ['date', 'inflation_value', 'unemployment_value', 'interest_rates_value']

    # Loading stock data with parse_dates and usecols
    stock_data_df = pd.read_csv(
        stock_data_path,
        parse_dates=['date'],
        usecols=['date', 'sector', 'symbol', 'close_price']
    )

    # Filtering and aggregating stock data
    stock_data_filtered = stock_data_df[stock_data_df['sector'].isin(["Technology", "Automotive"])]

    # Monthly grouping and average closing price aggregation
    stock_data_monthly = (
        stock_data_filtered
        .groupby([stock_data_filtered['date'].dt.to_period("M"), 'sector', 'symbol'])['close_price']
        .mean()
        .reset_index()
    )
    stock_data_monthly['date'] = stock_data_monthly['date'].dt.to_timestamp()
    stock_data_monthly.columns = ['date', 'sector', 'symbol', 'average_close_price']

    # Merging macroeconomic data with stock data based on the 'date' column
    combined_data = pd.merge(
        stock_data_monthly,
        macro_data_monthly,
        on='date',
        how='inner'
    )

    # Displaying result information in logs
    context.log.info("Final DF transformed and merged.")
    context.log.info(f"Final DF:\n{combined_data.head()}")

    return combined_data

@asset()
def analyze_and_visualize_correlation(context, process_combined_data: pd.DataFrame) -> None:
    """
    Calculates correlations between macroeconomic indicators and stock closing prices,
    creates a correlation chart, and saves it in the project directory.
    """

    # Calculating correlation
    correlation_matrix = process_combined_data[
        ["inflation_value", "unemployment_value", "interest_rates_value", "average_close_price"]
    ].corr()

    # Path for saving the chart
    output_path = os.path.join(constants.RESULTS_DATA_PATH, "correlation_matrix.png")

    # Creating a correlation chart with improved readability
    plt.figure(figsize=(10, 8))  
    sns.heatmap(
        correlation_matrix, 
        annot=True, 
        cmap="coolwarm", 
        cbar=True,
        annot_kws={"size": 12},  
        fmt=".2f",  
        vmin=-1, vmax=1  
    )
    plt.title("Correlation Matrix between Macroeconomic Indicators and Stock Prices", fontsize=14)
    plt.xticks(rotation=45, ha="right", fontsize=12)  
    plt.yticks(rotation=0, fontsize=12)  
    plt.tight_layout()  
    plt.savefig(output_path, dpi=300)  
    plt.close()

    context.log.info(f"Heatmap saved in: {output_path}")