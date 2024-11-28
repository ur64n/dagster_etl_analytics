from dagster import asset
import pandas as pd
import os
from . import constants
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.lines as mlines

@asset(
    deps=["clean_and_structure_data"]
)
def load_and_prepare_seasonity_data() -> pd.DataFrame:
    """
    Load and prepare stock data for seasonality analysis.
    """

    # Path to the file
    stock_data_path = os.path.join(constants.STRUCTURED_DATA_PATH, "structured_stock_data.csv")

    # Loading data from CSV file
    stock_data_df = pd.read_csv(stock_data_path)

    # Converting 'date' column to date format
    stock_data_df['date'] = pd.to_datetime(stock_data_df['date'], format=constants.DATE_FORMAT)

    # Adding 'month' column based on 'date' column for seasonality analysis
    stock_data_df['month'] = stock_data_df['date'].dt.month

    # Aggregating monthly data for each sector
    monthly_sector_data = stock_data_df.groupby(['sector', 'month']).agg({
        'close_price': 'mean',
        'volume': 'sum'
    }).reset_index()

    return monthly_sector_data

@asset()
def analyze_seasonality(load_and_prepare_seasonity_data: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze seasonality by identifying the months with the highest and lowest average close prices for each sector.
    """

    # Identifying the month with the highest and lowest average close price for each sector
    seasonality_analysis = load_and_prepare_seasonity_data.groupby('sector').apply(
        lambda x: pd.DataFrame({
            'highest_month': [x.loc[x['close_price'].idxmax(), 'month']],
            'highest_avg_close': [x['close_price'].max()],
            'lowest_month': [x.loc[x['close_price'].idxmin(), 'month']],
            'lowest_avg_close': [x['close_price'].min()]
        })
    ).reset_index(drop=False)

    return seasonality_analysis

@asset()
def visualize_seasonality(analyze_seasonality: pd.DataFrame) -> None:
    """
    Generates and saves a visualization of the seasonal patterns in stock prices across sectors.
    """
    # Path to save the plot
    results_path = os.path.join(constants.RESULTS_DATA_PATH, "seasonality_analysis.png")
    os.makedirs(constants.RESULTS_DATA_PATH, exist_ok=True)

    # Setting up the plot
    plt.figure(figsize=(12, 8))

    # Defining colors for each sector
    colors = {
        'Automotive': ('#1f77b4', '#ff7f0e'),
        'Consumer': ('#2ca02c', '#d62728'),
        'Technology': ('#9467bd', '#8c564b')
    }

    # Drawing bar chart for highest and lowest close prices
    for sector in analyze_seasonality['sector'].unique():
        sector_data = analyze_seasonality[analyze_seasonality['sector'] == sector]

        plt.bar(
            sector_data['highest_month'],
            sector_data['highest_avg_close'],
            color=colors[sector][0],
            label=f"{sector} - Highest",
            alpha=0.7
        )

        plt.bar(
            sector_data['lowest_month'],
            sector_data['lowest_avg_close'],
            color=colors[sector][1],
            label=f"{sector} - Lowest",
            alpha=0.7
        )

    # Adding separated groups in the legend
    legend_elements = [
        mpatches.Patch(color=colors['Automotive'][0], label='Automotive - Highest'),
        mpatches.Patch(color=colors['Automotive'][1], label='Automotive - Lowest'),
        mlines.Line2D([], [], color="gray", linestyle="--"),  # separator
        mpatches.Patch(color=colors['Consumer'][0], label='Consumer - Highest'),
        mpatches.Patch(color=colors['Consumer'][1], label='Consumer - Lowest'),
        mlines.Line2D([], [], color="gray", linestyle="--"),  # separator
        mpatches.Patch(color=colors['Technology'][0], label='Technology - Highest'),
        mpatches.Patch(color=colors['Technology'][1], label='Technology - Lowest')
    ]

    # Adding legend with elements and separators
    plt.legend(handles=legend_elements, loc="upper left", fontsize=10)

    # Setting axis labels and plot title
    plt.xlabel("Month", fontsize=12)
    plt.ylabel("Average Stock Close Price", fontsize=12)
    plt.title("Seasonal Patterns in Stock Prices by Sector", fontsize=16)

    # Configuring X-axis with month names
    plt.xticks(
        ticks=range(1, 13),
        labels=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        rotation=0
    )

    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()

    # Saving the plot
    plt.savefig(results_path)
    plt.close()

    # Information about saving the plot
    print(f"Plot saved to file: {results_path}")
