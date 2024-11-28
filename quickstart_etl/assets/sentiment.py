from dagster import asset
import pandas as pd
import os
from . import constants
from textblob import TextBlob
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# === Pipeline: Sentiment Analysis - General Sentiment === #

@asset(
    deps=["clean_and_structure_data"]
)
def load_and_prepare_sentiment_data(context) -> pd.DataFrame:
    """Load, filter, and prepare structured news and stock data for sentiment analysis."""
    
    # File path definitions
    news_data_path = os.path.join(constants.STRUCTURED_DATA_PATH, "structured_news_data.csv")
    stock_data_path = os.path.join(constants.STRUCTURED_DATA_PATH, "structured_stock_data.csv")
    
    # Loading data from CSV files
    news_data_df = pd.read_csv(news_data_path)
    stock_data_df = pd.read_csv(stock_data_path)
    
    # Filtering necessary columns
    news_data_filtered = news_data_df[['date', 'content', 'symbol']].dropna(subset=['symbol'])
    stock_data_filtered = stock_data_df[['date', 'symbol', 'close_price', 'sector']]
    
    # Merging data on 'date' and 'symbol' columns - inner join
    combined_df = pd.merge(news_data_filtered, stock_data_filtered, on=['date', 'symbol'], how='inner')
    
    return combined_df

@asset()
def analyze_sentiment(load_and_prepare_sentiment_data: pd.DataFrame) -> pd.DataFrame:
    """Perform sentiment analysis on the news data and add sentiment polarity to the dataset."""
    
    # Helper function to determine binary sentiment (positive/negative)
    def get_sentiment(text):
        polarity = TextBlob(text).sentiment.polarity
        return 'positive' if polarity >= 0 else 'negative'

    # Adding 'sentiment' column with positive/negative values based on 'content'
    load_and_prepare_sentiment_data['sentiment'] = load_and_prepare_sentiment_data['content'].apply(get_sentiment)
    
    # Returning processed DataFrame with additional 'sentiment' column
    return load_and_prepare_sentiment_data

@asset()
def visualize_sentiment_impact(analyze_sentiment: pd.DataFrame) -> None:
    """Generates and saves a clearer visualization of monthly aggregated stock prices alongside news sentiment."""

    # Path to save the plot
    results_path = os.path.join(constants.RESULTS_DATA_PATH, "aggregated_sentiment_vs_stock_price.png")
    os.makedirs(constants.RESULTS_DATA_PATH, exist_ok=True)

    # Preparing data for visualization
    df = analyze_sentiment.copy()
    df['date'] = pd.to_datetime(df['date'])

    # Monthly aggregation: average closing price and dominant sentiment
    monthly_df = df.resample('M', on='date').agg({
        'close_price': 'mean',       # Average closing price
        'sentiment': lambda x: x.mode()[0]  # Most frequent sentiment
    }).reset_index()

    # Setting up the plot
    plt.figure(figsize=(14, 8))

    # Stock price plot - draw the line first so points and labels are on top
    sns.lineplot(data=monthly_df, x='date', y='close_price', linewidth=2.0, label="Stock Price", color="dimgray", zorder=1)

    # Adding color according to news sentiment (all in turquoise)
    for sentiment in monthly_df['sentiment'].unique():
        sentiment_data = monthly_df[monthly_df['sentiment'] == sentiment]
        
        # Drawing points on top of the line
        plt.scatter(
            sentiment_data['date'], sentiment_data['close_price'], color="turquoise", s=60,
            label=f"Sentiment: {sentiment}", alpha=0.7, zorder=2  # zorder sets the order
        )

        # Adding value labels below the points
        for i, row in sentiment_data.iterrows():
            y_value = row['close_price']
            plt.text(
                row['date'], y_value - 20,  # Set below the point
                f"{int(y_value)}", ha='center', color="turquoise", fontsize=10, zorder=3  # zorder for labels
            )

    # Axis settings and plot title
    plt.title("Monthly Impact of News Sentiment on Stock Prices", fontsize=16)
    plt.xlabel("Date", fontsize=12)
    plt.ylabel("Average Stock Close Price", fontsize=12)
    plt.legend(loc="upper left", fontsize=10)
    plt.xticks(rotation=45)
    plt.yticks(np.arange(int(monthly_df['close_price'].min()) - 50, int(monthly_df['close_price'].max()) + 50, 50))

    # Adding grid
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)

    plt.tight_layout()

    # Saving the plot
    plt.savefig(results_path)
    plt.close()

    # Information about saving the plot
    print(f"Plot saved to file: {results_path}")

# === Pipeline2: Sentiment2 Impact - Technology Sector === #

@asset(
    deps=["clean_and_structure_data"]
)
def load_and_prepare_sentiment2_data() -> pd.DataFrame:
    """
    Load and filter news and stock data for the technology sector.
    """
    # Defining file paths for data
    news_data_path = os.path.join(constants.STRUCTURED_DATA_PATH, "structured_news_data.csv")
    stock_data_path = os.path.join(constants.STRUCTURED_DATA_PATH, "structured_stock_data.csv")
    
    # Loading data from CSV files
    news_data = pd.read_csv(news_data_path, usecols=["date", "content", "symbol"])
    stock_data = pd.read_csv(stock_data_path, usecols=["date", "close_price", "symbol", "sector"])
    
    # Converting 'date' column to date format for both datasets
    news_data["date"] = pd.to_datetime(news_data["date"], format=constants.DATE_FORMAT)
    stock_data["date"] = pd.to_datetime(stock_data["date"], format=constants.DATE_FORMAT)
    
    # Filtering data for the technology sector
    tech_stock_data = stock_data[stock_data["sector"] == "Technology"]
    
    # Merging data based on symbol and date (inner join)
    filtered_data = pd.merge(news_data, tech_stock_data, on=["date", "symbol"], how="inner")
    
    return filtered_data

@asset()
def analyze_technology_sentiment2(load_and_prepare_sentiment2_data: pd.DataFrame) -> pd.DataFrame:
    """
    Perform sentiment analysis on news data for the technology sector and attach sentiment score.
    """
    tech_data = load_and_prepare_sentiment2_data.copy()

    # Adding a column with sentiment analysis result
    tech_data['sentiment'] = tech_data['content'].apply(lambda text: TextBlob(text).sentiment.polarity)
    
    # Averaging sentiment based on date and symbol
    sentiment_summary = tech_data.groupby(['date', 'symbol']).agg({
        'sentiment': 'mean',
        'close_price': 'first'
    }).reset_index()

    return sentiment_summary

@asset()
def analyze_tech_sentiment_impact(analyze_technology_sentiment2: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze the impact of sentiment on stock price changes in the technology sector.
    """
    sentiment_df = analyze_technology_sentiment2.copy()

    # Calculating day-to-day closing price change
    sentiment_df['price_change'] = sentiment_df.groupby('symbol')['close_price'].pct_change() * 100
    
    # Shifting sentiment values by one day forward to see the impact of sentiment on future price changes
    sentiment_df['previous_day_sentiment'] = sentiment_df.groupby('symbol')['sentiment'].shift(1)

    # Filtering data to exclude days without price changes (e.g., first days or missing data)
    sentiment_analysis_result = sentiment_df.dropna(subset=['price_change', 'previous_day_sentiment']).reset_index(drop=True)

    # Saving the result to a CSV file
    monitoring_path = os.path.join(constants.MONITORING_DATA_PATH, "analyze_tech_sentiment_impact.csv")
    sentiment_analysis_result.to_csv(monitoring_path, index=False)

    return sentiment_analysis_result
    
@asset()
def visualize_tech_sentiment2_impact(analyze_tech_sentiment_impact: pd.DataFrame) -> None:
    """
    Generate and save a scatter plot visualizing the impact of sentiment on future stock price changes.
    """
    # Path to save the plot
    results_path = os.path.join(constants.RESULTS_DATA_PATH, "sentiment_impact_on_price_change.png")
    os.makedirs(constants.RESULTS_DATA_PATH, exist_ok=True)

    # Creating the plot
    plt.figure(figsize=(12, 6))
    plt.scatter(analyze_tech_sentiment_impact['previous_day_sentiment'], analyze_tech_sentiment_impact['price_change'],
                color='cyan', label="Price Change vs Previous Day Sentiment")
    
    # Plot settings
    plt.xlabel("Previous Day Sentiment Score")
    plt.ylabel("Price Change (%)")
    plt.title("Impact of Previous Day Sentiment on Future Price Changes")
    plt.grid(True, color='white', linestyle='--', linewidth=0.5)
    plt.legend()

    # Plot background
    plt.gca().set_facecolor("#7a7a7a")  # Dark gray plot background
    plt.gcf().set_facecolor("#e6e6e6")  # Light gray outer background

    # Saving the plot
    plt.tight_layout()
    plt.savefig(results_path)
    plt.close()
    print(f"Plot saved to file: {results_path}")
