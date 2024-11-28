from dagster import asset
from sqlalchemy import text
import pandas as pd
import os
from . import constants
from ..partitions import monthly_partition

@asset(required_resource_keys={"postgres_db"},
       partitions_def=monthly_partition)
def extract_all_data(context):
    """
    Asset1: Extraction of all required tables from PostgreSQL database.
    Saves the extracted data to RAW_DATA_PATH in CSV format.
    """
    conn = context.resources.postgres_db
    partition_key = context.partition_key

    # Define table names for each group
    macro_tables = ["macro_data.macro_data1", "macro_data.macro_data2", "macro_data.macro_data3", "macro_data.macro_data4"]
    news_tables_3col = ["news_data.news_data1", "news_data.news_data4"]
    news_tables_4col = ["news_data.news_data2", "news_data.news_data3"]
    stock_tables = ["stock_data.stock_data1", "stock_data.stock_data2", "stock_data.stock_data3", "stock_data.stock_data4"]

    # Helper function for extracting and saving tables to CSV
    def load_and_save_tables(tables, filename, columns, date_column):
        data_frames = []
        for table in tables:
            # Pobierz dane z bazy
            query = f"SELECT * FROM {table}"
            df = pd.read_sql(text(query), conn)
            df.columns = columns  # Assign target column names

            # Oczyść kolumnę z datą
            df['date'] = df[date_column].str.extract(r'(\d{4}-\d{2}-\d{2})')

            # Filtrowanie według klucza partycji
            df = df[df['date'].str.startswith(partition_key[:7])]

            data_frames.append(df)
        
        combined_df = pd.concat(data_frames, ignore_index=True)
        path = os.path.join(constants.RAW_DATA_PATH, filename)

        # Load existing data if the file exists
        if os.path.exists(path):
            existing_df = pd.read_csv(path)
            combined_df = pd.concat([existing_df, combined_df], ignore_index=True)

        combined_df.to_csv(path, index=False)

    # Extraction and saving each category
    load_and_save_tables(macro_tables, "combined_macro_data.csv", ["date", "indicator", "value"], "date")
    load_and_save_tables(news_tables_3col, "combined_news_data_3col.csv", ["date", "title", "content"], "date")
    load_and_save_tables(news_tables_4col, "combined_news_data_4col.csv", ["date", "title", "content", "symbol"], "date")
    load_and_save_tables(stock_tables, "combined_stock_data.csv", ["date", "symbol", "open_price", "close_price", "volume", "sector"], "date")

    return {
        "macro_data_path": os.path.join(constants.RAW_DATA_PATH, "combined_macro_data.csv"),
        "news_data_3col_path": os.path.join(constants.RAW_DATA_PATH, "combined_news_data_3col.csv"),
        "news_data_4col_path": os.path.join(constants.RAW_DATA_PATH, "combined_news_data_4col.csv"),
        "stock_data_path": os.path.join(constants.RAW_DATA_PATH, "combined_stock_data.csv")
    }

@asset(deps=["extract_all_data"],
       partitions_def=monthly_partition)
def clean_and_structure_data(context):
    """
    Asset2: Loads data from RAW_DATA_PATH, cleans, structures, and sorts it by date.
    1. Removes unwanted text patterns from specified columns.
    2. Sorts each DataFrame by the 'date' column in ascending order.
    3. Saves the final structured data to STRUCTURED_DATA_PATH.
    """

    # Load datasets
    macro_data = pd.read_csv(os.path.join(constants.RAW_DATA_PATH, "combined_macro_data.csv"))
    
    # Load news data with 3 and 4 columns, fill missing 'symbol' in 3-column data for consistency
    news_data_3col = pd.read_csv(os.path.join(constants.RAW_DATA_PATH, "combined_news_data_3col.csv"))
    news_data_4col = pd.read_csv(os.path.join(constants.RAW_DATA_PATH, "combined_news_data_4col.csv"))
    news_data_3col["symbol"] = None  # Fill missing 'symbol' column in 3-col data

    # Function to clean specific unwanted patterns from a DataFrame's columns
    def clean_text_patterns(df, columns):
        for col in columns:
            # Remove specific patterns at the beginning and end of values
            df[col] = df[col].str.replace(r'^({date":"|title":"|content":"|symbol":"|})|["\'}]+$', '', regex=True)
        return df

    # Apply text cleaning to news data
    news_data_3col = clean_text_patterns(news_data_3col, ["date", "title", "content"])
    news_data_4col = clean_text_patterns(news_data_4col, ["date", "title", "content", "symbol"])

    # Merge the two news data DataFrames
    news_data = pd.concat([news_data_3col, news_data_4col], ignore_index=True)

    # Load stock data and apply cleaning
    stock_data = pd.read_csv(os.path.join(constants.RAW_DATA_PATH, "combined_stock_data.csv"))
    stock_data = clean_text_patterns(stock_data, ["date", "symbol"])

    # Sort each DataFrame by date in ascending order
    macro_data = macro_data.sort_values(by="date").reset_index(drop=True)
    news_data = news_data.sort_values(by="date").reset_index(drop=True)
    stock_data = stock_data.sort_values(by="date").reset_index(drop=True)

    # Save cleaned, sorted, and structured data
    macro_data_path = os.path.join(constants.STRUCTURED_DATA_PATH, "structured_macro_data.csv")
    news_data_path = os.path.join(constants.STRUCTURED_DATA_PATH, "structured_news_data.csv")
    stock_data_path = os.path.join(constants.STRUCTURED_DATA_PATH, "structured_stock_data.csv")

    macro_data.to_csv(macro_data_path, index=False)
    news_data.to_csv(news_data_path, index=False)
    stock_data.to_csv(stock_data_path, index=False)

    return {
        "structured_macro_data_path": macro_data_path,
        "structured_news_data_path": news_data_path,
        "structured_stock_data_path": stock_data_path
    }