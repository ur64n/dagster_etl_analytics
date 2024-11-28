from dagster import asset
import pandas as pd
import matplotlib.pyplot as plt
import os
from . import constants

# Helper function to store outliers
def store_outliers(outliers: pd.DataFrame):
    outliers.to_csv(os.path.join(constants.RESULTS_DATA_PATH, "outliers.csv"), index=False)

@asset
def visualize_price_change_outliers() -> None:
    """
    Generate a boxplot for price change outliers and save it to RESULTS_DATA_PATH.
    """
    # Load outliers from CSV file
    outliers_df = pd.read_csv(os.path.join(constants.RESULTS_DATA_PATH, "outliers.csv"))

    # Path to save the plot
    results_path = os.path.join(constants.RESULTS_DATA_PATH, "price_change_outliers_boxplot.png")
    os.makedirs(constants.RESULTS_DATA_PATH, exist_ok=True)

    # Create boxplot
    plt.figure(figsize=(10, 6))
    plt.boxplot(outliers_df['price_change'], vert=False, patch_artist=True)
    plt.title("Boxplot of Price Change Outliers")
    plt.xlabel("Price Change (%)")
    plt.grid(True, linestyle='--', linewidth=0.5)

    # Save the plot
    plt.tight_layout()
    plt.savefig(results_path)
    plt.close()
    print(f"Plot saved to file: {results_path}")
