from dagster import sensor, RunRequest, SensorEvaluationContext, SensorResult
from ..jobs import tech_sentiment_job
from ..assets.sensorequest import store_outliers, visualize_price_change_outliers
import pandas as pd
import os
from ..assets import constants
import hashlib

PRICE_CHANGE_THRESHOLD = 10  # Example threshold value

def calculate_hash(df: pd.DataFrame) -> str:
    """Calculate a hash of the DataFrame to detect changes."""
    return hashlib.md5(pd.util.hash_pandas_object(df, index=True).values).hexdigest()

@sensor(job=tech_sentiment_job)
def price_change_sensor(context: SensorEvaluationContext):
    # Path to the CSV file
    data_path = os.path.join(constants.MONITORING_DATA_PATH, "analyze_tech_sentiment_impact.csv")

    # Load data from the CSV file
    df = pd.read_csv(data_path)

    # Calculate hash of the current data
    current_hash = calculate_hash(df)

    # Get the last hash from the cursor
    last_hash = context.cursor if context.cursor else ""

    # Check if the hash has changed
    if current_hash != last_hash:
        # Check if any values in the 'price_change' column exceed the threshold
        outliers = df[df['price_change'].abs() > PRICE_CHANGE_THRESHOLD]

        if not outliers.empty:
            # Save outliers in the asset
            store_outliers(outliers)

            # Trigger the visualization asset
            visualize_price_change_outliers()

            # Return the sensor result with the new cursor
            return SensorResult(
                run_requests=[RunRequest(run_key=f"price_change_outliers_{current_hash}")],
                cursor=current_hash
            )

    # If there are no changes, return an empty result
    return SensorResult(run_requests=[], cursor=last_hash)