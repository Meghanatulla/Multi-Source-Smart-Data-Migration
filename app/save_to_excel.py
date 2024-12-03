import os
from pyspark.sql import DataFrame

def save_to_excel(result_df: DataFrame, output_path: str):
    try:
        pandas_df = result_df.toPandas()
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        pandas_df.to_excel(output_path, index=False)
    except Exception as e:
        print(f"Error saving to Excel: {e}")
