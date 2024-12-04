import pandas as pd
from pyspark.sql import SparkSession

def load_uploaded_csv(spark: SparkSession, uploaded_files):
    try:
        table_names = []
        for uploaded_file in uploaded_files:
            table_name = uploaded_file.name.split('.')[0]
            table_names.append(table_name)
            pd_df = pd.read_csv(uploaded_file)

            spark_df = spark.createDataFrame(pd_df)
            spark_df.createOrReplaceTempView(table_name)

        return table_names
    except Exception as e:
        print(f"Error loading CSV files: {e}")
        return None
