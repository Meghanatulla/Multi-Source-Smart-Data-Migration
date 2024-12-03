from pyspark.sql import SparkSession

def execute_user_query(spark: SparkSession, query: str):
    try:
        result_df = spark.sql(query)
        return result_df
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
