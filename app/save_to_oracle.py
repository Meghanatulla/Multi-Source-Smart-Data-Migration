from pyspark.sql import DataFrame

def save_to_oracle(df: DataFrame, jdbc_url: str, user: str, password: str):
    try:
        df.write.format("jdbc").options(
            url=jdbc_url,
            driver="oracle.jdbc.driver.OracleDriver",
            dbtable="target_table_name",
            user=user,
            password=password
        ).mode("append").save()
    except Exception as e:
        raise Exception(f"Error saving data to Oracle: {e}")
