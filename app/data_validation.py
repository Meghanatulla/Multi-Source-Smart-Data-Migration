import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def initialize_spark(app_name="DataValidationApp"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark


# ------------------- Data Loading Functions -------------------

def load_csv(spark, file_path, infer_schema=True, header=True):
    try:
        df = spark.read.csv(file_path, inferSchema=infer_schema, header=header)
        return df
    except Exception as e:
        raise ValueError(f"Error loading CSV file: {e}")


def load_from_database(spark, url, table_name, user, password, driver="com.mysql.cj.jdbc.Driver"):
    try:
        df = spark.read.format("jdbc").option("url", url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver).load()
        return df
    except Exception as e:
        raise ValueError(f"Error loading data from database: {e}")


def load_flat_file(spark, file_path, file_format="parquet"):
    try:
        df = spark.read.format(file_format).load(file_path)
        return df
    except Exception as e:
        raise ValueError(f"Error loading flat file: {e}")


# ------------------- Data Validation Functions -------------------

def check_nulls(df, columns):
    available_columns = df.columns
    invalid_columns = [col for col in columns if col not in available_columns]
    if invalid_columns:
        raise ValueError(f"Columns not found in DataFrame: {', '.join(invalid_columns)}")

    null_count = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in columns]).collect()[0]
    return {col: null_count[col] for col in columns}


def check_duplicates(df, columns):
    available_columns = df.columns
    invalid_columns = [col for col in columns if col not in available_columns]
    if invalid_columns:
        raise ValueError(f"Columns not found in DataFrame: {', '.join(invalid_columns)}")

    duplicate_count = df.groupBy(columns).count().filter("count > 1").count()
    return duplicate_count


def check_data_types(df, expected_schema):
    schema_check = {}
    for column, expected_type in expected_schema.items():
        actual_type = dict(df.dtypes)[column] if column in df.columns else None
        schema_check[column] = (actual_type == expected_type)
    return schema_check


def validate_data(df, expected_schema, columns_to_check):
    results = {}
    null_check = check_nulls(df, columns_to_check)
    results['null_check'] = null_check

    duplicate_check = check_duplicates(df, columns_to_check)
    results['duplicate_check'] = duplicate_check

    data_type_check = check_data_types(df, expected_schema)
    results['data_type_check'] = data_type_check

    return results
