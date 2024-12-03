import streamlit as st
import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from cryptography.fernet import Fernet
from app.load_csv import load_uploaded_csv
from app.transform import execute_user_query
from app.save_to_excel import save_to_excel
from app.save_to_oracle import save_to_oracle
import tempfile


pyl = sys.executable
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jdk-11"
os.environ['PYSPARK_PYTHON'] = pyl


log_file = "app.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


spark = SparkSession.builder.appName("StreamlitMultiSourceProcessing").getOrCreate()


st.title("Multi-Source Smart Data Migration")
st.markdown(
    "Upload your data from CSV, MySQL, Oracle databases, or flat files, write SQL queries, validate data, aggregate results, and download or migrate them."
)


if "data_loaded" not in st.session_state:
    st.session_state.data_loaded = False

if "table_names" not in st.session_state:
    st.session_state.table_names = []

if "result_df" not in st.session_state:
    st.session_state.result_df = None


PAGE_SIZE = 10


key = Fernet.generate_key()
cipher_suite = Fernet(key)

st.sidebar.header("Data Source Configuration")
data_source = st.sidebar.selectbox(
    "Select Data Source Type:",
    options=["CSV", "MySQL Database", "Oracle Database", "Flat File"]
)


# Step 1: Data Source-Specific Input
if data_source == "CSV":
    st.markdown("### Upload CSV Files")
    uploaded_files = st.file_uploader("Upload CSV files", accept_multiple_files=True, type=["csv"])
    if uploaded_files:
        try:
            table_names = load_uploaded_csv(spark, uploaded_files)
            if table_names:
                st.session_state.data_loaded = True
                st.session_state.table_names = table_names
                st.success(f"Successfully loaded tables: {', '.join(table_names)}")
                logging.info(f"CSV files loaded successfully: {', '.join(table_names)}")
            else:
                st.error("Failed to load CSV files. Please check your files and try again.")
                logging.warning("CSV loading failed: Invalid files or format.")
        except Exception as e:
            st.error(f"An error occurred while loading CSV files: {e}")
            logging.error(f"Error loading CSV files: {e}")

elif data_source == "MySQL Database":
    st.markdown("### Connect to MySQL Database")
    host = st.text_input("MySQL Host:")
    port = st.text_input("MySQL Port:", value="3306")
    database = st.text_input("Database Name:")
    user = st.text_input("Username:")
    password = st.text_input("Password:", type="password")

    if st.button("Connect to MySQL Database"):
        try:
            jdbc_url = f"jdbc:mysql://{host}:{port}/{database}"


            tables_query = "(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{}')".format(database)


            tables_df = spark.read.format("jdbc").options(
                url=jdbc_url,
                driver="com.mysql.cj.jdbc.Driver",
                user=user,
                password=password,
                dbtable=tables_query
            ).load()

            table_names = tables_df.toPandas()["TABLE_NAME"].tolist()
            if table_names:
                st.session_state.data_loaded = True
                st.session_state.table_names = table_names
                st.success("Connected to MySQL Database!")
                st.markdown("### Available Tables")
                st.write(", ".join(table_names))
                logging.info(f"Connected to MySQL DB and fetched tables: {', '.join(table_names)}")
            else:
                st.warning("No tables available in the connected database.")
                logging.warning("No tables found in MySQL DB.")
        except Exception as e:
            st.error(f"Failed to connect to the MySQL database: {e}")
            logging.error(f"Error connecting to MySQL DB: {e}")

elif data_source == "Oracle Database":
    st.markdown("### Connect to Oracle Database")
    host = st.text_input("Oracle Host:")
    port = st.text_input("Oracle Port:", value="1521")
    service_name = st.text_input("Service Name:")
    user = st.text_input("Username:")
    password = st.text_input("Password:", type="password")

    if st.button("Connect to Oracle Database"):
        try:

            jdbc_url = f"jdbc:oracle:thin:@{host}:{port}/{service_name}"


            tables_query = "(SELECT table_name FROM user_tables)"


            tables_df = spark.read.format("jdbc").options(
                url=jdbc_url,
                driver="oracle.jdbc.driver.OracleDriver",
                user=user,
                password=password,
                dbtable=tables_query
            ).load()

            table_names = tables_df.toPandas()["TABLE_NAME"].tolist()
            if table_names:
                st.session_state.data_loaded = True
                st.session_state.table_names = table_names
                st.success("Connected to Oracle Database!")
                st.markdown("### Available Tables")
                st.write(", ".join(table_names))
                logging.info(f"Connected to Oracle DB and fetched tables: {', '.join(table_names)}")
            else:
                st.warning("No tables available in the connected schema.")
                logging.warning("No tables found in Oracle DB schema.")
        except Exception as e:
            st.error(f"Failed to connect to the Oracle database: {e}")
            logging.error(f"Error connecting to Oracle DB: {e}")

elif data_source == "Flat File":
    st.markdown("### Upload Flat Files")
    uploaded_files = st.file_uploader("Upload Flat files (TXT, TSV)", accept_multiple_files=True, type=["txt", "tsv"])
    delimiter = st.text_input("Enter the delimiter for the file:", value="\t")

    if uploaded_files:
        try:
            table_names = []
            for file in uploaded_files:
                # Save uploaded file to a temporary file
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    temp_file.write(file.read())
                    temp_path = temp_file.name

                df = spark.read.csv(temp_path, sep=delimiter, header=True, inferSchema=True)
                table_name = file.name.split(".")[0]
                df.createOrReplaceTempView(table_name)
                table_names.append(table_name)

            # Store table names in session state
            st.session_state.table_names = table_names
            st.success(f"Loaded flat files as tables: {', '.join(table_names)}")
        except Exception as e:
            st.error(f"Failed to load flat files: {e}")

# Step 2: Show Schema for Available Tables
if st.session_state.data_loaded and st.session_state.table_names:
    st.markdown("### Table Schema Preview")
    selected_table = st.selectbox("Select a table to view its schema:", options=st.session_state.table_names)

    if st.button("Show Schema"):
        try:
            schema_df = spark.sql(f"DESCRIBE {selected_table}")
            st.markdown(f"#### Schema for `{selected_table}`")
            st.dataframe(schema_df.toPandas())
            logging.info(f"Schema for table `{selected_table}` displayed.")
        except Exception as e:
            st.error(f"Failed to fetch schema: {e}")
            logging.error(f"Error fetching schema for table `{selected_table}`: {e}")


# Step 3: Enable SQL Query Box
if st.session_state.data_loaded:
    st.markdown("### Write Your SQL Query")
    query = st.text_area("Enter your SQL query:", height=200)

    if st.button("Execute Query"):
        if query.strip():
            try:
                start_time = datetime.now()

                result_df = execute_user_query(spark, query)

                if result_df is not None and not result_df.rdd.isEmpty():
                    st.success("Query executed successfully!")
                    st.session_state.result_df = result_df
                    start_row = (1 - 1) * PAGE_SIZE
                    paginated_df = result_df.limit(PAGE_SIZE).offset(start_row)

                    st.markdown("### Query Results")
                    st.dataframe(paginated_df.toPandas())

                    execution_time = (datetime.now() - start_time).total_seconds()
                    logging.info(f"Query executed successfully in {execution_time} seconds.")
                else:
                    st.warning("No data returned. Please check your query.")
                    logging.warning("Query executed but returned no data.")
            except Exception as query_error:
                st.error(f"An error occurred while executing the query: {query_error}")
                logging.error(f"SQL query execution failed: {query_error}")
        else:
            st.warning("Please enter a valid SQL query before executing.")

# Step 4: Aggregation
if st.session_state.result_df is not None:
    st.markdown("### Aggregation Operations")
    aggregation = st.selectbox(
        "Select Aggregation Operation:",
        options=["None", "Count", "Sum", "Average"]
    )

    if aggregation != "None":
        try:
            if aggregation == "Count":
                aggregated_df = st.session_state.result_df.groupBy().count()
            elif aggregation == "Sum":
                aggregated_df = st.session_state.result_df.groupBy().sum()
            elif aggregation == "Average":
                aggregated_df = st.session_state.result_df.groupBy().avg()

            st.markdown(f"### Aggregated Data ({aggregation})")
            st.dataframe(aggregated_df.toPandas())
        except Exception as e:
            st.error(f"Error performing aggregation: {e}")
            logging.error(f"Error performing {aggregation} aggregation: {e}")

def encrypt_data(data: str) -> str:
    encrypted_data = cipher_suite.encrypt(data.encode())
    return encrypted_data.decode()

def convert_to_csv(df):
    # Convert the Spark DataFrame to Pandas for CSV export
    pandas_df = df.toPandas()
    return pandas_df.to_csv(index=False).encode('utf-8')

# Step 6: Data Masking and Encryption
if st.session_state.result_df is not None:
    st.markdown("### Data Masking and Encryption Options")
    apply_masking = st.checkbox("Enable Data Masking")
    sensitive_columns = st.multiselect("Select columns to mask or encrypt:", options=st.session_state.result_df.columns)

    if apply_masking:
        masked_df = st.session_state.result_df
        for column in sensitive_columns:
            masked_df = masked_df.withColumn(column, lit("MASKED"))
        st.dataframe(masked_df.toPandas())
        logging.info(f"Applied data masking to columns: {', '.join(sensitive_columns)}")

    apply_encryption = st.checkbox("Enable Data Encryption")
    if apply_encryption:
        encrypted_df = st.session_state.result_df
        for column in sensitive_columns:
            encrypted_df = encrypted_df.withColumn(column, lit(encrypt_data("some_value")))
        st.dataframe(encrypted_df.toPandas())
        logging.info(f"Applied data encryption to columns: {', '.join(sensitive_columns)}")

    save_option = st.selectbox("Select save option:", ["None", "Save as CSV", "Save as Excel", "Save as JSON", "Save to Oracle DB"])

    if save_option != None:
        if save_option == "Save as CSV":
            if apply_masking:
                try:
                    output_path = "output/transformed_data.csv"
                    masked_df.toPandas().to_csv(output_path, index=False)
                    with open(output_path, "rb") as file:
                        st.download_button(
                            label="Download CSV File",
                            data=file,
                            file_name="transformed_data.csv",
                            mime="text/csv"
                        )
                    st.success("Results have been successfully saved as a CSV file!")
                    logging.info("Query results saved as CSV successfully.")
                except Exception as e:
                    st.error(f"Error saving query results as CSV: {e}")
                    logging.error(f"Error saving query results as CSV: {e}")
            elif apply_encryption:
                try:
                    output_path = "output/transformed_data.csv"
                    encrypted_df.toPandas().to_csv(output_path, index=False)
                    with open(output_path, "rb") as file:
                        st.download_button(
                            label="Download CSV File",
                            data=file,
                            file_name="transformed_data.csv",
                            mime="text/csv"
                        )
                    st.success("Results have been successfully saved as a CSV file!")
                    logging.info("Query results saved as CSV successfully.")
                except Exception as e:
                    st.error(f"Error saving query results as CSV: {e}")
                    logging.error(f"Error saving query results as CSV: {e}")
        elif save_option == "Save as Excel":
            if apply_masking:
                try:
                    output_path = "output/transformed_data.xlsx"
                    st.session_state.masked_df.toPandas().to_excel(output_path, index=False)
                    with open(output_path, "rb") as file:
                        st.download_button(
                            label="Download Excel File",
                            data=file,
                            file_name="transformed_data.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    st.success("Results have been successfully saved as an Excel file!")
                    logging.info("Query results saved as Excel successfully.")
                except Exception as e:
                    st.error(f"Error saving query results as Excel: {e}")
                    logging.error(f"Error saving query results as Excel: {e}")
            elif apply_encryption:
                try:
                    output_path = "output/transformed_data.xlsx"
                    st.session_state.encrypted_df.toPandas().to_excel(output_path, index=False)
                    with open(output_path, "rb") as file:
                        st.download_button(
                            label="Download Excel File",
                            data=file,
                            file_name="transformed_data.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    st.success("Results have been successfully saved as an Excel file!")
                    logging.info("Query results saved as Excel successfully.")
                except Exception as e:
                    st.error(f"Error saving query results as Excel: {e}")
                    logging.error(f"Error saving query results as Excel: {e}")
        elif save_option == "Save as JSON":
            if apply_masking:
                try:
                    output_path = "output/transformed_data.json"
                    st.session_state.masked_df.toPandas().to_json(output_path, orient="records", lines=True)
                    with open(output_path, "rb") as file:
                        st.download_button(
                            label="Download JSON File",
                            data=file,
                            file_name="transformed_data.json",
                            mime="application/json"
                        )
                    st.success("Results have been successfully saved as a JSON file!")
                    logging.info("Query results saved as JSON successfully.")
                except Exception as e:
                    st.error(f"Error saving query results as JSON: {e}")
                    logging.error(f"Error saving query results as JSON: {e}")
            elif apply_encryption:
                try:
                    output_path = "output/transformed_data.json"
                    st.session_state.encrypted_df.toPandas().to_json(output_path, orient="records", lines=True)
                    with open(output_path, "rb") as file:
                        st.download_button(
                            label="Download JSON File",
                            data=file,
                            file_name="transformed_data.json",
                            mime="application/json"
                        )
                    st.success("Results have been successfully saved as a JSON file!")
                    logging.info("Query results saved as JSON successfully.")
                except Exception as e:
                    st.error(f"Error saving query results as JSON: {e}")
                    logging.error(f"Error saving query results as JSON: {e}")
        elif save_option == "Save to Oracle DB":
            if apply_masking:
                try:
                    save_to_oracle(masked_df)
                    st.success("Results have been successfully saved to the Oracle database!")
                    logging.info("Query results saved to Oracle DB successfully.")
                except Exception as e:
                    st.error(f"Error saving query results to Oracle DB: {e}")
                    logging.error(f"Error saving query results to Oracle DB: {e}")
            elif apply_encryption:
                try:
                    save_to_oracle(encrypted_df)
                    st.success("Results have been successfully saved to the Oracle database!")
                    logging.info("Query results saved to Oracle DB successfully.")
                except Exception as e:
                    st.error(f"Error saving query results to Oracle DB: {e}")
                    logging.error(f"Error saving query results to Oracle DB: {e}")
    else:
        st.error("select any one option")


st.write("\n")
st.markdown("---")
st.caption("© 2024 Multi-Source Smart Data Migration")

