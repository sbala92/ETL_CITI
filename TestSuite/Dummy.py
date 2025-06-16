import os
import logging
from datetime import datetime
import json
import pandas as pd
import openpyxl
import argparse

# Argument parsing
parser = argparse.ArgumentParser()
parser.add_argument('--files', required=False,
                    help='Comma-separated list of JSON files to process, e.g., ARRG_DIM.json,CRD_ARRG_DIM.json')
parser.add_argument('--columns', required=False,
                    help='Pipe-separated lists of comma-separated columns for each JSON file. '
                         'E.g., ARRG_NM,ARRG_DESC|COL1,COL2')
parser.add_argument('--logfile', required=True, help='Path to the log file')
parser.add_argument('--resultfile', required=True, help='Path to the result Excel file')
args = parser.parse_args()

# Logging setup
log_file = args.logfile
TestResultPath = args.resultfile

logging.basicConfig(filename=log_file, level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt="%d-%m-%Y %H-%M-%S", force=True)

# Set config path
config_path = r'C:\Users\suren\PycharmProjects\ETLAutomation_CITI\ConfigFiles_Scenarios'

# Determine which JSON files and columns to use
if args.files:
    json_files = args.files.split(',')
    column_map = {}
    if args.columns:
        all_column_groups = args.columns.split('|')
        for i, file in enumerate(json_files):
            if i < len(all_column_groups):
                column_map[file] = all_column_groups[i].split(',')
    else:
        column_map = {file: None for file in json_files}
else:
    json_files = [f for f in os.listdir(config_path) if f.endswith('.json')]
    column_map = {file: None for file in json_files}


def test_DomainCheck(Oracle_Conn):
    all_Table_result = []

    # Process each JSON file
    for json_filename in json_files:
        each_json_file = os.path.join(config_path, json_filename)

        if not os.path.exists(each_json_file):
            logging.error(f"File {each_json_file} not found.")
            continue

        logging.info(f"** Processing file {each_json_file} **")

        try:
            with open(each_json_file, 'r') as SQL_file:
                SQL_Queries = json.load(SQL_file)
                t_table = SQL_Queries["Source and Target Tables"]["t_table"]
                logging.info(f"Target Table name: {t_table}")

                column_defs = SQL_Queries["tc_04_DomainValue Check"]["columns"]
                where = SQL_Queries["tc_04_DomainValue Check"].get("where_con", "")
                columns_to_check = column_map.get(json_filename)
                columns_to_process = columns_to_check if columns_to_check else list(column_defs.keys())

                logging.info(
                    f"Columns to be validated: {columns_to_process}")
                logging.info(f"WHERE clause: {where}")

                cursor = Oracle_Conn.cursor()

                for column in columns_to_process:
                    if column not in column_defs:
                        logging.warning(f"Column {column} not found in {json_filename}. Skipping.")
                        continue

                    allowed_values = column_defs[column]["allowed_values"]
                    query = f"SELECT DISTINCT {column} FROM {t_table} {where}"

                    logging.info(f"Executing query for column: {column}")
                    logging.info(f"Query: {query}")
                    cursor.execute(query)
                    values = [row[0] for row in cursor.fetchall()]

                    invalid_values = [val for val in values if val not in allowed_values]

                    result = {
                        "Table": t_table,
                        "Column": column,
                        "Found Values": str(values),
                        "Allowed Values": str(allowed_values),
                        "Invalid Values": str(invalid_values) if invalid_values else "",
                        "Status": "FAIL" if invalid_values else "PASS",
                        "Error": ""
                    }

                    all_Table_result.append(result)
                    logging.info(
                        f"Column: {column}, Found: {values}, Allowed: {allowed_values}, Invalid: {invalid_values}")

        except Exception as e:
            logging.error(f"Error processing file {each_json_file}: {str(e)}")

    # Write results to Excel
    df = pd.DataFrame(all_Table_result)
    with pd.ExcelWriter(TestResultPath, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df.to_excel(writer, index=False, sheet_name='Domain_Validation_Results')

    # Optional: Fail the test if any column failed
    failed_columns = [r for r in all_Table_result if r["Status"] == "FAIL"]
    if failed_columns:
        logging.error(f"Some columns failed domain validation. Check the results in {TestResultPath}")
    else:
        logging.info("All columns passed domain validation.")


def get_oracle_connection():
    import cx_Oracle
    return cx_Oracle.connect("c##target/target@suri:1521/xe")  # Use your actual connection string


if __name__ == "__main__":
    Oracle_Conn = get_oracle_connection()
    test_DomainCheck(Oracle_Conn)
