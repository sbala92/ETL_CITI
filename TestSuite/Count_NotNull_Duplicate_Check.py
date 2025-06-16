import os
import logging
from datetime import datetime
import json
import pandas as pd


dt=datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
log_file=f"C:\\Users\\suren\\PycharmProjects\\ETLAutomation_CITI\\Logs\\Testing_log_{dt}.log"
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt="%d-%m-%Y %H-%M-%S", force=True)
TestResultPath=f"C:\\Users\\suren\\PycharmProjects\\ETLAutomation_CITI\\TestResults\\Result_{dt}.xlsx"
config_path= r'C:\Users\suren\PycharmProjects\ETLAutomation_CITI\ConfigFiles_Scenarios'

def test_Count_Check(Oracle_Conn):
    #TestResultPath=f"C:\\Users\\suren\\PycharmProjects\\ETLAutomation_CITI\\TestResults\\Result_{dt}.xlsx"
    #config_path= r'C:\Users\suren\PycharmProjects\ETLAutomation_CITI\ConfigFiles_Scenarios'
    all_Table_result=[]
    for filename in os.listdir(config_path):
        each_json_file=config_path + '\\' + filename
        logging.info("** Started Count Validation **")
        logging.info(f"** Processing file {each_json_file} **")

        try:
            with (open(each_json_file, 'r') as SQL_file):
                SQL_Queries = json.load(SQL_file)
                t_table=SQL_Queries["Source and Target Tables"]["t_table"]
                logging.info(f"Target Table name: {t_table}")

                count_query=SQL_Queries["tc_01_Verifying the Count"]["t_count"]
                target_cursor=Oracle_Conn.cursor()
                target_cursor.execute(count_query)
                df_count=pd.DataFrame(target_cursor)
                logging.info(f"Count Query:{count_query}")
                logging.info(f"Count of {t_table} is: {df_count.iloc[0,0]}")

                #Write data in to Excel
                df_count_result=pd.DataFrame(
                    {
                        "Database":["Oracle"],
                        "Table Name":[t_table],
                        "Count": [df_count.iloc[0,0]]
                     }
                )
                all_Table_result.append(df_count_result)
                logging.info(f"Table result {all_Table_result}")
        except Exception as e:
            logging.error(f"in file - {filename}: {e}")
            continue
    df_count_results = pd.concat(all_Table_result, ignore_index=True)
    df_count_results.to_excel(TestResultPath, sheet_name='Count_Result', index=False)


def test_Null_check(Oracle_Conn):
    all_Table_result = []
    for filename in os.listdir(config_path):
        each_json_file = config_path + '\\' + filename
        logging.info("** Started Not Null Check Validation **")
        logging.info(f"** Processing file {each_json_file} **")

        try:
            with open(each_json_file, 'r') as SQL_file:
                SQL_Queries = json.load(SQL_file)
                t_table = SQL_Queries["Source and Target Tables"]["t_table"]
                logging.info(f"Target Table name: {t_table}")

                null_query=SQL_Queries["tc_02_Verifying Null Records"]["null_count"]
                target_cursor=Oracle_Conn.cursor()
                target_cursor.execute(null_query)
                df_null_count=pd.DataFrame(target_cursor)
                logging.info(f"Null Records:{df_null_count.iloc[0,0]}")
                if df_null_count.empty or df_null_count.iloc[0,0]==0:
                    null_count=0
                    null_records=None
                    status="Pass"
                    result="No Null records Found!"
                else:
                    status="Fail"
                    result="Null records Found!"
                    null_count=df_null_count.iloc[0,0]
                    null_rec_query=SQL_Queries["tc_02_Verifying Null Records"]["null_records"]
                    target_cursor=Oracle_Conn.cursor().execute(null_rec_query)
                    null_records=pd.DataFrame(target_cursor).to_string(index=False,header=False)
                    null_records=','.join(null_records.split())
                df_nulls=pd.DataFrame(
                    {
                        "Database":['Oracle'],
                        "Table Name":[t_table],
                        "Null Count":[null_count],
                        "Null Records":[null_records],
                        "Status":[status],
                        "Result":[result]

                    }
                )
                all_Table_result.append(df_nulls)
                logging.info(f"Table result {all_Table_result}")
        except Exception as e:
            logging.error(f"in file - {filename}: {e}")
            continue
    df_null_results = pd.concat(all_Table_result, ignore_index=True)
    with pd.ExcelWriter(TestResultPath,  mode='a', if_sheet_exists='replace') as writer:
        df_null_results.to_excel(writer, sheet_name='Null Check', index=False)

def test_DuplicateCheck(Oracle_Conn):
    all_Table_result = []
    logging.info("** Started Duplicate Check Validation **")
    for filename in os.listdir(config_path):
        each_json_file = config_path + '\\' + filename
        logging.info(f"** Processing file {each_json_file} **")

        try:
            with open(each_json_file, 'r') as SQL_file:
                SQL_Queries = json.load(SQL_file)
                t_table = SQL_Queries["Source and Target Tables"]["t_table"]
                logging.info(f"Target Table name: {t_table}")

                null_query = SQL_Queries["tc_03_Duplicate Check"]["dup_count"]
                target_cursor = Oracle_Conn.cursor()
                target_cursor.execute(null_query)
                df_null_count = pd.DataFrame(target_cursor)
                logging.info(f"Duplicate Records:{df_null_count.iloc[0, 0]}")
                if df_null_count.empty or df_null_count.iloc[0, 0] == 0:
                    dup_count = 0
                    dup_records = None
                    status = "Pass"
                    result = "No Duplicate records Found!"
                else:
                    status = "Fail"
                    result = "Duplicate records Found!"
                    dup_count = df_null_count.iloc[0, 0]
                    dup_rec_query = SQL_Queries["tc_03_Duplicate Check"]["dup_records"]
                    target_cursor = Oracle_Conn.cursor().execute(dup_rec_query)
                    dup_records = pd.DataFrame(target_cursor).to_string(index=False, header=False)
                    dup_records = ','.join(dup_records.split())
                df_dup = pd.DataFrame(
                    {
                        "Database": ['Oracle'],
                        "Table Name": [t_table],
                        "Duplicate Count": [dup_count],
                        "Duplicate Records": [dup_records],
                        "Status": [status],
                        "Result": [result]

                    }
                )
                all_Table_result.append(df_dup)
                logging.info(f"Table result {all_Table_result}")
        except Exception as e:
            logging.error(f"in file - {filename}: {e}")
            continue
    df_dup_results = pd.concat(all_Table_result, ignore_index=True)
    with pd.ExcelWriter(TestResultPath,engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df_dup_results.to_excel(writer, sheet_name='Duplicate Check', index=False)