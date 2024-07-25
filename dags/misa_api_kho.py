import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
import requests
import json

# Variables
MISA_API_DANHMUC = Variable.get("misa_api_danhmuc")
TOKEN = Variable.get("misa_token")
MISA_APP_ID = Variable.get("misa_app_id")

## Local path
TEMP_PATH = Variable.get("temp_path")

## Conection
HOOK_MSSQL = Variable.get("mssql_connection")

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}
@dag(
    default_args=default_args,
    schedule_interval="5 */12 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Misa", "Kho", "Warehouse"],
    max_active_runs=1,
)
def Misa_API_Kho():
    ######################################### API ################################################
    @task
    def get_data_misa():
        body = {
            "data_type": 3,
            "branch_id": None,
            "skip": 0,
            "take": 1000,
            "app_id": f"{MISA_APP_ID}",
            "last_sync_time": None
        }
        headers = {
            "Content-Type": "application/json",
            "X-MISA-AccessToken": f"{TOKEN}"
        }
        while True:
            response = requests.post(MISA_API_DANHMUC, headers=headers, json=body, timeout=None)
            if response.status_code == 200:
                data = json.loads(response.json().get("Data"))
                if not data:
                    break
                insert_kho(data)
                body["skip"] += 1000
            else:
                print("Error please check api")
                break

    ######################################### INSERT DATA ################################################
    def insert_kho(data: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        values = []
        sql = """
            INSERT INTO [dbo].[3rd_misa_api_kho](
                [dictionary_type]
                ,[is_sync_corp]
                ,[stock_id]
                ,[branch_id]
                ,[inactive]
                ,[stock_code]
                ,[stock_name]
                ,[inventory_account]
                ,[reftype_category]
                ,[reftype]
                ,[from_stock_id]
                ,[to_stock_id]
                ,[isCustomPrimaryKey]
                ,[isFromProcessBalance]
                ,[excel_row_index]
                ,[is_valid]
                ,[created_date]
                ,[created_by]
                ,[modified_date]
                ,[modified_by]
                ,[auto_refno]
                ,[pass_edit_version]
                ,[state]
                ,[description]
                ,[dtm_creation_date])
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s
            , getdate())
        """
        sql_select = "select distinct stock_id from [3rd_misa_api_kho]"
        df_sql = pd.read_sql(sql_select, sql_conn)
        df = pd.DataFrame(data)
        df = df[~df['stock_id'].isin(df_sql['stock_id'])]
        if not df.empty:
            for _index, row in df.iterrows():
                value = (
                    str(row[0]),
                    str(row[1]),
                    str(row[2]),
                    str(row[3]),
                    str(row[4]),
                    str(row[5]),
                    str(row[6]),
                    str(row[7]),
                    str(row[8]),
                    str(row[9]),
                    str(row[10]),
                    str(row[11]),
                    str(row[12]),
                    str(row[13]),
                    str(row[14]),
                    str(row[15]),
                    str(row[16]),
                    str(row[17]),
                    str(row[18]),
                    str(row[19]),
                    str(row[20]),
                    str(row[21]),
                    str(row[22]),
                    str(row[23]),
                )
                values.append(value)
            cursor.executemany(sql, values)

        print(f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()


    ############ DAG FLOW ############
    get_data_misa()



dag = Misa_API_Kho()
