import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
import requests
import json

# Variables
MISA_API_CONGNO = Variable.get("misa_api_congno")
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
    tags=["Misa", "cong no"],
    max_active_runs=1,
)
def Misa_API_Congno():
    ######################################### API ################################################
    @task
    def call_api_get_cong_no():
        headers = {
            "Content-Type": "application/json",
            "X-MISA-AccessToken": f"{TOKEN}"
        }
        body = {
            "data_type": 0,
            "branch_id": None,
            "org_company_code": "actapp",
            "skip": 0,
            "take": 1000,
            "app_id": f"{MISA_APP_ID}",
            "last_sync_time": None
        }
        while True:
            response = requests.post(MISA_API_CONGNO, headers=headers, json=body, timeout=None)
            if response.status_code == 200:
                data = json.loads(response.json().get("Data"))
                if not data:
                    break
                insert_cong_no(data, type = 0)
                body["skip"] += 1000
            else:
                print("Error please check api")
                break
    
    @task
    def call_api_get_cong_no_1():
        headers = {
            "Content-Type": "application/json",
            "X-MISA-AccessToken": f"{TOKEN}"
        }
        body = {
            "data_type": 1,
            "branch_id": None,
            "org_company_code": "actapp",
            "skip": 0,
            "take": 1000,
            "app_id": f"{MISA_APP_ID}",
            "last_sync_time": None
        }
        while True:
            response = requests.post(MISA_API_CONGNO, headers=headers, json=body, timeout=None)
            if response.status_code == 200:
                data = json.loads(response.json().get("Data"))
                if not data:
                    break
                insert_cong_no(data, type=1)
                body["skip"] += 1000
            else:
                print("Error please check api")
                break
    ######################################### INSERT DATA ################################################
    def insert_cong_no(data: list, type) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        values = []
        sql = """
            INSERT INTO [dbo].[3rd_misa_api_congno](
                [account_object_id]
                ,[account_object_code]
                ,[account_object_name]
                ,[organization_unit_id]
                ,[debt_amount]
                ,[invoice_debt_amount]
                ,[organization_unit_code]
                ,[organization_unit_name]
                ,[loai_cong_no]
                ,[dtm_creation_date])
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
        """
        sql_select = 'select distinct account_object_id from [3rd_misa_api_congno]'
        df_sql = pd.read_sql(sql_select, sql_conn)
        df = pd.DataFrame(data)
        df = df[~df['account_object_id'].isin(df_sql['account_object_id'])]

        col = ['account_object_id',
                'account_object_code',
                'account_object_name',
                'organization_unit_id',
                'debt_amount',
                'invoice_debt_amount',
                'organization_unit_code',
                'organization_unit_name']
        df = df[col]
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
                    str(type)
                )
                values.append(value)
            cursor.executemany(sql, values)

        print(f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()


    ############ DAG FLOW ############
    call_api_get_cong_no() >> call_api_get_cong_no_1()

dag = Misa_API_Congno()
