import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
import requests
import json
# Variables
MISA_API_TONKHO = Variable.get("misa_api_tonkho")
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
    tags=["Misa", "Ton Kho", "Inventory"],
    max_active_runs=1,
)
def Misa_API_TonKho():
    ######################################### API ################################################
    @task
    def call_api_get_tonkho():
        headers = {
            "Content-Type": "application/json",
            "X-MISA-AccessToken": f"{TOKEN}"
        }
        body = {
            "app_id": f"{MISA_APP_ID}",
            "stock_id": "",
            "branch_id": None,
            "skip": 0,
            "take": 1000,
            "last_sync_time": None
        }
        while True:
            response = requests.post(MISA_API_TONKHO, headers=headers, json=body, timeout=None)
            if response.status_code == 200:
                data = json.loads(response.json().get("Data"))
                if not data:
                    break
                insert_ton_kho(data)
                body["skip"] += 1000
            else:
                print("Error please check api")
                break
    ######################################### INSERT DATA ################################################
    def insert_ton_kho(data: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        values = []
        sql = """
                    INSERT INTO [dbo].[3rd_misa_api_tonkho](
                        [inventory_item_id]
                        ,[inventory_item_code]
                        ,[inventory_item_name]
                        ,[stock_id]
                        ,[stock_code]
                        ,[stock_name]
                        ,[organization_unit_id]
                        ,[organization_unit_code]
                        ,[organization_unit_name]
                        ,[quantity_balance]
                        ,[amount_balance]
                        ,[unit_price]
                        ,[expiry_date]
                        ,[lot_no]
                        ,[dtm_creation_date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, getdate())
                """
        sql_select = "truncate table [3rd_misa_api_tonkho]"
        cursor.execute(sql_select)
        sql_conn.commit()
        df = pd.DataFrame(data)
        col = ['inventory_item_id',
                'inventory_item_code',
                'inventory_item_name',
                'stock_id',
                'stock_code',
                'stock_name',
                'organization_unit_id',
                'organization_unit_code',
                'organization_unit_name',
                'quantity_balance',
                'amount_balance',
                'unit_price',
                'expiry_date',
                'lot_no']
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
                    str(row[8]),
                    str(row[9]),
                    str(row[10]),
                    str(row[11]),
                    str(row[12]),
                    str(row[13]),
                )
                values.append(value)
            cursor.executemany(sql, values)

        print(f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()


    ############ DAG FLOW ############
    call_api_get_tonkho()



dag = Misa_API_TonKho()
