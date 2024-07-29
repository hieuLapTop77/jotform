import requests
import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
import json

# Variables
NHANH_TOKEN = Variable.get("nhanh_token")
NHANH_GET_LIST_ORDERS = Variable.get("nhanh_get_list_orders")
NHANH_APPID = Variable.get("nhanh_appId")
NHANH_BUSINESSID = Variable.get("nhanh_businessId")
NHANH_SECRETKEY = Variable.get("nhanh_secretkey")

## Local path
TEMP_PATH = Variable.get("temp_path")

## Conection
HOOK_MSSQL = Variable.get("mssql_connection")

FOLDER_NAME = 'sochitietbanhang'

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="0 */4 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Nhanh.vn", "order", "Don hang"],
    max_active_runs=1,
)
def Nhanh_Order():
    ######################################### API ################################################
    @task
    def get_data_misa():
        page = 1
        payload = {
                "version": "2.0",
                "appId": NHANH_APPID,
                "secretKey": NHANH_SECRETKEY,
                "businessId": NHANH_BUSINESSID,
                "accessToken": NHANH_TOKEN,
                "data": '{"page": ' + str(page) + '}'
            }
        
        while True:
            response = requests.post(NHANH_GET_LIST_ORDERS, data=payload, timeout=None)
            if response.status_code == 200:
                data = json.loads(response.json().get("data"))
                if not data:
                    break
                insert_kho(data)
                page += 1
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



dag = Nhanh_Order()
