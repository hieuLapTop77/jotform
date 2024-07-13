import concurrent.futures
import copy
import json
from datetime import datetime
from typing import Union

import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from common.utils import handle_df

# Variables
TEMP_PATH = Variable.get("temp_path")
CLICKUP_DELETE_TASK = Variable.get("clickup_delete_task")
CLICKUP_CREATE_TASK = Variable.get("clickup_create_task")
HOOK_MSSQL = Variable.get("mssql_connection")
API_TOKEN = Variable.get("api_token")
CLICKUP_GET_TASKS = Variable.get("clickup_get_tasks")
ID_LIST_DEFAULT = 901802308538

# Khong co TKNNo, TKCo, TKChietKhau, TKGiaVon, TKKho
HEADERS = {
    "Authorization": f"{API_TOKEN}",
    "Content-Type": "application/json",
}

BODY_TEMPLATE = {
    "name": "Order form + Tên khách hàng cũ + Tên khách hàng mới; + Địa chỉ khách hàng mới + số điện thoại Khách hàng mới",
    "text_content": "",
    "description": "",
    "status": "đơn đặt hàng",
    "date_created": "",
    "date_updated": None,
    "date_closed": None,
    "date_done": None,
    "archived": False,
    "assignees": [],
    "group_assignees": [],
    "checklists": [],
    "tags": [],
    "parent": None,
    "priority": None,
    "due_date": None,
    "start_date": None,
    "points": None,
    "time_estimate": None,
    "time_spent": 0,
    "custom_fields": [
        {"id": "8d3faf19-817c-4d1f-9d52-efdeb8545a2c",
            "name": "Ghi chú", "value": None},
        {"id": "20bd1409-47ad-41cd-b134-31c8cfb9cfae",
            "name": "Khách hàng mới/cũ", "value": None},
        {"id": "6af6fa83-8742-4ee8-86a3-dea1c7f71d52",
            "name": "Loại Hình KH", "value": None},
        {"id": "04a66008-9468-46f6-abee-b5fb408983e0",
            "name": "Loại đơn hàng", "value": None},
        {"id": "cd3ec20a-9461-4543-bb05-c97854932714",
            "name": "Số lượng", "value": None},
        {"id": "2cfec67c-7fb9-40d0-a927-1d0c68795a20",
            "name": "Tên khách hàng", "value": None},
    ],
    "attachments": [],
}

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Jotform", "clickup"],
    max_active_runs=1,
)
def Jotform_Clickup():
    ######################################### API ################################################
    def call_api_delete(task_id) -> None:
        response = requests.delete(CLICKUP_DELETE_TASK.format(
            task_id), timeout=None, headers=HEADERS)
        print(CLICKUP_DELETE_TASK.format(task_id))
        if response.status_code in [200, 204]:
            print('Delete successful task: ', task_id)
        else:
            print("Error please check api")

    def call_tasks() -> Union[str, None]:
        params = {
            "order_by": "created",
            "statuses[]": "đơn đặt hàng"  # sua theo status tren clickup
        }
        res = requests.get(CLICKUP_GET_TASKS.format(
            ID_LIST_DEFAULT), params=params, headers=HEADERS, timeout=None)
        if res.status_code == 200:
            return pd.DataFrame(res.json().get('tasks', [])).to_json()
        else:
            print("API Error")
            return None

    def create_task_payload(df_row, is_child=False, parent_id=None):
        body = copy.deepcopy(BODY_TEMPLATE)
        body['name'] = (
            f"{df_row['id']}| Order form; {df_row['name_c']}; {df_row['name_b']}; {df_row['customer_name'] or df_row['Ten_cua_hang'] + '-' + df_row['Ten_nguoi_lien_lac']}; {df_row['addr_line1']}; {df_row['phone']}"
        )
        body['date_created'] = int(datetime.now().timestamp() * 1000)
        if is_child:
            body['parent'] = parent_id
            body['name'] = df_row['product_name']

        for field in body["custom_fields"]:
            if field["id"] == "8d3faf19-817c-4d1f-9d52-efdeb8545a2c":
                field["value"] = df_row['ghi_chu']
            elif field["id"] == "20bd1409-47ad-41cd-b134-31c8cfb9cfae":
                field["value"] = df_row['orderindex_b']
            elif field["id"] == "04a66008-9468-46f6-abee-b5fb408983e0":
                field["value"] = df_row['orderindex_c']
            elif field["id"] == "cd3ec20a-9461-4543-bb05-c97854932714" and is_child:
                field["value"] = df_row['quantity']
            elif field["id"] == "2cfec67c-7fb9-40d0-a927-1d0c68795a20":
                field["value"] = df_row['customer_name'] or df_row['Ten_cua_hang']

        return json.loads(json.dumps(body, ensure_ascii=False))

    def get_addr_line1(json_str):
        if json_str:
            try:
                st = json_str.replace("'", '"')
                address_dict = json.loads(st)
                return address_dict['addr_line1']
            except json.JSONDecodeError:
                return ""
        return ""

    def create_order_clickup_task(df_row):
        main_task_payload = create_task_payload(df_row)
        res = requests.post(CLICKUP_CREATE_TASK.format(
            ID_LIST_DEFAULT), json=main_task_payload, headers=HEADERS)
        if res.status_code == 200:
            task_id = res.json()['id']
            print(f"Created parent task: {task_id}")
            # Assuming there are at most 10 products
            for product_index in range(1, 11):
                if pd.notna(df_row.get(f"Ten_SP_{product_index}")):
                    df_row['product_name'] = df_row[f"Ten_SP_{product_index}"]
                    df_row['quantity'] = df_row[f'SL_{product_index}']
                    child_task_payload = create_task_payload(
                        df_row, is_child=True, parent_id=task_id)
                    res = requests.post(CLICKUP_CREATE_TASK.format(
                        ID_LIST_DEFAULT), json=child_task_payload, headers=HEADERS)
                    print(res.status_code)
                    if res.status_code == 200:
                        print(
                            f"Created child task for product {product_index}")
                    else:
                        print(
                            f"Failed to create child task for product {product_index}")
                else:
                    break

            update_jotform(df_row['id'])


    def update_jotform(id):
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        sql_update = f"""
                update [dbo].[3rd_jotform_form_submissions]
                set status_clickup = 'true'
                where id = '{id}'
                """
        print(sql_update)
        cursor.execute(sql_update)
        sql_conn.commit()
        print(f"updated id: {id} successfully")

    @task
    def check_tasks_clickup() -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = f"""select a.id from [dbo].[3rd_clickup_task_details] a
                inner join [dbo].[3rd_jotform_form_submissions] b on b.id = SUBSTRING(a.name, 1, CHARINDEX('|', a.name) - 1)
                where JSON_VALUE(space, '$.id') = {ID_LIST_DEFAULT} and a.dtm_creation_date >= DATEADD(hour, -2, GETDATE())"""
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(call_api_delete, df['id'].tolist())

    @task
    def delete_tasks() -> None:
        df = call_tasks()
        df_tasks = handle_df(df)    
        if df_tasks is not None:
            hook = mssql.MsSqlHook(HOOK_MSSQL)
            sql_conn = hook.get_conn()
            sql = """select id from [dbo].[3rd_jotform_form_submissions] where DATEADD(SECOND, cast(created_at as bigint) / 1000, '1970-01-01 00:00:00') >= DATEADD(hour, -24, GETDATE())"""
            df = pd.read_sql(sql, sql_conn)
            sql_conn.close()

            task_ids_to_delete = [df_tasks['id'][i] for i in range(
                len(df_tasks)) if df_tasks['name'][i].split('|')[0] in df["id"].tolist()]
            print(task_ids_to_delete)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(call_api_delete, task_ids_to_delete)

    @task
    def create_order_clickup():
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = """select * from jotform_view"""
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        df['addr_line1'] = df['address'].apply(get_addr_line1)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(create_order_clickup_task, [
                         row for _, row in df.iterrows()])

    ############ DAG FLOW ############
    # task_check = check_tasks_clickup()
    create_order_clickup()
    # delete_task = delete_tasks()
    # jotform_task = TriggerDagRunOperator(
    #     task_id='Jotform_task',
    #     trigger_dag_id='Jotform',  
    #     wait_for_completion=True
    # )
    # jotform_task >> delete_task >> task_create
    # delete_task >> task_create


dag = Jotform_Clickup()
