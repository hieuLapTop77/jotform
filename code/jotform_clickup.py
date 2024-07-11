import json
import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from typing import Union
import requests
from datetime import datetime
import copy
# Variables

## Local path
TEMP_PATH = Variable.get("temp_path")

CLICKUP_DELETE_TASK = Variable.get("clickup_delete_task")
CLICKUP_CREATE_TASK = Variable.get("clickup_create_task")

# Khong co TKNNo, TKCo, TKChietKhau, TKGiaVon, TKKho
## Conection
HOOK_MSSQL = Variable.get("mssql_connection")

# Token
API_TOKEN = Variable.get("api_token")

CLICKUP_GET_TASKS = Variable.get("clickup_get_tasks")
ID_LIST_DEFAULT = 901802308538

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
    headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
    body = {
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
                {
                    "id": "8d3faf19-817c-4d1f-9d52-efdeb8545a2c",
                    "name": "Ghi chú",
                    "value": None
                },
                {
                    "id": "20bd1409-47ad-41cd-b134-31c8cfb9cfae",
                    "name": "Khách hàng mới/cũ",
                    "value": None
                },
                {
                    "id": "6af6fa83-8742-4ee8-86a3-dea1c7f71d52",
                    "name": "Loại Hình KH",
                    "value": None
                },
                {
                    "id": "04a66008-9468-46f6-abee-b5fb408983e0",
                    "name": "Loại đơn hàng",
                    "value": None

                },
                {
                    "id": "cd3ec20a-9461-4543-bb05-c97854932714",
                    "name": "Số lượng",
                    "value": None
                },
                {
                    "id": "2cfec67c-7fb9-40d0-a927-1d0c68795a20",
                    "name": "Tên khách hàng",
                    "value": None
                }
            ],
            "attachments": []
        }
    headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
    ######################################### API ################################################
    def call_api_delete(task_id) -> None:
        response = requests.delete(CLICKUP_DELETE_TASK.format(task_id),timeout=None, headers=headers)
        print(CLICKUP_DELETE_TASK.format(task_id))
        if response.status_code in [200, 204]:
            print('Delete successful task: ', task_id)
        else:
            print("Error please check api")
    @task
    def check_tasks_clickup() -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = f"""select a.id from [dbo].[3rd_clickup_task_details] a
                inner join [dbo].[3rd_jotform_form_submissions] b on b.id = SUBSTRING(a.name, 1, CHARINDEX('|', a.name) - 1)
                where JSON_VALUE(space, '$.id') = {ID_LIST_DEFAULT} and a.dtm_creation_date >= DATEADD(hour, -2, GETDATE())"""
        print(sql)
        df = pd.read_sql(sql, sql_conn)
        print(df)
        for i in df['id']:
            call_api_delete(task_id=i)
        sql_conn.close()

    def call_tasks() -> Union[pd.DataFrame, None]:
        params = {
           "order_by": "created",
           "statuses[]" : "đơn đặt hàng"   ####### sua theo status tren clickup
        }
        res =  requests.get(CLICKUP_GET_TASKS.format(ID_LIST_DEFAULT), params=params, headers=headers, timeout=None)
        df = None
        if res.status_code == 200:
            df = pd.DataFrame(res.json()['tasks'])
            # try:
            #     list_data = df[df['name'].str.contains('|', regex=False)]['id'].tolist()
            # except KeyError as e:
            #     print('Error at: ', e)
        else:
            print("API Error")
        return df
    @task
    def delete_tasks() -> None:
        df_tasks = call_tasks() # hien thi full task by status
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = """select id 
                from [dbo].[3rd_jotform_form_submissions] 
                where created_at >= DATEADD(hour, -24, GETDATE())
                 -- dtm_creation_date >= DATEADD(hour, -24, GETDATE())"""
        df = pd.read_sql(sql, sql_conn)
        print(df["id"].tolist())
        for i in range(len(df_tasks)):  
            print(df_tasks['name'][i].split('|')[0])
            if df_tasks['name'][i].split('|')[0] in df["id"].tolist():
                call_api_delete(task_id=df_tasks["id"][i])

        print("delete task successfully")

    def get_addr_line1(json_str):
        if json_str:
            try:
                st = json_str.replace("'", '"')
                address_dict = json.loads(st)
                return address_dict['addr_line1']
            except json.JSONDecodeError:
                return ""
        return ""


    @task
    def create_order_clickup():
        # Create timestamp
        current_time = datetime.now()
        date_created = int(current_time.timestamp()*1000)
        # Create hook
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = """select * from jotform_view"""
        df = pd.read_sql(sql, sql_conn)
        body_parent = copy.deepcopy(body)
        body_child = copy.deepcopy(body)
        name = ""
        for i in range(0,len(df['id'])):
            df['addr_line1'] = df['address'].apply(get_addr_line1)
            if len(str(df["customer_name"][i])) > 0:
                name = str(df["id"][i]) + "| Order form" + '; ' + df["name_c"][i] + '; ' + df["name_b"][i] + '; ' + df["customer_name"][i] + '; ' + df["addr_line1"][i] + '; ' + df["phone"][i]
            else:
                name = str(df["id"][i]) + "| Order form" + '; ' + df["name_c"][i] + '; ' + df["name_b"][i] + '; ' + df["Ten_cua_hang"][i] + '-' + df["Ten_nguoi_lien_lac"][i] + '; ' + df["addr_line1"][i] + '; ' + df["phone"][i]
            body_parent['name'] = name
            body_parent['date_created'] = date_created
            for field in body_parent["custom_fields"]:
                if field["id"] == "8d3faf19-817c-4d1f-9d52-efdeb8545a2c" or field["name"] == "Ghi chú":
                    field["value"] = df['ghi_chu'][i]
                    
                if field["id"] == "20bd1409-47ad-41cd-b134-31c8cfb9cfae" or field["name"] == "Khách hàng mới/cũ":
                    field["value"] = df['orderindex_b'][i]
                
                if field["id"] == "04a66008-9468-46f6-abee-b5fb408983e0" or field["name"] == "Loại đơn hàng":
                    field["value"] = df['orderindex_c'][i]

                if field["id"] == "2cfec67c-7fb9-40d0-a927-1d0c68795a20" or field["name"] == "Tên khách hàng":
                    field["value"] = df['customer_name'][i] if len(df['customer_name'][i]) > 0 else df['Ten_cua_hang'][i]

            json_data = json.loads(json.dumps(body_parent, ensure_ascii=False))
            # Tạo đối tượng Request
            res = requests.post(CLICKUP_CREATE_TASK.format(ID_LIST_DEFAULT), json=json_data, headers=headers)
            if res.status_code == 200:
                task_id = res.json()['id']
                print(task_id)
                body_child['parent'] = task_id
                for j in range(0,len([col for col in df.columns.tolist() if col.startswith("Ten_SP")])):
                    current_time = datetime.now()
                    date_created = int(current_time.timestamp()*1000)
                    if not pd.isna(df[f"Ten_SP_{j + 1}"][i]):
                        body_child['name'] = df[f"Ten_SP_{j + 1}"][i]
                        body_child['date_created'] = date_created
                        for field in body_child["custom_fields"]:
                            if field["id"] == "8d3faf19-817c-4d1f-9d52-efdeb8545a2c" or field["name"] == "Ghi chú":
                                field["value"] = df['ghi_chu'][i]
                                
                            if field["id"] == "20bd1409-47ad-41cd-b134-31c8cfb9cfae" or field["name"] == "Khách hàng mới/cũ":
                                field["value"] = df['orderindex_b'][i]
                                
                            # if field["id"] == "6af6fa83-8742-4ee8-86a3-dea1c7f71d52" or field["name"] == "Loại Hình KH":
                            #     field["value"] = df['orderindex'][i]
                            
                            if field["id"] == "04a66008-9468-46f6-abee-b5fb408983e0" or field["name"] == "Loại đơn hàng":
                                field["value"] = df['orderindex_c'][i]

                            if field["id"] == "cd3ec20a-9461-4543-bb05-c97854932714" or field["name"] == "Số lượng":
                                field["value"] = df[f'SL_{j + 1}'][i]

                            if field["id"] == "2cfec67c-7fb9-40d0-a927-1d0c68795a20" or field["name"] == "Tên khách hàng":
                                field["value"] = df['customer_name'][i] if len(df['customer_name'][i]) > 0 else df['Ten_cua_hang'][i]

                        json_data_child = json.loads(json.dumps(body_child, ensure_ascii=False))
                        print(json_data_child)
                        res = requests.post(CLICKUP_CREATE_TASK.format(ID_LIST_DEFAULT), json=json_data_child, headers=headers)
                        if res.status_code == 200:
                            print("create task successful")
                        else:
                            print("create task fail")
                    else:
                        print("Ten san pham khong co")




    ############ DAG FLOW ############
    # task_check = check_tasks_clickup()
    task_create = create_order_clickup()
    delete_task = delete_tasks()
    delete_task >> task_create


dag = Jotform_Clickup()
