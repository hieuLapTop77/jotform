import json

import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
import requests
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from common.utils import call_api_mutiple_pages, call_multiple_thread
from common.utils_nikko import insert_tasks, insert_task_details, update_status, init_date
import copy
import concurrent.futures

# Variables
LIST_ID_TASK_SOURCE = 223604957
LIST_ID_TASK_DESTINATION = 901802621445
FOLDER_NAME = 'Clickup_purchase_files/'
TABLE_NAME_TASKS = '[nikko].[3rd_clickup_tasks_merge_email_purchase]'
TABLE_NAME_TASK_DETAILS = '[nikko].[3rd_clickup_tasks_details_merge_email_purchase]'

# Clickup
CLICKUP_GET_TASKS = Variable.get("clickup_get_tasks")
CLICKUP_GET_TASKS_DETAILS = Variable.get("clickup_get_task_details")
CLICKUP_CREATE_TASK = Variable.get("clickup_create_task")
CLICKUP_STATUS = Variable.get("status_clickup_purchase")

# Local path
TEMP_PATH = Variable.get("temp_path")

# Token
API_TOKEN_NIKO = Variable.get("api_token_niko")

# Conection
HOOK_MSSQL = Variable.get("mssql_connection")

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}
BODY_TEMPLATE = {
    "name": "",
    "text_content": "",
    "description": "",
    "status": None,
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
    ],
    "attachments": [],
}
@dag(
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Clickup Email", "Clickup sales"],
    max_active_runs=1,
)
def Nikko_Clickup_Merge_Email_Purchase():
    headers = {
            "Authorization": f"{API_TOKEN_NIKO}",
            "Content-Type": "application/json",
        }
    ######################################### API ################################################

    def call_api_get_tasks(space_id):
        date = init_date()
        params = {
            "page": 0, 
            "include_closed": "true"
            # "date_created_gt": date["date_from"],
            # "date_created_lt": date["date_to"],
            # "date_updated_gt": date["date_from"],
            # "date_updated_lt": date["date_to"]
        }
        name_url = 'CLICKUP_GET_TASKS'
        return call_api_mutiple_pages(headers=headers,params=params, name_url=name_url,url=CLICKUP_GET_TASKS,task_id=space_id)
    
    @task
    def call_mutiple_process_tasks_by_list() -> list:
        sql = f"select {LIST_ID_TASK_SOURCE} id;"
        return call_multiple_thread(hook_sql=HOOK_MSSQL,sql=sql,function=call_api_get_tasks,function_name='call_mutiple_process_tasks_by_list')

    def call_api_get_task_details(task_id):
        params = {
            'include_subtasks': 'true',
            "page": 0
        }
        name_url = 'CLICKUP_GET_TASKS_DETAILS'
        return call_api_mutiple_pages(headers=headers,params=params, name_url=name_url,url=CLICKUP_GET_TASKS_DETAILS,task_id=task_id)
    
    @task
    def call_mutiple_process_task_details() -> list:
        sql = f"select id from {TABLE_NAME_TASKS};"
        return call_multiple_thread(hook_sql=HOOK_MSSQL,sql=sql,function=call_api_get_task_details,function_name='call_api_get_task_details')
    
    ######################################### INSERT DATA ################################################

    @task
    def insert_tasks_sql(list_tasks: list) -> None:
        insert_tasks(list_tasks=list_tasks, hook_mssql=HOOK_MSSQL, table_name=TABLE_NAME_TASKS)

    @task
    def insert_task_details_sql(list_task_details: list) -> None:
        insert_task_details(list_task_details=list_task_details, hook_mssql=HOOK_MSSQL, table_name=TABLE_NAME_TASK_DETAILS)

    @task
    def call_procedure() -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
       
        sql = f"exec [dbo].[sp_Update_tasks_merge_email_purchase];"
        cursor.execute(sql)
        sql_conn.commit()
        sql_conn.close()
    
    def create_task_payload(df_row):
        body = copy.deepcopy(BODY_TEMPLATE)
        body['name'] = df_row['name']
        body['date_created'] = int(datetime.now().timestamp() * 1000)
        body['status'] = CLICKUP_STATUS
        body["description"] = df_row["description"]
        return json.loads(json.dumps(body, ensure_ascii=False))
    
    
    def create_task(df_row):
        main_task_payload = create_task_payload(df_row)
        res = requests.post(CLICKUP_CREATE_TASK.format(
            LIST_ID_TASK_DESTINATION), json=main_task_payload, headers=headers)
        print(main_task_payload)
        print('url: ', CLICKUP_CREATE_TASK.format(LIST_ID_TASK_DESTINATION))
        if res.status_code == 200:
            task_id = res.json()['id']
            print(f"Created parent task: {task_id}")
            update_status(task_id=df_row["id"], hook_mssql=HOOK_MSSQL, table_name=TABLE_NAME_TASKS)
        else:
            print("create task fail: ", res.status_code)

    @task
    def create_order_clickup():
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = f"""
            select id, name, description
                ,case when description <> '' 
                    and substring(description, 0, 30) like '%@%'  
                    and  CHARINDEX(CHAR(10), description) > 0
                then replace((SUBSTRING(description, 1, CHARINDEX(CHAR(10) , description) - 1)),' ','') else '' end created_by 
            from {TABLE_NAME_TASKS} where order_nikko = 1 and status_nikko = 'no';
        """
        print("SQL: ", sql)
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(create_task, [
                         row for _, row in df.iterrows()])
    
    
    ############ DAG FLOW ############

    list_tasks = call_mutiple_process_tasks_by_list()
    insert_tasks_task = insert_tasks_sql(list_tasks)

    list_task_details_task = call_mutiple_process_task_details()
    insert_task_details_task = insert_task_details_sql(list_task_details_task)

    call_procedure_task = call_procedure()
    create_order_clickup_task = create_order_clickup()

    list_tasks >> insert_tasks_task  >> list_task_details_task >> insert_task_details_task >> call_procedure_task >> create_order_clickup_task
    # create_order_clickup()

dag = Nikko_Clickup_Merge_Email_Purchase()
