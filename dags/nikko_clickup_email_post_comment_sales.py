import os
import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
import requests
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from common.utils import call_api_mutiple_pages, call_multiple_thread
from requests_toolbelt.multipart.encoder import MultipartEncoder
import concurrent.futures
from common.utils_nikko import download_single_file, update_clickup_task_gmail, insert_tasks, init_date
import copy
import concurrent.futures
import json 

# Variables
LIST_ID_TASK_DESTINATION = 901802542742
FOLDER_NAME = 'Clickup_sales_files/'
TABLE_NAME_TASKS = '[nikko].[3rd_clickup_tasks_merge_email_sales]'
TABLE_TASK_COMMENTS = '[nikko].[3rd_clickup_tasks_email_comment_sales]'

# Clickup
CLICKUP_GET_TASKS = Variable.get("clickup_get_tasks")
CLICKUP_ATTACHMENT = Variable.get("clickup_attachment")
CLICKUP_COMMENT = Variable.get("clickup_comment")
CLICKUP_STATUS = Variable.get("status_clickup_sales")
CLICKUP_CREATE_TASK = Variable.get("clickup_create_task")

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

@dag(
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Clickup comment " "Clickup sales", " comment", " sales"],
    max_active_runs=1,
)
def Clickup_Comment_Sales():
    headers = {
            "Authorization": f"{API_TOKEN_NIKO}",
            "Content-Type": "application/json",
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
            {
                "id": "f382f1c5-41a7-4222-9787-ec83cd4ad7cc",
                "name": "Created by",
                "value": None
            }
        ],
        "attachments": [],
    }
    ######################################### API ################################################

    def call_api_get_tasks(space_id):
        date = init_date()
        params = {
            "page": 0 
            # "include_closed": "true",
            # "statuses[]": "open"
            # "date_created_gt": date["date_from"],
            # "date_created_lt": date["date_to"],
            # "date_updated_gt": date["date_from"],
            # "date_updated_lt": date["date_to"]
        }
        # params["statuses[]"] = CLICKUP_STATUS
        id_ = int(space_id)
        name_url = 'CLICKUP_GET_TASKS'
        return call_api_mutiple_pages(headers=headers,params=params, name_url=name_url,url=CLICKUP_GET_TASKS,task_id=id_)
    
    @task
    def call_mutiple_process_tasks_by_list() -> list:
        sql = f"select {LIST_ID_TASK_DESTINATION} id;"
        return call_multiple_thread(hook_sql=HOOK_MSSQL,sql=sql,function=call_api_get_tasks,function_name='call_mutiple_process_tasks_by_list')

    @task
    def check_temp_path():
        path = TEMP_PATH + FOLDER_NAME
        if not os.path.exists(path):
            os.mkdir(path)

    @task
    def empty_temp_dir():
        path_ = TEMP_PATH + FOLDER_NAME
        files = os.listdir(path_)
        for f in files:
            os.remove(os.path.join(path_, f))
    ######################################### INSERT DATA ################################################

    @task
    def insert_tasks_sql(list_tasks: list) -> None:
        sql_truncate = f"truncate table {TABLE_TASK_COMMENTS}; "
        insert_tasks(list_tasks=list_tasks, hook_mssql=HOOK_MSSQL,table_name=TABLE_TASK_COMMENTS, sql=sql_truncate)

    @task
    def call_procedure() -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
       
        sql = f"exec [dbo].[sp_Update_tasks_merge_email_sales_v1];"
        cursor.execute(sql)
        sql_conn.commit()
        sql_conn.close()

    def post_comment(df_row):
        task_id = df_row["orginal_id"]
        url_comment = CLICKUP_COMMENT.format(task_id)
        body = {
            "comment_text": df_row["text_content"],
            "assignee": None,
            "group_assignee": None,
            "notify_all": True
        }
        headers['Content-Type'] = 'application/json'
        response_comment = requests.post(url_comment, headers=headers, json=body)

        if response_comment.status_code == 200:
            update_clickup_task_gmail(task_id=task_id, hook_mssql=HOOK_MSSQL, table_name=TABLE_NAME_TASKS)
            print('COMMENT: Comment added successfully.')
        else:
            print(f'COMMENT: Failed {response_comment.status_code} to add comment: {response_comment.json()} on task: {task_id}')
        
    
    def create_task_payload(df_row):
        body = copy.deepcopy(BODY_TEMPLATE)
        body['name'] = df_row['name']
        body['date_created'] = int(datetime.now().timestamp() * 1000)
        body['status'] = CLICKUP_STATUS
        body["description"] = df_row["description"]
        for field in body["custom_fields"]:
            if field["id"] == "f382f1c5-41a7-4222-9787-ec83cd4ad7cc": #"created by"
                field["value"] = df_row['created_by'] if df_row['created_by'] else None
        return json.loads(json.dumps(body, ensure_ascii=False))
    
    def update_new_orginal_id(task_id: str, orginal_id: str):
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        sql_update = f"""
                update {TABLE_NAME_TASKS}
                set new_orginal_id = '{task_id}'
                where orginal_id = '{orginal_id}' and is_newtask is not null
                """
        print(sql_update)
        cursor.execute(sql_update)
        sql_conn.commit()
        print(f"updated status clickup on table name '{TABLE_NAME_TASKS}' with task_id '{task_id}' successfully")
        sql_conn.close()

    def create_task(df_row):
        main_task_payload = create_task_payload(df_row)
        res = requests.post(CLICKUP_CREATE_TASK.format(LIST_ID_TASK_DESTINATION), json=main_task_payload, headers=headers)
        if res.status_code == 200:
            task_id = res.json()['id']
            print(f"Created parent task: {task_id}")
            update_new_orginal_id(task_id=task_id, orginal_id=df_row['orginal_id'])
        else:
            print("create task fail: ", res.status_code)
    
    def post_file(df_row):
        task_id = df_row["orginal_id"]
        if len(task_id) < 1: 
            print("Not found task parent")
            return
        local_path = download_single_file(url_download=df_row["url_download"], temp_path=TEMP_PATH, folder_name=FOLDER_NAME)
        url_upload = CLICKUP_ATTACHMENT.format(task_id)
        with open(local_path, 'rb') as file:
            m = MultipartEncoder(
                fields={
                    'attachment': (local_path.split('/')[-1], file, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                }
            )
            headers = {
                'Authorization': f"{API_TOKEN_NIKO}",
                'Content-Type': m.content_type
            }
            response_upload = requests.post(url_upload, headers=headers, data=m)
        file_name = local_path.split('/')[-1]
        if response_upload.status_code == 200:
            print(f'ATTACHMENT: Upload file {file_name} uploaded successfully.')
        else:
            print(f'ATTACHMENT: Failed {response_upload.status_code} with file name {file_name} error: {response_upload.json()}')

    @task
    def create_task_by_child():
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = f"""
            select id, name, description, orginal_id, is_newtask, new_orginal_id,
                case when description <> '' and substring(description, 0, 30) like '%@%' then replace((SUBSTRING(description, 1, CHARINDEX(CHAR(10) , description) - 1)),' ','') else '' end created_by 
            from {TABLE_NAME_TASKS} where is_newtask = 'Y'
        """
        print("SQL: ", sql)
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(create_task, [
                         row for _, row in df.iterrows()])
            

    @task
    def create_comment_clickup():
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = """select *
                from [dbo].[vw_Clickup_Comment_By_Sales] 
                order by orginal_id, order_nikko; 
        """
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(post_comment, [
                         row for _, row in df.iterrows()])
    
    @task
    def create_comment_clickup_v1():
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = """select *
                from [dbo].[vw_Clickup_Comment_By_Sales_v1]
                order by orginal_id, order_nikko; 
        """
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(post_comment, [
                         row for _, row in df.iterrows()])
    
    @task
    def post_attachment():
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = """select *
                from [vw_Clickup_Attachment_By_Sales]
                order by orginal_id, order_nikko; 
        """
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(post_file, [
                         row for _, row in df.iterrows()])

    

    ############ DAG FLOW ############
    check_temp_path_task = check_temp_path()
    list_tasks = call_mutiple_process_tasks_by_list()
    insert_tasks_task = insert_tasks_sql(list_tasks)
    call_procedure_task = call_procedure()
    create_task_child = create_task_by_child()
    create_comment_clickup_task = create_comment_clickup()
    create_comment_clickup_task_v1 = create_comment_clickup_v1()
    create_attachment_task = post_attachment()
    remove_files = empty_temp_dir()
    check_temp_path_task >> list_tasks >> insert_tasks_task >> call_procedure_task >> create_task_child >> create_comment_clickup_task >> create_comment_clickup_task_v1 >> create_attachment_task >> remove_files
    # create_comment_clickup()


dag = Clickup_Comment_Sales()
