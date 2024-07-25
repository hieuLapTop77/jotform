import json
import os
import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import Dict
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from common.utils import call_api_mutiple_pages, call_multiple_thread
from requests_toolbelt.multipart.encoder import MultipartEncoder
import concurrent.futures

# Variables
# Clickup
CLICKUP_GET_TASKS = Variable.get("clickup_get_tasks")
CLICKUP_GET_TASKS_DETAILS = Variable.get("clickup_get_task_details")
CLICKUP_GET_CUSTOM_FIELDS = Variable.get("clickup_get_custom_fields")
CLICKUP_ATTACHMENT = Variable.get("clickup_attachment")
CLICKUP_COMMENT = Variable.get("clickup_comment")
# Local path
TEMP_PATH = Variable.get("temp_path")
CLICKUP_DELETE_TASK = Variable.get("clickup_delete_task")
CLICKUP_CREATE_TASK = Variable.get("clickup_create_task")
HOOK_MSSQL = Variable.get("mssql_connection")
API_TOKEN = Variable.get("api_token")
ID_LIST_TASK = 901802542742
FOLDER_NAME = 'Clickup_files/'

# Token
API_TOKEN_NIKO = Variable.get("api_token_niko")

# Conection
HOOK_MSSQL = Variable.get("mssql_connection")

data = []

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
    "custom_fields": [],
    "attachments": [],
}
@dag(
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Clickup comment"],
    max_active_runs=1,
)
def Clickup_Comment():
    headers = {
            "Authorization": f"{API_TOKEN_NIKO}",
            "Content-Type": "application/json",
        }
    ######################################### API ################################################

    def init_date() -> Dict[str, str]:
        current_time = datetime.now()
        date_to = int(current_time.timestamp()*1000)
        time_minus_24_hours = current_time - timedelta(hours=24)
        date_from = int(time_minus_24_hours.timestamp() * 1000)
        return {"date_from": date_from, "date_to": date_to}

    def call_api_get_tasks(space_id):
        date = init_date()
        params = {
            "page": 0, 
            "include_closed": "true",
            "statuses[]": "open"
            # "date_created_gt": date["date_from"],
            # "date_created_lt": date["date_to"],
            # "date_updated_gt": date["date_from"],
            # "date_updated_lt": date["date_to"]
        }
        id_ = int(space_id)
        
        name_url = 'CLICKUP_GET_TASKS'
        return call_api_mutiple_pages(headers=headers,params=params, name_url=name_url,url=CLICKUP_GET_TASKS,task_id=id_)
    
    @task
    def call_mutiple_process_tasks_by_list() -> list:
        sql = "select 901802542742 id;"
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
    def insert_tasks(list_tasks: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        filtered_data = [item for item in list_tasks if not (isinstance(item, list) and len(item) > 0 and isinstance(item[0], dict) and item[0].get('tasks') == [] and item[0].get('last_page'))]
        data = [item for sublist in filtered_data for t in sublist for item in t["tasks"]]
        
        sql_truncate = "truncate table [nikko].[3rd_clickup_tasks_gmail_comment]; "
        cursor.execute(sql_truncate)
        sql_conn.commit()

        df = pd.DataFrame(data)
        values = []
        if len(df) > 0:
            columns_to_json = [
                "status", "creator", "assignees", "group_assignees", "watchers",
                "checklists", "tags", "custom_fields", "dependencies", "linked_tasks",
                "locations", "sharing", "list", "project", "folder", "space"]
            
            for column in columns_to_json:
                if column in df.columns.tolist():
                    df[column] = df[column].apply(lambda x: json.dumps(x))

            if 'subtasks' in df.columns.tolist():
                df["subtasks"] = df["subtasks"].apply(lambda x: json.dumps(x))
            else:
                df["subtasks"] = '[]'
            df["status_nikko"] = 'no'
            df['orginal_id'] = ''
            df['order_nikko'] = ''
            sql = """
                    INSERT INTO [nikko].[3rd_clickup_tasks_gmail_comment](
                        id,
                        custom_id,
                        custom_item_id,
                        name,
                        text_content,
                        description,
                        status,
                        orderindex,
                        date_created,
                        date_updated,
                        date_closed,
                        date_done,
                        archived,
                        creator,
                        assignees,
                        group_assignees,
                        watchers,
                        checklists,
                        tags,
                        parent,
                        priority,
                        due_date,
                        start_date,
                        points,
                        time_estimate,
                        custom_fields,
                        dependencies,
                        linked_tasks,
                        locations,
                        team_id,
                        url,
                        sharing,
                        permission_level,
                        list,
                        project,
                        folder,
                        space
                        ,subtasks
                        ,[status_nikko]
                        ,[orginal_id]
                        ,[order_nikko]
                        ,[dtm_Creation_Date])
                    VALUES( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, getdate())
                """
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
                    str(row[24]),
                    str(row[25]),
                    str(row[26]),
                    str(row[27]),
                    str(row[28]),
                    str(row[29]),
                    str(row[30]),
                    str(row[31]),
                    str(row[32]),
                    str(row[33]),
                    str(row[34]),
                    str(row[35]),
                    str(row[36]),
                    str(row[37]),
                    str(row[38]),
                    str(row[39]),
                    str(row[40])
                )
                values.append(value)
            cursor.executemany(sql, values)
        print(
            f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()
    
    @task
    def call_procedure() -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
       
        sql = f"exec [dbo].[sp_Update_tasks_gmail_v1];"
        cursor.execute(sql)
        sql_conn.commit()
        sql_conn.close()

    def download_single_file(url_download: str) -> str:
        file_name = url_download.split('/')[-1]
        response = requests.get(url_download)
        local_path = TEMP_PATH + FOLDER_NAME + file_name
        if response.status_code == 200:
            with open(local_path, 'wb') as file:
                file.write(response.content)
            print(f"DOWNLOAD: File downloaded {file_name} successfully")
        else:
            print(f"DOWNLOAD: Failed to download file. Status code: {response.status_code} on file name {file_name}")
        return local_path
    
    def post_single_file(df_row):
        # post files
        task_id = df_row["orginal_id"]
        try:
            local_path = download_single_file(url_download=df_row["url_download"])
            if not os.path.isfile(local_path):
                print(f"Error: File {local_path} does not exist.")
                return
            url_upload = CLICKUP_ATTACHMENT.format(task_id)
            print(f"url_upload: {url_upload}")
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

            # post comment
            file_name = local_path.split('/')[-1]
            if response_upload.status_code == 200:
                print('ATTACHMENT: Upload file uploaded successfully.')
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
                    update_clickup_task_gmail(task_id=task_id)
                    print('COMMENT: Comment added successfully.')
                else:
                    print(f'COMMENT: Failed to add comment: {response_comment.status_code} on task: {task_id}')
            else:
                print(f'ATTACHMENT: Failed {response_upload.status_code} to upload file: {file_name}')
        except Exception as e:
            print(f"An error occurred: {str(e)}")

    def update_clickup_task_gmail(task_id: str):
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        sql_update = f"""
                update [nikko].[3rd_clickup_tasks_gmail]
                set status_nikko = 'yes', status_attachment = 'yes'
                where orginal_id = '{task_id}'
                """
        print(sql_update)
        cursor.execute(sql_update)
        sql_conn.commit()
        print(f"SQL: updated status clickup: {task_id} successfully")
        sql_conn.close()

    @task
    def create_comment_clickup():
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = """select *
                from Clickup_comment 
                order by orginal_id, order_nikko; 
        """
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(post_single_file, [
                         row for _, row in df.iterrows()])

    ############ DAG FLOW ############
    check_temp_path_task = check_temp_path()
    list_tasks = call_mutiple_process_tasks_by_list()
    insert_tasks_task = insert_tasks(list_tasks)
    call_procedure_task = call_procedure()
    create_comment_clickup_task = create_comment_clickup()
    remove_files = empty_temp_dir()
    check_temp_path_task >> list_tasks >> insert_tasks_task >> call_procedure_task >> create_comment_clickup_task >> remove_files
    # create_comment_clickup()


dag = Clickup_Comment()
