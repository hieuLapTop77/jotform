import os
import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from common.utils import call_api_mutiple_pages, call_multiple_thread
from common.utils_nikko import download_single_file, update_clickup_task_gmail, insert_tasks, init_date
from requests_toolbelt.multipart.encoder import MultipartEncoder
import concurrent.futures

# Variables
LIST_ID_TASK_DESTINATION = 901802621445
FOLDER_NAME = 'Clickup_purchase_files/'
TABLE_NAME_TASKS = '[nikko].[3rd_clickup_tasks_merge_email_purchase]'
TABLE_TASK_COMMENTS = '[nikko].[3rd_clickup_tasks_email_comment_purchase]'

# Clickup
CLICKUP_GET_TASKS = Variable.get("clickup_get_tasks")
CLICKUP_ATTACHMENT = Variable.get("clickup_attachment")
CLICKUP_COMMENT = Variable.get("clickup_comment")
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

@dag(
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Clickup comment" "Clickup purchase", "comment", "purchase"],
    max_active_runs=1,
)
def Clickup_Comment_Purchase():
    headers = {
            "Authorization": f"{API_TOKEN_NIKO}",
            "Content-Type": "application/json",
        }
    ######################################### API ################################################

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
        params["statuses[]"] = CLICKUP_STATUS
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
       
        sql = f"exec [dbo].[sp_Update_tasks_merge_email_purchase_v1];"
        cursor.execute(sql)
        sql_conn.commit()
        sql_conn.close()

    
    def post_single_file(df_row):
        # post files
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

        # post comment
        file_name = local_path.split('/')[-1]
        if response_upload.status_code == 200:
            print(f'ATTACHMENT: Upload file {file_name} uploaded successfully.')
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
                print(f'COMMENT: Failed to add comment: {response_comment.status_code} on task: {task_id}')
        else:
            print(f'ATTACHMENT: Failed {response_upload.status_code} to upload file: {file_name}')

    @task
    def create_comment_clickup():
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = """select *
                from [dbo].[vw_Clickup_Comment_By_Purchase] 
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
    insert_tasks_task = insert_tasks_sql(list_tasks)
    call_procedure_task = call_procedure()
    create_comment_clickup_task = create_comment_clickup()
    remove_files = empty_temp_dir()
    check_temp_path_task >> list_tasks >> insert_tasks_task >> call_procedure_task >> create_comment_clickup_task >> remove_files
    # create_comment_clickup()


dag = Clickup_Comment_Purchase()
