import json

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
import copy
import concurrent.futures
from requests_toolbelt.multipart.encoder import MultipartEncoder

# Variables
# Clickup
CLICKUP_GET_TASKS = Variable.get("clickup_get_tasks")
CLICKUP_GET_TASKS_DETAILS = Variable.get("clickup_get_task_details")
CLICKUP_GET_CUSTOM_FIELDS = Variable.get("clickup_get_custom_fields")

# Local path
TEMP_PATH = Variable.get("temp_path")
CLICKUP_DELETE_TASK = Variable.get("clickup_delete_task")
CLICKUP_CREATE_TASK = Variable.get("clickup_create_task")
HOOK_MSSQL = Variable.get("mssql_connection")
API_TOKEN = Variable.get("api_token")
ID_LIST_TASK = 901802542742
FOLDER_NAME = 'Clickup_files/'

CLICKUP_ATTACHMENT = Variable.get("clickup_attachment")
# CLICKUP_COMMENT = Variable.get("clickup_comment")

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
    "custom_fields": [
        {
            "id": "f382f1c5-41a7-4222-9787-ec83cd4ad7cc",
            "name": "Created by",
            "value": None
        }
    ],
    "attachments": [],
}
@dag(
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Clickup Gmail"],
    max_active_runs=1,
)
def Clickup_Gmail():
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
        sql = "select 223604951 id;"
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
        sql = "select id from [nikko].[3rd_clickup_tasks_gmail];"
        return call_multiple_thread(hook_sql=HOOK_MSSQL,sql=sql,function=call_api_get_task_details,function_name='call_api_get_task_details')
    
    ######################################### INSERT DATA ################################################

    @task
    def insert_tasks(list_tasks: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        print(list_tasks)
        filtered_data = [item for item in list_tasks if not (isinstance(item, list) and len(item) > 0 and isinstance(item[0], dict) and item[0].get('tasks') == [] and item[0].get('last_page'))]
        data = [item for sublist in filtered_data for t in sublist for item in t["tasks"]]
        sql_select = 'select distinct id from [nikko].[3rd_clickup_tasks_gmail]'
        df_sql = pd.read_sql(sql_select, sql_conn)
        df = pd.DataFrame(data)
        df = df[~df['id'].isin(df_sql['id'])]
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
                    INSERT INTO [nikko].[3rd_clickup_tasks_gmail](
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
    def insert_task_details(list_task_details: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        list_data = [j for i in list_task_details for j in i]
        # for i in list_data:
        #     sql_del = f"delete from [nikko].[3rd_clickup_task_details_gmail] where id = '{i['id']}';"
        #     cursor.execute(sql_del)
        #     sql_conn.commit()
        # df = pd.DataFrame(list_data)
        sql_select = 'select distinct id from [nikko].[3rd_clickup_task_details_gmail]'
        df_sql = pd.read_sql(sql_select, sql_conn)
        df = pd.DataFrame(list_data)
        df = df[~df['id'].isin(df_sql['id'])]
        values = []
        if len(df) > 0:
            columns_to_json = [
                "status", "creator", "assignees", "group_assignees", "watchers",
                "checklists", "tags", "custom_fields", "dependencies", "linked_tasks",
                "locations", "sharing", "list", "project", "folder", "space", "attachments"
            ]
            for column in columns_to_json:
                if column in df.columns.tolist():
                    df[column] = df[column].apply(lambda x: json.dumps(x))
            if 'subtasks' in df.columns.tolist():
                df["subtasks"] = df["subtasks"].apply(lambda x: json.dumps(x))
            else:
                df["subtasks"] = '[]'

            if 'time_sent' in df.columns.tolist():
                df["time_sent"] = df["time_sent"].apply(lambda x: json.dumps(x))
            else:
                df["time_sent"] = ''

            df["status_nikko"] = 'no'
            df['orginal_id'] = ''
            df['order_nikko'] = ''
            new_col = ['id','custom_id','custom_item_id','name','text_content','description','status','orderindex','date_created','date_updated','date_closed','date_done','archived',
                        'creator','assignees','group_assignees','watchers','checklists','tags','parent','priority','due_date','start_date','points','time_estimate',
                        'time_sent','custom_fields','dependencies','linked_tasks','locations','team_id','url','sharing','permission_level','list',
                        'project','folder','space','subtasks','attachments', 'status_nikko', 'orginal_id', 'order_nikko']
            df = df[new_col]
            print(df.columns.tolist())
            sql = """
                    INSERT INTO [nikko].[3rd_clickup_task_details_gmail](
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
                        time_sent,
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
                        ,attachments
                        ,[status_nikko]
                        ,[orginal_id]
                        ,[order_nikko]
                        ,[dtm_Creation_Date])
                    VALUES( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s, %s, getdate())
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
                    str(row[40]),
                    str(row[41]),
                    str(row[42])
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
       
        sql = f"exec [dbo].[sp_Update_tasks_gmail];"
        cursor.execute(sql)
        sql_conn.commit()
        sql_conn.close()
    
    def create_task_payload(df_row):
        body = copy.deepcopy(BODY_TEMPLATE)
        body['name'] = df_row['name']
        body['date_created'] = int(datetime.now().timestamp() * 1000)
        body['status'] = "open"
        body["description"] = df_row["description"]
        for field in body["custom_fields"]:
            if field["id"] == "f382f1c5-41a7-4222-9787-ec83cd4ad7cc": #"created by"
                field["value"] = df_row['created_by'] if df_row['created_by'] else None
        return json.loads(json.dumps(body, ensure_ascii=False))
    
    def update_status(task_id: str):
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        sql_update = f"""
                update [nikko].[3rd_clickup_tasks_gmail]
                set status_nikko = 'yes'
                where id = '{task_id}'
                """
        print(sql_update)
        cursor.execute(sql_update)
        sql_conn.commit()
        print(f"updated status clickup email: {task_id} successfully")
        sql_conn.close()
    
    def create_task(df_row):
        main_task_payload = create_task_payload(df_row)
        res = requests.post(CLICKUP_CREATE_TASK.format(
            ID_LIST_TASK), json=main_task_payload, headers=headers)
        if res.status_code == 200:
            task_id = res.json()['id']
            print(f"Created parent task: {task_id}")
            update_status(task_id=df_row["id"])
        else:
            print("create task fail: ", res.status_code)

    @task
    def create_order_clickup():
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = """
            select id, name, description
                ,case when description <> '' and substring(description, 0, 30) like '%@%' then replace((SUBSTRING(description, 1, CHARINDEX(CHAR(10) , description) - 1)),' ','') else '' end created_by 
            from [nikko].[3rd_clickup_tasks_gmail] where order_nikko = 1 and status_nikko = 'no';
        """
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(create_task, [
                         row for _, row in df.iterrows()])
    
    
    ############ DAG FLOW ############

    list_tasks = call_mutiple_process_tasks_by_list()
    insert_tasks_task = insert_tasks(list_tasks)

    list_task_details_task = call_mutiple_process_task_details()
    insert_task_details_task = insert_task_details(list_task_details_task)

    call_procedure_task = call_procedure()
    create_order_clickup_task = create_order_clickup()

    list_tasks >> insert_tasks_task  >> list_task_details_task >> insert_task_details_task >> call_procedure_task >> create_order_clickup_task
    # create_order_clickup()

dag = Clickup_Gmail()
