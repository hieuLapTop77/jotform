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
from concurrent.futures import ThreadPoolExecutor, as_completed
import billiard as multiprocessing
from common.utils import call_api_get_list, call_api_mutiple_pages, call_multiple_thread

# Variables
# Clickup
CLICKUP_GET_SPACE_IDS = Variable.get("clickup_get_space_ids")
CLICKUP_GET_SPACES = Variable.get("clickup_get_spaces")
CLICKUP_GET_SPACE_DETAILS = Variable.get("clickup_get_space_details")
CLICKUP_GET_FOLDERS = Variable.get("clickup_get_folders")
CLICKUP_GET_FOLDER_DETAILS = Variable.get("clickup_get_folder_details")
CLICKUP_GET_LISTS = Variable.get("clickup_get_lists")
CLICKUP_GET_LIST_DETAILS = Variable.get("clickup_get_list_details")
CLICKUP_GET_TASKS = Variable.get("clickup_get_tasks")
CLICKUP_GET_TASKS_DETAILS = Variable.get("clickup_get_task_details")
CLICKUP_GET_CUSTOM_FIELDS = Variable.get("clickup_get_custom_fields")

# Local path
TEMP_PATH = Variable.get("temp_path")

# Token
API_TOKEN = Variable.get("api_token")

# Conection
HOOK_MSSQL = Variable.get("mssql_connection")

data = []

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="0 */2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Clickup"],
    max_active_runs=1,
)
def Clickup():
    headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
    ######################################### API ################################################
    @task
    def call_api_get_space_ids() -> list:
        list_space_ids = []
        response = requests.get(CLICKUP_GET_SPACE_IDS,
                                headers=headers, timeout=None)
        if response.status_code == 200:
            for i in response.json()["teams"]:
                list_space_ids.append(i["id"])
        else:
            print("Error please check api")
        return list_space_ids

    @task
    def call_api_get_spaces(list_space_ids: list) -> list:
        list_spaces = []

        for i in list_space_ids:
            response = requests.get(
                CLICKUP_GET_SPACES.format(i), headers=headers, timeout=None
            )
            if response.status_code == 200:
                list_spaces.append(response.json())
            else:
                print("Error please check api")
        return list_spaces

    @task
    def call_api_get_space_details() -> list:
        sql = "select distinct id from [3rd_clickup_list_spaces];"
        return call_api_get_list(sql=sql,hook_sql=HOOK_MSSQL,url=CLICKUP_GET_SPACE_DETAILS,headers=headers)

    @task
    def call_api_get_folders() -> list:
        sql = "select distinct id from [3rd_clickup_list_spaces];"
        return call_api_get_list(sql=sql,hook_sql=HOOK_MSSQL,url=CLICKUP_GET_FOLDERS,headers=headers)

    @task
    def call_api_get_folder_details() -> dict:
        sql = "select distinct id from [3rd_clickup_folders];"
        return call_api_get_list(sql=sql,hook_sql=HOOK_MSSQL,url=CLICKUP_GET_FOLDER_DETAILS,headers=headers)

    @task
    def call_api_get_lists() -> list:
        sql = "select distinct id from [3rd_clickup_folder_details];"
        return call_api_get_list(sql=sql,hook_sql=HOOK_MSSQL,url=CLICKUP_GET_LISTS, headers=headers)

    @task
    def call_api_get_list_details() -> dict:
        sql = "select distinct id from [3rd_clickup_lists];"
        return call_api_get_list(sql=sql,hook_sql=HOOK_MSSQL,url=CLICKUP_GET_LIST_DETAILS, headers=headers)

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
            "date_created_gt": date["date_from"],
            "date_created_lt": date["date_to"],
            "date_updated_gt": date["date_from"],
            "date_updated_lt": date["date_to"]
        }
        name_url = 'CLICKUP_GET_TASKS'
        return call_api_mutiple_pages(headers=headers,params=params, name_url=name_url,url=CLICKUP_GET_TASKS,task_id=space_id)
    
    @task
    def call_mutiple_process_tasks():
        sql = "select distinct id from [3rd_clickup_space_details];"
        return call_multiple_thread(hook_sql=HOOK_MSSQL,sql=sql,function=call_api_get_tasks,function_name='call_api_get_tasks')

    @task
    def call_mutiple_process_tasks_by_list_20() -> list:
        # sql = "select distinct id from [3rd_clickup_list_details] where id in ('901802181276', '901802168382');"
        sql = "select distinct id from [3rd_clickup_list_details];"
        return call_multiple_thread(hook_sql=HOOK_MSSQL,sql=sql,function=call_api_get_tasks,function_name='call_mutiple_process_tasks_by_list_20', range_from=0, range_to=20)

    @task
    def call_mutiple_process_tasks_by_list_40() -> list:
        # sql = "select distinct id from [3rd_clickup_list_details] where id not in ('901802181276', '901802168382');"
        sql = "select distinct id from [3rd_clickup_list_details];"
        return call_multiple_thread(hook_sql=HOOK_MSSQL,sql=sql,function=call_api_get_tasks,function_name='call_mutiple_process_tasks_by_list_40',range_from=20, range_to=None)

    def call_api_get_task_details(task_id):
        params = {
            'include_subtasks': 'true',
            "page": 0
        }
        name_url = 'CLICKUP_GET_TASKS_DETAILS'
        return call_api_mutiple_pages(headers=headers,params=params, name_url=name_url,url=CLICKUP_GET_TASKS_DETAILS,task_id=task_id)
    
    @task
    def call_mutiple_process_task_details_0() -> list:
        sql = "select distinct id from [3rd_clickup_tasks]"# where dtm_Creation_Date >= DATEADD(hour, -20, GETDATE()) order by dtm_Creation_Date desc;"
        return call_multiple_thread(hook_sql=HOOK_MSSQL,sql=sql,function=call_api_get_task_details,function_name='call_api_get_task_details',range_from=0, range_to=5500)
    
    @task
    def call_mutiple_process_task_details_1() -> list:
        sql = "select distinct id from [3rd_clickup_tasks]" #where dtm_Creation_Date >= DATEADD(hour, -20, GETDATE()) order by dtm_Creation_Date desc;"
        return call_multiple_thread(hook_sql=HOOK_MSSQL,sql=sql,function=call_api_get_task_details,function_name='call_api_get_task_details',range_from=5500, range_to=None)

    def call_api_get_custom_fields(space_id):
        params = {
            "page": 0
        }
        name_url = 'CLICKUP_GET_CUSTOM_FIELDS'
        return call_api_mutiple_pages(headers=headers,params=params, name_url=name_url,url=CLICKUP_GET_CUSTOM_FIELDS,task_id=space_id)
    
    @task
    def call_mutiple_process_custom_fields() -> list:
        sql = "select distinct id from [3rd_clickup_space_details];"
        return call_multiple_thread(hook_sql=HOOK_MSSQL,sql=sql,function=call_api_get_custom_fields,function_name='call_api_get_custom_fields')

    ######################################### INSERT DATA ################################################
    @task
    def insert_spaces(list_spaces: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        sql_del = "delete from [dbo].[3rd_clickup_list_spaces];"
        cursor.execute(sql_del)
        all_spaces = []
        for item in list_spaces:
            spaces = item["spaces"]
            for space in spaces:
                space["statuses"] = json.dumps(space["statuses"])
                space["features"] = json.dumps(space["features"])
                all_spaces.append(space)

        df = pd.DataFrame(all_spaces)
        values = []
        if len(df) > 0:
            sql = """
                    INSERT INTO [dbo].[3rd_clickup_list_spaces](
                        [id]
                        ,[name]
                        ,[color]
                        ,[private]
                        ,[avatar]
                        ,[admin_can_manage]
                        ,[statuses]
                        ,[multiple_assignees]
                        ,[features]
                        ,[archived] 
                        ,[dtm_Creation_Date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
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
                )
                values.append(value)
            cursor.executemany(sql, values)

        print(
            f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()

    @task
    def insert_space_details(list_space: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        data = [
            item for sublist in [d["lists"] for d in list_space] for item in sublist
        ]
        for i in data:
            sql_del = f"delete from [dbo].[3rd_clickup_space_details] where id = '{i['id']}';"
            print(sql_del)
            cursor.execute(sql_del)
            sql_conn.commit()
        df = pd.DataFrame(data)
        values = []
        if len(df) > 0:
            df["folder"] = df["folder"].apply(lambda x: json.dumps(x))
            df["space"] = df["space"].apply(lambda x: json.dumps(x))
            sql = """
                    INSERT INTO [dbo].[3rd_clickup_space_details](
                        id, 
                        name, 
                        orderindex, 
                        content, 
                        status, 
                        priority, 
                        assignee,
                        task_count, 
                        due_date, 
                        start_date, 
                        folder, 
                        space, 
                        archived,
                        override_statuses, 
                        permission_level
                        ,[dtm_Creation_Date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
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
                )
                values.append(value)
            print(values)
            cursor.executemany(sql, values)
        print(
            f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()

    @task
    def insert_folders(list_folders: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        data = [
            item for sublist in [d["folders"] for d in list_folders] for item in sublist
        ]
        for i in data:
            sql_del = f"delete from [dbo].[3rd_clickup_folders] where id = '{i['id']}';"
            print(sql_del)
            cursor.execute(sql_del)
            sql_conn.commit()
        df = pd.DataFrame(data)
        values = []
        if len(df) > 0:
            df["lists"] = df["lists"].apply(lambda x: json.dumps(x))
            df["statuses"] = df["statuses"].apply(lambda x: json.dumps(x))
            df["space"] = df["space"].apply(lambda x: json.dumps(x))
            sql = """
                    INSERT INTO [dbo].[3rd_clickup_folders](
                        id, 
                        name, 
                        orderindex, 
                        override_statuses,
                        hidden,
                        space,
                        task_count,
                        archived,
                        statuses,
                        lists,
                        permission_level
                        ,[dtm_Creation_Date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
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
                )
                values.append(value)
            print(values)
            cursor.executemany(sql, values)
        print(
            f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()

    @task
    def insert_folder_details(list_folder_details: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        for i in list_folder_details:
            sql_del = f"delete from [dbo].[3rd_clickup_folder_details] where id = '{i['id']}';"
            print(sql_del)
            cursor.execute(sql_del)
            sql_conn.commit()
        df = pd.DataFrame(list_folder_details)
        values = []
        if len(df) > 0:
            df["statuses"] = df["statuses"].apply(lambda x: json.dumps(x))
            df["lists"] = df["lists"].apply(lambda x: json.dumps(x))
            df["space"] = df["space"].apply(lambda x: json.dumps(x))
            sql = """
                    INSERT INTO [dbo].[3rd_clickup_folder_details](
                        id, 
                        name, 
                        orderindex, 
                        override_statuses,
                        hidden,
                        space,
                        task_count,
                        archived,
                        statuses,
                        lists, 
                        permission_level
                        ,[dtm_Creation_Date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
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
                )
                values.append(value)
            print(values)
            cursor.executemany(sql, values)
        print(
            f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()

    @task
    def insert_lists(list_lists: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        data = [
            item for sublist in [d["lists"] for d in list_lists] for item in sublist
        ]
        for i in data:
            sql_del = f"delete from [dbo].[3rd_clickup_lists] where id = '{i['id']}';"
            print(sql_del)
            cursor.execute(sql_del)
            sql_conn.commit()
        df = pd.DataFrame(data)
        values = []
        if len(df) > 0:
            columns_to_json = [
                 "status", "folder", "assignee", "space"
            ]
            for column in columns_to_json:
                if column in df.columns.tolist():
                    df[column] = df[column].apply(lambda x: json.dumps(x))
            if 'content' not in df.columns.tolist():
                df['content'] = None

            cols = list(df.columns)
            cols.insert(3, cols.pop(cols.index('content')))
            df = df[cols]
            print(df.columns.tolist())
            sql = """
                    INSERT INTO [dbo].[3rd_clickup_lists](
                        id, 
                        name, 
                        orderindex, 
                        content,
                        status,
                        priority,
                        assignee,
                        task_count,
                        due_date,
                        start_date,
                        folder,
                        space,
                        archived,
                        override_statuses,
                        permission_level
                        ,[dtm_Creation_Date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
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
                )
                values.append(value)
                print("values: ", value)
            # print(values)
            cursor.executemany(sql, values)
        print(
            f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()

    @task
    def insert_list_details(list_list_details: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        for i in list_list_details:
            sql_del = f"delete from [dbo].[3rd_clickup_list_details] where id = '{i['id']}';"
            print(sql_del)
            cursor.execute(sql_del)
            sql_conn.commit()
        df = pd.DataFrame(list_list_details)
        values = []
        if len(df) > 0:
            columns_to_json = [
                 "folder", "space", "assignee", "statuses"
            ]
            for column in columns_to_json:
                if column in df.columns.tolist():
                    df[column] = df[column].apply(lambda x: json.dumps(x))
            if 'status' not in df.columns.tolist():
                df['status'] = None
            else:
                df["status"] = df["status"].apply(lambda x: json.dumps(x))
            new_col = ['id', 'name', 'deleted', 'orderindex', 'content', 'status',
                       'priority', 'assignee', 'due_date', 'start_date', 'folder', 
                       'space', 'inbound_address', 'archived', 'override_statuses', 'statuses', 
                       'permission_level']

            df = df[new_col]
            print(df.columns.tolist())
            sql = """
                    INSERT INTO [dbo].[3rd_clickup_list_details](
                        id,
                        name,
                        deleted,
                        orderindex,
                        content,
                        status,
                        priority,
                        assignee,
                        due_date,
                        start_date,
                        folder,
                        space,
                        inbound_address,
                        archived,
                        override_statuses,
                        statuses,
                        permission_level
                        ,[dtm_Creation_Date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
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
                )
                values.append(value)
            print(values)
            cursor.executemany(sql, values)
        print(
            f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()

    @task
    def insert_tasks(list_tasks: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        filtered_data = [item for item in list_tasks if not (isinstance(item, list) and len(item) > 0 and isinstance(item[0], dict) and item[0].get('tasks') == [] and item[0].get('last_page'))]
        data = [item for sublist in filtered_data for t in sublist for item in t["tasks"]]
        for i in data:
            sql_del = f"delete from [dbo].[3rd_clickup_tasks] where id = '{i['id']}';"
            cursor.execute(sql_del)
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
            sql = """
                    INSERT INTO [dbo].[3rd_clickup_tasks](
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
                        ,[dtm_Creation_Date])
                    VALUES( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s, getdate())
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
        for i in list_data:
            sql_del = f"delete from [dbo].[3rd_clickup_task_details] where id = '{i['id']}';"
            cursor.execute(sql_del)
            sql_conn.commit()
        df = pd.DataFrame(list_data)
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

            new_col = ['id','custom_id','custom_item_id','name','text_content','description','status','orderindex','date_created','date_updated','date_closed','date_done','archived',
                        'creator','assignees','group_assignees','watchers','checklists','tags','parent','priority','due_date','start_date','points','time_estimate',
                        'time_sent','custom_fields','dependencies','linked_tasks','locations','team_id','url','sharing','permission_level','list',
                        'project','folder','space','subtasks','attachments']
            df = df[new_col]
            print(df.columns.tolist())
            sql = """
                    INSERT INTO [dbo].[3rd_clickup_task_details](
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
                        ,[dtm_Creation_Date])
                    VALUES( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, getdate())
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
                )
                values.append(value)
            cursor.executemany(sql, values)
        print(
            f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()

    @task
    def insert_custom_fields(list_custom_fields: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        data = [item for sublist in list_custom_fields for t in sublist for item in t["fields"]]
        for i in data:
            sql_del = f"delete from [dbo].[3rd_clickup_custom_fields] where id = '{i['id']}';"
            cursor.execute(sql_del)
            sql_conn.commit()
        df = pd.DataFrame(data)
        values = []
        if len(df) > 0:
            df["type_config"] = df["type_config"].apply(
                lambda x: json.dumps(x))
            sql = """
                    INSERT INTO [dbo].[3rd_clickup_custom_fields](
                        id,
                        name,
                        type,
                        type_config,
                        date_created,
                        hide_from_guests,
                        required
                        ,[dtm_Creation_Date])
                    VALUES( %s,%s,%s,%s,%s,%s,%s, getdate())
                """
            for _index, row in df.iterrows():
                value = (
                    str(row[0]),
                    str(row[1]),
                    str(row[2]),
                    str(row[3]),
                    str(row[4]),
                    str(row[5]),
                    str(row[6])
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
       
        sql = f"exec [sp_Custom_Fields_List];"
        cursor.execute(sql)
        sql_conn.commit()
        sql_conn.close()

    ############ DAG FLOW ############

    list_ids = call_api_get_space_ids()
    list_spaces = call_api_get_spaces(list_ids)
    insert_spaces_task = insert_spaces(list_spaces)

    list_space_details_task = call_api_get_space_details()
    insert_space_details_task = insert_space_details(list_space_details_task)

    list_folders = call_api_get_folders()
    insert_folders_task = insert_folders(list_folders)

    list_folder_details_task = call_api_get_folder_details()
    insert_folder_details_task = insert_folder_details(
        list_folder_details_task)

    list_lists = call_api_get_lists()
    insert_lists_task = insert_lists(list_lists)

    list_list_details_task = call_api_get_list_details()
    insert_list_details_task = insert_list_details(list_list_details_task)

    list_tasks = call_mutiple_process_tasks() 
    insert_tasks_task = insert_tasks(list_tasks)
    list_tasks_20 = call_mutiple_process_tasks_by_list_20()
    insert_tasks_task_20 = insert_tasks(list_tasks)
    list_tasks_40 = call_mutiple_process_tasks_by_list_40()
    insert_tasks_task_40 = insert_tasks(list_tasks_40)

    list_task_details_task_0 = call_mutiple_process_task_details_0()
    insert_task_details_task_0 = insert_task_details(list_task_details_task_0)

    list_task_details_task_1 = call_mutiple_process_task_details_1()
    insert_task_details_task_1 = insert_task_details(list_task_details_task_1)

    list_custom_fields_task = call_mutiple_process_custom_fields()
    insert_custom_fields_task = insert_custom_fields(list_custom_fields_task)
    call_procedure_task = call_procedure()

    # list_folder_details_task = call_api_get_folder_details()
    # list_tasks >> list_tasks_20 >> list_tasks_40 >> insert_tasks_task 


    insert_spaces_task >> list_space_details_task >> insert_space_details_task >> list_folders >> insert_folders_task >> list_folder_details_task >> insert_folder_details_task >> list_lists >> insert_lists_task >> list_list_details_task >> insert_list_details_task  >> list_tasks >> insert_tasks_task >> list_tasks_20 >> insert_tasks_task_20 >> list_tasks_40 >> insert_tasks_task_40>> list_task_details_task_0 >> insert_task_details_task_0 >> list_task_details_task_1 >> insert_task_details_task_1 >> list_custom_fields_task >> insert_custom_fields_task >> call_procedure_task


dag = Clickup()
