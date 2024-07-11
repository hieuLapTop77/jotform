import json

import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from concurrent.futures import ProcessPoolExecutor, as_completed


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
    ######################################### API ################################################
    @task
    def call_api_get_space_ids() -> list:
        headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
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
        headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
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
    def call_api_get_space_details() -> dict:
        headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
        list_space = []
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = "select distinct id from [3rd_clickup_list_spaces];"
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        if len(df["id"]) > 0:
            for i in df["id"]:
                response = requests.get(
                    CLICKUP_GET_SPACE_DETAILS.format(i),
                    headers=headers,
                    timeout=None,
                )
                if response.status_code == 200:
                    list_space.append(response.json())
                else:
                    print("Error please check api")
        return list_space

    @task
    def call_api_get_folders() -> dict:
        headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
        list_folders = []
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = "select distinct id from [3rd_clickup_list_spaces];"
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        if len(df["id"]) > 0:
            for i in df["id"]:
                response = requests.get(
                    CLICKUP_GET_FOLDERS.format(i),
                    headers=headers,
                    timeout=None,
                )
                if response.status_code == 200:
                    list_folders.append(response.json())
                else:
                    print("Error please check api")
        return list_folders

    @task
    def call_api_get_folder_details() -> dict:
        headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
        list_folder_details = []
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = "select distinct id from [3rd_clickup_folders];"
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        if len(df["id"]) > 0:
            for i in df["id"]:
                response = requests.get(
                    CLICKUP_GET_FOLDER_DETAILS.format(i),
                    headers=headers,
                    timeout=None,
                )
                if response.status_code == 200:
                    list_folder_details.append(response.json())
                else:
                    print("Error please check api")
        return list_folder_details

    @task
    def call_api_get_lists() -> dict:
        headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
        list_lists = []
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = "select distinct id from [3rd_clickup_folder_details];"
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        if len(df["id"]) > 0:
            for i in df["id"]:
                response = requests.get(
                    CLICKUP_GET_LISTS.format(i),
                    headers=headers,
                    timeout=None,
                )
                if response.status_code == 200:
                    list_lists.append(response.json())
                else:
                    print("Error please check api")
        return list_lists

    @task
    def call_api_get_list_details() -> dict:
        headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
        list_list_details = []
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = "select distinct id from [3rd_clickup_lists];"
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        if len(df["id"]) > 0:
            for i in df["id"]:
                response = requests.get(
                    CLICKUP_GET_LIST_DETAILS.format(i),
                    headers=headers,
                    timeout=None,
                )
                if response.status_code == 200:
                    list_list_details.append(response.json())
                else:
                    print("Error please check api")
        return list_list_details

    @task
    def call_mutiple_process_tasks():
        headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
        list_tasks = []
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = "select distinct id from [3rd_clickup_space_details];"
        df = pd.read_sql(sql, sql_conn)

        with ProcessPoolExecutor(max_workers=4) as executor:  # Adjust max_workers as needed
            futures = [executor.submit(call_api_get_tasks, space_id, headers) for space_id in df["id"]]

            for future in as_completed(futures):
                try:
                    list_tasks.append(future.result())
                except Exception as e:
                    print(f"Error occurred: {e}")

        return list_tasks



    def call_api_get_tasks(space_id, headers):
        list_tasks = []
        params = {"page": 1}

        while True:
            print("Calling api CLICKUP_GET_TASKS at page: ", params["page"])
            response = requests.get(
                CLICKUP_GET_TASKS.format(space_id), headers=headers, params=params, timeout=None
            )
            if response.status_code == 200:
                data = response.json()
                list_tasks.append(data)
                if data["last_page"]:
                    break
                params["page"] += 1
            else:
                print("Error please check api: ", CLICKUP_GET_TASKS.format(space_id))
                break

        return list_tasks


    # def call_api_get_tasks() -> list:
    #     headers = {
    #         "Authorization": f"{API_TOKEN}",
    #         "Content-Type": "application/json",
    #     }
    #     list_tasks = []
    #     hook = mssql.MsSqlHook(HOOK_MSSQL)
    #     sql_conn = hook.get_conn()
    #     sql = "select distinct id from [3rd_clickup_space_details];"
    #     df = pd.read_sql(sql, sql_conn)
    #     for i in df["id"]:
    #         params = {
    #             "page": 1  
    #         }
    #         while True:
    #             print("Calling api CLICKUP_GET_TASKS at page: ", params["page"])
    #             response = requests.get(
    #                 CLICKUP_GET_TASKS.format(i), headers=headers,params=params, timeout=None
    #             )
    #             if response.status_code == 200:
    #                     data = response.json()
    #                     list_tasks.append(data)
    #                     if data["last_page"]:
    #                         break
    #                     params["page"] += 1
    #             else:
    #                 print("Error please check api: ", CLICKUP_GET_TASKS.format(i))

    #     return list_tasks
    @task
    def call_api_get_tasks_by_list(list_tasks: list):
        headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = "select distinct id from [3rd_clickup_list_details];"
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        if len(df["id"]) > 0:
            for i in df["id"]:
                params = {
                    "page": 1  
                }
                while True:
                    print("Calling api CLICKUP_GET_TASKS at page: ", params["page"])
                    response = requests.get(
                        CLICKUP_GET_TASKS.format(i), headers=headers,params=params, timeout=None
                    )
                    if response.status_code == 200:
                        data = response.json()
                        list_tasks.append(data)
                        if data["last_page"]:
                            break
                        params["page"] += 1
                    else:
                        print("Error please check api: ", CLICKUP_GET_TASKS.format(i))
        return list_tasks

    @task
    def call_api_get_task_details() -> list:
        headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
        params = {
            'include_subtasks': 'true',
            "page": 1 
        }
        list_task_details = []
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = "select distinct id from [3rd_clickup_tasks];"
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        if len(df["id"]) > 0:
            for i in df["id"]:
                while True:
                    print("Calling api CLICKUP_GET_TASKS_DETAILS at page: ", params["page"])
                    response = requests.get(
                        CLICKUP_GET_TASKS_DETAILS.format(i), headers=headers, params=params, timeout=None
                    )
                    if response.status_code == 200:
                        data = response.json()
                        list_tasks.append(data)
                        if data["last_page"]:
                            break
                        params["page"] += 1
                    else:
                        print("Error please check api: ", CLICKUP_GET_TASKS_DETAILS.format(i))
        return list_task_details

    @task
    def call_api_get_custom_fields() -> list:
        headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
        list_custom_fields = []
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = "select distinct id from [3rd_clickup_space_details];"
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        if len(df["id"]) > 0:
            for i in df["id"]:
                params = {
                    "page": 1  
                }
                while True:
                    print("Calling api CLICKUP_GET_CUSTOM_FIELDS at page: ", params["page"])
                    response = requests.get(
                        CLICKUP_GET_CUSTOM_FIELDS.format(i), headers=headers, params=params, timeout=None
                    )
                    if response.status_code == 200:
                        data = response.json()
                        list_tasks.append(data)
                        if data["last_page"]:
                            break
                        params["page"] += 1
                    else:
                        print("Error please check api: ", CLICKUP_GET_CUSTOM_FIELDS.format(i))
        return list_custom_fields

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
            df["status"] = df["status"].apply(lambda x: json.dumps(x))
            df["assignee"] = df["assignee"].apply(lambda x: json.dumps(x))
            df["folder"] = df["folder"].apply(lambda x: json.dumps(x))
            df["space"] = df["space"].apply(lambda x: json.dumps(x))
            if 'content' not in df.columns.tolist():
                df['content'] = None
                cols = list(df.columns)
                cols.insert(3, cols.pop(cols.index('content')))
                df = df[cols]
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
            df["status"] = df["status"].apply(lambda x: json.dumps(x))
            df["assignee"] = df["assignee"].apply(lambda x: json.dumps(x))
            df["statuses"] = df["statuses"].apply(lambda x: json.dumps(x))
            df["folder"] = df["folder"].apply(lambda x: json.dumps(x))
            df["space"] = df["space"].apply(lambda x: json.dumps(x))
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
        data = [
            item for sublist in [t["tasks"] for t in list_tasks] for item in sublist
        ]
        for i in data:
            sql_del = f"delete from [dbo].[3rd_clickup_tasks] where id = '{i['id']}';"
            cursor.execute(sql_del)
            sql_conn.commit()
        df = pd.DataFrame(data)
        values = []
        if len(df) > 0:
            df["status"] = df["status"].apply(lambda x: json.dumps(x))
            df["creator"] = df["creator"].apply(lambda x: json.dumps(x))
            df["assignees"] = df["assignees"].apply(lambda x: json.dumps(x))
            df["group_assignees"] = df["group_assignees"].apply(
                lambda x: json.dumps(x))
            df["watchers"] = df["watchers"].apply(lambda x: json.dumps(x))
            df["checklists"] = df["checklists"].apply(lambda x: json.dumps(x))
            df["tags"] = df["tags"].apply(lambda x: json.dumps(x))
            df["custom_fields"] = df["custom_fields"].apply(
                lambda x: json.dumps(x))

            df["dependencies"] = df["dependencies"].apply(
                lambda x: json.dumps(x))
            df["linked_tasks"] = df["linked_tasks"].apply(
                lambda x: json.dumps(x))
            df["locations"] = df["locations"].apply(lambda x: json.dumps(x))

            df["sharing"] = df["sharing"].apply(lambda x: json.dumps(x))
            df["list"] = df["list"].apply(lambda x: json.dumps(x))
            df["project"] = df["project"].apply(lambda x: json.dumps(x))
            df["folder"] = df["folder"].apply(lambda x: json.dumps(x))
            df["space"] = df["space"].apply(lambda x: json.dumps(x))
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
        for i in list_task_details:
            sql_del = f"delete from [dbo].[3rd_clickup_task_details] where id = '{i['id']}';"
            cursor.execute(sql_del)
            sql_conn.commit()
        df = pd.DataFrame(list_task_details)
        values = []
        if len(df) > 0:
            df["status"] = df["status"].apply(lambda x: json.dumps(x))
            df["creator"] = df["creator"].apply(lambda x: json.dumps(x))
            df["assignees"] = df["assignees"].apply(lambda x: json.dumps(x))
            df["group_assignees"] = df["group_assignees"].apply(
                lambda x: json.dumps(x))
            df["watchers"] = df["watchers"].apply(lambda x: json.dumps(x))
            df["checklists"] = df["checklists"].apply(lambda x: json.dumps(x))
            df["tags"] = df["tags"].apply(lambda x: json.dumps(x))
            df["custom_fields"] = df["custom_fields"].apply(
                lambda x: json.dumps(x))

            df["dependencies"] = df["dependencies"].apply(
                lambda x: json.dumps(x))
            df["linked_tasks"] = df["linked_tasks"].apply(
                lambda x: json.dumps(x))
            df["locations"] = df["locations"].apply(lambda x: json.dumps(x))

            df["sharing"] = df["sharing"].apply(lambda x: json.dumps(x))
            df["list"] = df["list"].apply(lambda x: json.dumps(x))
            df["project"] = df["project"].apply(lambda x: json.dumps(x))
            df["folder"] = df["folder"].apply(lambda x: json.dumps(x))
            df["space"] = df["space"].apply(lambda x: json.dumps(x))
            df["attachments"] = df["attachments"].apply(
                lambda x: json.dumps(x))
            if 'subtasks' in df.columns.tolist():
                df["subtasks"] = df["subtasks"].apply(lambda x: json.dumps(x))
            else:
                df["subtasks"] = '[]'
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
        data = [
            item for sublist in [t["fields"] for t in list_custom_fields] for item in sublist
        ]
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

    list_tasks = call_api_get_tasks() 
    list_tasks_ = call_api_get_tasks_by_list(list_tasks)
    insert_tasks_task = insert_tasks(list_tasks_)

    list_task_details_task = call_api_get_task_details()
    insert_task_details_task = insert_task_details(list_task_details_task)

    list_custom_fields_task = call_api_get_custom_fields()
    insert_custom_fields_task = insert_custom_fields(list_custom_fields_task)

    # list_folder_details_task = call_api_get_folder_details()
    insert_spaces_task >> list_space_details_task >> insert_space_details_task >> list_folders >> insert_folders_task >> list_folder_details_task >> insert_folder_details_task >> list_lists >> insert_lists_task >> list_list_details_task >> insert_list_details_task >> list_tasks >> list_tasks_ >> insert_tasks_task >> list_task_details_task >> insert_task_details_task >> list_custom_fields_task >> insert_custom_fields_task


dag = Clickup()
