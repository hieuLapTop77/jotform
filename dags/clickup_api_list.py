import json

import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from common.utils import call_api_get_list

# Variables
# Clickup
CLICKUP_GET_SPACE_IDS = Variable.get("clickup_get_space_ids")
CLICKUP_GET_SPACES = Variable.get("clickup_get_spaces")
CLICKUP_GET_SPACE_DETAILS = Variable.get("clickup_get_space_details")
CLICKUP_GET_FOLDERS = Variable.get("clickup_get_folders")
CLICKUP_GET_FOLDER_DETAILS = Variable.get("clickup_get_folder_details")
CLICKUP_GET_LISTS = Variable.get("clickup_get_lists")
CLICKUP_GET_LIST_DETAILS = Variable.get("clickup_get_list_details")

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
def Clickup_List():
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

    insert_spaces_task >> list_space_details_task >> insert_space_details_task >> list_folders >> insert_folders_task >> list_folder_details_task >> insert_folder_details_task >> list_lists >> insert_lists_task >> list_list_details_task >> insert_list_details_task  

dag = Clickup_List()
