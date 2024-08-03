import json
from typing import Dict
from datetime import datetime, timedelta
import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
import requests

def insert_tasks(list_tasks: list, hook_mssql:str, table_name: str, sql=None) -> None:
    HOOK_MSSQL = hook_mssql
    hook = mssql.MsSqlHook(HOOK_MSSQL)
    sql_conn = hook.get_conn()
    cursor = sql_conn.cursor()
    print("---------------------------------------------INSERT DATA----------------------------------------------------------")
    filtered_data = [item for item in list_tasks if not (isinstance(item, list) and len(item) > 0 and isinstance(item[0], dict) and item[0].get('tasks') == [] and item[0].get('last_page'))]
    data = [item for sublist in filtered_data for t in sublist for item in t["tasks"]]
    df: pd.DataFrame = None
    if len(data) > 0:
        if sql is not None:
            cursor.execute(sql)
            sql_conn.commit()
            df = pd.DataFrame(data)
        else:
            sql_select = f"select distinct id from {table_name}"
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
                    INSERT INTO {}(
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
                """.format(table_name)
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
            f"Inserted {len(values)} rows in table name {table_name} with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()

def insert_task_details(list_task_details: list, hook_mssql: str, table_name: str) -> None:
    HOOK_MSSQL = hook_mssql
    hook = mssql.MsSqlHook(HOOK_MSSQL)
    sql_conn = hook.get_conn()
    cursor = sql_conn.cursor()
    list_data = [j for i in list_task_details for j in i]
    # for i in list_data:
    #     sql_del = f"delete from [nikko].[3rd_clickup_task_details_gmail] where id = '{i['id']}';"
    #     cursor.execute(sql_del)
    #     sql_conn.commit()
    # df = pd.DataFrame(list_data)
    if len(list_data) > 0:
        sql_select = f"select distinct id from {table_name}"
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
            new_col = ['id','custom_id','custom_item_id','name','text_content','description','status','orderindex','date_created',
                       'date_updated','date_closed','date_done','archived','creator','assignees','group_assignees','watchers','checklists',
                       'tags','parent','priority', 'due_date','start_date','points','time_estimate','time_sent','custom_fields','dependencies',
                       'linked_tasks','locations','team_id','url','sharing','permission_level','list','project','folder','space','subtasks','attachments', 
                       'status_nikko', 'orginal_id', 'order_nikko']
            df = df[new_col]
            print(df.columns.tolist())
            sql = f"""
                    INSERT INTO {table_name}(
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

def update_status(task_id: str, hook_mssql:str, table_name: str):
    hook = mssql.MsSqlHook(hook_mssql)
    sql_conn = hook.get_conn()
    cursor = sql_conn.cursor()
    sql_update = f"""
            update {table_name}
            set status_nikko = 'yes'
            where id = '{task_id}'
            """
    print(sql_update)
    cursor.execute(sql_update)
    sql_conn.commit()
    print(f"updated status clickup on table name '{table_name}' with task_id '{task_id}' successfully")
    sql_conn.close()

def call_query_sql(hook_mssql:str, query: str):
    hook = mssql.MsSqlHook(hook_mssql)
    sql_conn = hook.get_conn()
    cursor = sql_conn.cursor()
    print(query)
    cursor.execute(query)
    sql_conn.commit()
    print("--------------------------Run query successfully-----------------------")
    sql_conn.close()


def download_single_file(url_download: str, temp_path: str, folder_name: str) -> str:
    file_name = url_download.split('/')[-1]
    response = requests.get(url_download)
    local_path = temp_path + folder_name + file_name
    if response.status_code == 200:
        with open(local_path, 'wb') as file:
            file.write(response.content)
        print(f"DOWNLOAD: File downloaded {file_name} successfully")
    else:
        print(f"DOWNLOAD: Failed to download file. Status code: {response.status_code} on file name {file_name}")
    return local_path

def init_date() -> Dict[str, str]:
    current_time = datetime.now()
    date_to = int(current_time.timestamp()*1000)
    time_minus_24_hours = current_time - timedelta(hours=24)
    date_from = int(time_minus_24_hours.timestamp() * 1000)
    return {"date_from": date_from, "date_to": date_to}
