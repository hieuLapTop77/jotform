import json

import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from common.utils import call_api_mutiple_pages, call_multiple_thread

# Variables
# Clickup
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
def Clickup_Custom_fields():
    headers = {
            "Authorization": f"{API_TOKEN}",
            "Content-Type": "application/json",
        }
    ######################################### API ################################################

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

    list_task_details_task_0 = call_mutiple_process_task_details_0()
    insert_task_details_task_0 = insert_task_details(list_task_details_task_0)

    list_task_details_task_1 = call_mutiple_process_task_details_1()
    insert_task_details_task_1 = insert_task_details(list_task_details_task_1)

    list_custom_fields_task = call_mutiple_process_custom_fields()
    insert_custom_fields_task = insert_custom_fields(list_custom_fields_task)
    call_procedure_task = call_procedure()

    clickup_tasks = TriggerDagRunOperator(
        task_id='clickup_tasks',
        trigger_dag_id='Clickup_Tasks',  
        wait_for_completion=True
    )

    clickup_tasks >> list_task_details_task_0 >> insert_task_details_task_0 >> list_task_details_task_1 >> insert_task_details_task_1 >> list_custom_fields_task >> insert_custom_fields_task >> call_procedure_task


dag = Clickup_Custom_fields()
