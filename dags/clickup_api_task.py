import json

import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from common.utils import call_api_mutiple_pages, call_multiple_thread

# Variables
# Clickup
CLICKUP_GET_TASKS = Variable.get("clickup_get_tasks")

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
def Clickup_Tasks():
    headers = {
            "Authorization": f"{API_TOKEN}",
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
        # date = init_date()
        params = {
            "page": 0,
            # "date_updated_gt": date["date_from"],
            # "date_updated_lt": date["date_to"]
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

    ######################################### INSERT DATA ################################################

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


    ############ DAG FLOW ############

    list_tasks = call_mutiple_process_tasks() 
    insert_tasks_task = insert_tasks(list_tasks)
    list_tasks_20 = call_mutiple_process_tasks_by_list_20()
    insert_tasks_task_20 = insert_tasks(list_tasks)
    list_tasks_40 = call_mutiple_process_tasks_by_list_40()
    insert_tasks_task_40 = insert_tasks(list_tasks_40)

    clickup_List = TriggerDagRunOperator(
        task_id='Clickup_List',
        trigger_dag_id='Clickup_List',  
        wait_for_completion=True
    )

    clickup_List >> list_tasks >> insert_tasks_task >> list_tasks_20 >> insert_tasks_task_20 >> list_tasks_40 >> insert_tasks_task_40 


dag = Clickup_Tasks()
