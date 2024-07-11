import json
from datetime import datetime, timedelta, timezone

import airflow.providers.amazon.aws.hooks.s3 as s3
import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context, task
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
# Variables
## Fillout
FILLOUT_GET_FORM = Variable.get("fillout_get_form")
FILLOUT_GET_FORM_METADATA = Variable.get("fillout_get_form_metadata")
FILLOUT_GET_ALL_SUBMISSIONS = Variable.get("fillout_get_all_submissions")
FILLOUT_GET_SUBMISSION_BY_ID = Variable.get("fillout_get_submission_by_id")
FILLOUT_CREATE_WEBHOOK = Variable.get("fillout_create_webhook")
FILLOUT_REMOVE_WEBHOOK = Variable.get("fillout_remove_webhook")

## Local path
TEMP_PATH = Variable.get("temp_path")

## Token
BEARER_TOKEN = Variable.get("bearer_token")

## Conection
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
    tags=["Fillout"],
    max_active_runs=1,
)
def Fillout():
    
    @task
    def call_api_get_form() -> list:
        headers = {
            "Authorization": f"Bearer {BEARER_TOKEN}",
            "Content-Type": "application/json",
        }
        list_form_ids = None
        response = requests.get(FILLOUT_GET_FORM, headers=headers, timeout=None)
        if response.status_code == 200:
            list_form_ids = response.json()
        else:
            print("Error please check api")
        return list_form_ids

    @task
    def call_api_get_form_metadata() -> list:
        headers = {
            "Authorization": f"Bearer {BEARER_TOKEN}",
            "Content-Type": "application/json",
        }
        list_forms = []
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = "select distinct formId from [3rd_fillout_forms];"
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        print("df:", df)
        if len(df['formId']) > 0:
            for i in df['formId']:
                response = requests.get(
                    FILLOUT_GET_FORM_METADATA.format(i),
                    headers=headers,
                    timeout=None,
                )
                print(response.json())
                if response.status_code == 200:
                    list_forms.append(response.json())
                else:
                    print("Error please check api")
        else:
            print('Not found formId')
        return list_forms

    @task
    def call_api_get_all_submissions() -> list:
        headers = {
            "Authorization": f"Bearer {BEARER_TOKEN}",
            "Content-Type": "application/json",
        }
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql = "select distinct formId from [3rd_fillout_forms];"
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        list_submissions = []
        print('df: ',df)
        if len(df['formId']) > 0:
            for i in df['formId']:
                response = requests.get(
                    FILLOUT_GET_ALL_SUBMISSIONS.format(i),
                    headers=headers,
                    timeout=None,
                )
                print(response.json())
                if response.status_code == 200:
                    list_submissions.append(response.json())
                else:
                    print("Error please check api")
        else:
            print('Not found formId')
        return list_submissions

    @task
    def insert_forms(list_get_forms: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        sql_del = "delete from [dbo].[3rd_fillout_forms];"
        cursor.execute(sql_del)
        df = pd.DataFrame(list_get_forms)
        sql = """
                INSERT INTO [dbo].[3rd_fillout_forms](
                    [name]
                    ,[formId]
                    ,[id]
                    ,[dtm_Creation_Date])
                VALUES(%s, %s, %s, getdate())
            """
        values = []
        for _index, row in df.iterrows():
            value = (str(row[0]), str(row[1]), str(row[2]))
            values.append(value)
        cursor.executemany(sql, values)
        print(f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()

    @task
    def insert_form_metadata(list_form_metadata: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        for i in list_form_metadata:
            sql_del = f"delete from [dbo].[3rd_fillout_form_metadata] where id = '{i['id']}';"
            cursor.execute(sql_del)
        sql_conn.commit()
        values = []
        df = pd.DataFrame(list_form_metadata)
        if len(df) > 0:
            df["questions"] = df["questions"].apply(lambda x: json.dumps(x))
            df["calculations"] = df["calculations"].apply(lambda x: json.dumps(x))
            df["urlParameters"] = df["urlParameters"].apply(lambda x: json.dumps(x))
            df["documents"] = df["documents"].apply(lambda x: json.dumps(x))
            df["scheduling"] = df["scheduling"].apply(lambda x: json.dumps(x))
            sql = """
                    INSERT INTO [dbo].[3rd_fillout_form_metadata](
                        id,
                        name,
                        questions,
                        calculations,
                        urlParameters,
                        documents,
                        scheduling
                        ,[dtm_Creation_Date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, getdate())
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
                )
                values.append(value)
            cursor.executemany(sql, values)
        print(f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()

    @task
    def insert_submissions(list_all_submissions: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        data = [item for sublist in [t["responses"] for t in list_all_submissions] for item in sublist]
        for i in data:
            sql_del = f"delete from [dbo].[3rd_fillout_submissions] where submissionId = '{i['submissionId']}';"
            print(sql_del)
            cursor.execute(sql_del)
            sql_conn.commit()
        df = pd.DataFrame(data)
        values = []
        if len(df) > 0:
            df["questions"] = df["questions"].apply(lambda x: json.dumps(x))
            df["calculations"] = df["calculations"].apply(lambda x: json.dumps(x))
            df["urlParameters"] = df["urlParameters"].apply(lambda x: json.dumps(x))
            df["documents"] = df["documents"].apply(lambda x: json.dumps(x))
            df["scheduling"] = df["scheduling"].apply(lambda x: json.dumps(x))
            sql = """
                    INSERT INTO [dbo].[3rd_fillout_submissions](
                        submissionId,
                        submissionTime,
                        lastUpdatedAt,
                        questions,
                        calculations,
                        urlParameters,
                        documents,
                        scheduling,
                        [dtm_Creation_Date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, getdate())
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
                )
                values.append(value)
            cursor.executemany(sql, values)
        print(f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()

    ############ DAG FLOW ############
    list_forms = call_api_get_form()
    insert_forms_task = insert_forms(list_forms) 
    list_metadata = call_api_get_form_metadata()
    insert_form_metadata_task = insert_form_metadata(list_metadata)
    list_submissions = call_api_get_all_submissions()
    insert_submissions_task = insert_submissions(list_submissions) 
    insert_forms_task >> list_metadata >> insert_form_metadata_task >> list_submissions >> insert_submissions_task


dag = Fillout()
