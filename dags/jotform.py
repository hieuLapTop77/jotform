import json
from datetime import datetime, timedelta
from typing import Union

import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago

from common.utils import handle_df

# Variables
# Jotform
# JOTFORM_GET_USER = Variable.get("jotform_get_user")
# JOTFORM_GET_USER_USAGE = Variable.get("jotform_get_user_usage")
# JOTFORM_GET_USER_FOLDERS = Variable.get("jotform_get_user_folders")
# JOTFORM_GET_USER_REPORTS = Variable.get("jotform_get_user_reports")

JOTFORM_GET_USER_FORMS = Variable.get("jotform_get_user_forms")
JOTFORM_GET_USER_SUBMISSIONS = Variable.get("jotform_get_user_submissions")
JOTFORM_GET_FORM_SUBMISSIONS_BY_ID = Variable.get("jotform_get_form_submissions_by_id")
JOTFORM_GET_FORM_BY_ID_QUESTIONS = Variable.get(
    "jotform_get_form_by_id_questions")

ID_JOTFORM_SUBMISSION = Variable.get("id_jotform_submission")

# JOTFORM_GET_FORM_BY_ID = Variable.get("jotform_get_form_by_id")
# JOTFORM_GET_FORM_BY_ID_QUESTIONS_BY_QID = Variable.get("jotform_get_form_by_id_questions_by_qid")
# JOTFORM_GET_FORM_PROPERTIES_BY_ID = Variable.get("jotform_get_form_properties_by_id")
# JOTFORM_GET_FORM_PROPERTIES_BY_ID_BY_KEY = Variable.get("jotform_get_form_properties_by_id_by_key")
# JOTFORM_GET_FORM_REPORTS_BY_ID = Variable.get("jotform_get_form_reports_by_id")
# JOTFORM_GET_FORM_FILES_BY_ID = Variable.get("jotform_get_form_files_by_id")
# JOTFORM_GET_FORM_WEBHOOKS_BY_ID = Variable.get("jotform_get_form_webhooks_by_id")
# JOTFORM_GET_FORM_SUBMISSIONS_BY_ID = Variable.get("jotform_get_form_submissions_by_id")
# JOTFORM_GET_FORM_SUBMISSIONS_BY_SUBID = Variable.get("jotform_get_form_submissions_by_subid")
# JOTFORM_GET_REPORT_BY_ID = Variable.get("jotform_get_report_by_id")
# JOTFORM_GET_FOLDER_BY_ID = Variable.get("jotform_get_folder_by_id")

JOTFORM_API_KEY = Variable.get("api_key_jotform")

# Local path
TEMP_PATH = Variable.get("temp_path")

# Token
BEARER_TOKEN = Variable.get("bearer_token")

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
    schedule_interval="*/2 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Jotform"],
    max_active_runs=1,
)
def Jotform():

    @task
    def call_api_get_user_forms():
        params = {
            "apiKey": JOTFORM_API_KEY,
            "limit": 1000
        }
        forms = None
        response = requests.get(JOTFORM_GET_USER_FORMS,
                                params=params, timeout=None)
        print(response.status_code)
        if response.status_code == 200:
            forms = response.json()
        else:
            print("Error please check api")
        return forms

    @task
    def call_api_get_user_submissions() -> Union[str, None]:
        params = {
            "apiKey": JOTFORM_API_KEY,
            "orderby": "created_at",
            "limit": 1000
        }
        response = requests.get(
            JOTFORM_GET_FORM_SUBMISSIONS_BY_ID.format(ID_JOTFORM_SUBMISSION), params=params, timeout=None
        )
        if response.status_code == 200:
            df = pd.DataFrame(response.json()['content'])
            df['created_at'] = pd.to_datetime(df['created_at'])
            yesterday = pd.to_datetime(
                datetime.today().strftime('%Y-%m-%d')) - timedelta(days=1)
            return df[df['created_at'].dt.date >= yesterday.date()].to_json()
        else:
            print("Error please check api")
            return None

    @task
    def insert_forms(forms) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        sql_del = "delete from [dbo].[3rd_jotform_user_forms];"
        cursor.execute(sql_del)
        if forms.get('content') is not None and (isinstance(forms.get('content'), list)):
            df = pd.DataFrame(forms.get('content'))
            sql = """
                    INSERT INTO [dbo].[3rd_jotform_user_forms](
                        [form_id]
                        ,[username]
                        ,[title]
                        ,[height]
                        ,[status]
                        ,[created_at]
                        ,[updated_at]
                        ,[last_submission]
                        ,[new]
                        ,[count]
                        ,[type]
                        ,[favorite]
                        ,[archived]
                        ,[url]
                        ,[dtm_Creation_Date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
                """
            values = []
            for _index, row in df.iterrows():
                value = (str(row[0]),
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
                         str(row[13]))
                values.append(value)
            cursor.executemany(sql, values)
            print(
                f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
            sql_conn.commit()
            sql_conn.close()
        else:
            print("Response not correct format: ", forms)

    @task
    def insert_form_submissions(df: str) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        df = handle_df(df)

        sql = """select distinct id from [dbo].[3rd_jotform_form_submissions]"""
        df_sql = pd.read_sql(sql, sql_conn)
        df = df[~df['id'].isin(df_sql['id'])]
        values = []
        if not df.empty:
            # print(tuple(df['id'].tolist()))
            # sql_del = f"delete from [dbo].[3rd_jotform_form_submissions] where id in {tuple(df['id'].tolist())};"
            # if ',)' in sql_del:
            #     sql_del = sql_del.replace(',)', ')')
            # print(sql_del)
            # cursor.execute(sql_del)
            # sql_conn.commit()

            df["answers"] = df["answers"].apply(lambda x: json.dumps(x))
            rows = []
            answers = []
            for i in range(len(df)):
                base_row = {
                    'id': df["id"][i],
                    'form_id': df["form_id"][i],
                    'ip': df["ip"][i],
                    'created_at': df["created_at"][i],
                    'status': df["status"][i],
                    'new': df["new"][i],
                    'flag': df["flag"][i],
                    'notes': df["notes"][i],
                    'answer': df["answers"][i],
                    'updated_at': df["updated_at"][i]
                }
                answers = json.loads(df["answers"][i])
                for _key, value in answers.items():
                    text = value.get("text", '')
                    answer = value.get("answer", '')
                    if text != 'Order Form' and text != 'Gửi Order' and text != 'Thêm' and text != '':
                        base_row[text] = answer
                rows.append(base_row)
            df_ = pd.DataFrame(rows)
            print(df_.columns.tolist())
            col = ['id', 'form_id', 'ip', 'created_at', 'status',
                   'new', 'flag', 'notes', 'answer', 'updated_at',
                   'Loại đơn hàng', 'Tên sản phẩm 1', 'Tên sản phẩm 2',
                   'Tên sản phẩm 3', 'Tên sản phẩm 4', 'Tên sản phẩm 5',
                   'Tên sản phẩm 6', 'Tên sản phẩm 7', 'Tên sản phẩm 8',
                   'Tên sản phẩm 9', 'Tên sản phẩm 10', 'Số lượng sản phẩm 1',
                   'Số lượng sản phẩm 2', 'Số lượng sản phẩm 3', 'Số lượng sản phẩm 4',
                   'Số lượng sản phẩm 5', 'Số lượng sản phẩm 6', 'Số lượng sản phẩm 7',
                   'Số lượng sản phẩm 8', 'Số lượng sản phẩm 9', 'Số lượng sản phẩm 10',
                   'Loại khách hàng', 'Địa chỉ', 'Tên cửa hàng, doanh nghiệp', 'Tên người liên lạc - khách hàng', 'Số điện thoại', 'Ghi chú', 'Tên khách hàng']
            df_ = df_[col]
            # df_.to_csv(TEMP_PATH + 'test.csv')
            sql = """
                    INSERT INTO [dbo].[3rd_jotform_form_submissions](
                        [id]
                        ,[form_id]
                        ,[ip]
                        ,[created_at]
                        ,[status]
                        ,[new]
                        ,[flag]
                        ,[notes]
                        ,[answer]
                        ,[updated_at]
                        ,[Loai_don_hang]
                        ,[Ten_SP_1]
                        ,[Ten_SP_2]
                        ,[Ten_SP_3]
                        ,[Ten_SP_4]
                        ,[Ten_SP_5]
                        ,[Ten_SP_6]
                        ,[Ten_SP_7]
                        ,[Ten_SP_8]
                        ,[Ten_SP_9]
                        ,[Ten_SP_10]
                        ,[SL_1]
                        ,[SL_2]
                        ,[SL_3]
                        ,[SL_4]
                        ,[SL_5]
                        ,[SL_6]
                        ,[SL_7]
                        ,[SL_8]
                        ,[SL_9]
                        ,[SL_10]
                        ,[loai_khach_hang]
                        ,[address]
                        ,[Ten_cua_hang]
                        ,[Ten_nguoi_lien_lac]
                        ,[phone]
                        ,[ghi_chu]
                        ,[customer_name]
                        ,[dtm_Creation_Date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,getdate())
                """
            for _index, row in df_.iterrows():
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
            # print(values[0])
            cursor.executemany(sql, values)
        print(
            f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()

    ############ DAG FLOW ############
    forms = call_api_get_user_forms()
    insert_forms_task = insert_forms(forms)
    submissions = call_api_get_user_submissions()
    insert_form_metadata_task = insert_form_submissions(submissions)
    insert_forms_task >> submissions >> insert_form_metadata_task


dag = Jotform()
