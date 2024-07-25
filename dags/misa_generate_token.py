import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
import requests
import json
# Variables
MISA_API_GET_TOKEN = Variable.get("misa_api_get_token")
MISA_APP_ID = Variable.get("misa_app_id")
MISA_ACCESS_CODE = Variable.get("misa_access_code")

## Local path
TEMP_PATH = Variable.get("temp_path")

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}

@dag(
    default_args=default_args,
    schedule_interval="0 */12 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Misa", "Token"],
    max_active_runs=1,
)
def Misa_Generate_Token():
    ######################################### API ################################################
    @task
    def call_api_generate_token():
        body = {
            "app_id": f"{MISA_APP_ID}",
            "access_code": f"{MISA_ACCESS_CODE}",
            "org_company_code": "actapp"
        }
        response = requests.post(MISA_API_GET_TOKEN, json=body, timeout=None)
        if response.status_code == 200:
            data = json.loads(response.json().get("Data"))
            return data.get('access_token')
        else:
            print("Error please check api with status ", response.status_code)
    ######################################### INSERT DATA ################################################
    @task
    def update_airflow_variable(data):
        Variable.set("misa_token", data)


    ############ DAG FLOW ############
    data = call_api_generate_token()
    update_airflow_variable(data)

dag = Misa_Generate_Token()
