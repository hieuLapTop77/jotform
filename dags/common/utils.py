import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List
from datetime import datetime, timedelta
import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import billiard as multiprocessing
import pandas as pd
import requests
from airflow.models import Variable

API_KEY = Variable.get("api_key")
URL_DRIVER_REQUESTS = Variable.get("url_driver_requests")
URL_DRIVER_DOWNLOAD = Variable.get("url_driver_download")

# Local path
TEMP_PATH = Variable.get("temp_path")


def download_file_lastest_drive(folder_name: str, folder_id: str) -> str:
    url = URL_DRIVER_REQUESTS.format(folder_id, API_KEY)
    response = requests.get(url)

    response.raise_for_status()

    files = response.json().get('files', [])

    if not files:
        print('No files found.')
        return
    latest_file = files[0]
    file_id = latest_file['id']
    file_name = latest_file['name']

    download_url = URL_DRIVER_DOWNLOAD.format(file_id, API_KEY)
    download_response = requests.get(download_url)
    download_response.raise_for_status()
    file_local = os.path.join(TEMP_PATH, folder_name, file_name)
    os.makedirs(os.path.dirname(file_local), exist_ok=True)
    with open(file_local, 'wb') as f:
        f.write(download_response.content)
    print(f"Downloaded {file_name}")
    return file_local

def download_file_drive(folder_name: str, folder_id: str) -> list:
    url = URL_DRIVER_REQUESTS.format(folder_id, API_KEY)
    response = requests.get(url)

    response.raise_for_status()

    files = response.json().get('files', [])

    if not files:
        print('No files found.')
        return []
    time_limit = datetime.utcnow() - timedelta(hours=6)
    recent_files = [f for f in files if 'modifiedTime' in f and datetime.strptime(f['modifiedTime'], "%Y-%m-%dT%H:%M:%S.%fZ") > time_limit]

    if not recent_files:
        print('No files updated in the last 6 hours.')
        return []

    downloaded_files = []

    for file in recent_files:
        file_id = file['id']
        file_name = file['name']

        download_url = URL_DRIVER_DOWNLOAD.format(file_id, API_KEY)
        download_response = requests.get(download_url)
        download_response.raise_for_status()
        
        file_local = os.path.join(TEMP_PATH, folder_name, file_name)
        os.makedirs(os.path.dirname(file_local), exist_ok=True)
        
        with open(file_local, 'wb') as f:
            f.write(download_response.content)
        
        print(f"Downloaded {file_name}")
        downloaded_files.append(file_local)

    return downloaded_files


def call_api_get_list(sql: str, hook_sql: str, url: str, headers=None, params=None) -> list:
    HOOK_MSSQL = hook_sql
    list_results = []
    hook = mssql.MsSqlHook(HOOK_MSSQL)
    sql_conn = hook.get_conn()
    df = pd.read_sql(sql, sql_conn)
    sql_conn.close()
    if len(df["id"]) > 0:
        for i in df["id"]:
            response = requests.get(
                url.format(i),
                headers=headers,
                params=params,
                timeout=None,
            )
            if response.status_code == 200:
                list_results.append(response.json())
            else:
                print("Error please check at api: ", url.format(i))
    return list_results


def call_api_mutiple_pages(headers, params, name_url: str, url: str, task_id):
    list_results = []
    task_ids = task_id
    print(f"Calling api {url.format(task_ids)} at page: ", params["page"])
    while True:
        print(
            f"Calling api {name_url} for task_id {task_ids} at page: ", params["page"])
        print(f"Calling api {url.format(task_ids)} at page: ", params["page"])
        response = requests.get(
            url.format(task_ids), headers=headers, params=params, timeout=None
        )
        if response.status_code == 200:
            data = response.json()
            list_results.append(data)
            if data.get("last_page", True):
                break
            params["page"] += 1
        else:
            print("Error please check api: ", url.format(task_ids))
            break

    return list_results


def call_multiple_thread(hook_sql: str, sql: str, function: Callable[[int], any], function_name: str, range_from: int = -1, range_to: int = -1) -> List[any]:
    list_results = []
    hook = mssql.MsSqlHook(hook_sql)
    sql_conn = hook.get_conn()
    df = pd.read_sql(sql, sql_conn)
    sql_conn.close()

    if not df.empty and "id" in df.columns:
        num_cpus = multiprocessing.cpu_count()
        max_workers = num_cpus * 5

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            if range_from != -1 and range_to != -1:
                futures = [executor.submit(function, space_id)
                           for space_id in df["id"][range_from:range_to]]
            else:
                futures = [executor.submit(function, space_id)
                           for space_id in df["id"]]

            for future in as_completed(futures):
                try:
                    list_results.append(future.result())
                except Exception as e:
                    print(f"Error at function {function_name}: with error {e}")

    return list_results


def handle_df(df: str):
    data = json.loads(df)
    df = pd.DataFrame.from_dict(data, orient='columns')
    return df
