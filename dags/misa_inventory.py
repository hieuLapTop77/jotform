import glob
import os

import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago

from common.utils import download_file_drive

# Variables

# Local path
TEMP_PATH = Variable.get("temp_path")

# Khong co TKNNo, TKCo, TKChietKhau, TKGiaVon, TKKho
# Conection
HOOK_MSSQL = Variable.get("mssql_connection")


default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="30 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Misa", "inventory", "warehouse"],
    max_active_runs=1,
)
def Misa_inventory():
    @task
    def remove_files():
        files = glob.glob(os.path.join(TEMP_PATH, '*'))
        for i in files:
            try:
                os.remove(i)
                print(f"Deleted file: {i}")
            except Exception as e:
                print(f"Error deleting file {i} : {e}")
    ######################################### API ################################################

    @task
    def download_latest_file() -> str:
        folder_name = 'inventory'
        folder_id = Variable.get("folder_id_tonkho")
        return download_file_drive(folder_name=folder_name, folder_id=folder_id)

    ######################################### INSERT DATA ################################################
    @task
    def insert_inventory(file_local: str) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        # sql_del = "delete from [dbo].[3rd_misa_sales_details_v1] where ;"
        # cursor.execute(sql_del)

        df = pd.read_excel(file_local, skiprows=3, index_col=None,
                           engine='openpyxl', skipfooter=1, header=[0, 1])
        sql_del = f"delete from [dbo].[3rd_misa_inventory] where [Ma_hang] in {tuple(df['Mã hàng']['Unnamed: 1_level_1'].tolist())};"
        print(sql_del)
        cursor.execute(sql_del)
        values = []
        if len(df) > 0:

            sql = """
                    INSERT INTO [dbo].[3rd_misa_inventory](
                        [Ten_kho]
                        ,[Ma_hang]
                        ,[Ten_hang]
                        ,[DVT]
                        ,[Dau_ky_so_luong]
                        ,[Dau_ky_gia_tri]
                        ,[Nhap_kho_so_luong]
                        ,[Nhap_kho_gia_tri]
                        ,[Xuat_kho_so_luong]
                        ,[Xuat_kho_gia_tri]
                        ,[Cuoi_ky_so_luong]
                        ,[Cuoi_ky_gia_tri]
                        ,[dtm_creation_date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                           %s, %s, getdate())
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
                )
                values.append(value)
            cursor.executemany(sql, values)

        print(
            f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()

    ############ DAG FLOW ############

    local_file = download_latest_file()
    insert_task = insert_inventory(local_file)
    remove_files() >> local_file >> insert_task


dag = Misa_inventory()