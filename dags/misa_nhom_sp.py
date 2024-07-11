import os
import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
import glob
from common.utils import download_file_drive
# Variables

## Local path
TEMP_PATH = Variable.get("temp_path")

# Khong co TKNNo, TKCo, TKChietKhau, TKGiaVon, TKKho
## Conection
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
    tags=["Misa", "group product"],
    max_active_runs=1,
)
def Misa_group_products():
    @task
    def remove_files():
        folder = os.path.join(TEMP_PATH , 'nhomsanpham')
        files = glob.glob(os.path.join(folder, '*'))
        for i in files:
            try:
                os.remove(i)
                print(f"Deleted file: {i}")
            except Exception as e:
                print(f"Error deleting file {i} : {e}")
    ######################################### API ################################################
    @task
    def download_latest_file() -> str:
        folder_name = 'nhomsanpham'
        folder_id = Variable.get("folder_id_sanpham_nhom")
        return download_file_drive(folder_name=folder_name, folder_id=folder_id)

    ######################################### INSERT DATA ################################################
    @task
    def insert_group_products(local_file: str) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        df = pd.read_excel(local_file, index_col=None, engine='openpyxl')
        print(df.columns)
        sql_del = f"delete from [dbo].[3rd_misa_group_products] where [ma_hang] in {tuple([str(i) for i in df['MÃ HÀNG HOÁ'].tolist()])};"
        print(sql_del)
        cursor.execute(sql_del)
        values = []
        if len(df) > 0:
            sql = """
                    INSERT INTO [dbo].[3rd_misa_group_products](
                        [ma_hang]
                        ,[ten_hang]
                        ,[nhan_hang]
                        ,[nhom_nhan_hang]
                        ,[dtm_creation_date])
                    VALUES(%s, %s, %s, %s, getdate())
                """
            for _index, row in df.iterrows():
                value = (
                            str(row[0]),
                            str(row[1]),
                            str(row[2]),
                            str(row[3]),
                )
                values.append(value)
            cursor.executemany(sql, values)

        print(f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()

    ############ DAG FLOW ############

    local_file = download_latest_file()
    insert_task = insert_group_products(local_file)
    remove_files() >> local_file >> insert_task
    # remove_files() >> insert_task


dag = Misa_group_products()
