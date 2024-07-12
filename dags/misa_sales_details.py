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

FOLDER_NAME = 'sochitietbanhang'

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="0 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Misa", "sales details"],
    max_active_runs=1,
)
def Misa_sales_details():
    @task
    def remove_files():
        folder = os.path.join(TEMP_PATH, FOLDER_NAME)
        files = glob.glob(os.path.join(folder, '*'))
        for i in files:
            try:
                os.remove(i)
                print(f"Deleted file: {i}")
            except Exception as e:
                print(f"Error deleting file {i} : {e}")
    ######################################### API ################################################
    @task
    def download_latest_file() -> list:
        folder_id = Variable.get("folder_id_sochitietbanhang")
        return download_file_drive(folder_name=FOLDER_NAME, folder_id=folder_id)

    ######################################### INSERT DATA ################################################
    @task
    def insert_sales_details(list_file_local: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        sql = """
                    INSERT INTO [dbo].[3rd_misa_sales_details](
                        [NgayHachToan]
                        ,[NgayChungTu]
                        ,[SoChungTu]
                        ,[DienGiaiChung]
                        ,[customers_code]
                        ,[MaHang]
                        ,[DVT]
                        ,[SoLuongBan]
                        ,[SLBanKhuyenMai]
                        ,[TongSoLuongBan]
                        ,[DonGia]
                        ,[DoanhSoBan]
                        ,[ChietKhau]
                        ,[TongSoLuongTraLai]
                        ,[GiaTriTraLai]
                        ,[ThueGTGT]
                        ,[TongThanhToanNT]
                        ,[GiaVon]
                        ,[MaNhanVienBanHang]
                        ,[TenNhanVienBanHang]
                        ,[MaKho]
                        ,[TenKho]
                        ,[dtm_create_time])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
                """
        if len(list_file_local) > 0:
            for file_local in list_file_local:
                df = pd.read_excel(file_local, skiprows=3, index_col=None, engine='openpyxl', skipfooter=1)
                sql_del = f"delete from [dbo].[3rd_misa_sales_details] where NgayChungTu >= '{df['Ngày chứng từ'].min()}' and NgayChungTu <= '{df['Ngày chứng từ'].max()}';"
                print(sql_del)
                cursor.execute(sql_del)
                values = []
                if len(df) > 0:
                    for _index, row in df.iterrows():
                        value = (
                                    str(row[0]),
                                    str(row[1]),
                                    str(row[2]),
                                    str(row[3]),
                                    str(row[4]),
                                    # str(row[5]), # tên khách
                                    str(row[6]),
                                    # str(row[7]), # tên hàng
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
                        )
                        values.append(value)
                    cursor.executemany(sql, values)

                print(f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
                sql_conn.commit()
        sql_conn.close()


    ############ DAG FLOW ############

    list_local_file = download_latest_file()
    insert_task = insert_sales_details(list_local_file)
    remove_files() >> list_local_file >> insert_task


dag = Misa_sales_details()
