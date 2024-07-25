import os
import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
import glob
from common.utils import download_file_drive
import chardet
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
    schedule_interval="0 */4 * * *",
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

    def find_header_row(file_path):
        for i in range(10):
            df = pd.read_excel(file_path, header=i, engine="openpyxl")
            if 'Số chứng từ' in df.columns:
                return i
        return 0
    ######################################### INSERT DATA ################################################
    @task
    def insert_sales_details(list_file_local: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        sql = """
                INSERT INTO [dbo].[3rd_misa_sales_details](
                    [Ngay_hach_toan]
                    ,[Ngay_chung_tu]
                    ,[So_chung_tu]
                    ,[Dien_giai_chung]
                    ,[Ma_khach_hang]
                    ,[Ten_khach_hang]
                    ,[Dia_chi]
                    ,[Ma_hang]
                    ,[Ten_hang]
                    ,[DVT]
                    ,[So_luong_ban]
                    ,[SL_ban_khuyen_mai]
                    ,[Tong_so_luong_ban]
                    ,[Don_gia]
                    ,[Doanh_so_ban]
                    ,[Chiet_khau]
                    ,[Tong_so_luong_tra_lai]
                    ,[Gia_tri_tra_lai]
                    ,[Thue_GTGT]
                    ,[Tong_thanh_toan_NT]
                    ,[Ma_nhan_vien_ban_hang]
                    ,[Ten_nhan_vien_ban_hang]
                    ,[Ma_kho]
                    ,[Ten_kho]
                    ,[dtm_create_time])
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
            """
        col = ['Ngày hạch toán',
                'Ngày chứng từ',
                'Số chứng từ',
                'Diễn giải chung',
                'Mã khách hàng',
                'Tên khách hàng',
                'Địa chỉ',
                'Mã hàng',
                'Tên hàng',
                'ĐVT',
                'Số lượng bán',
                'SL bán khuyến mại',
                'Tổng số lượng bán',
                'Đơn giá',
                'Doanh số bán',
                'Chiết khấu',
                'Tổng số lượng trả lại',
                'Giá trị trả lại',
                'Thuế GTGT',
                'Tổng thanh toán NT',
                'Mã nhân viên bán hàng',
                'Tên nhân viên bán hàng',
                'Mã kho',
                'Tên kho']
        if len(list_file_local) > 0:
            for file_local in list_file_local:
                print("Insert data of file name: ", file_local.split("/")[-1])
                try:
                    header_row = find_header_row(file_local)
                except Exception as e:
                    print("Error at: ", e)
                    header_row = 3
                print("header_row: ", header_row)
                if header_row is not None:
                    df = pd.read_excel(file_local, skiprows=header_row, index_col=None, engine='openpyxl', skipfooter=1)
                    if len(df) > 1000: 
                        my_list = df['Số chứng từ'].tolist()
                        size = 1000
                        for i in range(0, len(my_list), size):
                            chunk = my_list[i:i + size]
                            sql_del = f"delete from [dbo].[3rd_misa_sales_details] where So_chung_tu in {tuple(chunk)};"
                            print(sql_del)
                            cursor.execute(sql_del)
                    values = []
                    df = df[col]
                    if len(df) > 0:
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
