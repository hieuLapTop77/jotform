import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
import requests
import json
# Variables
MISA_API_DANHMUC = Variable.get("misa_api_danhmuc")
TOKEN = Variable.get("misa_token")
MISA_APP_ID = Variable.get("misa_app_id")

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
    schedule_interval="5 */12 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Misa", "Vat Tu", "hang hoa"],
    max_active_runs=1,
)
def Misa_API_VatTu():
    ######################################### API ################################################
    @task
    def get_data_misa():
        body = {
            "data_type": 2,
            "branch_id": None,
            "skip": 0,
            "take": 1000,
            "app_id": f"{MISA_APP_ID}",
            "last_sync_time": None
        }
        headers = {
            "Content-Type": "application/json",
            "X-MISA-AccessToken": f"{TOKEN}"
        }
        while True:
            response = requests.post(MISA_API_DANHMUC, headers=headers, json=body, timeout=None)
            if response.status_code == 200:
                data = json.loads(response.json().get("Data"))
                if not data:
                    break
                insert_vattu(data)
                body["skip"] += 1000
            else:
                print("Error please check api")
                break

    ######################################### INSERT DATA ################################################
    def insert_vattu(data: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        values = []
        sql = """
                    INSERT INTO [dbo].[3rd_misa_api_vattu_hanghoa](
                        [dictionary_type]
                        ,[inventory_item_id]
                        ,[inventory_item_name]
                        ,[inventory_item_code]
                        ,[inventory_item_type]
                        ,[unit_id]
                        ,[minimum_stock]
                        ,[inventory_item_category_code_list]
                        ,[inventory_item_category_name_list]
                        ,[inventory_item_category_id_list]
                        ,[inventory_item_category_misa_code_list]
                        ,[branch_id]
                        ,[default_stock_id]
                        ,[discount_type]
                        ,[cost_method]
                        ,[inventory_item_cost_method]
                        ,[base_on_formula]
                        ,[is_unit_price_after_tax]
                        ,[is_system]
                        ,[inactive]
                        ,[is_follow_serial_number]
                        ,[is_allow_duplicate_serial_number]
                        ,[purchase_discount_rate]
                        ,[unit_price]
                        ,[sale_price1]
                        ,[sale_price2]
                        ,[sale_price3]
                        ,[fixed_sale_price]
                        ,[import_tax_rate]
                        ,[export_tax_rate]
                        ,[fixed_unit_price]
                        ,[description]
                        ,[specificity]
                        ,[purchase_description]
                        ,[sale_description]
                        ,[inventory_account]
                        ,[cogs_account]
                        ,[sale_account]
                        ,[unit_list]
                        ,[unit_name]
                        ,[is_temp_from_sync]
                        ,[reftype]
                        ,[reftype_category]
                        ,[purchase_fixed_unit_price_list]
                        ,[purchase_last_unit_price_list]
                        ,[quantityBarCode]
                        ,[allocation_type]
                        ,[allocation_time]
                        ,[allocation_account]
                        ,[tax_reduction_type]
                        ,[purchase_last_unit_price]
                        ,[is_specific_inventory_item]
                        ,[has_delete_fixed_unit_price]
                        ,[has_delete_unit_price]
                        ,[has_delete_discount]
                        ,[has_delete_unit_convert]
                        ,[has_delete_norm]
                        ,[has_delete_serial_type]
                        ,[is_edit_multiple]
                        ,[is_not_sync_crm]
                        ,[isUpdateRebundant]
                        ,[is_special_inv]
                        ,[isCustomPrimaryKey]
                        ,[isFromProcessBalance]
                        ,[is_drug]
                        ,[status_sync_medicine_national]
                        ,[is_sync_corp]
                        ,[convert_rate]
                        ,[is_update_main_unit]
                        ,[is_image_duplicate]
                        ,[is_group]
                        ,[discount_value]
                        ,[is_set_discount]
                        ,[excel_row_index]
                        ,[is_valid]
                        ,[created_date]
                        ,[created_by]
                        ,[modified_date]
                        ,[modified_by]
                        ,[auto_refno]
                        ,[pass_edit_version]
                        ,[state]
                        ,[discount_account]
                        ,[sale_off_account]
                        ,[return_account]
                        ,[tax_rate]
                        ,[guaranty_period]
                        ,[inventory_item_source]
                        ,[dtm_creation_date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    , getdate())
                """
        sql_select = 'select distinct inventory_item_id from [3rd_misa_api_vattu_hanghoa]'
        df_sql = pd.read_sql(sql_select, sql_conn)
        df = pd.DataFrame(data)
        df = df[~df['inventory_item_id'].isin(df_sql['inventory_item_id'])]
        # df = pd.DataFrame(data)
        if not df.empty:
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
                    str(row[40]),
                    str(row[41]),
                    str(row[42]),
                    str(row[43]),
                    str(row[44]),
                    str(row[45]),
                    str(row[46]),
                    str(row[47]),
                    str(row[48]),
                    str(row[49]),
                    str(row[50]),
                    str(row[51]),
                    str(row[52]),
                    str(row[53]),
                    str(row[54]),
                    str(row[55]),
                    str(row[56]),
                    str(row[57]),
                    str(row[58]),
                    str(row[59]),
                    str(row[60]),
                    str(row[61]),
                    str(row[62]),
                    str(row[63]),
                    str(row[64]),
                    str(row[65]),
                    str(row[66]),
                    str(row[67]),
                    str(row[68]),
                    str(row[69]),
                    str(row[70]),
                    str(row[71]),
                    str(row[72]),
                    str(row[73]),
                    str(row[74]),
                    str(row[75]),
                    str(row[76]),
                    str(row[77]),
                    str(row[78]),
                    str(row[79]),
                    str(row[80]),
                    str(row[81]),
                    str(row[82]),
                    str(row[83]),
                    str(row[84]),
                    str(row[85]),
                    str(row[86]),
                    str(row[87]),
                )
                values.append(value)
            cursor.executemany(sql, values)

        print(f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()


    ############ DAG FLOW ############
    get_data_misa()



dag = Misa_API_VatTu()
