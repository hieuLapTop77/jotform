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
    tags=["Misa", "khach hang", "customer"],
    max_active_runs=1,
)
def Misa_API_Khachhang():
    ######################################### API ################################################
    @task
    def call_api_get_khach_hang():
        headers = {
            "Content-Type": "application/json",
            "X-MISA-AccessToken": f"{TOKEN}"
        }
        body = {
            "data_type": 1,
            "branch_id": None,
            "skip": 0,
            "take": 1000,
            "app_id": f"{MISA_APP_ID}",
            "last_sync_time": None
        }
        while True:
            response = requests.post(MISA_API_DANHMUC, headers=headers, json=body, timeout=None)
            if response.status_code == 200:
                data = json.loads(response.json().get("Data"))
                if not data:
                    break
                insert_khach_hang(data)
                body["skip"] += 1000
            else:
                print("Error please check api")
                break
    ######################################### INSERT DATA ################################################
    def insert_khach_hang(data: list) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        values = []
        sql = """
            INSERT INTO [dbo].[3rd_misa_api_account_objects](
                [dictionary_type]
                ,[account_object_id]
                ,[account_object_type]
                ,[is_vendor]
                ,[is_local_object]
                ,[is_customer]
                ,[is_employee]
                ,[inactive]
                ,[maximize_debt_amount]
                ,[receiptable_debt_amount]
                ,[account_object_code]
                ,[account_object_name]
                ,[address]
                ,[district]
                ,[ward_or_commune]
                ,[country]
                ,[province_or_city]
                ,[company_tax_code]
                ,[is_same_address]
                ,[closing_amount]
                ,[reftype]
                ,[reftype_category]
                ,[branch_id]
                ,[is_convert]
                ,[is_group]
                ,[is_sync_corp]
                ,[is_remind_debt]
                ,[isUpdateRebundant]
                ,[list_object_type]
                ,[database_id]
                ,[isCustomPrimaryKey]
                ,[excel_row_index]
                ,[is_valid]
                ,[created_date]
                ,[created_by]
                ,[modified_date]
                ,[modified_by]
                ,[auto_refno]
                ,[pass_edit_version]
                ,[state]
                ,[gender]
                ,[due_time]
                ,[agreement_salary]
                ,[salary_coefficient]
                ,[insurance_salary]
                ,[employee_contract_type]
                ,[contact_address]
                ,[contact_title]
                ,[email_address]
                ,[fax]
                ,[employee_id]
                ,[number_of_dependent]
                ,[account_object_group_id_list]
                ,[account_object_group_code_list]
                ,[account_object_group_name_list]
                ,[account_object_group_misa_code_list]
                ,[employee_tax_code]
                ,[einvoice_contact_name]
                ,[einvoice_contact_mobile]
                ,[contact_name]
                ,[contact_mobile]
                ,[tel]
                ,[description]
                ,[website]
                ,[mobile]
                ,[contact_fixed_tel]
                ,[bank_branch_name]
                ,[other_contact_mobile]
                ,[pay_account]
                ,[shipping_address]
                ,[receive_account]
                ,[organization_unit_name]
                ,[bank_account]
                ,[bank_name]
                ,[prefix]
                ,[account_object_shipping_address]
                ,[bank_province_or_city]
                ,[contact_email]
                ,[legal_representative]
                ,[organization_unit_id]
                ,[account_object_bank_account]
                ,[payment_term_id]
                ,[einvoice_contact_email]
                ,[issue_date]
                ,[issue_by]
                ,[identification_number]
                ,[amis_platform_id]
                ,[birth_date]
                ,[crm_id]
                ,[crm_group_id]
                ,[dtm_creation_date])
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
        """
        sql_select = 'select distinct account_object_id from [3rd_misa_api_account_objects]'
        df_sql = pd.read_sql(sql_select, sql_conn)
        df = pd.DataFrame(data)
        df = df[~df['account_object_id'].isin(df_sql['account_object_id'])]
        list_col = ['contact_fixed_tel', 'bank_branch_name', 'other_contact_mobile', 'pay_account', 
                    'shipping_address', 'receive_account', 'organization_unit_name', 'bank_account', 'bank_name', 'prefix', 
                    'account_object_shipping_address', 'bank_province_or_city', 'contact_email', 'legal_representative', 
                    'organization_unit_id', 'account_object_bank_account', 'payment_term_id', 'einvoice_contact_email', 'issue_date', 
                    'issue_by', 'identification_number', 'amis_platform_id', 'birth_date', 'crm_id', 'crm_group_id']
        
        for i in list_col:
            if i not in df.columns.tolist():
                df[f'{i}'] = None

        col = ['dictionary_type', 'account_object_id', 'account_object_type', 'is_vendor', 
               'is_local_object', 'is_customer', 'is_employee', 'inactive', 'maximize_debt_amount', 
               'receiptable_debt_amount', 'account_object_code', 'account_object_name', 'address', 'district', 
               'ward_or_commune', 'country', 'province_or_city', 'company_tax_code', 'is_same_address', 'closing_amount', 
               'reftype', 'reftype_category', 'branch_id', 'is_convert', 'is_group', 'is_sync_corp', 'is_remind_debt', 
               'isUpdateRebundant', 'list_object_type', 'database_id', 'isCustomPrimaryKey', 'excel_row_index', 'is_valid', 
               'created_date', 'created_by', 'modified_date', 'modified_by', 'auto_refno', 'pass_edit_version', 'state', 'gender', 
               'due_time', 'agreement_salary', 'salary_coefficient', 'insurance_salary', 'employee_contract_type', 'contact_address', 
               'contact_title', 'email_address', 'fax', 'employee_id', 'number_of_dependent', 'account_object_group_id_list', 
               'account_object_group_code_list', 'account_object_group_name_list', 'account_object_group_misa_code_list', 
               'employee_tax_code', 'einvoice_contact_name', 'einvoice_contact_mobile', 'contact_name', 'contact_mobile', 
               'tel', 'description', 'website', 'mobile', 'contact_fixed_tel', 'bank_branch_name', 'other_contact_mobile', 
               'pay_account', 'shipping_address', 'receive_account', 'organization_unit_name', 'bank_account', 'bank_name', 
               'prefix', 'account_object_shipping_address', 'bank_province_or_city', 'contact_email', 'legal_representative', 
               'organization_unit_id', 'account_object_bank_account', 'payment_term_id', 'einvoice_contact_email', 'issue_date', 
               'issue_by', 'identification_number', 'amis_platform_id', 'birth_date', 'crm_id', 'crm_group_id']
        df = df[col]
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
                    str(row[88]),
                    str(row[89]),
                )
                values.append(value)
            cursor.executemany(sql, values)

        print(f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()


    ############ DAG FLOW ############
    call_api_get_khach_hang()



dag = Misa_API_Khachhang()
