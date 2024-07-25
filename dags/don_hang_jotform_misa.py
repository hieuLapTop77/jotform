import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
import requests
import json

# Variables
MISA_API_POST_ORDER = Variable.get("misa_luu_don_hang")
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
    tags=["Misa", "dat hang", "don hang", "order"],
    max_active_runs=1,
)
def Misa_API_Post_Order():
    ######################################### API ################################################
    body = {
        "app_id": "",
        "org_company_code": "actapp",
        "voucher": [
            {
                "detail": [],
                "voucher_type": None,
                "is_get_new_id": None,
                "org_refid": None,
                "is_allow_group": None,
                "org_reftype": None,
                "org_reftype_name": None,
                "refno_finance": None,
                "refid": None,
                "branch_id": None,
                "account_object_id": None,
                "delivered_status": None,
                "due_day": None,
                "is_calculated_cost": None,
                "exchange_rate": None,
                "total_amount_oc": None,
                "total_amount": None,
                "total_discount_amount_oc": None,
                "total_discount_amount": None,
                "total_vat_amount_oc": None,
                "total_vat_amount": None,
                "account_object_name": None,
                "account_object_code": None,
                "journal_memo": None,
                "currency_id": None,
                "employee_code": None,
                "employee_name": None,
                "discount_type": None,
                "discount_rate_voucher": None,
                "total_sale_amount_oc": None,
                "total_sale_amount": None,
                "organization_unit_id": None,
                "organization_unit_type_id": None,
                "total_invoice_amount": None,
                "total_invoice_amount_oc": None,
                "crm_id": None,
                "isUpdateRevenue": None,
                "check_quantity": None,
                "excel_row_index": None,
                "is_valid": None,
                "reftype": None,
                "auto_refno": None,
                "state": None
            }
        ]
    }
    HEADERS = {
        "Content-Type": "application/json",
        "X-MISA-AccessToken": TOKEN
    }
    @task
    def get_data_misa():
        json_data = get_data_sql(body=body)
        print("json: ", json_data)
        response = requests.post(MISA_API_POST_ORDER, headers=HEADERS, json=body, timeout=None)
        if response.status_code == 200:
            if response.json()['Success']:
                print("Post don hang thanh cong", response.json())
            else:
                print("Post don hang khong thanh cong", response.json())
        else:
            print("Error please check api")
        
    ######################################### INSERT DATA ################################################
    def get_data_sql(body) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        sql_select = "exec [rp_don_dat_hang] 5965663886427730007"
        df = pd.read_sql(sql_select, sql_conn)
        
        df_detail = df.iloc[:, 0:40]
        products = df_detail.to_dict(orient='records')
        body["voucher"][0]["detail"] = products
        body["app_id"] = MISA_APP_ID
        body["voucher"][0]["voucher_type"] = int(df.iloc[0, 40])
        body["voucher"][0]["is_get_new_id"] = df.iloc[0, 41]
        body["voucher"][0]["org_refid"] = df.iloc[0, 42]
        body["voucher"][0]["is_allow_group"] = df.iloc[0, 43]
        body["voucher"][0]["org_reftype"] = int(df.iloc[0, 44])
        body["voucher"][0]["org_reftype_name"] = df.iloc[0, 45]
        body["voucher"][0]["refno_finance"] = df.iloc[0, 46]
        body["voucher"][0]["refid"] = df.iloc[0, 47]
        body["voucher"][0]["branch_id"] = df.iloc[0, 48]
        body["voucher"][0]["account_object_id"] = df.iloc[0, 49]
        body["voucher"][0]["delivered_status"] = int(df.iloc[0, 50])
        body["voucher"][0]["due_day"] = int(df.iloc[0, 51])
        body["voucher"][0]["is_calculated_cost"] = df.iloc[0, 52]
        body["voucher"][0]["exchange_rate"] = float(df.iloc[0, 53])
        body["voucher"][0]["total_amount_oc"] = float(df.iloc[0, 54])
        body["voucher"][0]["total_amount"] = float(df.iloc[0, 55])
        body["voucher"][0]["total_discount_amount_oc"] = float(df.iloc[0, 56])
        body["voucher"][0]["total_discount_amount"] = float(df.iloc[0, 57])
        body["voucher"][0]["total_vat_amount_oc"] = float(df.iloc[0, 58])
        body["voucher"][0]["total_vat_amount"] = float(df.iloc[0, 59])
        body["voucher"][0]["account_object_name"] = df.iloc[0, 60]
        body["voucher"][0]["account_object_code"] = df.iloc[0, 61]
        body["voucher"][0]["journal_memo"] = df.iloc[0, 62] if len(str(df.iloc[0, 62])) > 1 else 'Đơn hàng bán cho ' + str(df.iloc[0, 60])
        body["voucher"][0]["currency_id"] = df.iloc[0, 63]
        body["voucher"][0]["employee_code"] = df.iloc[0, 64]
        body["voucher"][0]["employee_name"] = df.iloc[0, 65]
        body["voucher"][0]["discount_type"] = int(df.iloc[0, 66])
        body["voucher"][0]["discount_rate_voucher"] = float(df.iloc[0, 67])
        body["voucher"][0]["total_sale_amount_oc"] = float(df.iloc[0, 68])
        body["voucher"][0]["total_sale_amount"] = float(df.iloc[0, 69])
        body["voucher"][0]["organization_unit_id"] = df.iloc[0, 70]
        body["voucher"][0]["organization_unit_type_id"] = int(df.iloc[0, 71])
        body["voucher"][0]["total_invoice_amount"] = float(df.iloc[0, 72])
        body["voucher"][0]["total_invoice_amount_oc"] = float(df.iloc[0, 73])
        body["voucher"][0]["crm_id"] = str(df.iloc[0, 74])
        body["voucher"][0]["isUpdateRevenue"] = df.iloc[0, 75]
        body["voucher"][0]["check_quantity"] = df.iloc[0, 76]
        body["voucher"][0]["excel_row_index"] = int(df.iloc[0, 77])
        body["voucher"][0]["is_valid"] = df.iloc[0, 78]
        body["voucher"][0]["reftype"] = int(df.iloc[0, 79])
        body["voucher"][0]["auto_refno"] = bool(df.iloc[0, 80])
        body["voucher"][0]["state"] = df.iloc[0, 81]
        return json.dumps(body, ensure_ascii=False)


    # def get_data_sql(body) -> None:
    #     hook = mssql.MsSqlHook(HOOK_MSSQL)
    #     sql_conn = hook.get_conn()
    #     sql_select = "exec [rp_don_dat_hang] 5965663886427730007"
    #     df = pd.read_sql(sql_select, sql_conn)
        
    #     df_detail = df.iloc[:, 0:40]
    #     products = df_detail.to_dict(orient='records')
    #     body["voucher"][0]["detail"] = products
        
    #     # Gán các giá trị scalar từ DataFrame vào body
    #     body["voucher"][0]["voucher_type"] = df.iloc[0, 41]
    #     body["voucher"][0]["is_get_new_id"] = df.iloc[0, 42]
    #     body["voucher"][0]["org_refid"] = df.iloc[0, 43]
    #     body["voucher"][0]["is_allow_group"] = df.iloc[0, 44]
    #     body["voucher"][0]["org_reftype"] = df.iloc[0, 45]
    #     body["voucher"][0]["org_reftype_name"] = df.iloc[0, 46]
    #     body["voucher"][0]["refno_finance"] = df.iloc[0, 47]
    #     body["voucher"][0]["refid"] = df.iloc[0, 48]
    #     body["voucher"][0]["branch_id"] = df.iloc[0, 49]
    #     body["voucher"][0]["account_object_id"] = df.iloc[0, 50]
    #     body["voucher"][0]["delivered_status"] = df.iloc[0, 51]
    #     body["voucher"][0]["due_day"] = df.iloc[0, 52]
    #     body["voucher"][0]["is_calculated_cost"] = df.iloc[0, 53]
    #     body["voucher"][0]["exchange_rate"] = df.iloc[0, 54]
    #     body["voucher"][0]["total_amount_oc"] = df.iloc[0, 55]
    #     body["voucher"][0]["total_amount"] = df.iloc[0, 56]
    #     body["voucher"][0]["total_discount_amount_oc"] = df.iloc[0, 57]
    #     body["voucher"][0]["total_discount_amount"] = df.iloc[0, 58]
    #     body["voucher"][0]["total_vat_amount_oc"] = df.iloc[0, 59]
    #     body["voucher"][0]["total_vat_amount"] = df.iloc[0, 60]
    #     body["voucher"][0]["account_object_name"] = df.iloc[0, 61]
    #     body["voucher"][0]["account_object_code"] = df.iloc[0, 62]
    #     body["voucher"][0]["journal_memo"] = df.iloc[0, 63]
    #     body["voucher"][0]["currency_id"] = df.iloc[0, 64]
    #     body["voucher"][0]["employee_code"] = df.iloc[0, 65]
    #     body["voucher"][0]["employee_name"] = df.iloc[0, 66]
    #     body["voucher"][0]["discount_type"] = df.iloc[0, 67]
    #     body["voucher"][0]["discount_rate_voucher"] = df.iloc[0, 68]
    #     body["voucher"][0]["total_sale_amount_oc"] = df.iloc[0, 69]
    #     body["voucher"][0]["total_sale_amount"] = df.iloc[0, 70]
    #     body["voucher"][0]["organization_unit_id"] = df.iloc[0, 71]
    #     body["voucher"][0]["organization_unit_type_id"] = df.iloc[0, 72]
    #     body["voucher"][0]["total_invoice_amount"] = df.iloc[0, 73]
    #     body["voucher"][0]["total_invoice_amount_oc"] = df.iloc[0, 74]
    #     body["voucher"][0]["crm_id"] = df.iloc[0, 75]
    #     body["voucher"][0]["isUpdateRevenue"] = df.iloc[0, 76]
    #     body["voucher"][0]["check_quantity"] = df.iloc[0, 77]
    #     body["voucher"][0]["excel_row_index"] = df.iloc[0, 78]
    #     body["voucher"][0]["is_valid"] = df.iloc[0, 79]
    #     body["voucher"][0]["reftype"] = df.iloc[0, 80]
    #     body["voucher"][0]["auto_refno"] = df.iloc[0, 81]
    #     # body["voucher"][0]["state"] = df.iloc[0, 82]
        
    #     results = json.dumps(body, ensure_ascii=False)
    #     print(results)


        


    ############ DAG FLOW ############
    get_data_misa()



dag = Misa_API_Post_Order()
