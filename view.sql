alter view [dbo].[data_ban_hang] as
select distinct bh.*, p.Ten_SP, s.SoLuongBan, s.DienGiaiChung, s.TongThanhToanNT, s.MaHang, s.MaNhanVienBanHang, s.TenNhanVienBanHang, s.ChietKhau, s.TongSoLuongTraLai, s.GiaTriTraLai, b.Don_vi_phu_trach, b.Ten_kenh_phan_phoi, b.Nhan_vien_kinh_doanh, orderindex, b.Dia_chi
from [dbo].[3rd_misa_ban_hang] bh
left join [dbo].[3rd_misa_sales_details] s on s.SoChungTu = bh.So_chung_tu
left join [dbo].[3rd_misa_products] p on p.ma_sp = s.MaHang
left join [dbo].[3rd_misa_khachhang] b on s.customers_code=b.ma_khach_hang
left join (
    select  JSON_VALUE(value, '$.id') id
        ,JSON_VALUE(value, '$.name') name
        ,JSON_VALUE(value, '$.orderindex') orderindex
    from [dbo].[3rd_clickup_custom_fields]
    cross apply openjson(type_config, '$.options') as value
    where id = '40c4e8d2-7ea2-4d3a-9ecf-6ed491ea19c8'
)c on b.Don_vi_phu_trach = c.name
where So_chung_tu =  'BH2405/321'



CREATE TABLE [dbo].[3rd_misa_ban_hang](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[STT] [varchar](255) NULL,
	[Ngay_hach_toan] [varchar](255) NULL,
	[So_chung_tu] [varchar](255) NULL,
	[Khach_hang] [nvarchar](250) NULL,
	[Dia_chi] [nvarchar](500) NULL,
	[Dien_giai] [nvarchar](max) NULL,
	[Tong_tien_thanh_toan] [varchar](50) NULL,
	[status_clickup] [varchar](250) DEFAULT ('false'),
	[dtm_creation_date] [datetime] NULL
)
CREATE TABLE [3rd_misa_account_objects] (
    dictionary_type                      NVARCHAR(255) ,
    account_object_id                    NVARCHAR(255) ,
    account_object_type                  NVARCHAR(255),
    is_vendor                            NVARCHAR(255) ,
    is_local_object                      NVARCHAR(255) ,
    is_customer                          NVARCHAR(255) ,
    is_employee                          NVARCHAR(255) ,
    inactive                             NVARCHAR(255) ,
    maximize_debt_amount                 NVARCHAR(255) ,
    receiptable_debt_amount              NVARCHAR(255) ,
    account_object_code                  NVARCHAR(255) ,
    account_object_name                  NVARCHAR(255) ,
    address                              NVARCHAR(255),
    legal_representative                 NVARCHAR(255),
    district                             NVARCHAR(255),
    ward_or_commune                      NVARCHAR(255),
    country                              NVARCHAR(255),
    province_or_city                     NVARCHAR(255),
    company_tax_code                     NVARCHAR(255),
    is_same_address                      NVARCHAR(255) ,
    pay_account                          NVARCHAR(255),
    receive_account                      NVARCHAR(255),
    closing_amount                       NVARCHAR(255) ,
    reftype                              NVARCHAR(255) ,
    reftype_category                     NVARCHAR(255) ,
    branch_id                            NVARCHAR(255) ,
    is_convert                           NVARCHAR(255) ,
    is_group                             NVARCHAR(255) ,
    is_sync_corp                         NVARCHAR(255) ,
    is_remind_debt                       NVARCHAR(255) ,
    isUpdateRebundant                    NVARCHAR(255) ,
    list_object_type                     NVARCHAR(255) ,
    database_id                          NVARCHAR(255) ,
    isCustomPrimaryKey                   NVARCHAR(255) ,
    excel_row_index                      NVARCHAR(255) ,
    is_valid                             NVARCHAR(255) ,
    created_date                         NVARCHAR(255) ,
    created_by                           NVARCHAR(255) ,
    modified_date                        NVARCHAR(255) ,
    modified_by                          NVARCHAR(255) ,
    auto_refno                           NVARCHAR(255) ,
    pass_edit_version                    NVARCHAR(255) ,
    state                                NVARCHAR(255) ,
    gender                               NVARCHAR(255),
    due_time                             NVARCHAR(255),
    agreement_salary                     NVARCHAR(255),
    salary_coefficient                   NVARCHAR(255),
    insurance_salary                     NVARCHAR(255),
    employee_contract_type               NVARCHAR(255),
    contact_address                      NVARCHAR(255),
    contact_title                        NVARCHAR(255),
    email_address                        NVARCHAR(255),
    fax                                  NVARCHAR(255),
    bank_account                         NVARCHAR(255),
    bank_name                            NVARCHAR(255),
    account_object_bank_account          NVARCHAR(255),
    employee_id                          NVARCHAR(255),
    number_of_dependent                  NVARCHAR(255),
    account_object_group_id_list         NVARCHAR(255),
    account_object_group_code_list       NVARCHAR(255),
    account_object_group_name_list       NVARCHAR(255),
    account_object_group_misa_code_list  NVARCHAR(255),
    employee_tax_code                    NVARCHAR(255),
    einvoice_contact_name                NVARCHAR(255),
    einvoice_contact_mobile              NVARCHAR(255),
    contact_name                         NVARCHAR(255),
    contact_mobile                       NVARCHAR(255),
    tel                                  NVARCHAR(255),
    payment_term_id                      NVARCHAR(255),
    description                          NVARCHAR(255),
    contact_fixed_tel                    NVARCHAR(255),
    shipping_address                     NVARCHAR(255),
    website                              NVARCHAR(255),
    account_object_shipping_address      NVARCHAR(255),
    einvoice_contact_email               NVARCHAR(255),
    contact_email                        NVARCHAR(255),
    prefix                               NVARCHAR(255),
    mobile                               NVARCHAR(255),
    bank_branch_name                     NVARCHAR(255),
    bank_province_or_city                NVARCHAR(255),
    organization_unit_id                 NVARCHAR(255),
    organization_unit_name               NVARCHAR(255),
    other_contact_mobile                 NVARCHAR(255),
	dtm_creation_date					datetime
)