CREATE TABLE [3rd_jotform_user_forms] (
    id INT PRIMARY KEY IDENTITY(1,1),
    form_id varchar(50),
    username NVARCHAR(50),
    title NVARCHAR(250),
    height varchar(20),
    status NVARCHAR(50),
    created_at varchar(20),
    updated_at varchar(20),
    last_submission varchar(20),
    new varchar(20),
    count varchar(20),
    type NVARCHAR(50),
    favorite varchar(20),
    archived varchar(20),
    url NVARCHAR(500),
    dtm_creation_date datetime
);
