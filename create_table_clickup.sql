CREATE TABLE [3rd_clickup_list_spaces] (
    id VARCHAR(50) ,
    name NVARCHAR(255),
    color VARCHAR(50),
    private VARCHAR(10),
    avatar VARCHAR(max),
    admin_can_manage VARCHAR(255),
    statuses NVARCHAR(max),
    multiple_assignees NVARCHAR(10),
    features NVARCHAR(max),
    archived VARCHAR(10),
    dtm_Creation_Date DATETIME
);

create table [dbo].[3rd_clickup_folders]
(
    id VARCHAR(50), 
    name NVARCHAR(255),
    orderindex VARCHAR(50),
    override_statuses NVARCHAR(max),
    hidden VARCHAR(50),
    space NVARCHAR(max),
    task_count VARCHAR(10),
    archived VARCHAR(200),
    statuses NVARCHAR(max),
    lists NVARCHAR(max),
    permission_level VARCHAR(50)
    ,[dtm_Creation_Date] DATETIME
)
create table [dbo].[3rd_clickup_folder_details]
(
    id VARCHAR(50), 
    name NVARCHAR(255),
    orderindex VARCHAR(50),
    override_statuses NVARCHAR(max),
    hidden VARCHAR(50),
    space NVARCHAR(max),
    task_count VARCHAR(10),
    archived VARCHAR(200),
    statuses NVARCHAR(max),
    lists NVARCHAR(max),
    permission_level VARCHAR(50)
    ,[dtm_Creation_Date] DATETIME
)
create table [dbo].[3rd_clickup_lists]
(
    id VARCHAR(50), 
    name NVARCHAR(255),
    orderindex VARCHAR(50),
    content NVARCHAR(250),
    status NVARCHAR(max),
    priority VARCHAR(50),
    assignee NVARCHAR(max),
    task_count VARCHAR(10),
    due_date VARCHAR(200),
    start_date VARCHAR(200),
    folder NVARCHAR(max),
    space NVARCHAR(max),
    archived VARCHAR(200),
    override_statuses NVARCHAR(max),
    permission_level VARCHAR(50)
    ,[dtm_Creation_Date] DATETIME
)
create table [dbo].[3rd_clickup_list_details]
(
    id VARCHAR(50), 
    name NVARCHAR(255),
    deleted VARCHAR(50),
    orderindex VARCHAR(50),
    content NVARCHAR(max),
    status NVARCHAR(max),
    priority VARCHAR(50),
    assignee NVARCHAR(max),
    due_date VARCHAR(200),
    start_date VARCHAR(200),
    folder NVARCHAR(max),
    space NVARCHAR(max),
    inbound_address NVARCHAR(max),
    archived NVARCHAR(max),
    override_statuses NVARCHAR(max),
    statuses NVARCHAR(max),
    permission_level VARCHAR(50)
    ,[dtm_Creation_Date] DATETIME
)
CREATE TABLE [3rd_clickup_space_details] (
    id VARCHAR(50),
    name NVARCHAR(255),
    orderindex VARCHAR(50),
    content NVARCHAR(max),
    status NVARCHAR(max),
    priority NVARCHAR(50),
    assignee NVARCHAR(50),
    task_count VARCHAR(50),
    due_date VARCHAR(50),
    start_date VARCHAR(50),
    folder NVARCHAR(MAX),
    space NVARCHAR(MAX),
    archived VARCHAR(10),
    override_statuses VARCHAR(10),
    permission_level NVARCHAR(50),
    dtm_Creation_Date DATETIME
);

CREATE TABLE [3rd_clickup_tasks] (
    id NVARCHAR(255),
    custom_id NVARCHAR(255),
    custom_item_id VARCHAR(50),
    name NVARCHAR(255),
    text_content NVARCHAR(MAX),
    description NVARCHAR(MAX),
    status NVARCHAR(MAX),
    orderindex VARCHAR(255),
    date_created VARCHAR(50),
    date_updated VARCHAR(50),
    date_closed VARCHAR(50),
    date_done VARCHAR(50),
    archived VARCHAR(10),
    creator NVARCHAR(max),
    assignees NVARCHAR(MAX),
    group_assignees NVARCHAR(MAX),
    watchers NVARCHAR(MAX),
    checklists NVARCHAR(MAX),
    tags NVARCHAR(MAX),
    parent NVARCHAR(255),
    priority NVARCHAR(255),
    due_date VARCHAR(50),
    start_date VARCHAR(50),
    points NVARCHAR(255),
    time_estimate VARCHAR(50),
    custom_fields NVARCHAR(MAX),
    dependencies NVARCHAR(MAX),
    linked_tasks NVARCHAR(MAX),
    locations NVARCHAR(MAX),
    team_id NVARCHAR(255),
    url NVARCHAR(MAX),
    sharing NVARCHAR(MAX),
    permission_level NVARCHAR(200),
    list NVARCHAR(MAX),
    project NVARCHAR(MAX),
    folder NVARCHAR(MAX),
    space NVARCHAR(MAX),
	subtasks NVARCHAR(MAX),
    dtm_Creation_Date DATETIME
);


CREATE TABLE [3rd_clickup_task_details] (
    id NVARCHAR(255),
    custom_id NVARCHAR(255),
    custom_item_id VARCHAR(50),
    name NVARCHAR(255),
    text_content NVARCHAR(MAX),
    description NVARCHAR(MAX),
    status NVARCHAR(MAX),
    orderindex VARCHAR(255),
    date_created VARCHAR(50),
    date_updated VARCHAR(50),
    date_closed VARCHAR(50),
    date_done VARCHAR(50),
    archived VARCHAR(10),
    creator NVARCHAR(MAX),
    assignees NVARCHAR(MAX),
    group_assignees NVARCHAR(MAX),
    watchers NVARCHAR(MAX),
    checklists NVARCHAR(MAX),
    tags NVARCHAR(MAX),
    parent NVARCHAR(255),
    priority NVARCHAR(255),
    due_date VARCHAR(50),
    start_date VARCHAR(50),
    points NVARCHAR(255),
    time_estimate VARCHAR(50),
    time_sent VARCHAR(50),
    custom_fields NVARCHAR(MAX),
    dependencies NVARCHAR(MAX),
    linked_tasks NVARCHAR(MAX),
    locations NVARCHAR(MAX),
    team_id NVARCHAR(255),
    url NVARCHAR(MAX),
    sharing NVARCHAR(MAX),
    permission_level NVARCHAR(200),
    list NVARCHAR(MAX),
    project NVARCHAR(MAX),
    folder NVARCHAR(MAX),
    space NVARCHAR(MAX),
	subtasks NVARCHAR(MAX),
    attachments NVARCHAR(MAX),
    dtm_Creation_Date DATETIME
);


CREATE TABLE [3rd_clickup_custom_fields] (
    id VARCHAR(50),
    name NVARCHAR(255),
    type VARCHAR(200),
    type_config NVARCHAR(max),
    date_created VARCHAR(50),
    hide_from_guests VARCHAR(10),
    required VARCHAR(10),
    dtm_Creation_Date DATETIME
);

