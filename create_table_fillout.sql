CREATE TABLE [3rd_fillout_forms] (
    name VARCHAR(255),
    formId VARCHAR(255),
    id INT,
    dtm_Creation_Date DATETIME
);


CREATE TABLE [3rd_fillout_form_metadata] (
    id VARCHAR(255),
    name NVARCHAR(255),
    questions NVARCHAR(max),
    calculations NVARCHAR(max),
    urlParameters NVARCHAR(max),
    documents NVARCHAR(max),
    scheduling NVARCHAR(max),
    dtm_Creation_Date DATETIME
);

CREATE TABLE [3rd_fillout_submissions] (
    submissionId VARCHAR(255),
    submissionTime DATETIME,
    lastUpdatedAt DATETIME,
    questions NVARCHAR(max),
    calculations NVARCHAR(max),
    urlParameters NVARCHAR(max),
    documents NVARCHAR(max),
    scheduling NVARCHAR(max),
    dtm_Creation_Date DATETIME
);