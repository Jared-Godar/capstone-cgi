-- Create Athena Table - BOOTCAMP_CUSTOMERS_<EMP_ID>
-- Replace relevant placeholders in the template to match your values
-- Remove these comments before running DDL on Athena's Query Editor

CREATE EXTERNAL TABLE bootcamp_customers_<EMP_ID> 
(
  `venue` string,
  `band` string,
  `supporting_act` string,
  `date` string,
  `time` string,
  `price` float
) 
PARTITIONED BY 
(
  venue string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION 's3://bootcamp-s3-<EMP_ID>/datalake/customers/'
TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count'='1');