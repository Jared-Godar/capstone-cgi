-- Create Athena Table - BOOTCAMP_CUSTOMERS_<EMP_ID>
-- Replace relevant placeholders in the template to match your values
-- Remove these comments before running DDL on Athena's Query Editor

CREATE EXTERNAL TABLE capstone_venues_478003 
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
LOCATION 's3://capstone-s3-478003/datalake/venues/duck'
TBLPROPERTIES ('has_encrypted_data'='true', 'skip.header.line.count'='1');