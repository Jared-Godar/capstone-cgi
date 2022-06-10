-- Create Athena Table

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

-- Create partitions

ALTER TABLE capstone_venues_478003
ADD PARTITION (venues_folder = 'duck') 
  LOCATION 's3://capstone-s3-478003/datalake/venues/duck/';

ALTER TABLE capstone_venues_478003 
ADD PARTITION (venues_folder = 'heights') 
  LOCATION 's3://capstone-s3-478003/datalake/venues/heights/';

ALTER TABLE capstone_venues_478003
ADD PARTITION (venues_folder = 'white_oak') 
  LOCATION 's3://capstone-s3-478003/datalake/venues/white_oak/';
  
ALTER TABLE capstone_venues_478003
ADD PARTITION (venues_folder = 'warehouse_live') 
  LOCATION 's3://capstone-s3-478003/datalake/venues/warehouse_live/';

-- Sample Queries

SELECT * FROM capstone_venues_478003
  WHERE price < 25 AND
  DATE < 