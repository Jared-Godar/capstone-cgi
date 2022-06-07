# Code  from Gaja

## Build Partitions

- Athena BOTO3 client
- 

```python
# Build Transformation partition
v_queryString_transformation = "ALTER TABLE " + v_athena_transformation_table + " ADD IF NOT EXISTS PARTITION\
(source_system_folder = '"+ source_system.lower() + "', table_name_folder = '"+ table_name.lower() + "', year_folder = " + v_timestamp_elements[0] + ", month_folder = " + v_timestamp_elements[1] + ", day_folder = " + v_timestamp_elements[2] + ")\
LOCATION 's3://" + v_s3_bucket + "/inbound/logs/transformation/" + source_system.lower() + "/" + table_name.lower() + "/" + v_timestamp_elements[0] + "/" + v_timestamp_elements[1] + "/" + v_timestamp_elements[2] + "';"
```

## Lambda

```python
try:
athena_query(athena_clt, v_queryString) # Dynamicaly building query string
except Exception as e:
v_process_step = 'Lambda Launch - Request to Athena Failure - Aborted'
v_job_details = 'Unable to create Athena partition within Insert Log function: ' + v_process_step + ' for file: ' + file_name + ' - AWS Athena failure'
v_error = str(e)
# Send failure notification
v_subject = v_process_step
publish_multi_message(efm_sns_topic_arn, efm_sns_subject, file_name + '\n' + '-----' + '\n' + v_job_details + '\n' + '-----' + '\n' + v_error)
# Exit
sys.exit('Exit - ' + v_job_details + ' - ' + v_error)
```

## Query Athena

```python

# Query athena
def athena_query(athenaClient, queryString):
response = athenaClient.start_query_execution(
QueryString=queryString,
QueryExecutionContext={
'Database': v_athena_db
},
ResultConfiguration={
'OutputLocation': 's3://' + v_s3_bucket + '/' + v_athena_results_folder
},
WorkGroup = v_athena_workgroup
)
return response
```
