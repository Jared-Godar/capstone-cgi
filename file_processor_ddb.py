###
# Program Name  : fileProcessor_ddb.py
# Creation Date	: 2021-12-21
# Authors	    : Gaja Vaidyanatha
# Description	: This program parses the filename landing in the s3 bucket - inbound/data/ folder, sets the various job variables, uploads the data into DynamoDB, 
#                 notifies the subscriber and deletes the inbound file.
# 		  
#
# Modification History:
# ---------------------
# Date			    Item						
# ----			    ----
# 2021-12-28	  	Initial creation ###
# 2021-12-29	  	Incorporated Batch Write ###
# 2021-12-30	  	Cleaned up code,
# 2021-12-30        Got batch_writer() to work
# 2022-01-01        Incorporated duplicate key overwrites



# Import required libraries
import urllib
import urllib.parse
import boto3
import sys
import os
import csv
import logging
import time
import datetime

from botocore.exceptions import ClientError

# Define logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

## Retrieve AWS Lambda Environmental Variables
aws_region = os.environ['AWS_REGION']
job_name = os.environ['AWS_LAMBDA_FUNCTION_NAME']

# Function to retrieve dataframe from CSV object on S3
def get_csv_body(bucket_name, object_key, s3_client):
    try:
        csv_obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        csv_body = csv_obj["Body"].read().decode("utf-8").split("\n")
        return csv_body
    except Exception as e:
        v_job_status = 'Aborted'
        v_process_step = 'File Processor Error - CSV records retrieval failed - Aborted'
        print(v_process_step)
        v_job_details = 'Unable to retrieve CSV records'
        v_error = str(e)
        sys.exit('Exit - ' + v_job_details + ' - ' + v_error)
        
# Function to write CSV body into DynamoDB
def ddb_batch_write(ddb_csv_body, ddb_table_name):
    print('Preparing to insert into DynamoDB...')
    # Get an instance of v_table_name from DynamoDB
    
    try:
        with ddb_table_name.batch_writer(overwrite_by_pkeys=['customer_id']) as batch:
            for row in ddb_csv_body:
                customer_record = row.split("|")
                if len(customer_record) > 0:
                    v_Items = {
                        "customer_id"   : int(customer_record[0]),
                        "created_date"  : customer_record[1],
                        "customer_name" : customer_record[2],
                        "addr_street"   : customer_record[3],
                        "addr_city"     : customer_record[4],
                        "addr_state"    : customer_record[5],
                        "addr_zip"      : customer_record[6]
                            }
                    try:
                        response = batch.put_item(Item=v_Items)
                    except Exception as e:
                        v_job_status = 'Aborted'
                        v_process_step = 'File Processor Error - DynamoDB batch_writer().put_item failed - Aborted'
                        print(v_process_step)
                        v_job_details = 'Unable to execute batch_writer().put_item in DynamoDB!'
                        v_error = str(e)
                        sys.exit('Exit - ' + v_job_details + ' - ' + v_error)
                else:
                    print ('CSV record read is NULL')
        print('Batch Insert into DynamoDB completed successfully')
    
    except Exception as e:
        v_job_status = 'Aborted'
        v_process_step = 'File Processor Error - DynamoDB Write ddb_scv_body loop failed - Aborted'
        print(v_process_step)
        v_job_details = 'Unable to write to DynamoDB...CSV records could not loaded!'
        v_error = str(e)
        sys.exit('Exit - ' + v_job_details + ' - ' + v_error)
            

## Main Lambda Function
def lambda_handler(event, context):
    ###
    # Pre-Processing
    ###
    
    # Set timezone to CDT/CST
    os.putenv('TZ', 'America/Chicago')
    time.tzset()
    
    # Define misc variables
    v_start_time = round(time.time(),2) # Get current time
    v_min_file_size_bytes = 10
     
    # Define log variables
    v_process_step = 'File Processor - Start'
    v_job_status = 'Started'
    v_error = 'No errors encountered'
    v_parse_error_msg = 'Expected format is yyyy_mm_dd_hh24miss-[data,meta]-[normal,reproc]-[full,incr,dups,dels,errs]-[source]-[source_schema]-[env]-[version]-[table_name]-[partition_name].[file_ext].'
    function_id = sys._getframe().f_code.co_name
    logger.info("Function Name: {function_id}")
    
    # Get the s3 bucket, fully-qualified filename, and region from the event
    v_s3_bucket = event['Records'][0]['s3']['bucket']['name']
    v_source_full_s3_path = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    v_aws_region = event['Records'][0]['awsRegion']
    
    # Define S3, SNS and DynamoDB clients and resources
    s3_clt = boto3.client('s3')
    s3_rsc = boto3.resource('s3')
    sns_clt = boto3.client('sns')
    ddb_clt = boto3.client('dynamodb')
    ddb_rsc = boto3.resource('dynamodb')
    
    # Retrive metadata about object
    response = s3_clt.head_object(Bucket=v_s3_bucket, Key=v_source_full_s3_path)
    object_size = int(response['ContentLength'])
    print('Processing S3 bucket: ' + v_s3_bucket)
    print('Processing file: ' + v_source_full_s3_path)
    print('Size of file: ' + str(object_size) + ' bytes')

    ###
    # Begin Processing
    ###

    # Set process step
    v_process_step = 'File Processor Start - Parameter parsing'
    print(v_process_step)
    
    # Initialize parsed variables
    v_file_name = 'undefined'
    v_table_name = 'UNDEFINED'
    v_full_table_name = 'UNDEFINED'
    v_partition_name = 'UNDEFINED'
    v_source_system = 'UNDEFINED'
    v_source_schema = 'UNDEFINED'
    v_environment = 'UNDEFINED'
    v_version = 'UNDEFINED'
    v_file_timestamp = 'UNDEFINED'
    v_file_type = 'UNDEFINED'
    v_process_type = 'UNDEFINED'
    v_process_subtype = 'UNDEFINED'
    
        
    # Create date for s3 inbound file path
    v_current_timestamp = datetime.datetime.now()
    format_filepath = '%Y/%m/%d'
    
    # Begin parsing fully-qualified filename
    try:
        # Parse filename from s3 path
        v_file_name = v_source_full_s3_path[len('inbound/data/'):len(v_source_full_s3_path)]
        
                
        ###
        # Parse filename for all relevant parameters
        ###
        v_table_name_underscore = v_file_name_parameters[8].upper()
        v_table_name = v_table_name_underscore.replace('_','-')

        # Parse file timestamp
        v_file_timestamp = v_file_name_parameters[0]
        
        # Parse filetype
        v_file_type = v_file_name_parameters[1].upper()
        
        # Parse processtype
        v_process_type = v_file_name_parameters[2].upper()

        # Parse process subtype
        v_process_subtype = v_file_name_parameters[3].upper()
        
        # Parse source system
        v_source_system = v_file_name_parameters[4].upper()
        
        # Parse source schema
        v_source_schema = v_file_name_parameters[5].upper()
        
        # Parse environment
        v_environment = v_file_name_parameters[6].upper()
        
        # Parse version
        v_version = v_file_name_parameters[7]

        # Parse tablename
        v_table_name = v_file_name_parameters[8].upper()
        
        # Parse last parameter and split
        v_remainder_parameters = v_file_name_parameters[9]
        v_last_parameters = v_remainder_parameters.split('.')

        # Parse partition_name
        v_partition_name = v_last_parameters[0].upper()
        
        # Check parse file extension
        v_file_extension = v_last_parameters[1].upper()
        
        # Set full (unique) table name as concatenation of schema and table name
        v_full_table_name = v_source_schema + '_' + v_table_name
        
    except Exception as e:
        v_job_status = 'Aborted'
        v_process_step = 'File Processor Error - Parsing failed - Aborted'
        v_job_details = 'Filename could not be parsed. {}'.format(v_parse_error_msg)
        v_error = str(e)
     
	# Exit
        sys.exit('Exit - ' + v_job_details + ' - ' + v_error)
        
    
    ###
    # Check File Size in Bytes
    ###
    if object_size < v_min_file_size_bytes:
        v_job_status = 'Aborted'
        v_process_step = 'File Processor Error - File Empty - Aborted'
        print(v_process_step)
        v_job_details = 'File size less than {} bytes; please ensure file contains data.'.format(v_min_file_size_bytes)
        # Send failure notification
        # Exit
        sys.exit('Exit - ' + v_job_details)
    
    # Begin parsed variable checks
    
    # Set process step
    v_process_step = 'File Processor Checks - File parameter checks'
    print(v_process_step)
    
    # Set v_table_id based on parsed values
    v_table_id = v_source_system + '_' + v_full_table_name + '_' + v_environment + '_' + v_version
    print('Processing parameters for TABLE_ID - ' + v_table_id)
    
    ## Retrieve CSV file from S3
    print ('Bucket Name - ' + v_s3_bucket)
    print ('Object Name - ' + v_source_full_s3_path)
    
    v_csv_body = get_csv_body(v_s3_bucket, v_source_full_s3_path, s3_clt)
    print ('CSV Body - ' + str(v_csv_body))
    
    v_table_name = v_table_name.lower()
    print (' Looking for DynamoDB Table Name - ' + v_table_name)
    try:
        v_ddb_table_name = ddb_rsc.Table(v_table_name)
        print ('DynamoDB Table instantiation was succcessful')
    except Exception as e:
        v_job_status = 'Aborted'
        v_process_step = 'File Processor Error - DynamoDB table instantiation failed - Aborted'
        print(v_process_step)
        v_job_details = 'Referenced table - ' + ddb_table_name + ' does not exist'
        v_error = str(e)
        sys.exit('Exit - ' + v_job_details + ' - ' + v_error)
   
    # Insert data into DynamoDB...call ddb_put_items
    ddb_batch_write(v_csv_body, v_ddb_table_name)
    
    # Delete file from source folder
    try:
        s3_rsc.Object(v_s3_bucket, v_source_full_s3_path).delete()
    except Exception as e:
        v_job_status = 'Aborted'
        v_process_step = 'File Processor Error - File Delete failed - Aborted'
        print(v_process_step)
        v_job_details = 'Error deleting s3 file from {} in the s3 bucket {}'.format(v_source_full_s3_path, v_s3_bucket)
        v_error = str(e)
        # Log error
        # Exit
        print (v_error)
        sys.exit('Exit - ' + v_job_details + ' - ' + v_error)
 
    # Send SNS message
    v_sns_topic_arn ='arn:aws:sns:us-west-2:897423702380:bootcamp-sns-topic-444395'
    v_sns_message 	='File for TABLE_ID - ' + v_table_id + ' was successfully parsed and loaded into DynamoDB and deleted.'
    v_subject	= 'Data Engineering Academy - Pipeline Notifications'

    try:
        response = sns_clt.publish(
                        TopicArn = v_sns_topic_arn,
                        Message = v_sns_message,
                        Subject =v_subject,
            )
    except ClientError as e:
        v_job_status = 'Aborted'
        v_process_step = 'File Processor - Processing failed - Aborted'
        print(v_process_step)
        v_job_details = 'Unable to publish to SNS topic - ' + v_sns_topic_arn + ' - AWS SNS failure'
        v_error = str(e)
        # Log error
	    # Exit
        sys.exit('Exit - ' + v_job_details + ' - ' + v_error) 

    # Log success
    v_process_step = 'File Processor Finish - File parsed, checked and deleted'
    print(v_process_step)

