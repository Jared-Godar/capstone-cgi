###
# Program Name  : fileProcessor
# Creation Date	: 2021-12-21
# Authors	    : Sahil Patel, Gaja Vaidyanatha & You :)
# Description	: This program parses the filename landing in the s3 bucket - inbound/ folder, sets the various job variables and deletes the inbound file.
# 		  
#
# Modification History:
# ---------------------
# Date			    	Item						
# ----			    	----
# 2021-12-21	    	Initial creation ###

# Import required libraries
import urllib
import urllib.parse
import boto3
import sys
import os
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
    
    # Define S3 client and resource
    s3_clt = boto3.client('s3')
    s3_rsc = boto3.resource('s3')
    
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
    v_pipeline_name = 'UNDEFINED'
    
    
    # Create date for s3 inbound file path
    v_current_timestamp = datetime.datetime.now()
    format_filepath = '%Y/%m/%d'
    v_move_file_timestamp = v_current_timestamp.strftime(format_filepath)
    
    # Begin parsing fully-qualified filename
    try:
        # Parse filename from s3 path
        v_file_name = v_source_full_s3_path[len('inbound/staging/'):len(v_source_full_s3_path)]
        
                
        ###
        # Parse filename
        ###
        
        v_file_name_parameters = v_file_name.split('-')

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
        
        # Assign Matiliion Pipeline
        v_pipeline_name = 'EFM_DATA_PIPELINE_' + v_process_type + '_' + v_process_subtype + '_PART'
        
        # Set full (unique) table name as concatenation of schema and table name
        v_full_table_name = v_source_schema + '_' + v_table_name
        
    except Exception as e:
        v_job_status = 'Aborted'
        v_process_step = 'File Processor Error - Parsing Failed - Aborted'
        print(v_process_step)
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
        sys.exit('Exit - ' + v_job_details + ' - ' + v_error)
        
    # Begin parsed variable checks
    
    # Set process step
    v_process_step = 'File Processor Checks - File parameter checks'
    print(v_process_step)
    
    # Set v_table_id based on parsed values
    v_table_id = v_source_system + '_' + v_full_table_name + '_' + v_environment + '_' + v_version
    print('Processing parameters for TABLE_ID - ' + v_table_id)
    
    # Delete file from source folder
    try:
        s3_rsc.Object(v_s3_bucket, v_source_full_s3_path).delete()
    except Exception as e:
        v_job_status = 'Aborted'
        v_process_step = 'File Processor Error - Pre-processing Failed - Aborted'
        print(v_process_step)
        v_job_details = 'Error deleting s3 file from {} in the s3 bucket {}'.format(v_source_full_s3_path, v_s3_bucket)
        v_error = str(e)
        # Log error
        # Exit
        print (v_error)
        sys.exit('Exit - ' + v_job_details + ' - ' + v_error)

    # Log success
    v_process_step = 'File Processor Finish - File parsed, checked and deleted'
    print(v_process_step)
