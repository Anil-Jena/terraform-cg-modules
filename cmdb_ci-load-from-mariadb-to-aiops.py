import json
import tempfile
import os
import io
import csv


import boto3
import requests
import pandas as pd
from datetime import datetime


#print("all lib impvorted")

## S3 Details

## S3 - Connection
s3Client = boto3.client('s3')
s3Resource = boto3.resource('s3')

## Bucket - Source Bucket
inputBucket = 'da-dop-ew1-0bc7-prod-eu-data-bronzerawdatalake'

## Bucket - Target Bucket
outputBucket = 'da-dop-ew1-0bc7-prod-eu-data-bronzerawdatalake'

## Folder Details

# Bucket - Source Folder
inputS3Folder = 'view_0BC7_cmdb_ci_aiops_final'

# Bucket - Target Folder
outputS3Folder = 'view_0BC7_cmdb_ci_aiops_final_processed'

# Bucket Object - Will be used to copy, or delete files from the bucket
sourceBucket = s3Resource.Bucket(inputBucket)
targetBucket = s3Resource.Bucket(outputBucket)



def lambda_handler(event, context):
    
    def file_availability_check():
        source_bucket_objects = [objects.key for objects in sourceBucket.objects.filter(Prefix=inputS3Folder + "/")]
    
        if len(source_bucket_objects) == 2:
            return True
        elif len(source_bucket_objects) == 1:
            return False
        elif len(source_bucket_objects) > 2:
            return "Issue - Pending Files"
        else:
            return "Error - Check Code"
    #print(file_availability_check())
    
    
    def getting_file_name():
        return [objects.key for objects in sourceBucket.objects.filter(Prefix= inputS3Folder + "/")][-1].split("/")[-1]
    #print(getting_file_name())
    
    
    def current_date_time_string():
        
        #from datetime import datetime
        now = datetime.now()
        current_date_time = now.strftime("%Y-%m-%d %H:%M:%S")
        return current_date_time

    
    ## File Name Details
    inputFileName = getting_file_name()
    
    ## File/Key Details
    inputfileS3Uri = inputS3Folder + "/" + inputFileName
    outputfileS3Uri = outputS3Folder + "/" + inputFileName
    #print(inputfileS3Uri + ", " + outputfileS3Uri)
    
    
    # Reading data as object from s3
    obj = s3Resource.Object(inputBucket, inputfileS3Uri)
    data_csv = obj.get()['Body'].read().decode('utf-8')
    #print(type(data_csv))
    
    
    # Reading data as csv from object (Previously created)
    csv_data = []
    with io.StringIO(data_csv) as csv_file:
        reader = csv.reader(csv_file, delimiter='|')
        header = next(reader)
        for row in reader:
            csv_data.append(row)
    #print(header)
    #print(csv_data)
    #print(csv_data[0])
        
        
    # converting csv data into json/dictionary
    json_data = pd.DataFrame(data = csv_data, columns = header).to_dict()
    #print(type(json_data))
    #print(json_data)
    

    def send_data_api(api_json_data):
        # API endpoint details
        #api_url = 'https://indexers.0bc7-dop-aiops.aws.cloud.airbus-v.corp:8088/services/collector/raw' # Dev
        api_url = 'https://ip-10-84-148-50.0bc7-dop-aiops-lan.aws.cloud.airbus.corp:8088/services/collector/raw'
        #print(api_url)

        #hec_token = "Splunk fac9ca48-305d-48c5-a054-6dfe457f1eda" # Dev
        hec_token = "Splunk fcfa4e5e-a8fe-41d3-81ee-9b1cd2bc59b9"
        #print(hec_token)

        # Prepare headers with authentication token
        api_headers = {
            'Authorization': f'{hec_token}',
            'Content-Type': 'application/json'
        }
        #print(api_headers)

        try:
            response = requests.post(url=api_url
                                     ,json=api_json_data
                                     ,headers=api_headers
                                     #,cert=ssl_cert_path
                                     #,cert="/var/task/ssl/cert.pem"
                                     #,verify = "/var/task/ssl/cert.pem"
                                     ,verify=False
                                     ,timeout=2.50
                                     )

            if response.status_code == 200:
                print('Data sent to the API successfully!')
                print('API Response:', response.json())
            else:
                print('Failed to send data to the API.')
                print('API Response:', response.text)

        except requests.exceptions.RequestException as e:
            print('Error sending request:', e)
        return response.status_code   
    

    csv_data = []
    with io.StringIO(data_csv) as file:
       
        # Read CSV file
        reader = csv.DictReader(file, delimiter='|')
       
        # Convert CSV data to a list of dictionaries0
        for row in reader:
            json_dict = {"sourcetype":"_json", "fields": ""}
            
            row['api_response_created_on'] = current_date_time_string()
            
            row = dict(sorted(row.items()))
           
            json_dict['fields'] = row
           
            csv_data.append(json_dict)
           
    #print(csv_data)
    
    
    json_data = csv_data
    No = 1
    
    api_data_sent_log = {
        
        "sys_id":[],
        "status":[]
        }
        
    
    for i in json_data:
        send_data_api_sys_id = i['fields']['sys_id']
        send_data_api_status = send_data_api(i)
        #send_data_api_status = 'TestIng'
        api_data_sent_log["sys_id"].append(send_data_api_sys_id)
        api_data_sent_log["status"].append(send_data_api_status)
        print("Row Number - {}, Sys ID - {}, Status - {}" .format(No, send_data_api_sys_id, send_data_api_status))
        No += 1
    
    #print(api_data_sent_log)
    
    #FileSentToAPiStatus = send_data_api(json_data)
    
    def s3_data_move_and_delete(sourceBucket, sourceFileKey, destinationFilePath):
        copy_source = {
            'Bucket': sourceBucket,
            'Key': sourceFileKey
        }
        copy_source
    
        # Copying File to Processed Folder
        targetBucket.copy(copy_source, destinationFilePath)
    
        # Deleting File - From/Input the source bucket
        s3Resource.Object(sourceBucket, sourceFileKey).delete()
    
        return "File Moved and Deleted"
    
    #print(inputBucket, ", ", inputfileS3Uri, ", ", outputfileS3Uri)
    
    print(api_data_sent_log)
    
    
    FileMovedAndDeleteStatus = s3_data_move_and_delete(sourceBucket = inputBucket,sourceFileKey = inputfileS3Uri, destinationFilePath = outputfileS3Uri)


    # TODO implement
    return {
        'statusCode': 200,
        #'codeOutputFileSentSample': json_data,
        #'codeOutputFileSentToAPiStatus': FileSentToAPiStatus,
        'codeOutputFileMovedAndDeleteStatus': FileMovedAndDeleteStatus,
        'codeOutputFileApiDataSentLog': api_data_sent_log,
        'body': json.dumps('Hello from Lambda!')
    }
