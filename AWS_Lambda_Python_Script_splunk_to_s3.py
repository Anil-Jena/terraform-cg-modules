### AWS Lambda Python Script

import json
import tempfile
import os
import io
import csv

import boto3
import requests
import pandas as pd
from datetime import datetime

import urllib
import httplib2
import urllib.parse
import time
import re
from time import localtime,strftime
from xml.dom import minidom

# AWS S3 configuration
s3 = boto3.client('s3')

print("all lib imported")

def lambda_handler(event, context):
    
    
    #baseurl = 'https://splunkawsshdvm1.0bc7-dop-aiops.aws.cloud.airbus-v.corp:8000'
    #baseurl = 'https://ip-10-84-148-51.0bc7-dop-aiops-lan.aws.cloud.airbus.corp:8089'
    #username = 'data_repo1'
    #password = 'Datarepo@airbus2024'
    
    
    baseurl = 'https://ip-10-84-148-51.0bc7-dop-aiops-lan.aws.cloud.airbus.corp:8089'
    username = 'data_repo2'
    password = 'L6tm64n@a13ps'
    
    myhttp = httplib2.Http(disable_ssl_certificate_validation=True)
    
    
    #Step 1: Get a session key
    servercontent = myhttp.request(baseurl + '/services/auth/login', 'POST',headers={}, body=urllib.parse.urlencode({'username':username, 'password':password}))[1]
    sessionkey = minidom.parseString(servercontent).getElementsByTagName('sessionKey')[0].childNodes[0].nodeValue
    print("====>sessionkey: {} <====".format(sessionkey))
    
    
    #Step 2: Create a search job
    #searchquery = 'index="_internal" | head 10'
    
    
    #searchquery = """
    #            index=ah_eon OR index=ah_ngp_zabbix   fields.short_desc != "HeartBeat Event" earliest=01/25/2024:00:00:01 latest=04/02/2024:23:59:59
    #            | rename fields.alert_creation_date as alert_creation_date fields.alertId as alertId fields.alertKey as alertKey fields.assignment_group as assignment_group fields.category as category fields.correlationFlag as correlationFlag fields.cpuThreshold as cpuThreshold fields.customer_name as customer_name fields.companyName as companyName fields.deviceType as deviceType fields.diskName as diskName fields.externalSystem as externalSystem fields.full_desc as full_desc fields.hostname as hostname fields.impact as impact fields.node_name as node_name fields.monitorName as monitorName fields.objectName as objectName fields.modelClass as modelClass fields.modelType as modelType fields.node_ip as node_ip fields.platform as platform fields.short_desc as short_desc fields.sourceName as sourceName fields.urgency as urgency fields.memoryThreshold as memoryThreshold 
    #            | eval category="Availability issue" 
    #            | eval ticket_short_desc= externalSystem." "."Alert on"." ".node_name." "."for"." ".monitorName." "."on"." ".objectName 
    #            | eval requestFor="COP Event Management" 
    #            | eval serviceOffering="Monitoring_StdOps_12/5_CoreOperate" 
    #            | eval NEAPname ="AH_EON_CI2.1_STD_NEAPName" 
    #            | eval episode_title = "AH_EON_CI2.1_Standard" 
    #            | eval node_name=lower(node_name) 
    #            | eval node_identifier= node_name." ".objectName." ".monitorName 
    #            | eval full_desc = node_identifier.""." -Alert@: ".full_desc 
    #            | dedup node_identifier 
    #            | lookup app_snow_ticket_updates_lookup.csv node_identifier OUTPUT node_identifier dv_assigned_to dv_state dv_sys_id dv_correlation_id dv_number dv_priority dv_active dv_created_time dv_sys_updated_on 
    #            | table _time alert_creation_date alertId alertKey assignment_group category correlationFlag customer_name companyName deviceType diskName externalSystem full_desc hostname impact monitorName objectName ticket_short_desc dv_correlation_id memoryThreshold modelClass modelType node_ip node_name platform requestFor serviceOffering short_desc source urgency node_identifier NEAPname episode_title dv_number dv_state dv_priority dv_active dv_created_time dv_sys_id
    #            """
    


    #searchquery = """
    #            index=app_snow_ticket_updates earliest=-60m AND latest=now() | table *
    #            """    

    searchquery = """
                index="itsi_grouped_alerts" earliest=-60m AND latest=now()    | table  *
                """     

    if not searchquery.startswith('search'):
        searchquery = 'search ' + searchquery
    
    searchjob = myhttp.request(baseurl + '/services/search/jobs','POST',headers={'Authorization': 'Splunk %s' % sessionkey},body=urllib.parse.urlencode({'search': searchquery}))[1]
    sid = minidom.parseString(searchjob).getElementsByTagName('sid')[0].childNodes[0].nodeValue
    print("====>sid: {} <====".format(sid))
    
    
    #Step 3: Get the search status
    myhttp.add_credentials(username, password)
    servicessearchstatusstr = '/services/search/jobs/%s/' % sid
    isnotdone = True
    while isnotdone:
        searchstatus = myhttp.request(baseurl + servicessearchstatusstr, 'GET')[1] # We are getting output in bytes
        searchstatus = str(searchstatus) # Converting bytes into string
        isdonestatus = re.compile('isDone">(0|1)')
        isdonestatus = isdonestatus.search(searchstatus).groups()[0]
        if (isdonestatus == '1'):
            isnotdone = False
    print("====>search status: {} <====".format(isdonestatus))
    
    
    #Step 4: Get the search results
    services_search_results_str = '/services/search/jobs/%s/results?output_mode=json&count=0' % sid
    searchresults = myhttp.request(baseurl + services_search_results_str, 'GET')[1]
    print("====>search result: {} <====".format(searchresults))
    
     # Convert bytes to string
    search_results_str = searchresults.decode('utf-8')

    # Convert to DataFrame
    df = pd.DataFrame(searchresults)

    # Save DataFrame to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload CSV to S3
    s3_client = boto3.client('s3', region_name='eu-west-1')
    s3_client.put_object(Bucket='dop-datarepo-prod-test-bucket', Key='itsi_grouped_alerts.csv', Body=csv_buffer.getvalue())

#    # Store results in S3 bucket
#    s3.put_object(
#        Bucket='dop-datarepo-prod-test-bucket',
#        Key='splunk_results.json',
#        Body=json.dumps(search_results_str))

    # TODO implement
    return {
        'statusCode': 200,
        'code_output_1': searchresults,
        #'body': json.dumps('Hello from Lambda!')
        'body': json.dumps('Splunk data stored in S3 successfully')
    }
