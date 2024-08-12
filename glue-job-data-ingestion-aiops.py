import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


import json

import pymysql.cursors
import datetime
from datetime import datetime, date, timedelta

import pandas as pd
from io import StringIO

import boto3  # AWS

print("Step 00 - Library Imported")


import json
import tempfile
import os
import io
import csv

import boto3
import requests
import pandas as pd
import numpy as np
from datetime import datetime

import urllib
import httplib2
import urllib.parse
import time
import re
from time import localtime,strftime
from xml.dom import minidom



print("Step 01 - Library Imported")

pendingTasksList = {
    1: 'Code beautify'
}


def main():
    # TODO implement
    
    "Old Credentials"
    baseurl = 'https://splunkawsshdvm1.0bc7-dop-aiops.aws.cloud.airbus-v.corp:8089'
    username = 'data_repo'
    password = 'a1rbusa10ps'
    
    "New Credentials"
    baseurl = 'https://splunkawsshdvm1.0bc7-dop-aiops.aws.cloud.airbus-v.corp:8089'
    username = 'data_repo1'
    password = 'Datarepo@airbus2024'
    
    myhttp = httplib2.Http(disable_ssl_certificate_validation=True)
    
    
    #Step 1: Get a session key
    servercontent = myhttp.request(baseurl + '/services/auth/login', 'POST',headers={}, body=urllib.parse.urlencode({'username':username, 'password':password}))[1]
    sessionkey = minidom.parseString(servercontent).getElementsByTagName('sessionKey')[0].childNodes[0].nodeValue
    print("====>sessionkey: {} <====".format(sessionkey))
    
    
    #Step 2: Create a search job
    searchquery = 'index="_internal" | head 10'
    
    
    searchquery = """
                index=ah_eon fields.short_desc != "HeartBeat Event" earliest=01/25/2020:00:00:01 latest=02/25/2024:23:59:59 
                
                | head 10
                | rename fields.alert_creation_date as alert_creation_date  fields.alertId as alertId fields.alertKey as alertKey fields.assignment_group as assignment_group fields.category as category fields.correlationFlag as correlationFlag fields.cpuThreshold as cpuThreshold fields.customer_name as customer_name fields.companyName as companyName fields.deviceType  as deviceType fields.diskName as diskName fields.externalSystem as externalSystem  fields.full_desc as full_desc fields.hostname as hostname fields.impact as impact fields.node_name as node_name fields.monitorName as monitorName fields.objectName as objectName fields.modelClass as modelClass  fields.modelType as modelType  fields.node_ip as node_ip fields.platform as platform fields.short_desc as short_desc fields.sourceName as sourceName fields.urgency as urgency fields.memoryThreshold as memoryThreshold
                
                | eval category="Availability issue"  
                | eval ticket_short_desc= externalSystem." "."Alert on"." ".node_name." "."for"." ".monitorName." "."on"." ".objectName
                | eval requestFor="COP Event Management"
                | eval serviceOffering="Monitoring_StdOps_12/5_CoreOperate"
                | eval NEAPname ="AH_EON_CI2.1_STD_NEAPName"
                | eval episode_title =  "AH_EON_CI2.1_Standard"
                | eval node_name=lower(node_name)
                | eval node_identifier= node_name." ".objectName." ".monitorName
                | eval full_desc = node_identifier.""." -Alert@: ".full_desc
                | dedup node_identifier
                | lookup app_snow_ticket_updates_lookup.csv node_identifier OUTPUT node_identifier dv_assigned_to dv_state dv_sys_id dv_correlation_id dv_number dv_priority dv_active dv_created_time dv_sys_updated_on
                
                | table  alert_creation_date alertId alertKey assignment_group category correlationFlag customer_name companyName deviceType diskName externalSystem full_desc hostname impact monitorName objectName ticket_short_desc  dv_correlation_id memoryThreshold modelClass modelType node_ip node_name platform requestFor serviceOffering short_desc source urgency node_identifier NEAPname episode_title dv_number dv_state dv_priority dv_active dv_created_time dv_sys_id dv_sys_updated_on

                """
    searchquery = """
                index=ah_eon fields.short_desc != "HeartBeat Event"
                
                | head 40
                | rename fields.alert_creation_date as alert_creation_date  fields.alertId as alertId fields.alertKey as alertKey fields.assignment_group as assignment_group fields.category as category fields.correlationFlag as correlationFlag fields.cpuThreshold as cpuThreshold fields.customer_name as customer_name fields.companyName as companyName fields.deviceType  as deviceType fields.diskName as diskName fields.externalSystem as externalSystem  fields.full_desc as full_desc fields.hostname as hostname fields.impact as impact fields.node_name as node_name fields.monitorName as monitorName fields.objectName as objectName fields.modelClass as modelClass  fields.modelType as modelType  fields.node_ip as node_ip fields.platform as platform fields.short_desc as short_desc fields.sourceName as sourceName fields.urgency as urgency fields.memoryThreshold as memoryThreshold
                
                | eval category="Availability issue"  
                | eval ticket_short_desc= externalSystem." "."Alert on"." ".node_name." "."for"." ".monitorName." "."on"." ".objectName
                | eval requestFor="COP Event Management"
                | eval serviceOffering="Monitoring_StdOps_12/5_CoreOperate"
                | eval NEAPname ="AH_EON_CI2.1_STD_NEAPName"
                | eval episode_title =  "AH_EON_CI2.1_Standard"
                | eval node_name=lower(node_name)
                | eval node_identifier= node_name." ".objectName." ".monitorName
                | eval full_desc = node_identifier.""." -Alert@: ".full_desc
                | dedup node_identifier
                | lookup app_snow_ticket_updates_lookup.csv node_identifier OUTPUT node_identifier dv_assigned_to dv_state dv_sys_id dv_correlation_id dv_number dv_priority dv_active dv_created_time dv_sys_updated_on
                
                | table  alert_creation_date alertId alertKey assignment_group category correlationFlag customer_name companyName deviceType diskName externalSystem full_desc hostname impact monitorName objectName ticket_short_desc  dv_correlation_id memoryThreshold modelClass modelType node_ip node_name platform requestFor serviceOffering short_desc source urgency node_identifier NEAPname episode_title dv_number dv_state dv_priority dv_active dv_created_time dv_sys_id dv_sys_updated_on

                """
    print(searchquery)

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
    print("1.0")
    #print(searchresults)
    
    #searchresults = myhttp.request(baseurl + services_search_results_str, 'GET')
    
    print("2.0")
    #print(searchresults)
    #print("====>search result: {} <====".format(searchresults))
    
    print("3.0")
    print(type(searchresults))
    
    print("3.1")
    #print(searchresults)
    
    
    print("3.2")
    #print(str(searchresults))
    
    print("3.3")
    #searchresults_data = json.loads(searchresults)

    print("4.0")
    index_length = len(searchresults_data['results'])
    json_data_in_list = searchresults_data['results']
    
    print("5.0")
    df = pd.DataFrame(json_data_in_list, index = np.arange(index_length))
    
    print(df)
    
    # TODO implement
    return {
        'statusCode': 200,
        'code_output_1': searchresults,
        'code_input_query': searchquery,
        'body': json.dumps('Hello from Lambda!')
    }



main()



job.commit()
