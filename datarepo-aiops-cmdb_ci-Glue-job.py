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
import csv
import re
# import awswrangler as wr
# import openpyxl
# import fastparquet
# import pyarrow

# import mysql.connector
import pymysql.cursors

import datetime
from datetime import datetime, date, timedelta

import pandas as pd
# import io as BytesIO
from io import BytesIO
from io import StringIO

import boto3  # AWS

print("Step 00 - Library Imported")

pendingTasksList = {
    1: 'Code beautify'
}


def main():
    # TODO implement

    def puttingDataInS3(var_field_sourceTable):

        # Dictionary for storing logging information
        LogTableData = {
            'jobInitialStatus': 'Started',
            'rowsCount': '',
            'sourceSchema': '',                 # 'dataBasename'        #Change_NewFields_ThisCommentNeedToBeRemovedLater
            'jobSourceDataStore': '',           # 'dbTableName'         #Change_NewFields_ThisCommentNeedToBeRemovedLater
            'jobTargetSchema': '',                                      #Change_NewFields_ThisCommentNeedToBeRemovedLater
            'jobTargetDataStore': '',           # 'outputCsvFileName'   #Change_NewFields_ThisCommentNeedToBeRemovedLater
            'jobStartAt': '',
            'jobEndAt': '',
            'dataFrom': '',
            'dataTo': '',
            'dataSqlQuery': '',
            'jobFinalStatus': '',
            'dwh_batch_id': '',                       #Change_NewFields_ThisCommentNeedToBeRemovedLater
            'dwh_created_date': '',                   #Change_NewFields_ThisCommentNeedToBeRemovedLater
            'dwh_updated_date': ''                    #Change_NewFields_ThisCommentNeedToBeRemovedLater


        }

        # print("Start Time is {}".format(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        LogTableData['jobStartAt'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


        # Getting Current Time and Date
        currentDateTime = datetime.utcnow()
        dateString = currentDateTime.strftime("%d/%m/%Y")
        dateTimeStringFormat1 = currentDateTime.strftime("%Y-%m-%d %H:%M:%S")  # Format - DD/MM/YY HH:MM:SS - Will be used for dynamodb last_run_date field
        dateTimeStringFormat2 = currentDateTime.strftime("%Y%m%d%H%M%S")  # Format - DDMMYYHHMMSS - Will be used for csv file
        # print(dateTimeStringFormat1)
        # print(dateTimeStringFormat2)


        ## Systems Manager (SSM Agent) Details
        ssm = boto3.client('ssm')

        # AIRBUS Snowdb Instance Details - From SSM
        dbEndpoint = ssm.get_parameter(Name='/airbus/prod/snowdb/url')['Parameter']['Value']

        # AIRBUS Snowdb Details-
        dbDatabaseName = var_field_sourceTable.split("|")[0]
        dbTableName = var_field_sourceTable.split("|")[1]
        dbUserName = ssm.get_parameter(Name='/airbus/prod/snowdb/username')['Parameter']['Value']
        dbUserPassword = ssm.get_parameter(Name='/airbus/prod/snowdb/password', WithDecryption=True)['Parameter']['Value']
        # print(dbEndpoint)
        # print(dbUserName)
        # print(dbUserPassword)


        # Creating dwh details variables
        dwh_batch_id  = "{Source_Name}-{Table_Name}-{ID}".format(Source_Name = dbDatabaseName, Table_Name = dbTableName , ID = dateTimeStringFormat2)
        dwh_created_date = dateTimeStringFormat1
        dwh_updated_date = dateTimeStringFormat1

        # updating dwh details for log table
        LogTableData['dwh_batch_id'] = dwh_batch_id                 #Change_NewFields_ThisCommentNeedToBeRemovedLater
        LogTableData['dwh_created_date'] = dwh_created_date         #Change_NewFields_ThisCommentNeedToBeRemovedLater
        LogTableData['dwh_updated_date'] = dwh_updated_date         #Change_NewFields_ThisCommentNeedToBeRemovedLater
        print(LogTableData['dwh_batch_id'])                                     #Change_NewFields_ThisCommentNeedToBeRemovedLater


        # DynamoDb Details
        dynamodb = boto3.resource('dynamodb')
        client = boto3.client('dynamodb')
        field_sourceTable = var_field_sourceTable
        tableName = "dop_datapipeline_prod_parameters"


        # S3 Details
        s3Client = boto3.client('s3')
        s3Resource = boto3.resource('s3')
        inputBucket = 'da-dop-prod-eu-data-bronzerawdatalake'
        inputBucket = 'da-dop-ew1-0bc7-prod-eu-data-bronzerawdatalake' #GauravAdded_5_9_2023
        folderName = field_sourceTable.split("|")[1]  # S3 Folder Name, Where CSV Files Data is stored it is same as table name in database
        fileName = field_sourceTable.split("|")[1]  # CSV File Name, Where Data is stored it is same as table name in database
        pathSeperator = '/'
        filePath = folderName + pathSeperator
        fileExtension = '.csv'
        # fileExtension       = '.xlsx'
        # fileExtension       = '.parquet'
        fileSeperator = '-'
        filePathAndName = filePath + fileName + fileExtension
        filePathNameTime = filePath + fileName + fileSeperator + dateTimeStringFormat2 + fileExtension

        # Getting Values from DynamoDb Table
        dynamodb = boto3.resource('dynamodb')
        client = boto3.client('dynamodb')
        # table               = dynamodb.Table(tableName)

        response = client.get_item(
            TableName=tableName,
            Key={
                "source_table": {"S": field_sourceTable}
            },
            ConsistentRead=True
        )
        
        
        def last_run_date_modified(actual_last_run_date):
            '''
            :param actual_last_run_date:
            :return: This function will return week old date from the today, if the current day is monday else return the actual last run date
            Facing some data discrepency issue, this is the fix to reload weekly data,
            this function will check they given condition and create a delta date
            '''
            from datetime import datetime, timedelta
        
            week_old_date_time = datetime.utcnow() - timedelta(days=7)
            # Monday is Day Zero "0"
            if datetime.utcnow().weekday() == 0:
                return week_old_date_time.strftime("%Y-%m-%d 00:00:00")
            else:
                return actual_last_run_date
        
        
        # Sql Query Variables - for where condition, getting latest data
        sqlColumnCreateDate = response['Item']['create_date']['S']
        sqlColumnUpdateDate = response['Item']['update_date']['S']
        sqlTableLastValue = last_run_date_modified(response['Item']['last_run_date']['S'])
        sqlTableLastValue = response['Item']['last_run_date']['S']
        sqlTableLastValueNew = dateTimeStringFormat1
        sqlTableQuey = response['Item']['query']['S']

        # Getting Fields/Column names from Dynamo DB
        fieldsList = response['Item']['fields']['L']
        fieldsListNames = [fieldsList[i]['S'] for i in range(len(fieldsList))]
        #print(fieldsListNames)

        # Filtering fields in mySql Query itself, Taking fields from DynamoDB
        fieldsNames = ", ".join(fieldsListNames)
        # print(fieldsNames)

        # print("Sql Query Here From Dynamo DB")
        # print(sqlTableQuey)

        # Getting Query From DynamoDB
        querySelectWhereCreate = sqlTableQuey.format(
            varTableName=dbTableName,
            varColName_1=sqlColumnCreateDate,
            varColName_2=sqlColumnUpdateDate,
            varColValueFrom=sqlTableLastValue,
            varColValueTo=sqlTableLastValueNew
        )
        print(querySelectWhereCreate)
        
        # log table Bucket Name in the Log Table
        LogTableData['jobTargetSchema'] = inputBucket

        # Recording DataFrom and DataTo time in log table
        LogTableData['dataFrom'] = sqlTableLastValue
        LogTableData['dataTo'] = sqlTableLastValueNew
        '''
        querySelectWhereCreate      = """Select * From {varTableName} Where `{varColName_1}` >= '{varColValueFrom}' and `{varColName_1}` < '{varColValueTo}' UNION ALL Select * From {varTableName} Where `{varColName_1}` >= '{varColValueFrom}' and `{varColName_1}` < '{varColValueTo}';""".format(
                                                                                        varTableName        = dbTableName,
                                                                                        varColName_1        = sqlColumnCreateDate,
                                                                                        varColName_2        = sqlColumnCreateDate,
                                                                                        varColValueFrom     = sqlTableLastValue,
                                                                                        varColValueTo       = sqlTableLastValueNew
                                                                                        )

        '''
        # Sql Query
        # querySelect                 = "Select * From {} limit 0,50;".format(dbTableName)  # Working
        # querySelectWhereCreate      = """Select * From {} Where `{}` >= '{}';""".format(dbTableName, sqlColumnCreateDate, sqlTableLastValue)  # Working
        # querySelectWhereUpdate      = """Select * From {} Where `{}` >= '{}';""".format(dbTableName, sqlColumnUpdateDate, sqlTableLastValue )  # Working

        ## Sql Query - Using this query for downloading historical data - Month Wise, Once Done - Need To Be Removed
        # querySelectWhereCreate      = "Select * From {} Where `{}` >= '2022-10-01 00:00:00' and `{}` < '2022-11-01 00:00:00';".format(dbTableName, sqlColumnCreateDate, sqlColumnCreateDate)   # Working - Oct
        # querySelectWhereCreate      = "Select * From {} Where `{}` >= '2022-11-01 00:00:00' and `{}` < '2022-12-01 00:00:00';".format(dbTableName, sqlColumnCreateDate, sqlColumnCreateDate)   # Working - Nov
        # querySelectWhereCreate      = "Select * From {} Where `{}` >= '2022-12-01 00:00:00' and `{}` < '2023-01-01 00:00:00';".format(dbTableName, sqlColumnCreateDate, sqlColumnCreateDate)   # Working - Dec
        # querySelectWhereCreate      = "Select * From {} Where `{}` >= '2023-01-01 00:00:00' and `{}` < '2023-02-01 00:00:00';".format(dbTableName, sqlColumnCreateDate, sqlColumnCreateDate)   # Working - Jan
        # querySelectWhereCreate      = "Select * From {} Where `{}` >= '2023-02-01 00:00:00' and `{}` < '2023-03-01 00:00:00';".format(dbTableName, sqlColumnCreateDate, sqlColumnCreateDate)   # Working - Feb
        # querySelectWhereCreate      = "Select * From {} Where `{}` >= '2023-03-01 00:00:00' and `{}` < '2023-03-27 00:00:00';".format(dbTableName, sqlColumnCreateDate, sqlColumnCreateDate)   # Working - Mar Till 26th
        # querySelectWhereCreate      = "Select * From {} Where sys_created_on >= '2023-02-01 00:00:00' and sys_created_on < '2023-02-03 00:00:00';".format(dbTableName)  # Working - 2 Days
        # querySelectWhereCreate      = "Select * From {} Where `{}` >= '2022-12-01 00:00:00' and `{}` < '{}';".format(dbTableName, sqlColumnCreateDate, sqlColumnCreateDate, sqlTableLastValueNew)   # Working - Dec To Till Now
        # querySelectWhereCreate      = "Select * From {} limit 0,50;".format(dbTableName)  # Working
        redshift_data_fetch_query   = querySelectWhereCreate
        querySelectWhereCreate      = "Select * From view_0BC7_cmdb_ci limit 10;"
        querySelectWhereCreate_temp      = "Select * From view_0BC7_cmdb_ci limit 10;"

        # print(sqlTableLastValue)
        # print(querySelectWhereUpdate)

        print("Sql Query Passed Final")
        print(querySelectWhereCreate)

        # Recording the SQL Query Passed in client database, in log table
        LogTableData['dataSqlQuery'] = querySelectWhereCreate

        # Recording the run date and time in log table
        LogTableData['dataRunTimeValue'] = sqlTableLastValueNew

        try:
            
            print("""Testing Print Remark - Data Reading Redshift - Creating Variable""")
            ## Creating Variables
            # Redshift Connection Variables
            var_redshiftTmpDir = "s3://aws-glue-assets-204955557577-eu-west-1/temporary/"
            var_aws_iam_role = "arn:aws:iam::204955557577:role/da-dop-pipeline-prod-iam-glue-v2"
            var_connectionName = "da-dop-pipeline-prod-connection-jdbc-v1-without-route53"
            
            print("""Testing Print Remark - Data Reading Redshift - Creating Query""")
            #redshift_data_fetch_query = "Select * From public.core_company limit 10;"
            print(redshift_data_fetch_query)
            
            print("""Testing Print Remark - Data Reading Redshift - Data Reading and Creating DynamicFrame""")
            # Reading Redshift Data - All Data - For now we are using S3
            
            redshift_data_table_01 = (
                glueContext.create_dynamic_frame.from_options(
                    connection_type="redshift",
                    connection_options={
                        "sampleQuery": redshift_data_fetch_query,
                        "redshiftTmpDir": var_redshiftTmpDir,
                        "useConnectionProperties": "true",
                        "aws_iam_role": var_aws_iam_role,
                        "connectionName": var_connectionName,
                    },
                    # transformation_ctx="redshift_data_table_01", # Commented This One to ignore bookmarking for this transformation, As we need past data for the same as well
                )
            )
            print("Reading Redshift Data Table 01")
            print("Rows Count - {}".format(redshift_data_table_01.count()))
            
            # updating details for log table
            LogTableData['sourceSchema'] = 're_datahub'
            LogTableData['jobSourceDataStore'] = 'dynamic_query_mulitple_table'
            LogTableData['rowsCount'] = redshift_data_table_01.count()
            

        except Exception as e1:
            print("Database Connection Error - ", e1)

        try:

            print("""Data Reading and Writing""")
            
            print("""Testing Print Remark - Data Reading Redshift - Converting DynamicFrame to Pandas DataFrame""")
            df = redshift_data_table_01.toDF().toPandas()
            print(df.head(5))

            print("""Testing Print Remark - Data Reading Redshift - Filtering fields, Taking fields from DynamoDB""")
            # Filtering fields, Taking fields from DynamoDB
            
            print("""Testing Print Remark - Data Reading Redshift - printing fields new df columns name - """)
            print(df.columns)
            
            print("""Testing Print Remark - Data Reading Redshift - printing fields DynamoDB fields name - """)
            df = df[fieldsListNames]
            print(df.head(5))
            
            print("""Testing Print Remark - Data Reading Redshift - updating dwh details for log table""")
            # updating dwh details for log table
            df['dwh_batch_id'] = dwh_batch_id               # Change_NewFields_ThisCommentNeedToBeRemovedLater
            df['dwh_created_date'] = dwh_created_date       # Change_NewFields_ThisCommentNeedToBeRemovedLater
            df['dwh_updated_date'] = dwh_updated_date       # Change_NewFields_ThisCommentNeedToBeRemovedLater
            print(df.head(5))

            # print("Step 02 - MySql Data Read Done")
            # Writing MySql Data S3 - Using Pandas

            ## Writing CSV File in S3
            csv_buffer = StringIO()
            # df.to_csv(csv_buffer, index=False), # Saving file in CSV Fromat
            df.to_csv(csv_buffer, sep='|', index=False),  # Saving file in CSV Fromat
            s3Resource.Object(bucket_name=inputBucket, key=filePathNameTime).put(Body=csv_buffer.getvalue())  # For CSV Format

            ## Writing Excel File in S3
            # excel_buffer = BytesIO()
            # df.to_excel(excel_buffer, index=False, engine='openpyxl'), # Saving file in Excel Fromat
            # s3Resource.Object(bucket_name=inputBucket, key=filePathNameTime).put(Body=excel_buffer.getvalue()) # For Excel Format

            ## Writing Parquet File in S3
            # parquet_buffer = BytesIO()
            # df.to_parquet(parquet_buffer, index=False)
            # s3Resource.Object(bucket_name=inputBucket, key=filePathNameTime).put(Body=parquet_buffer.getvalue()) # For Parquet Format

            LogTableData['jobTargetDataStore'] = filePathNameTime
            # print("Step 03 - Data Write In S3 - csv Format")

            # Update Item or Multiple Items- With Resource
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table(tableName)

            # Updating Last Run Date
            table.update_item(
                Key={
                    'source_table': field_sourceTable,
                },
                UpdateExpression="""
                                    SET 
                                    last_run_date = :val1
                                """,
                ExpressionAttributeValues={
                    ':val1': dateTimeStringFormat1
                }
            )
            # print("Step 99 - Values Updated")
            LogTableData['jobFinalStatus'] = "Values Updated"

        except Exception as e2:
            print("Dynamodb Error - ", e2)

        # print("End Time is {}".format(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        LogTableData['jobEndAt'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        return LogTableData

    # puttingDataInS3()

    def listOfSourceTablesInDynamoDbTable():
        # DynamoDb Details
        dynamodb = boto3.resource('dynamodb')
        client = boto3.client('dynamodb')
        tableName = "dop_datapipeline_prod_parameters"

        try:

            # Getting the list of all the items available source_tables field in the dynamoDB table
            table = dynamodb.Table(tableName)
            response = table.scan()

            dynamoDbTableScannedCount = response['ScannedCount']
            dynamoDbTableScannedNameList = [response['Items'][i]['source_table'] for i in
                                            range(dynamoDbTableScannedCount)]
            # print(dynamoDbTableScannedNameList)

        except Exception as e1:
            print("dynamoDB Table Scan - Source Table List Error ", e1)

        return dynamoDbTableScannedNameList

    tablesList = listOfSourceTablesInDynamoDbTable()

    tablesList = ['DataHub|view_0BC7_cmdb_ci_aiops']

    logDf = pd.DataFrame()
    for i in tablesList:
        print(i)
        # print(puttingDataInS3(i))
        if len(logDf) == 0:
            logDf = pd.DataFrame(puttingDataInS3(i), index=[0])
        else:
            logDf = pd.concat([logDf, pd.DataFrame(puttingDataInS3(i), index=[0])])

    logDf.reset_index(drop=True, inplace=True)
    print(logDf)

    # Writing MySql Data In CSV - Using Pandas
    csv_buffer = StringIO()
    logDf.to_csv(csv_buffer, index=False),

    # Variables For logFiles
    currentDateTime = datetime.utcnow()
    dateTimeStringFormat2 = currentDateTime.strftime("%Y%m%d%H%M%S")  # Format - DDMMYYHHMMSS - Will be used for csv file

    etlLogBucket = 'da-dop-prod-eu-data-etllogs'
    etlLogFilePathNameTime = 'lambdaFunctionLog/lambdaLogFile{}{}.csv'.format('-', dateTimeStringFormat2)
    
    etlLogBucket = 'da-dop-ew1-0bc7-prod-eu-data-etllogs' #GauravAdded_5_9_2023
    etlLogFilePathNameTime = 'pipeline-prod-0-log_table/lambdaLogFile{}{}.csv'.format('-', dateTimeStringFormat2) #GauravAdded_5_9_2023

    # Writing CSV File in S3
    s3Resource = boto3.resource('s3')
    s3Resource.Object(bucket_name=etlLogBucket, key=etlLogFilePathNameTime).put(Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!'),
        # 'result1': listOfSourceTablesInDynamoDbTable(),
        # 'result2': puttingDataInS3('DataHub|view_0BC7_incident'),
        # 'result3': puttingDataInS3('DataHub|view_0BC7_em_remediation_task'),
        # 'result3': puttingDataInS3('DataHub|view_0BC7_sys_user_group'),
        # 'JobLog': LogTableData,
        # 'JobLog': logDf,
        # 'pendingTasks': pendingTasksList,
        'Status': 'Job Done'
    }


main()

job.commit()
