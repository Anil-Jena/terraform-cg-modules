### AWS Glue Python Script

import sys
import boto3
import pandas as pd
import splunklib.client as client
import splunklib.results as results
from io import StringIO
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

## Get the parameters from the Glue job
#args = getResolvedOptions(sys.argv, [
#    'S3_BUCKET',
#    'SPLUNK_HOST',
#    'SPLUNK_PORT',
#    'SPLUNK_USERNAME',
#    'SPLUNK_PASSWORD',
#    'SPLUNK_QUERY_1',
#    'SPLUNK_QUERY_2'
#])

# Glue context and Spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Splunk configuration
SPLUNK_HOST = 'ip-10-84-148-51.0bc7-dop-aiops-lan.aws.cloud.airbus.corp'
SPLUNK_PORT = 8089
SPLUNK_USERNAME = 'data_repo2'
SPLUNK_PASSWORD = 'L6tm64n@a13ps'
SPLUNK_QUERY_1 = 'search index="itsi_grouped_alerts" earliest=-60m AND latest=now | table  *'
SPLUNK_QUERY_2 = 'search index="app_snow_ticket_updates" earliest=-60m AND latest=now | table  *'

# S3 configuration
S3_BUCKET = 'dop-datarepo-prod-test-bucket'

def fetch_splunk_data(query):
    # Connect to Splunk
    service = client.connect(
        host=SPLUNK_HOST,
        port=SPLUNK_PORT,
        username=SPLUNK_USERNAME,
        password=SPLUNK_PASSWORD
    )

    # Run Splunk query
    job = service.jobs.create(query)
    while not job.is_done():
        pass

    # Retrieve results
    reader = results.ResultsReader(job.results())
    data = [dict(result) for result in reader]

    # Convert to DataFrame
    df = pd.DataFrame(data)
    return df

# Fetch data from Splunk
df1 = fetch_splunk_data(SPLUNK_QUERY_1)
df2 = fetch_splunk_data(SPLUNK_QUERY_2)

# Convert DataFrames to CSV in memory
csv_buffer1 = StringIO()
csv_buffer2 = StringIO()
df1.to_csv(csv_buffer1, index=False)
df2.to_csv(csv_buffer2, index=False)

# Upload CSVs to S3
s3_client = boto3.client('s3')
s3_client.put_object(Bucket=S3_BUCKET, Key='Splunk_data/itsi_grouped_alerts.csv', Body=csv_buffer1.getvalue())
s3_client.put_object(Bucket=S3_BUCKET, Key='Splunk_data/app_snow_ticket_updates.csv', Body=csv_buffer2.getvalue())

print("Data loaded successfully from Splunk to S3")
