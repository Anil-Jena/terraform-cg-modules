import json
import os
import boto3
import base64
import logging
import ssl
import http.client
import tempfile
 
# Constants

SECRETS = {
    'public_key': 'arn:aws:secretsmanager:eu-west-1:204955557577:secret:cert/prod/client-pem-pgQdc9',
    'private_key': 'arn:aws:secretsmanager:eu-west-1:204955557577:secret:cert/prod/client-key-6reMh7',
    'cacert': 'arn:aws:secretsmanager:eu-west-1:204955557577:secret:cert/prod/cacert-1wcZvV' 
}
API_URL = 'https://ssobroker.airbus.com:10443/as/token.oauth2'
API_URL1 = 'https://prod.api-platform.airbus.corp/servicenow-gw-e-l-v1/api/incident'
POST_DATA = 'grant_type=client_credentials&client_id=CLI_0BC7_DOP-M2M&scope=SCO_2D83_INCIDENTS_READ+SCO_2D83_INCIDENTS_UPDATE'.encode('utf-8')

# Setup logging
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
 
secrets_manager = boto3.client('secretsmanager')
 
def get_secret(arn):
    logger.info(f"Fetching secret for ARN: {arn}")
    result = secrets_manager.get_secret_value(SecretId=arn)
    if 'SecretString' in result:
        return result['SecretString']
    else:
        return base64.b64decode(result['SecretBinary']).decode('utf-8')
 
def create_temp_file(data):
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(data.encode())
        logger.info(f"Temporary file created at: {temp_file.name}")
        return temp_file.name
 
def get_ssl_context(cert_path, key_path):
    logger.info("Creating SSL context...")
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.load_cert_chain(certfile=cert_path, keyfile=key_path)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    return ssl_context
 
def get_token():
    logger.info("Getting token...")
    cert_path = create_temp_file(get_secret(SECRETS['public_key']))
    key_path = create_temp_file(get_secret(SECRETS['private_key']))
 
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    connection = http.client.HTTPSConnection('ssobroker.airbus.com', 10443, context=get_ssl_context(cert_path, key_path))
    connection.request('POST', API_URL, POST_DATA, headers)
    response = connection.getresponse()
    token_data = json.loads(response.read())
 
    os.remove(cert_path)
    os.remove(key_path)
 
    if response.status == 200:
        logger.info("Token successfully retrieved.")
        return token_data['access_token']
    else:
        logger.error(f"Failed to retrieve token. Response: {response.read()}")
        return None
 
def post_incident(token,event):
    logger.info("Posting incident...")
    cert_path = create_temp_file(get_secret(SECRETS['public_key']))
    key_path = create_temp_file(get_secret(SECRETS['private_key']))
 
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
        'client_secret': '51b1a30e550A40979E3A798c576f902B',
        'client_id': '5aea0a54b9ec449faf6732e871c5f9f8',
    }
 
    connection = http.client.HTTPSConnection('prod.api-platform.airbus.corp', 443, context=get_ssl_context(cert_path, key_path))
    connection.request('POST', API_URL1, json.dumps(event), headers)
    response = connection.getresponse()
 
    if response.status != 200 and response.status != 201:
        logger.error(f"Error posting incident: {json.loads(response.read())}")
    else:
        logger.info(f"Incident successfully posted. Incident payload: {json.loads(response.read())}")
 
def lambda_handler(event, context):
    logger.info("Lambda handler started.")
    logger.info(f"event is: {event}")
    token = get_token()
    logger.info(f"Token is: {token}")
    if token:
        post_incident(token,event)
    logger.info("Lambda handler execution completed.")
    return {
        'statusCode': 200,
        'body': json.dumps('Execution completed.')
    }