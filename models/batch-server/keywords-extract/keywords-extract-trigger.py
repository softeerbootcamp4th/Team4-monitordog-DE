import json
import urllib3
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


http = urllib3.PoolManager()


def lambda_handler(event, context):
    s3_event = event['Records'][0]['s3']
    bucket_name = s3_event['bucket']['name']
    file_name = s3_event['object']['key']
    
    api_endpoint = os.environ["API_ENDPOINT"]
    logging.info(bucket_name + "/" + file_name + ", URL: " + api_endpoint)
    payload = {
        "bucket_name": bucket_name,
        "file_name": file_name
    }
    try:
        response = http.request('POST',
                        api_endpoint,
                        body = json.dumps(payload),
                        headers = {'Content-Type': 'application/json'},
                        retries = False)
        return {
            "statusCode": response.status,
            "body": json.loads(response.data)
        }
    except Exception as e:
        logging.error("Exception: " + str(e))
        return {
            "statusCode": 500,
            "body": str(e)
        }
    






        
    