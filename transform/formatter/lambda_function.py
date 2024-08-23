import json
import logging
import os
import urllib.parse
from io import BytesIO

import boto3
from data_formatting import preprocess_post


s3_client = boto3.client('s3')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def preprocess(input_file_obj, output_file_obj, data_source):
    while True:
        line = input_file_obj.readline()
        if not line:
            break
        line = line.decode('utf-8')

        post_info = json.loads(line)
        if len(post_info['created_at'].strip()) == 0:
            continue

        post_info = preprocess_post(post_info, data_source)

        processed_line = (json.dumps(post_info, ensure_ascii=False) + '\n').encode('utf-8')

        output_file_obj.write(processed_line)


def folder2source(foldername):
    if foldername == 'dc':
        return 'dcinside'
    elif foldername == 'naver':
        return 'naver'
    elif foldername == 'bobae':
        return 'bobae'
    elif foldername == 'clien':
        return 'clien'
    else:
        raise ValueError(f"Cannot use folder name '{foldername}'")


def lambda_handler(event, context):
    try:
        s3_event = event['Records'][0]['s3']
        source_bucket = s3_event['bucket']['name']
        # key: dc/dc_아이오닉6_2024-8-10_14-00-00.jsonl
        source_key = urllib.parse.unquote_plus(s3_event['object']['key'])
        source_basename = os.path.splitext(source_key)[0].split('/') 
        source_folder = source_basename[0]
        source_file = source_basename[1]
        source_date = '_'.join(source_file.split('_')[-2:])
        
        destination_bucket = source_bucket  # 또는 다른 버킷을 지정할 수 있습니다.
        destination_key = f"done/{source_date}/{source_file}{os.path.splitext(source_key)[1]}"

        logger.info(f"Processing {source_key} from bucket {source_bucket}")

        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        input_file_obj = response['Body']

        output_file_obj = BytesIO()

        # 파일을 한 줄씩 처리합니다
        data_source = folder2source(source_folder)
        preprocess(input_file_obj, output_file_obj, data_source)

        output_file_obj.seek(0)

        s3_client.put_object(Bucket=destination_bucket, Key=destination_key, Body=output_file_obj.getvalue())

        logger.info(f"Processed and saved as {destination_key} in bucket {destination_bucket}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'bucket': destination_bucket,
                'key': destination_key
            })
        }
    except Exception as e:
        logger.info(f'Error processing {source_bucket}/{source_key}: {str(e)}')
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing {source_bucket}/{source_key}: {str(e)}')
        }
