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


def preprocess(input_file_obj, output_file_obj, data_source, car_model):
    '''
    수집된 게시글이 담긴 S3 오브젝트에서 형식 통일 및 데이터 전처리 후 저장
    '''
    # Todo: 보배드림에서 클래스 이름이 변경됨에 따라 데이터를 수집하지 못한 경우도 발생
    # 이에 제대로 저장되지 않은 데이터는 무시하고 온전히 기록된 데이터만 처리
    # 크롤러에서 발생한 문제 수정 후 해당 JSONDecodeError 무시를 위한 try-except 문을 지울 것
    while True:
        try:
            line = input_file_obj.readline()
        except:
            break
        
        if not line:
            break

        try:
            line = line.decode('utf-8')
            post_info = json.loads(line)
        except Exception as e:
            logger.error(str(e))
            continue
        
        if len(post_info['created_at'].strip()) == 0:
            continue

        try:
            post_info = preprocess_post(post_info, data_source)
            post_info['data_source'] = data_source
            post_info['model'] = car_model

            processed_line = (json.dumps(post_info, ensure_ascii=False) + '\n').encode('utf-8')

            output_file_obj.write(processed_line)
        except Exception as e:
            logger.error(str(e))
            continue


def lambda_handler(event, context):
    try:
        # 이벤트 파싱
        s3_event = event['Records'][0]['s3']
        source_bucket = s3_event['bucket']['name']
        # key: raw/2024-08-10T14-00-00/dc_아이오닉6_2024-08-10_14-00-00.jsonl
        source_key = urllib.parse.unquote_plus(s3_event['object']['key'])
        source_filename = source_key.split('/')[-1]

        # Source 정보
        data_source, car_model, source_date = source_filename.split('_')
        source_date = source_date.split('.')[0]
        
        # Destination 정보
        destination_bucket = source_bucket  # 또는 다른 버킷을 지정할 수 있습니다.
        destination_key = f"formatted/{source_date}/{source_filename}"

        logger.info("Processing %s from bucket %s", source_key, source_bucket)

        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        input_file_obj = response['Body']

        output_file_obj = BytesIO()

        # 파일을 한 줄씩 처리합니다
        preprocess(input_file_obj, output_file_obj, data_source, car_model)

        output_file_obj.seek(0)

        s3_client.put_object(Bucket=destination_bucket, Key=destination_key, Body=output_file_obj.getvalue())

        logger.info("Processed and saved as %s in bucket %s", destination_key, destination_bucket)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'bucket': destination_bucket,
                'key': destination_key
            })
        }
    except Exception as e:
        logger.info('Error processing %s/%s: %s', source_bucket, source_key, str(e))
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing {source_bucket}/{source_key}: {str(e)}')
        }
