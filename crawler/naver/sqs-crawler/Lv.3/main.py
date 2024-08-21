import json
import boto3

sqs = boto3.client('sqs')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    queue_url = event['queue_url']
    bucket_name = event['bucket']['name']
    bucket_path = event['bucket']['path']
    file_name = event['file_name']

    messages = []    
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,  # 최대 10개 메시지 폴링
            WaitTimeSeconds=1,       # Long polling으로 1초 대기
        )
        
        # 메시지가 없으면 종료
        if 'Messages' not in response:
            break
        
        for message in response['Messages']:
            messages.append(json.loads(message["Body"]))
            # 메시지 삭제
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )

    # 메시지가 없으면 작업 종료
    if not messages:
        return {'statusCode': 204, 'body': 'No messages to process'}
    
    # 메시지를 JSONL 포맷으로 변환
    jsonl_data = "\n".join([json.dumps(msg, ensure_ascii=False) for msg in messages])

    # S3에 업로드
    s3.put_object(
        Body=jsonl_data, 
        Bucket=bucket_name, 
        Key=f"{bucket_path}/{file_name}",
        ContentType='application/jsonl'
        )
    
    return {
        'statusCode': 200,
        'body': f'Successfully uploaded {len(messages)} messages to {bucket_path}/{file_name} in {bucket_name}.'
    }
