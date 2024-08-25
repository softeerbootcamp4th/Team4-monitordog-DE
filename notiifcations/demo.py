import boto3
import json

message = {
    "AlarmName": "<ISSUED_KEYWORD>",
    "NewStateValue": "<ISSUE_POINT>",
    "NewStateReason": "<SIMILAR_HISTORY>"
}

client = boto3.client('sns', region_name='ap-northeast-2')
response = client.publish(    
    TargetArn="<SNS_TOPIC_ARN>", # slack_notifier가 구독하는 토픽
    Message=json.dumps(message)
)