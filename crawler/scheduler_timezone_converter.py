from datetime import datetime, timedelta
import os

TIMEZONE_OFFSET = os.environ["TIMEZONE_OFFSET"]

def lambda_handler(event, context):
    keyword = event['keyword']
    period = event['period']
    start_date = datetime.fromisoformat(event['start_date'][:-1])
    
    converted_date = start_date + timedelta(hours=TIMEZONE_OFFSET)
    converted_date = converted_date.strftime('%Y-%m-%dT%H:%M:%S')    
    
    body = {
        "keyword": keyword,
        "period": period,
        "start_date": str(converted_date)
    }
    
    return {
        'statusCode': 200,
        'body': body
    }
