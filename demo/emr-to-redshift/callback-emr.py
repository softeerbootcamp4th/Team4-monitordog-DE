import boto3
import json

def lambda_handler(event, context):
    # S3 버킷 및 객체 키 정보 추출
    s3_info = event['Records'][0]['s3']
    bucket_name = s3_info['bucket']['name']
    object_key = s3_info['object']['key']
    
    # EMR 클러스터 ID 및 스텝 정의
    emr_client = boto3.client('emr', region_name='ap-northeast-2')
    cluster_id = '<Cluster_ID>'  # EMR 클러스터 ID를 넣으세요
    
    step_args = [
        'spark-submit', 
        '--deploy-mode', 'cluster',
        's3://<your-bucket>/path-to-your-pyspark-script.py',
        bucket_name,
        object_key
    ]
    
    step = {
        'Name': 'Process new data and load to Redshift',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': step_args
        }
    }
    
    response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[step]
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('EMR step added: ' + response['StepIds'][0])
    }
