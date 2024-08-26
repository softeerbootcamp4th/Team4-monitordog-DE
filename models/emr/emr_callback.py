import boto3
import json


def lambda_handler(event, context):
    BUCKET_NAME = "monitordog-data"
    DIRECTORY_PATH = "keywords/"
    required_file_count = 4
    
    s3_client = boto3.client('s3', region_name='ap-northeast-2')
    emr_client = boto3.client('emr', region_name='ap-northeast-2')
    
    file_list = s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=DIRECTORY_PATH)["Contents"]
    file_list.sort(key=lambda x: x["LastModified"], reverse=True)
    recent_file = [file["Key"] for file in file_list][0]
    print("recent_file", recent_file)
    recent_time = recent_file.split("_")[-1].split(".")[0]
    print("recent_time", recent_time)

    DEEP_DIRECTORY_PATH = f"keywords/{recent_time}"
    s3_objects = s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=DEEP_DIRECTORY_PATH)
    
    print(s3_objects)
    
    if 'Contents' in s3_objects:
        uploaded_files_count = len(s3_objects['Contents'])
    else:
        uploaded_files_count = 0
    
    if uploaded_files_count >= required_file_count:
        
        cluster_id = 'j-3U3DUF2VU89CD'  # EMR 클러스터 ID를 넣으세요
        step_args = [
            'spark-submit', 
            '--deploy-mode', 'cluster',
            's3://ex-emr/scripts/emr.py',
            '--recent_time', recent_time  # recent_time 값을 매개변수로 추가
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
        print(s3_objects['Contents'])
        return {
            'statusCode': 200,
            'body': json.dumps('EMR step added: ' + response['StepIds'][0])
        }
    else:
        print("not yet")
        return {
            'statusCode': 200,
            'body': json.dumps('Required number of files not yet uploaded.')
        }
