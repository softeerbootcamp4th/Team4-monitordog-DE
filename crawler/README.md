## StepFunctions + Lambda를 이용한 크롤러(스크래이퍼)

각 디렉토리에 존재하는 람다 Lv.1, Lv.2 ...는 StepFunctions의 각 단계를 의미한다.  
각 하위 디렉토리에 존재하는 StepFunctions.json 파일: 사이트별 크롤러의 StepFunctions 정의  

### 'aggregator.py'
각 사이트별 크롤러와 연결된 SQS 상의 메시지(게시글 정보)를 수집하여 하나의 jsonl 파일로 취합해 S3 버킷에 업로드한다.  
![alt text](/assets/aggregator.png)  

### 'scheduler_timezone_converter.py'
EventBridge 스케줄러가 제공하는 입력 "<aws.scheduler.scheduled-time>"의 UTC 시간을 KST로 변환한다.  

### 'workflow/StepFunctions.json'
4개 사이트의 크롤러를 하나로 묶어 동작시키는 크롤링 워크플로우를 정의한다.  
![workflow_statemachine.png](/assets/workflow_statemachine.png)  

## AWS 자원 생성 
1. 각 소스별 람다 함수를 작성한다.
    - 이미지를 빌드하고 ECR에 업로드해야함.
2. StepFunctions 정의를 통해 새로운 상태머신을 생성한다.  
3. 다음과 같이 SQS를 생성해준다. 여기서 생성한 SQS는 각 사이트별 크롤러가 수집한 페이지 정보를 모으는 버퍼로 동작한다.  
![alt text](/assets/sqs.png)    

4. S3에 이벤트 알림을 등록한다.   
![alt text](/assets/s3_event.png)   

5. 'workflow/StepFunctions.json'으로 생성한 상태머신을 이벤트 브릿지 스케줄러에 등록한다.
- 예제 입력은 다음과 같다.
    ```json
    {
        "keyword": "코나",
        "period": 2,
        "start_date": "<aws.scheduler.scheduled-time>"
    }
    ```

## 결과
다음과 같이 크롤러가 실행된 시간별로 S3에 쌓이게된다.  
![alt text](/assets/s3_data.png)  


