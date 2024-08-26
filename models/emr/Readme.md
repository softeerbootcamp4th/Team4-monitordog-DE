# EMR 데이터 처리 파이프라인

AWS EMR(Elastic MapReduce) 클러스터를 사용하여 데이터를 처리하는 파이프라인을 구현합니다.

── emr<br>
       ├── Readme.md<br>
       ├── config<br>
       │   └── bootstrap3.sh<br>
       ├── emr.py<br>
       └── emr_callback.py

<br>

![emr drawio](https://github.com/user-attachments/assets/30c09db6-bfd5-4e1d-8b7e-aa47cc0d7a1c)

<br>

## 주요 구성 요소

### 1. Lambda
Lambda 함수는 s3에 가공된 데이터가 들어오는걸 감지하고 EMR 클러스터에서 Spark 작업을 트리거합니다.

### 2. EMR 클러스터
EMR 클러스터는 Spark 작업을 실행하여 데이터를 처리합니다.

### 3. Spark 작업
Spark 작업은 다음과 같은 주요 작업을 수행합니다:
   - 데이터를 S3에서 읽어들입니다.
   - 데이터 전처리 및 변환 작업을 수행합니다.
     - 현재 이슈 키워드 추출
     - 현재 이슈화 정도 측정
     - 과거 사건과의 유사도 측정
   - 처리된 데이터를 Redshift에 저장합니다.

### 4. Redshift
처리된 데이터는 Amazon Redshift에 저장되어 이후 분석 및 시각화에 사용됩니다.

### 5. Tableau 대시보드
Redshift에 저장된 데이터는 Tableau에서 시각화되어 대시보드에서 확인할 수 있습니다.

