# ML 배치 처리 서버  
```bash
├── keywords-extract # 키워드 추출 (게시글을 대표하는 키워드 0~5개를 추출합니다.)
└── sentiment-server # 감정분석 (텍스트 분석을 통해 SNS 유저의 감정을 0~1 사이 점수로 점수화합니다.)
```

## Flask 서버
ML 모델의 추론을 제공하기 위해 간단한 Flask 서버를 이용합니다.  
EC2 인스턴스를 생성창에서 bootstrap.sh를 사용자 데이터에 업로드하면 8080 포트로 Flask 서버가 열립니다.  
사용자는 EC2의 'IP:8080/[keywords-extraction | sentiment_analysis]'에 접근하여 API를 활용할 수 있습니다.   
다만, API 사용과 관련된 부분은 기본적으로 '*-trigger.py'의 람다 함수가 알아서 처리합니다.  
사용자는 '*-trigger.py'의 람다 함수를 버킷의 알림 등록 해두면됩니다.

## 람다 함수
하위 디렉터리의 두 람다 함수('*-trigger.py')를 사용하기 위해서는 환경변수에 API_ENDPOINT(각 Flask 서버가 제공하는 API)를 추가해야 합니다.  

> 안내: 아래 두 파일의 내용은 같습니다. 람다를 등록하고 두 람다 함수의 환경변수를 다르게 설정해주세요.  
>- 'keywords-extract/keywords-extract-trigger.py'
>- 'sentiment-server/sentiment-analysis-trigger.py'