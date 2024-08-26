# ML 배치 처리 서버  

```bash
├── keywords-extract # 키워드 추출 (게시글을 대표하는 키워드 0~5개를 추출합니다.)
└── sentiment-server # 감정분석 (텍스트 분석을 통해 SNS 유저의 감정을 0~1 사이 점수로 점수화합니다.)
```

아래 두 람다 함수를 사용하기 위해서는 환경변수에 API_ENDPOINT(각 Flask 서버가 제공하는 API)를 추가해야 합니다.  
- keywords-extract/keywords-extract-trigger.py
- sentiment-server/sentiment-analysis-trigger.py