# Team4-monitordog-DE
현대자동차 소프티어 부트캠프 4기 Data Engineering 4조

## 팀원 소개
|팀원|github|
|-|-|
|장철희|[@jang-namu](https://github.com/jang-namu)|
|최철웅|[@steelbear](https://github.com/steelbear)|
|홍주원|[@juwon00](https://github.com/juwon00)|

## 폴더 구조
- [crawler/](https://github.com/softeerbootcamp4th/Team4-monitordog-DE/tree/main/crawler): AWS StepFunctions(+Lambda) 기반 크롤러 사용을 위한 파일들
- [models/](https://github.com/softeerbootcamp4th/Team4-monitordog-DE/tree/main/models): 감정분석, 키워드 추출 모델 서빙을 위한 Flask 서버와 EMR 과정을 담은 파일들
- [notifications/](https://github.com/softeerbootcamp4th/Team4-monitordog-DE/tree/main/notifications): 외부 연동, Slack 알림 제공을 위한 람다 함수와 사용예제
- [transform/](https://github.com/softeerbootcamp4th/Team4-monitordog-DE/tree/main/transform): 데이터 포맷 일치화 등 데이터 변환을 위한 과정을 담은 파일들
- [assets/](https://github.com/softeerbootcamp4th/Team4-monitordog-DE/tree/main/assets): 리드미 작성을 위한 리소스(이미지, 영상, 파일)

## 아키텍처
![arichitecture](/assets/architecture.png)  

