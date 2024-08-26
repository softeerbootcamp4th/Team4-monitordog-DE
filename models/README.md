## 모델 (추론)서빙을 위한 Flask 서버

- [api-server/](https://github.com/softeerbootcamp4th/Team4-monitordog-DE/tree/main/models/api-server) : HTTP 요청의 Body로 json 하나씩 받아 추론결과를 반환합니다.
- [batch-server/](https://github.com/softeerbootcamp4th/Team4-monitordog-DE/tree/main/models/batch-server) : S3에 존재하는 jsonl에 대한 메타데이터(경로, 이름)를 받아 배치 처리 후 결과를 S3에 다시 저장합니다.