## 슬랙 알림 전송을 위한 람다 함수

1. 'slack_notifier.py'를 이용해 람다 함수를 생성합니다.
2. 메시지를 게시할 SNS 토픽을 생성합니다.
3. 프로듀서가 게재한 메시지를 슬랙 알림으로 전송하기 위해 'slack_notifier'가 SNS 토픽을 구독하도록 합니다.
![SNS 구독](/assets/sns_subscribe.png)  
4. 슬랙 워크플로우를 생성합니다. (아래 사진 참고)
5. 'demo.py'를 통해 앞서 생성한 SNS 토픽에 메시지를 게시합니다.  


### 슬랙 워크플로우 생성

![slack_workflow_1](/assets/slack_workflow_1.png)  
![slack_workflow_2](/assets/slack_workflow_2.png)  
![slack_workflow_3](/assets/slack_workflow_3.png)  
![slack_workflow_4](/assets/slack_workflow_4.png)  
![slack_workflow_5](/assets/slack_workflow_5.png)  
![slack_workflow_6](/assets/slack_workflow_6.png)  
![slack_workflow_7](/assets/slack_workflow_7.png)  

### 결과
![slack_notification](/assets/slack_notification.png)