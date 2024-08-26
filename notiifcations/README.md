## 슬랙 알림 전송을 위한 람다 함수

1. 'slack_notifier.py'를 이용해 람다 함수를 생성합니다.
2. 메시지를 게시할 SNS 토픽을 생성합니다.
3. 프로듀서에게 메시지를 받아 슬랙 알림으로 전송하기 위해 'slack_notifier'를 SNS 토픽에 구독시킵니다..  
![SNS 구독](/assets/sns_subscribe.png)  
4. 슬랙 워크플로우를 생성합니다. (설명 생략)
5. 'demo.py'를 통해 앞서 생성한 SNS 토픽에 메시지를 게시합니다.  