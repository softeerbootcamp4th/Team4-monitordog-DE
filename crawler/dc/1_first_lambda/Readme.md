docker build --platform linux/amd64 -t selenium-chrome-driver:first .

docker tag selenium-chrome-driver:first 367354627828.dkr.ecr.ap-northeast-2.amazonaws.com/dc:first

aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin 367354627828.dkr.ecr.ap-northeast-2.amazonaws.com/dc

docker push 367354627828.dkr.ecr.ap-northeast-2.amazonaws.com/dc:first



{
    "target_prefix": "dc",
    "keyword": "코나",
    "url": "https://gall.dcinside.com/board/lists?id=car_new1",
    "start_date": "2024-08-13T15:40:00",
    "period": 3,
    "queue_url": "https://sqs.ap-northeast-2.amazonaws.com/367354627828/dc-crawler-sqs"
}
