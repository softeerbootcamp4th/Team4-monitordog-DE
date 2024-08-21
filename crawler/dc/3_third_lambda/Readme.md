docker build --platform linux/amd64 -t selenium-chrome-driver:third .

docker tag selenium-chrome-driver:third 367354627828.dkr.ecr.ap-northeast-2.amazonaws.com/dc:third

aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin 367354627828.dkr.ecr.ap-northeast-2.amazonaws.com/dc

docker push 367354627828.dkr.ecr.ap-northeast-2.amazonaws.com/dc:third