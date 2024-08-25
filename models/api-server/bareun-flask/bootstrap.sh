#!/bin/bash
# Add Docker's official GPG key:

# https://docs.aws.amazon.com/ko_kr/serverless-application-model/latest/developerguide/install-docker.html
sudo yum update -y
sudo yum install -y docker

# Docker 서비스 시작 및 부팅 시 자동 시작 설정
sudo service docker start

sudo usermod -a -G docker ec2-user

sudo docker pull public.ecr.aws/t7d3a2t0/monitordog/bareun:latest
mkdir -p ~/bareun/var

sudo docker run \
   -d \
   --restart unless-stopped \
   --name bareun \
   --user $UID:$GID \
   -p 5757:5757 \
   -p 9902:9902 \
   -v ~/bareun/var:/bareun/var \
   bareunai/bareun:latest

sudo docker exec bareun /bareun/bin/bareun -reg <API_KEY>

sudo yum install -y python3 python3-pip
sudo pip install numpy scikit-learn sentence-transformers bareunpy flask

sudo mkdir -p /opt/flask/logs

aws s3 cp s3://monitordog-model/kpfSBERT.tar.gz ./kpfSBERT.tar.gz
aws s3 cp s3://monitordog-model/keyword-extract.py /opt/flask/app.py

tar -xvf kpfSBERT.tar.gz
mv output/kpfSBERT /opt/flask/kpfSBERT

sudo chmod -R 777 /opt/flask/ 
sudo chmod 755 /opt/flask/app.py

rm -rf output/
rm -rf kpfSBERT.tar.gz
echo 'API_KEY="<API_KEY>"' >> /etc/environment
sudo nohup python3 /opt/flask/app.py > /opt/flask/logs/flask.log 2>&1 &
