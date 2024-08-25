#!/bin/bash
# Add Docker's official GPG key:

sudo yum update -y
sudo yum install -y python3 python3-pip
# transformers[torch]: for CPU-support only, install Transformers and PyTorch in one line 
sudo pip install transformers[torch] flask

sudo mkdir -p /opt/flask/logs
sudo chmod -R 777 /opt/flask/ 
sudo aws s3 cp s3://monitordog-model/sentiment-analysis.py /opt/flask/app.py
sudo chmod 755 /opt/flask/app.py
sudo nohup python3 /opt/flask/app.py > /opt/flask/logs/flask.log 2>&1 &
