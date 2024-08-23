FROM amazon/aws-lambda-python:3.10

RUN pip install soynlp
COPY data_formatting.py .
COPY lambda_function.py .
CMD [ "lambda_function.lambda_handler" ]