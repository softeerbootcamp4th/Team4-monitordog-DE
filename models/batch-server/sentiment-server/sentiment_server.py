import boto3
import json
import logging
from urllib import parse
from flask import Flask, request, jsonify
from transformers import TextClassificationPipeline, BertForSequenceClassification, AutoTokenizer


app = Flask(__name__)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,  # 로그 레벨 설정
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # 로그 포맷
    handlers=[
        logging.FileHandler("/var/log/flask.log"),  # 로그 파일 위치 설정
        logging.StreamHandler()  # 콘솔에 출력 설정
    ]
)

logger = logging.getLogger(__name__)


def batch_inference(bucket_name, file_name):
    model = SentimentModel()
    file_name = parse.unquote(file_name)
    logging.info("파라미터 확인, 버킷: " + bucket_name + ", 파일: " + file_name)

    local_file_path = "/tmp/local_file.jsonl"
    try:
        s3 = boto3.client('s3')
        # S3에서 파일 다운로드
        s3.download_file(bucket_name, file_name, local_file_path)
    except Exception as e:
        logging.info("파일 다운로드 실패", e.with_traceback())

    logging.info("jsonl inference 시작")
    line_num = 1
    # JSONL 파일 읽기 및 추론
    with open(local_file_path, 'r') as infile, open('/tmp/modified_file.jsonl', 'w') as outfile:
        for line in infile:
            line_num += 1
            record = json.loads(line)
            input_data = record['title'] + " " + record['content']
            sentiment = model.get_sentiment_score(input_data)

            # 결과 추가
            record['sentiment'] = sentiment

            # 수정된 행 저장
            outfile.write(json.dumps(record, ensure_ascii=False) + '\n')
    logging.info("jsonl inference 종료")


    try:
        logging.info("파일 업로드")
        file_metadata = file_name.split("/")
        origin_file_name = file_metadata[-1]
        origin_file_path = file_metadata[-2]
        # 수정된 파일을 S3에 업로드
        s3.upload_file('/tmp/modified_file.jsonl', bucket_name, f"sentiment/{origin_file_path}/{origin_file_name}")
    except Exception as e:

        logging.info("파일 업로드 실패", e.with_traceback())

class SentimentModel(object):
    """
    huggingfaces 감정 분석 모델 클래스
    """
    def __init__(self, model_name: str = 'smilegate-ai/kor_unsmile', **configs):
        """
        감정 분석 모델을 사용하기 위한 초기화 함수
        :param model_name: huggingfaces 모델 이름
        """
        self.model_name = model_name
        self.model = BertForSequenceClassification.from_pretrained(model_name)
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, clean_up_tokenization_spaces=False)
        self.pipe = TextClassificationPipeline(
            model = self.model,
            tokenizer = self.tokenizer,
            device = -1,   # cpu: -1, gpu: gpu number
            top_k = None, # 모든 class 점수 반환
            function_to_apply = 'sigmoid'
        )
        if len(configs) > 0:
            self.configs = configs
        else:
            self.configs = {
                'padding': True,
                'truncation': True,
                'max_length': 300,
            }


    def get_sentiment_score(self, text: str) -> float:
        """
        감정 분석 모델을 통해 텍스트의 부정적 감정을 점수화
        :param pipe: 감정 분석을 수행하는 모델 파이프라인
        :param text: 감정 분석 대상 텍스트
        :return: 텍스트 내용에 담긴 부정적 감정의 정도를 점수화, 1에 가까울수록 부정적
        """
        results = self.pipe(text, **self.configs)
        # 결과 중 clean 텍스트일 확률 고르기
        labels = list(map(lambda x: x['label'], results[0]))
        score = results[0][labels.index('clean')]['score']

        return 1.0 - score # clean 텍스트 확률을 욕설이나 비방글일 확률로 변환


@app.route('/sentiment_analysis', methods=['POST'])
def extract_keywords():
    data = request.json
    bucket_name = data.get('bucket_name')
    file_name = data.get('file_name')

    if not bucket_name:
        return jsonify({'error': 'No bucket_name provided'}), 400

    if not file_name:
        return jsonify({'error': 'No file_name provided'}), 400

    batch_inference(bucket_name, file_name)
    return "batch process is called"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)